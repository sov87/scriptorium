from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable


def ensure_ai_tables(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        create table if not exists ai_runs(
          run_id text primary key,
          kind text not null,
          generated_utc text,
          corpus_filter text,
          query text,
          out_dir text,
          llm_base_url text,
          llm_model text,
          embed_model text,
          use_e5_prefix integer,
          params_json text
        );

        create table if not exists gloss(
          run_id text not null,
          segment_id text not null,
          corpus_id text,
          work_id text,
          loc text,
          gloss text,
          literal text,
          notes_json text,
          primary key (run_id, segment_id)
        );

        create virtual table if not exists gloss_fts using fts5(
          segment_id,
          run_id,
          corpus_id,
          gloss,
          literal,
          tokenize = 'unicode61'
        );

        create table if not exists answers(
          run_id text primary key,
          query text,
          corpus_filter text,
          answer text,
          citations_json text,
          notes_json text,
          retrieval_json text,
          validation_json text,
          meta_json text
        );

        create virtual table if not exists answers_fts using fts5(
          run_id,
          query,
          answer,
          tokenize = 'unicode61'
        );
        """
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue


def _fts_delete_run(con: sqlite3.Connection, table: str, run_id: str) -> None:
    con.execute(f"delete from {table} where run_id=?", (run_id,))




def _extract_cites(citations_json: str, retrieval_json: str, max_items: int = 12) -> list[str]:
    '''Best-effort extraction of cited corpus_id:segment_id pairs from stored JSON blobs.

    Scope: parse stored JSON fields only (citations_json / retrieval_json). No inference from answer text.
    Strategy:
      - Prefer explicit fields in dicts (corpus_id/corpus + segment_id/id).
      - Also scan all string values for corpus_id:segment_id patterns (e.g., oe_beowulf_9701:000959).
      - Recursively walk dict/list structures and stop at max_items.
    '''
    out: list[str] = []
    seen: set[str] = set()

    # Require a letter-starting corpus id to avoid accidental matches (e.g., "http:").
    _re = re.compile(r"([A-Za-z][A-Za-z0-9_]{2,}):([0-9]{3,})")

    def add(corpus: str, seg) -> None:
        if seg is None:
            return
        corpus_s = (str(corpus) if corpus is not None else "").strip()
        seg_s = (str(seg) if seg is not None else "").strip()
        if not corpus_s or not seg_s:
            return

        # Normalize cases where seg already includes a corpus prefix, e.g. "oe_beowulf_9701:000959".
        if ":" in seg_s:
            mm = _re.search(seg_s)
            if mm:
                corpus_s = mm.group(1)
                seg_s = mm.group(2)
            elif seg_s.startswith(corpus_s + ":"):
                seg_s = seg_s.split(":", 1)[1]

        key = f"{corpus_s}:{seg_s}"
        if key in seen:
            return
        seen.add(key)
        out.append(key)

    def walk(obj) -> None:
        if len(out) >= max_items:
            return

        if isinstance(obj, dict):
            corpus = ""
            seg = None

            for ck in ("corpus_id", "corpus", "corpus_filter"):
                v = obj.get(ck)
                if isinstance(v, str) and v:
                    corpus = v
                    break

            for sk in ("segment_id", "id", "seg_id"):
                v = obj.get(sk)
                if isinstance(v, (str, int)) and v is not None and v != "":
                    seg = v
                    break

            if corpus and seg is not None:
                add(corpus, seg)

            for v in obj.values():
                walk(v)
                if len(out) >= max_items:
                    return
            return

        if isinstance(obj, list):
            for it in obj:
                walk(it)
                if len(out) >= max_items:
                    return
            return

        if isinstance(obj, str):
            for m in _re.finditer(obj):
                add(m.group(1), m.group(2))
                if len(out) >= max_items:
                    return
            return

        return

    def consume(s: str) -> None:
        if not s:
            return
        try:
            parsed = json.loads(s)
        except Exception:
            walk(s)
            return
        walk(parsed)

    consume(citations_json)
    consume(retrieval_json)

    return out


def import_gloss_run(db_path: Path, run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    meta_path = run_dir / "meta.json"
    gloss_path = run_dir / "gloss.jsonl"

    if not gloss_path.exists():
        raise SystemExit(f"missing gloss.jsonl: {gloss_path}")

    meta: dict[str, Any] = {}
    if meta_path.exists():
        m = _read_json(meta_path)
        if isinstance(m, dict):
            meta = m

    run_id = meta.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        run_id = run_dir.name

    generated_utc = meta.get("generated_utc") if isinstance(meta.get("generated_utc"), str) else ""
    corpus = meta.get("corpus") if isinstance(meta.get("corpus"), str) else ""
    llm_base_url = meta.get("llm_base_url") if isinstance(meta.get("llm_base_url"), str) else ""
    llm_model = meta.get("llm_model") if isinstance(meta.get("llm_model"), str) else ""
    embed_model = meta.get("embed_model") if isinstance(meta.get("embed_model"), str) else ""
    use_e5_prefix = 1 if bool(meta.get("use_e5_prefix", False)) else 0

    con = sqlite3.connect(str(db_path))
    try:
        ensure_ai_tables(con)

        # Replace-safe
        con.execute("delete from gloss where run_id=?", (run_id,))
        _fts_delete_run(con, "gloss_fts", run_id)

        con.execute(
            """
            insert into ai_runs(run_id,kind,generated_utc,corpus_filter,query,out_dir,llm_base_url,llm_model,embed_model,use_e5_prefix,params_json)
            values(?,?,?,?,?,?,?,?,?,?,?)
            on conflict(run_id) do update set
              kind=excluded.kind,
              generated_utc=excluded.generated_utc,
              corpus_filter=excluded.corpus_filter,
              out_dir=excluded.out_dir,
              llm_base_url=excluded.llm_base_url,
              llm_model=excluded.llm_model,
              embed_model=excluded.embed_model,
              use_e5_prefix=excluded.use_e5_prefix,
              params_json=excluded.params_json
            """,
            (
                run_id,
                "gloss",
                generated_utc,
                corpus,
                "",
                str(run_dir),
                llm_base_url,
                llm_model,
                embed_model,
                use_e5_prefix,
                json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
            ),
        )

        n = 0
        for rec in _read_jsonl(gloss_path):
            sid = rec.get("id")
            if not isinstance(sid, str) or not sid:
                continue
            corpus_id = rec.get("corpus_id") if isinstance(rec.get("corpus_id"), str) else ""
            work_id = rec.get("work_id") if isinstance(rec.get("work_id"), str) else ""
            loc = rec.get("loc") if isinstance(rec.get("loc"), str) else ""
            gloss = rec.get("gloss") if isinstance(rec.get("gloss"), str) else ""
            literal = rec.get("literal") if isinstance(rec.get("literal"), str) else ""
            notes = rec.get("notes")
            notes_json = "[]" if notes is None else json.dumps(notes, ensure_ascii=False, separators=(",", ":"))

            con.execute(
                """
                insert or replace into gloss(run_id,segment_id,corpus_id,work_id,loc,gloss,literal,notes_json)
                values(?,?,?,?,?,?,?,?)
                """,
                (run_id, sid, corpus_id, work_id, loc, gloss, literal, notes_json),
            )
            con.execute(
                "insert into gloss_fts(segment_id,run_id,corpus_id,gloss,literal) values(?,?,?,?,?)",
                (sid, run_id, corpus_id, gloss, literal),
            )
            n += 1

        con.commit()
        return {"run_id": run_id, "imported": n, "db": str(db_path), "run_dir": str(run_dir)}
    finally:
        con.close()


def import_answer_run(db_path: Path, run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    meta_path = run_dir / "meta.json"
    answer_path = run_dir / "answer.json"
    retrieval_path = run_dir / "retrieval.json"
    validation_path = run_dir / "validation.json"

    if not answer_path.exists():
        raise SystemExit(f"missing answer.json: {answer_path}")

    meta: dict[str, Any] = {}
    if meta_path.exists():
        m = _read_json(meta_path)
        if isinstance(m, dict):
            meta = m

    run_id = meta.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        run_id = run_dir.name

    ans_obj = _read_json(answer_path)
    if not isinstance(ans_obj, dict):
        raise SystemExit("answer.json is not an object")

    retrieval_obj: Any = _read_json(retrieval_path) if retrieval_path.exists() else {}
    validation_obj: Any = _read_json(validation_path) if validation_path.exists() else {}

    query = ""
    corpus = ""
    if isinstance(retrieval_obj, dict):
        query = retrieval_obj.get("query") if isinstance(retrieval_obj.get("query"), str) else ""
        corpus = retrieval_obj.get("corpus") if isinstance(retrieval_obj.get("corpus"), str) else ""

    answer_text = ans_obj.get("answer") if isinstance(ans_obj.get("answer"), str) else ""
    citations = ans_obj.get("citations", [])
    notes = ans_obj.get("notes", [])

    con = sqlite3.connect(str(db_path))
    try:
        ensure_ai_tables(con)

        # Replace-safe
        con.execute("delete from answers where run_id=?", (run_id,))
        _fts_delete_run(con, "answers_fts", run_id)

        con.execute(
            """
            insert into ai_runs(run_id,kind,generated_utc,corpus_filter,query,out_dir,llm_base_url,llm_model,embed_model,use_e5_prefix,params_json)
            values(?,?,?,?,?,?,?,?,?,?,?)
            on conflict(run_id) do update set
              kind=excluded.kind,
              generated_utc=excluded.generated_utc,
              corpus_filter=excluded.corpus_filter,
              query=excluded.query,
              out_dir=excluded.out_dir,
              llm_base_url=excluded.llm_base_url,
              llm_model=excluded.llm_model,
              embed_model=excluded.embed_model,
              use_e5_prefix=excluded.use_e5_prefix,
              params_json=excluded.params_json
            """,
            (
                run_id,
                "answer",
                meta.get("generated_utc") if isinstance(meta.get("generated_utc"), str) else "",
                meta.get("corpus_filter") if isinstance(meta.get("corpus_filter"), str) else corpus,
                query,
                str(run_dir),
                meta.get("llm_base_url") if isinstance(meta.get("llm_base_url"), str) else "",
                meta.get("llm_model") if isinstance(meta.get("llm_model"), str) else "",
                meta.get("embed_model") if isinstance(meta.get("embed_model"), str) else "",
                1 if bool(meta.get("use_e5_prefix", False)) else 0,
                json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
            ),
        )

        con.execute(
            """
            insert or replace into answers(run_id,query,corpus_filter,answer,citations_json,notes_json,retrieval_json,validation_json,meta_json)
            values(?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                query,
                corpus,
                answer_text,
                json.dumps(citations, ensure_ascii=False, separators=(",", ":")),
                json.dumps(notes, ensure_ascii=False, separators=(",", ":")),
                json.dumps(retrieval_obj, ensure_ascii=False, separators=(",", ":")),
                json.dumps(validation_obj, ensure_ascii=False, separators=(",", ":")),
                json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
            ),
        )

        con.execute(
            "insert into answers_fts(run_id,query,answer) values(?,?,?)",
            (run_id, query, answer_text),
        )

        con.commit()
        return {"run_id": run_id, "imported": 1, "db": str(db_path), "run_dir": str(run_dir)}
    finally:
        con.close()


def gloss_search(db_path: Path, q: str, k: int = 10, corpus: str = "") -> list[dict[str, Any]]:
    con = sqlite3.connect(str(db_path))
    try:
        ensure_ai_tables(con)

        # NOTE: FTS5 requires the real table name on the left side of MATCH (aliases are unreliable).
        where = "gloss_fts match ?"
        params: list[Any] = [q]
        if corpus:
            where += " and f.corpus_id=?"
            params.append(corpus)

        rows = con.execute(
            f"""
            select
              g.corpus_id,
              g.segment_id,
              g.work_id,
              g.loc,
              g.gloss,
              g.literal
            from gloss_fts f
            join gloss g on g.segment_id=f.segment_id and g.run_id=f.run_id
            where {where}
            order by bm25(gloss_fts)
            limit ?
            """,
            (*params, int(k)),
        ).fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "corpus_id": r[0],
                    "id": r[1],
                    "work_id": r[2],
                    "loc": r[3],
                    "gloss": r[4],
                    "literal": r[5],
                }
            )
        return out
    finally:
        con.close()
def answer_search(db_path: Path, q: str, k: int = 10, corpus: str = "", show_cites: bool = False) -> list[dict[str, Any]]:
    """Search imported answers via FTS.

    v1 scope: return run-level hits (no segment_id extraction). Corpus filtering is applied
    by joining to the real `answers` table and filtering on `answers.corpus_filter`.

    v2: if show_cites=True, parse stored JSON fields to surface cited corpus_id:segment_id pairs.
    """
    con = sqlite3.connect(str(db_path))
    try:
        ensure_ai_tables(con)

        # NOTE: FTS5 requires the real table name on the left side of MATCH (aliases are unreliable).
        where = "answers_fts match ?"
        params: list[Any] = [q]
        if corpus:
            where += " and a.corpus_filter=?"
            params.append(corpus)

        if show_cites:
            rows = con.execute(
                f"""
                select
                  a.run_id,
                  a.query,
                  a.corpus_filter,
                  a.answer,
                  a.citations_json,
                  a.retrieval_json,
                  bm25(answers_fts) as score
                from answers_fts
                join answers a on a.run_id=answers_fts.run_id
                where {where}
                order by score
                limit ?
                """,
                (*params, int(k)),
            ).fetchall()
        else:
            rows = con.execute(
                f"""
                select
                  a.run_id,
                  a.query,
                  a.corpus_filter,
                  a.answer,
                  bm25(answers_fts) as score
                from answers_fts
                join answers a on a.run_id=answers_fts.run_id
                where {where}
                order by score
                limit ?
                """,
                (*params, int(k)),
            ).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            if show_cites:
                run_id, query, corpus_filter, answer, citations_json, retrieval_json, score = r
            else:
                run_id, query, corpus_filter, answer, score = r
                citations_json, retrieval_json = "", ""

            query_s = query if isinstance(query, str) else ""
            answer_s = answer if isinstance(answer, str) else ""
            cites = _extract_cites(citations_json, retrieval_json) if show_cites else []
            out.append(
                {
                    "run_id": run_id,
                    "corpus_filter": corpus_filter or "",
                    "query": query_s,
                    "query_snip": query_s[:120],
                    "answer_snip": answer_s[:300],
                    "score": float(score) if score is not None else None,
                    "cites": cites,
                }
            )
        return out
    finally:
        con.close()

def answer_show(db_path: Path, run_id: str, max_cites: int = 8, chars: int = 400) -> dict[str, Any]:
    """Load a stored answer run and (best-effort) resolve cited passages from segments.

    Audit convenience: run_id -> answer + cited corpus_id:segment_id -> excerpted segments.
    """
    con = sqlite3.connect(str(db_path))
    try:
        ensure_ai_tables(con)

        row = con.execute(
            "select run_id, query, corpus_filter, answer, citations_json, retrieval_json from answers where run_id=?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"run_id not found: {run_id}")

        _, query, corpus_filter, answer, citations_json, retrieval_json = row
        query_s = query if isinstance(query, str) else ""
        answer_s = answer if isinstance(answer, str) else ""
        cites = _extract_cites(citations_json or "", retrieval_json or "", max_items=int(max_cites))

        seg_cols = [r[1] for r in con.execute("pragma table_info(segments)").fetchall()]
        text_col = next((c for c in ("text", "seg_text", "content", "segment_text") if c in seg_cols), "")
        work_col = next((c for c in ("work_id", "work", "workid") if c in seg_cols), "")
        loc_col = next((c for c in ("loc", "location") if c in seg_cols), "")

        select_cols = ["corpus_id", "id"]
        if work_col:
            select_cols.append(work_col)
        if loc_col:
            select_cols.append(loc_col)
        if text_col:
            select_cols.append(text_col)

        sel_sql = "select " + ", ".join(select_cols) + " from segments where corpus_id=? and id=?"
        sel_sql_any = "select " + ", ".join(select_cols) + " from segments where id=?"

        resolved: list[dict[str, Any]] = []
        missing: list[str] = []

        def _candidates(corpus: str, seg_id: str, full_key: str) -> list[str]:
            cands: list[str] = []
            seg_id = seg_id.strip()
            corpus = corpus.strip()
            full_key = full_key.strip()

            # try raw seg id
            if seg_id:
                cands.append(seg_id)

                # try stripping leading zeros (common mismatch)
                nz = seg_id.lstrip("0")
                if nz and nz != seg_id:
                    cands.append(nz)

            # try full key as an id
            if full_key:
                cands.append(full_key)

            # try explicit corpus-prefixed id (if segments.id stores corpus:seg)
            if corpus and seg_id:
                cands.append(f"{corpus}:{seg_id}")
            if corpus and seg_id:
                nz = seg_id.lstrip("0")
                if nz and nz != seg_id:
                    cands.append(f"{corpus}:{nz}")

            # de-dup preserving order
            seen: set[str] = set()
            out: list[str] = []
            for x in cands:
                x = x.strip()
                if not x or x in seen:
                    continue
                seen.add(x)
                out.append(x)
            return out

        for key in cites:
            if ":" not in key:
                missing.append(key)
                continue
            corpus, seg_id = key.split(":", 1)

            seg = None
            for cand in _candidates(corpus, seg_id, key):
                seg = con.execute(sel_sql, (corpus, cand)).fetchone()
                if seg is not None:
                    break

            # last resort: id-only lookup (useful when corpus_id differs but id is globally unique)
            if seg is None:
                for cand in _candidates(corpus, seg_id, key):
                    seg = con.execute(sel_sql_any, (cand,)).fetchone()
                    if seg is not None:
                        break

            if seg is None:
                missing.append(key)
                continue

            # Normalize segment_id so printing corpus:segment_id doesn't double-prefix
            sid = seg[1]
            if isinstance(sid, str):
                pref = str(seg[0]) + ":"
                if sid.startswith(pref):
                    sid = sid.split(":", 1)[1]

            d: dict[str, Any] = {"corpus_id": seg[0], "segment_id": sid}
            idx = 2
            if work_col:
                d["work_id"] = seg[idx]
                idx += 1
            if loc_col:
                d["loc"] = seg[idx]
                idx += 1
            if text_col:
                t = seg[idx] if isinstance(seg[idx], str) else ""
                d["text_snip"] = t[: int(chars)]
            resolved.append(d)

        return {
            "run_id": run_id,
            "corpus_filter": corpus_filter or "",
            "query": query_s,
            "answer": answer_s,
            "cites": cites,
            "resolved": resolved,
            "missing": missing,
        }
    finally:
        con.close()
