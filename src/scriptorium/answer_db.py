from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slug(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip())
    s = re.sub(r"[^A-Za-z0-9 _-]+", "", s)
    s = s.replace(" ", "_")
    return s[:48] if s else "q"


def rrf_fuse(a: list[str], b: list[str], k: int = 60) -> list[str]:
    rank_a = {rid: i + 1 for i, rid in enumerate(a)}
    rank_b = {rid: i + 1 for i, rid in enumerate(b)}
    scores: dict[str, float] = {}
    for rid, r in rank_a.items():
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (k + r)
    for rid, r in rank_b.items():
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (k + r)
    return [rid for rid, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)]


def _http_json(url: str, payload: dict[str, Any], api_key: str, timeout_s: int = 120) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _http_get_json(url: str, api_key: str, timeout_s: int = 30) -> dict[str, Any]:
    req = urllib.request.Request(
        url=url,
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def pick_model_id(base_url: str, api_key: str) -> str | None:
    try:
        j = _http_get_json(base_url.rstrip("/") + "/models", api_key=api_key, timeout_s=15)
        data = j.get("data") or []
        if data and isinstance(data, list):
            mid = data[0].get("id")
            if isinstance(mid, str) and mid:
                return mid
    except Exception:
        return None
    return None


FTS_TOKEN_RE = re.compile(r"[0-9A-Za-z\u00C0-\u024F\u1E00-\u1EFFþðæǣÞÐÆǢ]+")

def fts_query(q: str) -> str:
    # FTS5 MATCH is not "free text": punctuation like ? can break parsing.
    toks = FTS_TOKEN_RE.findall(q)
    if not toks:
        return q
    return " ".join(toks)
def fts_ids(con: sqlite3.Connection, q: str, k: int, corpus: str) -> list[str]:
    q2 = fts_query(q)
    where = "segments_fts match ?"
    params: list[Any] = [q2]
    if corpus:
        where += " and corpus_id = ?"
        params.append(corpus)
    rows = con.execute(
        f"select id from segments_fts where {where} order by bm25(segments_fts) limit ?",
        (*params, int(k)),
    ).fetchall()
    return [r[0] for r in rows if r and isinstance(r[0], str)]


def vec_ids(vec_dir: Path, model: SentenceTransformer, q: str, k: int, use_e5_prefix: bool) -> list[str]:
    idx = faiss.read_index(str(vec_dir / "index.faiss"))
    ids = json.loads((vec_dir / "ids.json").read_text(encoding="utf-8"))
    qtxt = ("query: " + q) if use_e5_prefix else q
    qv = model.encode([qtxt], normalize_embeddings=True).astype("float32")
    _, I = idx.search(qv, int(k))
    out = []
    for i in I[0]:
        if i < 0:
            continue
        rid = ids[i]
        if isinstance(rid, str):
            out.append(rid)
    return out




def filter_ids_by_corpus(con: sqlite3.Connection, ids: list[str], corpus: str) -> list[str]:
    """Restrict an ID list to a single corpus, preserving order.

    Chunked to avoid SQLite variable limits.
    """
    if not corpus or not ids:
        return ids
    allowed: set[str] = set()
    chunk_n = 900
    for off in range(0, len(ids), chunk_n):
        chunk = ids[off : off + chunk_n]
        qmarks = ",".join(["?"] * len(chunk))
        rows = con.execute(
            f"select id from segments where corpus_id=? and id in ({qmarks})",
            (corpus, *chunk),
        ).fetchall()
        for r in rows:
            if r and isinstance(r[0], str):
                allowed.add(r[0])
    return [rid for rid in ids if rid in allowed]
def fetch_segments(con: sqlite3.Connection, ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    qmarks = ",".join(["?"] * len(ids))
    rows = con.execute(
        f"select corpus_id,id,coalesce(work_id,''),coalesce(loc,''),text from segments where id in ({qmarks})",
        ids,
    ).fetchall()
    by_id = {r[1]: r for r in rows}
    out = []
    for rid in ids:
        r = by_id.get(rid)
        if not r:
            continue
        out.append(
            {
                "corpus_id": r[0],
                "id": r[1],
                "work_id": r[2],
                "loc": r[3],
                "text": r[4],
            }
        )
    return out


def build_prompt(query: str, passages: list[dict[str, Any]]) -> tuple[str, str]:
    system = (
        "You are producing a scholarly, retrieval-grounded answer.\n"
        "Return ONLY valid JSON. No markdown. No extra keys.\n"
        "You MUST cite ONLY from the provided candidate passages by their exact 'id'.\n"
        "If the passages are insufficient, say so in the answer and cite what you used.\n"
        "Do not invent citations.\n"
        "\n"
        "JSON schema:\n"
        "{"
        "\"answer\": string,"
        "\"citations\": [{\"id\": string, \"quote\": string}],"
        "\"notes\": [string]"
        "}\n"
    )

    # Keep input bounded
    blocks = []
    for p in passages:
        txt = p["text"]
        if len(txt) > 1600:
            txt = txt[:1600] + "…"
        blocks.append(
            f"ID: {p['id']}\nCORPUS: {p['corpus_id']}\nWORK: {p['work_id']}\nLOC: {p['loc']}\nTEXT:\n{txt}\n"
        )

    user = (
        f"QUERY:\n{query}\n\n"
        "CANDIDATE PASSAGES (cite ONLY by ID):\n\n"
        + "\n---\n".join(blocks)
        + "\n\n"
        "Write a careful answer grounded in these passages."
    )
    return system, user


def validate_answer_obj(obj: dict[str, Any], allowed_ids: set[str]) -> list[str]:
    errs: list[str] = []
    if not isinstance(obj, dict):
        return ["root is not a JSON object"]
    if "answer" not in obj or not isinstance(obj.get("answer"), str):
        errs.append("missing/invalid 'answer' (string)")
    cits = obj.get("citations")
    if not isinstance(cits, list):
        errs.append("missing/invalid 'citations' (list)")
        cits = []
    else:
        for i, c in enumerate(cits):
            if not isinstance(c, dict):
                errs.append(f"citations[{i}] not an object")
                continue
            cid = c.get("id")
            if not isinstance(cid, str) or not cid:
                errs.append(f"citations[{i}].id missing/invalid")
            elif cid not in allowed_ids:
                errs.append(f"citations[{i}].id not in candidates: {cid}")
            q = c.get("quote")
            if not isinstance(q, str) or not q:
                errs.append(f"citations[{i}].quote missing/invalid")
    notes = obj.get("notes")
    if notes is None:
        pass
    elif not isinstance(notes, list) or any(not isinstance(x, str) for x in notes):
        errs.append("invalid 'notes' (list of strings)")
    return errs


@dataclass
class AnswerDbArgs:
    db_path: Path
    vec_dir: Path
    embed_model: str
    use_e5_prefix: bool
    query: str
    k: int
    fts_k: int
    vec_k: int
    corpus: str
    out_root: Path
    dry_run: bool
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    max_tokens: int
    temperature: float


def run_answer_db(a: AnswerDbArgs) -> Path:
    run_id = f"{utc_stamp()}_{slug(a.query)}"
    out_dir = a.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Retrieval
    con = sqlite3.connect(str(a.db_path))
    try:
        fts = fts_ids(con, a.query, a.fts_k, a.corpus)
        st = SentenceTransformer(a.embed_model)
        vec = vec_ids(a.vec_dir, st, a.query, a.vec_k, a.use_e5_prefix)
        vec = filter_ids_by_corpus(con, vec, a.corpus)
        fused = rrf_fuse(fts, vec)
        top_ids = fused[: a.k]
        passages = fetch_segments(con, top_ids)
    finally:
        con.close()

    (out_dir / "retrieval.json").write_text(
        json.dumps(
            {
                "generated_utc": utc_iso(),
                "query": a.query,
                "corpus": a.corpus,
                "k": a.k,
                "fts_k": a.fts_k,
                "vec_k": a.vec_k,
                "top_ids": top_ids,
                "passages": passages,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    allowed_ids = {p["id"] for p in passages}

    if a.dry_run:
        (out_dir / "answer.json").write_text(
            json.dumps(
                {"answer": "", "citations": [], "notes": ["dry_run=true"]},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        print(str(out_dir))
        return out_dir

    # LLM
    base = a.llm_base_url.rstrip("/")
    model = a.llm_model.strip()
    if not model:
        picked = pick_model_id(base, a.llm_api_key)
        if picked:
            model = picked
        else:
            raise SystemExit("No llm_model provided and /models lookup failed.")

    system, user = build_prompt(a.query, passages)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": float(a.temperature),
        "max_tokens": int(a.max_tokens),
    }

    (out_dir / "prompt_system.txt").write_text(system, encoding="utf-8")
    (out_dir / "prompt_user.txt").write_text(user, encoding="utf-8")

    t0 = time.time()
    resp = _http_json(base + "/chat/completions", payload, api_key=a.llm_api_key, timeout_s=240)
    dt = time.time() - t0

    (out_dir / "llm_response_raw.json").write_text(json.dumps(resp, ensure_ascii=False, indent=2), encoding="utf-8")

    content = ""
    try:
        content = resp["choices"][0]["message"]["content"]
    except Exception:
        raise SystemExit("Unexpected LLM response shape; see llm_response_raw.json")

    (out_dir / "answer_raw.txt").write_text(content, encoding="utf-8")

    try:
        obj = json.loads(content)
    except Exception as e:
        raise SystemExit(f"LLM did not return valid JSON. See answer_raw.txt. Error: {e}")

    errs = validate_answer_obj(obj, allowed_ids)
    (out_dir / "validation.json").write_text(
        json.dumps(
            {
                "generated_utc": utc_iso(),
                "ok": (len(errs) == 0),
                "errors": errs,
                "allowed_ids": sorted(allowed_ids),
                "model": model,
                "latency_s": dt,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if errs:
        raise SystemExit("answer-db validation failed; see validation.json")

    (out_dir / "answer.json").write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    (out_dir / "meta.json").write_text(
        json.dumps(
            {
                "generated_utc": utc_iso(),
                "run_id": run_id,
                "db": str(a.db_path),
                "vec_dir": str(a.vec_dir),
                "embed_model": a.embed_model,
                "use_e5_prefix": a.use_e5_prefix,
                "llm_base_url": a.llm_base_url,
                "llm_model": model,
                "max_tokens": a.max_tokens,
                "temperature": a.temperature,
                "corpus_filter": a.corpus,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(str(out_dir))
    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--vec-dir", required=True)
    ap.add_argument("--embed-model", required=True)
    ap.add_argument("--use-e5-prefix", action="store_true")
    ap.add_argument("--q", required=True)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--fts-k", type=int, default=50)
    ap.add_argument("--vec-k", type=int, default=50)
    ap.add_argument("--corpus", default="")
    ap.add_argument("--out-root", default="runs/answer_db")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--llm-base-url", default=os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1"))
    ap.add_argument("--llm-model", default=os.getenv("SCRIPTORIUM_LLM_MODEL", ""))
    ap.add_argument("--llm-api-key", default=os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio"))
    ap.add_argument("--max-tokens", type=int, default=900)
    ap.add_argument("--temperature", type=float, default=0.2)
    args = ap.parse_args()

    a = AnswerDbArgs(
        db_path=Path(args.db),
        vec_dir=Path(args.vec_dir),
        embed_model=str(args.embed_model),
        use_e5_prefix=bool(args.use_e5_prefix),
        query=args.q,
        k=int(args.k),
        fts_k=int(args.fts_k),
        vec_k=int(args.vec_k),
        corpus=str(args.corpus),
        out_root=Path(args.out_root),
        dry_run=bool(args.dry_run),
        llm_base_url=str(args.llm_base_url),
        llm_model=str(args.llm_model),
        llm_api_key=str(args.llm_api_key),
        max_tokens=int(args.max_tokens),
        temperature=float(args.temperature),
    )
    run_answer_db(a)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
