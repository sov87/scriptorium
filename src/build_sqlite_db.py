from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


ID_KEYS = ("id", "rid", "rec_id", "record_id", "uid")
TEXT_KEYS = ("text", "oe", "oe_text", "ms_text", "content", "line", "raw", "txt")
LOC_KEYS = ("loc", "ref", "citation", "passage")


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                raise SystemExit(f"bad jsonl at {path} line {i}: {e}") from e
            if isinstance(obj, dict):
                yield obj


def pick_first_str(rec: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for k in keys:
        v = rec.get(k)
        if isinstance(v, str) and v.strip() != "":
            return v
    return None


def pick_id(rec: dict[str, Any]) -> str | None:
    for k in ID_KEYS:
        v = rec.get(k)
        if isinstance(v, str) and v.strip() != "":
            return v
    return None


def load_canon_paths_registry_only(root: Path) -> list[dict[str, Any]]:
    """
    Authoritative: docs/corpora.json only.
    If you want a corpus in the DB, it must be registered.
    """
    reg = root / "docs" / "corpora.json"
    if not reg.exists():
        raise SystemExit("Missing docs/corpora.json (registry is required for db build)")

    j = json.loads(reg.read_text(encoding="utf-8-sig"))  # BOM-safe

    out: list[dict[str, Any]] = []
    for c in (j.get("corpora") or []):
        if not isinstance(c, dict):
            continue
        cid = c.get("corpus_id")
        if not cid:
            continue
        title = c.get("title") or ""
        canon = (c.get("canon_jsonl") or {}).get("path") or c.get("canon_path") or ""
        if not canon:
            continue
        canon_path = (root / canon).resolve() if not Path(canon).is_absolute() else Path(canon).resolve()
        if canon_path.exists():
            out.append({"corpus_id": cid, "title": title, "canon_path": canon_path})

    if not out:
        raise SystemExit("Registry has no valid canon paths (docs/corpora.json)")
    return out


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS meta (
          key   TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS corpora (
          corpus_id TEXT PRIMARY KEY,
          title     TEXT NOT NULL,
          canon_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS segments (
          id         TEXT PRIMARY KEY,
          corpus_id  TEXT NOT NULL,
          work_id    TEXT,
          witness_id TEXT,
          edition_id TEXT,
          loc        TEXT,
          lang       TEXT,
          text       TEXT NOT NULL,
          text_norm  TEXT,
          source_refs_json TEXT,
          notes_json TEXT,
          record_json TEXT NOT NULL,
          FOREIGN KEY (corpus_id) REFERENCES corpora(corpus_id)
        );

        CREATE INDEX IF NOT EXISTS idx_segments_corpus_work ON segments(corpus_id, work_id);
        CREATE INDEX IF NOT EXISTS idx_segments_lang ON segments(lang);
        """
    )

def rebuild_segments_fts(conn: sqlite3.Connection) -> None:
    """
    Build segments_fts (FTS5) from segments. Deterministic: drop + recreate each db-build.
    Tokenizer: unicode61 with remove_diacritics=2 (diacritic-insensitive, Greek/Latin-friendly).
    Column order is fixed with text first to keep snippet()/highlight() column indices stable.
    """
    # Drop old FTS table if present
    conn.execute("DROP TABLE IF EXISTS segments_fts;")

    # Ensure FTS5 is available (will throw if not)
    conn.execute("CREATE VIRTUAL TABLE __fts_probe USING fts5(x);")
    conn.execute("DROP TABLE __fts_probe;")

    # Fixed schema: text indexed; everything else UNINDEXED metadata for filtering/printing
    conn.execute(
        """
        CREATE VIRTUAL TABLE segments_fts USING fts5(
text,
          corpus_id UNINDEXED,
          work_id UNINDEXED,
          loc UNINDEXED,
          id UNINDEXED,
          witness_id UNINDEXED,
          edition_id UNINDEXED,
          lang UNINDEXED,
          text_norm UNINDEXED,
          source_refs_json UNINDEXED,
          notes_json UNINDEXED,
          record_json UNINDEXED,
          tokenize='unicode61 remove_diacritics 2'
);"""
    )

    conn.execute(
        """
        INSERT INTO segments_fts(
          text, corpus_id, work_id, loc, id, witness_id, edition_id, lang,
          text_norm, source_refs_json, notes_json, record_json
        )
        SELECT
          text, corpus_id, work_id, loc, id, witness_id, edition_id, lang,
          text_norm, source_refs_json, notes_json, record_json
        FROM segments;
        """
    )

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--out", default="db/scriptorium.sqlite")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_path = (Path(args.out) if Path(args.out).is_absolute() else (root / args.out)).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.overwrite and out_path.exists():
        out_path.unlink()

    corpora = load_canon_paths_registry_only(root)

    conn = sqlite3.connect(str(out_path))
    try:
        init_db(conn)
        conn.execute("DELETE FROM segments;")
        conn.execute("DELETE FROM corpora;")

        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("schema", "scriptorium.sqlite.v2"))
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("project_root", str(root)))

        conn.executemany(
            "INSERT INTO corpora(corpus_id,title,canon_path) VALUES(?,?,?)",
            [(c["corpus_id"], c.get("title") or "", str(c["canon_path"])) for c in corpora],
        )

        total = 0
        stats: dict[str, dict[str, int]] = {c["corpus_id"]: {"loaded": 0, "skipped_no_id": 0} for c in corpora}
        buf = []

        for c in corpora:
            cid = c["corpus_id"]
            canon_path: Path = c["canon_path"]

            for rec in iter_jsonl(canon_path):
                rid = pick_id(rec)
                if not rid:
                    stats[cid]["skipped_no_id"] += 1
                    continue

                text = pick_first_str(rec, TEXT_KEYS) or ""
                loc = pick_first_str(rec, LOC_KEYS)

                row = (
                    rid,
                    cid,  # enforce registry corpus_id
                    rec.get("work_id") or rec.get("work"),
                    rec.get("witness_id"),
                    rec.get("edition_id"),
                    loc,
                    rec.get("lang"),
                    text,
                    rec.get("text_norm"),
                    json.dumps(rec.get("source_refs", []), ensure_ascii=False, separators=(",", ":")),
                    json.dumps(rec.get("notes", []), ensure_ascii=False, separators=(",", ":")),
                    json.dumps(rec, ensure_ascii=False, separators=(",", ":")),
                )
                buf.append(row)
                stats[cid]["loaded"] += 1
                total += 1

                if len(buf) >= 5000:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO segments(
                          id, corpus_id, work_id, witness_id, edition_id, loc, lang,
                          text, text_norm, source_refs_json, notes_json, record_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        buf,
                    )
                    buf.clear()

        if buf:
            conn.executemany(
                """
                INSERT OR REPLACE INTO segments(
                  id, corpus_id, work_id, witness_id, edition_id, loc, lang,
                  text, text_norm, source_refs_json, notes_json, record_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                buf,
            )

        rebuild_segments_fts(conn)
        conn.commit()
        print(str(out_path))
        print(f"[OK] corpora={len(corpora)} segments={total}")
        for cid in sorted(stats.keys()):
            s = stats[cid]
            print(f"[STAT] {cid}: loaded={s['loaded']} skipped_no_id={s['skipped_no_id']}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
