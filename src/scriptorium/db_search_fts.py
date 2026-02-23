from __future__ import annotations

import re
import sqlite3
from pathlib import Path


def sanitize_fts_query(q: str) -> str:
    """Best-effort sanitize user input into a safe FTS5 query.

    Strategy:
    - strip, drop characters that commonly trigger FTS5 syntax errors
    - split into tokens and join with AND
    This is intentionally conservative; it favors "no error" over advanced FTS syntax.
    """
    q = (q or "").strip()
    if not q:
        return ""

    # Replace characters that frequently break FTS5 query parsing.
    q = q.replace("<", " ").replace(">", " ")
    q = q.replace("\u0000", " ")

    # Normalize whitespace.
    q = re.sub(r"\s+", " ", q).strip()
    if not q:
        return ""

    # Tokenize and keep only "word-ish" tokens (letters/digits/_/'/-).
    # This avoids FTS operators/syntax punctuation from user copy-paste.
    toks: list[str] = []
    for t in q.split(" "):
        t = t.strip().strip('"').strip("'")
        if not t:
            continue
        t2 = re.sub(r"[^0-9A-Za-z_\-\']+", "", t)
        if not t2:
            continue
        toks.append(t2)

    if not toks:
        return ""

    if len(toks) == 1:
        return toks[0]

    return " AND ".join(toks)


def run_db_search(db_path: Path, q: str, k: int = 10, corpus: str = "") -> int:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("select 1 from segments_fts limit 1").fetchone()

        where = "segments_fts match ?"
        params = [q]
        if corpus:
            where += " and corpus_id = ?"
            params.append(corpus)

        sql = f"""select
  corpus_id,
  id,
  coalesce(work_id,'') as work_id,
  coalesce(loc,'') as loc,
  snippet(segments_fts, 4, '[', ']', '…', 12) as snip
from segments_fts
where {where}
order by bm25(segments_fts)
limit ?
"""

        params.append(int(k))

        try:
            rows = con.execute(sql, params).fetchall()
        except sqlite3.OperationalError as e:
            msg = str(e)
            if "fts5: syntax error" not in msg:
                raise

            # Fallback: sanitize the query and retry once.
            q2 = sanitize_fts_query(q)
            if not q2:
                print(f"[OK] 0 hits (q={q!r}, corpus={corpus!r})")
                return 0

            params2 = [q2]
            if corpus:
                params2.append(corpus)
            params2.append(int(k))

            try:
                rows = con.execute(sql, params2).fetchall()
            except sqlite3.OperationalError:
                print(f"[OK] 0 hits (q={q!r}, corpus={corpus!r})")
                return 0

        if not rows:
            print(f"[OK] 0 hits (q={q!r}, corpus={corpus!r})")
            return 0

        print(f"[OK] hits={len(rows)} (q={q!r}, corpus={corpus!r})")
        for corpus_id, rid, work_id, loc, snip in rows:
            print(f"{corpus_id}\t{rid}\t{work_id}\t{loc}")
            print(f"  {snip}")
        return 0
    finally:
        con.close()
