from __future__ import annotations

import sqlite3
from pathlib import Path


def run_db_search(db_path: Path, q: str, k: int = 10, corpus: str = "") -> int:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("select 1 from segments_fts limit 1").fetchone()

        where = "segments_fts match ?"
        params = [q]
        if corpus:
            where += " and corpus_id = ?"
            params.append(corpus)

        sql = f"""
        select
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

        rows = con.execute(sql, params).fetchall()
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
