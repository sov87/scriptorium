from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

import faiss
from sentence_transformers import SentenceTransformer


FTS_TOKEN_RE = re.compile(r"[0-9A-Za-z\u00C0-\u024F\u1E00-\u1EFFþðæǣÞÐÆǢ]+")


def fts_query(q: str) -> str:
    # FTS5 MATCH is not free text; punctuation like ? can break parsing.
    toks = FTS_TOKEN_RE.findall(q)
    return " ".join(toks) if toks else q


def rrf_fuse(rank_a: dict[str, int], rank_b: dict[str, int], k: int = 60) -> list[tuple[str, float]]:
    scores: dict[str, float] = {}
    for rid, r in rank_a.items():
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (k + r)
    for rid, r in rank_b.items():
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (k + r)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--q", required=True)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--fts-k", type=int, default=50)
    ap.add_argument("--vec-k", type=int, default=50)
    ap.add_argument("--vec-dir", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--use-e5-prefix", action="store_true")
    ap.add_argument("--corpus", default="")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)

    # FTS hits -> rank dict (never crash on syntax; just fall back to empty)
    fts_ids: list[str] = []
    try:
        q2 = fts_query(args.q)
        where = "segments_fts match ?"
        params: list[Any] = [q2]
        if args.corpus:
            where += " and corpus_id=?"
            params.append(args.corpus)

        fts_rows = con.execute(
            f"select id from segments_fts where {where} order by bm25(segments_fts) limit ?",
            (*params, int(args.fts_k)),
        ).fetchall()
        fts_ids = [r[0] for r in fts_rows if r and isinstance(r[0], str)]
    except sqlite3.OperationalError:
        fts_ids = []

    fts_rank = {rid: i + 1 for i, rid in enumerate(fts_ids)}

    # Vector hits
    vec_dir = Path(args.vec_dir)
    idx = faiss.read_index(str(vec_dir / "index.faiss"))
    ids = json.loads((vec_dir / "ids.json").read_text(encoding="utf-8"))

    model = SentenceTransformer(args.model)
    qtxt = ("query: " + args.q) if args.use_e5_prefix else args.q
    qv = model.encode([qtxt], normalize_embeddings=True).astype("float32")
    _, I = idx.search(qv, int(args.vec_k))
    vec_ids = [ids[i] for i in I[0] if i >= 0]
    # Enforce corpus filter for vector hits too
    if args.corpus and vec_ids:
        allowed: set[str] = set()
        # SQLite has a variable limit; chunk to stay safe.
        chunk_n = 900
        for off in range(0, len(vec_ids), chunk_n):
            chunk = vec_ids[off : off + chunk_n]
            qmarks2 = ",".join(["?"] * len(chunk))
            rows2 = con.execute(
                f"select id from segments where corpus_id=? and id in ({qmarks2})",
                (args.corpus, *chunk),
            ).fetchall()
            for r in rows2:
                if r and isinstance(r[0], str):
                    allowed.add(r[0])
        vec_ids = [rid for rid in vec_ids if rid in allowed]

    vec_rank = {rid: i + 1 for i, rid in enumerate(vec_ids)}

    fused = rrf_fuse(fts_rank, vec_rank)
    top = [rid for rid, _ in fused[: int(args.k)]]

    if not top:
        print("[OK] 0 results")
        con.close()
        return 0

    # Fetch records
    qmarks = ",".join(["?"] * len(top))
    rows = con.execute(
        f"select corpus_id,id,coalesce(work_id,''),coalesce(loc,''),substr(text,1,260) from segments where id in ({qmarks})",
        top,
    ).fetchall()
    con.close()

    by_id = {r[1]: r for r in rows}
    for rid in top:
        r = by_id.get(rid)
        if not r:
            continue
        print(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}")
        print(f"  {r[4]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
