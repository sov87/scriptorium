from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


def rrf_fuse(rank_a: dict[str,int], rank_b: dict[str,int], k: int = 60) -> list[tuple[str,float]]:
    scores = {}
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

    db = args.db
    con = sqlite3.connect(db)

    # FTS hits -> rank dict
    where = "segments_fts match ?"
    params = [args.q]
    if args.corpus:
        where += " and corpus_id=?"
        params.append(args.corpus)

    fts_rows = con.execute(
        f"select id from segments_fts where {where} order by bm25(segments_fts) limit ?",
        (*params, args.fts_k),
    ).fetchall()
    fts_ids = [r[0] for r in fts_rows]
    fts_rank = {rid: i+1 for i, rid in enumerate(fts_ids)}

    # Vector hits
    vec_dir = Path(args.vec_dir)
    idx = faiss.read_index(str(vec_dir / "index.faiss"))
    ids = json.loads((vec_dir / "ids.json").read_text(encoding="utf-8"))

    model = SentenceTransformer(args.model)
    qtxt = ("query: " + args.q) if args.use_e5_prefix else args.q
    qv = model.encode([qtxt], normalize_embeddings=True).astype("float32")
    D, I = idx.search(qv, args.vec_k)
    vec_ids = [ids[i] for i in I[0] if i >= 0]
    vec_rank = {rid: i+1 for i, rid in enumerate(vec_ids)}

    fused = rrf_fuse(fts_rank, vec_rank)
    top = [rid for rid, _ in fused[: args.k]]

    # Fetch records
    qmarks = ",".join(["?"] * len(top))
    rows = con.execute(
        f"select corpus_id,id,coalesce(work_id,''),coalesce(loc,''),substr(text,1,260) from segments where id in ({qmarks})",
        top,
    ).fetchall()
    con.close()

    # keep fused order
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
