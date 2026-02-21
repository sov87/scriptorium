import argparse, json, pickle, re
from pathlib import Path
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

WORD_RE = re.compile(r"[A-Za-z\u00C0-\u017FþðæƿȝĀĒĪŌŪȲāēīōūȳ]+", re.UNICODE)

def tok(s: str):
    return [m.group(0).lower() for m in WORD_RE.finditer(s)]

def rrf(rank, k=60):
    return 1.0 / (k + rank)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asc", required=True)
    ap.add_argument("--bm25", required=True)
    ap.add_argument("--vec_dir", required=True)
    ap.add_argument("--model", default="intfloat/multilingual-e5-base")
    ap.add_argument("--use_e5_prefix", action="store_true")
    ap.add_argument("--topk", type=int, default=8)
    ap.add_argument("--bm25_k", type=int, default=50)
    ap.add_argument("--vec_k", type=int, default=50)
    ap.add_argument("--out_dir", default="runs/retrieval_hybrid_faiss_v1")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # BM25
    with open(args.bm25, "rb") as f:
        bm = pickle.load(f)
    bm25 = bm["bm25"]
    bm_meta = bm["meta"]

    # FAISS
    vec_dir = Path(args.vec_dir)
    index = faiss.read_index(str(vec_dir / "oe_bede_prod.index"))

    meta_lines = (vec_dir / "oe_bede_prod_meta.jsonl").read_text(encoding="utf-8").splitlines()
    vec_meta = [json.loads(x) for x in meta_lines if x.strip()]

    model = SentenceTransformer(args.model)

    def prep_q(t: str) -> str:
        return ("query: " + t) if args.use_e5_prefix else t

    asc_recs = [json.loads(x) for x in Path(args.asc).read_text(encoding="utf-8").splitlines() if x.strip()]

    out_jsonl = out_dir / "candidates.jsonl"
    out_md = out_dir / "candidates.md"

    m_by_id = {m["id"]: m for m in vec_meta}

    with out_jsonl.open("w", encoding="utf-8") as fj, out_md.open("w", encoding="utf-8") as fm:
        fm.write("# ASC → Bede candidate links (HYBRID: BM25 + FAISS)\n\n")

        for a in asc_recs:
            asc_id = a["id"]
            asc_txt = a["txt"]

            # BM25
            q_tok = tok(asc_txt)
            scores = bm25.get_scores(q_tok)
            bm_top = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[: args.bm25_k]
            bm_rank = {bm_meta[i]["id"]: r+1 for r, i in enumerate(bm_top)}

            # Vector
            q_emb = model.encode([prep_q(asc_txt)], normalize_embeddings=True, show_progress_bar=False).astype(np.float32)
            D, I = index.search(q_emb, min(args.vec_k, len(vec_meta)))
            vec_ids = [vec_meta[i]["id"] for i in I[0].tolist() if i >= 0]
            vec_rank = {bid: r+1 for r, bid in enumerate(vec_ids)}

            # RRF fuse
            all_ids = set(bm_rank) | set(vec_rank)
            fused = []
            for bid in all_ids:
                s = 0.0
                if bid in bm_rank:
                    s += rrf(bm_rank[bid])
                if bid in vec_rank:
                    s += rrf(vec_rank[bid])
                fused.append((bid, s))
            fused.sort(key=lambda x: x[1], reverse=True)
            fused = fused[: args.topk]

            cands = []
            for bid, fscore in fused:
                m = m_by_id.get(bid)
                if not m:
                    continue
                cands.append({
                    "bede_id": bid,
                    "score": float(fscore),
                    "loc": m["loc"],
                    "src": m["src"],
                    "srcp": m["srcp"],
                    "txt": m["txt"],
                })

            out_obj = {
                "asc_id": asc_id,
                "asc_src": a["src"],
                "asc_srcp": a["srcp"],
                "asc_txt": asc_txt,
                "candidates": cands,
            }
            fj.write(json.dumps(out_obj, ensure_ascii=False, separators=(",", ":")) + "\n")

            fm.write(f"## {asc_id}\n\n{asc_txt.strip()}\n\n### Top Bede candidates\n\n")
            for r, c in enumerate(cands, 1):
                fm.write(f"**{r}. {c['bede_id']}** (rrf {c['score']:.6f})\n\n{c['txt'].strip()}\n\n")
            fm.write("---\n\n")

    print(f"Wrote:\n  {out_jsonl}\n  {out_md}")

if __name__ == "__main__":
    main()