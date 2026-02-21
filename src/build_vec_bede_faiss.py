import argparse, json
from pathlib import Path
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--out_dir", default="indexes/vec_faiss")
    ap.add_argument("--model", default="intfloat/multilingual-e5-base")
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--use_e5_prefix", action="store_true")
    ap.add_argument("--corpus-id", default="oe_bede_prod", help="Corpus ID prefix for index filenames")
    args = ap.parse_args()
    corpus_id = args.corpus_id

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # load records, drop toc
    recs = []
    with Path(args.infile).open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            flags = o.get("flags", [])
            if isinstance(flags, list) and ("toc" in flags):
                continue
            recs.append({
                "id": o["id"],
                "txt": o["txt"],
                "loc": o["loc"],
                "src": o["src"],
                "srcp": o["srcp"],
            })

    model = SentenceTransformer(args.model)
    dim = model.get_sentence_embedding_dimension()

    def prep_passage(t: str) -> str:
        return ("passage: " + t) if args.use_e5_prefix else t

    Xs = []
    for i in range(0, len(recs), args.batch):
        batch = [prep_passage(r["txt"]) for r in recs[i:i+args.batch]]
        emb = model.encode(batch, normalize_embeddings=True, show_progress_bar=False)
        Xs.append(emb.astype(np.float32))
    X = np.vstack(Xs)

    # cosine similarity via inner product on normalized vectors
    index = faiss.IndexFlatIP(dim)
    index.add(X)

    index_path = out_dir / f"{corpus_id}.index"
    ids_path = out_dir / f"{corpus_id}_ids.json"
    meta_path = out_dir / f"{corpus_id}_meta.jsonl"


    faiss.write_index(index, str(index_path))

    ids_path.write_text(
        json.dumps([r["id"] for r in recs], ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8"
    )
    meta_path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in recs) + "\n",
        encoding="utf-8"
    )
    print(f"Wrote FAISS index:\n  {index_path}\n  docs={len(recs)} dim={dim}\n  model={args.model}")

if __name__ == "__main__":
    main()
