from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--batch", type=int, default=256)
    ap.add_argument("--use-e5-prefix", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(args.db)
    rows = con.execute("select id, text from segments where length(text)>0").fetchall()
    con.close()

    ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]

    model = SentenceTransformer(args.model)

    def prep(s: str) -> str:
        return ("passage: " + s) if args.use_e5_prefix else s

    embs = []
    for i in range(0, len(texts), args.batch):
        batch = [prep(x) for x in texts[i:i+args.batch]]
        v = model.encode(batch, normalize_embeddings=True, show_progress_bar=True)
        embs.append(v)
    X = np.vstack(embs).astype("float32")

    dim = X.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(X)

    faiss.write_index(index, str(out_dir / "index.faiss"))
    (out_dir / "ids.json").write_text(json.dumps(ids, ensure_ascii=False), encoding="utf-8")

    meta = {"count": len(ids), "dim": dim, "model": args.model, "normalize": True}
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(str(out_dir))
    print(f"[OK] vectors={len(ids)} dim={dim}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
