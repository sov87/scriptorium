from __future__ import annotations

import argparse
import json
import subprocess
import sys
import hashlib
from datetime import datetime
from pathlib import Path

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--out_dir", default="")  # if empty, auto under runs\query_hybrid\q_YYYYMMDD_HHMMSS
    ap.add_argument("--bm25", default="")
    ap.add_argument("--vec_dir", default="")
    ap.add_argument("--model", default="intfloat/multilingual-e5-base")
    ap.add_argument("--use_e5_prefix", action="store_true", default=True)

    # mirror your release defaults
    ap.add_argument("--topk", type=int, default=8)
    ap.add_argument("--bm25_k", type=int, default=24)
    ap.add_argument("--vec_k", type=int, default=24)

    args = ap.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    src_dir = project_root / "src"

    bm25 = Path(args.bm25) if args.bm25 else (project_root / "indexes" / "bm25" / "oe_bede_prod_utf8.pkl")
    vec_dir = Path(args.vec_dir) if args.vec_dir else (project_root / "indexes" / "vec_faiss")

    retrieve_script = src_dir / "retrieve_bede_hybrid_faiss.py"
    if not retrieve_script.exists():
        raise FileNotFoundError(f"Missing: {retrieve_script}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) if args.out_dir else (project_root / "runs" / "query_hybrid" / f"q_{stamp}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # One-record ASC-shaped JSONL. Keep fields minimal and harmless.
    qfile = out_dir / "query_asc.jsonl"
    rec = {
    "schema": "scriptorium.record.v1",
    "id": "ASC_A:QUERY:0001",
    "work": "ASC",
    "witness": "A",
    "lang": "eng",
    "loc": "free_text_query",
    "src": "free_text",
    "srcp": {"query": True},
    "txt": args.query,
}
    qfile.write_text(json.dumps(rec, ensure_ascii=False) + "\n", encoding="utf-8")

    cmd = [
        sys.executable,
        str(retrieve_script),
        "--asc", str(qfile),
        "--bm25", str(bm25),
        "--vec_dir", str(vec_dir),
        "--model", args.model,
        "--topk", str(args.topk),
        "--bm25_k", str(args.bm25_k),
        "--vec_k", str(args.vec_k),
        "--out_dir", str(out_dir),
    ]
    if args.use_e5_prefix:
        cmd.append("--use_e5_prefix")

        subprocess.run(cmd, check=True)

    # Record what we actually used (defensible, local-first)
    meta = {
        "query": args.query,
        "bm25_path": str(bm25),
        "bm25_sha256": sha256_file(bm25) if bm25.exists() else None,
        "vec_dir": str(vec_dir),
        "embed_model": args.model,
        "use_e5_prefix": bool(args.use_e5_prefix),
        "topk": args.topk,
        "bm25_k": args.bm25_k,
        "vec_k": args.vec_k,
    }

    # Hash newest FAISS indexes (best effort)
    indexes = sorted(vec_dir.glob("*.index"), key=lambda p: p.stat().st_mtime, reverse=True)
    meta["faiss_indexes"] = [{"path": str(p), "sha256": sha256_file(p)} for p in indexes[:3]]

    (out_dir / "query_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Convenience: echo where results are
    cand = out_dir / "candidates.jsonl"
    if cand.exists():
        print(str(cand))
        return 0

    raise FileNotFoundError(f"Expected candidates.jsonl not found in: {out_dir}")


if __name__ == "__main__":
    raise SystemExit(main())