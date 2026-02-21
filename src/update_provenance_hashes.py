#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()

def utc_mtime(path: Path) -> str:
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def upsert_step(processing: List[Dict[str, Any]], step_name: str, step_obj: Dict[str, Any]) -> None:
    for i, s in enumerate(processing):
        if s.get("step") == step_name:
            processing[i] = step_obj
            return
    processing.append(step_obj)

def main() -> int:
    ap = argparse.ArgumentParser(description="Update provenance JSON with sha256 hashes and timestamps from local files.")
    ap.add_argument("--prov", required=True, help="Path to provenance JSON (e.g., docs/provenance/oe_beowulf_9700.json)")
    ap.add_argument("--root", required=True, help="Project root (e.g., F:\\Books\\as_project)")
    ap.add_argument("--corpus-id", required=True, help="Corpus id (e.g., oe_beowulf_9700)")
    ap.add_argument("--json", action="store_true", help="Emit machine JSON result")
    args = ap.parse_args()

    root = Path(args.root)
    prov_path = Path(args.prov)
    corpus_id = args.corpus_id

    prov = json.loads(prov_path.read_text(encoding="utf-8"))
    prov.setdefault("processing", [])

    # Canon
    canon_rel = Path(f"data_proc/{corpus_id}_prod.jsonl")
    canon_path = root / canon_rel
    canon_sha = sha256_file(canon_path)

    # Count records fast (line count)
    rec_count = sum(1 for _ in canon_path.open("r", encoding="utf-8"))

    # FAISS artifacts
    vec_dir = root / "indexes" / "vec_faiss"
    faiss_index = vec_dir / f"{corpus_id}.index"
    faiss_ids   = vec_dir / f"{corpus_id}_ids.json"
    faiss_meta  = vec_dir / f"{corpus_id}_meta.jsonl"

    # BM25 artifact
    bm25_dir = root / "indexes" / "bm25"
    bm25_pkl = bm25_dir / f"{corpus_id}_utf8.pkl"

    # Script hashes
    build_vec = root / "src" / "build_vec_bede_faiss.py"
    build_bm25 = root / "src" / "build_bm25_utf8.py"

    # Update ingest output sha/count if present
    for s in prov["processing"]:
        if s.get("step") == "ingest":
            for o in s.get("outputs", []):
                if o.get("path") == str(canon_rel).replace("\\", "/"):
                    o["sha256"] = canon_sha
                    o["record_count"] = rec_count

    faiss_step = {
        "step": "build_faiss",
        "script": "src/build_vec_bede_faiss.py",
        "run_utc": utc_mtime(faiss_index),
        "inputs": [
            {"path": str(canon_rel).replace("\\", "/"), "sha256": canon_sha},
            {"path": "src/build_vec_bede_faiss.py", "sha256": sha256_file(build_vec)},
        ],
        "outputs": [
            {"path": f"indexes/vec_faiss/{corpus_id}.index", "sha256": sha256_file(faiss_index), "record_count": rec_count},
            {"path": f"indexes/vec_faiss/{corpus_id}_ids.json", "sha256": sha256_file(faiss_ids), "record_count": rec_count},
            {"path": f"indexes/vec_faiss/{corpus_id}_meta.jsonl", "sha256": sha256_file(faiss_meta), "record_count": rec_count},
        ],
        "params": {"corpus_id": corpus_id, "vec_dir": "indexes/vec_faiss"},
        "runtime": {"python": "3.12.x", "platform": "Windows-11"},
    }

    bm25_step = {
        "step": "build_bm25",
        "script": "src/build_bm25_utf8.py",
        "run_utc": utc_mtime(bm25_pkl),
        "inputs": [
            {"path": str(canon_rel).replace("\\", "/"), "sha256": canon_sha},
            {"path": "src/build_bm25_utf8.py", "sha256": sha256_file(build_bm25)},
        ],
        "outputs": [
            {"path": f"indexes/bm25/{corpus_id}_utf8.pkl", "sha256": sha256_file(bm25_pkl), "record_count": rec_count},
        ],
        "params": {"corpus_id": corpus_id, "bm25_dir": "indexes/bm25"},
        "runtime": {"python": "3.12.x", "platform": "Windows-11"},
    }

    upsert_step(prov["processing"], "build_faiss", faiss_step)
    upsert_step(prov["processing"], "build_bm25", bm25_step)

    # Write minified JSON
    prov_path.write_text(json.dumps(prov, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

    result = {
        "ok": True,
        "prov": str(prov_path),
        "canon_sha256": canon_sha,
        "records": rec_count,
        "faiss_index_sha256": sha256_file(faiss_index),
        "bm25_sha256": sha256_file(bm25_pkl),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print("[OK] updated provenance:", prov_path)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
