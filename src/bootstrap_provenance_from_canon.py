#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def utc_mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def count_jsonl(path: Path) -> int:
    return sum(1 for _ in path.open("r", encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Bootstrap provenance JSON for an existing canon JSONL (minified).")
    ap.add_argument("--root", required=True, help="Project root (e.g., F:\\Books\\as_project)")
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--title", required=True)
    ap.add_argument("--canon-rel", required=True, help="Canon JSONL path relative to project root (e.g., data_proc/asc_A_prod.jsonl)")
    ap.add_argument("--rights-status", default="unknown", help="e.g., public_domain, licensed, unknown")
    ap.add_argument("--rights-notes", default="Fill in rights/provenance details.", help="Notes")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    cid = args.corpus_id
    canon_rel = args.canon_rel.replace("\\", "/")
    canon_path = root / Path(canon_rel)

    if not canon_path.exists():
        raise SystemExit(f"missing canon file: {canon_path}")

    out_dir = root / "docs" / "provenance"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{cid}.json"
    if out_path.exists() and not args.force:
        raise SystemExit(f"provenance already exists: {out_path} (use --force to overwrite)")

    canon_sha = sha256_file(canon_path)
    recs = count_jsonl(canon_path)
    run_utc = utc_mtime(canon_path)

    prov = {
        "corpus_id": cid,
        "title": args.title,
        "rights": {"status": args.rights_status, "jurisdiction": "US", "notes": args.rights_notes},
        "sources": [],
        "processing": [
            {
                "step": "ingest",
                "script": "unknown",
                "run_utc": run_utc,
                "inputs": [],
                "outputs": [{"path": canon_rel, "sha256": canon_sha, "record_count": recs}],
                "params": {},
                "runtime": {"python": "3.12.x", "platform": "Windows-11"},
            }
        ],
    }

    out_path.write_text(json.dumps(prov, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())