# File: src/ingest/ingest_tei_cts.py
# Generic TEI/CTS ingest wrapper: TEI XML -> canonical JSONL
# Deterministic emission (minified JSON, sorted keys, UTF-8, \n newlines).
#
# Example:
#   python src/ingest/ingest_tei_cts.py --tei path\to.xml --corpus-id lat_demo --out data_proc\lat_demo_prod.jsonl

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Optional

from scriptorium.ingest.tei_cts import (
    parse_tei,
    parse_work_id,
    iter_segment_drafts,
    sanitize_local_id_from_loc,
)

def _minijson(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _seq_id(i: int) -> str:
    return f"{i:06d}"

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tei", required=True, help="Input TEI XML path")
    ap.add_argument("--corpus-id", required=True, help="Corpus ID (used in segments.id)")
    ap.add_argument("--out", required=True, help="Output canonical JSONL path")
    ap.add_argument("--use-milestones", action="store_true", help="Prefer milestone segmentation when available")
    ap.add_argument("--lang", default=None, help="Optional language tag (e.g., 'lat', 'grc')")
    ap.add_argument("--with-sha256", action="store_true", help="Include source sha256 in meta (deterministic but slower)")
    args = ap.parse_args()

    tei_path = Path(args.tei)
    out_path = Path(args.out)
    corpus_id = args.corpus_id.strip()

    if not corpus_id or ":" in corpus_id:
        raise SystemExit("corpus-id must be non-empty and must not contain ':'")
    if not tei_path.exists():
        raise SystemExit(f"missing TEI file: {tei_path}")

    tree = parse_tei(str(tei_path))
    work_id: Optional[str] = parse_work_id(tree)

    src_meta = {
        "path": tei_path.as_posix(),
    }
    if args.with_sha256:
        src_meta["sha256"] = _sha256_file(tei_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        i = 0
        for sd in iter_segment_drafts(tree, use_milestones=bool(args.use_milestones)):
            i += 1
            if sd.loc:
                local_id = sanitize_local_id_from_loc(sd.loc) or _seq_id(i)
            else:
                local_id = _seq_id(i)

            # hard rule: local_id must not contain ':'
            local_id = local_id.replace(":", "_")

            meta = sd.meta or {}
            meta.setdefault("source", src_meta)
            if args.lang:
                meta.setdefault("lang", args.lang)

            rec = {
                "corpus_id": corpus_id,
                "local_id": local_id,
                "id": f"{corpus_id}:{local_id}",
                "work_id": work_id,
                "loc": sd.loc,
                "text": sd.text,
                "meta": meta,
            }
            f.write(_minijson(rec) + "\n")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
