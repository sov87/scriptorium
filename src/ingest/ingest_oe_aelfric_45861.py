from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--in", dest="in_path", required=True, help="Input file/dir under data_raw or elsewhere (local-only).")
    ap.add_argument("--out", required=True, help="Output canon JSONL under data_proc.")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    in_path = Path(args.in_path)
    if not in_path.is_absolute():
        in_path = (root / in_path).resolve()
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # TODO: parse source into segments. Emit JSONL records with stable ids.
    # Each record should minimally include:
    # id, corpus_id, work_id, witness_id, edition_id, loc, lang, text, text_norm, source_refs, notes

    raise SystemExit("ingest stub not implemented for oe_aelfric_45861")


if __name__ == "__main__":
    raise SystemExit(main())
