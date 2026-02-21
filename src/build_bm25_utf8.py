#!/usr/bin/env python3
"""Build a BM25 index (pickle) from canon JSONL.

Output format matches retrieve_bede_hybrid_faiss.py expectations:
  pickle -> {"bm25": <BM25Okapi>, "meta": [ {"id": ...}, ... ], ...}

Tokenization is UTF-8 friendly and includes common Old English characters.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


WORD_RE = re.compile(r"[A-Za-z\u00C0-\u017FþðæƿȝĀĒĪŌŪȲāēīōūȳ]+", re.UNICODE)


def tok(s: str) -> List[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(s)]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build BM25 (UTF-8) index from canon JSONL")
    ap.add_argument("--in", dest="in_path", required=True, help="Input canon JSONL")
    ap.add_argument("--bm25_dir", default="indexes/bm25", help="Output directory for BM25 pickle")
    ap.add_argument("--corpus-id", default="oe_bede_prod", help="Corpus ID prefix for output filename")
    ap.add_argument("--out", default="", help="Override output pickle path")
    ap.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = ap.parse_args()

    try:
        from rank_bm25 import BM25Okapi  # type: ignore
    except Exception as e:
        msg = (
            "Missing dependency: rank_bm25. Your existing oe_bede_prod_utf8.pkl implies it is installed, "
            "but this environment cannot import it. Install with: pip install rank-bm25"
        )
        if args.json:
            print(json.dumps({"ok": False, "error": msg, "exception": str(e)}, ensure_ascii=False, separators=(",", ":")))
            return 2
        raise SystemExit(msg)

    in_path = Path(args.in_path)
    bm25_dir = Path(args.bm25_dir)

    out_path = Path(args.out) if args.out else (bm25_dir / f"{args.corpus_id}_utf8.pkl")

    tokenized_corpus: List[List[str]] = []
    meta: List[Dict[str, Any]] = []

    with in_path.open("r", encoding="utf-8") as f:
        for ln_no, ln in enumerate(f, start=1):
            ln = ln.strip("\n")
            if not ln.strip():
                continue
            try:
                obj = json.loads(ln)
            except json.JSONDecodeError as je:
                raise SystemExit(f"Invalid JSON on line {ln_no}: {je}")

            doc_id = obj.get("id")
            txt = obj.get("txt")
            if not isinstance(doc_id, str) or not isinstance(txt, str):
                raise SystemExit(f"Bad record on line {ln_no}: requires string 'id' and 'txt'")

            tokenized_corpus.append(tok(txt))
            meta.append({"id": doc_id})

    bm25 = BM25Okapi(tokenized_corpus)

    created_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    payload: Dict[str, Any] = {
        "bm25": bm25,
        "meta": meta,
        "corpus_id": args.corpus_id,
        "record_count": len(meta),
        "created_utc": created_utc,
        "token_regex": WORD_RE.pattern,
        "source_jsonl": str(in_path.as_posix()),
    }

    data = pickle.dumps(payload, protocol=4)
    atomic_write_bytes(out_path, data)

    out_sha = sha256_file(out_path)
    atomic_write_bytes(out_path.with_suffix(out_path.suffix + ".sha256"), (out_sha + "\n").encode("utf-8"))

    result = {
        "ok": True,
        "in": str(in_path.resolve()),
        "out": str(out_path.resolve()),
        "records": len(meta),
        "sha256": out_sha,
        "created_utc": created_utc,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(f"Wrote BM25: {out_path} (records={len(meta)})")
        print(f"SHA256: {out_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
