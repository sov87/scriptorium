from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def split_paragraphs(s: str) -> list[str]:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = s.strip()
    if not s:
        return []
    parts = re.split(r"\n{2,}", s)
    return [p.strip() for p in parts if p.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--lang", default="ang")
    ap.add_argument("--strip-gutenberg-header", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    in_path = Path(args.in_path)
    if not in_path.is_absolute():
        in_path = (root / in_path).resolve()
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()

    raw = in_path.read_text(encoding="utf-8", errors="replace")

    if args.strip_gutenberg_header:
        # very conservative: if markers exist, keep only body between them
        start = raw.find("*** START")
        end = raw.find("*** END")
        if start != -1 and end != -1 and end > start:
            raw = raw[start:end]

    paras = split_paragraphs(raw)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sha = sha256_file(in_path)
    rel = ""
    try:
        rel = in_path.relative_to(root).as_posix()
    except Exception:
        rel = in_path.as_posix()

    seen = set()
    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        for i, p in enumerate(paras, start=1):
            rid = f"{args.corpus_id}:{i:06d}"
            if rid in seen:
                raise SystemExit("duplicate id")
            seen.add(rid)
            rec = {
                "id": rid,
                "corpus_id": args.corpus_id,
                "work_id": args.corpus_id,
                "witness_id": "plaintext",
                "edition_id": "ProjectGutenberg",
                "loc": f"{rel}#p{i}",
                "lang": args.lang,
                "text": p,
                "text_norm": None,
                "source_refs": [{"type": "file", "path": rel, "sha256": sha, "offset": {"para": i}}],
                "notes": [],
            }
            out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(str(out_path))
    print(f"[OK] records={len(paras)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
