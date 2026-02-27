import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import List, Optional


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_text_bomsafe(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="utf-8-sig")


def slug(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    return s.strip("_")[:120] or "sec"


def chunk_by_max_chars(text: str, max_chars: int) -> List[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    out: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_chars, n)
        if end < n:
            cut = text.rfind(" ", start, end)
            if cut > start + int(max_chars * 0.6):
                end = cut
        chunk = text[start:end].strip()
        if chunk:
            out.append(chunk)
        start = end
    return out


def normalize(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # collapse excessive whitespace, preserve paragraph breaks
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def split_blocks(text: str) -> List[str]:
    # default: paragraphs separated by blank lines
    parts = [p.strip() for p in text.split("\n\n")]
    return [p for p in parts if p]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--txt", required=True)
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--out-jsonl", required=True)
    ap.add_argument("--lang", default="eng")
    ap.add_argument("--work-id", default=None)
    ap.add_argument("--source-url", default=None)
    ap.add_argument("--max-chars", type=int, default=1400)
    ap.add_argument(
        "--section-regex",
        default=None,
        help="Optional regex; if a line matches, it starts a new section. "
             "If it has a capture group, that group becomes the section label.",
    )
    args = ap.parse_args()

    txt_path = Path(args.txt).resolve()
    out_path = Path(args.out_jsonl).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw = read_text_bomsafe(txt_path)
    text = normalize(raw)

    section_rx = re.compile(args.section_regex) if args.section_regex else None

    records_written = 0
    with out_path.open("w", encoding="utf-8") as f:
        if section_rx:
            cur_label = "start"
            cur_lines: List[str] = []

            def flush(section_label: str, buf: List[str]) -> None:
                nonlocal records_written
                block = normalize("\n".join(buf))
                if not block:
                    return
                for idx, ch in enumerate(chunk_by_max_chars(block, args.max_chars), start=1):
                    sec = slug(section_label)
                    local_id = f"{sec}.s{idx:04d}"          # no colons
                    seg_id = f"{args.corpus_id}:{local_id}"  # exactly one colon total
                    loc = f"txt:{args.corpus_id}.{sec}.s{idx:04d}"

                    rec = {
                        "corpus_id": args.corpus_id,
                        "id": seg_id,
                        "loc": loc,
                        "local_id": local_id,
                        "meta": {
                            "lang": args.lang,
                            "source": {"path": str(txt_path), **({"url": args.source_url} if args.source_url else {})},
                            "txt": {"section": section_label},
                        },
                        "text": ch,
                        "work_id": args.work_id,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    records_written += 1

            for line in text.split("\n"):
                m = section_rx.search(line)
                if m:
                    flush(cur_label, cur_lines)
                    cur_lines = []
                    cur_label = m.group(1) if m.lastindex else m.group(0)
                    continue
                cur_lines.append(line)
            flush(cur_label, cur_lines)

        else:
            blocks = split_blocks(text)
            for i, block in enumerate(blocks, start=1):
                for j, ch in enumerate(chunk_by_max_chars(block, args.max_chars), start=1):
                    local_id = f"s{i:06d}.{j:02d}"
                    seg_id = f"{args.corpus_id}:{local_id}"
                    loc = f"txt:{args.corpus_id}.s{i:06d}.{j:02d}"

                    rec = {
                        "corpus_id": args.corpus_id,
                        "id": seg_id,
                        "loc": loc,
                        "local_id": local_id,
                        "meta": {
                            "lang": args.lang,
                            "source": {"path": str(txt_path), **({"url": args.source_url} if args.source_url else {})},
                        },
                        "text": ch,
                        "work_id": args.work_id,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    records_written += 1

    print(json.dumps({
        "ok": True,
        "txt": str(txt_path),
        "out_jsonl": str(out_path),
        "records_written": records_written,
        "sha256": sha256_file(out_path),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())