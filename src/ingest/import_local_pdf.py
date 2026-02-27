import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

# Prefer PyMuPDF for text-based PDFs.
# pip install pymupdf
try:
    import fitz  # PyMuPDF
except Exception as e:
    fitz = None


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_pdf_text(s: str) -> str:
    # De-hyphenate line breaks: "exam-\nple" -> "example"
    s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)
    # Preserve paragraph breaks: collapse 2+ newlines to a marker, then flatten remaining newlines.
    s = re.sub(r"\n{2,}", "\n\n", s)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = s.replace("\r", "")
    # Convert single newlines inside paragraphs into spaces.
    s = re.sub(r"(?<!\n)\n(?!\n)", " ", s)
    # Clean extra whitespace
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def split_paragraphs(s: str) -> List[str]:
    parts = [p.strip() for p in s.split("\n\n")]
    return [p for p in parts if p]


def chunk_by_max_chars(text: str, max_chars: int) -> List[str]:
    if len(text) <= max_chars:
        return [text]
    out: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_chars, n)
        # Try to break on whitespace near the end.
        if end < n:
            cut = text.rfind(" ", start, end)
            if cut > start + int(max_chars * 0.6):
                end = cut
        chunk = text[start:end].strip()
        if chunk:
            out.append(chunk)
        start = end
    return out


def iter_pages_text(pdf_path: Path, pages: Optional[Tuple[int, int]]) -> Iterable[Tuple[int, str]]:
    if fitz is None:
        raise RuntimeError("PyMuPDF not installed. Run: pip install pymupdf")

    doc = fitz.open(str(pdf_path))
    total = doc.page_count

    start_i = 0
    end_i = total - 1
    if pages:
        # pages are 1-indexed from CLI
        start_i = max(0, pages[0] - 1)
        end_i = min(total - 1, pages[1] - 1)

    for i in range(start_i, end_i + 1):
        page = doc.load_page(i)
        txt = page.get_text("text") or ""
        yield (i + 1, txt)  # 1-indexed page number


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Path to a text-based PDF (no OCR performed here)")
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--out-jsonl", required=True, help="Output canonical JSONL")
    ap.add_argument("--lang", default="eng")
    ap.add_argument("--work-id", default=None)
    ap.add_argument("--max-chars", type=int, default=1400)
    ap.add_argument("--pages", default=None, help="Optional page range like 1-50 (1-indexed)")
    args = ap.parse_args()

    pdf_path = Path(args.pdf).resolve()
    out_path = Path(args.out_jsonl).resolve()

    pages = None
    if args.pages:
        m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", args.pages)
        if not m:
            raise SystemExit("Invalid --pages. Use like 1-50")
        pages = (int(m.group(1)), int(m.group(2)))

    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with out_path.open("w", encoding="utf-8") as f:
        for page_no, raw in iter_pages_text(pdf_path, pages):
            norm = normalize_pdf_text(raw)
            if not norm:
                continue
            paras = split_paragraphs(norm)

            seg_idx = 1
            for para in paras:
                chunks = chunk_by_max_chars(para, args.max_chars)
                for ch in chunks:
                    local_id = f"p{page_no:04d}.s{seg_idx:04d}"
                    seg_id = f"{args.corpus_id}:{local_id}"  # exactly one colon
                    loc = f"pdf:{args.corpus_id}.p{page_no:04d}.s{seg_idx:04d}"

                    rec = {
                        "corpus_id": args.corpus_id,
                        "id": seg_id,
                        "loc": loc,
                        "local_id": local_id,
                        "meta": {
                            "lang": args.lang,
                            "source": {"path": str(pdf_path)},
                            "pdf": {"page": page_no},
                        },
                        "text": ch,
                        "work_id": args.work_id,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    written += 1
                    seg_idx += 1

    print(json.dumps({
        "ok": True,
        "pdf": str(pdf_path),
        "out_jsonl": str(out_path),
        "records_written": written,
        "sha256": sha256_file(out_path),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())