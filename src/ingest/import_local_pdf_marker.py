import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.config.parser import ConfigParser

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    if BeautifulSoup is None:
        # crude fallback
        txt = re.sub(r"<[^>]+>", " ", html)
        return re.sub(r"\s{2,}", " ", txt).strip()
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text(" ", strip=True)
    return re.sub(r"\s{2,}", " ", txt).strip()


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


def parse_pages_arg(pages: Optional[str]) -> Optional[Tuple[int, int]]:
    if not pages:
        return None
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", pages)
    if not m:
        raise SystemExit("Invalid --pages. Use like 1-60 (1-indexed).")
    a, b = int(m.group(1)), int(m.group(2))
    if a <= 0 or b <= 0 or b < a:
        raise SystemExit("Invalid --pages range.")
    return a, b


def marker_page_range_0_index(p: Optional[Tuple[int, int]]) -> Optional[str]:
    if not p:
        return None
    a, b = p
    # marker expects 0-index page ranges like "0-2"
    return f"{a-1}-{b-1}"


def walk_blocks(block: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    yield block
    kids = block.get("children")
    if isinstance(kids, list):
        for k in kids:
            if isinstance(k, dict):
                yield from walk_blocks(k)


def extract_block_text(b: Dict[str, Any]) -> str:
    t = b.get("text")
    if isinstance(t, str) and t.strip():
        return re.sub(r"\s{2,}", " ", t).strip()
    h = b.get("html")
    if isinstance(h, str) and h.strip():
        return html_to_text(h)
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--out-jsonl", required=True)
    ap.add_argument("--lang", default="eng")
    ap.add_argument("--work-id", default=None)
    ap.add_argument("--max-chars", type=int, default=1400)
    ap.add_argument("--pages", default=None, help="Optional page range like 1-60 (1-indexed)")
    ap.add_argument("--force-ocr", action="store_true")
    ap.add_argument("--debug-dump", action="store_true", help="Write marker JSON dump alongside output for debugging")
    args = ap.parse_args()

    pdf_path = Path(args.pdf).resolve()
    out_path = Path(args.out_jsonl).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pages = parse_pages_arg(args.pages)
    page_range = marker_page_range_0_index(pages)

    config: Dict[str, Any] = {"output_format": "json"}
    if page_range:
        config["page_range"] = page_range
    if args.force_ocr:
        config["force_ocr"] = True

    config_parser = ConfigParser(config)
    converter = PdfConverter(
        config=config_parser.generate_config_dict(),
        artifact_dict=create_model_dict(),
        processor_list=config_parser.get_processors(),
        renderer=config_parser.get_renderer(),
        llm_service=config_parser.get_llm_service(),
    )

    rendered = converter(str(pdf_path))

    # Pydantic exclude=... can break across versions; use JSON round-trip.
    dumped = json.loads(rendered.model_dump_json())
    if isinstance(dumped, dict):
        dumped.pop("metadata", None)

    if args.debug_dump:
        dbg = out_path.with_suffix(out_path.suffix + ".marker.json")
        dbg.write_text(json.dumps(dumped, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # Marker output may be:
    #  - a root block with children (pages)
    #  - or a list of page blocks
    if isinstance(dumped, dict) and isinstance(dumped.get("children"), list):
        page_blocks = [p for p in dumped["children"] if isinstance(p, dict)]
    elif isinstance(dumped, list):
        page_blocks = [p for p in dumped if isinstance(p, dict)]
    else:
        raise RuntimeError(f"Unexpected marker JSON shape: {type(dumped)}")

    written = 0
    pages_seen = 0
    blocks_seen = 0

    with out_path.open("w", encoding="utf-8") as f:
        for page in page_blocks:
            if page.get("block_type") != "Page":
                continue

            pages_seen += 1

            # Page number from id like "/page/10/Page/366"
            page_no = None
            pid = page.get("id")
            if isinstance(pid, str):
                m = re.match(r"^/page/(\d+)/", pid)
                if m:
                    page_no = int(m.group(1)) + 1
            if page_no is None:
                page_no = pages_seen  # fallback

            texts: List[str] = []
            for b in walk_blocks(page):
                if not isinstance(b, dict):
                    continue
                bt = b.get("block_type")
                if bt in ("Page", "PageHeader", "PageFooter"):
                    continue
                blocks_seen += 1
                t = extract_block_text(b)
                if t:
                    texts.append(t)

            page_text = "\n\n".join(texts).strip()
            if not page_text:
                continue

            seg_idx = 1
            for chunk in chunk_by_max_chars(page_text, args.max_chars):
                local_id = f"p{page_no:04d}.s{seg_idx:04d}"  # no colons
                seg_id = f"{args.corpus_id}:{local_id}"     # exactly one colon total
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
                        "parser": {"name": "marker", "output_format": "json"},
                    },
                    "text": chunk,
                    "work_id": args.work_id,
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
                seg_idx += 1

    print(
        json.dumps(
            {
                "ok": True,
                "pdf": str(pdf_path),
                "out_jsonl": str(out_path),
                "pages_seen": pages_seen,
                "blocks_seen": blocks_seen,
                "records_written": written,
                "sha256": sha256_file(out_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
