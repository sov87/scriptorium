#!/usr/bin/env python3
"""
Scrape Platner & Ashby 'A Topographical Dictionary of Ancient Rome' from Perseus,
convert to canonical JSONL for Scriptorium ingest.

Source: https://www.perseus.tufts.edu/hopper/text?doc=Perseus:text:1999.04.0054
Public domain text (1929). Perseus XML endpoint used for clean structured output.

Usage:
    python platner_ashby_scraper.py --out data_raw/private/platner_ashby_1929.jsonl
    python platner_ashby_scraper.py --out data_raw/private/platner_ashby_1929.jsonl --resume
    python platner_ashby_scraper.py --out data_raw/private/platner_ashby_1929.jsonl --dry-run

Options:
    --out PATH        Output JSONL path (required)
    --resume          Skip entries already in the output file and continue
    --dry-run         Fetch first 5 entries only, print to stdout, don't write
    --delay FLOAT     Seconds between requests (default: 1.5)
    --start-entry STR Start from a specific entry key (e.g. 'forum-romanum')
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterator


# ── Constants ────────────────────────────────────────────────────────────────

BASE = "https://www.perseus.tufts.edu/hopper/"
TEXT_ID = "Perseus:text:1999.04.0054"

# First entry URL — we walk via next links from here
FIRST_ENTRY_URL = (
    BASE + "text?doc=Perseus%3Atext%3A1999.04.0054%3Aalphabetic%20letter%3DA"
    "%3Aentry%20group%3D1%3Aentry%3Dabd"
)

CORPUS_ID = "platner_ashby_1929"
HEADERS = {
    "User-Agent": (
        "Scriptorium-Ingest/1.0 (historical research tool; "
        "polite scraper; contact: scriptorium-project)"
    ),
    "Accept": "text/html,application/xhtml+xml",
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch(url: str, retries: int = 3, backoff: float = 5.0) -> str:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = backoff * (attempt + 1)
            print(f"  [WARN] fetch error ({e}), retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ── Entry URL discovery ───────────────────────────────────────────────────────

def find_first_entry_url(letter_url: str) -> str | None:
    """From a letter index page, find the first entry link."""
    html = fetch(letter_url)
    # Entry links look like: href="text?doc=Perseus%3Atext%3A1999.04.0054%3Aentry%3Dfoo"
    m = re.search(
        r'href="(text\?doc=Perseus%3Atext%3A1999\.04\.0054(?:%3A[^"]+)?%3Aentry%3D[^"]+)"',
        html,
    )
    if m:
        return BASE + m.group(1)
    return None


def get_all_letter_urls() -> list[str]:
    """Return letter index URLs for A through Z (plus front matter)."""
    letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    urls = []
    for letter in letters:
        urls.append(
            f"{BASE}text?doc=Perseus%3Atext%3A1999.04.0054%3Aalphabetic%20letter%3D{letter}"
        )
    return urls


# ── HTML parsing ──────────────────────────────────────────────────────────────

def extract_entry_from_html(html: str, url: str) -> dict | None:
    """
    Extract entry data from a Perseus HTML page.
    Returns a dict with: id, headword, text, loc, url, notes
    Returns None if no entry content found.
    """
    # --- headword: in <h4> tag ---
    headword_match = re.search(r"<h4[^>]*>\s*(.*?)\s*</h4>", html, re.DOTALL)
    if not headword_match:
        return None
    headword_raw = headword_match.group(1)
    headword = _clean_html(headword_raw).strip()
    if not headword:
        return None

    # --- entry text: in div.text, after the h4 ---
    text_div_match = re.search(
        r'<div[^>]+class="[^"]*\btext\b[^"]*"[^>]*>(.*?)</div>\s*<div[^>]+class="[^"]*footnotes',
        html,
        re.DOTALL,
    )
    if not text_div_match:
        # fallback: grab everything between </h4> and </div> in the main content area
        text_div_match = re.search(
            r"</h4>(.*?)</div>\s*<div[^>]+class=\"[^\"]*footnote",
            html,
            re.DOTALL,
        )
    if not text_div_match:
        return None

    raw_text = text_div_match.group(1)
    # Remove the h4 if it leaked in
    raw_text = re.sub(r"<h4[^>]*>.*?</h4>", "", raw_text, flags=re.DOTALL)
    entry_text = _clean_html(raw_text).strip()

    if not entry_text:
        return None

    # --- next entry URL ---
    next_match = re.search(
        r'<a class="arrow" href="(text\?doc=Perseus%3Atext%3A1999\.04\.0054%3Aentry%3D[^"]+)">'
        r'\s*<img[^>]+alt="next"',
        html,
    )
    next_url = (BASE + next_match.group(1)) if next_match else None

    # --- entry key from URL ---
    entry_key = _entry_key_from_url(url)

    # --- segment id ---
    seg_id = f"platner_ashby_1929:{entry_key}"

    return {
        "id": seg_id,
        "corpus_id": CORPUS_ID,
        "work_id": "platner_ashby_topographical_dict",
        "loc": headword,
        "text": f"{headword}. {entry_text}",
        "text_norm": entry_text,
        "lang": "en",
        "source_refs": [],
        "notes": [f"source_url:{url}"],
        "headword": headword,
        "_next_url": next_url,
    }


def _entry_key_from_url(url: str) -> str:
    """Extract the entry key slug from a Perseus URL."""
    m = re.search(r"entry%3D([^&]+)$", url) or re.search(r"entry=([^&]+)$", url)
    if m:
        return urllib.parse.unquote(m.group(1)).lower()
    # fallback: hash the url
    return str(abs(hash(url)) % 1_000_000)


def _clean_html(raw: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    # Replace <p/>, <br/>, <br> with space
    text = re.sub(r"<p\s*/?>|<br\s*/?>", " ", raw, flags=re.IGNORECASE)
    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode common HTML entities
    text = (
        text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .replace("&#160;", " ")
    )
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ── Entry iterator ────────────────────────────────────────────────────────────

def iter_entries(
    start_url: str,
    delay: float = 1.5,
    start_from: str = "",
) -> Iterator[dict]:
    """
    Walk Perseus entries via next links, yielding entry dicts.
    Handles letter boundaries by detecting when next_url is missing
    and jumping to the next letter's first entry.
    """
    url = start_url
    seen: set[str] = set()
    skipping = bool(start_from)

    letter_urls = get_all_letter_urls()
    letter_idx = 0

    while url:
        if url in seen:
            print(f"  [WARN] cycle detected at {url}, stopping", file=sys.stderr)
            break
        seen.add(url)

        try:
            html = fetch(url)
        except Exception as e:
            print(f"  [ERR] fetch failed: {url}: {e}", file=sys.stderr)
            # Try to advance to next letter
            letter_idx += 1
            if letter_idx < len(letter_urls):
                first = find_first_entry_url(letter_urls[letter_idx])
                url = first or ""
            else:
                break
            time.sleep(delay)
            continue

        entry = extract_entry_from_html(html, url)

        if entry:
            key = entry["id"]

            if skipping:
                if start_from and start_from in key:
                    skipping = False
                else:
                    url = entry.get("_next_url") or ""
                    time.sleep(delay * 0.3)  # fast-skip with minimal delay
                    continue

            # Yield a clean copy without internal fields
            out = {k: v for k, v in entry.items() if not k.startswith("_")}
            yield out

            next_url = entry.get("_next_url")
            if next_url:
                url = next_url
            else:
                # End of letter — advance to next letter
                letter_idx += 1
                if letter_idx < len(letter_urls):
                    print(f"  [INFO] advancing to letter index {letter_idx}", file=sys.stderr)
                    time.sleep(delay)
                    first = find_first_entry_url(letter_urls[letter_idx])
                    url = first or ""
                    if not url:
                        letter_idx += 1
                        continue
                else:
                    break
        else:
            # Page loaded but no entry — might be a letter index page
            # Try to find first entry on this page
            first = find_first_entry_url(url)
            if first and first != url:
                url = first
                time.sleep(delay * 0.5)
                continue
            else:
                # Give up on this URL, advance letter
                letter_idx += 1
                if letter_idx < len(letter_urls):
                    first = find_first_entry_url(letter_urls[letter_idx])
                    url = first or ""
                else:
                    break

        time.sleep(delay)


# ── Resume support ────────────────────────────────────────────────────────────

def load_existing_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    if not path.exists():
        return ids
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if "id" in obj:
                    ids.add(obj["id"])
            except Exception:
                pass
    return ids


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape Platner & Ashby from Perseus → JSONL")
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument("--resume", action="store_true", help="Resume from last entry in output file")
    ap.add_argument("--dry-run", action="store_true", help="Fetch 5 entries, print to stdout only")
    ap.add_argument("--delay", type=float, default=1.5, help="Seconds between requests (default 1.5)")
    ap.add_argument("--start-entry", default="", help="Start from entry key slug (for manual resume)")
    args = ap.parse_args()

    out_path = Path(args.out)

    if args.dry_run:
        print("[DRY RUN] Fetching first 5 entries from Perseus...\n", file=sys.stderr)
        count = 0
        # Find first real entry
        first_letter_url = get_all_letter_urls()[0]
        start_url = find_first_entry_url(first_letter_url)
        if not start_url:
            raise SystemExit("[ERR] Could not find first entry URL")
        for entry in iter_entries(start_url, delay=1.0):
            print(json.dumps(entry, ensure_ascii=False))
            count += 1
            if count >= 5:
                break
        print(f"\n[DRY RUN] Done. {count} entries.", file=sys.stderr)
        return 0

    # Normal run
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing_ids: set[str] = set()
    last_key = args.start_entry.strip()

    if args.resume and out_path.exists():
        existing_ids = load_existing_ids(out_path)
        print(f"[RESUME] Found {len(existing_ids)} existing entries in {out_path}", file=sys.stderr)
        # Find last entry key from file
        last_line = ""
        with out_path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    last_line = line.strip()
        if last_line:
            try:
                last_obj = json.loads(last_line)
                raw_id = last_obj.get("id", "")
                last_key = raw_id.replace(f"{CORPUS_ID}:", "")
                print(f"[RESUME] Last entry: {last_key}", file=sys.stderr)
            except Exception:
                pass

    # Find start URL
    first_letter_url = get_all_letter_urls()[0]
    start_url = find_first_entry_url(first_letter_url)
    if not start_url:
        raise SystemExit("[ERR] Could not find first entry URL from Perseus")

    print(f"[INFO] Writing to: {out_path}", file=sys.stderr)
    print(f"[INFO] Delay between requests: {args.delay}s", file=sys.stderr)
    print(f"[INFO] Starting from: {start_url}", file=sys.stderr)

    mode = "a" if args.resume else "w"
    count = 0
    skipped = 0

    with out_path.open(mode, encoding="utf-8", newline="\n") as f:
        for entry in iter_entries(start_url, delay=args.delay, start_from=last_key):
            if entry["id"] in existing_ids:
                skipped += 1
                continue

            line = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
            f.write(line + "\n")
            count += 1

            if count % 25 == 0:
                f.flush()
                print(f"  [OK] {count} entries written (last: {entry['loc']})", file=sys.stderr)

    print(f"\n[DONE] {count} entries written, {skipped} skipped (already existed)", file=sys.stderr)
    print(f"[DONE] Output: {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
