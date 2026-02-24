#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest a Perseus / CTS-style TEI XML file (Greek/Latin) into Scriptorium canonical JSONL.

Design goals:
- Local-first and reproducible.
- No hallucination: emitted text is extracted verbatim from source XML (whitespace-normalized only).
- Stable identifiers: segments.id = "<corpus_id>:<local_id>" where local_id is derived from CTS/Stephanus milestones.

Output JSONL record (minimum):
- id (canonical, prefixed)
- work_id (CTS URN if present)
- loc (e.g., Stephanus section like "2a")
- lang ("grc" or "lat")
- text (speaker label + utterance for dialogues)
- source_refs (list; contains source_id + xml filename + loc)
- notes (optional)
Extra fields are preserved in meta_json by db-build.
"""

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def local(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def norm_ws(s: str) -> str:
    return " ".join(s.split()).strip()


def extract_text_skip(elem: ET.Element, skip_local: set[str]) -> str:
    """Extract text from elem, skipping children whose localname is in skip_local.
    Preserves ordering; normalizes whitespace at the end.
    """
    parts: list[str] = []

    def rec(e: ET.Element) -> None:
        if local(e.tag) in skip_local:
            return
        if e.text:
            parts.append(e.text)
        for ch in list(e):
            rec(ch)
            if ch.tail:
                parts.append(ch.tail)

    rec(elem)
    return norm_ws("".join(parts))


def safe_local_id(s: str) -> str:
    # allow a conservative set; replace everything else with "_"
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input TEI XML path")
    ap.add_argument("--out", dest="out_path", required=True, help="Output JSONL path")
    ap.add_argument("--corpus-id", required=True, help="Scriptorium corpus_id (used as id prefix)")
    ap.add_argument("--source-id", required=True, help="Source catalog source_id for provenance")
    ap.add_argument("--lang", default="", help="Override language (e.g., grc, lat)")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit for debugging (0=all)")
    return ap.parse_args()


def main() -> int:
    a = parse_args()
    in_path = Path(a.in_path).resolve()
    out_path = Path(a.out_path).resolve()
    corpus_id = a.corpus_id.strip()
    source_id = a.source_id.strip()

    if not in_path.exists():
        raise SystemExit(f"missing input: {in_path}")

    tree = ET.parse(str(in_path))
    root = tree.getroot()

    # TEI header metadata (best-effort)
    title = ""
    author = ""
    editor = ""

    t_el = root.find(".//tei:teiHeader//tei:titleStmt/tei:title", TEI_NS)
    if t_el is not None and (t_el.text or "").strip():
        title = norm_ws(t_el.text or "")

    a_el = root.find(".//tei:teiHeader//tei:titleStmt/tei:author", TEI_NS)
    if a_el is not None and (a_el.text or "").strip():
        author = norm_ws(a_el.text or "")

    e_el = root.find(".//tei:teiHeader//tei:titleStmt/tei:editor", TEI_NS)
    if e_el is not None and (e_el.text or "").strip():
        editor = norm_ws(e_el.text or "")

    # Work-level URN (Perseus CTS)
    work_id = ""
    ed_div = root.find(".//tei:text/tei:body/tei:div[@type='edition']", TEI_NS)
    if ed_div is not None:
        work_id = (ed_div.attrib.get("n") or "").strip()

    # Language
    lang = a.lang.strip()
    if not lang:
        # TEI often has xml:lang on <text>
        t = root.find(".//tei:text", TEI_NS)
        if t is not None:
            lang = (t.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") or "").strip()
    if not lang:
        lang = "und"

    # Iterate paragraphs in document order under the edition div
    if ed_div is None:
        raise SystemExit("no <div type='edition'> found in TEI")

    ps = ed_div.findall(".//tei:p", TEI_NS)

    current_section = ""
    current_page = ""
    per_loc_count: dict[str, int] = {}
    global_i = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)

    n_written = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for p in ps:
            if a.limit and n_written >= int(a.limit):
                break

            # Update section/page markers if present in this paragraph
            sec_ms = p.findall(".//tei:milestone[@unit='section']", TEI_NS)
            if sec_ms:
                n = (sec_ms[-1].attrib.get("n") or "").strip()
                if n:
                    current_section = n

            page_ms = p.findall(".//tei:milestone[@unit='page']", TEI_NS)
            if page_ms:
                n = (page_ms[-1].attrib.get("n") or "").strip()
                if n:
                    current_page = n

            loc = current_section or current_page or ""
            loc = loc.strip()

            # Dialogue: prefer <said> inside <p>
            said = p.find(".//tei:said", TEI_NS)
            if said is None:
                # Fall back to paragraph text (skip milestones)
                body = extract_text_skip(p, {"milestone"})
                if not body:
                    continue
                label_text = ""
                who = ""
                text_out = body
            else:
                who = (said.attrib.get("who") or "").strip()
                lab = said.find("tei:label", TEI_NS)
                label_text = norm_ws("".join(lab.itertext())) if lab is not None else ""
                body = extract_text_skip(said, {"milestone", "label"})
                if not body:
                    continue
                text_out = (label_text + " " + body).strip() if label_text else body

            # Stable local id
            global_i += 1
            key = loc if loc else "p"
            per_loc_count.setdefault(key, 0)
            per_loc_count[key] += 1

            if loc:
                local_id = f"{safe_local_id(loc)}.{per_loc_count[key]:03d}"
            else:
                local_id = f"p{global_i:05d}"

            seg_id = f"{corpus_id}:{local_id}"

            rec: dict[str, Any] = {
                "id": seg_id,
                "corpus_id": corpus_id,
                "work_id": work_id,
                "loc": loc,
                "lang": lang,
                "text": text_out,
                "source_refs": [
                    {
                        "source_id": source_id,
                        "input": in_path.name,
                        "work_id": work_id,
                        "loc": loc,
                    }
                ],
                "notes": [],
                "meta": {
                    "title": title,
                    "author": author,
                    "editor": editor,
                    "who": who,
                    "label": label_text,
                    "stephanus_section": current_section,
                    "stephanus_page": current_page,
                },
            }

            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n_written += 1

    print(str(out_path))
    print(f"[OK] wrote segments={n_written} corpus_id={corpus_id} work_id={work_id or 'UNKNOWN'} lang={lang}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
