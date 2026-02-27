#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html.entities
import json
import re
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Iterable, Optional, Tuple, List, Dict, Any

XML_NS = "http://www.w3.org/XML/1998/namespace"
XML_ID = f"{{{XML_NS}}}id"
NAME2CP = html.entities.name2codepoint

def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _norm_ws(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _sha256_txt(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def _sanitize_local_id(s: str) -> str:
    s = (s or "").strip().replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "seg"

# --- Sanitization for Perseus TEI.2 / DTDs / entity weirdness ---
_CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

# Remove DOCTYPE blocks (external subsets break stdlib parser)
_DOCTYPE_RE = re.compile(r"<!DOCTYPE[^>]*(\[[\s\S]*?\])?>", re.IGNORECASE)

# General entity declarations with literal value (we can map these)
_ENTITY_DECL_VALUE_RE = re.compile(
    r"<!ENTITY\s+([A-Za-z][A-Za-z0-9._-]*)\s+(\"[^\"]*\"|'[^']*')\s*>",
    re.IGNORECASE,
)

# Parameter entity declarations or SYSTEM/PUBLIC entity declarations (we cannot resolve offline)
_ENTITY_DECL_ANY_RE = re.compile(r"<!ENTITY\b[\s\S]*?>", re.IGNORECASE)

# Parameter entity references like %PersDict;
_PARAM_REF_RE = re.compile(r"%[A-Za-z][A-Za-z0-9._-]*;")

# General entity refs like &foo;
_ENTITY_REF_RE = re.compile(r"&([A-Za-z][A-Za-z0-9._-]*);")

_XML_BUILTINS = {"amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'"}

def _read_and_sanitize_xml_text(p: Path) -> str:
    raw = p.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    text = raw.decode("utf-8", errors="replace")
    text = _CTRL_RE.sub("", text)

    # Strip DOCTYPE first
    text = _DOCTYPE_RE.sub("", text)

    # Capture literal entity decls into a map, then remove ALL entity decls (including % and SYSTEM/PUBLIC)
    custom: Dict[str, str] = {}
    for m in _ENTITY_DECL_VALUE_RE.finditer(text):
        name = m.group(1)
        val = m.group(2)
        if val and val[0] in ("'", '"'):
            val = val[1:-1]
        custom[name] = val

    text = _ENTITY_DECL_ANY_RE.sub("", text)

    # Remove parameter entity refs left behind (e.g. %PersDict;)
    text = _PARAM_REF_RE.sub("", text)

    # Remove leftover internal-subset terminator artifacts (common after DOCTYPE stripping)
    # e.g. "]>" before root element
    text = text.replace("]>", "")

    # Replace general entity refs
    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name in _XML_BUILTINS:
            return _XML_BUILTINS[name]
        if name in custom:
            return custom[name]
        cp = NAME2CP.get(name)
        if cp is not None:
            return chr(cp)
        return f"[ENTITY:{name}]"

    text = _ENTITY_REF_RE.sub(repl, text)
    return text

def _iter_text_with_placeholders(node: ET.Element) -> Iterable[str]:
    tag = _localname(node.tag)

    if tag == "teiHeader":
        return
    if tag == "note":
        return

    if tag == "gap":
        yield "[ ]"
        return
    if tag == "lb":
        yield "\n"
        return
    if tag == "pb":
        return

    if node.text:
        yield node.text

    for ch in list(node):
        ch_tag = _localname(ch.tag)
        if ch_tag == "unclear":
            inner = "".join(_iter_text_with_placeholders(ch))
            inner = _norm_ws(inner)
            if inner:
                yield "[" + inner + "]"
        else:
            yield from _iter_text_with_placeholders(ch)
        if ch.tail:
            yield ch.tail

def _extract_txt(node: ET.Element) -> str:
    return _norm_ws("".join(_iter_text_with_placeholders(node)))

def _pick_segment_nodes(root: ET.Element) -> Tuple[str, List[ET.Element]]:
    text_node = root.find(".//{*}text")
    scope = text_node if text_node is not None else root

    # Prefer <entryFree> for reference works (Smith etc.)
    entries = list(scope.findall(".//{*}entryFree"))
    if entries:
        return ("entryFree", entries)

    ps = list(scope.findall(".//{*}p"))
    if ps:
        return ("p", ps)

    divs = list(scope.findall(".//{*}div"))
    if divs:
        return ("div", divs)

    return ("text", [scope])

def _get_xml_id(node: ET.Element) -> str:
    v = node.attrib.get(XML_ID, "") or node.attrib.get("id", "")
    return str(v).strip()

def build_records(*, corpus_id: str, work: str, lang: str, in_path: Path, min_chars: int) -> List[Dict[str, Any]]:
    xml_text = _read_and_sanitize_xml_text(in_path)
    try:
        root = ET.fromstring(xml_text.encode("utf-8"))
    except Exception as e:
        head = xml_text[:260].replace("\n", "\\n")
        raise RuntimeError(f"XML parse failed after sanitization: {e}. Head={head!r}")

    kind, nodes = _pick_segment_nodes(root)

    out: List[Dict[str, Any]] = []
    seen_local: set[str] = set()
    base = in_path.name

    for i, n in enumerate(nodes, start=1):
        txt = _extract_txt(n)
        if not txt or len(txt) < min_chars:
            continue

        xmlid = _get_xml_id(n)
        local_id = _sanitize_local_id(xmlid) if xmlid else f"{kind}.{i:06d}"
        if local_id in seen_local:
            local_id = f"{local_id}.{i:06d}"
        seen_local.add(local_id)

        loc = f"{base}#{xmlid or local_id}"
        out.append({
            "id": local_id,
            "src": corpus_id,
            "work": work,
            "loc": loc,
            "srcp": loc,
            "lang": lang,
            "txt": txt,
            "sha256": _sha256_txt(txt),
        })

    return out

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--lang", required=True)
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--min-chars", type=int, default=40)
    args = ap.parse_args(argv)

    corpus_id = str(args.corpus).strip()
    if ":" in corpus_id:
        print("[FATAL] --corpus must not contain ':'", file=sys.stderr)
        return 2

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    if not in_path.exists():
        print(f"[FATAL] input not found: {in_path}", file=sys.stderr)
        return 2
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records = build_records(
        corpus_id=corpus_id,
        work=str(args.work).strip(),
        lang=str(args.lang).strip(),
        in_path=in_path,
        min_chars=int(args.min_chars),
    )

    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(f"[OK] wrote {len(records)} records -> {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())