# File: src/scriptorium/ingest/tei_cts.py
# Reusable TEI/CTS ingest helpers (lxml + namespace-aware XPath)
#
# Implements deterministic TEI normalization policy:
# - <choice>: prefer <reg> over <orig> for searchable text; preserve alternate in meta
# - <unclear>: keep text; mark uncertainty consistently; never drop silently
# - <gap>/<supplied>: represent damaged/supplied text consistently; never drop silently
#
# CTS/URN policy hooks:
# - parse_work_id() tries to extract CTS URN from teiHeader/idno
# - iter_segment_drafts() yields deterministic segments with a CTS-like loc when possible
#
# This module does NOT write JSONL; wrappers should:
# - derive local_id deterministically from loc when possible (no ':')
# - set segments.id = f"{corpus_id}:{local_id}"

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import re
import unicodedata

from lxml import etree as ET


__all__ = [
    "SegmentDraft",
    "parse_tei",
    "parse_work_id",
    "iter_segment_drafts",
    "sanitize_local_id_from_loc",
    "normalize_search_text",
    "get_xml_id",
]

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"

NS = {"tei": TEI_NS, "xml": XML_NS}

_WS_RE = re.compile(r"\s+", flags=re.UNICODE)


@dataclass(frozen=True)
class SegmentDraft:
    """Deterministic, not-yet-canonical segment."""
    loc: Optional[str]
    text: str
    meta: Dict[str, Any]


# ----------------------------
# Parsing / helpers
# ----------------------------

def parse_tei(path: Union[str, Path]) -> ET._ElementTree:
    """
    Parse TEI XML deterministically.

    - Entity resolution disabled.
    - Comments/PIs preserved.
    - Recover disabled (fail fast on malformed TEI).
    """
    p = str(path)
    parser = ET.XMLParser(
        resolve_entities=False,
        remove_comments=False,
        remove_pis=False,
        ns_clean=True,
        recover=False,
        huge_tree=True,
    )
    return ET.parse(p, parser)


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", s).strip()


def normalize_search_text(s: str) -> str:
    """
    Final normalization for searchable text:
    - Unicode NFC
    - collapse whitespace
    """
    return _norm_ws(_nfc(s))


def get_xml_id(el: ET._Element) -> Optional[str]:
    return el.get(f"{{{XML_NS}}}id")


def sanitize_local_id_from_loc(loc: str) -> str:
    """
    Deterministic local_id derived from loc.

    Hard rules:
    - must not contain ':'
    - should be stable across runs
    """
    s = loc.strip().replace(":", "_")
    # Allow letters/digits/underscore, dots, hyphen. Replace everything else.
    s = re.sub(r"[^\w\.\-]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


# ----------------------------
# CTS / URN extraction
# ----------------------------

def parse_work_id(tree: ET._ElementTree) -> Optional[str]:
    """
    Extract a CTS URN when available from TEI header.

    Heuristics (priority order):
    1) tei:idno where @type suggests CTS URN (contains 'cts' and 'urn')
    2) tei:idno whose text contains 'urn:cts:'
    """
    root = tree.getroot()
    idnos = root.xpath(".//tei:teiHeader//tei:idno", namespaces=NS)
    if not idnos:
        return None

    for el in idnos:
        t = (el.get("type") or "").strip().lower()
        txt = (el.text or "").strip()
        if txt and ("cts" in t and "urn" in t):
            return _nfc(txt)

    for el in idnos:
        txt = (el.text or "").strip()
        if txt and "urn:cts:" in txt:
            return _nfc(txt)

    return None


# ----------------------------
# TEI normalization (policy)
# ----------------------------

def _meta_bucket(meta: Dict[str, Any]) -> Dict[str, Any]:
    tei = meta.setdefault("tei", {})
    tei.setdefault("choices", [])
    tei.setdefault("unclear_spans", [])
    tei.setdefault("supplied_spans", [])
    tei.setdefault("gaps", [])
    return meta


def normalize_node_to_text(node: ET._Element) -> Tuple[str, Dict[str, Any]]:
    """
    Normalize a TEI element into searchable text while preserving alternates/uncertainty
    in metadata. Deterministic. No silent drops for TEI policy elements.
    """
    meta: Dict[str, Any] = {}
    _meta_bucket(meta)

    out: List[str] = []

    def rec(el: ET._Element, out_list: List[str]) -> None:
        local = ET.QName(el).localname

        # Insert whitespace markers for break-like tags to prevent token collisions.
        # These will collapse to a single space in normalize_search_text().
        if local in ("lb", "pb", "cb"):
            out_list.append(" ")
            # still include tail via caller logic
            # (lb/pb/cb are empty most of the time)
            # do not return; still walk children if any (rare)
        if local == "choice":
            _handle_choice(el, out_list, meta)
            return
        if local == "unclear":
            _handle_unclear(el, out_list, meta)
            return
        if local == "supplied":
            _handle_supplied(el, out_list, meta)
            return
        if local == "gap":
            _handle_gap(el, out_list, meta)
            return

        if el.text:
            out_list.append(el.text)

        for child in el:
            rec(child, out_list)
            if child.tail:
                out_list.append(child.tail)

    rec(node, out)

    text = normalize_search_text("".join(out))
    if text == "":
        meta["empty_text"] = True

    return text, meta


def _extract_inner_text(el: ET._Element, meta: Dict[str, Any]) -> str:
    tmp: List[str] = []

    def rec(el2: ET._Element, out_list: List[str]) -> None:
        local = ET.QName(el2).localname

        if local in ("lb", "pb", "cb"):
            out_list.append(" ")

        if local == "choice":
            _handle_choice(el2, out_list, meta)
            return
        if local == "unclear":
            _handle_unclear(el2, out_list, meta)
            return
        if local == "supplied":
            _handle_supplied(el2, out_list, meta)
            return
        if local == "gap":
            _handle_gap(el2, out_list, meta)
            return

        if el2.text:
            out_list.append(el2.text)

        for ch in el2:
            rec(ch, out_list)
            if ch.tail:
                out_list.append(ch.tail)

    if el.text:
        tmp.append(el.text)
    for ch in el:
        rec(ch, tmp)
        if ch.tail:
            tmp.append(ch.tail)

    return normalize_search_text("".join(tmp))


def _handle_choice(el: ET._Element, out_list: List[str], meta: Dict[str, Any]) -> None:
    reg = el.find("./tei:reg", namespaces=NS)
    orig = el.find("./tei:orig", namespaces=NS)

    preferred_el = reg if reg is not None else (el[0] if len(el) > 0 else None)

    preferred_text = ""
    preferred_tag: Optional[str] = None
    if preferred_el is not None:
        preferred_tag = ET.QName(preferred_el).localname
        preferred_text = _extract_inner_text(preferred_el, meta)
        if preferred_text:
            out_list.append(preferred_text)

    alternate_text = ""
    if orig is not None:
        alternate_text = _extract_inner_text(orig, meta)

    meta["tei"]["choices"].append(
        {
            "preferred": preferred_text,
            "alternate": alternate_text,
            "preferred_tag": ("reg" if reg is not None else preferred_tag),
            "alternate_tag": ("orig" if orig is not None else None),
            "xml_id": get_xml_id(el),
        }
    )


def _handle_unclear(el: ET._Element, out_list: List[str], meta: Dict[str, Any]) -> None:
    txt = _extract_inner_text(el, meta)
    out_list.append(f"[{txt}]" if txt else "[...]")
    meta["tei"]["unclear_spans"].append(
        {"text": txt, "reason": el.get("reason"), "cert": el.get("cert"), "xml_id": get_xml_id(el)}
    )


def _handle_supplied(el: ET._Element, out_list: List[str], meta: Dict[str, Any]) -> None:
    txt = _extract_inner_text(el, meta)
    out_list.append(f"[{txt}]" if txt else "[...]")
    meta["tei"]["supplied_spans"].append(
        {
            "text": txt,
            "reason": el.get("reason"),
            "source": el.get("source"),
            "cert": el.get("cert"),
            "xml_id": get_xml_id(el),
        }
    )


def _handle_gap(el: ET._Element, out_list: List[str], meta: Dict[str, Any]) -> None:
    out_list.append("[...]")
    meta["tei"]["gaps"].append(
        {
            "reason": el.get("reason"),
            "extent": el.get("extent"),
            "unit": el.get("unit"),
            "quantity": el.get("quantity"),
            "xml_id": get_xml_id(el),
        }
    )


# ----------------------------
# Segmentation (deterministic)
# ----------------------------

_DEFAULT_SEG_TAGS: Tuple[str, ...] = ("ab", "p", "l", "seg")


def iter_segment_drafts(
    tree: ET._ElementTree,
    *,
    prefer_tags: Tuple[str, ...] = _DEFAULT_SEG_TAGS,
    use_milestones: bool = True,
) -> Iterator[SegmentDraft]:
    """
    Yield SegmentDraft objects in deterministic document order.

    Strategy (deterministic):
    1) If use_milestones and body contains tei:milestone[@n], segment by milestones.
    2) Else, segment by elements in prefer_tags within tei:text/tei:body (doc order).
    """
    root = tree.getroot()
    bodies = root.xpath(".//tei:text/tei:body", namespaces=NS)
    if not bodies:
        raise ValueError("TEI parse error: no tei:text/tei:body found")
    body = bodies[0]

    if use_milestones:
        ms = body.xpath(".//tei:milestone[@n]", namespaces=NS)
        if ms:
            yield from _segment_by_milestones(body)
            return

    yield from _segment_by_elements(body, prefer_tags=prefer_tags)


def _segment_by_elements(body: ET._Element, *, prefer_tags: Tuple[str, ...]) -> Iterator[SegmentDraft]:
    xp = " | ".join([f".//tei:{t}" for t in prefer_tags])
    elems: List[ET._Element] = body.xpath(xp, namespaces=NS)  # doc order
    for el in elems:
        loc = _compute_cts_like_loc(el)
        text, meta = normalize_node_to_text(el)
        yield SegmentDraft(loc=loc, text=text, meta=meta)


def _segment_by_milestones(body: ET._Element) -> Iterator[SegmentDraft]:
    """
    Conservative milestone segmentation:
    - each milestone[@n] starts a new segment with loc = @n
    - text accumulated until next milestone[@n]
    """
    nodes = list(body.iter())
    current_loc: Optional[str] = None
    buf: List[str] = []
    meta: Dict[str, Any] = {}

    def flush() -> Optional[SegmentDraft]:
        nonlocal buf, meta, current_loc
        if current_loc is None and not buf:
            return None
        _meta_bucket(meta)
        text = normalize_search_text("".join(buf))
        if text == "":
            meta["empty_text"] = True
        sd = SegmentDraft(loc=current_loc, text=text, meta=meta)
        buf = []
        meta = {}
        return sd

    for el in nodes:
        local = ET.QName(el).localname
        if local == "milestone":
            n = (el.get("n") or "").strip()
            if n:
                prior = flush()
                if prior is not None:
                    yield prior
                current_loc = n
            continue

        # only consume content from common text containers to reduce duplication
        if local in ("ab", "p", "l", "seg"):
            t, m = normalize_node_to_text(el)
            if t:
                buf.append(t + " ")
            _merge_meta(meta, m)

    last = flush()
    if last is not None:
        yield last


def _merge_meta(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    if not src:
        return
    _meta_bucket(dst)
    _meta_bucket(src)

    for k in ("choices", "unclear_spans", "supplied_spans", "gaps"):
        dst["tei"][k].extend(src["tei"].get(k, []))

    if src.get("empty_text"):
        dst["empty_text"] = True


def _compute_cts_like_loc(el: ET._Element) -> Optional[str]:
    """
    Default loc heuristic:
    - join ancestor div@n (outer→inner) + element@n, separated by '.'
    - else fallback to xml:id
    """
    n_parts: List[str] = []
    ancestors = list(el.iterancestors())
    ancestors.reverse()

    for a in ancestors:
        local = ET.QName(a).localname
        if local.startswith("div"):
            n = (a.get("n") or "").strip()
            if n:
                n_parts.append(n)

    n_self = (el.get("n") or "").strip()
    if n_self:
        n_parts.append(n_self)

    if n_parts:
        return ".".join(n_parts)

    xml_id = get_xml_id(el)
    if xml_id:
        return xml_id

    return None
