from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable, Optional

XML_NS = "http://www.w3.org/XML/1998/namespace"


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") and "}" in tag else tag


def detect_tei_ns(root_el: ET.Element) -> str | None:
    # default namespace appears as {uri}TEI
    if root_el.tag.startswith("{") and "}" in root_el.tag:
        return root_el.tag.split("}", 1)[0][1:]
    return None


def norm_ws(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def extract_text_with_breaks(elem: ET.Element) -> str:
    parts: list[str] = []

    def walk(e: ET.Element) -> None:
        name = strip_ns(e.tag)
        if name in ("lb", "pb", "cb"):
            parts.append("\n")

        if e.text:
            parts.append(e.text)

        for ch in list(e):
            walk(ch)
            if ch.tail:
                parts.append(ch.tail)

    walk(elem)
    return norm_ws("".join(parts))


def iter_by_localname(root: ET.Element, local: str) -> Iterable[ET.Element]:
    for e in root.iter():
        if strip_ns(e.tag) == local:
            yield e


def find_first_body(root_el: ET.Element, tei_ns: str | None) -> Optional[ET.Element]:
    # Try namespaced path first if we have a namespace
    if tei_ns:
        ns = {"tei": tei_ns}
        body = root_el.find(".//tei:text/tei:body", ns)
        if body is not None:
            return body
        body = root_el.find(".//tei:body", ns)
        if body is not None:
            return body

    # Fallback: no-namespace paths
    body = root_el.find(".//text/body")
    if body is not None:
        return body
    body = root_el.find(".//body")
    if body is not None:
        return body

    # Last resort: any element with localname 'body'
    for b in iter_by_localname(root_el, "body"):
        return b
    return None


def pick_blocks(body: ET.Element) -> list[ET.Element]:
    # Prefer common TEI block carriers in order
    # (Many TEI corpora use <ab> or <l>/<lg> rather than <p>.)
    blocks: list[ET.Element] = []
    for name in ("p", "ab", "lg", "l", "head", "seg"):
        blocks = [e for e in body.iter() if strip_ns(e.tag) == name]
        if blocks:
            return blocks

    # If nothing matched, treat the whole body as one block
    return [body]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--in-dir", required=True, help="Directory containing ECHOE TEI XML files")
    ap.add_argument("--out", required=True, help="Output canon JSONL path")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    in_dir = Path(args.in_dir)
    if not in_dir.is_absolute():
        in_dir = (root / in_dir).resolve()

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()

    if not in_dir.exists():
        raise SystemExit(f"missing in-dir: {in_dir}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    xml_files = sorted([p for p in in_dir.rglob("*.xml") if p.is_file()])
    if not xml_files:
        raise SystemExit(f"no xml files under: {in_dir}")

    def rel_to_root(p: Path) -> str:
        try:
            return p.resolve().relative_to(root).as_posix()
        except Exception:
            return p.as_posix()

    written = 0
    seen: set[str] = set()

    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        for fp in xml_files:
            try:
                tree = ET.parse(fp)
            except Exception:
                continue

            root_el = tree.getroot()
            tei_ns = detect_tei_ns(root_el)

            body = find_first_body(root_el, tei_ns)
            if body is None:
                continue

            doc = fp.stem
            file_sha = sha256_file(fp)
            src_rel = rel_to_root(fp)

            tei_id = root_el.get(f"{{{XML_NS}}}id")

            blocks = pick_blocks(body)

            idx = 0
            for b in blocks:
                txt = extract_text_with_breaks(b)
                if not txt:
                    continue
                idx += 1

                b_xml_id = b.get(f"{{{XML_NS}}}id")
                rid = f"echoe_tei:{doc}:{idx:05d}"
                if rid in seen:
                    raise SystemExit(f"duplicate id: {rid}")
                seen.add(rid)

                rec: dict[str, Any] = {
                    "id": rid,
                    "corpus_id": "echoe_tei",
                    "work_id": doc,
                    "witness_id": tei_id or doc,
                    "edition_id": "ECHOEProject/echoe:xml",
                    "loc": f"{fp.relative_to(in_dir).as_posix()}#b{idx}",
                    "lang": "ang",
                    "text": txt,
                    "text_norm": None,
                    "source_refs": [
                        {
                            "type": "file",
                            "path": src_rel,
                            "sha256": file_sha,
                            "offset": {"block": idx, "xml_id": b_xml_id, "tag": strip_ns(b.tag)},
                        }
                    ],
                    "notes": [],
                }

                out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                written += 1

    print(str(out_path))
    print(f"[OK] files={len(xml_files)} records={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
