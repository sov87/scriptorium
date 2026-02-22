from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}
XML_NS = "http://www.w3.org/XML/1998/namespace"


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm_ws(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def extract_text_with_breaks(elem: ET.Element) -> str:
    parts: list[str] = []

    def walk(e: ET.Element) -> None:
        tag = e.tag
        if tag.endswith("lb") or tag.endswith("pb") or tag.endswith("cb"):
            parts.append("\n")

        if e.text:
            parts.append(e.text)

        for ch in list(e):
            walk(ch)
            if ch.tail:
                parts.append(ch.tail)

    walk(elem)
    return norm_ws("".join(parts))


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

            doc = fp.stem
            file_sha = sha256_file(fp)
            src_rel = rel_to_root(fp)

            root_el = tree.getroot()
            tei_id = root_el.get(f"{{{XML_NS}}}id")

            body = root_el.find(".//tei:text/tei:body", TEI_NS)
            if body is None:
                continue

            # Prefer direct <p> within body; if none, fall back to any nested <p>
            ps = body.findall("./tei:p", TEI_NS)
            if not ps:
                ps = body.findall(".//tei:p", TEI_NS)

            idx = 0
            for p in ps:
                txt = extract_text_with_breaks(p)
                if not txt:
                    continue
                idx += 1

                p_xml_id = p.get(f"{{{XML_NS}}}id")
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
                    "loc": f"{fp.relative_to(in_dir).as_posix()}#p{idx}",
                    "lang": "ang",
                    "text": txt,
                    "text_norm": None,
                    "source_refs": [
                        {
                            "type": "file",
                            "path": src_rel,
                            "sha256": file_sha,
                            "offset": {"p": idx, "xml_id": p_xml_id},
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
