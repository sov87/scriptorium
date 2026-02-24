# File: src/ingest/ingest_echoe_tei.py
# ECHOE TEI ingest wrapper (updated to respect canonical ID invariant and TEI normalization policy).
#
# - Uses lxml + namespace-aware XPath.
# - Emits canonical JSONL records matching the common shape:
#     {corpus_id, local_id, id, work_id, loc, text, meta}
# - segments.id invariant: id == f"{corpus_id}:{local_id}" (exactly one colon)
# - Never silently drops <choice>/<unclear>/<gap>/<supplied>; relies on scriptorium.ingest.tei_cts normalization.

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from lxml import etree as ET

from scriptorium.ingest.tei_cts import (
    NS,
    XML_NS,
    normalize_node_to_text,
    parse_tei,
    parse_work_id,
    sanitize_local_id_from_loc,
)

def _minijson(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _find_body(root: ET._Element) -> Optional[ET._Element]:
    bodies = root.xpath(".//tei:text/tei:body", namespaces=NS)
    if bodies:
        return bodies[0]
    bodies = root.xpath(".//tei:body", namespaces=NS)
    return bodies[0] if bodies else None

def _pick_blocks(body: ET._Element) -> List[ET._Element]:
    # Prefer common TEI block carriers in order (deterministic).
    for name in ("p", "ab", "lg", "l", "head", "seg"):
        els = body.xpath(f".//tei:{name}", namespaces=NS)
        if els:
            return list(els)
    return [body]

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--in-dir", required=True, help="Directory containing ECHOE TEI XML files")
    ap.add_argument("--out", required=True, help="Output canonical JSONL path")
    ap.add_argument("--with-sha256", action="store_true", help="Include per-file sha256 in meta (deterministic but slower)")
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

    seen: set[str] = set()
    written = 0

    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        for fp in xml_files:
            try:
                tree = parse_tei(str(fp))
            except Exception:
                continue

            root_el = tree.getroot()
            body = _find_body(root_el)
            if body is None:
                continue

            # work_id: prefer CTS URN if present; else filename stem
            work_id = parse_work_id(tree) or fp.stem

            src_meta: Dict[str, Any] = {
                "path": rel_to_root(fp),
            }
            if args.with_sha256:
                src_meta["sha256"] = _sha256_file(fp)

            tei_xml_id = root_el.get(f"{{{XML_NS}}}id")

            blocks = _pick_blocks(body)

            for idx, b in enumerate(blocks, start=1):
                txt, tei_meta = normalize_node_to_text(b)
                if not txt:
                    continue

                loc = f"{fp.relative_to(in_dir).as_posix()}#b{idx:05d}"
                local_id = sanitize_local_id_from_loc(loc)
                if not local_id:
                    local_id = f"{fp.stem}_{idx:05d}"

                corpus_id = "echoe_tei"
                rid = f"{corpus_id}:{local_id}"
                if rid in seen:
                    raise SystemExit(f"duplicate id: {rid}")
                seen.add(rid)

                b_xml_id = b.get(f"{{{XML_NS}}}id")
                meta: Dict[str, Any] = {
                    "lang": "ang",
                    "witness_id": tei_xml_id or fp.stem,
                    "edition_id": "ECHOEProject/echoe:xml",
                    "source": src_meta,
                    "offset": {"block": idx, "xml_id": b_xml_id, "tag": ET.QName(b).localname},
                }
                # merge TEI policy metadata (choices/unclear/supplied/gaps)
                meta.update(tei_meta)

                rec = {
                    "corpus_id": corpus_id,
                    "local_id": local_id,
                    "id": rid,
                    "work_id": work_id,
                    "loc": loc,
                    "text": txt,
                    "meta": meta,
                }

                out.write(_minijson(rec) + "\n")
                written += 1

    print(str(out_path))
    print(f"[OK] files={len(xml_files)} records={written}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
