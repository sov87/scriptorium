#!/usr/bin/env python3
"""
make_demo_registry.py

Create a small demo registry (JSON) that is a subset of one or more existing registries.
Designed for Scriptorium's docs/corpora*.json schema.

- BOM-safe JSON reading (utf-8, fallback utf-8-sig)
- Can select corpora by explicit corpus_id and/or title regex matches
- Optionally verifies JSONL existence and SHA-256 (and can rewrite the SHA in the output file)
- Never mutates the input registries; writes a new registry file
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set


def read_json_bomsafe(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm_rel(p: str) -> str:
    return p.replace("\\", "/").lstrip("./")


def load_registry(path: Path) -> List[Dict[str, Any]]:
    obj = read_json_bomsafe(path)
    corpora = obj.get("corpora")
    if not isinstance(corpora, list):
        raise ValueError(f"Registry missing corpora[]: {path}")
    out: List[Dict[str, Any]] = []
    for c in corpora:
        if isinstance(c, dict) and isinstance(c.get("corpus_id"), str) and c["corpus_id"].strip():
            out.append(c)
    return out


def build_map(registries: List[Path]) -> Dict[str, Dict[str, Any]]:
    """First occurrence wins (avoids accidental override)."""
    m: Dict[str, Dict[str, Any]] = {}
    for reg in registries:
        for c in load_registry(reg):
            cid = c["corpus_id"].strip()
            if cid not in m:
                m[cid] = {"registry": reg, "entry": c}
    return m


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repo root for resolving canon_jsonl.path")
    ap.add_argument("--registry", action="append", required=True, help="Input registry JSON (repeatable)")
    ap.add_argument("--out", required=True, help="Output registry JSON")
    ap.add_argument("--corpus-id", action="append", default=[], help="Include a corpus_id (repeatable)")
    ap.add_argument("--match-title", action="append", default=[], help="Regex match against title (repeatable)")
    ap.add_argument("--verify-jsonl", action="store_true", help="Verify canon_jsonl.path exists")
    ap.add_argument("--verify-sha256", action="store_true", help="Verify canon_jsonl.sha256 matches file on disk")
    ap.add_argument("--rewrite-sha256", action="store_true", help="If sha mismatches, write disk sha into OUTPUT registry")
    ap.add_argument("--allow-missing", action="store_true", help="Do not fail if a requested corpus_id is missing")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON (indent=2)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    registries = [Path(p) if Path(p).is_absolute() else (root / p) for p in args.registry]
    out_path = Path(args.out) if Path(args.out).is_absolute() else (root / args.out)

    m = build_map([p.resolve() for p in registries])

    selected: Set[str] = set()
    missing: List[str] = []

    for cid in args.corpus_id:
        cid = cid.strip()
        if not cid:
            continue
        if cid in m:
            selected.add(cid)
        else:
            missing.append(cid)

    for pat in args.match_title:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for cid, obj in m.items():
            title = obj["entry"].get("title") or ""
            if isinstance(title, str) and rx.search(title):
                selected.add(cid)

    if missing and not args.allow_missing:
        raise SystemExit(
            "Requested corpus_id(s) not found in provided registries:\n  - " + "\n  - ".join(missing)
        )

    if not selected:
        raise SystemExit("No corpora selected. Use --corpus-id and/or --match-title.")

    out_corpora: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for cid in sorted(selected):
        entry = json.loads(json.dumps(m[cid]["entry"]))  # deep copy
        canon = entry.get("canon_jsonl") if isinstance(entry.get("canon_jsonl"), dict) else None
        if canon is None:
            warnings.append(f"{cid}: missing canon_jsonl in registry entry")
            out_corpora.append(entry)
            continue

        path_s = canon.get("path") or ""
        if not isinstance(path_s, str) or not path_s.strip():
            warnings.append(f"{cid}: canon_jsonl.path is empty")
            out_corpora.append(entry)
            continue

        abs_path = (root / path_s).resolve()
        if args.verify_jsonl and not abs_path.exists():
            raise SystemExit(f"{cid}: canon_jsonl.path not found: {abs_path}")

        if args.verify_sha256 and abs_path.exists():
            disk = sha256_file(abs_path).upper()
            reg = (canon.get("sha256") or "").upper()
            if reg and reg != disk:
                msg = f"{cid}: sha256 mismatch registry={reg} disk={disk} path={path_s}"
                if args.rewrite_sha256:
                    canon["sha256"] = disk
                    warnings.append(msg + " (rewrote output sha256)")
                else:
                    raise SystemExit(msg)
            elif not reg:
                warnings.append(f"{cid}: registry missing sha256; disk={disk} path={path_s}")
                if args.rewrite_sha256:
                    canon["sha256"] = disk

        out_corpora.append(entry)

    out_obj = {
        "generated_utc": None,  # leave null to avoid churn; update elsewhere if you want
        "corpora": out_corpora,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.pretty:
        out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    else:
        out_path.write_text(json.dumps(out_obj, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "out": norm_rel(str(out_path)),
                "selected": len(out_corpora),
                "missing": missing,
                "warnings": warnings,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
