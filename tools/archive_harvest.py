#!/usr/bin/env python3
from __future__ import annotations

import argparse, hashlib, json, time
from pathlib import Path
from typing import Any, Dict, List, Optional

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jload(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_registry(reg: Any) -> Dict[str, Any]:
    if not isinstance(reg, dict):
        raise SystemExit("[FATAL] registry must be JSON object")
    if "corpora" not in reg:
        reg["corpora"] = []
    if not isinstance(reg["corpora"], list):
        raise SystemExit("[FATAL] registry.corpora must be list")
    return reg

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--harvest", default="reports/harvest_rome_min.json", help="Harvest report produced by ingest_auto.py")
    ap.add_argument("--registry", default="docs/corpora.private.json", help="Registry JSON to patch/update")
    ap.add_argument("--fail-missing", action="store_true", help="Fail if any jsonl path is missing")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    harvest_path = (root / Path(args.harvest)).resolve()
    reg_path = (root / Path(args.registry)).resolve()

    if not harvest_path.exists():
        raise SystemExit(f"[FATAL] missing harvest: {harvest_path}")

    harvest = jload(harvest_path)
    items: List[Dict[str, Any]] = harvest.get("items", [])
    if not items:
        raise SystemExit("[FATAL] harvest has no items")

    if reg_path.exists():
        reg = ensure_registry(jload(reg_path))
    else:
        reg = {"generated_utc": utc_now(), "corpora": []}

    corpora: List[Dict[str, Any]] = reg["corpora"]
    by_id: Dict[str, Dict[str, Any]] = {
        c.get("corpus_id"): c for c in corpora if isinstance(c, dict) and c.get("corpus_id")
    }

    missing: List[str] = []
    added = 0
    updated = 0

    for it in items:
        cid = it["corpus_id"]
        rel = it["canon_jsonl"]["path"]
        abs_path = (root / Path(rel)).resolve()
        if not abs_path.exists():
            missing.append(f"{cid} -> {rel}")
            continue

        h = sha256_file(abs_path)

        entry = by_id.get(cid)
        if entry is None:
            entry = {"corpus_id": cid}
            corpora.append(entry)
            by_id[cid] = entry
            added += 1
        else:
            updated += 1

        entry["title"] = it.get("title", cid)
        entry["canon_jsonl"] = {"path": rel.replace("\\","/"), "sha256": h}
        if it.get("rights") is not None:
            entry["rights"] = it["rights"]

        print(f"[OK] {cid} sha256={h}")

    if missing and args.fail_missing:
        raise SystemExit("[FATAL] missing JSONL files:\n" + "\n".join(missing[:80]))

    reg["generated_utc"] = utc_now()
    reg["corpora"] = corpora

    jwrite_min(reg_path, reg)
    print(f"[OK] patched registry -> {reg_path} (added={added} updated={updated})")
    if missing:
        print("[WARN] missing (skipped):")
        for m in missing[:20]:
            print(" -", m)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())