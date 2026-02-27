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

def ensure_registry_shape(reg: Any) -> Dict[str, Any]:
    if not isinstance(reg, dict):
        raise SystemExit("[FATAL] registry must be a JSON object")
    if "corpora" not in reg:
        reg["corpora"] = []
    if not isinstance(reg["corpora"], list):
        raise SystemExit("[FATAL] registry.corpora must be a list")
    if "generated_utc" not in reg:
        reg["generated_utc"] = utc_now()
    return reg

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="docs/corpora.private.json", help="Registry JSON to update")
    ap.add_argument("--manifest", default="", help="Optional manifest JSON (adds/updates entries then hashes)")
    ap.add_argument("--only-prefix", default="", help="Only update corpora where corpus_id startswith prefix")
    ap.add_argument("--only", action="append", default=[], help="Only update specific corpus_id (repeatable)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fail-missing", action="store_true", help="Fail if any canon_jsonl.path is missing")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    reg_path = (root / Path(args.registry)).resolve()

    if reg_path.exists():
        reg = ensure_registry_shape(jload(reg_path))
    else:
        reg = {"generated_utc": utc_now(), "corpora": []}

    corpora: List[Dict[str, Any]] = reg["corpora"]
    by_id: Dict[str, Dict[str, Any]] = {
        c.get("corpus_id"): c for c in corpora if isinstance(c, dict) and c.get("corpus_id")
    }

    # If manifest provided, add/update registry entries from it
    if args.manifest:
        man_path = (root / Path(args.manifest)).resolve()
        man = jload(man_path)
        items = man.get("corpora", [])
        if not isinstance(items, list) or not items:
            raise SystemExit("[FATAL] manifest.corpora must be a non-empty list")

        for it in items:
            cid = str(it.get("corpus_id","")).strip()
            if not cid:
                raise SystemExit("[FATAL] manifest corpus missing corpus_id")
            entry = by_id.get(cid)
            if entry is None:
                entry = {"corpus_id": cid}
                corpora.append(entry)
                by_id[cid] = entry

            # minimal fields
            entry["title"] = it.get("title", cid)
            out_rel = it.get("out")
            if not out_rel:
                raise SystemExit(f"[FATAL] manifest corpus {cid} missing out path")
            entry["canon_jsonl"] = entry.get("canon_jsonl", {})
            entry["canon_jsonl"]["path"] = str(out_rel).replace("\\","/")

            if "rights" in it and it["rights"] is not None:
                entry["rights"] = it["rights"]

    # Filter set
    only_set = set(args.only) if args.only else set()
    prefix = args.only_prefix

    updated = 0
    missing: List[str] = []

    for c in corpora:
        if not isinstance(c, dict):
            continue
        cid = c.get("corpus_id")
        if not cid:
            continue

        if prefix and not str(cid).startswith(prefix):
            continue
        if only_set and cid not in only_set:
            continue

        canon = c.get("canon_jsonl")
        if not isinstance(canon, dict) or not canon.get("path"):
            continue

        rel = str(canon["path"])
        abs_path = (root / Path(rel)).resolve()
        if not abs_path.exists():
            missing.append(f"{cid} -> {rel}")
            continue

        h = sha256_file(abs_path)
        canon["sha256"] = h
        updated += 1
        print(f"[OK] {cid} sha256={h}")

    if missing and args.fail_missing:
        raise SystemExit("[FATAL] missing canon_jsonl files:\n" + "\n".join(missing[:50]))

    reg["generated_utc"] = utc_now()
    reg["corpora"] = corpora

    if args.dry_run:
        print(f"[DRY] would write registry -> {reg_path} (updated={updated})")
        if missing:
            print("[DRY] missing:", *missing[:20], sep="\n- ")
        return 0

    jwrite_min(reg_path, reg)
    print(f"[OK] wrote registry -> {reg_path} (updated={updated})")
    if missing:
        print("[WARN] missing canon_jsonl for some entries (not fatal):")
        for m in missing[:20]:
            print(" -", m)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())