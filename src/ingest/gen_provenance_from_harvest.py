#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOCAL_ONLY_REQUIRED_LINE = "LOCAL-ONLY; NOT FOR REDISTRIBUTION."


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(path: Path) -> Any:
    """
    BOM-safe:
      - try utf-8
      - on JSON decode failure, retry utf-8-sig (strips BOM)
    """
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raw2 = path.read_text(encoding="utf-8-sig")
        return json.loads(raw2)


def write_json(path: Path, obj: Any, *, sort_keys: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=sort_keys) + "\n"
    path.write_text(data, encoding="utf-8")


def normalize_relpath(p: str) -> str:
    return p.replace("\\", "/").lstrip("./")


def is_httpish(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def nonempty_str(x: Any) -> Optional[str]:
    if isinstance(x, str):
        s = x.strip()
        return s if s else None
    return None


def load_registry_map(registry_path: Path) -> Dict[str, Dict[str, Any]]:
    reg = read_json(registry_path)
    corpora = reg.get("corpora")
    if not isinstance(corpora, list):
        raise ValueError(f"Registry {registry_path} missing corpora[]")
    out: Dict[str, Dict[str, Any]] = {}
    for c in corpora:
        if not isinstance(c, dict):
            continue
        cid = nonempty_str(c.get("corpus_id"))
        if cid:
            out[cid] = c
    return out


def ensure_dict(parent: Dict[str, Any], key: str) -> Dict[str, Any]:
    v = parent.get(key)
    if isinstance(v, dict):
        return v
    parent[key] = {}
    return parent[key]


def ensure_list(parent: Dict[str, Any], key: str) -> List[Any]:
    v = parent.get(key)
    if isinstance(v, list):
        return v
    parent[key] = []
    return parent[key]


def has_identifying_field(src: Dict[str, Any]) -> bool:
    for k in ("url", "repo", "citation", "id", "path", "urn", "ref"):
        if nonempty_str(src.get(k)):
            return True
    return False


def append_note(existing: Optional[str], line: str) -> str:
    line = line.strip()
    if not line:
        return existing or ""
    if not existing:
        return line
    if line.lower() in existing.lower():
        return existing
    return existing.rstrip() + "\n" + line


def ensure_local_only_note(prov: Dict[str, Any]) -> None:
    rights = ensure_dict(prov, "rights")
    rights["notes"] = append_note(nonempty_str(rights.get("notes")), LOCAL_ONLY_REQUIRED_LINE)
    # Optional: also add a top-level notes field if you ever want it referenced elsewhere.
    prov["notes"] = append_note(nonempty_str(prov.get("notes")), LOCAL_ONLY_REQUIRED_LINE)


def upsert_required_source(
    prov: Dict[str, Any],
    *,
    repo_root: Optional[str],
    tei: Optional[str],
    work_id: Optional[str],
) -> None:
    sources = ensure_list(prov, "sources")
    for s in sources:
        if isinstance(s, dict) and has_identifying_field(s):
            return

    src: Dict[str, Any] = {"type": "upstream_repo"}
    rr = nonempty_str(repo_root)
    if rr:
        if is_httpish(rr):
            src["url"] = rr
        else:
            src["path"] = normalize_relpath(rr)

    t = nonempty_str(tei)
    if t:
        src["path"] = normalize_relpath(t) if "path" not in src else src["path"] + " | " + normalize_relpath(t)

    w = nonempty_str(work_id)
    if w:
        src["ref"] = w

    if not has_identifying_field(src):
        src["id"] = "auto"

    sources.append(src)


def upsert_required_processing(
    prov: Dict[str, Any],
    *,
    harvest_run_utc: str,
    promote_run_utc: str,
    report_relpath: str,
    tei: Optional[str],
    out_jsonl: Optional[str],
    canon_jsonl_path: Optional[str],
    batch_params: Dict[str, Any],
) -> None:
    processing = ensure_list(prov, "processing")

    def has_step(name: str) -> bool:
        for p in processing:
            if isinstance(p, dict) and nonempty_str(p.get("step")) == name:
                return True
        return False

    def clean_paths(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for x in xs:
            path = nonempty_str(x.get("path"))
            if path:
                out.append({"path": normalize_relpath(path)})
        return out

    if not has_step("harvest"):
        ins: List[Dict[str, Any]] = []
        outs: List[Dict[str, Any]] = []
        if nonempty_str(tei):
            ins.append({"path": tei})
        if nonempty_str(out_jsonl):
            outs.append({"path": out_jsonl})
        if nonempty_str(canon_jsonl_path):
            outs.append({"path": canon_jsonl_path})
        processing.append(
            {
                "step": "harvest",
                "script": "src/ingest/harvest_capitains_repo.py",
                "run_utc": harvest_run_utc,
                "inputs": clean_paths(ins),
                "outputs": clean_paths(outs),
                "notes": f"Derived from {normalize_relpath(report_relpath)}",
            }
        )

    if not has_step("promote"):
        outputs = clean_paths([{"path": canon_jsonl_path}]) if nonempty_str(canon_jsonl_path) else []
        processing.append(
            {
                "step": "promote",
                "script": "src/ingest/promote_harvest_report.py",
                "run_utc": promote_run_utc,
                "inputs": [{"path": normalize_relpath(report_relpath)}],
                "outputs": outputs,
                "params": batch_params,
            }
        )

    # Patch any existing step objects: ensure a step identifier and run_utc, and never null paths.
    for p in processing:
        if not isinstance(p, dict):
            continue
        if not nonempty_str(p.get("run_utc")):
            p["run_utc"] = promote_run_utc
        if not (nonempty_str(p.get("step")) or nonempty_str(p.get("name")) or nonempty_str(p.get("action"))):
            p["step"] = "unspecified"

        for k in ("inputs", "outputs"):
            lst = p.get(k)
            if isinstance(lst, list):
                p[k] = [x for x in lst if isinstance(x, dict) and nonempty_str(x.get("path"))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Repo root (e.g., .)")
    ap.add_argument("--report", required=True, help="Path to harvest_*.json")
    ap.add_argument("--registry", default="docs/corpora.json")
    ap.add_argument("--prov-dir", default="docs/provenance")
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--include", default=None)
    ap.add_argument("--exclude", default=None)

    # Rare overrides
    ap.add_argument("--tier", default=None)
    ap.add_argument("--license", dest="license_str", default=None)
    ap.add_argument("--distributable", type=int, choices=(0, 1), default=None)
    ap.add_argument("--title", default=None)

    ap.add_argument("--tag", default=None)
    ap.add_argument("--overwrite", action="store_true", help="Rewrite from scratch (else patch-in-place)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    report_path = Path(args.report).resolve()

    registry_path = Path(args.registry)
    if not registry_path.is_absolute():
        registry_path = (root / registry_path).resolve()
    else:
        registry_path = registry_path.resolve()

    prov_dir = Path(args.prov_dir)
    if not prov_dir.is_absolute():
        prov_dir = (root / prov_dir).resolve()
    else:
        prov_dir = prov_dir.resolve()

    registry_map = load_registry_map(registry_path)
    report = read_json(report_path)
    if not isinstance(report, dict):
        raise ValueError("Harvest report must be a JSON object")

    repo_root = nonempty_str(report.get("repo_root"))
    report_generated_utc = nonempty_str(report.get("generated_utc")) or utc_now_iso()

    items = report.get("items")
    if not isinstance(items, list):
        raise ValueError("Harvest report missing items[]")

    inc = re.compile(args.include) if args.include else None
    exc = re.compile(args.exclude) if args.exclude else None

    def match_filters(blob: str) -> bool:
        if inc and not inc.search(blob):
            return False
        if exc and exc.search(blob):
            return False
        return True

    picked: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = nonempty_str(it.get("corpus_id"))
        if not cid:
            continue
        blob = " | ".join(
            x for x in [cid, nonempty_str(it.get("tei")), nonempty_str(it.get("work_id"))] if x
        )
        if match_filters(blob):
            picked.append(it)

    batch = picked[args.offset : args.offset + args.limit]

    report_rel = str(report_path)
    try:
        report_rel = str(report_path.relative_to(root))
    except Exception:
        pass

    batch_params = {
        "offset": args.offset,
        "limit": args.limit,
        "include": args.include,
        "exclude": args.exclude,
        "tag": args.tag,
        "registry": normalize_relpath(str(registry_path.relative_to(root))) if registry_path.is_relative_to(root) else str(registry_path),
    }

    written: List[str] = []
    patched: List[str] = []
    missing_in_registry: List[str] = []

    for it in batch:
        cid = nonempty_str(it.get("corpus_id"))
        if not cid:
            continue

        reg_entry = registry_map.get(cid)
        if not reg_entry:
            missing_in_registry.append(cid)
            continue

        reg_title = nonempty_str(reg_entry.get("title")) or cid
        reg_rights = reg_entry.get("rights") if isinstance(reg_entry.get("rights"), dict) else {}
        reg_tier = nonempty_str(reg_rights.get("tier"))
        reg_license = nonempty_str(reg_rights.get("license"))
        reg_dist = reg_rights.get("distributable")

        title = nonempty_str(args.title) or reg_title
        tier = nonempty_str(args.tier) or reg_tier
        license_str = nonempty_str(args.license_str) or reg_license
        distributable = bool(args.distributable) if args.distributable is not None else bool(reg_dist)

        # Fail-fast: distributable corpora must have complete registry rights.
        if distributable and (tier is None or license_str is None):
            raise RuntimeError(
                f"Registry rights incomplete for distributable corpus_id={cid}. "
                f"Fix {registry_path} rights.tier and rights.license before generating provenance."
            )

        canon_jsonl_path: Optional[str] = None
        cj = reg_entry.get("canon_jsonl")
        if isinstance(cj, dict):
            canon_jsonl_path = nonempty_str(cj.get("path"))

        tei = nonempty_str(it.get("tei"))
        out_jsonl = nonempty_str(it.get("out_jsonl"))
        work_id = nonempty_str(it.get("work_id"))

        out_path = prov_dir / f"{cid}.json"
        exists = out_path.exists()

        # Patch-in-place default: load existing unless overwrite.
        if exists and not args.overwrite:
            prov_any = read_json(out_path)
            prov: Dict[str, Any] = prov_any if isinstance(prov_any, dict) else {}
        else:
            prov = {}

        # Force-match registry fields compared by strict-rights.
        prov["corpus_id"] = cid
        prov["title"] = title

        rights = ensure_dict(prov, "rights")
        rights["tier"] = tier
        rights["license"] = license_str
        rights["distributable"] = bool(distributable)
        rights["notes"] = append_note(
            nonempty_str(rights.get("notes")) or nonempty_str(reg_rights.get("notes")),
            f"Auto-patched from registry+harvest report on {utc_now_iso()}",
        )

        if distributable:
            upsert_required_source(prov, repo_root=repo_root, tei=tei, work_id=work_id)
            upsert_required_processing(
                prov,
                harvest_run_utc=report_generated_utc or utc_now_iso(),
                promote_run_utc=utc_now_iso(),
                report_relpath=report_rel,
                tei=tei,
                out_jsonl=out_jsonl,
                canon_jsonl_path=canon_jsonl_path,
                batch_params=batch_params,
            )
        else:
            ensure_local_only_note(prov)

        # Writing policy to avoid noisy diffs:
        # - existing file + patch-in-place: preserve key order
        # - new file or overwrite: sort keys for deterministic canonical output
        sort_keys = bool(args.overwrite) or (not exists)

        if args.dry_run:
            (patched if exists else written).append(str(out_path))
            continue

        write_json(out_path, prov, sort_keys=sort_keys)
        (patched if (exists and not args.overwrite) else written).append(str(out_path))

    print(
        json.dumps(
            {
                "report": str(report_path),
                "registry": str(registry_path),
                "prov_dir": str(prov_dir),
                "selected": len(batch),
                "written": written,
                "patched": patched,
                "missing_in_registry": missing_in_registry,
                "overwrite": bool(args.overwrite),
                "dry_run": bool(args.dry_run),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())