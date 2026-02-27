#!/usr/bin/env python3
"""
build_demo_teutoburg_db.py

Automates the confusing parts:
1) Builds a minimal demo registry JSON from docs/corpora.private.json for selected corpus_ids
   - verifies canon_jsonl.path exists
   - writes correct sha256 (disk) into the *demo* registry
2) Temporarily swaps docs/corpora.json -> demo registry for the duration of db-build
   (because scriptorium db-build in this repo is registry-path hardwired to docs/corpora.json)
3) Runs: python -m scriptorium db-build --config ... --out ... --overwrite --strict-provenance --strict-rights
4) Restores the original docs/corpora.json even if db-build fails.

USAGE (PowerShell, from repo root):
  python .\src\tools\build_demo_teutoburg_db.py `
    --config .\configs\window_0597_0865.toml `
    --db-out .\db\demo_teutoburg.sqlite `
    --corpus-id local_kalkriese_demo `
    --corpus-id local_dio_teutoburg_txt `
    --corpus-id local_velleius_varus_txt `
    --overwrite
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def read_json_bomsafe(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def load_registry_map(registry_path: Path) -> Dict[str, Dict[str, Any]]:
    obj = read_json_bomsafe(registry_path)
    corpora = obj.get("corpora")
    if not isinstance(corpora, list):
        raise SystemExit(f"Registry missing corpora[]: {registry_path}")
    m: Dict[str, Dict[str, Any]] = {}
    for c in corpora:
        if isinstance(c, dict) and isinstance(c.get("corpus_id"), str) and c["corpus_id"].strip():
            cid = c["corpus_id"].strip()
            m[cid] = c
    return m


def resolve_path(root: Path, p: str) -> Path:
    pp = Path(p)
    if pp.is_absolute():
        return pp
    return (root / pp).resolve()


def build_demo_registry(*, root: Path, private_registry: Path, corpus_ids: List[str], out_path: Path) -> Dict[str, Any]:
    priv = load_registry_map(private_registry)
    missing = [cid for cid in corpus_ids if cid not in priv]
    if missing:
        raise SystemExit(
            "These corpus_id(s) are not present in docs/corpora.private.json:\n  - "
            + "\n  - ".join(missing)
        )

    corpora_out: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for cid in corpus_ids:
        entry = json.loads(json.dumps(priv[cid]))  # deep copy
        canon = entry.get("canon_jsonl") if isinstance(entry.get("canon_jsonl"), dict) else None
        if not canon or not isinstance(canon.get("path"), str) or not canon["path"].strip():
            raise SystemExit(f"{cid}: missing canon_jsonl.path in private registry entry")

        canon_path = canon["path"]
        abs_jsonl = resolve_path(root, canon_path)
        if not abs_jsonl.exists():
            raise SystemExit(f"{cid}: canon_jsonl.path not found on disk: {abs_jsonl}")

        disk_sha = sha256_file(abs_jsonl)
        reg_sha = (canon.get("sha256") or "").strip().upper()
        if reg_sha and reg_sha != disk_sha:
            warnings.append(f"{cid}: sha256 mismatch in private registry; using disk sha in demo registry")
        canon["sha256"] = disk_sha  # always correct in demo registry

        rights = entry.get("rights") if isinstance(entry.get("rights"), dict) else {}
        if "distributable" not in rights:
            rights["distributable"] = False
        entry["rights"] = rights

        corpora_out.append(entry)

    out_obj = {"corpora": corpora_out}
    write_json(out_path, out_obj)
    return {"ok": True, "out": str(out_path), "warnings": warnings, "count": len(corpora_out)}


def swap_file(src: Path, dst: Path) -> None:
    dst.write_bytes(src.read_bytes())


def run_db_build(*, root: Path, config: Path, db_out: Path, overwrite: bool, strict_provenance: bool, strict_rights: bool) -> None:
    cmd = [sys.executable, "-m", "scriptorium", "db-build", "--config", str(config), "--out", str(db_out)]
    if overwrite:
        cmd.append("--overwrite")
    if strict_provenance:
        cmd.append("--strict-provenance")
    if strict_rights:
        cmd.append("--strict-rights")

    p = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, encoding="utf-8", errors="replace")
    sys.stdout.write(p.stdout)
    sys.stderr.write(p.stderr)
    if p.returncode != 0:
        raise SystemExit(f"db-build failed with exit code {p.returncode}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--config", required=True, help="Config TOML for db-build")
    ap.add_argument("--db-out", required=True, help="Output sqlite path for demo db")
    ap.add_argument("--private-registry", default="docs/corpora.private.json")
    ap.add_argument("--demo-registry-out", default="docs/corpora.demo_teutoburg.local.json")
    ap.add_argument("--corpus-id", action="append", required=True, help="Corpus ID to include (repeatable)")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-backup", action="store_true", help="Keep docs/corpora.json backup after success")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    config = resolve_path(root, args.config)
    db_out = resolve_path(root, args.db_out)
    private_registry = resolve_path(root, args.private_registry)
    demo_registry_out = resolve_path(root, args.demo_registry_out)

    if not config.exists():
        raise SystemExit(f"--config not found: {config}")
    if not private_registry.exists():
        raise SystemExit(f"Private registry not found: {private_registry}")

    corpus_ids = [c.strip() for c in args.corpus_id if c and c.strip()]
    if not corpus_ids:
        raise SystemExit("No --corpus-id specified")

    print("== Build demo registry ==")
    if args.dry_run:
        print(f"[dry-run] would build: {demo_registry_out}")
    else:
        info = build_demo_registry(
            root=root,
            private_registry=private_registry,
            corpus_ids=corpus_ids,
            out_path=demo_registry_out,
        )
        print(json.dumps(info, indent=2, ensure_ascii=False))

    corpora_json = resolve_path(root, "docs/corpora.json")
    if not corpora_json.exists():
        raise SystemExit(f"Expected to exist: {corpora_json}")

    backup = resolve_path(root, f"docs/corpora.json.__bak_demo_teutoburg__{utc_stamp()}")

    print("\n== Swap docs/corpora.json (temporary) ==")
    print("original:", corpora_json)
    print("backup  :", backup)
    print("demo    :", demo_registry_out)

    if args.dry_run:
        print("[dry-run] would copy original -> backup, demo -> docs/corpora.json, run db-build, then restore.")
        return 0

    swap_file(corpora_json, backup)
    try:
        swap_file(demo_registry_out, corpora_json)

        print("\n== Run db-build ==")
        db_out.parent.mkdir(parents=True, exist_ok=True)
        run_db_build(
            root=root,
            config=config,
            db_out=db_out,
            overwrite=bool(args.overwrite),
            strict_provenance=True,
            strict_rights=True,
        )
    finally:
        swap_file(backup, corpora_json)
        if (not args.keep_backup) and backup.exists():
            try:
                backup.unlink()
            except Exception:
                pass

    print("\n== Done ==")
    print("demo db:", db_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
