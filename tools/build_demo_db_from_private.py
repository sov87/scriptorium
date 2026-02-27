#!/usr/bin/env python3
"""
Build a Scriptorium DB from a subset of the private registry (prefix-filtered),
without touching docs/corpora.json. Replaces the old swap-and-restore pattern.
"""
from __future__ import annotations
import argparse, json, subprocess, sys, tempfile
from pathlib import Path
from typing import Any, Dict, List
import time


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jload(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def jwrite_min(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build a Scriptorium DB from a prefix-filtered subset of the private registry, "
                    "without swapping docs/corpora.json."
    )
    ap.add_argument("--config", required=True, help="db-build config TOML")
    ap.add_argument("--private-registry", default="docs/corpora.private.json")
    ap.add_argument("--prefix", default="rome_", help="Select corpora_id starting with this prefix")
    ap.add_argument("--out", default="", help="Override output DB path")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = (root / args.config).resolve()
    priv = (root / args.private_registry).resolve()

    if not cfg.exists():
        raise SystemExit(f"[FATAL] missing config: {cfg}")
    if not priv.exists():
        raise SystemExit(f"[FATAL] missing private registry: {priv}")

    reg = jload(priv)
    corpora: List[Dict[str, Any]] = [c for c in reg.get("corpora", []) if isinstance(c, dict)]
    picked = [c for c in corpora if str(c.get("corpus_id", "")).startswith(args.prefix)]
    if not picked:
        raise SystemExit(f"[FATAL] no corpora matched prefix {args.prefix!r} in {priv.name}")

    tmp_reg = {"generated_utc": utc_now(), "corpora": picked}
    print(f"[OK] selected corpora={len(picked)} prefix={args.prefix!r}")
    for c in picked[:20]:
        print(" -", c.get("corpus_id"))

    if args.dry_run:
        print("[DRY] not building DB (--dry-run)")
        return 0

    # Write subset registry to a temp file — never touches docs/corpora.json
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix=f"subset_registry_{args.prefix}_",
        dir=root / "docs",
        delete=False,
        encoding="utf-8",
        newline="\n",
    ) as tf:
        tf.write(json.dumps(tmp_reg, ensure_ascii=False, separators=(",", ":")))
        tmp_path = Path(tf.name)

    try:
        cmd = [
            sys.executable, "-m", "scriptorium", "db-build",
            "--config", str(cfg),
            "--registry-override", str(tmp_path),
            "--overwrite",
        ]
        if args.out:
            cmd += ["--out", args.out]
        print(f"[OK] subset registry written to: {tmp_path.name} (temp; will be deleted)")
        print("[RUN]", " ".join(cmd))
        subprocess.run(cmd, cwd=str(root), check=True)
        print("[OK] db-build completed (docs/corpora.json was NOT modified)")
        return 0
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
            print(f"[OK] removed temp registry: {tmp_path.name}")


if __name__ == "__main__":
    raise SystemExit(main())
