#!/usr/bin/env python3
"""
Build a Scriptorium DB from an explicit registry file, without touching
docs/corpora.json. Replaces the old swap-and-restore pattern.
"""
from __future__ import annotations
import argparse, subprocess, sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build a Scriptorium DB using a specific registry JSON, "
                    "without swapping docs/corpora.json."
    )
    ap.add_argument("--config", required=True, help="db-build config TOML (relative to repo root)")
    ap.add_argument("--registry", required=True, help="Registry JSON to use (relative to repo root, or absolute)")
    ap.add_argument("--overwrite", action="store_true", default=True, help="Overwrite existing DB (default: true)")
    ap.add_argument("--no-overwrite", dest="overwrite", action="store_false")
    ap.add_argument("--out", default="", help="Override output DB path")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = (root / args.config).resolve()
    reg = Path(args.registry)
    reg = reg if reg.is_absolute() else (root / reg).resolve()

    if not cfg.exists():
        raise SystemExit(f"[FATAL] missing config: {cfg}")
    if not reg.exists():
        raise SystemExit(f"[FATAL] missing registry: {reg}")

    cmd = [
        sys.executable, "-m", "scriptorium", "db-build",
        "--config", str(cfg),
        "--registry-override", str(reg),
    ]
    if args.overwrite:
        cmd.append("--overwrite")
    if args.out:
        cmd += ["--out", args.out]

    print(f"[OK] using registry: {reg}")
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, cwd=str(root), check=True)
    print("[OK] db-build completed (docs/corpora.json was NOT modified)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
