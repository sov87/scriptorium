#!/usr/bin/env python3
from __future__ import annotations
import argparse, shutil, subprocess, sys, time
from pathlib import Path

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--registry", required=True, help="Registry JSON to temporarily use as docs/corpora.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = (root / args.config).resolve()
    reg = (root / args.registry).resolve()
    corpora_json = (root / "docs" / "corpora.json").resolve()

    if not cfg.exists():
        raise SystemExit(f"[FATAL] missing config: {cfg}")
    if not reg.exists():
        raise SystemExit(f"[FATAL] missing registry: {reg}")
    if not corpora_json.exists():
        raise SystemExit(f"[FATAL] missing docs/corpora.json: {corpora_json}")

    backup = corpora_json.with_suffix(f".json.bak_registry_{int(time.time())}")
    shutil.copy2(corpora_json, backup)

    try:
        shutil.copy2(reg, corpora_json)
        print(f"[OK] swapped docs/corpora.json <- {reg.name} (backup {backup.name})")
        cmd = [sys.executable, "-m", "scriptorium", "db-build", "--config", str(cfg), "--overwrite"]
        print("[RUN]", " ".join(cmd))
        subprocess.run(cmd, cwd=str(root), check=True)
        print("[OK] db-build completed")
        return 0
    finally:
        shutil.copy2(backup, corpora_json)
        print("[OK] restored docs/corpora.json")

if __name__ == "__main__":
    raise SystemExit(main())