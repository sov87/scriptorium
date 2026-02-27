#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, shutil, subprocess, sys, time
from pathlib import Path
from typing import Any, Dict, List

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jload(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def jwrite_min(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="db-build config TOML")
    ap.add_argument("--private-registry", default="docs/corpora.private.json")
    ap.add_argument("--prefix", default="rome_", help="Select corpora_id starting with this prefix")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = (root / args.config).resolve()
    priv = (root / args.private_registry).resolve()
    corpora_json = (root / "docs" / "corpora.json").resolve()

    if not cfg.exists():
        raise SystemExit(f"[FATAL] missing config: {cfg}")
    if not priv.exists():
        raise SystemExit(f"[FATAL] missing private registry: {priv}")
    if not corpora_json.exists():
        raise SystemExit(f"[FATAL] missing docs/corpora.json: {corpora_json}")

    reg = jload(priv)
    corpora: List[Dict[str, Any]] = [c for c in reg.get("corpora", []) if isinstance(c, dict)]
    picked = [c for c in corpora if str(c.get("corpus_id","")).startswith(args.prefix)]
    if not picked:
        raise SystemExit(f"[FATAL] no corpora matched prefix {args.prefix!r} in {priv.name}")

    tmp = {"generated_utc": utc_now(), "corpora": picked}
    print(f"[OK] selected corpora={len(picked)} prefix={args.prefix!r}")
    for c in picked[:20]:
        print(" -", c.get("corpus_id"))

    if args.dry_run:
        print("[DRY] not swapping corpora.json / not building DB")
        return 0

    backup = corpora_json.with_suffix(f".json.bak_demo_{args.prefix}_{int(time.time())}")
    shutil.copy2(corpora_json, backup)

    try:
        jwrite_min(corpora_json, tmp)
        print(f"[OK] swapped docs/corpora.json (backup -> {backup.name})")

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