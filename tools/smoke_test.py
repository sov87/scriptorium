#!/usr/bin/env python3
from __future__ import annotations

import argparse
import platform
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], *, cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(cwd), check=check)


def newest_dir(parent: Path, prefix: str) -> Path | None:
    if not parent.exists():
        return None
    cands = [p for p in parent.iterdir() if p.is_dir() and p.name.startswith(prefix)]
    if not cands:
        return None
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]


def normalize_vec_bundle(vec_dir: Path) -> None:
    """
    Ensure we have a stable bundle named:
      oe_bede_prod.index / oe_bede_prod_ids.json / oe_bede_prod_meta.jsonl

    If build_vec produced a different base name, copy the newest bundle to oe_bede_prod.*
    """
    if not vec_dir.exists():
        return
    idx = sorted(vec_dir.glob("*.index"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not idx:
        return
    idx = idx[0]
    base = idx.stem
    ids = vec_dir / f"{base}_ids.json"
    meta = vec_dir / f"{base}_meta.jsonl"
    if not ids.exists() or not meta.exists():
        return

    tgt_idx = vec_dir / "oe_bede_prod.index"
    tgt_ids = vec_dir / "oe_bede_prod_ids.json"
    tgt_meta = vec_dir / "oe_bede_prod_meta.jsonl"

    if base != "oe_bede_prod":
        tgt_idx.write_bytes(idx.read_bytes())
        tgt_ids.write_bytes(ids.read_bytes())
        tgt_meta.write_bytes(meta.read_bytes())


def main() -> int:
    ap = argparse.ArgumentParser(prog="smoke_test")
    ap.add_argument("--config", default="configs/sample_demo.toml")
    ap.add_argument("--query", default="humility and pride")
    ap.add_argument("--skip-index", action="store_true")
    ap.add_argument("--skip-doctor", action="store_true")
    ap.add_argument("--skip-query", action="store_true")
    ap.add_argument("--skip-answer", action="store_true")
    ap.add_argument("--compileall", action="store_true")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = (repo_root / args.config).resolve()

    if args.compileall:
        run([sys.executable, "-m", "compileall", "-q", "src"], cwd=repo_root)

    try:
        from scriptorium.config import load_config  # type: ignore
    except Exception as e:
        print("ERR: failed to import scriptorium. Did you run `pip install -e .`?", file=sys.stderr)
        print(f"DETAILS: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    cfg = load_config(cfg_path)

    # 1) Bootstrap: build indexes first (so doctor doesn't fail on first run)
    if not args.skip_index:
        cfg.bm25_path.parent.mkdir(parents=True, exist_ok=True)
        cfg.vec_dir.mkdir(parents=True, exist_ok=True)

        bm25_ok = cfg.bm25_path.exists()
        vec_ok = (
            (cfg.vec_dir / "oe_bede_prod.index").exists()
            and (cfg.vec_dir / "oe_bede_prod_ids.json").exists()
            and (cfg.vec_dir / "oe_bede_prod_meta.jsonl").exists()
        )

        if not bm25_ok:
            run(
                [sys.executable, "src/build_bm25_bede_prod.py", "--in", str(cfg.bede_canon), "--out", str(cfg.bm25_path)],
                cwd=repo_root,
            )

        if not vec_ok:
            cmd = [
                sys.executable,
                "src/build_vec_bede_faiss.py",
                "--in",
                str(cfg.bede_canon),
                "--out_dir",
                str(cfg.vec_dir),
                "--model",
                str(cfg.embed_model),
                "--batch",
                "16",
            ]
            if cfg.use_e5_prefix:
                cmd.append("--use_e5_prefix")
            run(cmd, cwd=repo_root)

        normalize_vec_bundle(cfg.vec_dir)

    # 2) Doctor after bootstrap
    if not args.skip_doctor:
        p = run(
            [sys.executable, "-m", "scriptorium", "doctor", "--config", str(cfg_path), "--json"],
            cwd=repo_root,
            check=False,
        )
        if p.returncode != 0:
            return p.returncode

    # 3) Query (retrieval)
    if not args.skip_query:
        run([sys.executable, "-m", "scriptorium", "query", "--config", str(cfg_path), "--text", args.query], cwd=repo_root)

        latest_q = newest_dir(cfg.query_out_parent, "q_")
        if latest_q is None or not (latest_q / "candidates.jsonl").exists():
            print("ERR: expected candidates.jsonl not found under query output parent.", file=sys.stderr)
            return 3
        print(f"[OK] query candidates: {latest_q / 'candidates.jsonl'}")

    # 4) Answer dry-run (retrieval-only)
    if not args.skip_answer:
        run(
            [sys.executable, "-m", "scriptorium", "answer", "--config", str(cfg_path), "--text", args.query, "--dry-run"],
            cwd=repo_root,
        )

    print("[OK] smoke test complete")
    print(f"      platform: {platform.platform()}")
    print(f"      python:   {sys.version.split()[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())