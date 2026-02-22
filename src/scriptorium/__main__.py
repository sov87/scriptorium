from __future__ import annotations

import argparse
import os
import pathlib
from pathlib import Path

from .config import load_config
from .ps_bridge import run_release_window, format_release_window_cmd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="scriptorium")
    sub = p.add_subparsers(dest="cmd", required=True)

    # release / print-ps
    p_release = sub.add_parser("release", help="Run release pipeline via release_window.ps1 using a TOML config.")
    p_release.add_argument("--config", required=True, help="Path to TOML config (e.g., configs/window_0597_0865.toml)")
    p_release.add_argument("--make-subset", action="store_true")
    p_release.add_argument("--rebuild-indexes", action="store_true")
    p_release.add_argument("--run-retrieval", action="store_true")
    p_release.add_argument("--run-machine", action="store_true")
    p_release.add_argument("--snapshot", action="store_true")
    p_release.add_argument(
        "--snapshot-no-canon",
        action="store_true",
        help="When --snapshot, exclude canon JSONL files from the snapshot bundle.",
    )
    p_release.add_argument(
        "--skip-ps",
        action="store_true",
        help="Skip PowerShell release_window.ps1 (snapshot-only / debugging). Also enabled by env SCRIPTORIUM_SKIP_PS=1.",
    )
    p_release.add_argument("--print-only", action="store_true", help="Print the PowerShell command but do not run it.")

    p_cmd = sub.add_parser("print-ps", help="Print the PowerShell command that would be executed.")
    p_cmd.add_argument("--config", required=True)
    p_cmd.add_argument("--make-subset", action="store_true")
    p_cmd.add_argument("--rebuild-indexes", action="store_true")
    p_cmd.add_argument("--run-retrieval", action="store_true")
    p_cmd.add_argument("--run-machine", action="store_true")
    p_cmd.add_argument("--snapshot", action="store_true")

    # doctor
    p_doc = sub.add_parser("doctor", help="Validate expected files/deps from config (no retrieval math).")
    p_doc.add_argument("--config", required=True)
    p_doc.add_argument("--strict", action="store_true", help="Treat warnings as failures.")
    p_doc.add_argument("--json", action="store_true", help="Print machine-readable JSON report.")
    p_doc.add_argument("--llm", action="store_true", help="Also check LLM /v1/models reachability.")

    # paths
    p_paths = sub.add_parser("paths", help="Show resolved key paths from config.")
    p_paths.add_argument("--config", required=True)

    # db-build
    p_db = sub.add_parser("db-build", help="Build derived SQLite DB from canon JSONL.")
    p_db.add_argument("--config", required=True)
    p_db.add_argument("--out", default="db/scriptorium.sqlite")
    p_db.add_argument("--overwrite", action="store_true")

    # db-search (FTS)
    p_ds = sub.add_parser("db-search", help="Full-text search across all corpora (SQLite FTS5).")
    p_ds.add_argument("--config", required=True)
    p_ds.add_argument("--q", required=True)
    p_ds.add_argument("--k", type=int, default=10)
    p_ds.add_argument("--corpus", default="")

    # vec-build (global)
    p_vb = sub.add_parser("vec-build", help="Build global FAISS index over segments.")
    p_vb.add_argument("--config", required=True)
    p_vb.add_argument("--out-dir", default="indexes/vec_faiss_global")
    p_vb.add_argument("--batch", type=int, default=256)

    # retrieve (hybrid)
    p_r = sub.add_parser("retrieve", help="Hybrid retrieval (FTS + FAISS + RRF).")
    p_r.add_argument("--config", required=True)
    p_r.add_argument("--q", required=True)
    p_r.add_argument("--k", type=int, default=10)
    p_r.add_argument("--corpus", default="")

    # init-corpus
    p_ic = sub.add_parser("init-corpus", help="Create skeleton files for a new corpus_id and register it.")
    p_ic.add_argument("--config", required=True)
    p_ic.add_argument("--id", required=True, dest="corpus_id")
    p_ic.add_argument("--title", required=True)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)

    if args.cmd == "doctor":
        from .doctor import run_doctor
        return run_doctor(cfg, strict=args.strict, as_json_out=args.json, check_llm=args.llm)

    if args.cmd == "paths":
        print(f"project_root: {cfg.project_root}")
        print(f"window: {cfg.window}")
        print(f"tag: {cfg.tag}")
        print(f"release_ps1: {cfg.release_ps1}")
        return 0

    if args.cmd == "db-build":
        import subprocess
        import sys

        script = cfg.project_root / "src" / "build_sqlite_db.py"
        cmd = [sys.executable, str(script), "--root", str(cfg.project_root), "--out", args.out]
        if args.overwrite:
            cmd.append("--overwrite")
        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "db-search":
        from .db_search_fts import run_db_search

        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        return run_db_search(db_path, q=args.q, k=args.k, corpus=args.corpus)

    if args.cmd == "vec-build":
        import subprocess
        import sys

        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        out_dir = (cfg.project_root / args.out_dir).resolve()

        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
        model = str(model)

        script = cfg.project_root / "src" / "build_vec_index_global.py"
        cmd = [
            sys.executable,
            str(script),
            "--db",
            str(db_path),
            "--out-dir",
            str(out_dir),
            "--model",
            model,
            "--batch",
            str(args.batch),
        ]
        if getattr(cfg, "use_e5_prefix", False):
            cmd.append("--use-e5-prefix")

        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "retrieve":
        import subprocess
        import sys

        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()

        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
        model = str(model)

        script = cfg.project_root / "src" / "retrieve_hybrid.py"
        cmd = [
            sys.executable,
            str(script),
            "--db",
            str(db_path),
            "--q",
            args.q,
            "--k",
            str(args.k),
            "--vec-dir",
            str(vec_dir),
            "--model",
            model,
        ]
        if getattr(cfg, "use_e5_prefix", False):
            cmd.append("--use-e5-prefix")
        if args.corpus:
            cmd += ["--corpus", args.corpus]

        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "init-corpus":
        from .init_corpus import init_corpus

        paths = init_corpus(cfg.project_root, corpus_id=args.corpus_id, title=args.title)
        print(str(paths.provenance))
        print(str(paths.sources))
        print(str(paths.ingest_stub))
        return 0

    if args.cmd in ("print-ps", "release"):
        cmd_str = format_release_window_cmd(
            ps1_path=cfg.release_ps1,
            window=cfg.window,
            tag=cfg.tag,
            make_subset=args.make_subset,
            rebuild_indexes=args.rebuild_indexes,
            run_retrieval=args.run_retrieval,
            run_machine=args.run_machine,
            snapshot=args.snapshot,
        )

        if args.cmd == "print-ps" or getattr(args, "print_only", False):
            print(cmd_str)
            return 0

        skip_ps = getattr(args, "skip_ps", False) or (os.getenv("SCRIPTORIUM_SKIP_PS") == "1")
        if skip_ps:
            ret = 0
        else:
            ret = run_release_window(
                ps1_path=cfg.release_ps1,
                window=cfg.window,
                tag=cfg.tag,
                make_subset=args.make_subset,
                rebuild_indexes=args.rebuild_indexes,
                run_retrieval=args.run_retrieval,
                run_machine=args.run_machine,
                snapshot=args.snapshot,
            )

        if args.snapshot and ret == 0:
            from .snapshot_bundle import build_snapshot_bundle

            zip_path = build_snapshot_bundle(
                project_root=cfg.project_root,
                window=str(cfg.window),
                tag=str(cfg.tag),
                config_path=pathlib.Path(args.config),
                include_canon=(not getattr(args, "snapshot_no_canon", False)),
                include_extra=[
                    "docs/RIGHTS_LEDGER.md",
                    "docs/PROVENANCE_TEMPLATE.json",
                ],
            )
            print(str(zip_path))

        return ret

    raise SystemExit("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
