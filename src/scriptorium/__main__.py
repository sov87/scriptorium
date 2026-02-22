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
    p_release.add_argument("--config", required=True)
    p_release.add_argument("--make-subset", action="store_true")
    p_release.add_argument("--rebuild-indexes", action="store_true")
    p_release.add_argument("--run-retrieval", action="store_true")
    p_release.add_argument("--run-machine", action="store_true")
    p_release.add_argument("--snapshot", action="store_true")
    p_release.add_argument("--snapshot-no-canon", action="store_true")
    p_release.add_argument("--skip-ps", action="store_true")
    p_release.add_argument("--print-only", action="store_true")

    p_cmd = sub.add_parser("print-ps", help="Print the PowerShell command that would be executed.")
    p_cmd.add_argument("--config", required=True)
    p_cmd.add_argument("--make-subset", action="store_true")
    p_cmd.add_argument("--rebuild-indexes", action="store_true")
    p_cmd.add_argument("--run-retrieval", action="store_true")
    p_cmd.add_argument("--run-machine", action="store_true")
    p_cmd.add_argument("--snapshot", action="store_true")

    # doctor
    p_doc = sub.add_parser("doctor", help="Validate expected files/deps from config.")
    p_doc.add_argument("--config", required=True)
    p_doc.add_argument("--strict", action="store_true")
    p_doc.add_argument("--json", action="store_true")
    p_doc.add_argument("--llm", action="store_true")

    # paths
    p_paths = sub.add_parser("paths", help="Show resolved key paths from config.")
    p_paths.add_argument("--config", required=True)

    # db-build
    p_db = sub.add_parser("db-build", help="Build derived SQLite DB from canon JSONL.")
    p_db.add_argument("--config", required=True)
    p_db.add_argument("--out", default="db/scriptorium.sqlite")
    p_db.add_argument("--overwrite", action="store_true")

    # db-search
    p_ds = sub.add_parser("db-search", help="Full-text search across all corpora (SQLite FTS5).")
    p_ds.add_argument("--config", required=True)
    p_ds.add_argument("--q", required=True)
    p_ds.add_argument("--k", type=int, default=10)
    p_ds.add_argument("--corpus", default="")

    # vec-build
    p_vb = sub.add_parser("vec-build", help="Build global FAISS index over segments.")
    p_vb.add_argument("--config", required=True)
    p_vb.add_argument("--out-dir", default="indexes/vec_faiss_global")
    p_vb.add_argument("--batch", type=int, default=256)

    # retrieve
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

    # answer-db
    p_ad = sub.add_parser("answer-db", help="Hybrid retrieve then answer via local LLM (LM Studio).")
    p_ad.add_argument("--config", required=True)
    p_ad.add_argument("--q", required=True)
    p_ad.add_argument("--k", type=int, default=10)
    p_ad.add_argument("--fts-k", type=int, default=50)
    p_ad.add_argument("--vec-k", type=int, default=50)
    p_ad.add_argument("--corpus", default="")
    p_ad.add_argument("--out-root", default="runs/answer_db")
    p_ad.add_argument("--dry-run", action="store_true")
    p_ad.add_argument("--llm-base-url", default="")
    p_ad.add_argument("--llm-model", default="")
    p_ad.add_argument("--max-tokens", type=int, default=900)
    p_ad.add_argument("--temperature", type=float, default=0.2)

    # answer-batch-db
    p_abd = sub.add_parser("answer-batch-db", help="Run multiple answer-db queries from a file (one per line).")
    p_abd.add_argument("--config", required=True)
    p_abd.add_argument("--in", dest="in_path", required=True)
    p_abd.add_argument("--out-root", default="runs/answer_batch_db")
    p_abd.add_argument("--run-id", default="")
    p_abd.add_argument("--k", type=int, default=10)
    p_abd.add_argument("--fts-k", type=int, default=50)
    p_abd.add_argument("--vec-k", type=int, default=50)
    p_abd.add_argument("--corpus", default="")
    p_abd.add_argument("--dry-run", action="store_true")
    p_abd.add_argument("--continue", dest="cont", action="store_true")
    p_abd.add_argument("--llm-base-url", default="")
    p_abd.add_argument("--llm-model", default="")
    p_abd.add_argument("--max-tokens", type=int, default=900)
    p_abd.add_argument("--temperature", type=float, default=0.2)

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
        import subprocess, sys
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
        import subprocess, sys
        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        out_dir = (cfg.project_root / args.out_dir).resolve()
        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
        script = cfg.project_root / "src" / "build_vec_index_global.py"
        cmd = [
            sys.executable, str(script),
            "--db", str(db_path),
            "--out-dir", str(out_dir),
            "--model", str(model),
            "--batch", str(args.batch),
        ]
        if getattr(cfg, "use_e5_prefix", False):
            cmd.append("--use-e5-prefix")
        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "retrieve":
        import subprocess, sys
        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()
        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
        script = cfg.project_root / "src" / "retrieve_hybrid.py"
        cmd = [
            sys.executable, str(script),
            "--db", str(db_path),
            "--q", args.q,
            "--k", str(args.k),
            "--vec-dir", str(vec_dir),
            "--model", str(model),
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

    if args.cmd == "answer-db":
        from .answer_db import AnswerDbArgs, run_answer_db
        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()
        embed_model = getattr(cfg, "embed_model", None)
        if embed_model is None:
            embed_model = cfg.project_root / "models" / "multilingual-e5-base"
        llm_base = args.llm_base_url.strip() if args.llm_base_url else os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1")
        llm_model = args.llm_model.strip() if args.llm_model else os.getenv("SCRIPTORIUM_LLM_MODEL", "")
        llm_key = os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio")
        a = AnswerDbArgs(
            db_path=db_path,
            vec_dir=vec_dir,
            embed_model=str(embed_model),
            use_e5_prefix=bool(getattr(cfg, "use_e5_prefix", False)),
            query=args.q,
            k=int(args.k),
            fts_k=int(args.fts_k),
            vec_k=int(args.vec_k),
            corpus=str(args.corpus),
            out_root=(cfg.project_root / args.out_root).resolve(),
            dry_run=bool(args.dry_run),
            llm_base_url=str(llm_base),
            llm_model=str(llm_model),
            llm_api_key=str(llm_key),
            max_tokens=int(args.max_tokens),
            temperature=float(args.temperature),
        )
        run_answer_db(a)
        return 0

    if args.cmd == "answer-batch-db":
        from .answer_batch_db import BatchArgs, run_answer_batch_db
        db_path = cfg.project_root / "db" / "scriptorium.sqlite"
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()
        embed_model = getattr(cfg, "embed_model", None)
        if embed_model is None:
            embed_model = cfg.project_root / "models" / "multilingual-e5-base"
        llm_base = args.llm_base_url.strip() if args.llm_base_url else os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1")
        llm_model = args.llm_model.strip() if args.llm_model else os.getenv("SCRIPTORIUM_LLM_MODEL", "")
        llm_key = os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio")
        run_id = args.run_id.strip()

        a = BatchArgs(
            project_root=cfg.project_root,
            db_path=db_path,
            vec_dir=vec_dir,
            embed_model=str(embed_model),
            use_e5_prefix=bool(getattr(cfg, "use_e5_prefix", False)),
            queries_path=(cfg.project_root / args.in_path).resolve() if not Path(args.in_path).is_absolute() else Path(args.in_path),
            out_root=(cfg.project_root / args.out_root).resolve(),
            run_id=run_id if run_id else (datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_batch"),
            k=int(args.k),
            fts_k=int(args.fts_k),
            vec_k=int(args.vec_k),
            corpus=str(args.corpus),
            dry_run=bool(args.dry_run),
            cont=bool(args.cont),
            llm_base_url=str(llm_base),
            llm_model=str(llm_model),
            llm_api_key=str(llm_key),
            max_tokens=int(args.max_tokens),
            temperature=float(args.temperature),
        )
        run_answer_batch_db(a)
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
        if args.cmd == "print-ps" or args.print_only:
            print(cmd_str)
            return 0

        skip_ps = args.skip_ps or (os.getenv("SCRIPTORIUM_SKIP_PS") == "1")
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
                include_canon=(not args.snapshot_no_canon),
                include_extra=["docs/RIGHTS_LEDGER.md", "docs/PROVENANCE_TEMPLATE.json"],
            )
            print(str(zip_path))
        return ret

    raise SystemExit("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
