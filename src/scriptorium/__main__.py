from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_config
from .ps_bridge import format_release_window_cmd, run_release_window


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _json_min(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="scriptorium")
    sub = p.add_subparsers(dest="cmd", required=True)

    # release / print-ps
    p_release = sub.add_parser("release")
    p_release.add_argument("--config", required=True)
    p_release.add_argument("--make-subset", action="store_true")
    p_release.add_argument("--rebuild-indexes", action="store_true")
    p_release.add_argument("--run-retrieval", action="store_true")
    p_release.add_argument("--run-machine", action="store_true")
    p_release.add_argument("--snapshot", action="store_true")
    p_release.add_argument("--snapshot-no-canon", action="store_true")
    p_release.add_argument("--skip-ps", action="store_true")
    p_release.add_argument("--print-only", action="store_true")

    p_cmd = sub.add_parser("print-ps")
    p_cmd.add_argument("--config", required=True)
    p_cmd.add_argument("--make-subset", action="store_true")
    p_cmd.add_argument("--rebuild-indexes", action="store_true")
    p_cmd.add_argument("--run-retrieval", action="store_true")
    p_cmd.add_argument("--run-machine", action="store_true")
    p_cmd.add_argument("--snapshot", action="store_true")

    # doctor / paths
    p_doc = sub.add_parser("doctor")
    p_doc.add_argument("--config", required=True)
    p_doc.add_argument("--strict", action="store_true")
    p_doc.add_argument("--json", action="store_true")
    p_doc.add_argument("--llm", action="store_true")

    p_paths = sub.add_parser("paths")
    p_paths.add_argument("--config", required=True)

    # check-ai-fts (AI FTS health)
    p_caf = sub.add_parser("check-ai-fts")
    p_caf.add_argument("--config", required=True)
    p_caf.add_argument("--strict", action="store_true")
    p_caf.add_argument("--json", action="store_true")

    # db-build / db-search
    p_db = sub.add_parser("db-build")
    p_db.add_argument("--config", required=True)
    p_db.add_argument("--out", default="db/scriptorium.sqlite")
    p_db.add_argument("--overwrite", action="store_true")

    p_ds = sub.add_parser("db-search")
    p_ds.add_argument("--config", required=True)
    p_ds.add_argument("--q", required=True)
    p_ds.add_argument("--k", type=int, default=10)
    p_ds.add_argument("--corpus", default="")

    # vec-build / retrieve
    p_vb = sub.add_parser("vec-build")
    p_vb.add_argument("--config", required=True)
    p_vb.add_argument("--out-dir", default="indexes/vec_faiss_global")
    p_vb.add_argument("--batch", type=int, default=256)

    p_r = sub.add_parser("retrieve")
    p_r.add_argument("--config", required=True)
    p_r.add_argument("--q", required=True)
    p_r.add_argument("--k", type=int, default=10)
    p_r.add_argument("--corpus", default="")

    # init-corpus
    p_ic = sub.add_parser("init-corpus")
    p_ic.add_argument("--config", required=True)
    p_ic.add_argument("--id", required=True, dest="corpus_id")
    p_ic.add_argument("--title", required=True)

    # answer-db / answer-batch-db
    p_ad = sub.add_parser("answer-db")
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

    p_abd = sub.add_parser("answer-batch-db")
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

    # gloss-db / gloss-batch-db
    p_gd = sub.add_parser("gloss-db")
    p_gd.add_argument("--config", required=True)
    p_gd.add_argument("--corpus", default="")
    p_gd.add_argument("--ids", dest="ids_path", default="")
    p_gd.add_argument("--limit", type=int, default=0)
    p_gd.add_argument("--out-root", default="data_gen/gloss")
    p_gd.add_argument("--dry-run", action="store_true")
    p_gd.add_argument("--continue", dest="cont", action="store_true")
    p_gd.add_argument("--llm-base-url", default="")
    p_gd.add_argument("--llm-model", default="")
    p_gd.add_argument("--max-tokens", type=int, default=600)
    p_gd.add_argument("--temperature", type=float, default=0.2)

    p_gbd = sub.add_parser("gloss-batch-db")
    p_gbd.add_argument("--config", required=True)
    p_gbd.add_argument("--in", dest="in_path", required=True)
    p_gbd.add_argument("--out-root", default="data_gen/gloss_batch")
    p_gbd.add_argument("--run-id", default="")
    p_gbd.add_argument("--limit", type=int, default=0)
    p_gbd.add_argument("--dry-run", action="store_true")
    p_gbd.add_argument("--continue", dest="cont", action="store_true")
    p_gbd.add_argument("--llm-base-url", default="")
    p_gbd.add_argument("--llm-model", default="")
    p_gbd.add_argument("--max-tokens", type=int, default=600)
    p_gbd.add_argument("--temperature", type=float, default=0.2)

    # AI layer import/search
    p_gi = sub.add_parser("gloss-import-db")
    p_gi.add_argument("--config", required=True)
    p_gi.add_argument("--run-dir", required=True, help="gloss-db run dir containing gloss.jsonl + meta.json")

    p_ai = sub.add_parser("answer-import-db")
    p_ai.add_argument("--config", required=True)
    p_ai.add_argument("--run-dir", required=True, help="answer-db run dir containing answer.json + meta.json")

    p_gs = sub.add_parser("gloss-search")
    p_gs.add_argument("--config", required=True)
    p_gs.add_argument("--q", required=True)
    p_gs.add_argument("--k", type=int, default=10)
    p_gs.add_argument("--corpus", default="")

    p_as = sub.add_parser("answer-search")
    p_as.add_argument("--config", required=True)
    p_as.add_argument("--q", required=True)
    p_as.add_argument("--k", type=int, default=10)
    p_as.add_argument("--corpus", default="")
    p_as.add_argument("--show-cites", action="store_true")

    # catalog
    p_cs = sub.add_parser("catalog-status")
    p_cs.add_argument("--config", required=True)

    p_cf = sub.add_parser("catalog-fetch")
    p_cf.add_argument("--config", required=True)
    p_cf.add_argument("--source-id", action="append", default=[])

    p_ci = sub.add_parser("catalog-ingest")
    p_ci.add_argument("--config", required=True)
    p_ci.add_argument("--corpus-id", action="append", default=[])

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    default_db_path = cfg.project_root / "db" / "scriptorium.sqlite"
    db_path = getattr(cfg, "db_path", default_db_path)
    db_path = db_path.resolve() if hasattr(db_path, "resolve") else default_db_path

    if args.cmd == "doctor":
        from .doctor import run_doctor
        return run_doctor(cfg, strict=args.strict, as_json_out=args.json, check_llm=args.llm)

    if args.cmd == "paths":
        print(f"project_root: {cfg.project_root}")
        print(f"window: {cfg.window}")
        print(f"tag: {cfg.tag}")
        print(f"release_ps1: {cfg.release_ps1}")
        return 0


    if args.cmd == "check-ai-fts":
        import sqlite3
        def _table_exists(con, name: str) -> bool:
            return con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone() is not None

        warnings: list[str] = []
        info: dict[str, int] = {}

        con = sqlite3.connect(str(db_path))
        try:
            # answers
            answers = con.execute("SELECT count(*) FROM answers").fetchone()[0] if _table_exists(con, "answers") else 0
            answers_fts = con.execute("SELECT count(*) FROM answers_fts").fetchone()[0] if _table_exists(con, "answers_fts") else 0
            info["answers"] = int(answers)
            info["answers_fts"] = int(answers_fts)

            if answers > 0 and answers_fts == 0:
                warnings.append("answers_fts is empty but answers has rows (FTS not populated).")
            elif answers > 0 and answers_fts > 0 and answers_fts != answers:
                warnings.append(f"answers_fts rowcount mismatch (answers={answers}, answers_fts={answers_fts}).")

            # glosses
            glosses = con.execute("SELECT count(*) FROM glosses").fetchone()[0] if _table_exists(con, "glosses") else 0
            glosses_fts = con.execute("SELECT count(*) FROM glosses_fts").fetchone()[0] if _table_exists(con, "glosses_fts") else 0
            info["glosses"] = int(glosses)
            info["glosses_fts"] = int(glosses_fts)

            if glosses > 0 and glosses_fts == 0:
                warnings.append("glosses_fts is empty but glosses has rows (FTS not populated).")
            elif glosses > 0 and glosses_fts > 0 and glosses_fts != glosses:
                warnings.append(f"glosses_fts rowcount mismatch (glosses={glosses}, glosses_fts={glosses_fts}).")
        finally:
            con.close()

        ok = len(warnings) == 0
        if args.json:
            print(_json_min({"ok": ok, "warnings": warnings, "db": str(db_path), "counts": info}))
        else:
            print(f"[OK] check-ai-fts db={db_path}")
            for k, v in info.items():
                print(f"  {k}: {v}")
            if warnings:
                print("[WARN] " + " | ".join(warnings))
            else:
                print("[OK] no warnings")
        return 1 if (args.strict and warnings) else 0

    if args.cmd == "db-build":
        script = cfg.project_root / "src" / "build_sqlite_db.py"
        out_arg = args.out
        if out_arg == "db/scriptorium.sqlite":
            out_arg = str(db_path)
        cmd = [sys.executable, str(script), "--root", str(cfg.project_root), "--out", str(out_arg)]
        if args.overwrite:
            cmd.append("--overwrite")
        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "db-search":
        from .db_search_fts import run_db_search
        return run_db_search(db_path, q=args.q, k=args.k, corpus=args.corpus)

    if args.cmd == "vec-build":
        out_dir = (cfg.project_root / args.out_dir).resolve()
        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
        script = cfg.project_root / "src" / "build_vec_index_global.py"
        cmd = [
            sys.executable,
            str(script),
            "--db",
            str(db_path),
            "--out-dir",
            str(out_dir),
            "--model",
            str(model),
            "--batch",
            str(args.batch),
        ]
        if getattr(cfg, "use_e5_prefix", False):
            cmd.append("--use-e5-prefix")
        subprocess.run(cmd, check=True)
        return 0

    if args.cmd == "retrieve":
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()
        model = getattr(cfg, "embed_model", None)
        if model is None:
            model = cfg.project_root / "models" / "multilingual-e5-base"
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
            str(model),
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
        vec_dir = (cfg.project_root / "indexes" / "vec_faiss_global").resolve()
        embed_model = getattr(cfg, "embed_model", None)
        if embed_model is None:
            embed_model = cfg.project_root / "models" / "multilingual-e5-base"

        llm_base = args.llm_base_url.strip() if args.llm_base_url else os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1")
        llm_model = args.llm_model.strip() if args.llm_model else os.getenv("SCRIPTORIUM_LLM_MODEL", "")
        llm_key = os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio")

        run_id = args.run_id.strip() if args.run_id.strip() else (_utc_stamp() + "_batch")
        a = BatchArgs(
            project_root=cfg.project_root,
            db_path=db_path,
            vec_dir=vec_dir,
            embed_model=str(embed_model),
            use_e5_prefix=bool(getattr(cfg, "use_e5_prefix", False)),
            queries_path=(cfg.project_root / args.in_path).resolve() if not Path(args.in_path).is_absolute() else Path(args.in_path),
            out_root=(cfg.project_root / args.out_root).resolve(),
            run_id=run_id,
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

    if args.cmd == "gloss-db":
        from .gloss_db import GlossDbArgs, run_gloss_db
        llm_base = args.llm_base_url.strip() if args.llm_base_url else os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1")
        llm_model = args.llm_model.strip() if args.llm_model else os.getenv("SCRIPTORIUM_LLM_MODEL", "")
        llm_key = os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio")

        ids_path = None
        if args.ids_path.strip():
            p = Path(args.ids_path)
            ids_path = p if p.is_absolute() else (cfg.project_root / p).resolve()

        a = GlossDbArgs(
            db_path=db_path,
            corpus=str(args.corpus),
            ids_path=ids_path,
            out_root=(cfg.project_root / args.out_root).resolve(),
            dry_run=bool(args.dry_run),
            cont=bool(args.cont),
            limit=int(args.limit),
            llm_base_url=str(llm_base),
            llm_model=str(llm_model),
            llm_api_key=str(llm_key),
            max_tokens=int(args.max_tokens),
            temperature=float(args.temperature),
        )
        run_gloss_db(a)
        return 0

    if args.cmd == "gloss-batch-db":
        from .gloss_batch_db import BatchArgs, run_gloss_batch_db
        llm_base = args.llm_base_url.strip() if args.llm_base_url else os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1")
        llm_model = args.llm_model.strip() if args.llm_model else os.getenv("SCRIPTORIUM_LLM_MODEL", "")
        llm_key = os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio")

        run_id = args.run_id.strip() if args.run_id.strip() else (_utc_stamp() + "_batch")
        in_path = Path(args.in_path)
        items_path = in_path if in_path.is_absolute() else (cfg.project_root / in_path).resolve()

        a = BatchArgs(
            project_root=cfg.project_root,
            db_path=db_path,
            out_root=(cfg.project_root / args.out_root).resolve(),
            run_id=run_id,
            dry_run=bool(args.dry_run),
            cont=bool(args.cont),
            limit=int(args.limit),
            llm_base_url=str(llm_base),
            llm_model=str(llm_model),
            llm_api_key=str(llm_key),
            max_tokens=int(args.max_tokens),
            temperature=float(args.temperature),
            items_path=items_path,
        )
        run_gloss_batch_db(a)
        return 0

    if args.cmd == "gloss-import-db":
        from .ai_layers_db import import_gloss_run
        run_dir = Path(args.run_dir)
        run_dir = run_dir if run_dir.is_absolute() else (cfg.project_root / run_dir).resolve()
        res = import_gloss_run(db_path, run_dir)
        print(_json_min(res))
        return 0

    if args.cmd == "answer-import-db":
        from .ai_layers_db import import_answer_run
        import sqlite3
        run_dir = Path(args.run_dir)
        run_dir = run_dir if run_dir.is_absolute() else (cfg.project_root / run_dir).resolve()
        res = import_answer_run(db_path, run_dir)

        # Hardening: ensure answers_fts stays consistent for this run_id
        run_id = str(res.get("run_id", "")).strip()
        if run_id:
            con = sqlite3.connect(str(db_path))
            try:
                # Only act if answers_fts exists (schema is expected to create it)
                exists = con.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='answers_fts'"
                ).fetchone() is not None
                if exists:
                    row = con.execute(
                        "SELECT query, answer FROM answers WHERE run_id=?",
                        (run_id,),
                    ).fetchone()
                    if row is not None:
                        q, a = row[0], row[1]
                        con.execute("DELETE FROM answers_fts WHERE run_id=?", (run_id,))
                        con.execute(
                            "INSERT INTO answers_fts(run_id, query, answer) VALUES (?,?,?)",
                            (run_id, q, a),
                        )
                        con.commit()
            finally:
                con.close()

        print(_json_min(res))
        return 0

    if args.cmd == "gloss-search":
        from .ai_layers_db import gloss_search
        rows = gloss_search(db_path, q=args.q, k=int(args.k), corpus=str(args.corpus))
        if not rows:
            print(f"[OK] 0 hits (q={args.q!r}, corpus={args.corpus!r})")
            return 0
        print(f"[OK] hits={len(rows)} (q={args.q!r}, corpus={args.corpus!r})")
        for r in rows:
            print(f"{r['corpus_id']}	{r['id']}	{r['work_id']}	{r['loc']}")
            print(f"  gloss: {r['gloss']}")
            if r.get("literal"):
                print(f"  literal: {r['literal']}")
        return 0

    if args.cmd == "answer-search":
        from .ai_layers_db import answer_search
        rows = answer_search(db_path, q=args.q, k=int(args.k), corpus=str(args.corpus), show_cites=bool(getattr(args, "show_cites", False)))
        if not rows:
            print(f"[OK] 0 hits (q={args.q!r}, corpus={args.corpus!r})")
            return 0
        print(f"[OK] hits={len(rows)} (q={args.q!r}, corpus={args.corpus!r})")
        for r in rows:
            score = r.get("score")
            score_s = f"{score:.4f}" if isinstance(score, float) else ("" if score is None else str(score))
            print(f"{r.get('run_id','')}\t{r.get('corpus_filter','')}\t{score_s}")
            print(f"  q: {r.get('query_snip','')}")
            print(f"  a: {r.get('answer_snip','')}")
            if getattr(args, "show_cites", False):
                cites = r.get("cites")
                if isinstance(cites, list) and cites:
                    print("  cites: " + ", ".join(str(x) for x in cites))
        return 0

    if args.cmd == "catalog-status":
        from .catalog_ops import run_catalog_status
        return run_catalog_status(cfg.project_root)

    if args.cmd == "catalog-fetch":
        from .catalog_ops import run_catalog_fetch
        src_ids = [x for x in (args.source_id or []) if x]
        run_catalog_fetch(cfg.project_root, source_ids=src_ids if src_ids else None)
        return 0

    if args.cmd == "catalog-ingest":
        from .catalog_ops import run_catalog_ingest
        cids = [x for x in (args.corpus_id or []) if x]
        return run_catalog_ingest(cfg.project_root, corpus_ids=cids if cids else None)

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
        ret = 0 if skip_ps else run_release_window(
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
