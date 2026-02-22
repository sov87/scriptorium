from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .answer_db import AnswerDbArgs, run_answer_db, slug


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_queries(p: Path) -> list[str]:
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    out: list[str] = []
    for s in lines:
        s = s.strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        out.append(s)
    return out


def has_completed_run(qroot: Path) -> bool:
    # We consider a query "done" if any descendant contains answer.json and validation.json with ok=true.
    if not qroot.exists():
        return False
    for ans in qroot.rglob("answer.json"):
        v = ans.parent / "validation.json"
        if not v.exists():
            continue
        try:
            j = json.loads(v.read_text(encoding="utf-8"))
            if j.get("ok") is True:
                return True
        except Exception:
            continue
    return False


@dataclass
class BatchArgs:
    project_root: Path
    db_path: Path
    vec_dir: Path
    embed_model: str
    use_e5_prefix: bool
    queries_path: Path
    out_root: Path
    run_id: str
    k: int
    fts_k: int
    vec_k: int
    corpus: str
    dry_run: bool
    cont: bool
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    max_tokens: int
    temperature: float


def run_answer_batch_db(a: BatchArgs) -> Path:
    queries = read_queries(a.queries_path)
    if not queries:
        raise SystemExit(f"No queries found in: {a.queries_path}")

    batch_dir = (a.out_root / a.run_id).resolve()
    batch_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "schema": "scriptorium.answer_batch_db.v1",
        "generated_utc": utc_iso(),
        "run_id": a.run_id,
        "queries_path": str(a.queries_path),
        "count": len(queries),
        "k": a.k,
        "fts_k": a.fts_k,
        "vec_k": a.vec_k,
        "corpus": a.corpus,
        "dry_run": a.dry_run,
        "continue": a.cont,
        "llm_base_url": a.llm_base_url,
        "llm_model": a.llm_model,
        "results": [],
    }

    tsv_lines = ["idx\tstatus\tquery_root\trun_dir\terror\tquery"]

    for idx, q in enumerate(queries, start=1):
        qroot = batch_dir / f"q{idx:04d}_{slug(q)}"
        qroot.mkdir(parents=True, exist_ok=True)

        if a.cont and has_completed_run(qroot):
            summary["results"].append(
                {"idx": idx, "status": "skipped_done", "query": q, "query_root": str(qroot), "run_dir": ""}
            )
            tsv_lines.append(f"{idx}\tskipped_done\t{qroot}\t\t\t{q}")
            continue

        try:
            ad = AnswerDbArgs(
                db_path=a.db_path,
                vec_dir=a.vec_dir,
                embed_model=a.embed_model,
                use_e5_prefix=a.use_e5_prefix,
                query=q,
                k=a.k,
                fts_k=a.fts_k,
                vec_k=a.vec_k,
                corpus=a.corpus,
                out_root=qroot,
                dry_run=a.dry_run,
                llm_base_url=a.llm_base_url,
                llm_model=a.llm_model,
                llm_api_key=a.llm_api_key,
                max_tokens=a.max_tokens,
                temperature=a.temperature,
            )
            run_dir = run_answer_db(ad)
            summary["results"].append(
                {"idx": idx, "status": "ok", "query": q, "query_root": str(qroot), "run_dir": str(run_dir)}
            )
            tsv_lines.append(f"{idx}\tok\t{qroot}\t{run_dir}\t\t{q}")
        except SystemExit as e:
            err = str(e)
            summary["results"].append(
                {"idx": idx, "status": "error", "query": q, "query_root": str(qroot), "run_dir": "", "error": err}
            )
            tsv_lines.append(f"{idx}\terror\t{qroot}\t\t{err}\t{q}")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            summary["results"].append(
                {"idx": idx, "status": "error", "query": q, "query_root": str(qroot), "run_dir": "", "error": err}
            )
            tsv_lines.append(f"{idx}\terror\t{qroot}\t\t{err}\t{q}")

        # Avoid same-second collisions in answer_db run_id
        time.sleep(1.0)

    (batch_dir / "batch_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (batch_dir / "batch_summary.tsv").write_text("\n".join(tsv_lines) + "\n", encoding="utf-8")

    print(str(batch_dir))
    return batch_dir


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--vec-dir", required=True)
    ap.add_argument("--embed-model", required=True)
    ap.add_argument("--use-e5-prefix", action="store_true")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out-root", default="runs/answer_batch_db")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--fts-k", type=int, default=50)
    ap.add_argument("--vec-k", type=int, default=50)
    ap.add_argument("--corpus", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--continue", dest="cont", action="store_true")
    ap.add_argument("--llm-base-url", default=os.getenv("SCRIPTORIUM_LLM_BASE_URL", "http://localhost:1234/v1"))
    ap.add_argument("--llm-model", default=os.getenv("SCRIPTORIUM_LLM_MODEL", ""))
    ap.add_argument("--llm-api-key", default=os.getenv("SCRIPTORIUM_LLM_API_KEY", "lm-studio"))
    ap.add_argument("--max-tokens", type=int, default=900)
    ap.add_argument("--temperature", type=float, default=0.2)
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_root = Path(args.out_root)
    if not out_root.is_absolute():
        out_root = (root / out_root).resolve()

    run_id = args.run_id.strip() if args.run_id.strip() else utc_stamp() + "_batch"

    a = BatchArgs(
        project_root=root,
        db_path=Path(args.db),
        vec_dir=Path(args.vec_dir),
        embed_model=str(args.embed_model),
        use_e5_prefix=bool(args.use_e5_prefix),
        queries_path=Path(args.in_path) if Path(args.in_path).is_absolute() else (root / args.in_path).resolve(),
        out_root=out_root,
        run_id=run_id,
        k=int(args.k),
        fts_k=int(args.fts_k),
        vec_k=int(args.vec_k),
        corpus=str(args.corpus),
        dry_run=bool(args.dry_run),
        cont=bool(args.cont),
        llm_base_url=str(args.llm_base_url),
        llm_model=str(args.llm_model),
        llm_api_key=str(args.llm_api_key),
        max_tokens=int(args.max_tokens),
        temperature=float(args.temperature),
    )
    run_answer_batch_db(a)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
