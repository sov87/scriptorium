from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from .answer_local import run_answer
from .config import Config


def _read_lines(p: Path) -> list[str]:
    lines: list[str] = []
    for raw in p.read_text(encoding="utf-8").splitlines():
        q = raw.strip()
        if not q or q.startswith("#"):
            continue
        lines.append(q)
    return lines


def _qid(q: str) -> str:
    # stable across runs; normalize whitespace
    norm = " ".join(q.split())
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]


def run_answer_batch(
    cfg: Config,
    *,
    in_path: Path,
    out_dir: Path | None,
    topk: int | None,
    bm25_k: int | None,
    vec_k: int | None,
    k_passages: int | None,
    dry_run: bool,
    cont: bool = False,
    config_path: Path | None = None,
) -> Path:
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = out_dir or (cfg.answer_out_parent / f"batch_{stamp}")
    run_dir.mkdir(parents=True, exist_ok=True)

    queries = _read_lines(in_path)

    # batch.json: complete manifest (inputs + resolved settings)
    batch_manifest = {
        "schema": "scriptorium.answer_batch.v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "input_file": str(in_path),
        "count": len(queries),
        "config_path": str(config_path) if config_path else None,
        "resolved": {
            "project_root": str(cfg.project_root),
            "bm25_path": str(cfg.bm25_path),
            "vec_dir": str(cfg.vec_dir),
            "embed_model": cfg.embed_model,
            "use_e5_prefix": cfg.use_e5_prefix,
            "llm_base_url": None if dry_run else cfg.llm_base_url,
            "llm_model": None if dry_run else cfg.llm_model,
        },
        "params": {
            "topk": topk,
            "bm25_k": bm25_k,
            "vec_k": vec_k,
            "k_passages": k_passages,
            "dry_run": dry_run,
            "continue": cont,
        },
    }
    (run_dir / "batch.json").write_text(
        json.dumps(batch_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    results_path = run_dir / "results.jsonl"
    mode = "a" if (cont and results_path.exists()) else "w"

    total = len(queries)
    ok = 0
    failed = 0
    skipped = 0
    failures: list[dict] = []

    # For duplicate identical queries in the same batch
    seen: dict[str, int] = {}

    with results_path.open(mode, encoding="utf-8") as f:
        for i, q in enumerate(queries, 1):
            qid = _qid(q)
            seen[qid] = seen.get(qid, 0) + 1
            qid_full = qid if seen[qid] == 1 else f"{qid}_{seen[qid]}"
            q_dir = run_dir / f"q_{qid_full}"

            base = {
                "i": i,
                "qid": qid,
                "qid_full": qid_full,
                "query": q,
                "dir": str(q_dir),
            }

            # --continue skip logic
            if cont:
                marker = (q_dir / "retrieval" / "candidates.jsonl") if dry_run else (q_dir / "answer.json")
                if marker.exists():
                    rec = {**base, "ok": True, "skipped": True, "out": str(marker)}
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    f.flush()
                    skipped += 1
                    continue

            try:
                out = run_answer(
                    cfg,
                    query_text=q,
                    out_dir=q_dir,
                    topk=topk,
                    bm25_k=bm25_k,
                    vec_k=vec_k,
                    k_passages=k_passages,
                    dry_run=dry_run,
                )
                rec = {**base, "ok": True, "skipped": False, "out": str(out)}
                ok += 1
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                rec = {**base, "ok": False, "skipped": False, "error": err}
                failed += 1
                failures.append({"i": i, "qid": qid, "qid_full": qid_full, "query": q, "error": err})

            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()

    summary = {
        "schema": "scriptorium.answer_batch_summary.v1",
        "run_dir": str(run_dir),
        "total": total,
        "ok": ok,
        "failed": failed,
        "skipped": skipped,
        "failures": failures,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return run_dir