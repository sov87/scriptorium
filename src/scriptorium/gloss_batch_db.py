from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .gloss_db import GlossDbArgs, run_gloss_db, slug


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_items(p: Path) -> list[str]:
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    out: list[str] = []
    for s in lines:
        s = s.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


@dataclass
class BatchArgs:
    project_root: Path
    db_path: Path
    out_root: Path
    run_id: str
    dry_run: bool
    cont: bool
    limit: int
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    max_tokens: int
    temperature: float
    items_path: Path  # file containing corpus_ids (one per line)


def run_gloss_batch_db(a: BatchArgs) -> Path:
    items = read_items(a.items_path)
    if not items:
        raise SystemExit(f"No corpus_ids found in: {a.items_path}")

    batch_dir = (a.out_root / a.run_id).resolve()
    batch_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "schema": "scriptorium.gloss_batch_db.v1",
        "generated_utc": utc_iso(),
        "run_id": a.run_id,
        "items_path": str(a.items_path),
        "count": len(items),
        "limit_per_corpus": int(a.limit),
        "dry_run": bool(a.dry_run),
        "continue": bool(a.cont),
        "llm_base_url": a.llm_base_url,
        "llm_model": a.llm_model,
        "results": [],
    }

    tsv_lines = ["idx\tstatus\tcorpus_id\trun_dir\terror"]
    for idx, corpus_id in enumerate(items, start=1):
        # Each corpus gets its own run root under the batch dir
        croot = batch_dir / f"c{idx:03d}_{slug(corpus_id)}"
        croot.mkdir(parents=True, exist_ok=True)
        try:
            gd = GlossDbArgs(
                db_path=a.db_path,
                corpus=corpus_id,
                ids_path=None,
                out_root=croot,
                dry_run=a.dry_run,
                cont=a.cont,
                limit=a.limit,
                llm_base_url=a.llm_base_url,
                llm_model=a.llm_model,
                llm_api_key=a.llm_api_key,
                max_tokens=a.max_tokens,
                temperature=a.temperature,
            )
            run_dir = run_gloss_db(gd)
            summary["results"].append(
                {"idx": idx, "status": "ok", "corpus_id": corpus_id, "run_dir": str(run_dir)}
            )
            tsv_lines.append(f"{idx}\tok\t{corpus_id}\t{run_dir}\t")
        except SystemExit as e:
            err = str(e)
            summary["results"].append(
                {"idx": idx, "status": "error", "corpus_id": corpus_id, "run_dir": "", "error": err}
            )
            tsv_lines.append(f"{idx}\terror\t{corpus_id}\t\t{err}")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            summary["results"].append(
                {"idx": idx, "status": "error", "corpus_id": corpus_id, "run_dir": "", "error": err}
            )
            tsv_lines.append(f"{idx}\terror\t{corpus_id}\t\t{err}")

        # avoid same-second collisions in run ids
        time.sleep(1.0)

    (batch_dir / "batch_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (batch_dir / "batch_summary.tsv").write_text("\n".join(tsv_lines) + "\n", encoding="utf-8")
    print(str(batch_dir))
    return batch_dir
