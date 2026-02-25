# File: src/ingest/harvest_capitains_repo.py
# Purpose: Harvest Capitains-style TEI repos (canonical-greekLit / canonical-latinLit) into canonical JSONL
#          and optionally upsert docs/corpora.json.
#
# Safety invariants:
# - Default is FAIL-SAFE: does not overwrite existing out_jsonl unless --overwrite is set.
# - Can continue past bad TEI files with --continue-on-error (records errors in report).
# - Can harvest without touching registry with --no-upsert (records items in report).

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from scriptorium.ingest.tei_cts import (
    iter_segment_drafts,
    parse_tei,
    parse_work_id,
    sanitize_local_id_from_loc,
)
from scriptorium.registry_upsert import upsert_corpus


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _minijson(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _seq_id(i: int) -> str:
    return f"{i:06d}"


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _safe_id(s: str) -> str:
    s = s.lower().replace(".", "_").replace("-", "_")
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


@dataclass(frozen=True)
class HarvestItem:
    corpus_id: str
    tei_path: str
    out_jsonl: str
    sha256: str
    work_id: Optional[str]


def ingest_tei_to_canon_jsonl(
    *,
    tei_path: Path,
    corpus_id: str,
    out_path: Path,
    use_milestones: bool,
    lang: Optional[str],
    with_src_sha256: bool,
) -> HarvestItem:
    tree = parse_tei(str(tei_path))
    work_id = parse_work_id(tree)

    src_meta: Dict[str, Any] = {"path": tei_path.as_posix()}
    if with_src_sha256:
        src_meta["sha256"] = _sha256_file(tei_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        i = 0
        for sd in iter_segment_drafts(tree, use_milestones=bool(use_milestones)):
            i += 1
            if sd.loc:
                local_id = sanitize_local_id_from_loc(sd.loc) or _seq_id(i)
            else:
                local_id = _seq_id(i)

            # invariant: local_id must not contain ':'
            local_id = local_id.replace(":", "_")

            meta = sd.meta or {}
            meta.setdefault("source", src_meta)
            if lang:
                meta.setdefault("lang", lang)

            rec = {
                "corpus_id": corpus_id,
                "local_id": local_id,
                "id": f"{corpus_id}:{local_id}",
                "work_id": work_id,
                "loc": sd.loc,
                "text": sd.text,
                "meta": meta,
            }
            f.write(_minijson(rec) + "\n")

    sha = _sha256_file(out_path)
    return HarvestItem(
        corpus_id=corpus_id,
        tei_path=str(tei_path),
        out_jsonl=str(out_path),
        sha256=sha,
        work_id=work_id,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--repo-root", required=True, help="Capitains repo root (clone)")
    ap.add_argument("--out-dir", default="data_proc", help="Canonical JSONL output dir (usually untracked)")
    ap.add_argument("--registry", default="docs/corpora.json", help="Registry to upsert (tracked)")
    ap.add_argument("--prefix", default="grc", help="Corpus prefix (e.g., grc or lat)")
    ap.add_argument("--lang", default=None, help="Optional lang tag (e.g., grc/lat)")
    ap.add_argument("--license", required=True)
    ap.add_argument("--tier", default="A_open_license")
    ap.add_argument("--distributable", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0, help="0=all; else first N TEI files")
    ap.add_argument("--textgroup", default="", help="Optional: only under data/<textgroup>/")
    ap.add_argument("--work", default="", help="Optional: only under data/<textgroup>/<work>/")
    ap.add_argument("--only-stem", default="", help="Optional: ingest only TEI with this filename stem")
    ap.add_argument("--use-milestones", action="store_true", help="Enable milestone segmentation when available")
    ap.add_argument("--with-src-sha256", action="store_true", help="Include TEI sha256 in per-segment meta.source")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-upsert", action="store_true", help="Harvest JSONL + report but do not modify registry")
    ap.add_argument("--continue-on-error", action="store_true", help="Skip TEI files that fail ingest; record error in report")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing out_jsonl (default: skip existing)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    repo_root = Path(args.repo_root)
    repo_abs = repo_root if repo_root.is_absolute() else (root / repo_root).resolve()

    data_dir = repo_abs / "data"
    if not data_dir.exists():
        raise SystemExit(f"[ERR] missing data/ under repo-root: {repo_abs}")

    base = data_dir
    if args.textgroup.strip():
        base = base / args.textgroup.strip()
    if args.work.strip():
        base = base / args.work.strip()

    excludes = {"__cts__.xml", "__capitains__.xml"}
    tei_files = sorted(p for p in base.rglob("*.xml") if p.name.lower() not in excludes)

    if args.only_stem.strip():
        wanted = args.only_stem.strip()
        tei_files = [p for p in tei_files if p.stem == wanted]

    if not tei_files:
        raise SystemExit(f"[ERR] no TEI files found under: {base}")

    if args.limit and args.limit > 0:
        tei_files = tei_files[: int(args.limit)]

    out_dir = Path(args.out_dir)
    out_abs = out_dir if out_dir.is_absolute() else (root / out_dir).resolve()

    items: List[HarvestItem] = []
    errors: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []

    prefix = _safe_id(str(args.prefix).strip())
    if not prefix:
        raise SystemExit("[ERR] --prefix must be non-empty")

    for tei in tei_files:
        stem = tei.stem
        corpus_id = f"{prefix}_{_safe_id(stem)}"
        if ":" in corpus_id:
            raise SystemExit(f"[ERR] derived corpus_id contains ':': {corpus_id}")

        out_path = out_abs / f"{corpus_id}_prod.jsonl"

        if args.dry_run:
            print("[DRY]", tei.as_posix(), "->", out_path.as_posix())
            continue

        if out_path.exists() and not args.overwrite:
            skipped.append({"tei": tei.as_posix(), "out_jsonl": out_path.as_posix(), "reason": "exists"})
            continue

        try:
            it = ingest_tei_to_canon_jsonl(
                tei_path=tei,
                corpus_id=corpus_id,
                out_path=out_path,
                use_milestones=bool(args.use_milestones),
                lang=(str(args.lang) if args.lang else None),
                with_src_sha256=bool(args.with_src_sha256),
            )
        except Exception as e:
            errors.append({"tei": tei.as_posix(), "error": f"{type(e).__name__}: {e}"})
            if args.continue_on_error:
                continue
            raise

        items.append(it)

        if not args.no_upsert:
            title = it.work_id if it.work_id else f"[Capitains {stem}]"
            title = f"{title} [{repo_abs.name} {stem}]"
            upsert_corpus(
                project_root=root,
                registry_rel=str(args.registry),
                corpus_id=it.corpus_id,
                title=title,
                canon_jsonl_abs=Path(it.out_jsonl),
                tier=str(args.tier),
                license_str=str(args.license),
                distributable=bool(int(args.distributable)),
            )

    if args.dry_run:
        return 0

    report = {
        "schema": "scriptorium.harvest_report.v1",
        "generated_utc": _utc_iso(),
        "repo_root": str(repo_abs),
        "base": str(base),
        "count": len(items),
        "skipped": skipped,
        "errors": errors,
        "items": [
            {
                "corpus_id": it.corpus_id,
                "tei": it.tei_path,
                "out_jsonl": it.out_jsonl,
                "sha256": it.sha256,
                "work_id": it.work_id,
            }
            for it in items
        ],
    }
    rep_dir = (root / "runs" / "harvest").resolve()
    rep_dir.mkdir(parents=True, exist_ok=True)
    rep_path = rep_dir / f"harvest_{_utc_stamp()}.json"
    rep_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(str(rep_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
