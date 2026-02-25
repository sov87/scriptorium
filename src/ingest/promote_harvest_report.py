# File: src/ingest/promote_harvest_report.py
# Purpose: Promote harvested items from a harvest report into docs/corpora.json in controlled batches.
# Safety invariants:
# - Filtering options avoid accidental massive promotion.
# - Deterministic upsert + registry sort keeps stable order.

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from scriptorium.registry_upsert import upsert_corpus


def _load_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(p.read_text(encoding="utf-8-sig"))


def _compile(pat: str) -> Optional[re.Pattern]:
    pat = (pat or "").strip()
    if not pat:
        return None
    return re.compile(pat)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--report", required=True, help="runs/harvest/harvest_*.json")
    ap.add_argument("--registry", default="docs/corpora.json")
    ap.add_argument("--license", required=True)
    ap.add_argument("--tier", default="A_open_license")
    ap.add_argument("--distributable", type=int, default=1)
    ap.add_argument("--limit", type=int, default=100, help="How many items to promote (after filtering)")
    ap.add_argument("--offset", type=int, default=0, help="Start index into filtered items")
    ap.add_argument("--tag", default="harvest", help="Short source tag for title suffix")

    # Filters
    ap.add_argument("--include-corpus-regex", default="", help="Only promote items whose corpus_id matches")
    ap.add_argument("--exclude-corpus-regex", default="", help="Skip items whose corpus_id matches")
    ap.add_argument("--include-tei-regex", default="", help="Only promote items whose tei path matches")
    ap.add_argument("--exclude-tei-regex", default="", help="Skip items whose tei path matches")
    ap.add_argument("--include-work-regex", default="", help="Only promote items whose work_id matches")
    ap.add_argument("--exclude-work-regex", default="", help="Skip items whose work_id matches")
    ap.add_argument("--dry-run", action="store_true", help="Show counts and the first few corpus_ids, but do not modify registry")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    report_abs = Path(args.report)
    if not report_abs.is_absolute():
        report_abs = (root / report_abs).resolve()

    data = _load_json(report_abs)
    items: List[Dict[str, Any]] = data.get("items", [])
    if not items:
        raise SystemExit(f"[ERR] no items in report: {report_abs}")

    inc_c = _compile(args.include_corpus_regex)
    exc_c = _compile(args.exclude_corpus_regex)
    inc_t = _compile(args.include_tei_regex)
    exc_t = _compile(args.exclude_tei_regex)
    inc_w = _compile(args.include_work_regex)
    exc_w = _compile(args.exclude_work_regex)

    filtered: List[Dict[str, Any]] = []
    for it in items:
        corpus_id = str(it.get("corpus_id", ""))
        tei = str(it.get("tei", ""))
        work_id = str(it.get("work_id", "") or "")
        if inc_c and not inc_c.search(corpus_id):
            continue
        if exc_c and exc_c.search(corpus_id):
            continue
        if inc_t and not inc_t.search(tei):
            continue
        if exc_t and exc_t.search(tei):
            continue
        if inc_w and not inc_w.search(work_id):
            continue
        if exc_w and exc_w.search(work_id):
            continue
        filtered.append(it)

    off = int(args.offset)
    lim = int(args.limit)
    if lim <= 0:
        raise SystemExit("[ERR] --limit must be > 0")
    if off < 0 or off > len(filtered):
        raise SystemExit(f"[ERR] --offset out of range: offset={off} total_filtered={len(filtered)}")

    slice_items = filtered[off:off + lim]

    if args.dry_run:
        print(f"[DRY] report_total={len(items)} filtered={len(filtered)} offset={off} limit={lim} promoting={len(slice_items)}")
        for x in slice_items[:10]:
            print(" ", x.get("corpus_id"))
        return 0

    promoted = 0
    for it in slice_items:
        corpus_id = str(it["corpus_id"])
        out_jsonl = Path(str(it["out_jsonl"]))
        if not out_jsonl.is_absolute():
            out_jsonl = (root / out_jsonl).resolve()
        if not out_jsonl.exists():
            raise SystemExit(f"[ERR] missing out_jsonl on disk: {out_jsonl}")

        title = str(it.get("work_id") or f"[Capitains {corpus_id}]")
        title = f"{title} [{args.tag}]"

        upsert_corpus(
            project_root=root,
            registry_rel=str(args.registry),
            corpus_id=corpus_id,
            title=title,
            canon_jsonl_abs=out_jsonl,
            tier=str(args.tier),
            license_str=str(args.license),
            distributable=bool(int(args.distributable)),
        )
        promoted += 1

    print(f"[OK] promoted={promoted} report_total={len(items)} filtered={len(filtered)} offset={off} limit={lim} report={report_abs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
