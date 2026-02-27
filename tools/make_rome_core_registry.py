#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, time, re
from pathlib import Path
from typing import Any, Dict, List, Tuple

ANCHORS = [
  "rome","roma","roman","forum","forum romanum","palatine","capitoline","subura","tiber",
  "campus martius","via sacra","curia","rostra","comitium","circus maximus","janiculum",
  "transtiberim","portico","basilica","temple","aedes","templum","saturn","jupiter"
]

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jload(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def score_jsonl(path: Path, sample_lines: int) -> Tuple[int,int]:
    rec_with = 0
    hits = 0
    anchors = [a.lower() for a in ANCHORS]
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for i,line in enumerate(f):
                if i >= sample_lines:
                    break
                line=line.strip()
                if not line:
                    continue
                try:
                    obj=json.loads(line)
                except Exception:
                    continue
                txt=str(obj.get("txt","") or obj.get("text","") or "")
                t=txt.lower()
                local_hits=0
                for a in anchors:
                    if a and a in t:
                        local_hits += t.count(a)
                if local_hits>0:
                    rec_with += 1
                    hits += local_hits
    except FileNotFoundError:
        return (0,0)
    return (rec_with, hits)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--private-registry", default="docs/corpora.private.json")
    ap.add_argument("--out", default="docs/registry_rome_core.generated.json")
    ap.add_argument("--sample-lines", type=int, default=200)
    ap.add_argument("--min-records", type=int, default=6)
    ap.add_argument("--max-corpora", type=int, default=250)
    ap.add_argument("--include-prefix", default="rome_", help="Always include corpora_id starting with this prefix")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    reg_path = (root / args.private_registry).resolve()
    out_path = (root / args.out).resolve()

    reg = jload(reg_path)
    corpora = [c for c in reg.get("corpora", []) if isinstance(c, dict) and c.get("corpus_id")]

    scored = []
    always = []

    for c in corpora:
        cid = str(c["corpus_id"])
        if args.include_prefix and cid.startswith(args.include_prefix):
            always.append(c)
            continue
        canon = c.get("canon_jsonl", {})
        rel = canon.get("path")
        if not rel:
            continue
        fp = (root / Path(rel)).resolve()
        rec_with, hits = score_jsonl(fp, args.sample_lines)
        if rec_with >= args.min_records:
            scored.append((rec_with, hits, c))

    scored.sort(key=lambda x: (-x[0], -x[1]))

    picked = []
    seen = set()

    for c in always:
        cid=str(c["corpus_id"])
        if cid not in seen:
            picked.append(c); seen.add(cid)

    for rec_with, hits, c in scored:
        if len(picked) >= args.max_corpora:
            break
        cid=str(c["corpus_id"])
        if cid in seen:
            continue
        picked.append(c); seen.add(cid)

    out = {"generated_utc": utc_now(), "corpora": picked}
    jwrite_min(out_path, out)

    print(f"[OK] wrote rome-core registry -> {out_path}")
    print(f"[OK] selected corpora={len(picked)} (always={len(always)} scored={len(scored)})")
    print("[TOP 10 scored]:")
    for rec_with, hits, c in scored[:10]:
        print(f"  {rec_with:>4} recs  {hits:>5} hits  {c.get('corpus_id')}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())