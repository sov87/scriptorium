from __future__ import annotations

import argparse, json
from pathlib import Path

REQUIRED = ("id","corpus_id","lang","text")

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    args = ap.parse_args()

    p = Path(args.in_path)
    total = 0
    dict_lines = 0
    ok = 0
    missing_counts = {k: 0 for k in REQUIRED}
    sample_missing = []

    for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        total += 1
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        dict_lines += 1
        missing = [k for k in REQUIRED if k not in obj or obj.get(k) in (None,"")]
        if not missing:
            ok += 1
        else:
            for k in missing:
                missing_counts[k] += 1
            if len(sample_missing) < 8:
                sample_missing.append((i, missing, list(obj.keys())[:12]))

    print(f"[LINT] file={p}")
    print(f"[LINT] total_lines={total} dict_lines={dict_lines} ok_records={ok}")
    for k in REQUIRED:
        print(f"[LINT] missing_{k}={missing_counts[k]}")
    if sample_missing:
        print("[LINT] sample_missing:")
        for (ln, miss, keys) in sample_missing:
            print(f"  line {ln}: missing={miss} keys~={keys}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
