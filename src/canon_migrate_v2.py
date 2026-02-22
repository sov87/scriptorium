from __future__ import annotations

import argparse, json
from pathlib import Path
from typing import Any

ID_KEYS_DEFAULT = ["id","rid","rec_id","record_id","uid","key"]
TEXT_KEYS_DEFAULT = ["text","oe","oe_text","ms_text","content","line","raw","txt"]
LOC_KEYS_DEFAULT = ["loc","ref","citation","passage"]

def first_str(d: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--lang", default="ang")
    ap.add_argument("--id-keys", default=",".join(ID_KEYS_DEFAULT))
    ap.add_argument("--text-keys", default=",".join(TEXT_KEYS_DEFAULT))
    ap.add_argument("--loc-keys", default=",".join(LOC_KEYS_DEFAULT))
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    id_keys = [x.strip() for x in args.id_keys.split(",") if x.strip()]
    text_keys = [x.strip() for x in args.text_keys.split(",") if x.strip()]
    loc_keys = [x.strip() for x in args.loc_keys.split(",") if x.strip()]

    total = 0
    wrote = 0
    skipped_no_id = 0
    skipped_not_dict = 0

    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8", newline="\n") as fout:
        for ln, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                skipped_not_dict += 1
                continue

            rid = first_str(obj, id_keys)
            if not rid:
                skipped_no_id += 1
                continue

            # Start from original record (do not drop fields), then add/normalize required fields
            rec = dict(obj)

            rec["id"] = rid
            rec["corpus_id"] = args.corpus_id
            rec.setdefault("lang", args.lang)

            txt = first_str(rec, text_keys)
            if txt is None:
                txt = ""
            rec["text"] = txt

            # loc is optional, but keep if we can find one
            if "loc" not in rec or rec.get("loc") in (None, ""):
                loc = first_str(rec, loc_keys)
                if loc:
                    rec["loc"] = loc

            # Ensure standard optional fields exist (keeps downstream simple)
            rec.setdefault("work_id", None)
            rec.setdefault("witness_id", None)
            rec.setdefault("edition_id", None)
            rec.setdefault("text_norm", None)
            rec.setdefault("source_refs", [])
            rec.setdefault("notes", [])

            fout.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            wrote += 1

    print(str(out_path))
    print(f"[OK] total_lines={total} wrote={wrote} skipped_no_id={skipped_no_id} skipped_not_dict={skipped_not_dict}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
