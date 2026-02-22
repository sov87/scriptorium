import argparse
import json
import sys
from pathlib import Path

BAD_SEQ = ("�", "Ã", "Â", "â€™", "â€œ", "â€", "\uFFFD")

def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield ln, json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"[FATAL] Invalid JSON at {path}:{ln}: {e}")

def pick_text(rec: dict, who: str, ln: int):
    for k in ("txt", "text", "canon_text", "content", "oe_text", "latin_text", "txt_raw"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return k, v
    raise SystemExit(f"[FATAL] {who} record missing text-ish field at line {ln}; has keys={sorted(rec.keys())}")

def has_mojibake(s: str) -> bool:
    return any(x in s for x in BAD_SEQ)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asc", required=True)
    ap.add_argument("--bede", required=True)
    ap.add_argument("--machine", required=True)
    ap.add_argument("--max_gloss", type=int, default=600)
    ap.add_argument("--max_translation", type=int, default=900)
    ap.add_argument("--max_note", type=int, default=1200)
    args = ap.parse_args()

    asc_path = Path(args.asc)
    bede_path = Path(args.bede)
    machine_path = Path(args.machine)

    # Load ASC canon
    asc = {}
    asc_year = {}
    for ln, rec in iter_jsonl(asc_path):
        rid = rec.get("id")
        if not isinstance(rid, str) or not rid.strip():
            raise SystemExit(f"[FATAL] ASC missing string id at {asc_path}:{ln}")
        if rid in asc:
            raise SystemExit(f"[FATAL] Duplicate ASC id={rid} at {asc_path}:{ln}")
        _, txt = pick_text(rec, "ASC", ln)
        asc[rid] = rec
        y = rec.get("year")
        asc_year[rid] = y

        if has_mojibake(txt):
            raise SystemExit(f"[FATAL] Mojibake in ASC text for id={rid} at {asc_path}:{ln}")

    # Load BEDE canon
    bede = {}
    for ln, rec in iter_jsonl(bede_path):
        rid = rec.get("id")
        if not isinstance(rid, str) or not rid.strip():
            raise SystemExit(f"[FATAL] BEDE missing string id at {bede_path}:{ln}")
        if rid in bede:
            raise SystemExit(f"[FATAL] Duplicate BEDE id={rid} at {bede_path}:{ln}")
        _, txt = pick_text(rec, "BEDE", ln)
        bede[rid] = rec

        if has_mojibake(txt):
            raise SystemExit(f"[FATAL] Mojibake in BEDE text for id={rid} at {bede_path}:{ln}")

    # Validate machine layer records against canon
    n = 0
    for ln, rec in iter_jsonl(machine_path):
        n += 1
        asc_id = rec.get("asc_id")
        if not isinstance(asc_id, str) or not asc_id.strip():
            raise SystemExit(f"[FATAL] machine missing asc_id at {machine_path}:{ln}")
        if asc_id not in asc:
            raise SystemExit(f"[FATAL] machine asc_id not found in ASC canon: {asc_id} at {machine_path}:{ln}")

        sel = rec.get("selected_bede_ids")
        if not isinstance(sel, list) or not all(isinstance(x, str) and x.strip() for x in sel):
            raise SystemExit(f"[FATAL] machine selected_bede_ids must be list[str] at {machine_path}:{ln}")
        for bid in sel:
            if bid not in bede:
                raise SystemExit(f"[FATAL] selected_bede_id not found in BEDE canon: {bid} (asc_id={asc_id}) at {machine_path}:{ln}")

        gloss = rec.get("gloss", "")
        translation = rec.get("translation", "")
        note = rec.get("note", "")

        if not isinstance(gloss, str) or not isinstance(translation, str) or not isinstance(note, str):
            raise SystemExit(f"[FATAL] gloss/translation/note must be strings (asc_id={asc_id}) at {machine_path}:{ln}")

        if len(gloss) > args.max_gloss:
            raise SystemExit(f"[FATAL] gloss too long ({len(gloss)}>{args.max_gloss}) asc_id={asc_id} at {machine_path}:{ln}")
        if len(translation) > args.max_translation:
            raise SystemExit(f"[FATAL] translation too long ({len(translation)}>{args.max_translation}) asc_id={asc_id} at {machine_path}:{ln}")
        if len(note) > args.max_note:
            raise SystemExit(f"[FATAL] note too long ({len(note)}>{args.max_note}) asc_id={asc_id} at {machine_path}:{ln}")

        if any(has_mojibake(s) for s in (gloss, translation, note)):
            raise SystemExit(f"[FATAL] Mojibake in machine fields asc_id={asc_id} at {machine_path}:{ln}")

    print(f"[OK] reader-bundle validation passed: machine_records={n} asc_records={len(asc)} bede_records={len(bede)}")

if __name__ == "__main__":
    main()