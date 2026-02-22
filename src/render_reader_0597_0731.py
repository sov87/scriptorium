import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            yield ln, json.loads(line)

def pick_text(rec: dict):
    for k in ("txt", "text", "canon_text", "content", "oe_text", "latin_text", "txt_raw"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return k, v
    return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asc", required=True)
    ap.add_argument("--bede", required=True)
    ap.add_argument("--machine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--meta_out", required=True)
    ap.add_argument("--title", default="Reader: ASC ↔ OE Bede (0597–0731)")
    args = ap.parse_args()

    asc_path = Path(args.asc)
    bede_path = Path(args.bede)
    machine_path = Path(args.machine)
    out_path = Path(args.out)
    meta_path = Path(args.meta_out)

    # Load ASC canon
    asc = {}
    for ln, rec in iter_jsonl(asc_path):
        rid = rec.get("id")
        if not isinstance(rid, str) or not rid.strip():
            raise SystemExit(f"[FATAL] ASC missing string id at {asc_path}:{ln}")
        if rid in asc:
            raise SystemExit(f"[FATAL] Duplicate ASC id={rid} at {asc_path}:{ln}")
        k, txt = pick_text(rec)
        if not txt:
            raise SystemExit(f"[FATAL] ASC missing text-ish field at {asc_path}:{ln} (id={rid})")
        asc[rid] = rec

    # Load BEDE canon
    bede = {}
    for ln, rec in iter_jsonl(bede_path):
        rid = rec.get("id")
        if not isinstance(rid, str) or not rid.strip():
            raise SystemExit(f"[FATAL] BEDE missing string id at {bede_path}:{ln}")
        if rid in bede:
            raise SystemExit(f"[FATAL] Duplicate BEDE id={rid} at {bede_path}:{ln}")
        k, txt = pick_text(rec)
        if not txt:
            raise SystemExit(f"[FATAL] BEDE missing text-ish field at {bede_path}:{ln} (id={rid})")
        bede[rid] = rec

    # Load machine layer (validate existence; do not mutate text)
    items = []
    for ln, rec in iter_jsonl(machine_path):
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

        items.append(rec)

    # Deterministic sort: by ASC year (if present), then asc_id
    def sort_key(rec):
        a = asc.get(rec["asc_id"], {})
        y = a.get("year")
        try:
            yv = int(y)
        except Exception:
            yv = 10**9
        return (yv, rec["asc_id"])

    items.sort(key=sort_key)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    # Render
    with out_path.open("w", encoding="utf-8", newline="\n") as w:
        w.write(f"# {args.title}\n\n")
        w.write("Inputs:\n\n")
        w.write(f"- ASC canon: `{asc_path.as_posix()}`\n")
        w.write(f"- BEDE canon: `{bede_path.as_posix()}`\n")
        w.write(f"- Machine layer: `{machine_path.as_posix()}`\n\n")
        w.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}\n\n")
        w.write("---\n\n")

        for rec in items:
            asc_id = rec["asc_id"]
            a = asc[asc_id]
            _, asc_txt = pick_text(a)
            year = a.get("year", "")
            header = f"## {asc_id}" + (f" (Year {year})" if str(year).strip() else "")
            w.write(header + "\n\n")

            w.write("### ASC canon\n\n")
            w.write("```text\n")
            w.write(asc_txt.rstrip() + "\n")
            w.write("```\n\n")

            w.write("### Machine layer\n\n")
            gloss = rec.get("gloss", "")
            translation = rec.get("translation", "")
            note = rec.get("note", "")
            model = rec.get("model", "")
            prompt_sha = rec.get("prompt_sha256", "")

            if model or prompt_sha:
                w.write("**Provenance**\n\n")
                if model:
                    w.write(f"- model: `{model}`\n")
                if prompt_sha:
                    w.write(f"- prompt_sha256: `{prompt_sha}`\n")
                w.write("\n")

            w.write("**Gloss**\n\n")
            w.write("```text\n" + gloss.rstrip() + "\n```\n\n")

            w.write("**Translation**\n\n")
            w.write("```text\n" + translation.rstrip() + "\n```\n\n")

            w.write("**Note**\n\n")
            w.write("```text\n" + note.rstrip() + "\n```\n\n")

            w.write("### Linked Bede (verbatim canon)\n\n")
            for bid in rec["selected_bede_ids"]:
                b = bede[bid]
                _, bede_txt = pick_text(b)
                w.write(f"#### {bid}\n\n")
                w.write("```text\n")
                w.write(bede_txt.rstrip() + "\n")
                w.write("```\n\n")

            w.write("---\n\n")

    # Sidecar metadata for defensibility
    meta = {
        "generated_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "counts": {
            "machine_records": len(items),
            "asc_records": len(asc),
            "bede_records": len(bede),
        },
        "inputs": {
            "asc_path": str(asc_path),
            "bede_path": str(bede_path),
            "machine_path": str(machine_path),
            "asc_sha256": sha256_file(asc_path),
            "bede_sha256": sha256_file(bede_path),
            "machine_sha256": sha256_file(machine_path),
        },
        "output": {
            "reader_md_path": str(out_path),
        },
    }
    with meta_path.open("w", encoding="utf-8", newline="\n") as w:
        json.dump(meta, w, ensure_ascii=False, indent=2)
        w.write("\n")

    print(f"[OK] wrote reader -> {out_path}")
    print(f"[OK] wrote meta   -> {meta_path}")

if __name__ == "__main__":
    main()