# F:\Books\as_project\src\render_reader_fallback.py
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise SystemExit(f"[ERROR] {path.name}:{line_no}: invalid JSON: {e}")
            if not isinstance(obj, dict):
                raise SystemExit(f"[ERROR] {path.name}:{line_no}: JSONL line must be an object")
            yield obj


def count_jsonl(path: Path) -> int:
    n = 0
    for _ in iter_jsonl(path):
        n += 1
    return n


def md_escape(s: str) -> str:
    return s.replace("\r\n", "\n").replace("\r", "\n")


def write_section(out, title: str, body: Optional[str]) -> None:
    out.write(f"### {title}\n\n")
    if body is None:
        out.write("_[missing]_\n\n")
        return
    body = md_escape(body).strip()
    if not body:
        out.write("_[empty]_\n\n")
        return
    out.write(body + "\n\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asc", required=True)
    ap.add_argument("--bede", required=True)
    ap.add_argument("--machine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--meta_out", required=True)
    args = ap.parse_args()

    asc_path = Path(args.asc)
    bede_path = Path(args.bede)
    machine_path = Path(args.machine)
    out_path = Path(args.out)
    meta_path = Path(args.meta_out)

    for p in (asc_path, bede_path, machine_path):
        if not p.exists():
            raise SystemExit(f"[ERROR] missing file: {p}")

    # Minimal validation: machine must be valid JSONL objects and have asc_id
    machine_records: List[Dict[str, Any]] = []
    for rec in iter_jsonl(machine_path):
        if "asc_id" not in rec:
            raise SystemExit(f"[ERROR] machine record missing 'asc_id' (first bad record: {rec.keys()})")
        machine_records.append(rec)

    generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        out.write("# SCRIPTORIUM Reader\n\n")
        out.write(f"- generated_utc: {generated_utc}\n")
        out.write(f"- machine_records: {len(machine_records)}\n")
        out.write(f"- asc_canon: {asc_path.as_posix()}\n")
        out.write(f"- bede_canon: {bede_path.as_posix()}\n")
        out.write(f"- machine: {machine_path.as_posix()}\n\n")
        out.write("---\n\n")

        for rec in machine_records:
            asc_id = str(rec.get("asc_id"))
            out.write(f"## {asc_id}\n\n")

            sel = rec.get("selected_bede_ids")
            if isinstance(sel, list):
                out.write("**selected_bede_ids:** " + ", ".join(map(str, sel)) + "\n\n")
            elif sel is not None:
                out.write("**selected_bede_ids:** " + str(sel) + "\n\n")

            schema = rec.get("schema")
            if schema:
                out.write(f"**schema:** `{schema}`\n\n")

            write_section(out, "Gloss", rec.get("gloss"))
            write_section(out, "Translation", rec.get("translation"))
            write_section(out, "Note", rec.get("note"))

            model = rec.get("model")
            psha = rec.get("prompt_sha256")
            if model or psha:
                out.write("**provenance:**\n\n")
                if model:
                    out.write(f"- model: `{model}`\n")
                if psha:
                    out.write(f"- prompt_sha256: `{psha}`\n")
                out.write("\n")

            out.write("---\n\n")

    # Meta JSON (useful for auditing)
    meta = {
        "generated_utc": generated_utc,
        "inputs": {
            "asc": {"path": str(asc_path), "sha256": sha256_file(asc_path), "records": count_jsonl(asc_path)},
            "bede": {"path": str(bede_path), "sha256": sha256_file(bede_path), "records": count_jsonl(bede_path)},
            "machine": {"path": str(machine_path), "sha256": sha256_file(machine_path), "records": len(machine_records)},
        },
        "output": {"reader_md": str(out_path)},
        "renderer": {"name": "render_reader_fallback.py"},
    }

    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] wrote reader -> {out_path}")
    print(f"[OK] wrote meta   -> {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())