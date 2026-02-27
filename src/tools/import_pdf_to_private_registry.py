#!/usr/bin/env python3
r"""
import_pdf_to_private_registry.py

One-command local-only ingest for paid PDFs (no uploading):
  1) Run src/ingest/import_local_pdf_marker.py to produce canonical JSONL
  2) Upsert docs/corpora.private.json with canon_jsonl.path + sha256 + local-only rights
  3) Write a tiny local harvest report
  4) Run src/ingest/gen_provenance_from_harvest.py to patch docs/provenance/<corpus_id>.json

Usage example (PowerShell):
  python .\src\tools\import_pdf_to_private_registry.py `
    --pdf "F:\Books\as_project\data_raw\private\kalkriese\kalkriese.pdf" `
    --corpus-id "local_kalkriese_demo" `
    --title "Kalkriese, Römer im Osnabrücker Land (LOCAL-ONLY)" `
    --lang deu `
    --pages 20-80 `
    --force-ocr

Notes:
- Does NOT bypass DRM. Only use PDFs you can legally access as files.
- Intended for local-only corpora (distributable=false).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def read_json_bomsafe(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def relpath_posix(root: Path, p: Path) -> str:
    try:
        return str(p.relative_to(root)).replace("\\", "/")
    except Exception:
        return str(p).replace("\\", "/")


def ensure_private_registry(path: Path) -> Dict[str, Any]:
    if path.exists():
        obj = read_json_bomsafe(path)
        if not isinstance(obj, dict):
            raise SystemExit(f"Private registry is not a JSON object: {path}")
        if "corpora" not in obj or not isinstance(obj["corpora"], list):
            obj["corpora"] = []
        return obj
    return {"corpora": []}


def upsert_corpus(reg: Dict[str, Any], entry: Dict[str, Any]) -> None:
    corpora = reg.get("corpora")
    assert isinstance(corpora, list)
    cid = entry["corpus_id"]
    for i, c in enumerate(corpora):
        if isinstance(c, dict) and c.get("corpus_id") == cid:
            corpora[i] = entry
            return
    corpora.append(entry)


def run_marker_import(
    *,
    root: Path,
    pdf: Path,
    corpus_id: str,
    out_jsonl: Path,
    lang: str,
    work_id: Optional[str],
    pages: Optional[str],
    max_chars: int,
    force_ocr: bool,
) -> Dict[str, Any]:
    script = root / "src" / "ingest" / "import_local_pdf_marker.py"
    if not script.exists():
        raise SystemExit(f"Missing: {script}")

    cmd = [
        sys.executable,
        str(script),
        "--pdf", str(pdf),
        "--corpus-id", corpus_id,
        "--out-jsonl", str(out_jsonl),
        "--lang", lang,
        "--max-chars", str(max_chars),
    ]
    if work_id:
        cmd += ["--work-id", work_id]
    if pages:
        cmd += ["--pages", pages]
    if force_ocr:
        cmd += ["--force-ocr"]

    p = subprocess.run(cmd, check=False, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if p.returncode != 0:
        sys.stderr.write(p.stdout)
        sys.stderr.write(p.stderr)
        raise SystemExit(f"Marker import failed with code {p.returncode}")

    # Script prints a final JSON object to stdout; parse last {...} block.
    out = p.stdout.strip().splitlines()
    blob = None
    for line in reversed(out):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            blob = line
            break
    if not blob:
        # fallback: try whole stdout
        blob = p.stdout.strip()
    try:
        info = json.loads(blob)
    except Exception:
        sys.stderr.write(p.stdout)
        sys.stderr.write(p.stderr)
        raise SystemExit("Could not parse JSON summary from import_local_pdf_marker.py output")

    return info


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--pdf", required=True, help="Input PDF path")
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--title", required=True)
    ap.add_argument("--lang", default="eng")
    ap.add_argument("--work-id", default=None)
    ap.add_argument("--pages", default=None, help="Optional page range like 20-80 (1-indexed)")
    ap.add_argument("--max-chars", type=int, default=1400)
    ap.add_argument("--force-ocr", action="store_true")

    ap.add_argument("--private-registry", default="docs/corpora.private.json")
    ap.add_argument("--out-dir", default="data_proc/private")
    ap.add_argument("--out-jsonl", default=None, help="Override output JSONL path (default out-dir/<corpus_id>.jsonl)")

    # Rights (defaults are local-only)
    ap.add_argument("--tier", default="Z_local_only")
    ap.add_argument("--license", dest="license_str", default="LOCAL-ONLY")
    ap.add_argument("--distributable", type=int, choices=(0, 1), default=0)

    ap.add_argument("--write-harvest", action="store_true", help="Write a local harvest report under runs/harvest")
    ap.add_argument("--skip-provenance", action="store_true", help="Do not run gen_provenance_from_harvest.py")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    pdf = Path(args.pdf).resolve()

    priv_reg = Path(args.private_registry)
    if not priv_reg.is_absolute():
        priv_reg = (root / priv_reg).resolve()

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = (root / out_dir).resolve()

    if args.out_jsonl:
        out_jsonl = Path(args.out_jsonl)
        if not out_jsonl.is_absolute():
            out_jsonl = (root / out_jsonl).resolve()
    else:
        out_jsonl = (out_dir / f"{args.corpus_id}.jsonl").resolve()

    out_dir.mkdir(parents=True, exist_ok=True)

    info = run_marker_import(
        root=root,
        pdf=pdf,
        corpus_id=args.corpus_id,
        out_jsonl=out_jsonl,
        lang=args.lang,
        work_id=args.work_id,
        pages=args.pages,
        max_chars=args.max_chars,
        force_ocr=bool(args.force_ocr),
    )

    if int(info.get("records_written") or 0) <= 0:
        raise SystemExit("Import produced 0 records. Try different --pages or add --force-ocr or inspect --debug-dump in the marker importer.")

    # Upsert private registry
    reg = ensure_private_registry(priv_reg)
    rel_jsonl = relpath_posix(root, out_jsonl)

    entry = {
        "corpus_id": args.corpus_id,
        "title": args.title,
        "canon_jsonl": {
            "path": rel_jsonl,
            "sha256": (info.get("sha256") or "").upper(),
        },
        "rights": {
            "tier": args.tier,
            "license": args.license_str,
            "distributable": bool(args.distributable),
            "notes": "",
        },
    }

    upsert_corpus(reg, entry)
    write_json(priv_reg, reg)

    report_path = None
    if args.write_harvest or (not args.skip_provenance):
        runs_dir = (root / "runs" / "harvest")
        runs_dir.mkdir(parents=True, exist_ok=True)
        report_path = runs_dir / f"harvest_local_{args.corpus_id}_{utc_stamp()}.json"
        report = {
            "repo_root": "LOCAL-PDF",
            "generated_utc": utc_now_iso(),
            "items": [
                {
                    "corpus_id": args.corpus_id,
                    "work_id": args.work_id or f"pdf:{args.corpus_id}",
                    "tei": str(pdf).replace("\\", "/"),
                    "out_jsonl": rel_jsonl,
                }
            ],
        }
        write_json(report_path, report)

    if (not args.skip_provenance) and report_path is not None:
        prov_script = root / "src" / "ingest" / "gen_provenance_from_harvest.py"
        if not prov_script.exists():
            raise SystemExit(f"Missing: {prov_script}")
        cmd = [
            sys.executable,
            str(prov_script),
            "--root", str(root),
            "--report", str(report_path),
            "--registry", relpath_posix(root, priv_reg),
        ]
        p = subprocess.run(cmd, check=False, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if p.returncode != 0:
            sys.stderr.write(p.stdout)
            sys.stderr.write(p.stderr)
            raise SystemExit(f"Provenance generation failed with code {p.returncode}")

    print(json.dumps({
        "ok": True,
        "corpus_id": args.corpus_id,
        "jsonl": str(out_jsonl),
        "sha256": (info.get("sha256") or ""),
        "private_registry": str(priv_reg),
        "harvest_report": (str(report_path) if report_path else None),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
