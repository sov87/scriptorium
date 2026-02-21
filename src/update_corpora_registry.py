#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _norm_path(p: str) -> str:
    return p.replace("\\", "/")


def _find_output(step: Dict[str, Any], want_suffix: str) -> Optional[Dict[str, Any]]:
    for o in step.get("outputs", []) or []:
        op = _norm_path(o.get("path", ""))
        if op.endswith(want_suffix):
            return o
    return None


def _find_step(processing: List[Dict[str, Any]], step_name: str) -> Optional[Dict[str, Any]]:
    for s in processing:
        if s.get("step") == step_name:
            return s
    return None


def _extract_artifacts_from_provenance(prov: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns (entry, warnings). Entry schema matches docs/corpora.json expected by release integration.
    """
    warnings: List[Dict[str, Any]] = []

    corpus_id = prov.get("corpus_id", "")
    title = prov.get("title", "")
    processing = prov.get("processing", []) or []

    if not corpus_id:
        raise ValueError("missing corpus_id")
    if not title:
        warnings.append({"corpus_id": corpus_id, "warning": "missing title"})

    # Ingest (canon)
    ingest = _find_step(processing, "ingest")
    canon = None
    if ingest:
        want = f"data_proc/{corpus_id}_prod.jsonl"
        o = _find_output(ingest, want)
        if o:
            canon = {"path": want, "sha256": (o.get("sha256") or "").upper(), "records": o.get("record_count")}
    if not canon:
        warnings.append({"corpus_id": corpus_id, "warning": "missing ingest/canon output; expected data_proc/<corpus_id>_prod.jsonl"})

    # BM25
    bm25_step = _find_step(processing, "build_bm25")
    bm25 = None
    if bm25_step:
        want = f"indexes/bm25/{corpus_id}_utf8.pkl"
        o = _find_output(bm25_step, want)
        if o:
            bm25 = {"path": want, "sha256": (o.get("sha256") or "").upper()}
    if not bm25:
        warnings.append({"corpus_id": corpus_id, "warning": "missing build_bm25 output; expected indexes/bm25/<corpus_id>_utf8.pkl"})

    # FAISS
    faiss_step = _find_step(processing, "build_faiss")
    faiss = None
    if faiss_step:
        idx = f"indexes/vec_faiss/{corpus_id}.index"
        ids = f"indexes/vec_faiss/{corpus_id}_ids.json"
        meta = f"indexes/vec_faiss/{corpus_id}_meta.jsonl"
        o_idx = _find_output(faiss_step, idx)
        o_ids = _find_output(faiss_step, ids)
        o_meta = _find_output(faiss_step, meta)
        if o_idx and o_ids and o_meta:
            faiss = {
                "index_path": idx, "index_sha256": (o_idx.get("sha256") or "").upper(),
                "ids_path": ids, "ids_sha256": (o_ids.get("sha256") or "").upper(),
                "meta_path": meta, "meta_sha256": (o_meta.get("sha256") or "").upper(),
                "model": (faiss_step.get("params", {}) or {}).get("model"),
                "dim": (faiss_step.get("params", {}) or {}).get("dim"),
            }
    if not faiss:
        warnings.append({"corpus_id": corpus_id, "warning": "missing build_faiss outputs; expected indexes/vec_faiss/<corpus_id>.* trio"})

    entry: Dict[str, Any] = {"corpus_id": corpus_id, "title": title}
    if canon:
        entry["canon_jsonl"] = canon
    if bm25:
        entry["bm25"] = bm25
    if faiss:
        entry["faiss"] = faiss

    return entry, warnings


def _validate_registry_files(root: Path, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    warns: List[Dict[str, Any]] = []
    cid = entry.get("corpus_id", "?")

    def check(rel_path: str, want_sha: str) -> None:
        p = root / Path(rel_path)
        if not p.exists():
            warns.append({"corpus_id": cid, "warning": f"missing file: {rel_path}"})
            return
        got = sha256_file(p)
        if want_sha and got != want_sha.upper():
            warns.append({"corpus_id": cid, "warning": f"sha256 mismatch for {rel_path}", "expected": want_sha.upper(), "got": got})

    canon = entry.get("canon_jsonl")
    if canon:
        check(canon["path"], canon.get("sha256", ""))

    bm25 = entry.get("bm25")
    if bm25:
        check(bm25["path"], bm25.get("sha256", ""))

    faiss = entry.get("faiss")
    if faiss:
        check(faiss["index_path"], faiss.get("index_sha256", ""))
        check(faiss["ids_path"], faiss.get("ids_sha256", ""))
        check(faiss["meta_path"], faiss.get("meta_sha256", ""))

    return warns


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate docs/corpora.json from docs/provenance/*.json (minified).")
    ap.add_argument("--root", required=True, help="Project root (e.g., F:\\Books\\as_project)")
    ap.add_argument("--provenance-dir", default="docs/provenance", help="Directory containing provenance JSON files")
    ap.add_argument("--out", default="docs/corpora.json", help="Output registry JSON path")
    ap.add_argument("--validate-files", action="store_true", help="Verify that files exist and sha256 match")
    ap.add_argument("--json", action="store_true", help="Emit machine-readable JSON summary")
    args = ap.parse_args()

    root = Path(args.root)
    prov_dir = root / args.provenance_dir
    out_path = root / args.out

    entries: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    if not prov_dir.exists():
        raise SystemExit(f"provenance dir not found: {prov_dir}")

    for prov_path in sorted(prov_dir.glob("*.json")):
        prov = json.loads(prov_path.read_text(encoding="utf-8"))
        try:
            entry, w = _extract_artifacts_from_provenance(prov)
            entries.append(entry)
            warnings.extend(w)
        except Exception as e:
            warnings.append({"file": str(prov_path), "warning": f"could not parse provenance: {e}"})

    # Optional validation vs disk
    if args.validate_files:
        for e in entries:
            warnings.extend(_validate_registry_files(root, e))

    registry = {
        "generated_utc": utc_now(),
        "corpora": entries,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(registry, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")

    result = {
        "ok": True,
        "out": str(out_path),
        "corpora": len(entries),
        "warnings": warnings,
        "validated": bool(args.validate_files),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print(f"[OK] wrote {out_path} corpora={len(entries)} warnings={len(warnings)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
