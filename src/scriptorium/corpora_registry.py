from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _norm(p: str) -> str:
    return p.replace("\\", "/")


def _find_step(processing: list[dict], step_name: str) -> dict | None:
    for s in processing:
        if s.get("step") == step_name:
            return s
    return None


def _find_output(step: dict, suffix: str) -> dict | None:
    for o in step.get("outputs", []) or []:
        op = _norm(str(o.get("path", "")))
        if op.endswith(suffix):
            return o
    return None


def _extract_entry_from_provenance(prov: dict) -> tuple[dict | None, list[str]]:
    """
    Returns (entry, warnings). Entry is suitable for docs/corpora.json.
    Skips corpus if required artifacts cannot be identified in provenance.
    """
    warnings: list[str] = []
    corpus_id = prov.get("corpus_id")
    title = prov.get("title") or ""

    if not isinstance(corpus_id, str) or not corpus_id:
        return None, ["provenance missing corpus_id"]

    processing = prov.get("processing", []) or []
    if not isinstance(processing, list):
        return None, [f"{corpus_id}: provenance processing is not a list"]

    ingest = _find_step(processing, "ingest")
    if not ingest:
        return None, [f"{corpus_id}: missing processing step 'ingest'"]

    canon_suffix = f"data_proc/{corpus_id}_prod.jsonl"
    canon_out = _find_output(ingest, canon_suffix)
    if not canon_out:
        return None, [f"{corpus_id}: ingest outputs missing {canon_suffix}"]

    canon = {
        "path": canon_suffix,
        "sha256": str(canon_out.get("sha256", "")).upper(),
        "records": canon_out.get("record_count"),
    }

    bm25_step = _find_step(processing, "build_bm25")
    if not bm25_step:
        return None, [f"{corpus_id}: missing processing step 'build_bm25' (needed for snapshot packaging)"]
    bm25_suffix = f"indexes/bm25/{corpus_id}_utf8.pkl"
    bm25_out = _find_output(bm25_step, bm25_suffix)
    if not bm25_out:
        return None, [f"{corpus_id}: build_bm25 outputs missing {bm25_suffix}"]
    bm25 = {"path": bm25_suffix, "sha256": str(bm25_out.get("sha256", "")).upper()}

    faiss_step = _find_step(processing, "build_faiss")
    if not faiss_step:
        return None, [f"{corpus_id}: missing processing step 'build_faiss' (needed for snapshot packaging)"]

    idx = f"indexes/vec_faiss/{corpus_id}.index"
    ids = f"indexes/vec_faiss/{corpus_id}_ids.json"
    meta = f"indexes/vec_faiss/{corpus_id}_meta.jsonl"

    o_idx = _find_output(faiss_step, idx)
    o_ids = _find_output(faiss_step, ids)
    o_meta = _find_output(faiss_step, meta)
    if not (o_idx and o_ids and o_meta):
        return None, [f"{corpus_id}: build_faiss outputs incomplete; expected {idx}, {ids}, {meta}"]

    faiss = {
        "index_path": idx,
        "index_sha256": str(o_idx.get("sha256", "")).upper(),
        "ids_path": ids,
        "ids_sha256": str(o_ids.get("sha256", "")).upper(),
        "meta_path": meta,
        "meta_sha256": str(o_meta.get("sha256", "")).upper(),
        "model": (faiss_step.get("params", {}) or {}).get("model"),
        "dim": (faiss_step.get("params", {}) or {}).get("dim"),
    }

    entry: dict[str, Any] = {
        "corpus_id": corpus_id,
        "title": title,
        "canon_jsonl": canon,
        "bm25": bm25,
        "faiss": faiss,
    }
    return entry, warnings


def generate_registry(project_root: Path, *, provenance_dir: str = "docs/provenance", out_path: str = "docs/corpora.json") -> tuple[Path, list[str]]:
    """
    Build docs/corpora.json from provenance files. Only includes corpora that have
    ingest + build_bm25 + build_faiss recorded in provenance.
    """
    root = project_root
    prov_dir = root / provenance_dir
    outp = root / out_path

    warnings: list[str] = []
    corpora: list[dict] = []

    if not prov_dir.exists():
        raise FileNotFoundError(f"provenance directory not found: {prov_dir}")

    for p in sorted(prov_dir.glob("*.json")):
        try:
            prov = _read_json(p)
            entry, w = _extract_entry_from_provenance(prov)
            warnings.extend(w)
            if entry:
                corpora.append(entry)
        except Exception as e:
            warnings.append(f"{p.name}: failed to parse: {type(e).__name__}: {e}")

    reg = {"generated_utc": _utc_now(), "corpora": corpora}
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(reg, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    return outp, warnings


def validate_registry(project_root: Path, *, registry_path: str = "docs/corpora.json") -> dict:
    """
    Validates that all artifacts referenced by docs/corpora.json exist and sha256 match.
    """
    root = project_root
    regp = root / registry_path
    reg = _read_json(regp)
    results: list[dict] = []
    ok_all = True

    for c in reg.get("corpora", []) or []:
        cid = c.get("corpus_id", "?")
        checks: list[dict] = []

        def check(rel: str, want: str) -> None:
            nonlocal ok_all
            p = root / Path(rel)
            if not p.exists():
                checks.append({"path": rel, "ok": False, "error": "missing"})
                ok_all = False
                return
            got = _sha256_file(p)
            if want and got != want.upper():
                checks.append({"path": rel, "ok": False, "error": "sha256_mismatch", "expected": want.upper(), "got": got})
                ok_all = False
            else:
                checks.append({"path": rel, "ok": True, "got": got, "expected": want.upper() if want else None})

        canon = c.get("canon_jsonl") or {}
        if canon:
            check(canon.get("path", ""), canon.get("sha256", ""))
        bm25 = c.get("bm25") or {}
        if bm25:
            check(bm25.get("path", ""), bm25.get("sha256", ""))
        faiss = c.get("faiss") or {}
        if faiss:
            check(faiss.get("index_path", ""), faiss.get("index_sha256", ""))
            check(faiss.get("ids_path", ""), faiss.get("ids_sha256", ""))
            check(faiss.get("meta_path", ""), faiss.get("meta_sha256", ""))

        results.append({"corpus_id": cid, "ok": all(x.get("ok") for x in checks if isinstance(x, dict)), "checks": checks})

    return {"ok": ok_all, "registry": str(regp), "results": results}
