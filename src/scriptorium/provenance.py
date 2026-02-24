# File: src/scriptorium/provenance.py
# Purpose: enforce provenance/rights discipline by verifying canonical JSONL hashes
#
# This module is intentionally dependency-light and deterministic.

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_nested(d: Dict[str, Any], keys: List[str]) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _canon_path(entry: Dict[str, Any]) -> Optional[str]:
    # Common shapes we have used:
    # - {"canon_jsonl": {"path": "...", "sha256": "..."}}
    # - {"canon_jsonl_path": "..."} (legacy)
    # - {"path": "..."} (fallback)
    v = _get_nested(entry, ["canon_jsonl", "path"])
    if isinstance(v, str) and v.strip():
        return v.strip()
    v = entry.get("canon_jsonl_path")
    if isinstance(v, str) and v.strip():
        return v.strip()
    v = entry.get("path")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _canon_sha(entry: Dict[str, Any]) -> Optional[str]:
    v = _get_nested(entry, ["canon_jsonl", "sha256"])
    if isinstance(v, str) and v.strip():
        return v.strip()
    v = entry.get("sha256")
    if isinstance(v, str) and v.strip():
        return v.strip()
    v = entry.get("canon_jsonl_sha256")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def verify_canon_jsonl_sha256(
    project_root: Path,
    *,
    registry_rel: str = "docs/corpora.json",
    strict: bool = False,
) -> Dict[str, Any]:
    """
    Verify SHA256 for canonical JSONL files listed in the corpus registry.

    Behavior:
    - Always verifies any corpus that has a recorded sha256.
      - Missing file or mismatch => hard fail (SystemExit).
    - If strict=True:
      - Any corpus with a canon_jsonl.path but no sha256 => hard fail.

    Returns a summary dict for logging/testing.
    """
    reg_path = (project_root / registry_rel).resolve()
    if not reg_path.exists():
        raise SystemExit(f"[ERR] provenance: registry not found: {reg_path}")

    try:
        data = json.loads(reg_path.read_text(encoding="utf-8"))
    except Exception:
        # Be robust if file accidentally has BOM
        data = json.loads(reg_path.read_text(encoding="utf-8-sig"))

    corpora: List[Dict[str, Any]]
    if isinstance(data, dict):
        raw_list = data.get("corpora") or data.get("items") or data.get("entries") or []
        corpora = list(raw_list) if isinstance(raw_list, list) else []
    elif isinstance(data, list):
        corpora = list(data)
    else:
        raise SystemExit(f"[ERR] provenance: unrecognized registry JSON shape: {type(data)}")

    checked = 0
    skipped_no_sha = 0

    for entry in corpora:
        if not isinstance(entry, dict):
            continue
        corpus_id = (entry.get("corpus_id") or entry.get("id") or "").strip()
        canon_path = _canon_path(entry)
        if not canon_path:
            continue

        sha = _canon_sha(entry)
        if not sha:
            if strict:
                raise SystemExit(f"[ERR] provenance: missing sha256 for corpus_id={corpus_id!r} path={canon_path!r}")
            skipped_no_sha += 1
            continue

        canon_file = Path(canon_path)
        if not canon_file.is_absolute():
            canon_file = (project_root / canon_file).resolve()
        else:
            canon_file = canon_file.resolve()

        if not canon_file.exists():
            raise SystemExit(f"[ERR] provenance: missing canon_jsonl for corpus_id={corpus_id!r}: {canon_file}")

        actual = _sha256_file(canon_file)
        exp = sha.lower()
        if actual.lower() != exp:
            raise SystemExit(
                f"[ERR] provenance: sha256 mismatch for corpus_id={corpus_id!r}\n"
                f"  file: {canon_file}\n"
                f"  expected: {exp}\n"
                f"  actual:   {actual.lower()}"
            )

        checked += 1

    return {
        "ok": True,
        "checked": checked,
        "skipped_no_sha": skipped_no_sha,
        "registry": str(reg_path),
        "strict": bool(strict),
    }
