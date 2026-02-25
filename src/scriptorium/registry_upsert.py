# File: src/scriptorium/registry_upsert.py
# Purpose: deterministic upsert into docs/corpora.json with canon_jsonl.path + sha256

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def _dump_one_line(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(", ", ": "), sort_keys=False)


def _rel_under(root: Path, p: Path) -> str:
    try:
        return p.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return p.resolve().as_posix()


def upsert_corpus(
    *,
    project_root: Path,
    registry_rel: str,
    corpus_id: str,
    title: str,
    canon_jsonl_abs: Path,
    tier: str,
    license_str: str,
    distributable: bool,
) -> Dict[str, Any]:
    reg_path = (project_root / registry_rel).resolve()
    if not reg_path.exists():
        data: Dict[str, Any] = {"generated_utc": _utc_iso(), "corpora": []}
    else:
        raw = _load_json(reg_path)
        if not isinstance(raw, dict):
            raise SystemExit(f"[ERR] registry shape: expected dict: {reg_path}")
        data = raw

    corpora = data.get("corpora")
    if not isinstance(corpora, list):
        corpora = []
        data["corpora"] = corpora

    canon_sha = _sha256_file(canon_jsonl_abs)
    canon_rel = _rel_under(project_root, canon_jsonl_abs).replace("\\", "/")

    target: Optional[Dict[str, Any]] = None
    for e in corpora:
        if isinstance(e, dict) and str(e.get("corpus_id", "")).strip() == corpus_id:
            target = e
            break

    if target is None:
        target = {"corpus_id": corpus_id}
        corpora.append(target)

    target["title"] = title
    target["canon_jsonl"] = {"path": canon_rel, "sha256": canon_sha}
    target["rights"] = {"tier": tier, "license": license_str, "distributable": bool(distributable)}

    corpora.sort(key=lambda x: str(x.get("corpus_id", "")) if isinstance(x, dict) else "")
    data["generated_utc"] = _utc_iso()

    reg_path.parent.mkdir(parents=True, exist_ok=True)
    reg_path.write_text(_dump_one_line(data) + "\n", encoding="utf-8", newline="\n")

    return {"registry": str(reg_path), "corpus_id": corpus_id, "sha256": canon_sha, "canon_jsonl": canon_rel}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root (default: .)")
    ap.add_argument("--registry", default="docs/corpora.json")
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--title", required=True)
    ap.add_argument("--canon-jsonl", required=True)
    ap.add_argument("--tier", default="A_open_license")
    ap.add_argument("--license", required=True)
    ap.add_argument("--distributable", type=int, default=1)
    args = ap.parse_args()

    root = Path(args.root).resolve()
    canon = Path(args.canon_jsonl)
    canon_abs = canon if canon.is_absolute() else (root / canon).resolve()
    if not canon_abs.exists():
        raise SystemExit(f"[ERR] missing canon_jsonl: {canon_abs}")

    out = upsert_corpus(
        project_root=root,
        registry_rel=str(args.registry),
        corpus_id=str(args.corpus_id).strip(),
        title=str(args.title),
        canon_jsonl_abs=canon_abs,
        tier=str(args.tier),
        license_str=str(args.license),
        distributable=bool(int(args.distributable)),
    )
    print(out["registry"])
    print(out["sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())