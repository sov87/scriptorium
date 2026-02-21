#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def check_file(root: Path, rel: str, want_sha: str) -> Dict[str, Any]:
    p = root / Path(rel)
    if not p.exists():
        return {"path": rel, "ok": False, "error": "missing"}
    got = sha256_file(p)
    ok = (not want_sha) or (got == want_sha.upper())
    out: Dict[str, Any] = {"path": rel, "ok": ok, "got": got}
    if want_sha:
        out["expected"] = want_sha.upper()
    if not ok:
        out["error"] = "sha256_mismatch"
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate docs/corpora.json artifact existence + sha256.")
    ap.add_argument("--root", required=True)
    ap.add_argument("--corpora", default="docs/corpora.json")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    reg_path = root / args.corpora
    reg = json.loads(reg_path.read_text(encoding="utf-8"))

    results: List[Dict[str, Any]] = []
    ok = True

    for c in reg.get("corpora", []):
        cid = c.get("corpus_id", "?")
        checks: List[Dict[str, Any]] = []
        canon = c.get("canon_jsonl")
        if canon:
            checks.append(check_file(root, canon["path"], canon.get("sha256", "")))
        bm25 = c.get("bm25")
        if bm25:
            checks.append(check_file(root, bm25["path"], bm25.get("sha256", "")))
        faiss = c.get("faiss")
        if faiss:
            checks.append(check_file(root, faiss["index_path"], faiss.get("index_sha256", "")))
            checks.append(check_file(root, faiss["ids_path"], faiss.get("ids_sha256", "")))
            checks.append(check_file(root, faiss["meta_path"], faiss.get("meta_sha256", "")))

        corp_ok = all(x.get("ok") for x in checks) and len(checks) > 0
        if not corp_ok:
            ok = False
        results.append({"corpus_id": cid, "ok": corp_ok, "checks": checks})

    out = {"ok": ok, "results": results, "corpora_path": str(reg_path)}
    if args.json:
        print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    else:
        if ok:
            print(f"[OK] corpora registry valid: {reg_path}")
        else:
            print(f"[FAIL] corpora registry invalid: {reg_path}")
            for r in results:
                if not r["ok"]:
                    print(" ", r["corpus_id"])
                    for ch in r["checks"]:
                        if not ch["ok"]:
                            print("   ", ch["path"], ch.get("error"), "expected", ch.get("expected"), "got", ch.get("got"))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
