from __future__ import annotations

import json
import subprocess
import sys
from .catalog_http import download
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATALOG_PATH = Path("docs/sources_catalog.json")
FETCH_STATE_DIR = Path("docs/source_fetch")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_catalog(root: Path) -> dict[str, Any]:
    p = (root / CATALOG_PATH).resolve()
    if not p.exists():
        raise SystemExit(f"missing catalog: {p}")
    return json.loads(p.read_text(encoding="utf-8-sig"))


def write_catalog(root: Path, obj: dict[str, Any]) -> None:
    p = (root / CATALOG_PATH).resolve()
    obj["generated_utc"] = utc_iso()
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")


def subst_args(cmd: list[str], root: Path) -> list[str]:
    out = []
    for s in cmd:
        out.append(
            s.replace("{root}", str(root))
             .replace("{python}", sys.executable)
        )
    return out


def _run(cmd: list[str], cwd: Path) -> int:
    p = subprocess.run(cmd, cwd=str(cwd), check=False)
    return int(p.returncode)


def fetch_git(root: Path, src: dict[str, Any]) -> dict[str, Any]:
    repo = str(src.get("repo") or "").strip()
    dest_rel = str(src.get("dest") or "").strip()
    ref = str(src.get("ref") or "").strip()

    if not repo or not dest_rel:
        raise SystemExit(f"bad git source entry: {src.get('source_id')}")

    dest = (root / dest_rel).resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)

    if not (dest / ".git").exists():
        rc = _run(["git", "clone", repo, str(dest)], cwd=root)
        if rc != 0:
            raise SystemExit(f"git clone failed: {repo}")
    else:
        _run(["git", "-C", str(dest), "fetch", "--all", "--tags"], cwd=root)
        _run(["git", "-C", str(dest), "pull", "--ff-only"], cwd=root)

    if ref:
        rc = _run(["git", "-C", str(dest), "checkout", ref], cwd=root)
        if rc != 0:
            raise SystemExit(f"git checkout failed: {ref}")

    # capture HEAD
    head = ""
    try:
        head = subprocess.check_output(["git", "-C", str(dest), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        head = ""

    return {"ok": True, "dest": str(dest), "head": head}


def fetch_http(root: Path, src: dict[str, Any]) -> dict[str, Any]:
    url = str(src.get("url") or "").strip()
    dest_rel = str(src.get("dest") or "").strip()
    if not url or not dest_rel:
        raise SystemExit(f"bad http source entry: {src.get('source_id')}")
    dest = (root / dest_rel).resolve()
    return download(url, dest)
def fetch_manual(root: Path, src: dict[str, Any]) -> dict[str, Any]:
    dest_rel = str(src.get("dest") or "").strip()
    dest = (root / dest_rel).resolve() if dest_rel else root
    return {"ok": True, "dest": str(dest), "note": "manual"}


def run_catalog_fetch(root: Path, source_ids: list[str] | None = None) -> Path:
    cat = load_catalog(root)
    sources = cat.get("sources") or []
    if not isinstance(sources, list):
        raise SystemExit("catalog.sources must be a list")

    wanted = set(source_ids or [])
    fetched = []

    FETCH_STATE_DIR_ABS = (root / FETCH_STATE_DIR).resolve()
    FETCH_STATE_DIR_ABS.mkdir(parents=True, exist_ok=True)

    for s in sources:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("source_id") or "").strip()
        if not sid:
            continue
        if wanted and sid not in wanted:
            continue

        typ = str(s.get("type") or "").strip()
        if typ == "git":
            info = fetch_git(root, s)
        elif typ == "http":
            info = fetch_http(root, s)
        elif typ == "manual":
            info = fetch_manual(root, s)
        else:
            raise SystemExit(f"unsupported source type: {typ} (source_id={sid})")

        state = {
            "schema": "scriptorium.source_fetch_state.v1",
            "generated_utc": utc_iso(),
            "source_id": sid,
            "type": typ,
            "repo": s.get("repo", ""),
            "dest": s.get("dest", ""),
            "ref": s.get("ref", ""),
            "license": s.get("license", ""),
            "distributable": bool(s.get("distributable", False)),
            "result": info,
        }
        (FETCH_STATE_DIR_ABS / f"{sid}.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        fetched.append(sid)

    print(f"[OK] fetched={len(fetched)}")
    return FETCH_STATE_DIR_ABS


def run_catalog_status(root: Path) -> int:
    cat = load_catalog(root)

    print("SOURCES:")
    for s in (cat.get("sources") or []):
        if not isinstance(s, dict):
            continue
        sid = s.get("source_id", "")
        dest = s.get("dest", "")
        typ = s.get("type", "")
        exists = (root / dest).exists() if dest else False
        print(f"  {sid}\t{typ}\t{dest}\texists={exists}")

    print("\nINGESTS:")
    for ing in (cat.get("ingests") or []):
        if not isinstance(ing, dict):
            continue
        cid = ing.get("corpus_id", "")
        enabled = bool(ing.get("enabled", False))
        outs = ing.get("outputs") or []
        ok = True
        for o in outs:
            if not (root / str(o)).exists():
                ok = False
                break
        print(f"  {cid}\tenabled={enabled}\toutputs_ok={ok}")
    return 0


def run_catalog_ingest(root: Path, corpus_ids: list[str] | None = None) -> int:
    cat = load_catalog(root)
    ingests = cat.get("ingests") or []
    if not isinstance(ingests, list):
        raise SystemExit("catalog.ingests must be a list")

    wanted = set(corpus_ids or [])
    ran = 0

    for ing in ingests:
        if not isinstance(ing, dict):
            continue
        cid = str(ing.get("corpus_id") or "").strip()
        if not cid:
            continue
        if wanted and cid not in wanted:
            continue
        if not bool(ing.get("enabled", False)):
            continue

        cmd = ing.get("cmd")
        if not isinstance(cmd, list) or not all(isinstance(x, str) for x in cmd):
            raise SystemExit(f"bad ingest.cmd for {cid}")

        cmd2 = subst_args([str(x) for x in cmd], root)
        print("[RUN]", " ".join(cmd2))
        rc = _run(cmd2, cwd=root)
        if rc != 0:
            raise SystemExit(f"ingest failed: {cid} (rc={rc})")
        ran += 1

    print(f"[OK] ingests_ran={ran}")
    return 0
