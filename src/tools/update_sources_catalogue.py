from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json_bomsafe(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raw2 = path.read_text(encoding="utf-8-sig")
        return json.loads(raw2)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def relpath_from_root(root: Path, p: Path) -> str:
    try:
        return str(p.relative_to(root)).replace("\\", "/")
    except Exception:
        return str(p).replace("\\", "/")


def load_registry(reg_path: Path) -> List[Dict[str, Any]]:
    obj = read_json_bomsafe(reg_path)
    corpora = obj.get("corpora")
    if not isinstance(corpora, list):
        raise ValueError(f"Registry {reg_path} missing corpora[]")
    out: List[Dict[str, Any]] = []
    for c in corpora:
        if isinstance(c, dict) and isinstance(c.get("corpus_id"), str) and c["corpus_id"].strip():
            out.append(c)
    return out


def load_provenance(prov_path: Path) -> Optional[Dict[str, Any]]:
    if not prov_path.exists():
        return None
    obj = read_json_bomsafe(prov_path)
    return obj if isinstance(obj, dict) else None


def format_bool(b: Optional[bool]) -> str:
    if b is True:
        return "yes"
    if b is False:
        return "no"
    return ""


def md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ").strip()


def build_catalogue(
    *,
    root: Path,
    registries: List[Path],
    provenance_dir: Path,
    include_local_only: bool,
) -> Dict[str, Any]:
    seen: Dict[str, Dict[str, Any]] = {}

    for reg_path in registries:
        corpora = load_registry(reg_path)
        for c in corpora:
            cid = c["corpus_id"].strip()
            if cid in seen:
                # keep first occurrence; this avoids accidental override across registries
                continue
            seen[cid] = {"registry_path": reg_path, "entry": c}

    rows: List[Dict[str, Any]] = []
    for cid in sorted(seen.keys()):
        reg_path = seen[cid]["registry_path"]
        c = seen[cid]["entry"]

        title = (c.get("title") or "").strip()
        rights = c.get("rights") if isinstance(c.get("rights"), dict) else {}
        tier = (rights.get("tier") or "").strip()
        license_str = (rights.get("license") or "").strip()
        distributable = rights.get("distributable", None)

        if (distributable is False) and (not include_local_only):
            continue

        canon = c.get("canon_jsonl") if isinstance(c.get("canon_jsonl"), dict) else {}
        canon_path_s = (canon.get("path") or "").strip()
        canon_sha_s = (canon.get("sha256") or "").strip()

        canon_path = (root / canon_path_s).resolve() if canon_path_s else None
        canon_exists = bool(canon_path and canon_path.exists())
        canon_sha_disk = sha256_file(canon_path) if canon_exists else None
        canon_sha_match = (canon_sha_disk.lower() == canon_sha_s.lower()) if (canon_sha_disk and canon_sha_s) else None

        prov_path = provenance_dir / f"{cid}.json"
        prov = load_provenance(prov_path)

        prov_sources_n = None
        prov_processing_n = None
        prov_local_only_note_ok = None
        prov_rights = None
        if prov:
            srcs = prov.get("sources")
            procs = prov.get("processing")
            prov_sources_n = len(srcs) if isinstance(srcs, list) else 0
            prov_processing_n = len(procs) if isinstance(procs, list) else 0
            prov_rights = prov.get("rights") if isinstance(prov.get("rights"), dict) else {}
            if distributable is False:
                notes = ""
                if isinstance(prov_rights, dict):
                    notes = (prov_rights.get("notes") or "")
                notes2 = (prov.get("notes") or "") if isinstance(prov.get("notes"), str) else ""
                blob = f"{notes}\n{notes2}".lower()
                prov_local_only_note_ok = ("not for redistribution" in blob)

        rows.append(
            {
                "corpus_id": cid,
                "title": title,
                "rights": {
                    "tier": tier,
                    "license": license_str,
                    "distributable": distributable,
                },
                "registry": relpath_from_root(root, reg_path),
                "canon_jsonl": {
                    "path": canon_path_s,
                    "sha256_registry": canon_sha_s,
                    "exists": canon_exists,
                    "sha256_disk": canon_sha_disk,
                    "sha256_match": canon_sha_match,
                },
                "provenance": {
                    "path": relpath_from_root(root, prov_path),
                    "exists": prov is not None,
                    "sources_n": prov_sources_n,
                    "processing_n": prov_processing_n,
                    "local_only_note_ok": prov_local_only_note_ok,
                },
            }
        )

    return {
        "generated_utc": utc_now_iso(),
        "root": str(root).replace("\\", "/"),
        "registries": [relpath_from_root(root, p) for p in registries],
        "provenance_dir": relpath_from_root(root, provenance_dir),
        "corpora": rows,
    }


def render_md(cat: Dict[str, Any]) -> str:
    rows = cat["corpora"]
    dist = [r for r in rows if r["rights"]["distributable"] is True]
    local = [r for r in rows if r["rights"]["distributable"] is False]
    unk = [r for r in rows if r["rights"]["distributable"] not in (True, False)]

    def table(rs: List[Dict[str, Any]]) -> str:
        header = (
            "| corpus_id | title | tier | license | distributable | canon_jsonl | sha_match | prov | sources | processing |\n"
            "|---|---|---|---|---|---|---|---|---:|---:|\n"
        )
        lines = []
        for r in rs:
            cj = r["canon_jsonl"]
            pv = r["provenance"]
            lines.append(
                "| {cid} | {title} | {tier} | {lic} | {dist} | {path} | {sha} | {prov} | {sn} | {pn} |".format(
                    cid=md_escape(r["corpus_id"]),
                    title=md_escape(r["title"] or ""),
                    tier=md_escape(r["rights"]["tier"] or ""),
                    lic=md_escape(r["rights"]["license"] or ""),
                    dist=format_bool(r["rights"]["distributable"]),
                    path=md_escape(cj["path"] or ""),
                    sha=("" if cj["sha256_match"] is None else ("yes" if cj["sha256_match"] else "NO")),
                    prov=("yes" if pv["exists"] else "NO"),
                    sn=(pv["sources_n"] if pv["sources_n"] is not None else ""),
                    pn=(pv["processing_n"] if pv["processing_n"] is not None else ""),
                )
            )
        return header + "\n".join(lines) + "\n"

    out = []
    out.append(f"# Sources Catalogue\n")
    out.append(f"- Generated (UTC): `{cat['generated_utc']}`\n")
    out.append(f"- Registries: {', '.join('`'+x+'`' for x in cat['registries'])}\n")
    out.append(f"- Provenance dir: `{cat['provenance_dir']}`\n")
    out.append("")
    out.append(f"## Distributable corpora ({len(dist)})\n")
    out.append(table(dist) if dist else "_None._\n")
    out.append(f"## Local-only corpora ({len(local)})\n")
    out.append(table(local) if local else "_None._\n")
    if unk:
        out.append(f"## Unspecified distributable flag ({len(unk)})\n")
        out.append(table(unk))
    return "\n".join(out).rstrip() + "\n"


def atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument(
        "--registry",
        action="append",
        required=True,
        help="Registry JSON (repeatable). Example: --registry docs/corpora.public.json",
    )
    ap.add_argument("--provenance-dir", default="docs/provenance")
    ap.add_argument("--out-json", default="docs/SOURCES_CATALOGUE.json")
    ap.add_argument("--out-md", default="docs/SOURCES_CATALOGUE.md")
    ap.add_argument("--include-local-only", action="store_true", help="Include distributable=false corpora")
    ap.add_argument("--check", action="store_true", help="Exit nonzero if outputs would change")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    registries = [Path(r) if Path(r).is_absolute() else (root / r) for r in args.registry]
    provenance_dir = Path(args.provenance_dir) if Path(args.provenance_dir).is_absolute() else (root / args.provenance_dir)

    cat = build_catalogue(
        root=root,
        registries=[p.resolve() for p in registries],
        provenance_dir=provenance_dir.resolve(),
        include_local_only=bool(args.include_local_only),
    )

    out_json = Path(args.out_json) if Path(args.out_json).is_absolute() else (root / args.out_json)
    out_md = Path(args.out_md) if Path(args.out_md).is_absolute() else (root / args.out_md)

    json_text = json.dumps(cat, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    md_text = render_md(cat)

    if args.check:
        ok = True
        if out_json.exists() and out_json.read_text(encoding="utf-8") != json_text:
            ok = False
        if out_md.exists() and out_md.read_text(encoding="utf-8") != md_text:
            ok = False
        if not ok:
            print("SOURCES_CATALOGUE is out of date. Run update_sources_catalogue.py to regenerate.")
            return 2
        print("SOURCES_CATALOGUE up to date.")
        return 0

    atomic_write(out_json, json_text)
    atomic_write(out_md, md_text)

    print(json.dumps({"ok": True, "out_json": str(out_json), "out_md": str(out_md), "count": len(cat["corpora"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())