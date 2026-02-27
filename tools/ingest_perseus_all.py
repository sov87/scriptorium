#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, subprocess, sys, time, re
from pathlib import Path
from typing import Any, Dict, List, Tuple

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def sanitize(s: str) -> str:
    s = (s or "").strip().replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._/-]", "_", s)
    s = s.replace("/", "_")
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "corpus"

def ingest_one(root: Path, importer: Path, cid: str, lang: str, work: str, in_fp: Path, out_fp: Path, min_chars: int) -> Tuple[bool, str]:
    cmd = [
        sys.executable, str(importer),
        "--corpus", cid,
        "--lang", lang,
        "--work", work,
        "--in", str(in_fp.resolve()),
        "--out", str(out_fp.resolve()),
        "--min-chars", str(int(min_chars)),
    ]
    try:
        subprocess.run(cmd, cwd=str(root), check=True)
        return True, ""
    except subprocess.CalledProcessError as e:
        return False, f"CalledProcessError: {e}"

def ingest_tree(root: Path, importer: Path, scan_root: Path, repo_tag: str, lang: str, license_str: str,
                corpus_prefix: str, tier: str, namespace: str, min_chars: int, max_files: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    ok_items: List[Dict[str,Any]] = []
    bad_items: List[Dict[str,Any]] = []
    if not scan_root.exists():
        return ok_items, bad_items

    files = sorted(scan_root.rglob("*.xml"))
    if max_files > 0:
        files = files[:max_files]

    out_base = root / "data_proc" / tier / namespace / repo_tag
    out_base.mkdir(parents=True, exist_ok=True)

    for fp in files:
        stem = sanitize(fp.with_suffix("").relative_to(scan_root).as_posix())
        cid = sanitize(f"{corpus_prefix}{repo_tag}_{stem}")
        out_rel = f"data_proc/{tier}/{namespace}/{repo_tag}/{cid}.jsonl"
        out_abs = (root / Path(out_rel)).resolve()

        # resume: skip if output already exists and non-empty
        if out_abs.exists() and out_abs.stat().st_size > 100:
            ok_items.append({
                "corpus_id": cid,
                "title": f"{repo_tag}:{stem}",
                "canon_jsonl": {"path": out_rel},
                "rights": {"tier":"A_open_license","license":license_str,"distributable":True},
                "meta": {"kind":"tei_xml","in": str(fp.relative_to(root)).replace("\\","/"), "repo":repo_tag, "lang":lang, "skipped":"exists"},
            })
            continue

        print("[INGEST]", cid)
        success, err = ingest_one(root, importer, cid, lang, f"{repo_tag}:{stem}", fp, out_abs, min_chars)
        if success:
            ok_items.append({
                "corpus_id": cid,
                "title": f"{repo_tag}:{stem}",
                "canon_jsonl": {"path": out_rel},
                "rights": {"tier":"A_open_license","license":license_str,"distributable":True},
                "meta": {"kind":"tei_xml","in": str(fp.relative_to(root)).replace("\\","/"), "repo":repo_tag, "lang":lang},
            })
        else:
            bad_items.append({
                "corpus_id": cid,
                "title": f"{repo_tag}:{stem}",
                "in": str(fp.relative_to(root)).replace("\\","/"),
                "error": err,
            })
            print("[FAIL]", cid, err)

    return ok_items, bad_items

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="public", choices=["public","private"])
    ap.add_argument("--namespace", default="vivarium")
    ap.add_argument("--corpus-prefix", default="viv_")
    ap.add_argument("--min-chars", type=int, default=40)
    ap.add_argument("--max-files", type=int, default=0)
    ap.add_argument("--name", default="vivarium_perseus_all")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    importer = (root / "tools" / "import_perseus_tei_simple.py").resolve()
    if not importer.exists():
        raise SystemExit(f"[FATAL] missing importer: {importer}")

    base = root / "data_raw" / "public" / "perseus"
    repos = [
        ("pdlrefwk", "eng", "CC-BY-SA-4.0 (PerseusDL/canonical-pdlrefwk)", base / "canonical-pdlrefwk" / "data"),
        ("latinlit", "lat", "CC-BY-SA-4.0 (PerseusDL/canonical-latinLit)",   base / "canonical-latinLit" / "data"),
        ("greeklit", "grc", "CC-BY-SA-4.0 (PerseusDL/canonical-greekLit)",   base / "canonical-greekLit" / "data"),
    ]

    all_ok: List[Dict[str,Any]] = []
    all_bad: List[Dict[str,Any]] = []

    for repo_tag, lang, lic, scan_root in repos:
        ok, bad = ingest_tree(root, importer, scan_root, repo_tag, lang, lic,
                              args.corpus_prefix, args.tier, args.namespace, args.min_chars, args.max_files)
        print(f"[OK] repo={repo_tag} ok={len(ok)} bad={len(bad)}")
        all_ok.extend(ok)
        all_bad.extend(bad)

    harvest = {"generated_utc": utc_now(), "name": args.name, "items": all_ok, "failures": all_bad}
    harvest_path = root / "reports" / f"harvest_{args.name}.json"
    jwrite_min(harvest_path, harvest)
    print(f"[OK] wrote harvest -> {harvest_path}")
    if all_bad:
        print(f"[WARN] failures={len(all_bad)} (see harvest.failures)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())