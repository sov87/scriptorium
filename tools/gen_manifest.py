#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, re, time, glob
from pathlib import Path
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

# --- TEI sanitization (must handle Perseus weird entity decls) ---

_DOCTYPE_RE = re.compile(r"<!DOCTYPE[^>]*(\[[\s\S]*?\])?>", re.IGNORECASE)
_CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_ENTITY_DECL_RE = re.compile(
    r"<!ENTITY\s+([A-Za-z][A-Za-z0-9._-]*)\s+(\"[^\"]*\"|'[^']*')\s*>",
    re.IGNORECASE,
)
_ENTITY_REF_RE = re.compile(r"&([A-Za-z][A-Za-z0-9._-]*);")

# Minimal built-ins only (no external deps); unknowns become placeholders
_XML_BUILTINS = {"amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'"}

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def sanitize_id(s: str) -> str:
    s = (s or "").strip().replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "corpus"

def read_and_sanitize_xml_text(p: Path) -> str:
    raw = p.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    text = _CTRL_RE.sub("", text)

    # extract custom entity decls
    custom: Dict[str, str] = {}
    for m in _ENTITY_DECL_RE.finditer(text):
        name = m.group(1)
        val = m.group(2)
        val = val[1:-1] if val and (val[0] in ("'", '"')) else val
        custom[name] = val
    text = _ENTITY_DECL_RE.sub("", text)

    text = _DOCTYPE_RE.sub("", text)

    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name in _XML_BUILTINS:
            return _XML_BUILTINS[name]
        if name in custom:
            return custom[name]
        return f"[ENTITY:{name}]"

    text = _ENTITY_REF_RE.sub(repl, text)
    return text

def tei_title(p: Path) -> Optional[str]:
    try:
        xml_text = read_and_sanitize_xml_text(p)
        root = ET.fromstring(xml_text.encode("utf-8"))
    except Exception:
        return None

    # try TEI header titleStmt/title first
    for xp in [
        ".//{*}teiHeader//{*}titleStmt//{*}title",
        ".//{*}titleStmt//{*}title",
        ".//{*}title",
    ]:
        n = root.find(xp)
        if n is not None and (n.text or "").strip():
            t = " ".join((n.text or "").split())
            if t:
                return t
    return None

def expand_inputs(root: Path, specs: List[str], exts: List[str]) -> List[Path]:
    out: List[Path] = []
    for spec in specs:
        spec = spec.strip()
        if not spec:
            continue
        # allow globs
        if any(ch in spec for ch in ["*", "?", "["]):
            for m in glob.glob(str((root / spec).resolve())):
                p = Path(m)
                if p.is_file():
                    out.append(p)
            continue
        p = (root / spec).resolve()
        if p.is_dir():
            for e in exts:
                out.extend(sorted(p.rglob(f"*{e}")))
        elif p.is_file():
            out.append(p)
    # de-dup preserving order
    seen = set()
    uniq: List[Path] = []
    for p in out:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            uniq.append(p)
    return uniq

def smith_preset_id_title(file_name: str) -> Optional[Dict[str, str]]:
    # Known Smith dictionaries in canonical-pdlrefwk
    if file_name.startswith("viaf88890045.001"):
        return {"suffix": "smith_antiquities", "title": "Smith, Dictionary of Greek and Roman Antiquities (Perseus TEI)"}
    if file_name.startswith("viaf88890045.002"):
        return {"suffix": "smith_geography", "title": "Smith, Dictionary of Greek and Roman Geography (Perseus TEI)"}
    if file_name.startswith("viaf88890045.003"):
        return {"suffix": "smith_biography", "title": "Smith, Dictionary of Greek and Roman Biography and Mythology (Perseus TEI)"}
    return None

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, help="Manifest name (also default filename)")
    ap.add_argument("--scan", action="append", required=True, help="Relative path/dir/glob to include (repeatable)")
    ap.add_argument("--out", default="", help="Output manifest path (default manifests/<name>.json)")
    ap.add_argument("--kind", default="tei_xml", choices=["tei_xml", "txt_plain"])
    ap.add_argument("--lang", default="und")
    ap.add_argument("--work-prefix", default="", help="Optional prefix added to work/title")
    ap.add_argument("--corpus-prefix", default="", help="Prefix for corpus_id (e.g., rome_)")
    ap.add_argument("--namespace", default="misc", help="Output subfolder under data_proc/<tier>/<namespace>/")
    ap.add_argument("--tier", default="public", choices=["public", "private"])
    ap.add_argument("--rights-tier", default="A_open_license")
    ap.add_argument("--rights-license", default="")
    ap.add_argument("--distributable", default="true", choices=["true","false"])
    ap.add_argument("--ext", action="append", default=[], help="File extension filter when scanning dirs (repeatable), default for tei_xml=.xml")
    ap.add_argument("--preset", default="", choices=["", "smith_viaf88890045"], help="Optional preset for nicer corpus_ids/titles")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_path = (root / args.out).resolve() if args.out else (root / "manifests" / f"{args.name}.json")

    exts = args.ext[:] if args.ext else ([".xml"] if args.kind == "tei_xml" else [".txt"])
    files = expand_inputs(root, args.scan, exts)
    if not files:
        raise SystemExit("[FATAL] scan matched 0 files")

    distributable = (args.distributable.lower() == "true")
    rights = {
        "tier": args.rights_tier,
        "license": args.rights_license,
        "distributable": distributable,
    }

    corpora: List[Dict[str, Any]] = []

    for fp in files:
        rel_in = fp.resolve().relative_to(root.resolve()).as_posix()

        preset = None
        if args.preset == "smith_viaf88890045":
            preset = smith_preset_id_title(fp.name)

        if preset:
            corpus_id = sanitize_id(args.corpus_prefix + preset["suffix"])
            title = preset["title"]
            work = args.work_prefix + title if args.work_prefix else title
        else:
            # default: derive from file stem
            stem = fp.stem
            corpus_id = sanitize_id(args.corpus_prefix + stem.replace(".", "_"))
            title = None
            if args.kind == "tei_xml":
                title = tei_title(fp)
            if not title:
                title = stem
            if args.work_prefix:
                title = f"{args.work_prefix}{title}"
            work = title

        out_rel = Path("data_proc") / args.tier / args.namespace / f"{corpus_id}.jsonl"

        corpora.append({
            "corpus_id": corpus_id,
            "title": title,
            "kind": args.kind,
            "lang": args.lang,
            "work": work,
            "in": rel_in,
            "out": out_rel.as_posix(),
            "rights": rights,
        })

    manifest = {"name": args.name, "generated_utc": utc_now(), "corpora": corpora}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

    print(f"[OK] wrote manifest -> {out_path}")
    print(f"[OK] corpora={len(corpora)} kind={args.kind} tier={args.tier} namespace={args.namespace}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())