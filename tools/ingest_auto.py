#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, re, subprocess, sys, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def run(cmd: List[str], cwd: Path) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)

def read_head_text(p: Path, max_bytes: int = 250_000) -> str:
    b = p.read_bytes()[:max_bytes]
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    try:
        return b.decode("utf-8", errors="replace")
    except Exception:
        return b.decode("utf-8", errors="replace")

def slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9._-]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "corpus"

def discover_rome_min(root: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # --- Smith dictionaries (known fixed paths) ---
    smith_base = root / "data_raw" / "public" / "perseus" / "canonical-pdlrefwk" / "data" / "viaf88890045"
    smith = [
        ("rome_smith_antiquities",
         "Smith, Dictionary of Greek and Roman Antiquities (Perseus TEI)",
         smith_base / "001" / "viaf88890045.001.xml"),
        ("rome_smith_geography",
         "Smith, Dictionary of Greek and Roman Geography (Perseus TEI)",
         smith_base / "002" / "viaf88890045.002.xml"),
        ("rome_smith_biography",
         "Smith, Dictionary of Greek and Roman Biography and Mythology (Perseus TEI)",
         smith_base / "003" / "viaf88890045.003.perseus-eng1.xml"),
    ]

    for cid, title, in_path in smith:
        if in_path.exists():
            items.append({
                "corpus_id": cid,
                "title": title,
                "kind": "tei_xml",
                "lang": "eng",
                "work": title,
                "in": str(in_path.relative_to(root)).replace("\\","/"),
                "out": f"data_proc/public/rome/{cid}.jsonl",
                "rights": {
                    "tier": "A_open_license",
                    "license": "CC-BY-SA-4.0 (PerseusDL/canonical-pdlrefwk)",
                    "distributable": True,
                }
            })

    # --- Heuristic discovery: Vitruvius + Frontinus in canonical-latinLit ---
    latin_root = root / "data_raw" / "public" / "perseus" / "canonical-latinLit" / "data"
    if latin_root.exists():
        want = [
            ("rome_vitruvius_de_architectura", r"\bVitruvius\b", "Vitruvius, De Architectura (Perseus TEI)"),
            ("rome_frontinus_de_aquaeductu", r"\bFrontinus\b", "Frontinus, De Aquaeductu (Perseus TEI)"),
        ]
        # scan TEI heads only; stop once each is found
        found: Dict[str, Path] = {}
        for fp in latin_root.rglob("*.xml"):
            if len(found) == len(want):
                break
            head = read_head_text(fp)
            for cid, pat, title in want:
                if cid in found:
                    continue
                if re.search(pat, head, flags=re.IGNORECASE):
                    found[cid] = fp

        for cid, pat, title in want:
            fp = found.get(cid)
            if fp and fp.exists():
                items.append({
                    "corpus_id": cid,
                    "title": title,
                    "kind": "tei_xml",
                    "lang": "lat",
                    "work": title,
                    "in": str(fp.relative_to(root)).replace("\\","/"),
                    "out": f"data_proc/public/rome/{cid}.jsonl",
                    "rights": {
                        "tier": "A_open_license",
                        "license": "CC-BY-SA-4.0 (PerseusDL/canonical-latinLit)",
                        "distributable": True,
                    }
                })

    return items

def discover_generic_txt(root: Path) -> List[Dict[str, Any]]:
    # Optional: auto-ingest any .txt under data_raw/public/txt as its own corpus
    base = root / "data_raw" / "public" / "txt"
    out: List[Dict[str, Any]] = []
    if not base.exists():
        return out
    for fp in base.rglob("*.txt"):
        cid = "txt_" + slug(fp.stem)
        out.append({
            "corpus_id": cid,
            "title": fp.stem,
            "kind": "txt_plain",
            "lang": "und",
            "work": fp.stem,
            "in": str(fp.relative_to(root)).replace("\\","/"),
            "out": f"data_proc/public/txt/{cid}.jsonl",
            "rights": {
                "tier": "B_unknown",
                "license": "unknown",
                "distributable": False,
            },
            "split": "paragraphs",
            "min_chars": 40,
        })
    return out

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="rome_min", choices=["rome_min"])
    ap.add_argument("--include-public-txt", action="store_true", help="Also ingest data_raw/public/txt/*.txt automatically")
    ap.add_argument("--harvest", default="", help="Override harvest output path (default reports/harvest_<profile>.json)")
    ap.add_argument("--min-chars", type=int, default=40, help="Pass through to TEI importer / txt ingester")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    importer = root / "tools" / "import_perseus_tei_simple.py"
    if not importer.exists():
        print(f"[FATAL] missing TEI importer: {importer}", file=sys.stderr)
        return 2

    items: List[Dict[str, Any]] = []
    if args.profile == "rome_min":
        items.extend(discover_rome_min(root))

    if args.include_public_txt:
        items.extend(discover_generic_txt(root))

    if not items:
        print("[FATAL] discovered 0 corpora. Verify your Perseus clones exist under data_raw/public/perseus/.", file=sys.stderr)
        return 2

    harvest_path = (root / args.harvest).resolve() if args.harvest else (root / "reports" / f"harvest_{args.profile}.json")

    produced: List[Dict[str, Any]] = []
    for it in items:
        cid = it["corpus_id"]
        kind = it["kind"]
        in_path = (root / Path(it["in"])).resolve()
        out_path = (root / Path(it["out"])).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if not in_path.exists():
            print(f"[SKIP] missing input: {cid} -> {in_path}", file=sys.stderr)
            continue

        t0 = time.time()

        if kind == "tei_xml":
            cmd = [
                sys.executable, str(importer),
                "--corpus", cid,
                "--lang", it.get("lang","und"),
                "--work", it.get("work", it.get("title", cid)),
                "--in", str(in_path),
                "--out", str(out_path),
                "--min-chars", str(int(args.min_chars)),
            ]
            run(cmd, cwd=root)
        elif kind == "txt_plain":
            # delegate to ingest_manifest.py logic by calling it as a module-like script would be overkill;
            # implement minimal deterministic txt->jsonl here by reusing importer conventions:
            raw = in_path.read_text(encoding="utf-8", errors="replace").replace("\r\n","\n").replace("\r","\n")
            split = it.get("split","paragraphs")
            if split == "lines":
                chunks = [x.strip() for x in raw.split("\n")]
            else:
                chunks = [x.strip() for x in re.split(r"\n\s*\n+", raw)]
            chunks = [" ".join(x.split()) for x in chunks if x and len(" ".join(x.split())) >= int(args.min_chars)]

            import hashlib
            import json as _json
            def sha256_txt(s: str) -> str:
                return hashlib.sha256(s.encode("utf-8")).hexdigest()

            def sanitize_local_id(s: str) -> str:
                s = (s or "").strip().replace(":", "_")
                s = re.sub(r"\s+", "_", s)
                s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
                s = re.sub(r"_+", "_", s)
                return s.strip("_") or "seg"

            n = 0
            with out_path.open("w", encoding="utf-8", newline="\n") as f:
                for i, txt in enumerate(chunks, start=1):
                    local_id = sanitize_local_id(f"p.{i:06d}")
                    loc = f"{in_path.name}#p{i:06d}"
                    rec = {
                        "id": local_id,
                        "src": cid,
                        "work": it.get("work", it.get("title", cid)),
                        "loc": loc,
                        "srcp": loc,
                        "lang": it.get("lang","und"),
                        "txt": txt,
                        "sha256": sha256_txt(txt),
                    }
                    f.write(_json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                    n += 1
            print(f"[OK] {cid} txt_plain records={n}")
        else:
            print(f"[SKIP] unsupported kind={kind} for {cid}", file=sys.stderr)
            continue

        if not out_path.exists():
            print(f"[FATAL] ingest produced no output: {cid} -> {out_path}", file=sys.stderr)
            return 2

        # count records quickly
        recs = sum(1 for _ in out_path.open("r", encoding="utf-8", errors="replace"))
        dt = round(time.time() - t0, 2)
        print(f"[OK] {cid} records={recs} out={it['out']} ({dt}s)")

        produced.append({
            "corpus_id": cid,
            "title": it.get("title", cid),
            "canon_jsonl": {"path": it["out"].replace("\\","/")},
            "rights": it.get("rights", None),
            "meta": {
                "kind": kind,
                "in": it["in"].replace("\\","/"),
                "lang": it.get("lang","und"),
                "work": it.get("work", it.get("title", cid)),
                "record_count": recs,
            }
        })

    if not produced:
        print("[FATAL] nothing ingested (all inputs missing?)", file=sys.stderr)
        return 2

    harvest = {"generated_utc": utc_now(), "profile": args.profile, "items": produced}
    jwrite_min(harvest_path, harvest)
    print(f"[OK] wrote harvest -> {harvest_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())