#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, hashlib, re, subprocess, sys, time
from pathlib import Path
from typing import Any, Dict, List, Optional

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def jload(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def sha256_txt(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def norm_ws(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def sanitize_local_id(s: str) -> str:
    s = (s or "").strip().replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "seg"

def run(cmd: List[str], cwd: Path) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)

def ingest_txt_plain(root: Path, it: Dict[str, Any]) -> int:
    corpus_id = it["corpus_id"]
    lang = it.get("lang","und")
    work = it.get("work", it.get("title", corpus_id))

    in_path = (root / Path(it["in"])).resolve()
    out_path = (root / Path(it["out"])).resolve()

    split = it.get("split", "paragraphs")  # paragraphs|lines
    min_chars = int(it.get("min_chars", 40))

    raw = in_path.read_text(encoding="utf-8", errors="replace")
    raw = raw.replace("\r\n","\n").replace("\r","\n")

    if split == "lines":
        chunks = [x.strip() for x in raw.split("\n")]
    else:
        # paragraph split (blank-line)
        chunks = [x.strip() for x in re.split(r"\n\s*\n+", raw)]

    chunks = [norm_ws(c) for c in chunks if norm_ws(c)]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for i, txt in enumerate(chunks, start=1):
            if len(txt) < min_chars:
                continue
            local_id = sanitize_local_id(f"p.{i:06d}")
            loc = f"{in_path.name}#p{i:06d}"
            rec = {
                "id": local_id,
                "src": corpus_id,
                "work": work,
                "loc": loc,
                "srcp": loc,
                "lang": lang,
                "txt": txt,
                "sha256": sha256_txt(txt),
            }
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    return n

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="JSON manifest describing corpora to ingest")
    ap.add_argument("--harvest", default="", help="Optional harvest report path (default reports/harvest_<name>.json)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    man_path = (root / Path(args.manifest)).resolve()
    man = jload(man_path)

    name = man.get("name") or man_path.stem
    harvest_path = (root / args.harvest).resolve() if args.harvest else (root / "reports" / f"harvest_{name}.json")

    corpora = man.get("corpora", [])
    if not isinstance(corpora, list) or not corpora:
        raise SystemExit("[FATAL] manifest.corpora must be a non-empty list")

    importer = (root / "tools" / "import_perseus_tei_simple.py").resolve()

    results: List[Dict[str, Any]] = []
    for it in corpora:
        cid = it.get("corpus_id","").strip()
        if not cid:
            raise SystemExit("[FATAL] corpus missing corpus_id")
        if ":" in cid:
            raise SystemExit(f"[FATAL] corpus_id must not contain ':' -> {cid}")

        kind = it.get("kind","").strip()
        if kind not in ("tei_xml","txt_plain"):
            raise SystemExit(f"[FATAL] unsupported kind={kind} for {cid} (supported: tei_xml, txt_plain)")

        in_rel = it.get("in","")
        out_rel = it.get("out","")
        if not in_rel or not out_rel:
            raise SystemExit(f"[FATAL] {cid} must define 'in' and 'out'")

        in_path = (root / Path(in_rel)).resolve()
        out_path = (root / Path(out_rel)).resolve()
        if not in_path.exists():
            raise SystemExit(f"[FATAL] missing input for {cid}: {in_path}")

        t0 = time.time()

        if kind == "tei_xml":
            if not importer.exists():
                raise SystemExit(f"[FATAL] missing TEI importer: {importer}")
            cmd = [
                sys.executable, str(importer),
                "--corpus", cid,
                "--lang", str(it.get("lang","und")),
                "--work", str(it.get("work", it.get("title", cid))),
                "--in", str(in_path),
                "--out", str(out_path),
            ]
            if "min_chars" in it:
                cmd += ["--min-chars", str(int(it["min_chars"]))]
            run(cmd, cwd=root)
            # count lines cheaply
            recs = sum(1 for _ in out_path.open("r", encoding="utf-8", errors="replace"))
        else:
            recs = ingest_txt_plain(root, it)

        if not out_path.exists():
            raise SystemExit(f"[FATAL] ingest produced no output for {cid}: {out_path}")

        dt = round(time.time() - t0, 2)
        print(f"[OK] {cid} kind={kind} records={recs} out={out_rel} ({dt}s)")

        results.append({
            "corpus_id": cid,
            "title": it.get("title", cid),
            "kind": kind,
            "in": in_rel.replace("\\","/"),
            "out": out_rel.replace("\\","/"),
            "lang": it.get("lang","und"),
            "work": it.get("work", it.get("title", cid)),
            "rights": it.get("rights", None),
            "record_count": recs,
        })

    harvest = {"generated_utc": utc_now(), "name": name, "items": results}
    jwrite_min(harvest_path, harvest)
    print(f"[OK] wrote harvest -> {harvest_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())