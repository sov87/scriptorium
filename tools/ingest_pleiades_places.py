#!/usr/bin/env python3
from __future__ import annotations

import argparse, gzip, hashlib, json, time, urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PLEIADES_LATEST = "https://atlantides.org/downloads/pleiades/json/pleiades-places-latest.json.gz"

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def sha256_txt(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def jwrite_min(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8", newline="\n")

def download(url: str, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent":"scriptorium-vivarium"})
    with urllib.request.urlopen(req, timeout=240) as r:
        out.write_bytes(r.read())

def _largest_list_in_dict(d: Dict[str, Any]) -> Optional[List[Any]]:
    best = None
    best_len = -1
    for v in d.values():
        if isinstance(v, list) and len(v) > best_len:
            best = v
            best_len = len(v)
    return best

def extract_places(obj: Any) -> List[Any]:
    # common shapes:
    # - GeoJSON FeatureCollection: {"type":"FeatureCollection","features":[...]}
    # - JSON-LD graph: {"@graph":[...]}
    # - plain list: [...]
    # - other wrapper dicts: pick the largest list value as fallback
    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        for k in ["features", "@graph", "graph", "items", "places", "data"]:
            v = obj.get(k)
            if isinstance(v, list):
                return v

        if obj.get("type") == "FeatureCollection" and isinstance(obj.get("features"), list):
            return obj["features"]

        best = _largest_list_in_dict(obj)
        if isinstance(best, list):
            return best

    raise ValueError(f"unexpected JSON structure: type={type(obj).__name__} keys={list(obj.keys())[:20] if isinstance(obj, dict) else None}")

def get_id_title_desc(feature: Any) -> Tuple[str, str, str, List[str], Optional[Tuple[float,float]]]:
    # handle GeoJSON Feature or plain dict nodes
    if not isinstance(feature, dict):
        return ("", "", "", [], None)

    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else feature

    pid = props.get("id") or props.get("@id") or props.get("uri") or feature.get("id") or ""
    pid = str(pid)

    title = props.get("title") or props.get("name") or props.get("label") or ""
    title = str(title)

    desc = props.get("description") or props.get("summary") or ""
    desc = str(desc)

    # names/aliases (many possible fields)
    names: List[str] = []
    for key in ["names", "nameAttested", "name_attested", "aliases", "alt_names"]:
        v = props.get(key)
        if isinstance(v, list):
            for x in v[:20]:
                if isinstance(x, dict):
                    nm = x.get("name") or x.get("title") or x.get("label") or ""
                    if nm:
                        names.append(str(nm))
                elif isinstance(x, str):
                    names.append(x)
    # de-dup
    seen=set(); uniq=[]
    for n in names:
        n = n.strip()
        if n and n not in seen:
            seen.add(n); uniq.append(n)
    names = uniq[:12]

    # coordinates if GeoJSON
    coords = None
    geom = feature.get("geometry")
    if isinstance(geom, dict) and isinstance(geom.get("coordinates"), list) and len(geom["coordinates"]) >= 2:
        try:
            lon = float(geom["coordinates"][0])
            lat = float(geom["coordinates"][1])
            coords = (lat, lon)
        except Exception:
            coords = None

    return (pid, title, desc, names, coords)

def local_id_from_pid(pid: str) -> str:
    if not pid:
        return ""
    # often URLs like https://pleiades.stoa.org/places/423025
    s = pid.rstrip("/").split("/")[-1]
    # keep numeric ids clean
    return s.strip()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-jsonl", default="data_proc/public/vivarium/pleiades/viv_pleiades_places.jsonl")
    ap.add_argument("--raw-gz", default="data_raw/public/pleiades/pleiades-places-latest.json.gz")
    ap.add_argument("--corpus-id", default="viv_pleiades_places")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    gz_path = (root / args.raw_gz).resolve()
    out_jsonl = (root / args.out_jsonl).resolve()

    if args.refresh or (not gz_path.exists()):
        print("[RUN] downloading", PLEIADES_LATEST)
        download(PLEIADES_LATEST, gz_path)
        print("[OK] downloaded ->", gz_path)

    raw = gz_path.read_bytes()
    data = gzip.decompress(raw)
    obj = json.loads(data.decode("utf-8", errors="replace"))

    places = extract_places(obj)
    if not isinstance(places, list) or not places:
        raise SystemExit("[FATAL] extracted 0 places")

    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with out_jsonl.open("w", encoding="utf-8", newline="\n") as f:
        for feat in places:
            pid, title, desc, names, coords = get_id_title_desc(feat)
            if not pid:
                continue
            lid = local_id_from_pid(pid)
            if not lid:
                continue

            bits = []
            if title:
                bits.append(f"Title: {title}")
            bits.append(f"PleiadesID: {pid}")
            if names:
                bits.append("Names: " + "; ".join(names))
            if coords:
                lat, lon = coords
                bits.append(f"Coordinates: {lat},{lon}")
            if desc:
                bits.append("Description: " + desc)

            txt = " | ".join(bits).strip()
            if not txt:
                continue

            rec = {
                "id": f"pl.{lid}",
                "src": args.corpus_id,
                "work": "Pleiades Places (daily export)",
                "loc": title or f"pleiades:{lid}",
                "srcp": pid,
                "lang": "und",
                "txt": txt,
                "sha256": sha256_txt(txt),
            }
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1

    harvest = {
        "generated_utc": utc_now(),
        "name": "vivarium_pleiades",
        "items": [{
            "corpus_id": args.corpus_id,
            "title": "Pleiades Places (daily export)",
            "canon_jsonl": {"path": str(Path(args.out_jsonl)).replace("\\","/")},
            "rights": {"tier":"A_open_license","license":"CC BY (Pleiades)", "distributable": True},
        }]
    }
    harvest_path = root / "reports" / "harvest_vivarium_pleiades.json"
    jwrite_min(harvest_path, harvest)

    print(f"[OK] wrote {n} records -> {out_jsonl}")
    print(f"[OK] wrote harvest -> {harvest_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())