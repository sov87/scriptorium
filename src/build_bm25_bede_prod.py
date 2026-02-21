import argparse, json, pickle, re
from pathlib import Path
from rank_bm25 import BM25Okapi

WORD_RE = re.compile(r"[A-Za-z\u00C0-\u017FþðæƿȝĀĒĪŌŪȲāēīōūȳ]+", re.UNICODE)

def tokenize(s: str):
    return [m.group(0).lower() for m in WORD_RE.finditer(s)]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--out", dest="outfile", default="indexes/bm25/oe_bede_prod.pkl")
    args = ap.parse_args()

    inp = Path(args.infile)
    outp = Path(args.outfile)
    outp.parent.mkdir(parents=True, exist_ok=True)

    docs = []
    meta = []
    dropped_toc = 0

    with inp.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            flags = obj.get("flags", [])
            if isinstance(flags, list) and ("toc" in flags):
                dropped_toc += 1
                continue

            txt = obj["txt"]
            toks = tokenize(txt)
            if not toks:
                continue

            docs.append(toks)
            meta.append({
                "id": obj["id"],
                "loc": obj["loc"],
                "src": obj["src"],
                "srcp": obj["srcp"],
                "txt": txt,
                "flags": obj.get("flags", []),
                "sha256": obj["sha256"],
            })

    bm25 = BM25Okapi(docs)

    payload = {"schema":"scriptorium.bm25.v1","n":len(meta),"dropped_toc":dropped_toc,"meta":meta,"bm25":bm25}
    with outp.open("wb") as f:
        pickle.dump(payload, f)

    print(f"Wrote BM25 index: {outp} (docs={len(meta)} dropped_toc={dropped_toc})")

if __name__ == "__main__":
    main()