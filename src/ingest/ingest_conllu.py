# File: src/ingest/ingest_conllu.py
# Purpose: CoNLL-U -> canonical JSONL (one segment per sentence)

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


def _minijson(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _safe_local_id(s: str) -> str:
    s = s.strip()
    s = s.replace(":", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._()-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "sent"


def _is_mwt_or_empty_node(tok_id: str) -> bool:
    return ("-" in tok_id) or ("." in tok_id)


@dataclass
class Sent:
    sent_id: Optional[str]
    text: Optional[str]
    text_en: Optional[str]
    tokens: List[Tuple[str, str, str, str, str, str, str, str, str, str]]  # 10 cols
    file_rel: str


def iter_conllu_sents(path: Path, file_rel: str) -> Iterator[Sent]:
    sent_id = None
    text = None
    text_en = None
    tokens: List[Tuple[str, ...]] = []

    def flush():
        nonlocal sent_id, text, text_en, tokens
        if sent_id is None and text is None and not tokens:
            return
        yield Sent(sent_id=sent_id, text=text, text_en=text_en, tokens=tokens, file_rel=file_rel)
        sent_id = None
        text = None
        text_en = None
        tokens = []

    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.rstrip("\n")
        if not line.strip():
            yield from flush()
            continue

        if line.startswith("#"):
            # comments like: "# sent_id = ..." / "# text = ..." / "# text_en = ..."
            m = re.match(r"^#\s*([A-Za-z0-9_.-]+)\s*=\s*(.*)\s*$", line)
            if m:
                k = m.group(1)
                v = m.group(2)
                if k == "sent_id":
                    sent_id = v
                elif k == "text":
                    text = v
                elif k == "text_en":
                    text_en = v
            continue

        cols = line.split("\t")
        if len(cols) != 10:
            # skip malformed token line; sentence will still flush
            continue
        if _is_mwt_or_empty_node(cols[0]):
            continue
        tokens.append(tuple(cols))  # type: ignore[arg-type]

    # last sentence
    yield from flush()


def reconstruct_text(tokens: List[Tuple[str, ...]]) -> str:
    # Conservative reconstruction using SpaceAfter=No when present in MISC.
    out: List[str] = []
    for cols in tokens:
        form = cols[1]
        misc = cols[9]
        out.append(form)
        if "SpaceAfter=No" in misc:
            continue
        out.append(" ")
    s = "".join(out).strip()
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--in", dest="inp", required=True, help="Input file or directory")
    ap.add_argument("--glob", default="*.conllu", help="Glob when --in is a directory")
    ap.add_argument("--corpus-id", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--lang", default="ang")
    ap.add_argument("--with-tokens", action="store_true", help="Store token annotations in meta (larger JSONL)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    inp = Path(args.inp)
    inp_abs = inp if inp.is_absolute() else (root / inp).resolve()

    outp = Path(args.out)
    out_abs = outp if outp.is_absolute() else (root / outp).resolve()
    out_abs.parent.mkdir(parents=True, exist_ok=True)

    files: List[Path] = []
    if inp_abs.is_file():
        files = [inp_abs]
    else:
        files = sorted(inp_abs.glob(args.glob))

    if not files:
        raise SystemExit(f"[ERR] no input files matched: {inp_abs} / {args.glob}")

    seen: Dict[str, int] = {}
    n = 0

    with out_abs.open("w", encoding="utf-8", newline="\n") as w:
        for f in files:
            file_rel = str(f.resolve().relative_to(root.resolve())).replace("\\", "/") if str(f).startswith(str(root)) else str(f)
            for s in iter_conllu_sents(f, file_rel=file_rel):
                n += 1
                base_id = _safe_local_id(s.sent_id or f"{f.stem}.{n:06d}")
                k = seen.get(base_id, 0) + 1
                seen[base_id] = k
                local_id = base_id if k == 1 else f"{base_id}__{k}"

                txt = s.text if s.text else reconstruct_text(s.tokens)

                meta = {
                    "lang": args.lang,
                    "conllu": {"file": s.file_rel, "sent_id": s.sent_id, "tok_count": len(s.tokens)},
                }
                if s.text_en:
                    meta["text_en"] = s.text_en
                if args.with_tokens:
                    # store compact token tuples: (form, lemma, upos, feats, head, deprel, misc)
                    toks = []
                    for cols in s.tokens:
                        toks.append({
                            "id": cols[0],
                            "form": cols[1],
                            "lemma": cols[2],
                            "upos": cols[3],
                            "feats": cols[5],
                            "head": cols[6],
                            "deprel": cols[7],
                            "misc": cols[9],
                        })
                    meta["ud"] = toks

                rec = {
                    "corpus_id": args.corpus_id,
                    "local_id": local_id,
                    "id": f"{args.corpus_id}:{local_id}",
                    "work_id": "OEDT_UD",
                    "loc": s.sent_id,
                    "text": txt,
                    "meta": meta,
                }
                w.write(_minijson(rec) + "\n")

    print(str(out_abs))
    print(f"[OK] sents={n} files={len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())