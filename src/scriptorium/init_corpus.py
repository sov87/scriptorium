from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_utf8_nobom(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8-sig"))


def dump_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


@dataclass(frozen=True)
class InitPaths:
    provenance: Path
    sources: Path
    ingest_stub: Path
    corpora_json: Path
    canon_placeholder: Path


def ensure_registry_entry(root: Path, corpus_id: str, title: str, canon_rel: str) -> None:
    corpora_path = root / "docs" / "corpora.json"
    j = load_json(corpora_path)
    if not j:
        j = {
            "schema": "scriptorium.corpora.v1",
            "generated_utc": utc_now(),
            "corpora": [],
        }
    if "corpora" not in j or not isinstance(j["corpora"], list):
        j["corpora"] = []

    # remove any accidental blank corpus_id entries
    j["corpora"] = [c for c in j["corpora"] if isinstance(c, dict) and c.get("corpus_id")]

    found = None
    for c in j["corpora"]:
        if c.get("corpus_id") == corpus_id:
            found = c
            break

    if not found:
        found = {"corpus_id": corpus_id}
        j["corpora"].append(found)

    found["title"] = title
    found["canon_jsonl"] = {"path": canon_rel}
    found.setdefault("rights", {"tier": "UNSET", "license": "", "distributable": False})
    found.setdefault("bm25", {"path": ""})
    found.setdefault("faiss", {"index_path": "", "ids_path": "", "meta_path": ""})
    j["generated_utc"] = utc_now()

    write_utf8_nobom(corpora_path, dump_json(j))


def init_corpus(root: Path, corpus_id: str, title: str) -> InitPaths:
    root = root.resolve()
    prov = root / "docs" / "provenance" / f"{corpus_id}.json"
    srcs = root / "docs" / "sources" / f"{corpus_id}.json"
    stub = root / "src" / "ingest" / f"ingest_{corpus_id}.py"
    corp = root / "docs" / "corpora.json"
    canon = root / "data_proc" / f"{corpus_id}_prod.jsonl"

    # provenance skeleton
    if not prov.exists():
        prov_obj = {
            "schema": "scriptorium.provenance.v1",
            "generated_utc": utc_now(),
            "corpus_id": corpus_id,
            "title": title,
            "rights": {"tier": "UNSET", "license": "", "distributable": False},
            "sources": [],
            "processing": [],
            "outputs": {
                "canon_jsonl": {"path": f"data_proc/{corpus_id}_prod.jsonl", "sha256": ""},
                "bm25": {"path": "", "sha256": ""},
                "faiss": {"index_path": "", "ids_path": "", "meta_path": "", "sha256": {"index": "", "ids": "", "meta": ""}},
            },
            "notes": [],
        }
        write_utf8_nobom(prov, dump_json(prov_obj))

    # sources skeleton
    if not srcs.exists():
        src_obj = {
            "schema": "scriptorium.sources.v1",
            "generated_utc": utc_now(),
            "corpus_id": corpus_id,
            "items": [],
        }
        write_utf8_nobom(srcs, dump_json(src_obj))

    # ingestion stub
    if not stub.exists():
        stub_text = f'''from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--in", dest="in_path", required=True, help="Input file/dir under data_raw or elsewhere (local-only).")
    ap.add_argument("--out", required=True, help="Output canon JSONL under data_proc.")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    in_path = Path(args.in_path)
    if not in_path.is_absolute():
        in_path = (root / in_path).resolve()
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # TODO: parse source into segments. Emit JSONL records with stable ids.
    # Each record should minimally include:
    # id, corpus_id, work_id, witness_id, edition_id, loc, lang, text, text_norm, source_refs, notes

    raise SystemExit("ingest stub not implemented for {corpus_id}")


if __name__ == "__main__":
    raise SystemExit(main())
'''
        write_utf8_nobom(stub, stub_text)

    # placeholder canon file (empty) if missing
    if not canon.exists():
        canon.parent.mkdir(parents=True, exist_ok=True)
        canon.write_text("", encoding="utf-8", newline="\n")

    # ensure registry points at placeholder canon
    ensure_registry_entry(root, corpus_id=corpus_id, title=title, canon_rel=f"data_proc/{corpus_id}_prod.jsonl")

    return InitPaths(provenance=prov, sources=srcs, ingest_stub=stub, corpora_json=corp, canon_placeholder=canon)
