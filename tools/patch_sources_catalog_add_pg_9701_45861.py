from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "sources_catalog.json"


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))


def write_json_pretty(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def ensure_list(d: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    v = d.get(key)
    if isinstance(v, list):
        return v  # type: ignore[return-value]
    d[key] = []
    return d[key]  # type: ignore[return-value]


def upsert_by_key(items: List[Dict[str, Any]], key: str, value: str, new_obj: Dict[str, Any]) -> None:
    for i, obj in enumerate(items):
        if obj.get(key) == value:
            merged = dict(obj)
            merged.update(new_obj)
            items[i] = merged
            return
    items.append(new_obj)


def canonicalize_source(s: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "source_id": s.get("source_id", ""),
        "type": s.get("type", ""),
    }
    if "repo" in s:
        out["repo"] = s.get("repo", "")
    if "url" in s:
        out["url"] = s.get("url", "")
    out["dest"] = s.get("dest", "")
    out["ref"] = s.get("ref", "")
    out["license"] = s.get("license", "")
    out["distributable"] = bool(s.get("distributable", False))
    out["notes"] = s.get("notes", "")
    return out


def canonicalize_ingest(i: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "corpus_id": i.get("corpus_id", ""),
        "source_id": i.get("source_id", ""),
        "enabled": bool(i.get("enabled", True)),
        "cmd": i.get("cmd", []),
        "outputs": i.get("outputs", []),
    }


def main() -> int:
    if not CATALOG_PATH.exists():
        raise SystemExit(f"missing: {CATALOG_PATH}")

    data = load_json(CATALOG_PATH)
    data["schema"] = data.get("schema", "scriptorium.sources_catalog.v1")
    data.setdefault("generated_utc", "")

    sources = ensure_list(data, "sources")
    ingests = ensure_list(data, "ingests")

    upsert_by_key(
        sources,
        "source_id",
        "pg_9701_beowulf",
        {
            "source_id": "pg_9701_beowulf",
            "type": "http",
            "url": "https://www.gutenberg.org/ebooks/9701.txt.utf-8",
            "dest": "data_raw/pg_9701_beowulf/pg9701.txt",
            "ref": "",
            "license": "Project Gutenberg License",
            "distributable": True,
            "notes": "PG #9701: Beowulf + Finnsburh fragment (Plain Text UTF-8)",
        },
    )

    upsert_by_key(
        sources,
        "source_id",
        "pg_45861_aelfric_grammar",
        {
            "source_id": "pg_45861_aelfric_grammar",
            "type": "http",
            "url": "https://www.gutenberg.org/ebooks/45861.txt.utf-8",
            "dest": "data_raw/pg_45861_aelfric_grammar/pg45861.txt",
            "ref": "",
            "license": "Project Gutenberg License",
            "distributable": True,
            "notes": "PG #45861: Ælfrics Grammatik und Glossar (German + Old English; Plain Text UTF-8)",
        },
    )

    upsert_by_key(
        ingests,
        "corpus_id",
        "oe_beowulf_9701",
        {
            "corpus_id": "oe_beowulf_9701",
            "source_id": "pg_9701_beowulf",
            "enabled": True,
            "cmd": [
                "{python}",
                "src/ingest/ingest_gutenberg_txt.py",
                "--root",
                "{root}",
                "--in",
                "data_raw/pg_9701_beowulf/pg9701.txt",
                "--out",
                "data_proc/oe_beowulf_9701_prod.jsonl",
                "--corpus-id",
                "oe_beowulf_9701",
                "--lang",
                "ang",
                "--strip-gutenberg-header",
            ],
            "outputs": ["data_proc/oe_beowulf_9701_prod.jsonl"],
        },
    )

    upsert_by_key(
        ingests,
        "corpus_id",
        "oe_aelfric_45861",
        {
            "corpus_id": "oe_aelfric_45861",
            "source_id": "pg_45861_aelfric_grammar",
            "enabled": True,
            "cmd": [
                "{python}",
                "src/ingest/ingest_gutenberg_txt.py",
                "--root",
                "{root}",
                "--in",
                "data_raw/pg_45861_aelfric_grammar/pg45861.txt",
                "--out",
                "data_proc/oe_aelfric_45861_prod.jsonl",
                "--corpus-id",
                "oe_aelfric_45861",
                "--lang",
                "mul",
                "--strip-gutenberg-header",
            ],
            "outputs": ["data_proc/oe_aelfric_45861_prod.jsonl"],
        },
    )

    data["sources"] = sorted((canonicalize_source(s) for s in sources), key=lambda x: x.get("source_id", ""))
    data["ingests"] = sorted((canonicalize_ingest(i) for i in ingests), key=lambda x: x.get("corpus_id", ""))

    write_json_pretty(CATALOG_PATH, data)
    print(f"[OK] wrote {CATALOG_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
