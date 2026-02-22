# SCRIPTORIUM (Portfolio)

## What this is
Scriptorium is a local-first pipeline that builds an audit-friendly structured database for Old English / Anglo-Saxon texts.

Core principles:
- Canon JSONL is archival (data_proc/): stable IDs, stable schema, never silently mutated.
- Derived artifacts are rebuildable: SQLite DB, FTS index, FAISS vectors, machine answers.
- Rights-aware releases: snapshot bundles include canon only when a corpus is marked redistributable.

## Current capabilities
- Multi-corpus registry (docs/corpora.json)
- Source catalog runner (docs/sources_catalog.json + catalog-* CLI)
- Derived SQLite DB with FTS5 full-text search (db-build, db-search)
- Global vector index + hybrid retrieval (vec-build, retrieve)
- LLM answering grounded in retrieved segments with strict JSON + segment-ID citations (answer-db, answer-batch-db)
- Snapshot bundling with rights gating (release --snapshot)

## Quick demo (Windows PowerShell)
From project root:

```powershell
python -m scriptorium doctor --config configs\window_0597_0865.toml --strict --json
.\src\demo_full_pipeline.ps1
```

## Data layout
- data_raw/ : local-only raw sources (never shipped)
- data_proc/ : canon JSONL corpora (archival)
- db/scriptorium.sqlite : derived SQLite DB (FTS included)
- indexes/vec_faiss_global/ : global FAISS vector index
- runs/answer_db/ and runs/answer_batch_db/ : machine outputs
- releases/ : snapshot zip outputs

## Rights model
Each corpus in docs/corpora.json has rights.distributable. Snapshots include canon JSONL only when distributable=true.
