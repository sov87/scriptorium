# SCRIPTORIUM (Portfolio)

## What this is

Scriptorium is a local-first pipeline that builds an audit-friendly structured database for Old English / Anglo-Saxon texts.

Core principles:

- Canon JSONL is archival (`data_proc/`): stable IDs, stable schema, never silently mutated.
- Derived artifacts are rebuildable: SQLite DB (with FTS5), global FAISS vectors, and machine outputs.
- Rights-aware releases: snapshot bundles include canon only when a corpus is marked `rights.distributable=true`.

## Current capabilities

- Multi-corpus registry (`docs/corpora.json`)
- Derived SQLite DB with FTS5 (`db-build`, `db-search`)
- Global vector index + hybrid retrieval (`vec-build`, `retrieve`)
- Answer pipeline grounded in retrieved segments with strict JSON and segment-ID citations (`answer-db`, `answer-batch-db`)
- Snapshot bundling with rights gating (`release --snapshot`)

## Reproducible demo (clean checkout)

The default registry (`docs/corpora.json`) often points at local-only corpora under `data_proc/`, which are not shipped.
For CI and a public demo, swap in the public registry pointing at committed demo data:

```powershell
Copy-Item docs\corpora.public.json docs\corpora.json -Force
```

### Demo run (downloads embedding model during run)

```powershell
python -m scriptorium doctor    --config configs\sample_demo_ci.toml --json
python -m scriptorium db-build  --config configs\sample_demo_ci.toml --overwrite
python -m scriptorium vec-build --config configs\sample_demo_ci.toml --out-dir indexes\vec_faiss_global --batch 16
python -m scriptorium retrieve  --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample
python -m scriptorium answer-db --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample --dry-run
```

### Strict local-first demo (matches CI posture)

Provision the model once:

```powershell
@'
from sentence_transformers import SentenceTransformer
m = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
m.save("models/all-MiniLM-L6-v2")
print("saved model -> models/all-MiniLM-L6-v2")
'@ | python
```

Run strict:

```powershell
python -m scriptorium doctor    --config configs\sample_demo_ci_strict.toml --strict --json
python -m scriptorium db-build  --config configs\sample_demo_ci_strict.toml --overwrite
python -m scriptorium vec-build --config configs\sample_demo_ci_strict.toml --out-dir indexes\vec_faiss_global --batch 16
python -m scriptorium retrieve  --config configs\sample_demo_ci_strict.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample
python -m scriptorium answer-db --config configs\sample_demo_ci_strict.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample --dry-run
```

Restore tracked registry afterwards:

```powershell
git checkout -- docs\corpora.json
```

## Data layout

- `data_raw/` : local-only raw sources (never shipped)
- `data_proc/` : local-only canon JSONL corpora (archival)
- `sample_data/` : committed demo canon + demo indexes
- `db/scriptorium.sqlite` : derived SQLite DB (FTS included)
- `indexes/vec_faiss_global/` : derived global FAISS vector index
- `runs/` : machine outputs (query + answer traces)
- `releases/` : snapshot zip outputs

## Rights model

Each corpus entry in the registry has `rights.distributable`.
Snapshots include canon JSONL only when `distributable=true`.
