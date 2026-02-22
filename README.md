# SCRIPTORIUM

Local-first, audit-friendly pipeline for building a structured database of Old English / Anglo-Saxon texts.

## Reproducible demo (works from a clean checkout)

This repo ships:

- Tiny committed demo canon: `sample_data/data_proc/oe_bede_sample_utf8.jsonl`
- Public registry pointing only at committed demo data: `docs/corpora.public.json`

Because the default registry (`docs/corpora.json`) often points at local-only corpora under `data_proc/`, the demo and CI run by temporarily swapping in the public registry.

### Step 0 — Use the public registry (temporary swap)

```powershell
Copy-Item docs\corpora.public.json docs\corpora.json -Force
```

### Option A — Demo config (downloads embedding model during run)

```powershell
python -m scriptorium doctor    --config configs\sample_demo_ci.toml --json
python -m scriptorium db-build  --config configs\sample_demo_ci.toml --overwrite
python -m scriptorium vec-build --config configs\sample_demo_ci.toml --out-dir indexes\vec_faiss_global --batch 16
python -m scriptorium db-search --config configs\sample_demo_ci.toml --q "lareow" --k 3 --corpus oe_bede_sample
python -m scriptorium retrieve  --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample
python -m scriptorium answer-db --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample --dry-run
```

### Option B — Strict local-first config (embedding model must already be on disk)

Provision the model once (creates `models/all-MiniLM-L6-v2/`):

```powershell
@'
from sentence_transformers import SentenceTransformer
m = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
m.save("models/all-MiniLM-L6-v2")
print("saved model -> models/all-MiniLM-L6-v2")
'@ | python
```

Run the same pipeline with strict doctor:

```powershell
python -m scriptorium doctor    --config configs\sample_demo_ci_strict.toml --strict --json
python -m scriptorium db-build  --config configs\sample_demo_ci_strict.toml --overwrite
python -m scriptorium vec-build --config configs\sample_demo_ci_strict.toml --out-dir indexes\vec_faiss_global --batch 16
python -m scriptorium db-search --config configs\sample_demo_ci_strict.toml --q "lareow" --k 3 --corpus oe_bede_sample
python -m scriptorium retrieve  --config configs\sample_demo_ci_strict.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample
python -m scriptorium answer-db --config configs\sample_demo_ci_strict.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample --dry-run
```

### Step final — Restore the tracked registry file

```powershell
git checkout -- docs\corpora.json
```

## CI

GitHub Actions uses the strict path:

- provisions `models/all-MiniLM-L6-v2/`
- swaps `docs/corpora.public.json` into place as `docs/corpora.json`
- runs strict doctor + build + smoke

See `.github/workflows/ci.yml` for the exact sequence.
