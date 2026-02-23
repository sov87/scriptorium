# SCRIPTORIUM

Local-first, audit-friendly pipeline for building a structured database of Old English / Anglo-Saxon texts.

## LLM use disclosure

This project was developed with assistance from large language models (LLMs) used as a coding copilot and for drafting certain documentation and utilities. The repository also includes optional LLM-powered features (e.g., `answer-db` / AI answer generation) that are designed to be audit-traceable via saved prompts, raw model responses, and retrieval/citation JSON. Any LLM-generated outputs should be treated as machine-generated and verified as appropriate for your use case.

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

### Optional — Seed an answer run and test answer-search (no LLM required)

`answer-search` only searches **imported answers** (`answers_fts`). For a reproducible smoke test (local or CI) without calling an LLM, you can seed a minimal `answer-db` run directory and import it:

```powershell
@'
from pathlib import Path
import json

run = Path("runs/answer_db/20000101_000000_seed_king")
run.mkdir(parents=True, exist_ok=True)

(run / "meta.json").write_text(
    json.dumps({"run_id": run.name, "corpus_filter": "oe_bede_sample"}, ensure_ascii=False, separators=(",", ":")),
    encoding="utf-8",
)
(run / "retrieval.json").write_text(
    json.dumps({"query": "king", "corpus": "oe_bede_sample"}, ensure_ascii=False, separators=(",", ":")),
    encoding="utf-8",
)
(run / "answer.json").write_text(
    json.dumps({"answer": "Seeded answer containing king (smoke test).", "citations": [], "notes": []}, ensure_ascii=False, separators=(",", ":")),
    encoding="utf-8",
)
(run / "validation.json").write_text("{}", encoding="utf-8")

print(str(run))
'@ | python
```

Import, check FTS, then search (use prefix queries):

```powershell
python -m scriptorium answer-import-db --config configs\sample_demo_ci_strict.toml --run-dir runs\answer_db\20000101_000000_seed_king
python -m scriptorium check-ai-fts      --config configs\sample_demo_ci_strict.toml --json
python -m scriptorium answer-search     --config configs\sample_demo_ci_strict.toml --q "king*" --k 10 --corpus oe_bede_sample
```

For real answer generation (non-dry-run), configure an LLM endpoint and run `answer-db` without `--dry-run`, then import the printed run directory.


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
- runs strict doctor + build + smoke (including a seeded answer import/search to exercise `answer-import-db`, `answers_fts`, and `answer-search`)

See `.github/workflows/ci.yml` for the exact sequence.
