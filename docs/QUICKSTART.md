# Quickstart (public sample)

This repository is designed to be runnable on a fresh clone using the included `sample_data/` corpus.

## Requirements

- Python 3.11+
- Git
- (Recommended) a virtual environment

## Install

```bash
git clone https://github.com/sov87/scriptorium.git
cd scriptorium

python -m venv .venv
# Windows PowerShell:
#   .\\.venv\\Scripts\\Activate.ps1
# macOS/Linux:
#   source .venv/bin/activate

pip install -r requirements.txt
pip install -e .
```

## Run the smoke test (cross-platform)

```bash
python tools/smoke_test.py --config configs/sample_demo.toml --compileall
```

This will:

- run `doctor` (non-strict) against the sample config
- build BM25 + FAISS indexes under `sample_data/indexes/` if missing
- run `scriptorium query` and confirm `candidates.jsonl` exists
- run `scriptorium answer --dry-run` (retrieval-only)

## Windows-only (PowerShell) smoke test

```powershell
.\src\smoke_test.ps1
```
