# SCRIPTORIUM

Local-first, audit-friendly pipeline for building a structured database of Old English / Anglo-Saxon texts.

## Quick demo (Windows PowerShell)
```powershell
python -m scriptorium doctor --config configs\window_0597_0865.toml --strict --json
python -m scriptorium catalog-fetch  --config configs\window_0597_0865.toml
python -m scriptorium catalog-ingest --config configs\window_0597_0865.toml
python -m scriptorium db-build       --config configs\window_0597_0865.toml --overwrite
python -m scriptorium vec-build      --config configs\window_0597_0865.toml
python -m scriptorium db-search      --config configs\window_0597_0865.toml --q "we" --k 3 --corpus oe_beowulf_9700
python -m scriptorium retrieve       --config configs\window_0597_0865.toml --q "What is being claimed about Scyld?" --k 5 --corpus oe_beowulf_9700
```

LLM answering (LM Studio):
```powershell
$env:SCRIPTORIUM_LLM_BASE_URL = "http://localhost:1234/v1"
$env:SCRIPTORIUM_LLM_API_KEY  = "lm-studio"
$env:SCRIPTORIUM_LLM_MODEL    = ""   # optional; auto-pick from /v1/models

python -m scriptorium answer-db --config configs\window_0597_0865.toml --q "What is being claimed about Scyld?" --k 8 --corpus oe_beowulf_9700
```

## More detail
See docs/README_PORTFOLIO.md.
