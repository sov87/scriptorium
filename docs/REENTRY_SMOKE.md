# Re-entry smoke path (clean checkout)

This document is the canonical clean-checkout smoke path for Scriptorium.

## PowerShell demo commands

```powershell
Copy-Item docs\corpora.public.json docs\corpora.json -Force
python -m scriptorium doctor --config configs\sample_demo_ci.toml --json
python -m scriptorium db-build --config configs\sample_demo_ci.toml --overwrite
python -m scriptorium vec-build --config configs\sample_demo_ci.toml --out-dir indexes\vec_faiss_global --batch 16
python -m scriptorium db-search --config configs\sample_demo_ci.toml --q "lareow" --k 3 --corpus oe_bede_sample
python -m scriptorium retrieve --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample
python -m scriptorium answer-db --config configs\sample_demo_ci.toml --q "What is said about the lareow?" --k 5 --corpus oe_bede_sample --dry-run
git checkout -- docs\corpora.json
```

## Important warnings

- This smoke path generates derived artifacts (for example under `db/`, `indexes/`, and `runs/`) that must **not** be committed.
- The strict local-first embedding path requires `models/all-MiniLM-L6-v2/` to be provisioned first.
- Real `answer-db` generation (without `--dry-run`) requires a reachable OpenAI-compatible local LLM server.
- CI must stay LLM-free.
