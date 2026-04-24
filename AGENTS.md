# AGENTS.md

## Scriptorium agent guidance

- Scriptorium is a local-first, reproducible Digital Humanities pipeline.
- Canonical JSONL is the authoritative archival intermediate.
- SQLite DBs, vector indexes, LLM runs, downloaded corpora, and generated data are derived artifacts.

## Do not commit generated artifacts

- `.venv*`
- `data_raw/`
- `data_proc/`
- `db/`
- `indexes/`
- `runs/`
- `data_gen/`

## Registry and provenance discipline

- Do not mutate `docs/corpora.json` for local/subset builds. Use `--registry-override` instead.
- CI must never call an LLM.
- Do not guess provenance, rights, licenses, source URLs, corpus paths, or corpus metadata.

## Citation integrity

- Preserve citation validation: `citation.quote` must be a literal substring of the cited source passage after normalization.

## Workflow preferences

- Prefer Windows/PowerShell examples.
- Keep PRs small and focused.
- Before editing, read:
  - `README.md`
  - `docs/PROJECT_CONTEXT.md`
  - `docs/DECISIONS.md`
  - `docs/HANDOFF.md`
- For changes affecting CLI, config, DB build, retrieval, vector indexing, or LLM integration, run the relevant smoke commands from `README.md` if possible.
