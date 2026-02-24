# SCRIPTORIUM — Project Context (Living Document)
Last updated: 2026-02-24
## Core goal
Local-first, reproducible pipeline that ingests historical text corpora into a structured SQLite database with:
- Canonical archival JSONL intermediate
- FTS + vector retrieval
- Optional LLM answer/gloss generation with audit-traceable artifacts
- Defensible provenance/rights discipline

Long-term scope (planned):
- Old English / Anglo-Saxon (current)
- Latin (Perseus / Open Greek & Latin where openly licensed)
- Ancient Greek (canonical-greekLit / Open Greek & Latin where openly licensed)
- Patristic / liturgical public-domain corpora (e.g., CCEL subsets and other openly available DH repos)

## Non-negotiables
- Local-first/offline-capable: core build/search runs without cloud dependencies (LLM calls optional).
- Canonical intermediate: archival JSONL is the “source of truth” for processed text (no silent mutation).
- Provenance and rights: every corpus must have explicit rights/provenance notes in the catalog/ledger.
- Audit chain for AI: answer-search produces cites; answer-show resolves cited passages from the DB.
- CI must not call an LLM (seeded artifacts only).

## Environment (known working)
- Windows 11
- Project root: F:\Books\as_project
- Primary venv: F:\Books\as_project\.venv_clean\
  Activate: .\.venv_clean\Scripts\Activate.ps1

## Key configs and DBs
- Local work config: configs\window_0597_0865.toml
- Local DB: db\scriptorium.sqlite
- CI config: configs\sample_demo_ci_strict.toml
- CI DB: db\scriptorium_ci.sqlite (via root.db_path)

## Current capabilities (milestone reached)
- DB build + FTS search + vector index build + hybrid retrieval.
- AI answer pipeline:
  - answer-db → answer-import-db → answer-search (FTS) with --corpus and --show-cites
  - answer-show: run_id → cites → resolved segments with excerpts
  - answer-import-db hardened to maintain answers_fts
  - check-ai-fts command verifies AI table/FTS consistency
- CI smoke:
  - builds CI DB, seeds an answer run without LLM, imports it, checks FTS, asserts the audit chain.

## Catalog
- Source catalog: docs/sources_catalog.json
- Rule: do not add new corpora without updating catalog + rights/provenance notes.

## Current status quick check
- python -m scriptorium check-ai-fts --config configs\window_0597_0865.toml --json

## Next work (priority order)
1) Commit + CI lock-in for provenance gate (`--strict-provenance`): add pytest coverage and document the invariant.
2) Scale corpora (repeat the disciplined loop): add one additional Latin TEI corpus using the TEI/CTS module (ingest → register → provenance/rights → strict db-build → FTS smoke).
3) Optional: add semantic hash (`canon_jsonl.semantic_hash_v1`) to reduce false alarms from harmless byte-level changes.
4) Optional: Qwen3.5 integration (disable thinking via `extra_body` passthrough) once you decide to switch.

## Session protocol (prevents token spiral)
- End of session: update this file’s “milestone reached” and “next work”.
- Commit this file alongside the code changes it reflects.
- New chat start: “Read docs/PROJECT_CONTEXT.md and docs/DECISIONS.md and continue from Next work.”
