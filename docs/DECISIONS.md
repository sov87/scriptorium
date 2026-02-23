# SCRIPTORIUM — Design Decisions (Do Not Drift)

Each entry is a constraint for future sessions.

## D-001 Canonical archival JSONL intermediate
Canonical JSONL is the authoritative intermediate; no silent mutation.

## D-002 Local-first and offline-capable
All core indexing/search runs locally; cloud is optional.

## D-003 CI must not call an LLM
CI uses seeded artifacts to test AI pipeline pieces.

## D-004 Audit chain requirement for AI answers
Every answer must be traceable to retrieval artifacts; answer-show must resolve cited passages.

## D-005 Separate CI SQLite DB
CI builds a separate db file via root.db_path and must never overwrite the local work DB.

## D-006 Corpus ingest discipline
Every corpus added requires provenance/rights notes and a catalog entry.

## D-007 No “unknown URL guesses”
Do not add sources to the catalog with guessed URLs. Either verify and cite the source URL/license, or mark it as TODO without a URL and keep it non-executable.
