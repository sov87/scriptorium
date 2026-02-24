# SCRIPTORIUM — Design Decisions (Do Not Drift)

This file is the non‑negotiable constraint set for future work. If a future change contradicts an item below, it must be treated as a **breaking change** and recorded explicitly (including migration steps and rationale).

Definitions used throughout:
- **Canonical JSONL**: the authoritative archival intermediate (one record per segment) that feeds database builds and downstream artifacts.
- **Segment**: the smallest searchable/citable unit stored in SQLite `segments` (+ `segments_fts`) and referenced by answer citations.
- **Catalog/registry**: the curated list of corpora, with verified provenance/rights metadata and deterministic ingest configuration.

---

## D-001 Canonical archival JSONL intermediate (authoritative; no silent mutation)
Canonical JSONL is the authoritative intermediate for all corpora.

Requirements:
- Output must be deterministic: same input + same code/config → identical JSONL bytes (modulo explicitly allowed fields).
- Canonical JSONL records must preserve textual uncertainty/damage using archival conventions (e.g., brackets) and/or explicit metadata flags; **never drop information silently**.
- Any normalization that changes surface text must be:
  1) deterministic,
  2) documented (see TEI policy below),
  3) reversible in the sense that alternates/uncertainty are preserved in metadata when applicable.

---

## D-002 Local-first and offline-capable
All core pipeline steps must run locally and remain offline-capable:
- ingest → canonical JSONL
- DB build (SQLite + FTS5)
- lexical retrieval (FTS5)
- vector indexing (when enabled)
- audit/inspection tooling

Cloud or remote services (including LLMs) are optional and must never be required for CI or for core indexing/search.

---

## D-003 CI must not call an LLM
CI must never invoke an LLM (local or remote).

Requirements:
- CI uses seeded artifacts / fixtures to test AI pipeline components.
- CI must remain deterministic and fast; no network dependency for correctness.
- Any tests covering the answer layer must validate **structure and integrity** using seeded examples.

---

## D-004 Audit chain requirement for AI answers
AI answers must be audit-traceable end-to-end.

Requirements:
- Every answer must be traceable to retrieval artifacts (run_dir / run_id) that record:
  - query
  - retrieval results
  - model/config metadata (when used)
  - produced answer JSON
- `answer-show` must be able to resolve each cited `segments.id` back to `segments` and print an excerpt for verification.
- The audit chain must survive DB rebuilds as long as segment IDs and corpora are stable.

---

## D-005 Separate CI SQLite DB (path separation)
CI must use a separate SQLite DB file via `root.db_path`.

Requirements:
- CI must never overwrite the working/local DB.
- CI DB path must be explicitly configured (e.g., `db/scriptorium_ci.sqlite`).
- Local default DB remains distinct (e.g., `db/scriptorium.sqlite`).

---

## D-006 Corpus ingest discipline (provenance/rights + catalog entry required)
Every corpus added must have:
- a catalog/registry entry (corpus_id, title, language, work_id policy where applicable),
- provenance and rights notes adequate for defensibility (who/what edition, license status, jurisdiction if relevant),
- a deterministic ingest path that produces canonical JSONL.

If rights/provenance are not yet verified, the corpus may be added only as a **TODO** (non-executable) entry, without implied permission.

---

## D-007 No “unknown URL guesses”
Do not add sources to the catalog with guessed URLs or uncertain license claims.

Rule:
- Either (a) verify and record the source URL/license explicitly, or (b) mark as TODO **without a URL** and keep it non-executable until verified.

---

## D-008 Canonical segment IDs (hard invariant)
Segment IDs are canonical and must be enforced at DB-build time.

Requirements:
- `segments.id` must be exactly `<corpus_id>:<local_id>`.
- `local_id` must be deterministic and must **not contain a colon**.
- Where possible, derive `local_id` deterministically from `loc` (see CTS/URN policy), otherwise use a stable sequence.
- Any corpus ingest that cannot produce stable IDs is blocked until fixed.

---

## D-009 Quote integrity for AI citations (anti-hallucination)
Citations must be defensible.

Requirements:
- `citation.quote` MUST be a literal substring of the cited passage text.
- Normalize text for comparison using Unicode NFC (critical for Greek and other composed/diacritic scripts).
- Any fuzzy matching is strictly controlled, minimal, and must **fail closed** (i.e., never “accept” a fabricated quote).
- If validation fails, the answer pipeline must retry/repair (bounded attempts) using explicit validation error feedback; do not loosen the rule.

---

## D-010 Multilingual FTS tokenization stability (DB contract)
FTS tokenization settings are part of the DB contract.

Requirements:
- Use `unicode61` tokenizer with `remove_diacritics=2` for `segments_fts` to support diacritic-insensitive search (Greek pilot requirement).
- Any change to tokenizer settings requires:
  - explicit documentation in PROJECT_CONTEXT,
  - a deliberate rebuild/migration plan,
  - and verification on at least one non-English corpus.

---

## D-011 TEI normalization policy (deterministic; no silent drops)
Applies to all TEI ingests and must be deterministic.

Normalization rules:
- `<choice>`: prefer `<reg>` over `<orig>` for searchable text. Preserve the alternate reading in metadata if present.
- `<unclear>`: keep the text; mark uncertainty consistently (archival brackets and/or meta flag). Never drop silently.
- `<gap>` / `<supplied>`: represent damaged/supplied text consistently (archival brackets and/or metadata). Never drop silently.
- Output must be stable under repeated runs (same input TEI → identical canonical JSONL).

---

## D-012 CTS/URN policy (work_id, loc, deterministic IDs)
When CTS data is available, represent it consistently.

Requirements:
- `work_id` is the CTS URN when available (derived from TEI header / CTS declarations).
- `loc` uses CTS-style references (passage component / `@n` values / milestones as defined by the TEI/CTS scheme).
- `segments.id` stays `<corpus_id>:<local_id>`.
- `local_id` is derived deterministically from `loc` when possible (and must never contain a colon).

---

## D-013 Repository discipline (no committing generated artifacts)
Do not commit environment or generated data artifacts.

Must NOT be committed:
- `.venv*`
- `data_raw/`
- `data_proc/`
- `db/`
- `indexes/`
- `runs/`
- `data_gen/`

Exceptions:
- small, curated fixtures under `tests/fixtures/` (or similar) explicitly intended for CI.
- docs and config files required to reproduce builds.

---

## D-014 Determinism and reproducibility gates
Reproducibility is a core deliverable.

Requirements:
- Deterministic ordering: stable sorting for emitted records and DB build steps.
- Seeded randomness where randomness exists (tests, sampling).
- Explicit versioning of schema expectations; DB build steps must be repeatable.
- Validation tooling must fail fast on invariant violations (IDs, missing text, empty retrieval candidates, malformed citations, etc.).

---

## D-015 Scholarly sourcing discipline (no crowd-edited encyclopedias)
For project documentation and scholarly claims:
- Do not cite or rely on crowd-edited encyclopedias (e.g., Wikipedia).
- Prefer primary sources and vetted secondary scholarship (critical editions, reputable academic references).

## D-016 Provenance gate for canonical JSONL at db-build
DB builds must enforce catalog provenance for canonical JSONL inputs.

Requirements:
- `db-build` verifies `canon_jsonl.sha256` recorded in `docs/corpora.json` against the on-disk JSONL before ingest.
- Any mismatch or missing file is a hard failure.
- `db-build --strict-provenance` additionally requires that every corpus entry with `canon_jsonl.path` has `canon_jsonl.sha256` populated.
- Release-quality builds should use `--strict-provenance`.
