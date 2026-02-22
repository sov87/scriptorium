# Rights Ledger (SCRIPTORIUM)

Purpose: defend what is distributable, why, and under what license/terms. This file is the human-auditable companion to `docs/corpora.json`.

## Policy
- `rights.distributable=true` only when the corpus has explicit redistributable terms (license or clearly stated public-domain status).
- Any uncertainty -> `distributable=false` and `license=UNVERIFIED` until resolved.
- Snapshots may include canon JSONL only when `distributable=true`.

## Corpus Decisions

| corpus_id | title | source | license / terms | distributable | decision basis (1–3 bullets) | verification notes |
|---|---|---|---|---:|---|---|
| echoe | ... | ... | GPL-3.0 | true | ... | ... |
| echoe_tei | ... | ... | GPL-3.0 | true | ... | ... |
| oe_beowulf_9700 | ... | Project Gutenberg | PG License | true | ... | ... |
| asc_A | ... | ... | UNVERIFIED | false | “local-only until verified” | ... |
| oe_bede | ... | ... | UNVERIFIED | false | “local-only until verified” | ... |