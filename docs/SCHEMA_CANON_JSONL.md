# Canon JSONL Schema (SCHEMA_CANON_JSONL)

This project uses **canonical JSONL** as the archival source of truth.

**Rules**
- UTF-8 text, one JSON object per line (JSONL).
- **No silent mutation** of canonical records. Any correction is a new release of the canon with an auditable diff.
- If the source text is damaged/uncertain, use archival brackets: `[ ]`.
- Fields listed as **required** must exist on every record, even if empty strings are disallowed (see constraints below).

---

## Required fields

### `id` (string)
Stable unique identifier for the record.

Constraints:
- Unique within the corpus.
- Stable across releases unless the underlying segmentation/identity truly changes.
- Recommended pattern: `<corpus_id>.<work_id>.<loc_id>` (no spaces).

Examples:
- `bede.oe.book1.ch03.s01`
- `asc.msA.0755.entry01`

### `src` (string)
Short corpus/source key (machine-friendly).

Examples:
- `oe_bede_prod`
- `asc_A_prod`

### `work` (string)
Human-readable work label.

Examples:
- `Bede (Old English)`
- `Anglo-Saxon Chronicle, MS A`

### `loc` (string)
Human-facing location string (edition/manuscript locator).

Examples:
- `Book 1, Ch 3`
- `a.755`

### `srcp` (string)
Source-pointer string used for traceability (page/folio/line/anchor ID).

Notes:
- Must be stable and preferably resolvable back to the underlying transcription/anchor system.
- If you have no better pointer, set it equal to `loc`.

Examples:
- `fol. 12r, l. 3–9`
- `anchor:bed.oe.00123`

### `lang` (string)
BCP-47-ish language tag (project uses a small controlled set).

Recommended values:
- `ang` (Old English)
- `la` (Latin)
- `en` (Modern English notes only; avoid for primary text)

### `txt` (string)
The canonical text content.

Constraints:
- Must be the exact transcription content for this record.
- Use `[ ]` for damaged/uncertain spans.
- Do not normalize spelling silently. If normalization is provided, store it in an optional field.

### `sha256` (string)
SHA-256 hex digest of `txt` encoded as UTF-8.

Constraints:
- Lowercase hex, length 64.
- Computed strictly from `txt` (not from the full JSON record).

---

## Optional fields (allowed)

Optional fields must not change the meaning of the canonical text; they are metadata or parallel representations.

### `norm` (string)
A normalized spelling version of `txt` (if you maintain one).

### `tokens` (array of strings)
Tokenization output (deterministic if present).

### `meta` (object)
Freeform structured metadata. Keep keys stable and documented.

Common `meta` keys:
- `date_start`, `date_end` (ints)
- `ms`, `folio`, `line_start`, `line_end`
- `editor`, `edition`

### `flags` (array of strings)
Controlled markers like:
- `damaged`
- `uncertain`
- `supplied`
- `lacuna`

### `notes` (string)
Short editorial note about the record’s state. Not interpretive commentary.

---

## Forbidden patterns

- Extra top-level keys not documented here (unless the schema doc is updated first).
- Non-UTF8 encodings.
- Multi-line JSON objects (must be one line per record).
- Missing `sha256` or `sha256` not matching `txt`.

---

## Minimal example record

```json
{
  "id": "bede.oe.sample.0001",
  "src": "oe_bede_sample",
  "work": "Bede (Old English) — SAMPLE",
  "loc": "sample:1",
  "srcp": "sample:1",
  "lang": "ang",
  "txt": "Ða was micel storm on sæ, and þa scipu hæfdon earfoðnysse; ...",
  "sha256": "629fff52042d56b774bd3978e6c3af0f47b6e8ef0c4e88560604be9bcd235668"
}