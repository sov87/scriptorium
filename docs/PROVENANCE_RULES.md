
---

## `docs/PROVENANCE_RULES.md`

```markdown
# Provenance Rules (PROVENANCE_RULES)

Every corpus ingested into `data_proc/` must have a provenance record so the project is interview-defensible.

This project treats provenance as first-class: if provenance is missing, the corpus is not “production.”

---

## Required artifact: `data_raw/<corpus_id>/PROVENANCE.json`

Directory rule:
- Raw inputs live under `data_raw/<corpus_id>/`
- Canonical outputs live under `data_proc/` as `<corpus_id>_prod.jsonl` (or a documented naming variant)

`PROVENANCE.json` must be committed for public corpora (or redacted for private ones).

---

## Required fields (PROVENANCE.json)

### `corpus_id` (string)
Matches the corpus key used in `src` fields (e.g., `oe_bede_prod`).

### `title` (string)
Human-readable title.

### `scope` (string)
What this corpus contains and what it excludes (edition/manuscript scope, language, date range).

### `rights` (object)
Must include:
- `status` (string): `public_domain` | `licensed` | `restricted` | `unknown`
- `basis` (string): short justification (e.g., “manuscript facsimile; public domain”)
- `notes` (string, optional)

If `unknown`, ingestion stops until clarified.

### `sources` (array of objects)
Each source object must include:
- `type`: `manuscript` | `edition` | `scan` | `transcription` | `dataset` | `website` | `other`
- `description`: what it is
- `local_path`: where it lives under `data_raw/<corpus_id>/`
- `sha256`: hash of the raw file (or of the downloaded artifact)
- `retrieved_utc`: ISO timestamp
- `provider`: who/where it came from (institution/publisher/site name; no need to include a URL in public docs if you prefer)

### `processing` (array of objects)
Each processing step must include:
- `step`: short name (e.g., `ocr`, `tei_parse`, `anchor_parse`, `jsonl_build`)
- `script`: path under repo (e.g., `src/convert_bede_anchors_to_jsonl.py`)
- `inputs`: array of local paths
- `outputs`: array of local paths
- `params`: object (only the parameters that affect output)
- `ran_utc`: ISO timestamp
- `tool_versions`: object (python version + key libs if relevant)

### `canon_output` (object)
Must include:
- `path`: e.g., `data_proc/oe_bede_prod_utf8.jsonl`
- `record_count`: integer
- `sha256`: hash of the full JSONL file

### `notes` (string, optional)
Any special handling: damaged text conventions, normalization policy, known gaps.

---

## Operational rules

1) **No provenance, no canon.**
   - Do not treat any JSONL as production unless `PROVENANCE.json` exists and `canon_output` is populated.

2) **Hashes are mandatory.**
   - Hash raw inputs and canonical outputs. Store hashes in provenance.

3) **Scripts must be in-repo.**
   - Processing steps must reference scripts under `src/` or `tools/`.

4) **Reproducibility beats convenience.**
   - If you do a manual step, document it explicitly in `processing` with enough detail to repeat it.

5) **Never overwrite raw inputs.**
   - Raw files under `data_raw/` are immutable. If you obtain a better scan/transcription, add it as a new file with a new hash.

6) **Canonical JSONL is archival.**
   - If you fix errors, produce a new canonical release and update provenance with the new `canon_output` hash and record_count.

---

## Minimal PROVENANCE.json template

```json
{
  "corpus_id": "oe_bede_prod",
  "title": "Bede (Old English) — production corpus",
  "scope": "Old English Bede text, segmented by anchors; excludes commentary and modern translations.",
  "rights": {
    "status": "public_domain",
    "basis": "Public-domain manuscript/transcription sources; no copyrighted modern edition text included."
  },
  "sources": [
    {
      "type": "transcription",
      "description": "Anchor-based transcription export",
      "local_path": "data_raw/oe_bede_prod/anchors_export.json",
      "sha256": "…",
      "retrieved_utc": "2026-02-20T00:00:00Z",
      "provider": "Local archive"
    }
  ],
  "processing": [
    {
      "step": "jsonl_build",
      "script": "src/convert_bede_anchors_to_jsonl.py",
      "inputs": ["data_raw/oe_bede_prod/anchors_export.json"],
      "outputs": ["data_proc/oe_bede_prod_utf8.jsonl"],
      "params": {"utf8": true},
      "ran_utc": "2026-02-20T00:00:00Z",
      "tool_versions": {"python": "3.12.10"}
    }
  ],
  "canon_output": {
    "path": "data_proc/oe_bede_prod_utf8.jsonl",
    "record_count": 235,
    "sha256": "…"
  },
  "notes": "Damaged/uncertain spans are bracketed with [ ]."
}