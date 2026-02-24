from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scriptorium.provenance import verify_canon_jsonl_sha256


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def test_verify_canon_jsonl_sha256_pass_and_fail_on_tamper(tmp_path: Path) -> None:
    project_root = tmp_path

    canon_rel = "data_proc/test_corpus.jsonl"
    canon_path = project_root / canon_rel
    canon_path.parent.mkdir(parents=True, exist_ok=True)

    canon_bytes_1 = b'{"id":"x","txt":"hello"}\n'
    canon_path.write_bytes(canon_bytes_1)
    exp = _sha256_hex(canon_bytes_1).upper()

    reg = {
        "generated_utc": "2000-01-01T00:00:00Z",
        "corpora": [
            {
                "corpus_id": "test_corpus",
                "canon_jsonl": {"path": canon_rel, "sha256": exp},
                "rights": {"tier": "A_demo", "license": "test", "distributable": True},
            }
        ],
    }
    reg_path = project_root / "docs" / "corpora.json"
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    reg_path.write_text(json.dumps(reg, ensure_ascii=False, indent=2), encoding="utf-8")

    out = verify_canon_jsonl_sha256(project_root, strict=True)
    assert out["ok"] is True
    assert out["checked"] == 1

    # Tamper -> must fail closed
    canon_path.write_bytes(b'{"id":"x","txt":"HELLO"}\n')
    with pytest.raises(SystemExit) as ei:
        verify_canon_jsonl_sha256(project_root, strict=True)
    assert "sha256 mismatch" in str(ei.value).lower()


def test_verify_canon_jsonl_sha256_strict_rejects_missing_hash(tmp_path: Path) -> None:
    project_root = tmp_path

    canon_rel = "data_proc/missing_hash.jsonl"
    canon_path = project_root / canon_rel
    canon_path.parent.mkdir(parents=True, exist_ok=True)
    canon_path.write_bytes(b'{"id":"x"}\n')

    reg = {
        "generated_utc": "2000-01-01T00:00:00Z",
        "corpora": [
            {
                "corpus_id": "x",
                "canon_jsonl": {"path": canon_rel},  # no sha256
                "rights": {"tier": "A_demo", "license": "test", "distributable": True},
            }
        ],
    }
    reg_path = project_root / "docs" / "corpora.json"
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    reg_path.write_text(json.dumps(reg, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(SystemExit) as ei:
        verify_canon_jsonl_sha256(project_root, strict=True)
    assert "missing sha256" in str(ei.value).lower()