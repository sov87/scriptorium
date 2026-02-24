from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from scriptorium.provenance import verify_canon_jsonl_sha256


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest().upper()


def test_provenance_gate_pass_and_fail(tmp_path: Path):
    # Create a fake project root with docs/corpora.json and a canonical JSONL file.
    project_root = tmp_path
    docs = project_root / "docs"
    docs.mkdir(parents=True, exist_ok=True)

    canon_dir = project_root / "data_proc"
    canon_dir.mkdir(parents=True, exist_ok=True)

    canon_path = canon_dir / "demo_prod.jsonl"
    canon_bytes = b'{"id":"demo:000001","text":"hello"}\n'
    canon_path.write_bytes(canon_bytes)

    corpora = {
        "generated_utc": "2000-01-01T00:00:00Z",
        "corpora": [
            {
                "corpus_id": "demo",
                "canon_jsonl": {
                    "path": "data_proc/demo_prod.jsonl",
                    "sha256": _sha256_bytes(canon_bytes),
                },
            }
        ],
    }
    (docs / "corpora.json").write_text(json.dumps(corpora, indent=2) + "\n", encoding="utf-8")

    # Should pass (strict)
    res = verify_canon_jsonl_sha256(project_root, strict=True)
    assert res["ok"] is True
    assert res["checked"] == 1

    # Mutate canonical JSONL; should fail
    canon_path.write_bytes(b'{"id":"demo:000001","text":"tampered"}\n')
    with pytest.raises(SystemExit):
        verify_canon_jsonl_sha256(project_root, strict=True)