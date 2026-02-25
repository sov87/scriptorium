from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8", newline="\n")


GOOD_TEI = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <text><body><p>abc</p></body></text>
</TEI>
"""

BAD_TEI_NO_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader/>
</TEI>
"""


def test_harvest_no_upsert_continue_on_error_and_skip_existing(tmp_path: Path) -> None:
    repo = tmp_path / "data_raw/_repos/testcap"
    _write(repo / "data/tlg0001/tlg0001.tlg001/test.xml", GOOD_TEI)
    _write(repo / "data/tlg0001/tlg0001.tlg002/bad.xml", BAD_TEI_NO_BODY)

    out_dir = tmp_path / "data_proc/_harvest_test"
    reg = tmp_path / "docs/corpora.json"

    cmd = [
        sys.executable,
        "src/ingest/harvest_capitains_repo.py",
        "--root",
        str(tmp_path),
        "--repo-root",
        str(repo),
        "--out-dir",
        str(out_dir),
        "--registry",
        str(reg),
        "--prefix",
        "grc",
        "--lang",
        "grc",
        "--license",
        "TEST",
        "--tier",
        "A_open_license",
        "--distributable",
        "1",
        "--limit",
        "0",
        "--no-upsert",
        "--continue-on-error",
    ]
    subprocess.check_call(cmd)

    assert not reg.exists(), "registry must not be created/modified when --no-upsert is set"

    reports = sorted((tmp_path / "runs/harvest").glob("harvest_*.json"))
    assert reports, "harvest report must be written"
    rep = json.loads(reports[-1].read_text(encoding="utf-8"))
    assert rep["count"] == 1
    assert len(rep.get("errors", [])) == 1
    assert len(rep.get("skipped", [])) == 0

    # Second run without --overwrite: should skip existing out_jsonl
    subprocess.check_call(cmd)
    reports2 = sorted((tmp_path / "runs/harvest").glob("harvest_*.json"))
    rep2 = json.loads(reports2[-1].read_text(encoding="utf-8"))
    assert rep2["count"] == 0
    assert len(rep2.get("skipped", [])) >= 1


def test_promote_filters_apply(tmp_path: Path) -> None:
    out = tmp_path / "data_proc"
    out.mkdir(parents=True, exist_ok=True)
    (out / "a.jsonl").write_text("{\"id\":\"x\"}\n", encoding="utf-8")
    (out / "b.jsonl").write_text("{\"id\":\"y\"}\n", encoding="utf-8")
    (out / "c.jsonl").write_text("{\"id\":\"z\"}\n", encoding="utf-8")

    rep = {
        "schema": "scriptorium.harvest_report.v1",
        "generated_utc": "2000-01-01T00:00:00Z",
        "repo_root": "X",
        "base": "X",
        "count": 3,
        "skipped": [],
        "errors": [],
        "items": [
            {"corpus_id": "grc_keep_one", "tei": "t1.xml", "out_jsonl": str(out / "a.jsonl"), "sha256": "0", "work_id": "W1"},
            {"corpus_id": "grc_drop_two", "tei": "t2.xml", "out_jsonl": str(out / "b.jsonl"), "sha256": "0", "work_id": "W2"},
            {"corpus_id": "grc_keep_three", "tei": "t3.xml", "out_jsonl": str(out / "c.jsonl"), "sha256": "0", "work_id": "W3"},
        ],
    }
    rep_path = tmp_path / "runs/harvest/harvest_20000101_000000.json"
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")

    reg = tmp_path / "docs/corpora.json"
    cmd = [
        sys.executable,
        "src/ingest/promote_harvest_report.py",
        "--root",
        str(tmp_path),
        "--report",
        str(rep_path),
        "--registry",
        str(reg),
        "--license",
        "TEST",
        "--tier",
        "A_open_license",
        "--distributable",
        "1",
        "--include-corpus-regex",
        "keep",
        "--limit",
        "10",
        "--offset",
        "0",
        "--tag",
        "test",
    ]
    subprocess.check_call(cmd)

    data = json.loads(reg.read_text(encoding="utf-8"))
    ids = [c["corpus_id"] for c in data["corpora"]]
    assert "grc_keep_one" in ids and "grc_keep_three" in ids
    assert "grc_drop_two" not in ids
