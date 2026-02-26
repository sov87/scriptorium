import json
import subprocess
import sys
import hashlib
from pathlib import Path


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _wjson(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_dummy_jsonl(root: Path, relpath: str, lines: int = 1) -> str:
    # Create a tiny JSONL file and return its sha256.
    # Some validators check that registry sha256 matches the file.
    out = root / relpath
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = [{"id": i, "text": f"dummy {i}"} for i in range(lines)]
    out.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in payload) + "\n", encoding="utf-8")
    return _sha256_file(out)


def _run_generator(repo_root: Path, report_path: Path) -> None:
    gen = Path("src/ingest/gen_provenance_from_harvest.py")
    assert gen.exists(), "generator script missing from repo: src/ingest/gen_provenance_from_harvest.py"

    subprocess.run(
        [
            sys.executable,
            str(gen),
            "--root",
            str(repo_root),
            "--report",
            str(report_path),
            "--registry",
            "docs/corpora.json",
        ],
        check=True,
    )


def _run_validator(repo_root: Path) -> None:
    # Real signature (per your traceback):
    # validate_all_corpora(root: Path, *, registry_path: Optional[Path]=None, provenance_dir: Optional[Path]=None)
    import importlib

    vp = importlib.import_module("scriptorium.validate_provenance")
    fn = getattr(vp, "validate_all_corpora", None)
    assert callable(fn), "validate_all_corpora not found in scriptorium.validate_provenance"

    fn(
        repo_root,
        registry_path=repo_root / "docs" / "corpora.json",
        provenance_dir=repo_root / "docs" / "provenance",
    )


def test_autogen_provenance_distributable_passes_strict_rights_validator(tmp_path: Path):
    repo_root = tmp_path
    (repo_root / "docs" / "provenance").mkdir(parents=True, exist_ok=True)

    cid = "tst_corpus_1"
    canon_rel = "data_proc/test.jsonl"
    canon_sha = _write_dummy_jsonl(repo_root, canon_rel, lines=2)

    registry = {
        "corpora": [
            {
                "corpus_id": cid,
                "title": "Test Corpus One",
                "canon_jsonl": {"path": canon_rel, "sha256": canon_sha},
                "rights": {
                    "tier": "A_open_license",
                    "license": "CC-BY-SA-4.0",
                    "distributable": True,
                    "notes": "",
                },
            }
        ]
    }
    _wjson(repo_root / "docs" / "corpora.json", registry)

    report = {
        "repo_root": "https://example.invalid/repo",
        "generated_utc": "2026-02-25T12:00:00Z",
        "items": [
            {
                "corpus_id": cid,
                "work_id": "urn:cts:test:work.1",
                "tei": "data_raw/repo/work1.xml",
                "out_jsonl": "data_proc/_harvest_grc/work1.jsonl",
            }
        ],
    }
    report_path = repo_root / "runs" / "harvest" / "harvest_20260225_120000.json"
    _wjson(report_path, report)

    # Incomplete existing provenance (patch-in-place must fill required fields without deleting notes)
    _wjson(repo_root / "docs" / "provenance" / f"{cid}.json", {"corpus_id": cid, "rights": {"distributable": True}})

    _run_generator(repo_root, report_path)

    prov = json.loads((repo_root / "docs" / "provenance" / f"{cid}.json").read_text(encoding="utf-8"))
    assert prov["corpus_id"] == cid
    assert prov["title"] == "Test Corpus One"
    assert prov["rights"]["distributable"] is True
    assert prov["rights"]["tier"] == "A_open_license"
    assert prov["rights"]["license"] == "CC-BY-SA-4.0"

    assert isinstance(prov.get("sources"), list) and len(prov["sources"]) > 0
    assert isinstance(prov.get("processing"), list) and len(prov["processing"]) > 0

    for step in prov["processing"]:
        assert step.get("run_utc")
        for k in ("inputs", "outputs"):
            for it in step.get(k, []) or []:
                assert it.get("path")

    # Coupled regression: validator must accept the generated result.
    _run_validator(repo_root)


def test_autogen_provenance_local_only_inserts_required_note(tmp_path: Path):
    repo_root = tmp_path
    (repo_root / "docs" / "provenance").mkdir(parents=True, exist_ok=True)

    cid = "tst_local_only"
    canon_rel = "data_proc/local_only.jsonl"
    canon_sha = _write_dummy_jsonl(repo_root, canon_rel, lines=1)

    registry = {
        "corpora": [
            {
                "corpus_id": cid,
                "title": "Local Only Corpus",
                "canon_jsonl": {"path": canon_rel, "sha256": canon_sha},
                "rights": {
                    "tier": "Z_local_only",
                    "license": "LOCAL-ONLY",
                    "distributable": False,
                    "notes": "",
                },
            }
        ]
    }
    _wjson(repo_root / "docs" / "corpora.json", registry)

    report = {
        "repo_root": "C:/local/repo",
        "generated_utc": "2026-02-25T12:00:00Z",
        "items": [
            {
                "corpus_id": cid,
                "work_id": "urn:cts:test:work.local",
                "tei": "data_raw/local/work.xml",
                "out_jsonl": "data_proc/_harvest_local/work.jsonl",
            }
        ],
    }
    report_path = repo_root / "runs" / "harvest" / "harvest_20260225_120001.json"
    _wjson(report_path, report)

    # Existing provenance missing notes; generator must patch-in-place (no overwrite)
    _wjson(repo_root / "docs" / "provenance" / f"{cid}.json", {"corpus_id": cid, "rights": {"distributable": False}})

    _run_generator(repo_root, report_path)

    prov = json.loads((repo_root / "docs" / "provenance" / f"{cid}.json").read_text(encoding="utf-8"))
    notes = (prov.get("rights") or {}).get("notes", "") or ""
    assert "not for redistribution" in notes.lower()

    # If strict-rights checks the phrase, keep validator coupled too.
    _run_validator(repo_root)
