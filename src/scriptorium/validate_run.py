from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any

from .config import Config


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _read_jsonl(p: Path) -> list[dict]:
    out: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _extract_candidates(record: dict) -> list[dict]:
    if isinstance(record.get("candidates"), list):
        return record["candidates"]
    for k in ("bede_candidates", "top", "results"):
        if isinstance(record.get(k), list):
            return record[k]
    raise KeyError("Could not find candidate list in candidates.jsonl record.")


def _cand_id(c: dict) -> str:
    return str(c.get("id") or c.get("bede_id") or c.get("passage_id") or c.get("pid") or "")


def _detect_type(run_dir: Path) -> str:
    if (run_dir / "batch.json").exists():
        return "answer_batch"
    if (run_dir / "answer.json").exists() or (run_dir / "retrieval" / "candidates.jsonl").exists():
        return "answer_run"
    return "unknown"


def _validate_answer_json(obj: dict, allowed_ids: set[str]) -> None:
    if not isinstance(obj, dict):
        raise ValueError("answer.json is not a JSON object")

    allowed_top = {"schema", "query", "answer", "citations"}
    extra_top = set(obj.keys()) - allowed_top
    if extra_top:
        raise ValueError(f"Extra top-level keys not allowed: {sorted(extra_top)}")

    for k in ("schema", "query", "answer", "citations"):
        if k not in obj:
            raise ValueError(f"Missing key: {k}")

    if obj["schema"] != "scriptorium.answer.v1":
        raise ValueError(f"schema mismatch: {obj['schema']}")

    if not isinstance(obj["citations"], list):
        raise ValueError("citations must be a list")

    allowed_cit = {"id", "support"}
    for c in obj["citations"]:
        if not isinstance(c, dict):
            raise ValueError("citation entry must be an object")
        extra_c = set(c.keys()) - allowed_cit
        if extra_c:
            raise ValueError(f"Extra citation keys not allowed: {sorted(extra_c)}")
        cid = c.get("id")
        if cid not in allowed_ids:
            raise ValueError(f"citation id not allowed: {cid}")


def _validate_answer_run(run_dir: Path, *, strict: bool) -> tuple[list[str], list[str], dict]:
    errors: list[str] = []
    warnings: list[str] = []
    info: dict = {"type": "answer_run", "dir": str(run_dir)}

    retrieval = run_dir / "retrieval"
    cand_path = retrieval / "candidates.jsonl"
    ans_path = run_dir / "answer.json"
    meta_path = run_dir / "answer_meta.json"

    if not cand_path.exists():
        errors.append(f"missing: {cand_path}")
        return errors, warnings, info

    if not ans_path.exists():
        errors.append(f"missing: {ans_path}")
        return errors, warnings, info

    if not meta_path.exists():
        warnings.append(f"missing: {meta_path} (cannot validate allowed_ids; will fallback to candidates)")
    else:
        info["answer_meta_sha256"] = _sha256_file(meta_path)

    # Optional audit artifacts
    for opt in ("prompt_system.txt", "prompt_user.txt"):
        p = run_dir / opt
        if not p.exists():
            (errors if strict else warnings).append(f"missing: {p}")
        else:
            info[f"{opt}_sha256"] = _sha256_file(p)

    raw_attempts = sorted(run_dir.glob("answer_raw_attempt*.txt"))
    if not raw_attempts:
        p = run_dir / "answer_raw.txt"
        if not p.exists():
            (errors if strict else warnings).append(f"missing: {p} (no raw model output saved)")
        else:
            info["answer_raw_sha256"] = _sha256_file(p)
    else:
        info["answer_raw_attempts"] = [str(p) for p in raw_attempts]

    # Parse candidates and determine allowed IDs
    try:
        recs = _read_jsonl(cand_path)
        if not recs:
            errors.append("candidates.jsonl is empty")
            return errors, warnings, info
        rec0 = recs[0]
        cands = _extract_candidates(rec0)
        cand_ids = [_cand_id(c) for c in cands if _cand_id(c)]
        if not cand_ids:
            errors.append("no candidate IDs found in candidates.jsonl")
            return errors, warnings, info
        info["candidates_sha256"] = _sha256_file(cand_path)
        info["candidates_count"] = len(cand_ids)
    except Exception as e:
        errors.append(f"failed to parse candidates.jsonl: {type(e).__name__}: {e}")
        return errors, warnings, info

    allowed_ids: set[str] = set()
    if meta_path.exists():
        try:
            meta = _read_json(meta_path)
            allowed = meta.get("allowed_ids")
            if isinstance(allowed, list) and all(isinstance(x, str) for x in allowed):
                allowed_ids = set(allowed)
            else:
                warnings.append("answer_meta.json has no valid allowed_ids; using candidates list as allowed set")
        except Exception as e:
            warnings.append(f"failed to parse answer_meta.json; using candidates list: {type(e).__name__}: {e}")

    if not allowed_ids:
        allowed_ids = set(cand_ids)

    # Validate answer.json
    try:
        ans = _read_json(ans_path)
        _validate_answer_json(ans, allowed_ids)
        info["answer_sha256"] = _sha256_file(ans_path)
        info["citations_count"] = len(ans.get("citations", []))
    except Exception as e:
        errors.append(f"answer.json validation failed: {type(e).__name__}: {e}")

    return errors, warnings, info


def _validate_answer_batch(run_dir: Path, *, strict: bool) -> tuple[list[str], list[str], dict]:
    errors: list[str] = []
    warnings: list[str] = []
    info: dict = {"type": "answer_batch", "dir": str(run_dir)}

    batch_path = run_dir / "batch.json"
    results_path = run_dir / "results.jsonl"
    summary_path = run_dir / "summary.json"

    for p in (batch_path, results_path, summary_path):
        if not p.exists():
            errors.append(f"missing: {p}")
            return errors, warnings, info

    try:
        batch = _read_json(batch_path)
        if batch.get("schema") != "scriptorium.answer_batch.v1":
            (errors if strict else warnings).append(f"batch.json schema mismatch: {batch.get('schema')}")
        total = int(batch.get("count", 0))
    except Exception as e:
        errors.append(f"failed to parse batch.json: {type(e).__name__}: {e}")
        return errors, warnings, info

    try:
        summary = _read_json(summary_path)
        if summary.get("schema") != "scriptorium.answer_batch_summary.v1":
            (errors if strict else warnings).append(f"summary.json schema mismatch: {summary.get('schema')}")
        s_total = int(summary.get("total", -1))
        s_ok = int(summary.get("ok", -1))
        s_failed = int(summary.get("failed", -1))
        s_skipped = int(summary.get("skipped", -1))
        if s_total != total:
            errors.append(f"summary.total ({s_total}) != batch.count ({total})")
        if s_ok + s_failed + s_skipped != s_total:
            errors.append("summary counts do not add up: ok+failed+skipped != total")
    except Exception as e:
        errors.append(f"failed to parse summary.json: {type(e).__name__}: {e}")
        return errors, warnings, info

    # results.jsonl may contain multiple runs appended; take last status per qid_full
    try:
        lines = _read_jsonl(results_path)
        if not lines:
            errors.append("results.jsonl is empty")
            return errors, warnings, info
    except Exception as e:
        errors.append(f"failed to parse results.jsonl: {type(e).__name__}: {e}")
        return errors, warnings, info

    latest: dict[str, dict] = {}
    for r in lines:
        qid_full = r.get("qid_full") or r.get("qid")
        if isinstance(qid_full, str) and qid_full:
            latest[qid_full] = r

    if len(latest) != total:
        (errors if strict else warnings).append(
            f"unique qid_full in results ({len(latest)}) != batch.count ({total}); may indicate changed input file"
        )

    # Validate each latest record points to expected artifacts
    ok_ct = 0
    failed_ct = 0
    skipped_ct = 0
    failures: list[dict] = []

    for qid_full, r in latest.items():
        if "qid" not in r:
            (errors if strict else warnings).append(f"missing qid in results record for {qid_full}")
        if "dir" not in r:
            (errors if strict else warnings).append(f"missing dir in results record for {qid_full}")
            continue

        q_dir = Path(r["dir"])
        if not q_dir.exists():
            (errors if strict else warnings).append(f"dir does not exist: {q_dir}")
            continue

        okv = bool(r.get("ok"))
        sk = bool(r.get("skipped"))
        if okv and sk:
            skipped_ct += 1
        elif okv and not sk:
            ok_ct += 1
        else:
            failed_ct += 1
            failures.append({"qid_full": qid_full, "error": r.get("error", "")})

        # Artifact presence checks
        if okv:
            if batch.get("params", {}).get("dry_run", False):
                marker = q_dir / "retrieval" / "candidates.jsonl"
            else:
                marker = q_dir / "answer.json"
            if not marker.exists():
                errors.append(f"expected output missing for {qid_full}: {marker}")

    # Compare to summary (best-effort; allow warnings if mismatch due to appended logs)
    if ok_ct != s_ok or failed_ct != s_failed or skipped_ct != s_skipped:
        (errors if strict else warnings).append(
            f"latest results counts (ok={ok_ct}, failed={failed_ct}, skipped={skipped_ct}) "
            f"!= summary (ok={s_ok}, failed={s_failed}, skipped={s_skipped})"
        )

    info["batch_sha256"] = _sha256_file(batch_path)
    info["results_sha256"] = _sha256_file(results_path)
    info["summary_sha256"] = _sha256_file(summary_path)
    info["latest_counts"] = {"ok": ok_ct, "failed": failed_ct, "skipped": skipped_ct}
    info["failures_sample"] = failures[:10]

    return errors, warnings, info


def run_validate(cfg: Config, run_dir: Path, *, strict: bool = False, as_json_out: bool = False) -> int:
    # Resolve relative to project root for convenience
    run_dir = run_dir if run_dir.is_absolute() else (cfg.project_root / run_dir).resolve()

    rtype = _detect_type(run_dir)
    if rtype == "answer_run":
        errors, warnings, info = _validate_answer_run(run_dir, strict=strict)
    elif rtype == "answer_batch":
        errors, warnings, info = _validate_answer_batch(run_dir, strict=strict)
    else:
        errors = [f"could not detect run type under: {run_dir}"]
        warnings = []
        info = {"type": "unknown", "dir": str(run_dir)}

    report = {
        "schema": "scriptorium.validate_run.v1",
        "ok": (len(errors) == 0) and (len(warnings) == 0 if strict else True),
        "type": info.get("type"),
        "dir": str(run_dir),
        "errors": errors,
        "warnings": warnings,
        "info": info,
    }

    if as_json_out:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"[validate-run] ok={report['ok']} type={report['type']} dir={report['dir']}")
        for e in errors:
            print(f"[error] {e}")
        for w in warnings:
            print(f"[warn ] {w}")

    if errors:
        return 2
    if strict and warnings:
        return 2
    return 0