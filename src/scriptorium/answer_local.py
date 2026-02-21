from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config
from .llm_openai import chat_completions_raw


def _write_text(p: Path, s: str) -> None:
    p.write_text(s if s.endswith("\n") else (s + "\n"), encoding="utf-8")


def _write_json(p: Path, obj: Any, *, minify: bool = False, indent: int | None = 2) -> None:
    if minify:
        p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    else:
        p.write_text(json.dumps(obj, ensure_ascii=False, indent=indent), encoding="utf-8")


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


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


def _cand_text(c: dict) -> str:
    return str(c.get("txt") or c.get("text") or c.get("bede_txt") or c.get("snippet") or c.get("excerpt") or "")


def _load_bede_by_id(bede_jsonl: Path, need: set[str]) -> dict[str, dict]:
    got: dict[str, dict] = {}
    for line in bede_jsonl.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        rid = str(r.get("id", ""))
        if rid in need:
            got[rid] = r
            if len(got) == len(need):
                break
    return got


def _build_prompt(query: str, passages: list[tuple[str, str]]) -> str:
    # Deterministic + citation-locked.
    lines: list[str] = []
    lines.append("TASK: Answer the query using ONLY the provided passages.")
    lines.append("RETURN: Strict JSON only. No markdown. No extra keys.")
    lines.append("")
    lines.append('JSON SCHEMA (must match exactly):')
    lines.append(
        '{"schema":"scriptorium.answer.v1","query":string,"answer":string,'
        '"citations":[{"id":string,"support":string}]}'
    )
    lines.append("")
    lines.append("RULES:")
    lines.append("- You may cite ONLY passage IDs from the list below.")
    lines.append("- If uncertain, put uncertainty in brackets like [uncertain].")
    lines.append("- citations[].support must be a short phrase grounded in that passage (not invented).")
    lines.append("")
    lines.append(f"QUERY: {query}")
    lines.append("")
    lines.append("PASSAGES:")
    for pid, txt in passages:
        lines.append(f"[ID] {pid}")
        lines.append(txt.strip())
        lines.append("")
    return "\n".join(lines).strip()


def _validate_answer(obj: dict, allowed_ids: set[str]) -> None:
    if not isinstance(obj, dict):
        raise ValueError("Answer is not a JSON object.")

    allowed_top = {"schema", "query", "answer", "citations"}
    extra_top = set(obj.keys()) - allowed_top
    if extra_top:
        raise ValueError(f"Extra top-level keys not allowed: {sorted(extra_top)}")

    for k in ("schema", "query", "answer", "citations"):
        if k not in obj:
            raise ValueError(f"Missing key: {k}")

    if obj["schema"] != "scriptorium.answer.v1":
        raise ValueError("schema mismatch.")

    if not isinstance(obj["citations"], list):
        raise ValueError("citations must be a list.")

    allowed_cit = {"id", "support"}
    for c in obj["citations"]:
        if not isinstance(c, dict):
            raise ValueError("citation entry must be an object.")
        extra_c = set(c.keys()) - allowed_cit
        if extra_c:
            raise ValueError(f"Extra citation keys not allowed: {sorted(extra_c)}")
        cid = c.get("id")
        if cid not in allowed_ids:
            raise ValueError(f"citation id not allowed: {cid}")


def run_answer(
    cfg: Config,
    *,
    query_text: str,
    out_dir: Path | None,
    topk: int | None,
    bm25_k: int | None,
    vec_k: int | None,
    k_passages: int | None,
    dry_run: bool = False,
) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = out_dir or (cfg.answer_out_parent / f"q_{stamp}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Retrieval (subprocess to existing script)
    retrieval_dir = out_dir / "retrieval"
    retrieval_dir.mkdir(parents=True, exist_ok=True)

    qscript = cfg.project_root / "src" / "query_bede_hybrid_faiss.py"
    cmd = [
        sys.executable,
        str(qscript),
        "--query",
        query_text,
        "--out_dir",
        str(retrieval_dir),
        "--bm25",
        str(cfg.bm25_path),
        "--vec_dir",
        str(cfg.vec_dir),
        "--model",
        str(cfg.embed_model),
        "--topk",
        str(topk if topk is not None else cfg.query_topk),
        "--bm25_k",
        str(bm25_k if bm25_k is not None else cfg.query_bm25_k),
        "--vec_k",
        str(vec_k if vec_k is not None else cfg.query_vec_k),
    ]
    if cfg.use_e5_prefix:
        cmd.append("--use_e5_prefix")

    _write_text(out_dir / "retrieval_cmd.txt", " ".join(cmd))
    subprocess.run(cmd, check=True)

    cand_path = retrieval_dir / "candidates.jsonl"
    if not cand_path.exists():
        raise FileNotFoundError(f"Expected candidates.jsonl not found: {cand_path}")

    if dry_run:
        return cand_path

    recs = _read_jsonl(cand_path)
    if not recs:
        raise RuntimeError("Empty candidates.jsonl")
    cands = _extract_candidates(recs[0])

    k_pass = k_passages if k_passages is not None else cfg.answer_k_passages
    picked: list[dict] = []
    need_ids: list[str] = []
    for c in cands:
        pid = _cand_id(c)
        if not pid:
            continue
        picked.append(c)
        need_ids.append(pid)
        if len(picked) >= k_pass:
            break

    allowed = set(need_ids)

    # Ensure we have passage text
    missing = {pid for pid, c in zip(need_ids, picked) if not _cand_text(c).strip()}
    bede_map = _load_bede_by_id(cfg.bede_canon, missing) if missing else {}

    passages: list[tuple[str, str]] = []
    for c in picked:
        pid = _cand_id(c)
        txt = _cand_text(c).strip()
        if not txt:
            r = bede_map.get(pid)
            if r:
                txt = str(r.get("txt", "")).strip()
        passages.append((pid, txt))

    sys_msg = "You are a constrained question-answering component for SCRIPTORIUM. Output strict JSON only."
    user_msg = _build_prompt(query_text, passages)

    _write_text(out_dir / "prompt_system.txt", sys_msg)
    _write_text(out_dir / "prompt_user.txt", user_msg)
    _write_json(out_dir / "allowed_ids.json", sorted(allowed), minify=False)

    # Attempt strategy:
    #  1) normal answer
    #  2) formatting repair of previous output into valid JSON schema
    #  3) re-answer from scratch (strict) if repair fails
    max_attempts = 3
    last_content = ""
    last_error = ""

    # base messages for "answer from passages"
    base_messages = [
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": user_msg},
    ]

    messages = base_messages

    for attempt in range(1, max_attempts + 1):
        resp = chat_completions_raw(
            base_url=cfg.llm_base_url,
            model=cfg.llm_model,
            messages=messages,
            temperature=cfg.llm_temperature,
            max_output_tokens=cfg.llm_max_output_tokens,
            timeout_seconds=cfg.llm_timeout_seconds,
        )
        content = resp["content"].strip()
        last_content = content

        # Persist full request/response per attempt
        _write_json(out_dir / f"llm_request_attempt{attempt}.json", {"url": resp["url"], **resp["request"]}, minify=False)
        _write_json(out_dir / f"llm_response_attempt{attempt}.json", resp["response"], minify=False)
        _write_text(out_dir / f"answer_raw_attempt{attempt}.txt", content)
        _write_text(out_dir / "answer_raw.txt", content)

        try:
            obj = json.loads(content)
            _validate_answer(obj, allowed)

            _write_json(out_dir / "answer.json", obj, minify=True)
            meta = {
                "schema": "scriptorium.answer_meta.v2",
                "query": query_text,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "retrieval_candidates": str(cand_path),
                "allowed_ids": sorted(allowed),
                "prompt_sha256": {
                    "system": _sha256_text(sys_msg),
                    "user": _sha256_text(user_msg),
                },
                "llm": {
                    "url": resp["url"],
                    "base_url": cfg.llm_base_url,
                    "model": cfg.llm_model,
                    "temperature": cfg.llm_temperature,
                    "max_output_tokens": cfg.llm_max_output_tokens,
                    "timeout_seconds": cfg.llm_timeout_seconds,
                },
                "attempt": attempt,
                "content_sha256": _sha256_text(content),
            }
            _write_json(out_dir / "answer_meta.json", meta, minify=False)
            return out_dir / "answer.json"

        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            _write_json(
                out_dir / f"validation_attempt{attempt}.json",
                {
                    "ok": False,
                    "attempt": attempt,
                    "error": last_error,
                },
                minify=False,
            )

            if attempt == 1:
                # Attempt 2: formatting-only repair of the prior output.
                repair_sys = "You are a strict JSON formatter. Output ONLY valid JSON. No commentary."
                repair_user = "\n".join(
                    [
                        "REPAIR TASK:",
                        "Rewrite the prior output into VALID JSON that matches the schema exactly.",
                        "Do NOT add extra keys. Do NOT output markdown.",
                        "Preserve the answer text as much as possible; ensure citations use ONLY allowed IDs.",
                        "",
                        "SCHEMA:",
                        '{"schema":"scriptorium.answer.v1","query":string,"answer":string,"citations":[{"id":string,"support":string}]}',
                        "",
                        "ALLOWED IDS:",
                        ", ".join(sorted(allowed)),
                        "",
                        "PRIOR OUTPUT (may be invalid JSON):",
                        content,
                        "",
                        "ERROR:",
                        last_error,
                    ]
                )
                _write_text(out_dir / "repair_prompt_system.txt", repair_sys)
                _write_text(out_dir / "repair_prompt_user.txt", repair_user)
                messages = [
                    {"role": "system", "content": repair_sys},
                    {"role": "user", "content": repair_user},
                ]
                continue

            if attempt == 2:
                # Attempt 3: re-answer from passages with explicit failure note.
                messages = base_messages + [
                    {
                        "role": "user",
                        "content": f"Your prior output was invalid ({last_error}). Output ONLY valid JSON matching the schema.",
                    }
                ]
                continue

    # Failed all attempts
    raise RuntimeError(
        "LLM output could not be validated after retries; inspect answer_raw_attempt*.txt and validation_attempt*.json"
    )