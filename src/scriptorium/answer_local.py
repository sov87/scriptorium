from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import Config
from .llm_openai import chat_completions


def _read_jsonl(p: Path) -> list[dict]:
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _extract_candidates(record: dict) -> list[dict]:
    # Most likely key
    if isinstance(record.get("candidates"), list):
        return record["candidates"]
    # Fallbacks
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
    # Keep it deterministic and citation-locked.
    lines = []
    lines.append("TASK: Answer the query using ONLY the provided passages.")
    lines.append("RETURN: Strict JSON only. No markdown. No extra keys.")
    lines.append("")
    lines.append('JSON SCHEMA (must match exactly):')
    lines.append('{"schema":"scriptorium.answer.v1","query":string,"answer":string,"citations":[{"id":string,"support":string}]}')
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


def run_answer(cfg: Config, *, query_text: str, out_dir: Path | None, topk: int | None, bm25_k: int | None, vec_k: int | None, k_passages: int | None, dry_run: bool = False) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = out_dir or (cfg.answer_out_parent / f"q_{stamp}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Retrieval (unchanged): reuse existing query script, but place its outputs under out_dir\retrieval
    retrieval_dir = out_dir / "retrieval"
    retrieval_dir.mkdir(parents=True, exist_ok=True)

    qscript = cfg.project_root / "src" / "query_bede_hybrid_faiss.py"
    cmd = [
        sys.executable, str(qscript),
        "--query", query_text,
        "--out_dir", str(retrieval_dir),
        "--bm25", str(cfg.bm25_path),
        "--vec_dir", str(cfg.vec_dir),
        "--model", str(cfg.embed_model),
    ]
    cmd += ["--topk", str(topk if topk is not None else cfg.query_topk)]
    cmd += ["--bm25_k", str(bm25_k if bm25_k is not None else cfg.query_bm25_k)]
    cmd += ["--vec_k", str(vec_k if vec_k is not None else cfg.query_vec_k)]
    if cfg.use_e5_prefix:
        cmd.append("--use_e5_prefix")

    subprocess.run(cmd, check=True)

    cand_path = retrieval_dir / "candidates.jsonl"
    if not cand_path.exists():
        raise FileNotFoundError(f"Expected candidates.jsonl not found: {cand_path}")

    if dry_run:
        # Retrieval-only mode: do not parse or call LLM.
        return cand_path

    recs = _read_jsonl(cand_path)
    if not recs:
        raise RuntimeError("Empty candidates.jsonl")
    rec = recs[0]

    cands = _extract_candidates(rec)
    # Keep rank order as-is; take top k_passages for prompt
    k_pass = k_passages if k_passages is not None else cfg.answer_k_passages
    picked = []
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

    # If candidate entries don’t carry text, fetch from Bede canon by ID
    passages: list[tuple[str, str]] = []
    missing = {pid for pid, c in zip(need_ids, picked) if not _cand_text(c).strip()}
    bede_map = _load_bede_by_id(cfg.bede_canon, missing) if missing else {}

    for c in picked:
        pid = _cand_id(c)
        txt = _cand_text(c).strip()
        if not txt:
            r = bede_map.get(pid)
            if r:
                txt = str(r.get("txt", "")).strip()
        passages.append((pid, txt))

    # 2) LLM answer (strict JSON)
    sys_msg = "You are a constrained question-answering component for SCRIPTORIUM. Output strict JSON only."
    user_msg = _build_prompt(query_text, passages)
    (out_dir / "prompt_system.txt").write_text(sys_msg + "\n", encoding="utf-8")
    (out_dir / "prompt_user.txt").write_text(user_msg + "\n", encoding="utf-8")

    messages = [
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": user_msg},
    ]

    last = ""
    for attempt in range(2):
        last = chat_completions(
            base_url=cfg.llm_base_url,
            model=cfg.llm_model,
            messages=messages,
            temperature=cfg.llm_temperature,
            max_output_tokens=cfg.llm_max_output_tokens,
            timeout_seconds=cfg.llm_timeout_seconds,
        ).strip()
        
        (out_dir / f"answer_raw_attempt{attempt+1}.txt").write_text(last + "\n", encoding="utf-8")
        (out_dir / "answer_raw.txt").write_text(last + "\n", encoding="utf-8")

        try:
            obj = json.loads(last)
            _validate_answer(obj, allowed)
            # Write normalized JSON
            (out_dir / "answer.json").write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            meta = {
                "schema": "scriptorium.answer_meta.v1",
                "query": query_text,
                "retrieval_candidates": str(cand_path),
                "allowed_ids": sorted(allowed),
                "llm": {
                    "base_url": cfg.llm_base_url,
                    "model": cfg.llm_model,
                    "temperature": cfg.llm_temperature,
                    "max_output_tokens": cfg.llm_max_output_tokens,
                },
            }
            (out_dir / "answer_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            return out_dir / "answer.json"
        except Exception as e:
            # Retry once with explicit correction instructions
            messages = [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": user_msg},
                {"role": "user", "content": f"Your prior output was invalid: {type(e).__name__}: {e}. Output ONLY valid JSON matching the schema, and cite ONLY allowed IDs."},
            ]

    # If we get here, we failed twice
    (out_dir / "answer_raw.txt").write_text(last, encoding="utf-8")
    raise RuntimeError("LLM output could not be validated; wrote answer_raw.txt for inspection.")