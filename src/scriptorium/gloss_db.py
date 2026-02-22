from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slug(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip())
    s = re.sub(r"[^A-Za-z0-9 _-]+", "", s)
    s = s.replace(" ", "_")
    return s[:48] if s else "x"


def _http_json(url: str, payload: dict[str, Any], api_key: str, timeout_s: int = 180) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _http_get_json(url: str, api_key: str, timeout_s: int = 30) -> dict[str, Any]:
    req = urllib.request.Request(
        url=url,
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def pick_model_id(base_url: str, api_key: str) -> str | None:
    try:
        j = _http_get_json(base_url.rstrip("/") + "/models", api_key=api_key, timeout_s=15)
        data = j.get("data") or []
        if isinstance(data, list) and data:
            mid = data[0].get("id")
            if isinstance(mid, str) and mid:
                return mid
    except Exception:
        return None
    return None


def _read_lines(p: Path) -> list[str]:
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    out: list[str] = []
    for s in lines:
        s = s.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def _iter_segments(
    con: sqlite3.Connection,
    corpus: str,
    ids: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    if ids:
        # Preserve input order
        qmarks = ",".join(["?"] * len(ids))
        where = f"id in ({qmarks})"
        params: list[Any] = list(ids)
        if corpus:
            where = f"({where}) and corpus_id=?"
            params.append(corpus)
        rows = con.execute(
            f"select corpus_id,id,coalesce(work_id,''),coalesce(loc,''),text from segments where {where}",
            params,
        ).fetchall()
        by_id = {r[1]: r for r in rows}
        out = []
        for rid in ids:
            r = by_id.get(rid)
            if not r:
                continue
            out.append({"corpus_id": r[0], "id": r[1], "work_id": r[2], "loc": r[3], "text": r[4]})
        return out

    where = "1=1"
    params2: list[Any] = []
    if corpus:
        where = "corpus_id=?"
        params2.append(corpus)
    sql = f"select corpus_id,id,coalesce(work_id,''),coalesce(loc,''),text from segments where {where} order by corpus_id,id"
    if limit and limit > 0:
        sql += " limit ?"
        params2.append(int(limit))
    rows2 = con.execute(sql, params2).fetchall()
    return [{"corpus_id": r[0], "id": r[1], "work_id": r[2], "loc": r[3], "text": r[4]} for r in rows2]


ALLOWED_KEYS = {"gloss", "literal", "notes"}


def build_gloss_prompt(seg: dict[str, Any]) -> tuple[str, str]:
    system = (
        "You are producing a philological gloss for a short historical-language segment.\n"
        "Return ONLY valid JSON. No markdown. No extra keys.\n\n"
        "JSON schema:\n"
        "{"
        "\"gloss\": string, "
        "\"literal\": string, "
        "\"notes\": [string]"
        "}\n\n"
        "Rules:\n"
        "- gloss: concise modern-English meaning (1–3 sentences).\n"
        "- literal: if feasible, a more literal/word-by-word rendering; otherwise empty string.\n"
        "- notes: optional short notes; use [] if none.\n"
        "- Do not invent names, events, or context not present in the text.\n"
    )
    txt = seg.get("text", "")
    if len(txt) > 1600:
        txt = txt[:1600] + "…"
    user = (
        f"SEGMENT_ID: {seg.get('id','')}\n"
        f"CORPUS: {seg.get('corpus_id','')}\n"
        f"WORK: {seg.get('work_id','')}\n"
        f"LOC: {seg.get('loc','')}\n\n"
        "TEXT:\n"
        f"{txt}\n\n"
        "Produce the JSON gloss now."
    )
    return system, user


def validate_gloss_obj(obj: Any) -> list[str]:
    errs: list[str] = []
    if not isinstance(obj, dict):
        return ["root is not a JSON object"]
    extra = set(obj.keys()) - ALLOWED_KEYS
    if extra:
        errs.append("extra keys: " + ", ".join(sorted(extra)))
    g = obj.get("gloss")
    if not isinstance(g, str) or not g.strip():
        errs.append("missing/invalid 'gloss' (string)")
    lit = obj.get("literal", "")
    if lit is not None and not isinstance(lit, str):
        errs.append("invalid 'literal' (string)")
    notes = obj.get("notes", [])
    if notes is not None:
        if not isinstance(notes, list) or any(not isinstance(x, str) for x in notes):
            errs.append("invalid 'notes' (list of strings)")
    return errs


def _load_done_ids(*paths: Path) -> set[str]:
    done: set[str] = set()
    for p in paths:
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                rid = j.get("id")
                if isinstance(rid, str) and rid:
                    done.add(rid)
            except Exception:
                continue
    return done


@dataclass
class GlossDbArgs:
    db_path: Path
    corpus: str
    ids_path: Path | None
    out_root: Path
    dry_run: bool
    cont: bool
    limit: int
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    max_tokens: int
    temperature: float


def run_gloss_db(a: GlossDbArgs) -> Path:
    tag = slug(a.corpus) if a.corpus else ("ids" if a.ids_path else "all")
    run_id = f"{utc_stamp()}_{tag}"
    out_dir = (a.out_root / run_id).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    gloss_path = out_dir / "gloss.jsonl"
    raw_path = out_dir / "gloss_raw.jsonl"
    err_path = out_dir / "errors.jsonl"

    done_ids: set[str] = set()
    if a.cont:
        done_ids = _load_done_ids(gloss_path, err_path)

    con = sqlite3.connect(str(a.db_path))
    try:
        ids = _read_lines(a.ids_path) if a.ids_path else None
        segs = _iter_segments(con, a.corpus, ids, a.limit)
    finally:
        con.close()

    meta = {
        "schema": "scriptorium.gloss_db.meta.v1",
        "generated_utc": utc_iso(),
        "db": str(a.db_path),
        "corpus": a.corpus,
        "ids_path": str(a.ids_path) if a.ids_path else "",
        "limit": int(a.limit),
        "out_dir": str(out_dir),
        "dry_run": bool(a.dry_run),
        "continue": bool(a.cont),
        "llm_base_url": a.llm_base_url,
        "llm_model": a.llm_model,
        "max_tokens": int(a.max_tokens),
        "temperature": float(a.temperature),
        "count_total": len(segs),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    base = a.llm_base_url.rstrip("/")
    model = a.llm_model.strip()
    if not model and not a.dry_run:
        picked = pick_model_id(base, a.llm_api_key)
        if picked:
            model = picked
        else:
            raise SystemExit("No llm_model provided and /models lookup failed.")

    ok = 0
    skipped = 0
    failed = 0

    with gloss_path.open("a", encoding="utf-8", newline="\n") as f_out, raw_path.open(
        "a", encoding="utf-8", newline="\n"
    ) as f_raw, err_path.open("a", encoding="utf-8", newline="\n") as f_err:
        for seg in segs:
            sid = str(seg["id"])
            if a.cont and sid in done_ids:
                skipped += 1
                continue

            if a.dry_run:
                rec = {
                    "id": sid,
                    "corpus_id": seg["corpus_id"],
                    "work_id": seg["work_id"],
                    "loc": seg["loc"],
                    "gloss": "",
                    "literal": "",
                    "notes": ["dry_run=true"],
                }
                f_out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
                ok += 1
                continue

            system, user = build_gloss_prompt(seg)
            payload = {
                "model": model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": float(a.temperature),
                "max_tokens": int(a.max_tokens),
            }

            t0 = time.time()
            try:
                resp = _http_json(base + "/chat/completions", payload, api_key=a.llm_api_key, timeout_s=240)
                dt = time.time() - t0
                content = resp["choices"][0]["message"]["content"]
            except Exception as e:
                failed += 1
                f_err.write(
                    json.dumps({"id": sid, "error": f"{type(e).__name__}: {e}"}, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )
                continue

            f_raw.write(
                json.dumps(
                    {"id": sid, "model": model, "latency_s": dt, "content": content},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )

            try:
                obj = json.loads(content)
            except Exception as e:
                failed += 1
                f_err.write(
                    json.dumps(
                        {"id": sid, "error": f"invalid_json: {e}"},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                continue

            errs = validate_gloss_obj(obj)
            if errs:
                failed += 1
                f_err.write(
                    json.dumps(
                        {"id": sid, "error": "validation_failed", "errors": errs},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                continue

            rec = {
                "id": sid,
                "corpus_id": seg["corpus_id"],
                "work_id": seg["work_id"],
                "loc": seg["loc"],
                "gloss": obj.get("gloss", ""),
                "literal": obj.get("literal", "") if obj.get("literal") is not None else "",
                "notes": obj.get("notes", []) if obj.get("notes") is not None else [],
            }
            f_out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            ok += 1

    summary = {
        "schema": "scriptorium.gloss_db.summary.v1",
        "generated_utc": utc_iso(),
        "ok": ok,
        "skipped": skipped,
        "failed": failed,
        "count_total": len(segs),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(out_dir))
    return out_dir
