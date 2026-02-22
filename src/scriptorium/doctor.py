from __future__ import annotations

import json
import os
import platform
import sys
import urllib.request
from pathlib import Path
from typing import Any

from .config import Config


def _try_import(mod: str) -> tuple[bool, str]:
    try:
        __import__(mod)
        return True, ""
    except Exception as e:
        return False, f"{mod}: {type(e).__name__}: {e}"


def _is_probably_path(s: str) -> bool:
    # Windows-ish heuristics + allow relative paths
    if ":\\" in s or s.startswith("\\\\") or s.startswith(".\\") or s.startswith("..\\"):
        return True
    if s.startswith("/") or s.startswith("./") or s.startswith("../"):
        return True
    # If it exists as a path, treat as path even if heuristic misses it
    try:
        return Path(s).exists()
    except Exception:
        return False


def _faiss_bundle_global(vec_dir: Path) -> dict[str, Any] | None:
    idx = vec_dir / "index.faiss"
    ids = vec_dir / "ids.json"
    meta = vec_dir / "meta.json"
    if not (idx.exists() or ids.exists() or meta.exists()):
        return None
    return {
        "format": "global",
        "index": idx,
        "ids": ids,
        "meta": meta,
        "complete": idx.exists() and ids.exists() and meta.exists(),
    }


def _faiss_bundle_legacy(vec_dir: Path) -> dict[str, Any] | None:
    indexes = sorted(vec_dir.glob("*.index"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not indexes:
        return None
    idx = indexes[0]
    base = Path(str(idx)[:-len(".index")])
    ids = Path(str(base) + "_ids.json")
    meta = Path(str(base) + "_meta.jsonl")
    return {
        "format": "legacy",
        "index": idx,
        "ids": ids,
        "meta": meta,
        "complete": idx.exists() and ids.exists() and meta.exists(),
    }


def run_doctor(cfg: Config, *, strict: bool = False, as_json_out: bool = False, check_llm: bool = False) -> int:
    info: dict[str, Any] = {}
    errors: list[str] = []
    warnings: list[str] = []

    # Environment
    info["python_executable"] = sys.executable
    info["python_version"] = sys.version.split()[0]
    info["platform"] = platform.platform()
    info["venv_detected"] = (".venv" in str(sys.executable).lower()) or (os.environ.get("VIRTUAL_ENV") is not None)

    if sys.version_info < (3, 11):
        errors.append("Python >= 3.11 required (tomllib).")

    # Root + expected dirs (do not warn; local-first repos often keep these untracked)
    root = cfg.project_root
    info["project_root"] = str(root)
    if not root.exists():
        errors.append(f"project_root does not exist: {root}")

    expected = ["src", "configs", "docs", "sample_data"]
    missing_expected = [str(root / rel) for rel in expected if not (root / rel).exists()]
    if missing_expected:
        warnings.append("missing expected repo directories: " + ", ".join(missing_expected))

    # Release script presence (warn only)
    if not cfg.release_ps1.exists():
        warnings.append(f"release_window.ps1 not found: {cfg.release_ps1}")
    else:
        info["release_ps1"] = str(cfg.release_ps1)

    # Embedding model gate (local-first enforcement)
    info["embed_model"] = cfg.embed_model
    info["use_e5_prefix"] = cfg.use_e5_prefix
    is_path = _is_probably_path(cfg.embed_model)

    if is_path:
        mp = Path(cfg.embed_model)
        if not mp.exists():
            errors.append(f"Embedding model path does not exist: {mp}")
        else:
            info["embed_model_is_path"] = True
    else:
        msg = (
            "Embedding model is a Hugging Face ID (not local-first). "
            "Set [embeddings].model to a local path."
        )
        if strict:
            errors.append(msg)
        else:
            warnings.append(msg)

    # Imports (required for pipeline)
    for mod in ["faiss", "numpy", "sentence_transformers", "torch", "transformers"]:
        ok, msg = _try_import(mod)
        if not ok:
            warnings.append(f"import check failed: {msg}")

    # Derived artifacts (informational only)
    db_path = root / "db" / "scriptorium.sqlite"
    info["db_sqlite_exists"] = db_path.exists()

    global_vec = root / "indexes" / "vec_faiss_global"
    info["vec_faiss_global_dir"] = str(global_vec)
    gb = _faiss_bundle_global(global_vec) if global_vec.exists() else None
    if gb:
        info["faiss_bundle_global"] = {k: (str(v) if isinstance(v, Path) else v) for k, v in gb.items()}

    # Optional LLM reachability
    if check_llm:
        url = cfg.llm_base_url.rstrip("/") + "/models"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                j = json.loads(body)
                models = [m.get("id") for m in j.get("data", []) if isinstance(m, dict) and m.get("id")]
                info["llm_models_count"] = len(models)
                info["llm_models_sample"] = models[:10]
        except Exception as e:
            msg = f"LLM unreachable at {url}: {type(e).__name__}: {e}"
            if strict:
                errors.append(msg)
            else:
                warnings.append(msg)

    report = {
        "ok": (len(errors) == 0) and (len(warnings) == 0 if strict else True),
        "errors": errors,
        "warnings": warnings,
        "info": info,
        "config": {
            "window": cfg.window,
            "tag": cfg.tag,
            "query_out_parent": str(cfg.query_out_parent),
            "llm_base_url": cfg.llm_base_url,
            "llm_model": cfg.llm_model,
        },
    }

    if as_json_out:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"[doctor] ok={report['ok']}")
        for e in errors:
            print(f"[error] {e}")
        for w in warnings:
            print(f"[warn ] {w}")

    if errors:
        return 2
    if strict and warnings:
        return 2
    return 0