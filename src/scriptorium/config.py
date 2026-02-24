from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class Config:
    project_root: Path
    db_path: Path
    window: str
    tag: str
    release_ps1: Path

    # Index/retrieval defaults
    bm25_path: Path
    vec_dir: Path
    embed_model: str
    use_e5_prefix: bool

    # Query defaults
    query_out_parent: Path
    query_topk: int
    query_bm25_k: int
    query_vec_k: int

    # Canon
    bede_canon: Path

    # LLM (LM Studio / OpenAI-compatible)
    llm_base_url: str
    llm_model: str
    llm_temperature: float
    llm_max_output_tokens: int
    llm_timeout_seconds: int

    # Answer defaults
    answer_out_parent: Path
    answer_k_passages: int


def _req(d: dict, *keys: str):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            raise KeyError("Missing config key: " + ".".join(keys))
        cur = cur[k]
    return cur


def _get(d: dict, default, *keys: str):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _resolve_under(root: Path, p: str | Path) -> Path:
    p = Path(p)
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def load_config(path: str | Path) -> Config:
    path = Path(path)
    raw = tomllib.loads(path.read_text(encoding="utf-8-sig"))

    project_root = Path(_req(raw, "root", "project_root"))

    # db_path: allow root.db_path in TOML; resolve relative to project_root

    db_path_raw = (raw.get("root", {}) or {}).get("db_path", "db/scriptorium.sqlite")

    db_path = Path(str(db_path_raw))

    if not db_path.is_absolute():

        db_path = (project_root / db_path).resolve()

    else:

        db_path = db_path.resolve()
    window = str(_req(raw, "window", "name"))
    tag = str(_req(raw, "window", "tag"))

    base = path.parent.resolve()
    project_root = (base / project_root).resolve() if not project_root.is_absolute() else project_root.resolve()

    release_ps1 = _get(raw, r"src\release_window.ps1", "ps", "release_window_ps1")
    release_ps1 = _resolve_under(project_root, release_ps1)

    db_path_rel = _get(raw, "", "root", "db_path") or _get(raw, "db/scriptorium.sqlite", "indexes", "db_path")
    db_path = _resolve_under(project_root, db_path_rel)

    bm25_rel = _get(raw, r"indexes\bm25\oe_bede_prod_utf8.pkl", "indexes", "bm25")
    vec_dir_rel = _get(raw, r"indexes\vec_faiss", "indexes", "vec_dir")

    embed_model = str(_get(raw, "intfloat/multilingual-e5-base", "embeddings", "model"))
    use_e5_prefix = bool(_get(raw, True, "embeddings", "use_e5_prefix"))

    query_out_parent_rel = _get(raw, r"runs\query_hybrid", "query", "out_parent")
    query_topk = int(_get(raw, 8, "query", "topk"))
    query_bm25_k = int(_get(raw, 24, "query", "bm25_k"))
    query_vec_k = int(_get(raw, 24, "query", "vec_k"))

    bede_rel = _get(raw, r"data_proc\oe_bede_prod_utf8.jsonl", "canon", "bede")

    llm_base_url = str(_get(raw, "http://localhost:1234/v1", "llm", "base_url"))
    llm_model = str(_get(raw, "qwen/qwen3-30b-a3b-2507", "llm", "model"))
    llm_temperature = float(_get(raw, 0.2, "llm", "temperature"))
    llm_max_output_tokens = int(_get(raw, 1200, "llm", "max_output_tokens"))
    llm_timeout_seconds = int(_get(raw, 120, "llm", "timeout_seconds"))

    answer_out_parent_rel = _get(raw, r"runs\answer_local", "answer", "out_parent")
    answer_k_passages = int(_get(raw, 12, "answer", "k_passages"))

    return Config(
        project_root=project_root,
        db_path=db_path,
        window=window,
        tag=tag,
        release_ps1=release_ps1,

        bm25_path=_resolve_under(project_root, bm25_rel),
        vec_dir=_resolve_under(project_root, vec_dir_rel),
        embed_model=embed_model,
        use_e5_prefix=use_e5_prefix,

        query_out_parent=_resolve_under(project_root, query_out_parent_rel),
        query_topk=query_topk,
        query_bm25_k=query_bm25_k,
        query_vec_k=query_vec_k,

        bede_canon=_resolve_under(project_root, bede_rel),

        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_temperature=llm_temperature,
        llm_max_output_tokens=llm_max_output_tokens,
        llm_timeout_seconds=llm_timeout_seconds,

        answer_out_parent=_resolve_under(project_root, answer_out_parent_rel),
        answer_k_passages=answer_k_passages,
    )