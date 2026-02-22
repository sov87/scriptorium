from __future__ import annotations

import json
import platform
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .corpora_registry import generate_registry, validate_registry


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Always include these roots in every snapshot (defensible/reconstructible).
ALWAYS_INCLUDE_DIRS = ("src", "configs", "docs")

# Never include these dirs anywhere in the zip (even if found under included roots).
EXCLUDE_DIRNAMES = {
    ".git",
    ".venv",
    "__pycache__",
    "models",
    "data_raw",
    "releases",
}

# Never include these file types/names.
EXCLUDE_SUFFIXES = {".pyc", ".pyo"}
EXCLUDE_FILENAMES = {".DS_Store"}


def build_snapshot_bundle(
    project_root: Path,
    window: str,
    tag: str,
    config_path: Path,
    include_canon: bool = True,
    include_extra: list[str] | None = None,
) -> Path:
    """
    Creates a zip snapshot under releases/ that includes:
      - src/ (always)
      - configs/ (always)
      - docs/ (always)
      - docs/corpora.json (regenerated from provenance)
      - docs/provenance/*.json (best-effort per corpus)
      - indexes artifacts per corpus (BM25 + FAISS)
      - optionally canon JSONL per corpus
      - config_used.toml (the TOML config used)
      - optional extra paths (include_extra)
      - manifest.json describing included files

    Notes:
      - data_raw/, models/, .venv/, .git/ are never included.
      - Canon inclusion is controlled by include_canon.
    """
    root = Path(project_root)
    registry_path = root / "docs" / "corpora.json"

    # (Re)generate registry from provenance to keep it authoritative.
    reg_path, warnings = generate_registry(root, out_path=registry_path)

    v = validate_registry(root, registry_path=registry_path)
    if not v.get("ok"):
        bad = [r for r in (v.get("results", []) or []) if isinstance(r, dict) and not r.get("ok")]
        raise RuntimeError(
            f"corpora registry validation failed for {len(bad)} corpora; fix missing/mismatched artifacts before snapshot"
        )

    reg = json.loads(reg_path.read_text(encoding="utf-8"))
    corpora = reg.get("corpora", []) or []

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = root / "releases"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"snapshot_{window}_{tag}_{stamp}.zip"

    added: set[str] = set()
    included_files: list[str] = []
    included_corpora: list[dict[str, Any]] = []

    def _to_rel_posix(rel: str | Path) -> str:
        p = rel if isinstance(rel, Path) else Path(str(rel))
        if not isinstance(rel, Path):
            p = Path(str(rel).replace("\\", "/"))
        if p.is_absolute():
            try:
                p = p.relative_to(root)
            except Exception as e:
                raise ValueError(f"snapshot include path must be under project_root: {p}") from e
        return p.as_posix()

    def _is_excluded_path(rel_posix: str) -> bool:
        parts = Path(rel_posix).parts
        # Exclude if any component is in EXCLUDE_DIRNAMES
        if any(part in EXCLUDE_DIRNAMES for part in parts):
            return True
        # Exclude file rules
        name = Path(rel_posix).name
        if name in EXCLUDE_FILENAMES:
            return True
        if Path(rel_posix).suffix.lower() in EXCLUDE_SUFFIXES:
            return True
        return False

    def add_rel(z: zipfile.ZipFile, rel: str | Path) -> str:
        reln = _to_rel_posix(rel)
        if _is_excluded_path(reln):
            return reln
        if reln in added:
            return reln
        src = root / Path(reln)
        if not src.exists():
            raise FileNotFoundError(f"missing file for snapshot: {reln}")
        if src.is_dir():
            raise IsADirectoryError(f"add_rel expects a file, got directory: {reln}")
        z.write(src, arcname=reln)
        added.add(reln)
        included_files.append(reln)
        return reln

    def add_tree(z: zipfile.ZipFile, rel_dir: str | Path) -> None:
        reln = _to_rel_posix(rel_dir)
        if _is_excluded_path(reln):
            return
        base = root / Path(reln)
        if not base.exists():
            return
        if not base.is_dir():
            add_rel(z, reln)
            return
        # Deterministic ordering
        for p in sorted(base.rglob("*")):
            if p.is_dir():
                continue
            relp = p.relative_to(root).as_posix()
            if _is_excluded_path(relp):
                continue
            if relp in added:
                continue
            z.write(p, arcname=relp)
            added.add(relp)
            included_files.append(relp)

    # Resolve config path relative to root if needed
    cfg_src = config_path if config_path.is_absolute() else (root / config_path)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        # Always include code/config/docs roots
        for d in ALWAYS_INCLUDE_DIRS:
            add_tree(z, d)

        # Optional extras (docs/templates/etc.)
        if include_extra:
            for rel in include_extra:
                add_rel(z, rel)

        # Include provenance + artifacts for each corpus listed in corpora.json
        prov_dir = root / "docs" / "provenance"

        for c in corpora:
            if not isinstance(c, dict):
                continue
            cid = c.get("corpus_id")
            if not cid:
                continue

            # Canon (optional)
            canon = (c.get("canon_jsonl") or {}).get("path")
            if include_canon and canon:
                add_rel(z, canon)

            # BM25
            bm25 = (c.get("bm25") or {}).get("path")
            if bm25:
                add_rel(z, bm25)

            # FAISS bundle
            faiss = c.get("faiss") or {}
            for k in ("index_path", "ids_path", "meta_path"):
                pth = faiss.get(k)
                if pth:
                    add_rel(z, pth)

            # provenance JSON (best effort)
            if prov_dir.exists():
                prov_file = prov_dir / f"{cid}.json"
                if prov_file.exists():
                    arc = f"docs/provenance/{cid}.json"
                    if arc not in added:
                        z.write(prov_file, arcname=arc)
                        added.add(arc)
                        included_files.append(arc)

            included_corpora.append({"corpus_id": cid})

        # Include the config used
        if cfg_src.exists():
            if "config_used.toml" not in added:
                z.write(cfg_src, arcname="config_used.toml")
                added.add("config_used.toml")
                included_files.append("config_used.toml")

        manifest = {
            "schema": "scriptorium.snapshot.v2",
            "generated_utc": _utc_now(),
            "window": window,
            "tag": tag,
            "include_canon": bool(include_canon),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "always_included_dirs": list(ALWAYS_INCLUDE_DIRS),
            "excluded_dirnames": sorted(EXCLUDE_DIRNAMES),
            "registry_path": "docs/corpora.json",
            "registry_generated_utc": reg.get("generated_utc"),
            "registry_warnings": (warnings or [])[:50],
            "corpora": [x.get("corpus_id") for x in included_corpora if x.get("corpus_id")],
            "included_files": sorted(set(included_files)),
        }
        z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    return zip_path
