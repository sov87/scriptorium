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


def build_snapshot_bundle(
    *,
    project_root: Path,
    window: str,
    tag: str,
    config_path: Path,
    include_canon: bool = True,
    registry_path: str = "docs/corpora.json",
) -> Path:
    """
    Creates a zip snapshot under releases/ that includes:
      - docs/corpora.json
      - docs/provenance/*.json for included corpora (if present)
      - indexes (BM25 + FAISS bundle) for included corpora
      - optionally canon JSONL for included corpora
      - the TOML config used for the release command
      - a manifest.json with hashes already validated by docs/corpora.json

    Uses docs/provenance to (re)generate docs/corpora.json each time, then validates.
    """
    root = project_root

    # (Re)generate registry from provenance to keep it authoritative.
    reg_path, warnings = generate_registry(root, out_path=registry_path)
    v = validate_registry(root, registry_path=registry_path)
    if not v.get("ok"):
        # include diagnostics
        bad = [r for r in v.get("results", []) if not r.get("ok")]
        raise RuntimeError(f"corpora registry validation failed for {len(bad)} corpora; fix missing/mismatched artifacts before snapshot")

    reg = json.loads(reg_path.read_text(encoding="utf-8"))
    corpora = reg.get("corpora", []) or []

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = root / "releases"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"snapshot_{window}_{tag}_{stamp}.zip"

    # Decide included corpora files
    def add_rel(z: zipfile.ZipFile, rel: str) -> None:
        reln = rel.replace("\\", "/")
        src = root / Path(rel)
        if not src.exists():
            raise FileNotFoundError(f"missing file for snapshot: {rel}")
        z.write(src, arcname=reln)

    included_files: list[str] = []
    included_corpora: list[dict[str, Any]] = []

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        # Always include registry
        add_rel(z, registry_path)
        included_files.append(registry_path)

        # Include provenance files for corpora (if present)
        prov_dir = root / "docs" / "provenance"

        for c in corpora:
            cid = c.get("corpus_id")
            if not cid:
                continue

            # Canon (optional)
            canon = (c.get("canon_jsonl") or {}).get("path")
            if include_canon and canon:
                add_rel(z, canon)
                included_files.append(canon)

            # BM25
            bm25 = (c.get("bm25") or {}).get("path")
            if bm25:
                add_rel(z, bm25)
                included_files.append(bm25)

            # FAISS bundle
            faiss = c.get("faiss") or {}
            for k in ("index_path", "ids_path", "meta_path"):
                p = faiss.get(k)
                if p:
                    add_rel(z, p)
                    included_files.append(p)

            # provenance JSON (best effort: name match)
            if prov_dir.exists():
                prov_file = prov_dir / f"{cid}.json"
                if prov_file.exists():
                    z.write(prov_file, arcname=f"docs/provenance/{cid}.json")
                    included_files.append(f"docs/provenance/{cid}.json")

            included_corpora.append({"corpus_id": cid})

        # Include the config used
        if config_path.exists():
            z.write(config_path, arcname="config_used.toml")
            included_files.append("config_used.toml")

        manifest = {
            "schema": "scriptorium.snapshot.v1",
            "generated_utc": _utc_now(),
            "window": window,
            "tag": tag,
            "include_canon": bool(include_canon),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "registry_path": registry_path,
            "registry_generated_utc": reg.get("generated_utc"),
            "registry_warnings": warnings[:50],
            "corpora": [c.get("corpus_id") for c in corpora if c.get("corpus_id")],
            "included_files": sorted(set(included_files)),
        }
        z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    return zip_path
