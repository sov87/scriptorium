# File: src/scriptorium/validate_provenance.py
# Purpose: Fail-closed validation of RIGHTS + PROVENANCE completeness for executable corpora.
#
# This validator is intended for "strict-rights" gating:
# - It validates audit completeness (rights + provenance artifacts), not sha256 integrity.
# - It is parameterized by registry_path so sample/CI builds can validate docs/corpora.public.json
#   while production builds validate docs/corpora.json (or a config-specified registry).
#
# Rules (fail-closed):
# - "Executable corpus" means: corpus.canon_jsonl.path is present in the selected registry.
# - Every executable corpus must have docs/provenance/<corpus_id>.json that parses as a JSON object.
# - If rights.distributable == true (in registry):
#     - rights.tier != "UNSET"
#     - rights.license not empty and not "UNVERIFIED"
#     - provenance.sources is a non-empty list with identifying fields
#     - provenance.processing is a non-empty list (processing trail)
# - If rights.distributable == false (local-only):
#     - rights.license must be non-empty (use "UNVERIFIED" if unknown)
#     - an explicit "not for redistribution" note must be present in either:
#         - registry rights.notes, or
#         - provenance.notes, or
#         - provenance.rights.notes

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOCAL_ONLY_REQUIRED_PHRASES: Tuple[str, ...] = (
    "not for redistribution",
    "local research copy",
    "local-only",
    "local only",
)

IDENT_FIELDS: Tuple[str, ...] = ("url", "repo", "citation", "id", "path", "urn", "ref")


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def _is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _get_bool(d: Dict[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
    return default


def _get_str(d: Dict[str, Any], key: str, default: str = "") -> str:
    v = d.get(key, default)
    return v if isinstance(v, str) else default


def _canon_is_executable(c: Dict[str, Any]) -> bool:
    canon = c.get("canon_jsonl")
    return isinstance(canon, dict) and _is_nonempty_str(canon.get("path"))


def _note_satisfies_local_only(note: str) -> bool:
    n = (note or "").strip().lower()
    if not n:
        return False
    return any(p in n for p in LOCAL_ONLY_REQUIRED_PHRASES)


def _extract_note(reg_rights: Dict[str, Any], prov: Optional[Dict[str, Any]]) -> str:
    if _is_nonempty_str(reg_rights.get("notes")):
        return str(reg_rights["notes"]).strip()

    if isinstance(prov, dict):
        if _is_nonempty_str(prov.get("notes")):
            return str(prov["notes"]).strip()
        pr = prov.get("rights")
        if isinstance(pr, dict) and _is_nonempty_str(pr.get("notes")):
            return str(pr["notes"]).strip()

    return ""


def _validate_rights_registry_basics(cid: str, rights: Dict[str, Any], issues: List[str]) -> None:
    tier = _get_str(rights, "tier", "UNSET").strip()
    lic = _get_str(rights, "license", "").strip()
    if tier == "UNSET":
        issues.append(f"{cid}: rights.tier is UNSET")
    if lic == "":
        issues.append(f"{cid}: rights.license is empty")


def _validate_distributable_rights(cid: str, rights: Dict[str, Any], issues: List[str]) -> None:
    tier = _get_str(rights, "tier", "UNSET").strip()
    lic = _get_str(rights, "license", "").strip()
    if tier == "UNSET":
        issues.append(f"{cid}: distributable=true but rights.tier is UNSET")
    if lic in ("", "UNVERIFIED"):
        issues.append(f"{cid}: distributable=true but rights.license is empty/UNVERIFIED")


def _validate_local_only_rights(cid: str, rights: Dict[str, Any], note: str, issues: List[str]) -> None:
    lic = _get_str(rights, "license", "").strip()
    if lic == "":
        issues.append(f"{cid}: local-only but rights.license is empty (use UNVERIFIED if unknown)")
    if not _note_satisfies_local_only(note):
        issues.append(
            f"{cid}: local-only requires explicit note containing one of: "
            + ", ".join(repr(x) for x in LOCAL_ONLY_REQUIRED_PHRASES)
        )


def _validate_prov_sources(cid: str, prov: Dict[str, Any], issues: List[str]) -> None:
    sources = prov.get("sources")
    if not isinstance(sources, list) or not sources:
        issues.append(f"{cid}: provenance.sources missing/empty (required for distributable corpora)")
        return
    ok_any = False
    for i, s in enumerate(sources):
        if not isinstance(s, dict):
            issues.append(f"{cid}: provenance.sources[{i}] is not an object")
            continue
        if any(_is_nonempty_str(s.get(k)) for k in IDENT_FIELDS):
            ok_any = True
        else:
            issues.append(f"{cid}: provenance.sources[{i}] lacks identifying field ({', '.join(IDENT_FIELDS)})")
    if not ok_any:
        issues.append(f"{cid}: provenance.sources has no usable identifying source entries")


def _validate_prov_processing(cid: str, prov: Dict[str, Any], issues: List[str]) -> None:
    proc = prov.get("processing")
    if not isinstance(proc, list) or not proc:
        issues.append(f"{cid}: provenance.processing missing/empty (required for distributable corpora)")
        return
    for i, step in enumerate(proc):
        if not isinstance(step, dict):
            issues.append(f"{cid}: provenance.processing[{i}] is not an object")
            continue
        if not any(_is_nonempty_str(step.get(k)) for k in ("step", "name", "action")):
            issues.append(f"{cid}: provenance.processing[{i}] missing step identifier (step/name/action)")


def validate_all_corpora(
    root: Path,
    *,
    registry_path: Optional[Path] = None,
    provenance_dir: Optional[Path] = None,
) -> None:
    root = root.resolve()
    reg_path = registry_path.resolve() if isinstance(registry_path, Path) else (root / "docs" / "corpora.json")
    if not reg_path.exists():
        raise FileNotFoundError(f"Missing registry: {reg_path}")

    prov_dir = provenance_dir.resolve() if isinstance(provenance_dir, Path) else (root / "docs" / "provenance")

    reg = _load_json(reg_path)
    if not isinstance(reg, dict):
        raise ValueError(f"Invalid registry JSON shape: expected object: {reg_path}")

    corpora = reg.get("corpora", [])
    if not isinstance(corpora, list):
        raise ValueError(f"Invalid registry: corpora must be a list: {reg_path}")

    issues: List[str] = []
    executable = 0

    for c in corpora:
        if not isinstance(c, dict):
            continue
        cid = c.get("corpus_id")
        if not _is_nonempty_str(cid):
            continue
        cid = str(cid).strip()

        if not _canon_is_executable(c):
            continue
        executable += 1

        rights_any = c.get("rights")
        if not isinstance(rights_any, dict):
            issues.append(f"{cid}: missing rights object in registry {reg_path.as_posix()}")
            continue
        rights = rights_any

        distributable = _get_bool(rights, "distributable", False)

        # Always require provenance file for executable corpora.
        prov_path = prov_dir / f"{cid}.json"
        prov: Optional[Dict[str, Any]] = None
        if not prov_path.exists():
            issues.append(f"{cid}: missing provenance file {prov_path.as_posix()}")
        else:
            try:
                prov_any = _load_json(prov_path)
                if not isinstance(prov_any, dict):
                    issues.append(f"{cid}: provenance is not a JSON object: {prov_path.as_posix()}")
                else:
                    prov = prov_any
                    prov_cid = prov.get("corpus_id")
                    if _is_nonempty_str(prov_cid) and str(prov_cid).strip() != cid:
                        issues.append(f"{cid}: provenance corpus_id mismatch (found {prov_cid!r})")
            except Exception as e:
                issues.append(f"{cid}: invalid provenance JSON {prov_path.as_posix()} ({type(e).__name__}: {e})")

        # Registry rights: basic presence
        _validate_rights_registry_basics(cid, rights, issues)

        # If provenance declares distributable, it must not contradict registry.
        if prov is not None:
            pr = prov.get("rights")
            if isinstance(pr, dict) and "distributable" in pr:
                prov_distrib = _get_bool(pr, "distributable", distributable)
                if prov_distrib != distributable:
                    issues.append(f"{cid}: distributable mismatch (registry={distributable}, provenance={prov_distrib})")

        if distributable:
            _validate_distributable_rights(cid, rights, issues)
            if prov is not None:
                _validate_prov_sources(cid, prov, issues)
                _validate_prov_processing(cid, prov, issues)
        else:
            note = _extract_note(rights, prov)
            _validate_local_only_rights(cid, rights, note, issues)

    if issues:
        header = f"Provenance & rights gaps found (strict-rights) in registry={reg_path.as_posix()} (executable={executable}):"
        raise ValueError(header + "\n" + "\n".join(f"  - {x}" for x in issues))


def main(argv: List[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path(".")
    validate_all_corpora(root)
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv))
