import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

ROOT = Path(__file__).resolve().parents[1]
CORPORA_PATH = ROOT / "docs" / "corpora.json"

REQUIRED_RIGHTS_KEYS = {"tier", "license", "distributable"}

# ---------- helpers ----------

def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"[ERROR] Missing file: {path}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"[ERROR] Invalid JSON in {path}: {e}")

def is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""

def _looks_like_id_map(d: Dict[str, Any]) -> bool:
    # Heuristic: mostly string keys, values are dict-like corpus objects
    if not d:
        return False
    dict_values = sum(1 for v in d.values() if isinstance(v, dict))
    return dict_values >= max(1, int(0.8 * len(d)))

def normalize_corpora(payload: Any) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Returns (corpora_list, error_message).
    Supported:
      - list[dict]
      - {"corpora": list[dict]}
      - {"corpora": {id: dict, ...}}
      - {id: dict, ...}
    """
    if isinstance(payload, list):
        if all(isinstance(x, dict) for x in payload):
            return payload, None
        return None, "Top-level array must contain only objects."

    if isinstance(payload, dict):
        # Case: {"corpora": ...}
        if "corpora" in payload:
            c = payload["corpora"]
            if isinstance(c, list):
                if all(isinstance(x, dict) for x in c):
                    return c, None
                return None, "'corpora' array must contain only objects."
            if isinstance(c, dict) and _looks_like_id_map(c):
                out: List[Dict[str, Any]] = []
                for k, v in c.items():
                    if not isinstance(v, dict):
                        continue
                    vv = dict(v)
                    if not is_nonempty_str(vv.get("id")) and not is_nonempty_str(vv.get("corpus_id")):
                        vv["id"] = k
                    out.append(vv)
                return out, None
            return None, "'corpora' must be an array of objects or an id->object map."

        # Case: {id: dict, ...}
        if _looks_like_id_map(payload):
            out2: List[Dict[str, Any]] = []
            for k, v in payload.items():
                if not isinstance(v, dict):
                    continue
                vv = dict(v)
                if not is_nonempty_str(vv.get("id")) and not is_nonempty_str(vv.get("corpus_id")):
                    vv["id"] = k
                out2.append(vv)
            return out2, None

        return None, "Top-level object is not a recognized corpora container (expected key 'corpora' or an id->object map)."

    return None, f"Unsupported JSON type: {type(payload).__name__}"

# ---------- lint ----------

def lint_corpora(corpora: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    seen_ids = set()

    for i, c in enumerate(corpora):
        where = f"corpora[{i}]"

        cid = c.get("id") or c.get("corpus_id")
        if not is_nonempty_str(cid):
            errors.append(f"{where}: missing non-empty 'id' (or 'corpus_id')")
            continue

        if cid in seen_ids:
            errors.append(f"{where}: duplicate corpus id '{cid}'")
        seen_ids.add(cid)

        rights = c.get("rights")
        if not isinstance(rights, dict):
            errors.append(f"{where} ({cid}): missing rights object")
            continue

        missing = REQUIRED_RIGHTS_KEYS - set(rights.keys())
        if missing:
            errors.append(f"{where} ({cid}): rights missing keys: {sorted(missing)}")
            continue

        tier = rights.get("tier")
        lic = rights.get("license")
        dist = rights.get("distributable")

        if not is_nonempty_str(tier):
            errors.append(f"{where} ({cid}): rights.tier must be a non-empty string")
        if not is_nonempty_str(lic):
            errors.append(f"{where} ({cid}): rights.license must be a non-empty string")
        if not isinstance(dist, bool):
            errors.append(f"{where} ({cid}): rights.distributable must be boolean")

        # Policy rules
        if isinstance(dist, bool) and dist:
            if isinstance(tier, str) and tier.startswith("B_"):
                errors.append(f"{where} ({cid}): distributable=true but tier starts with 'B_'")
            if isinstance(lic, str) and lic.strip().upper() == "UNVERIFIED":
                errors.append(f"{where} ({cid}): distributable=true but license is UNVERIFIED")

        if isinstance(dist, bool) and not dist:
            if isinstance(tier, str) and tier.startswith("A_"):
                warnings.append(f"{where} ({cid}): tier is A_* but distributable=false (ok if intentional)")

        # Optional: canon path sanity
        canon_path = c.get("canon_path") or c.get("path")
        if isinstance(canon_path, str):
            if "data_raw" in canon_path.replace("\\", "/"):
                warnings.append(f"{where} ({cid}): canon_path points into data_raw (usually wrong): {canon_path}")

    return errors, warnings

def main() -> int:
    payload = load_json(CORPORA_PATH)
    corpora, norm_err = normalize_corpora(payload)
    if corpora is None:
        report = {
            "ok": False,
            "errors": [f"docs/corpora.json schema not recognized: {norm_err}"],
            "warnings": [],
            "counts": {"corpora": None, "errors": 1, "warnings": 0},
            "paths": {"corpora_json": str(CORPORA_PATH)},
        }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2

    errors, warnings = lint_corpora(corpora)

    report: Dict[str, Any] = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "counts": {"corpora": len(corpora), "errors": len(errors), "warnings": len(warnings)},
        "paths": {"corpora_json": str(CORPORA_PATH)},
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if len(errors) == 0 else 2

if __name__ == "__main__":
    sys.exit(main())