from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Sequence


def _run(cmd: Sequence[str]) -> int:
    # Stream output to console; fail hard on non-zero.
    p = subprocess.run(list(cmd), check=False)
    if p.returncode != 0:
        raise SystemExit(p.returncode)
    return p.returncode


def run_release_window(
    *,
    ps1_path: Path,
    window: str,
    tag: str,
    make_subset: bool,
    rebuild_indexes: bool,
    run_retrieval: bool,
    run_machine: bool,
    snapshot: bool,
) -> int:
    if not ps1_path.exists():
        raise FileNotFoundError(f"release_window.ps1 not found: {ps1_path}")

    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ps1_path),
        "-Window",
        window,
        "-Tag",
        tag,
    ]

    # These switches match the names you described in your handoff.
    # If your PS script uses slightly different switch names, change them here ONCE.
    if make_subset:
        cmd.append("-MakeSubset")
    if rebuild_indexes:
        cmd.append("-RebuildIndexes")
    if run_retrieval:
        cmd.append("-RunRetrieval")
    if run_machine:
        cmd.append("-RunMachine")
    if snapshot:
        cmd.append("-SnapshotRelease")

    return _run(cmd)


def format_release_window_cmd(
    *,
    ps1_path: Path,
    window: str,
    tag: str,
    make_subset: bool,
    rebuild_indexes: bool,
    run_retrieval: bool,
    run_machine: bool,
    snapshot: bool,
) -> str:
    parts = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        f'"{ps1_path}"',
        "-Window",
        window,
        "-Tag",
        tag,
    ]
    if make_subset:
        parts.append("-MakeSubset")
    if rebuild_indexes:
        parts.append("-RebuildIndexes")
    if run_retrieval:
        parts.append("-RunRetrieval")
    if run_machine:
        parts.append("-RunMachine")
    if snapshot:
        parts.append("-SnapshotRelease")
    return " ".join(parts)