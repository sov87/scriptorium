from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import tomllib


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def ensure_segments_fts(db_path: Path) -> None:
    import sqlite3

    con = sqlite3.connect(str(db_path))
    try:
        # Always rebuild for determinism and to avoid "missing column" loops.
        if con.execute(
            "select 1 from sqlite_master where type='table' and name='segments_fts'"
        ).fetchone():
            con.execute("DROP TABLE segments_fts")
            con.commit()

        # Ensure FTS5 is available
        try:
            con.execute("CREATE VIRTUAL TABLE __fts_probe USING fts5(x)")
            con.execute("DROP TABLE __fts_probe")
        except sqlite3.OperationalError as e:
            raise SystemExit(f"FTS5 not available in this SQLite build: {e}")

        seg_cols = [r[1] for r in con.execute("PRAGMA table_info(segments)").fetchall()]
        if not seg_cols:
            raise SystemExit("segments table not found or has no columns")

        seg_set = set(seg_cols)

        def pick(candidates: list[str]) -> str:
            for c in candidates:
                if c in seg_set:
                    return c
            raise SystemExit(f"segments table missing expected columns; have: {seg_cols}")

        # Identify core columns in segments
        corpus_src = pick(["corpus_id", "corpus"])
        id_src = pick(["id", "seg_id", "segment_id"])
        text_src = pick(["text", "content", "segment_text", "norm_text"])

        # Columns db-search appears to want even if segments doesn't have them
        required_meta = ["corpus_id", "work_id", "loc", "id"]

        # Build a meta column list: required first, then everything else from segments except text
        meta_cols: list[str] = []
        seen = set()

        def add(col: str) -> None:
            if col not in seen:
                meta_cols.append(col)
                seen.add(col)

        for c in required_meta:
            add(c)
        for c in seg_cols:
            if c == text_src:
                continue
            add(c)

        # Map each meta col to a SELECT expression
        def expr_for(col: str) -> str:
            if col == "corpus_id":
                return corpus_src
            if col == "id":
                return id_src
            if col in seg_set:
                return col
            return "''"  # ensure column exists even if segments lacks it

        # Create FTS table with all meta cols UNINDEXED + indexed text
        cols_ddl = ", ".join([f'"{c}" UNINDEXED' for c in meta_cols] + ['"text"'])
        con.execute(f'CREATE VIRTUAL TABLE segments_fts USING fts5({cols_ddl})')

        # Populate from segments
        select_exprs = ", ".join(
            [f'{expr_for(c)} as "{c}"' for c in meta_cols] + [f'{text_src} as "text"']
        )
        insert_cols = ", ".join([f'"{c}"' for c in meta_cols] + ['"text"'])
        con.execute(
            f'INSERT INTO segments_fts({insert_cols}) SELECT {select_exprs} FROM segments'
        )
        con.commit()
        print("[OK] rebuilt segments_fts (fts5) with full segments metadata + work_id/loc/id")
    finally:
        con.close()


def pick_default_corpus(db_path: Path) -> str:
    import sqlite3

    con = sqlite3.connect(str(db_path))
    try:
        rows = [
            r[0]
            for r in con.execute(
                "select distinct corpus_id from segments order by corpus_id"
            ).fetchall()
        ]
        if not rows:
            raise SystemExit("no corpora found in segments table")
        for preferred in ("oe_bede", "bede"):
            if preferred in rows:
                return preferred
        return rows[0]
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--query", default="king")
    ap.add_argument("--k", type=int, default=3)
    ap.add_argument("--corpus", default=None)
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--no-strict", action="store_true")
    args = ap.parse_args()

    cfg_path = Path(args.config)
    cfg = tomllib.loads(cfg_path.read_text(encoding="utf-8"))

    proj_root = (cfg_path.parent / cfg["root"]["project_root"]).resolve()
    if not proj_root.exists():
        raise SystemExit(f"project_root does not exist: {proj_root}")

    canon = cfg.get("canon", {})
    if not canon:
        raise SystemExit("config missing [canon] table")
    for name, rel in canon.items():
        p = (proj_root / rel).resolve()
        if not p.exists():
            raise SystemExit(f"missing canon file for {name}: {p}")

    doctor_cmd = [sys.executable, "-m", "scriptorium", "doctor", "--config", str(cfg_path), "--json"]
    if not args.no_strict:
        doctor_cmd.insert(doctor_cmd.index("--json"), "--strict")
    run(doctor_cmd)

    run([sys.executable, "-m", "scriptorium", "db-build", "--config", str(cfg_path), "--overwrite"])

    db_path = proj_root / "db" / "scriptorium.sqlite"
    if not db_path.exists():
        raise SystemExit(f"expected db not found: {db_path}")

    ensure_segments_fts(db_path)

    corpus = args.corpus or pick_default_corpus(db_path)
    run(
        [
            sys.executable,
            "-m",
            "scriptorium",
            "db-search",
            "--config",
            str(cfg_path),
            "--q",
            args.query,
            "--k",
            str(args.k),
            "--corpus",
            corpus,
        ]
    )

    if args.full:
        run([sys.executable, "-m", "scriptorium", "vec-build", "--config", str(cfg_path)])
        run(
            [
                sys.executable,
                "-m",
                "scriptorium",
                "retrieve",
                "--config",
                str(cfg_path),
                "--q",
                args.query,
                "--k",
                str(max(args.k, 5)),
                "--corpus",
                corpus,
            ]
        )

    print("[OK] smoke_demo passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())