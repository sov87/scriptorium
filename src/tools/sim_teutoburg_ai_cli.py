#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from typing import List, Optional

import requests


@dataclass
class Hit:
    seg_id: str
    corpus_id: str
    loc: str
    text: str


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def build_fts_query(q: str) -> str:
    toks = [t.strip() for t in q.replace('"', " ").split() if t.strip()]
    return " AND ".join(toks)


def query_hits(con: sqlite3.Connection, q: str, topk: int, corpora: Optional[List[str]]) -> List[Hit]:
    q = q.strip()
    if not q:
        return []
    cur = con.cursor()

    # Prefer FTS if available
    if table_exists(con, "segments_fts"):
        fts_q = build_fts_query(q)
        try:
            if corpora:
                ph = ",".join("?" for _ in corpora)
                sql = f"""
                SELECT s.id, s.corpus_id, s.loc, s.text
                FROM segments s
                JOIN segments_fts f ON f.rowid = s.rowid
                WHERE f.segments_fts MATCH ?
                  AND s.corpus_id IN ({ph})
                LIMIT ?
                """
                params = [fts_q] + corpora + [topk]
            else:
                sql = """
                SELECT s.id, s.corpus_id, s.loc, s.text
                FROM segments s
                JOIN segments_fts f ON f.rowid = s.rowid
                WHERE f.segments_fts MATCH ?
                LIMIT ?
                """
                params = [fts_q, topk]
            cur.execute(sql, params)
            rows = cur.fetchall()
            hits = [Hit(seg_id=r[0], corpus_id=r[1] or "", loc=r[2] or "", text=r[3] or "") for r in rows]
            if hits:
                return hits
        except sqlite3.Error:
            pass

    # Fallback: LIKE
    like = f"%{q}%"
    if corpora:
        ph = ",".join("?" for _ in corpora)
        sql = f"SELECT id, corpus_id, loc, text FROM segments WHERE text LIKE ? AND corpus_id IN ({ph}) LIMIT ?"
        params = [like] + corpora + [topk]
    else:
        sql = "SELECT id, corpus_id, loc, text FROM segments WHERE text LIKE ? LIMIT ?"
        params = [like, topk]
    cur.execute(sql, params)
    rows = cur.fetchall()
    return [Hit(seg_id=r[0], corpus_id=r[1] or "", loc=r[2] or "", text=r[3] or "") for r in rows]


def select_segment(con: sqlite3.Connection, seg_id: str) -> Optional[Hit]:
    cur = con.cursor()
    cur.execute("SELECT id, corpus_id, loc, text FROM segments WHERE id=?", (seg_id,))
    r = cur.fetchone()
    if not r:
        return None
    return Hit(seg_id=r[0], corpus_id=r[1] or "", loc=r[2] or "", text=r[3] or "")


def clip(s: str, n: int = 260) -> str:
    s = " ".join((s or "").split())
    return s if len(s) <= n else s[: n - 1] + "…"


SYSTEM_PROMPT = """\
You are generating a historically grounded "tabletop simulation" turn.
Hard rules:
- Use ONLY the provided evidence passages as factual basis.
- Every factual assertion must cite one or more segment_id values.
- If something is an inference, label it inference and cite what you inferred from.
- Do not invent named facts not present in the evidence.

Output MUST be valid JSON:
{
  "narration": "2-6 paragraphs",
  "claims": [
    {"text":"...", "epistemic":"fact|inference|source_claim", "evidence":["segment_id", "..."]}
  ]
}
Citations must be the exact segment_id strings provided.
"""


def call_llm(base_url: str, model: str, system: str, user: str, temperature: float, max_tokens: int) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    r = requests.post(url, json=payload, timeout=180)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]


def build_evidence_packet(hits: List[Hit]) -> str:
    parts = []
    for h in hits:
        parts.append(f"[{h.seg_id}] corpus={h.corpus_id} loc={h.loc}\n{h.text.strip()}")
    return "\n\n---\n\n".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--base-url", default="http://localhost:1234/v1")
    ap.add_argument("--model", required=True)
    ap.add_argument("--corpus", action="append", default=[])
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--temperature", type=float, default=0.2)
    ap.add_argument("--max-tokens", type=int, default=900)
    args = ap.parse_args()

    corpora = args.corpus if args.corpus else None
    con = sqlite3.connect(args.db)

    print("SCRIPTORIUM — Teutoburg AI Demo CLI")
    print(f"DB: {args.db}")
    print(f"LLM: {args.base_url} model={args.model}")
    if corpora:
        print("Corpora:", ", ".join(corpora))
    print("Commands: look | ask <q> | turn <q> | source <segment_id> | quit\n")

    while True:
        try:
            line = input("sim> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("")
            break

        if not line:
            continue
        if line in ("quit", "exit", "q"):
            break

        if line == "look":
            hits = query_hits(con, "Engpass Oberesch Wall Moor Wiehen Varus Arminius Legion", args.topk, corpora)
            if not hits:
                print("(no hits)")
            else:
                for i, h in enumerate(hits, 1):
                    print(f"{i:>2}. {h.seg_id} [{h.corpus_id}] {clip(h.text)}")
            continue

        if line.startswith("ask "):
            q = line.split(None, 1)[1]
            hits = query_hits(con, q, args.topk, corpora)
            if not hits:
                print("(no hits)")
            else:
                for i, h in enumerate(hits, 1):
                    print(f"{i:>2}. {h.seg_id} [{h.corpus_id}] {clip(h.text)}")
            continue

        if line.startswith("source "):
            seg_id = line.split(None, 1)[1].strip()
            h = select_segment(con, seg_id)
            if not h:
                print("(not found)")
            else:
                print(f"\n{h.seg_id}\ncorpus={h.corpus_id}\nloc={h.loc}\n")
                print(h.text.strip())
                print("")
            continue

        if line.startswith("turn "):
            q = line.split(None, 1)[1].strip()
            hits = query_hits(con, q, args.topk, corpora)
            if not hits:
                print("(no evidence hits; try a different query)")
                continue

            evidence = build_evidence_packet(hits)
            user_prompt = f"QUERY:\n{q}\n\nEVIDENCE PASSAGES (cite by segment_id):\n{evidence}\n"
            out = call_llm(args.base_url, args.model, SYSTEM_PROMPT, user_prompt, args.temperature, args.max_tokens)
            print(out)
            continue

        print("Unknown command. Try: look | ask <q> | turn <q> | source <segment_id> | quit")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())