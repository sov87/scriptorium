# File: tests/test_tei_normalization.py
from __future__ import annotations

from pathlib import Path

from scriptorium.ingest.tei_cts import (
    parse_tei,
    parse_work_id,
    iter_segment_drafts,
    sanitize_local_id_from_loc,
)

FIXTURE = Path(__file__).resolve().parent / "tei" / "mini_tei.xml"


def test_parse_work_id_cts_urn():
    tree = parse_tei(str(FIXTURE))
    wid = parse_work_id(tree)
    assert wid == "urn:cts:latinLit:phi0959.phi006.perseus-lat2"


def test_choice_unclear_supplied_gap_normalization_and_meta():
    tree = parse_tei(str(FIXTURE))
    segs = list(iter_segment_drafts(tree, use_milestones=False))
    assert len(segs) == 1

    s = segs[0]
    # loc should be div@n + p@n -> "1.1" with current default logic
    assert s.loc == "1.1"

    # <choice>: prefer <reg> ("honor") over <orig> ("honos")
    assert "honor" in s.text
    assert "honos" not in s.text

    # <unclear> and <supplied> bracketed; <gap> emits "[...]"
    assert "[abc]" in s.text
    assert "[def]" in s.text
    assert "[...]" in s.text

    tei = s.meta.get("tei", {})
    choices = tei.get("choices", [])
    assert choices and choices[0]["preferred"] == "honor"
    assert choices[0]["alternate"] == "honos"

    assert tei.get("unclear_spans"), "unclear_spans missing"
    assert tei.get("supplied_spans"), "supplied_spans missing"
    assert tei.get("gaps"), "gaps missing"


def test_local_id_sanitize_no_colon():
    loc = "1.2:3"
    local_id = sanitize_local_id_from_loc(loc)
    assert ":" not in local_id
    assert local_id  # non-empty


def test_determinism_repeat_parse_same_output():
    tree1 = parse_tei(str(FIXTURE))
    tree2 = parse_tei(str(FIXTURE))
    segs1 = list(iter_segment_drafts(tree1, use_milestones=False))
    segs2 = list(iter_segment_drafts(tree2, use_milestones=False))
    assert segs1 == segs2