"""Tests for robust LLM JSON extraction."""

import json

import pytest

from core.llm_client import parse_llm_json


def test_parse_llm_json_plain_object():
    raw = '{"a": 1, "b": "x"}'
    assert parse_llm_json(raw) == {"a": 1, "b": "x"}


def test_parse_llm_json_markdown_fence():
    raw = """Here you go:

```json
{"scenario_label": "x", "suggestions": []}
```
"""
    out = parse_llm_json(raw)
    assert out == {"scenario_label": "x", "suggestions": []}

    raw2 = """```json
{"ok": true, "n": 1}
```"""
    assert parse_llm_json(raw2) == {"ok": True, "n": 1}


def test_parse_llm_json_preamble_and_suffix():
    payload = {
        "scenario_label": "demo",
        "dataset_summary": "s",
        "target_column": None,
        "confidence": 0.5,
        "suggestions": [{"id": "a", "goal_for_agent": "g"}],
    }
    inner = json.dumps(payload)
    raw = f"Certainly! Here's the JSON you asked for.\n{inner}\nHope this helps."
    out = parse_llm_json(raw)
    assert out["scenario_label"] == "demo"
    assert len(out["suggestions"]) == 1


def test_parse_llm_json_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        parse_llm_json("   ")
    with pytest.raises(ValueError, match="None"):
        parse_llm_json(None)  # type: ignore[arg-type]
