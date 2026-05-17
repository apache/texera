"""Minimal LiteLLM OpenAI-compatible client for genesis classification."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

LITELLM_URL = os.getenv("LITELLM_URL", "http://localhost:9096").rstrip("/")
LITELLM_KEY = os.getenv("LITELLM_KEY", "")
MODEL = os.getenv("LITELLM_MODEL", "claude-haiku-4.5")


def parse_llm_json(raw: str | None) -> dict[str, Any]:
    """Extract and parse a JSON object from LLM text (handles markdown fences & preamble)."""
    if raw is None:
        raise ValueError("LLM content is None")
    s = raw.strip() if isinstance(raw, str) else str(raw).strip()
    if not s:
        raise ValueError("LLM content is empty after strip")

    m = re.search(r"```(?:json)?\s*\n?(.*?)```", s, re.DOTALL | re.IGNORECASE)
    if m:
        s = m.group(1).strip()

    if not s.startswith("{"):
        first = s.find("{")
        last = s.rfind("}")
        if first >= 0 and last > first:
            s = s[first : last + 1]

    return json.loads(s)


def _message_content_to_str(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                parts.append(str(part.get("text", part.get("content", ""))))
            else:
                parts.append(str(part))
        return "".join(parts)
    return str(content)


def chat_completion(
    system: str,
    user: str,
    *,
    json_mode: bool = True,
    timeout: float = 15.0,
) -> dict:
    """Call LiteLLM /chat/completions; return parsed JSON dict or ``{"content": str}``.

    Raises on HTTP errors or invalid JSON when ``json_mode`` is True.
    """
    payload: dict = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": 1500,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    headers = {
        "Content-Type": "application/json",
    }
    if LITELLM_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_KEY}"

    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            f"{LITELLM_URL}/chat/completions",
            json=payload,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()
        content = _message_content_to_str(data["choices"][0]["message"].get("content"))

    if json_mode:
        try:
            parsed = parse_llm_json(content)
            if not isinstance(parsed, dict):
                raise ValueError(f"LLM JSON root must be an object, got {type(parsed).__name__}")
            return parsed
        except (json.JSONDecodeError, ValueError) as e:
            preview = content[:8000] if content else "(empty)"
            logger.error(
                "LLM JSON parse failed: %s. Raw message content (truncated to 8000 chars): %s",
                e,
                preview,
            )
            raise
    return {"content": content}
