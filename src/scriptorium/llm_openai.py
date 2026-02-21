from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List


def _post_json(*, url: str, payload: dict, timeout_seconds: int) -> str:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"LLM HTTPError {e.code}: {err}") from e
    except Exception as e:
        raise RuntimeError(f"LLM request failed: {e}") from e


def chat_completions_raw(
    *,
    base_url: str,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float,
    max_output_tokens: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    """
    Returns a structured record you can write to disk:
      {
        "url": str,
        "request": {...},
        "response": {...},   # parsed JSON response body
        "content": str       # choices[0].message.content
      }
    """
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_output_tokens,
    }
    body = _post_json(url=url, payload=payload, timeout_seconds=timeout_seconds)
    try:
        j = json.loads(body)
    except Exception as e:
        raise RuntimeError(f"LLM returned non-JSON body: {type(e).__name__}: {e}\nBODY:\n{body}") from e

    try:
        content = j["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(f"LLM response JSON missing expected fields: {type(e).__name__}: {e}\nJSON:\n{j}") from e

    return {"url": url, "request": payload, "response": j, "content": str(content)}


def chat_completions(
    *,
    base_url: str,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float,
    max_output_tokens: int,
    timeout_seconds: int,
) -> str:
    # Backward-compatible wrapper: returns only the content string.
    return chat_completions_raw(
        base_url=base_url,
        model=model,
        messages=messages,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        timeout_seconds=timeout_seconds,
    )["content"]