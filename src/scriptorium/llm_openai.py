from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List


def _headers(api_key: str | None) -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _post_json(*, url: str, payload: dict, api_key: str | None, timeout_seconds: int) -> str:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers=_headers(api_key),
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


def _get_json(*, url: str, api_key: str | None, timeout_seconds: int) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"LLM HTTPError {e.code}: {err}") from e
    except Exception as e:
        raise RuntimeError(f"LLM request failed: {e}") from e

    try:
        return json.loads(body)
    except Exception as e:
        raise RuntimeError(f"LLM returned non-JSON body: {type(e).__name__}: {e}\nBODY:\n{body}") from e


def pick_first_model_id(*, base_url: str, api_key: str | None = None, timeout_seconds: int = 15) -> str | None:
    try:
        j = _get_json(url=base_url.rstrip("/") + "/models", api_key=api_key, timeout_seconds=timeout_seconds)
        data = j.get("data") or []
        if isinstance(data, list) and data:
            mid = (data[0] or {}).get("id")
            if isinstance(mid, str) and mid:
                return mid
    except Exception:
        return None
    return None


def chat_completions_raw(
    *,
    base_url: str,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float,
    max_output_tokens: int,
    api_key: str | None = None,
    timeout_seconds: int,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_output_tokens,
    }
    body = _post_json(url=url, payload=payload, api_key=api_key, timeout_seconds=timeout_seconds)
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
    api_key: str | None = None,
    timeout_seconds: int,
) -> str:
    return chat_completions_raw(
        base_url=base_url,
        model=model,
        messages=messages,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
    )["content"]
