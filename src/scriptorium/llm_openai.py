from __future__ import annotations

import json
import urllib.request
import urllib.error


def chat_completions(
    *,
    base_url: str,
    model: str,
    messages: list[dict],
    temperature: float,
    max_output_tokens: int,
    timeout_seconds: int,
) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_output_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"LLM HTTPError {e.code}: {err}") from e
    except Exception as e:
        raise RuntimeError(f"LLM request failed: {e}") from e

    j = json.loads(body)
    return j["choices"][0]["message"]["content"]