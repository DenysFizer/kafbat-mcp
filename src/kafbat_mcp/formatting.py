"""Shaping kafbat's JSON for a model's context: drop nulls, keep useful fields, truncate blobs."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

MESSAGE_FIELDS = ("partition", "offset", "timestamp", "keySerde", "valueSerde", "headers")


def path_seg(value: str) -> str:
    return quote(value, safe="")


def prune(obj: Any) -> Any:
    """kafbat returns many null metrics; dropping them saves context."""
    if isinstance(obj, dict):
        return {k: prune(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [prune(v) for v in obj]
    return obj


def to_json(obj: Any) -> str:
    return json.dumps(prune(obj), ensure_ascii=False, separators=(",", ":"))


def pick(d: dict, *keys: str) -> dict:
    return {k: d[k] for k in keys if d.get(k) is not None}


def truncate(value: str | None, limit: int) -> str | None:
    if value is None or len(value) <= limit:
        return value
    return f"{value[:limit]}…[truncated {len(value) - limit} chars]"


def parse_message_events(body: str, max_value_chars: int) -> dict:
    """Parses the text/event-stream body of kafbat's /messages/v2 endpoint."""
    messages, consuming, cursor = [], None, None
    for line in body.splitlines():
        if not line.startswith("data:"):
            continue
        event = json.loads(line[5:])
        kind = event.get("type")
        if kind == "MESSAGE" and event.get("message"):
            m = event["message"]
            messages.append(
                pick(m, *MESSAGE_FIELDS)
                | {"key": truncate(m.get("key"), max_value_chars), "value": truncate(m.get("value"), max_value_chars)}
            )
        elif kind == "CONSUMING":
            consuming = event.get("consuming")
        elif kind == "DONE":
            cursor = (event.get("cursor") or {}).get("id")
    return {"messages": messages, "stats": consuming, "nextCursor": cursor}
