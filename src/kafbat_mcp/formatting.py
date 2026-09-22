"""Shaping kafbat's JSON for a model's context: drop nulls, keep useful fields, truncate blobs."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

MESSAGE_FIELDS = ("partition", "offset", "timestamp", "keySerde", "valueSerde", "headers")

# Connect configs are a free-form map with no sensitivity flag, unlike topic/broker configs, so redact by key name.
SECRET_KEY_HINTS = ("password", "passwd", "secret", "token", "credential")


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


def as_text(value: Any) -> Any:
    """Kafka keys and values are text on the wire.

    The MCP SDK JSON-parses any string argument that looks like JSON, so a JSON payload reaches the tool as a
    dict or list. Re-serialise it — and unlike to_json, keep nulls: dropping them would alter the message.
    """
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def pick(d: dict, *keys: str) -> dict:
    return {k: d[k] for k in keys if d.get(k) is not None}


def non_default(configs: list[dict]) -> dict[str, Any]:
    """Config entries that differ from the cluster default, minus the ones kafbat flags as sensitive."""
    return {
        c["name"]: c.get("value")
        for c in configs
        if c.get("source") != "DEFAULT_CONFIG" and not c.get("isSensitive")
    }


def mask_secrets(config: Any) -> Any:
    if not isinstance(config, dict):
        return config
    return {k: ("***" if any(h in k.lower() for h in SECRET_KEY_HINTS) else v) for k, v in config.items()}


def trim_trace(status: Any, limit: int) -> None:
    """Connect failure traces are whole Java stacktraces; the top frames carry the cause."""
    if isinstance(status, dict) and status.get("trace"):
        status["trace"] = truncate(status["trace"], limit)


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
