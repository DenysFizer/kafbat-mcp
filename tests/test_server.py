"""Offline tests: a fake kafbat behind httpx.MockTransport, the server driven by the in-process MCP client."""

import json

import httpx
import pytest
from mcp import Client

from kafbat_mcp import client as kafbat_client
from kafbat_mcp.client import Kafbat, cookie_matches_host, open_command
from kafbat_mcp.config import Config
from kafbat_mcp.server import build_server

URL = "https://kafbat.test"
VALID = "good"
SSE = "\n".join(
    [
        'data:{"type":"PHASE","phase":{"name":"Polling"}}',
        "",
        'data:{"type":"MESSAGE","message":{"partition":0,"offset":7,"key":"k","value":"' + "x" * 50 + '","headers":{}}}',
        "",
        'data:{"type":"CONSUMING","consuming":{"messagesConsumed":1}}',
        "",
        'data:{"type":"DONE","cursor":{"id":"c-1"}}',
        "",
    ]
)


@pytest.fixture(autouse=True)
def fast_login_poll(monkeypatch):
    monkeypatch.setattr(kafbat_client, "LOGIN_POLL_SECONDS", 0.01)


class Harness:
    """Fake kafbat plus a scripted browser: `cookies` are what successive cookie reads return."""

    def __init__(self, cookies: list[str | None], auto_open: bool = True):
        self.requests: list[httpx.Request] = []
        self.opened: list[str] = []
        self._cookies = iter(cookies)
        self._last: str | None = None
        cfg = Config(url=URL, keepalive_seconds=0, auto_open=auto_open, login_wait_seconds=0.5)
        self.kafbat = Kafbat(cfg, httpx.MockTransport(self._handle), self._read_cookie, self.opened.append)
        self.server = build_server(self.kafbat)

    def _read_cookie(self) -> str | None:
        self._last = next(self._cookies, self._last)
        return self._last

    def _handle(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        if req.headers.get("Cookie") != f"SESSION={VALID}":
            return httpx.Response(302, headers={"Location": "/oauth2/authorization/google"})
        if req.url.path == "/api/clusters":
            return httpx.Response(200, json=[{"name": "local", "status": "ONLINE", "version": None, "noise": 1}])
        if req.url.path.endswith("/messages/v2"):
            return httpx.Response(200, text=SSE, headers={"Content-Type": "text/event-stream"})
        if req.url.path.startswith("/api/clusters/blocked/"):
            html = "<!DOCTYPE html><title>Attention Required! | Cloudflare</title>"
            return httpx.Response(403, text=html, headers={"Content-Type": "text/html; charset=UTF-8"})
        return httpx.Response(500, json={"message": "boom", "stackTrace": "at io.kafbat..."})


async def call(h: Harness, tool: str, args: dict | None = None):
    async with Client(h.server) as client:
        result = await client.call_tool(tool, args or {})
    return result, result.content[0].text


def test_cookie_matches_host():
    assert cookie_matches_host(".example.com", "kafka.example.com")
    assert cookie_matches_host("kafka.example.com", "kafka.example.com")
    assert not cookie_matches_host("ample.com", "kafka.example.com")


async def test_tools_are_registered_without_output_schema():
    async with Client(Harness([VALID]).server) as client:
        tools = (await client.list_tools()).tools
    assert {t.name for t in tools} == {
        "list_clusters",
        "list_topics",
        "describe_topic",
        "list_consumer_groups",
        "describe_consumer_group",
        "consume_messages",
        "list_schemas",
        "get_schema",
    }
    assert all(t.output_schema is None for t in tools)


async def test_expired_session_is_replaced_from_browser_without_opening_it():
    h = Harness([VALID])
    h.kafbat._cookie = "expired"
    result, text = await call(h, "list_clusters")
    assert not result.is_error, text
    assert json.loads(text) == [{"name": "local", "status": "ONLINE"}]  # picked fields, nulls dropped
    assert h.opened == []


async def test_stale_browser_cookie_opens_kafbat_and_waits_for_login():
    h = Harness(["stale", "stale", VALID])
    result, text = await call(h, "list_clusters")
    assert not result.is_error, text
    assert h.opened == [URL]


async def test_no_session_with_auto_open_disabled():
    h = Harness([None], auto_open=False)
    result, text = await call(h, "list_clusters")
    assert result.is_error and "No valid kafbat session" in text
    assert h.opened == []


async def test_login_timeout():
    h = Harness(["stale"])
    result, text = await call(h, "list_clusters")
    assert result.is_error and "No kafbat session after" in text
    assert h.opened == [URL]


async def test_consume_messages_parses_stream_and_truncates():
    h = Harness([VALID])
    args = {
        "cluster": "local",
        "topic": "orders.v1",
        "mode": "FROM_OFFSET",
        "offset": 5,
        "partitions": [0, 2],
        "max_value_chars": 10,
    }
    result, text = await call(h, "consume_messages", args)
    assert not result.is_error, text
    out = json.loads(text)
    assert out["nextCursor"] == "c-1"
    assert out["stats"] == {"messagesConsumed": 1}
    assert out["messages"][0]["value"] == "x" * 10 + "…[truncated 40 chars]"
    query = h.requests[-1].url.params
    assert query.get_list("partitions") == ["0", "2"]
    assert (query["mode"], query["offset"]) == ("FROM_OFFSET", "5")


async def test_consume_messages_requires_offset_for_offset_modes():
    result, text = await call(Harness([VALID]), "consume_messages", {"cluster": "c", "topic": "t", "mode": "TO_OFFSET"})
    assert result.is_error and "requires offset" in text


async def test_kafbat_error_returns_message_without_stacktrace_and_encodes_path():
    h = Harness([VALID])
    result, text = await call(h, "describe_consumer_group", {"cluster": "local", "group_id": "a/b"})
    assert result.is_error
    assert "HTTP 500" in text and "boom" in text and "io.kafbat" not in text
    assert h.requests[-1].url.raw_path.endswith(b"/consumer-groups/a%2Fb")


async def test_proxy_html_error_is_summarised():
    result, text = await call(Harness([VALID]), "list_topics", {"cluster": "blocked"})
    assert result.is_error
    assert "HTTP 403" in text and "non-JSON text/html" in text and "VPN" in text and "<html" not in text.lower()


def test_open_command_targets_chromium_profile(monkeypatch):
    monkeypatch.setattr(kafbat_client.sys, "platform", "linux")
    cfg = Config(url=URL, browser="chrome", browser_profile="Profile 2")

    def which(name):
        return "/usr/bin/google-chrome-stable" if name == "google-chrome-stable" else None

    assert open_command(cfg, which) == ["/usr/bin/google-chrome-stable", "--profile-directory=Profile 2"]


@pytest.mark.parametrize(
    "cfg",
    [
        Config(url=URL, browser="chrome", browser_profile="Profile 2"),  # executable not installed
        Config(url=URL, browser="chrome"),  # no profile configured
        Config(url=URL, browser="chrome", browser_profile="/home/me/.config/google-chrome/Profile 2"),  # path
        Config(url=URL, browser="firefox", browser_profile="work"),  # not Chromium
    ],
)
def test_open_command_falls_back_to_default_handler(monkeypatch, cfg):
    monkeypatch.setattr(kafbat_client.sys, "platform", "linux")
    assert open_command(cfg, lambda name: None) == ["xdg-open"]


def test_open_command_override_wins():
    cfg = Config.from_env(
        {"KAFBAT_URL": URL, "KAFBAT_OPEN_COMMAND": "flatpak run com.google.Chrome --profile-directory='Profile 2'"}
    )
    assert open_command(cfg, lambda name: "/usr/bin/" + name) == [
        "flatpak",
        "run",
        "com.google.Chrome",
        "--profile-directory=Profile 2",
    ]


def test_config_from_env():
    cfg = Config.from_env({"KAFBAT_URL": "https://x/", "KAFBAT_KEYRING": "gnomekeyring", "KAFBAT_AUTO_OPEN": "false"})
    assert (cfg.url, cfg.keyring, cfg.auto_open, cfg.browser) == ("https://x", "GNOMEKEYRING", False, "chrome")


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"KAFBAT_URL": "kafka.example.com"},
        {"KAFBAT_URL": "https://x", "KAFBAT_BROWSER": "netscape"},
        {"KAFBAT_URL": "https://x", "KAFBAT_KEYRING": "vault"},
        {"KAFBAT_URL": "https://x", "KAFBAT_LOG_LEVEL": "loud"},
        {"KAFBAT_URL": "https://x", "KAFBAT_OPEN_COMMAND": "google-chrome '--profile-directory=Profile 2"},
    ],
)
def test_config_rejects_invalid_env(env):
    with pytest.raises(ValueError):
        Config.from_env(env)
