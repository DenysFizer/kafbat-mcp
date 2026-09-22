"""Offline tests: a fake kafbat behind httpx2.MockTransport, the server driven by the in-process MCP client."""

import json

import httpx2
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


C = "/api/clusters/local"
FIXTURES = {
    f"{C}/stats": {"brokerCount": 3, "activeControllers": 1, "offlinePartitionCount": 0, "zooKeeperStatus": None},
    f"{C}/brokers": [{"id": 1, "host": "b1", "port": 9092, "partitions": 40, "bytesInPerSec": 1.5}],
    f"{C}/brokers/1/configs": [
        {"name": "log.retention.ms", "value": "604800000", "source": "STATIC_BROKER_CONFIG"},
        {"name": "num.io.threads", "value": "8", "source": "DEFAULT_CONFIG"},
        {"name": "ssl.keystore.password", "value": "pw", "source": "STATIC_BROKER_CONFIG", "isSensitive": True},
    ],
    f"{C}/brokers/logdirs": [{"name": "/data/kafka", "error": "KAFKA_STORAGE_ERROR", "topics": [{}, {}]}],
    f"{C}/connectors": [
        {"connect": "main", "name": "sink-1", "type": "SINK", "status": {"state": "FAILED"}, "failedTasksCount": 1}
    ],
    f"{C}/connects": [{"name": "main", "address": "http://connect:8083", "version": "3.7.0"}],
    f"{C}/connects/main/connectors/sink-1": {
        "name": "sink-1",
        "connect": "main",
        "status": {"state": "FAILED", "trace": "T" * 60},
        "config": {"topics": "orders.v1", "database.password": "hunter2", "consumer.override.sasl.jaas.config": "x"},
    },
    f"{C}/connects/main/connectors/sink-1/tasks": [
        {"id": {"task": 0}, "status": {"id": 0, "state": "FAILED", "trace": "E" * 60}, "config": {"k": "v"}}
    ],
    f"{C}/topics/orders.v1": {
        "name": "orders.v1",
        "partitionCount": 3,
        "partitions": [{"partition": 0}, {"partition": 1}, {"partition": 2}],
    },
    f"{C}/topics/orders.v1/config": [{"name": "retention.ms", "value": "1000", "source": "DYNAMIC_TOPIC_CONFIG"}],
    f"{C}/topics/orders.v1/consumer-groups": [{"groupId": "billing", "state": "STABLE", "consumerLag": 7}],
    f"{C}/schemas/orders.v1/latest": {"subject": "orders.v1", "version": "3", "schema": "{}"},
    f"{C}/schemas/orders.v1/versions": [{"version": "1"}, {"version": "2"}, {"version": "3"}],
    "/api/authorization": {"rbacEnabled": True, "userInfo": {"username": "denys", "permissions": []}},
}


@pytest.fixture(autouse=True)
def fast_login_poll(monkeypatch):
    monkeypatch.setattr(kafbat_client, "LOGIN_POLL_SECONDS", 0.01)


class Harness:
    """Fake kafbat plus a scripted browser: `cookies` are what successive cookie reads return.

    `accepts` decides whether a request counts as authenticated, i.e. which kafbat auth.type we are faking.
    """

    def __init__(
        self,
        cookies: list[str | None] | None = None,
        auto_open: bool = True,
        accepts=None,
        fixtures: dict | None = None,
        **cfg_kwargs,
    ):
        self.requests: list[httpx2.Request] = []
        self.opened: list[str] = []
        self._cookies = iter(cookies or [])
        self._last: str | None = None
        self._accepts = accepts or (lambda req: req.headers.get("Cookie") == f"SESSION={VALID}")
        # A path mapped to None is "this endpoint is broken", so optional() lookups can be exercised.
        self._fixtures = FIXTURES | (fixtures or {})
        cfg = Config(url=URL, keepalive_seconds=0, auto_open=auto_open, login_wait_seconds=0.5, **cfg_kwargs)
        self.kafbat = Kafbat(cfg, httpx2.MockTransport(self._handle), self._read_cookie, self.opened.append)
        self.server = build_server(self.kafbat)

    def _read_cookie(self) -> str | None:
        self._last = next(self._cookies, self._last)
        return self._last

    def _handle(self, req: httpx2.Request) -> httpx2.Response:
        self.requests.append(req)
        if req.url.path == "/login":
            if req.content != b"username=admin&password=s3cret":
                return httpx2.Response(302, headers={"Location": "/login?error"})
            return httpx2.Response(302, headers={"Location": "/", "Set-Cookie": f"SESSION={VALID}; Path=/; HttpOnly"})
        if not self._accepts(req):
            return httpx2.Response(302, headers={"Location": "/oauth2/authorization/google"})
        if req.url.path == "/api/clusters":
            return httpx2.Response(200, json=[{"name": "local", "status": "ONLINE", "version": None, "noise": 1}])
        if req.url.path.endswith("/messages/v2"):
            return httpx2.Response(200, text=SSE, headers={"Content-Type": "text/event-stream"})
        if req.url.path.endswith("/smartfilters"):
            return httpx2.Response(200, json={"id": "filter-9"})
        if req.method == "POST" and req.url.path == f"{C}/topics":
            return httpx2.Response(200, json={"name": "orders.v2", "partitionCount": 3})
        if req.method in ("POST", "DELETE"):
            return httpx2.Response(200, json={})  # kafbat answers writes with an empty body
        if self._fixtures.get(req.url.path) is not None:
            return httpx2.Response(200, json=self._fixtures[req.url.path])
        if req.url.path.endswith("/acl/csv"):
            return httpx2.Response(200, text="principal,host,resource\nUser:app,*,orders.v1\n")
        if req.url.path.startswith("/api/clusters/blocked/"):
            html = "<!DOCTYPE html><title>Attention Required! | Cloudflare</title>"
            return httpx2.Response(403, text=html, headers={"Content-Type": "text/html; charset=UTF-8"})
        return httpx2.Response(500, json={"message": "boom", "stackTrace": "at io.kafbat..."})


async def call(h: Harness, tool: str, args: dict | None = None):
    async with Client(h.server) as client:
        result = await client.call_tool(tool, args or {})
    return result, result.content[0].text


def test_cookie_matches_host():
    assert cookie_matches_host(".example.com", "kafka.example.com")
    assert cookie_matches_host("kafka.example.com", "kafka.example.com")
    assert not cookie_matches_host("ample.com", "kafka.example.com")


ADDITIVE = {"produce_message", "create_topic", "update_connector_state"}
DESTRUCTIVE = {"reset_consumer_group_offsets", "delete_topic"}


async def tool_names(h: Harness) -> set[str]:
    async with Client(h.server) as client:
        return {t.name for t in (await client.list_tools()).tools}


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
        "cluster_health",
        "describe_broker",
        "list_connectors",
        "describe_connector",
        "list_acls",
        "whoami",
    }
    assert all(t.output_schema is None for t in tools)
    # default tier is read-only, so every registered tool must say so
    assert all(t.annotations.read_only_hint for t in tools)


async def test_read_only_is_the_default_and_hides_every_write_tool():
    names = await tool_names(Harness([VALID]))
    assert not (names & (ADDITIVE | DESTRUCTIVE))


async def test_enabling_writes_adds_additive_tools_but_not_destructive_ones():
    names = await tool_names(Harness([VALID], read_only=False))
    assert ADDITIVE <= names
    assert not (names & DESTRUCTIVE)


async def test_allow_destructive_adds_the_rest():
    names = await tool_names(Harness([VALID], read_only=False, allow_destructive=True))
    assert (ADDITIVE | DESTRUCTIVE) <= names


async def test_annotations_match_the_tier_that_admitted_each_tool():
    async with Client(Harness([VALID], read_only=False, allow_destructive=True).server) as client:
        tools = {t.name: t.annotations for t in (await client.list_tools()).tools}
    assert tools["list_topics"].read_only_hint and not tools["list_topics"].destructive_hint
    assert not tools["produce_message"].read_only_hint and not tools["produce_message"].destructive_hint
    assert tools["delete_topic"].destructive_hint and not tools["delete_topic"].read_only_hint
    assert all(a.open_world_hint is False for a in tools.values())


async def test_enable_tools_opts_one_write_tool_back_in_under_read_only():
    names = await tool_names(Harness([VALID], enable_tools=frozenset({"produce_message"})))
    assert "produce_message" in names
    assert "create_topic" not in names and not (names & DESTRUCTIVE)


async def test_a_gated_tool_cannot_be_invoked_by_name():
    """Hiding is the enforcement: the SDK resolves tools/call through the same registry as tools/list."""
    result, text = await call(Harness([VALID]), "delete_topic", {"cluster": "local", "topic": "orders.v1"})
    assert result.is_error and "Unknown tool" in text


async def test_expired_session_is_replaced_from_browser_without_opening_it():
    h = Harness([VALID])
    h.kafbat._headers = {"Cookie": "SESSION=expired"}
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


async def test_auth_none_sends_no_credentials():
    h = Harness(accepts=lambda req: True, auth="none")
    result, text = await call(h, "list_clusters")
    assert not result.is_error, text
    assert "Cookie" not in h.requests[-1].headers and "Authorization" not in h.requests[-1].headers
    assert h.opened == []


async def test_auth_none_against_a_secured_kafbat_says_so_instead_of_opening_a_browser():
    h = Harness(auth="none")  # the fake kafbat still demands a session cookie
    result, text = await call(h, "list_clusters")
    assert result.is_error and "KAFBAT_AUTH" in text
    assert h.opened == []


async def test_form_login_posts_credentials_once_and_reuses_the_session():
    h = Harness(auth="form", username="admin", password="s3cret")
    result, text = await call(h, "list_clusters")
    assert not result.is_error, text
    logins = [r for r in h.requests if r.url.path == "/login"]
    assert len(logins) == 1 and logins[0].content == b"username=admin&password=s3cret"
    assert h.requests[-1].headers["Cookie"] == f"SESSION={VALID}"
    assert h.opened == []


async def test_form_login_with_bad_credentials_names_the_env_vars():
    h = Harness(auth="form", username="admin", password="wrong")
    result, text = await call(h, "list_clusters")
    assert result.is_error and "KAFBAT_USERNAME" in text
    assert h.opened == []


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


async def test_cluster_health_merges_stats_and_brokers():
    result, text = await call(Harness([VALID]), "cluster_health", {"cluster": "local"})
    assert not result.is_error, text
    out = json.loads(text)
    assert out["stats"]["activeControllers"] == 1
    assert "zooKeeperStatus" not in out["stats"]  # null pruned
    assert out["brokers"] == [{"id": 1, "host": "b1", "port": 9092, "partitions": 40}]  # bytesInPerSec not picked


async def test_describe_broker_keeps_non_default_config_and_flags_a_failed_log_dir():
    result, text = await call(Harness([VALID]), "describe_broker", {"cluster": "local", "broker_id": 1})
    assert not result.is_error, text
    out = json.loads(text)
    assert out["nonDefaultConfig"] == {"log.retention.ms": "604800000"}  # default dropped, sensitive dropped
    assert out["logDirs"] == [{"name": "/data/kafka", "error": "KAFKA_STORAGE_ERROR", "topicCount": 2}]


async def test_describe_connector_masks_credentials_and_truncates_traces():
    args = {"cluster": "local", "connect": "main", "connector": "sink-1", "max_trace_chars": 10}
    result, text = await call(Harness([VALID]), "describe_connector", args)
    assert not result.is_error, text
    out = json.loads(text)
    assert out["config"] == {
        "topics": "orders.v1",
        "database.password": "***",
        "consumer.override.sasl.jaas.config": "x",  # no secret-ish word in the key, left alone
    }
    assert "hunter2" not in text
    assert out["status"]["trace"] == "T" * 10 + "…[truncated 50 chars]"
    assert out["tasks"][0]["status"]["trace"] == "E" * 10 + "…[truncated 50 chars]"
    assert "config" not in out["tasks"][0]  # per-task copy of the connector config dropped


async def test_list_connectors_reports_connect_clusters_only_when_empty():
    _, text = await call(Harness([VALID]), "list_connectors", {"cluster": "local"})
    assert json.loads(text)["connectors"][0]["name"] == "sink-1"
    assert "connectClusters" not in json.loads(text)

    empty = Harness([VALID], fixtures={f"{C}/connectors": []})
    _, text = await call(empty, "list_connectors", {"cluster": "local"})
    assert json.loads(text)["connectClusters"] == [
        {"name": "main", "address": "http://connect:8083", "version": "3.7.0"}
    ]


async def test_describe_topic_lists_the_groups_reading_it():
    result, text = await call(Harness([VALID]), "describe_topic", {"cluster": "local", "topic": "orders.v1"})
    assert not result.is_error, text
    out = json.loads(text)
    assert out["nonDefaultConfig"] == {"retention.ms": "1000"}
    assert out["consumerGroups"] == [{"groupId": "billing", "state": "STABLE", "consumerLag": 7}]


async def test_get_schema_lists_available_versions():
    result, text = await call(Harness([VALID]), "get_schema", {"cluster": "local", "subject": "orders.v1"})
    assert not result.is_error, text
    assert json.loads(text)["availableVersions"] == ["1", "2", "3"]


async def test_a_failing_secondary_lookup_does_not_sink_the_tool():
    h = Harness([VALID], fixtures={f"{C}/topics/orders.v1/consumer-groups": None})  # that endpoint 500s
    result, text = await call(h, "describe_topic", {"cluster": "local", "topic": "orders.v1"})
    assert not result.is_error, text
    out = json.loads(text)
    assert out["name"] == "orders.v1" and "consumerGroups" not in out


async def test_smart_filter_is_registered_then_passed_by_id():
    h = Harness([VALID])
    args = {"cluster": "local", "topic": "orders.v1", "smart_filter": "value.orderId == 42"}
    result, text = await call(h, "consume_messages", args)
    assert not result.is_error, text
    register = [r for r in h.requests if r.url.path.endswith("/smartfilters")]
    assert len(register) == 1 and register[0].method == "POST"
    assert json.loads(register[0].content) == {"filterCode": "value.orderId == 42"}
    assert h.requests[-1].url.params["smartFilterId"] == "filter-9"


async def test_acls_come_back_as_csv_text():
    result, text = await call(Harness([VALID]), "list_acls", {"cluster": "local"})
    assert not result.is_error, text
    assert text.startswith("principal,host,resource")


async def test_whoami_reports_rbac_state():
    result, text = await call(Harness([VALID]), "whoami")
    assert not result.is_error, text
    assert json.loads(text) == {"rbacEnabled": True, "userInfo": {"username": "denys", "permissions": []}}


async def test_produce_message_posts_only_the_fields_given():
    h = Harness([VALID], read_only=False)
    args = {"cluster": "local", "topic": "orders.v1", "partition": 2, "value": '{"id":1}'}
    result, text = await call(h, "produce_message", args)
    assert not result.is_error, text
    post = h.requests[-1]
    assert post.method == "POST" and post.url.path == f"{C}/topics/orders.v1/messages"
    # serdes are always sent: kafbat 500s with "Value is undefined" unless both are present
    assert json.loads(post.content) == {
        "partition": 2,
        "value": '{"id":1}',
        "keySerde": "String",
        "valueSerde": "String",
    }


async def test_produce_message_reserialises_a_json_payload():
    """The SDK JSON-parses string args, so a JSON value arrives as a dict and must go back out as text."""
    h = Harness([VALID], read_only=False)
    args = {"cluster": "local", "topic": "orders.v1", "value": {"id": 1, "note": None}}
    result, text = await call(h, "produce_message", args)
    assert not result.is_error, text
    assert json.loads(h.requests[-1].content)["value"] == '{"id":1,"note":null}'  # null kept, not pruned


async def test_create_topic_returns_what_kafbat_created():
    h = Harness([VALID], read_only=False)
    args = {"cluster": "local", "topic": "orders.v2", "partitions": 3, "configs": {"retention.ms": "1000"}}
    result, text = await call(h, "create_topic", args)
    assert not result.is_error, text
    assert json.loads(h.requests[-1].content) == {
        "name": "orders.v2",
        "partitions": 3,
        "configs": {"retention.ms": "1000"},
    }
    assert json.loads(text)["name"] == "orders.v2"


async def test_update_connector_state_puts_the_action_in_the_path():
    h = Harness([VALID], read_only=False)
    args = {"cluster": "local", "connect": "main", "connector": "sink-1", "action": "RESTART_FAILED_TASKS"}
    result, text = await call(h, "update_connector_state", args)
    assert not result.is_error, text
    assert h.requests[-1].url.path == f"{C}/connects/main/connectors/sink-1/action/RESTART_FAILED_TASKS"


async def test_delete_topic_sends_delete():
    h = Harness([VALID], read_only=False, allow_destructive=True)
    result, text = await call(h, "delete_topic", {"cluster": "local", "topic": "orders.v1"})
    assert not result.is_error, text
    assert h.requests[-1].method == "DELETE" and h.requests[-1].url.path == f"{C}/topics/orders.v1"


async def test_offset_reset_builds_partition_offsets_and_rejects_incomplete_input():
    h = Harness([VALID], read_only=False, allow_destructive=True)
    args = {
        "cluster": "local",
        "group_id": "billing",
        "topic": "orders.v1",
        "reset_type": "OFFSET",
        "partitions": [0, 1],
        "offset": 500,
    }
    result, text = await call(h, "reset_consumer_group_offsets", args)
    assert not result.is_error, text
    assert json.loads(h.requests[-1].content) == {
        "topic": "orders.v1",
        "resetType": "OFFSET",
        "partitions": [0, 1],
        "partitionsOffsets": [{"partition": 0, "offset": 500}, {"partition": 1, "offset": 500}],
    }

    bad = Harness([VALID], read_only=False, allow_destructive=True)
    args = {"cluster": "local", "group_id": "billing", "topic": "orders.v1", "reset_type": "OFFSET"}
    result, text = await call(bad, "reset_consumer_group_offsets", args)
    assert result.is_error and "requires both partitions and offset" in text


async def test_offset_reset_without_partitions_resolves_them_from_the_topic():
    """kafbat answers 200 and resets nothing when `partitions` is absent, so it is never left out."""
    h = Harness([VALID], read_only=False, allow_destructive=True)
    args = {"cluster": "local", "group_id": "billing", "topic": "orders.v1", "reset_type": "EARLIEST"}
    result, text = await call(h, "reset_consumer_group_offsets", args)
    assert not result.is_error, text
    assert json.loads(h.requests[-1].content) == {
        "topic": "orders.v1",
        "resetType": "EARLIEST",
        "partitions": [0, 1, 2],
    }


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
    # must stay under the 60s default tool timeout of MCP clients
    assert cfg.login_wait_seconds <= 50


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"KAFBAT_URL": "kafka.example.com"},
        {"KAFBAT_URL": "https://x", "KAFBAT_BROWSER": "netscape"},
        {"KAFBAT_URL": "https://x", "KAFBAT_KEYRING": "vault"},
        {"KAFBAT_URL": "https://x", "KAFBAT_LOG_LEVEL": "loud"},
        {"KAFBAT_URL": "https://x", "KAFBAT_OPEN_COMMAND": "google-chrome '--profile-directory=Profile 2"},
        {"KAFBAT_URL": "https://x", "KAFBAT_AUTH": "basic"},  # kafbat never configures httpBasic()
        {"KAFBAT_URL": "https://x", "KAFBAT_AUTH": "form"},  # no credentials
        {"KAFBAT_URL": "https://x", "KAFBAT_AUTH": "form", "KAFBAT_USERNAME": "admin"},  # no password
        {"KAFBAT_URL": "https://x", "KAFBAT_ALLOW_DESTRUCTIVE": "true"},  # would be silently ignored
    ],
)
def test_config_rejects_invalid_env(env):
    with pytest.raises(ValueError):
        Config.from_env(env)
