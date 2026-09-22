"""Integration tests against a real kafbat UI and Kafka in Docker.

Opt-in, because they need Docker and take a couple of minutes:

    KAFBAT_INTEGRATION=1 uv run pytest tests/test_integration.py

The offline suite in test_server.py pins how we shape requests; these pin that kafbat actually accepts them.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from pathlib import Path

import httpx2
import pytest
from mcp import Client
from testcontainers.community.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from kafbat_mcp import client as kafbat_client
from kafbat_mcp.client import Kafbat
from kafbat_mcp.config import Config
from kafbat_mcp.server import build_server

pytestmark = pytest.mark.skipif(not os.getenv("KAFBAT_INTEGRATION"), reason="set KAFBAT_INTEGRATION=1 to run")

# Comma-separated, to run the whole suite against several kafbat releases:
#   KAFBAT_IMAGES=ghcr.io/kafbat/kafka-ui:v1.3.0,ghcr.io/kafbat/kafka-ui:main
_IMAGES = os.getenv("KAFBAT_IMAGES", "ghcr.io/kafbat/kafka-ui:v1.5.0")
KAFBAT_IMAGES = [i.strip() for i in _IMAGES.split(",") if i.strip()]
CONFLUENT = "7.6.0"
# Every JVM here would otherwise size its heap from the *host* (25% of RAM, 3.8 GiB each on a 15 GiB laptop), and
# the suite runs six of them: that froze a dev machine once. Cap the heap, and give each container a hard memory
# limit so a runaway JVM is killed inside its container instead of dragging the host into swap.
KAFBAT_HEAP, KAFBAT_MEM = "-Xmx512m -Xms64m", "1g"
BROKER_HEAP, BROKER_MEM = "-Xmx512m -Xms256m", "1g"  # Kafka and Connect both read KAFKA_HEAP_OPTS
CLUSTER = "test"
USERNAME, PASSWORD = "admin", "s3cret"
SUBJECT = "orders.v1-value"
CONNECT, CONNECTOR = "main", "heartbeat"
# cp-kafka-connect ships only the Mirror* plugins, so the heartbeat connector is what we can actually run.
CONNECTOR_CONFIG = {
    "name": CONNECTOR,
    "config": {
        "connector.class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
        "tasks.max": "1",
        "source.cluster.alias": "src",
        "target.cluster.alias": "dst",
        "source.cluster.bootstrap.servers": "kafka:9092",
        "target.cluster.bootstrap.servers": "kafka:9092",
        "source.cluster.ssl.truststore.password": "hunter2",  # must come back masked
    },
}


def start_kafbat(image: str, network: Network, **env: str) -> DockerContainer:
    ui = (
        DockerContainer(image, mem_limit=KAFBAT_MEM)
        .with_network(network)
        .with_exposed_ports(8080)
        .with_env("KAFKA_CLUSTERS_0_NAME", CLUSTER)
        # The BROKER listener advertises the container's own IP, so the alias resolves for both hops.
        .with_env("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", "kafka:9092")
        .with_env("DYNAMIC_CONFIG_ENABLED", "false")
        .with_env("JAVA_OPTS", KAFBAT_HEAP)
        # kafbat serves inactive consumer groups from a cache a scheduler refreshes; 30s is too slow to test
        .with_env("KAFKA_UPDATE_METRICS_RATE_MILLIS", "2000")
    )
    for key, value in env.items():
        ui.with_env(key, value)
    ui.waiting_for(LogMessageWaitStrategy(re.compile(r"Started KafkaUiApplication")).with_startup_timeout(180))
    ui.start()
    return ui


def url_of(ui: DockerContainer) -> str:
    return f"http://{ui.get_container_host_ip()}:{ui.get_exposed_port(8080)}"


def await_cluster_online(base: str) -> None:
    """kafbat reports a cluster as INITIALIZING for the first seconds after boot.

    Case-insensitive on purpose: v1.3.0 serialises the status lowercase, v1.4.2+ uppercase. No tool
    compares this value, so the change only ever affected this probe.
    """
    for _ in range(45):
        clusters = httpx2.get(f"{base}/api/clusters", timeout=10).json()
        if clusters and (clusters[0].get("status") or "").upper() == "ONLINE":
            return
        time.sleep(2)
    raise AssertionError(f"cluster never came online: {clusters}")


@pytest.fixture(scope="session", params=KAFBAT_IMAGES, ids=lambda i: i.rsplit(":", 1)[-1])
def kafbat_image(request) -> str:
    """Parametrised so every test re-runs per kafbat release. Only this container varies: Kafka, Schema
    Registry and Connect are version-independent, so they boot once and are shared across the runs."""
    return request.param


@pytest.fixture(scope="session")
def name(kafbat_image):
    """Topics and consumer groups are namespaced per release, since the Kafka broker is shared."""
    slug = re.sub(r"[^a-z0-9]+", "-", kafbat_image.rsplit(":", 1)[-1].lower()).strip("-")
    return lambda base: f"{base}.{slug}"


@pytest.fixture(scope="session")
def network():
    with Network() as net:
        yield net


@pytest.fixture(scope="session")
def kafka(network):
    container = (
        KafkaContainer(mem_limit=BROKER_MEM)
        .with_kraft()
        .with_network(network)
        .with_network_aliases("kafka")
        .with_env("KAFKA_HEAP_OPTS", BROKER_HEAP)
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def schema_registry(kafka, network):
    sr = (
        DockerContainer(f"confluentinc/cp-schema-registry:{CONFLUENT}", mem_limit="512m")
        .with_network(network)
        .with_network_aliases("schemaregistry")
        .with_exposed_ports(8081)
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
        .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_env("SCHEMA_REGISTRY_HEAP_OPTS", "-Xmx256m")
    )
    sr.waiting_for(
        LogMessageWaitStrategy(re.compile(r"Server started, listening for requests")).with_startup_timeout(180)
    )
    sr.start()
    url = f"http://{sr.get_container_host_ip()}:{sr.get_exposed_port(8081)}"
    for fields in ('[{"name":"id","type":"int"}]',
                   '[{"name":"id","type":"int"},{"name":"note","type":["null","string"],"default":null}]'):
        schema = f'{{"type":"record","name":"O","fields":{fields}}}'
        r = httpx2.post(
            f"{url}/subjects/{SUBJECT}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json={"schema": schema},
            timeout=30,
        )
        assert r.is_success, r.text
    yield sr
    sr.stop()


@pytest.fixture(scope="session")
def connect(kafka, network):
    worker = (
        DockerContainer(f"confluentinc/cp-kafka-connect:{CONFLUENT}", mem_limit=BROKER_MEM)
        .with_network(network)
        .with_network_aliases("connect")
        .with_exposed_ports(8083)
        .with_env("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
        .with_env("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
        .with_env("CONNECT_REST_PORT", "8083")
        .with_env("CONNECT_GROUP_ID", "connect-cluster")
        .with_env("CONNECT_CONFIG_STORAGE_TOPIC", "_connect_configs")
        .with_env("CONNECT_OFFSET_STORAGE_TOPIC", "_connect_offsets")
        .with_env("CONNECT_STATUS_STORAGE_TOPIC", "_connect_status")
        .with_env("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
        .with_env("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
        .with_env("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
        .with_env("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
        .with_env("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
        .with_env("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
        .with_env("KAFKA_HEAP_OPTS", BROKER_HEAP)
    )
    worker.waiting_for(LogMessageWaitStrategy(re.compile(r"Kafka Connect started")).with_startup_timeout(300))
    worker.start()
    url = f"http://{worker.get_container_host_ip()}:{worker.get_exposed_port(8083)}"
    r = httpx2.post(f"{url}/connectors", json=CONNECTOR_CONFIG, timeout=60)
    assert r.is_success, r.text
    yield worker
    worker.stop()


@pytest.fixture(scope="session")
def open_kafbat(kafbat_image, kafka, schema_registry, connect, network):
    """kafbat with auth.type DISABLED — kafbat's own default — wired to Schema Registry and Connect."""
    ui = start_kafbat(
        kafbat_image,
        network,
        KAFKA_CLUSTERS_0_SCHEMAREGISTRY="http://schemaregistry:8081",
        KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME=CONNECT,
        KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS="http://connect:8083",
    )
    base = url_of(ui)
    await_cluster_online(base)
    yield base
    ui.stop()


@pytest.fixture(scope="session")
def oauth_kafbat(kafbat_image, kafka, network):
    """kafbat with auth.type OAUTH2 and a dummy client: enough to build the filter chain, so an
    unauthenticated API call redirects to /oauth2/authorization/... without any IdP being reachable."""
    ui = start_kafbat(
        kafbat_image,
        network,
        AUTH_TYPE="OAUTH2",
        AUTH_OAUTH2_CLIENT_TEST_PROVIDER="oauth",
        AUTH_OAUTH2_CLIENT_TEST_CLIENTID="dummy",
        AUTH_OAUTH2_CLIENT_TEST_CLIENTSECRET="dummy",
        AUTH_OAUTH2_CLIENT_TEST_CLIENTNAME="test",
        AUTH_OAUTH2_CLIENT_TEST_SCOPE="openid",
        AUTH_OAUTH2_CLIENT_TEST_AUTHORIZATIONGRANTTYPE="authorization_code",
        AUTH_OAUTH2_CLIENT_TEST_REDIRECTURI="http://localhost:8080/login/oauth2/code/test",
        AUTH_OAUTH2_CLIENT_TEST_AUTHORIZATIONURI="http://idp.invalid/auth",
        AUTH_OAUTH2_CLIENT_TEST_TOKENURI="http://idp.invalid/token",
        AUTH_OAUTH2_CLIENT_TEST_USERINFOURI="http://idp.invalid/userinfo",
        AUTH_OAUTH2_CLIENT_TEST_USERNAMEATTRIBUTE="sub",
    )
    yield url_of(ui)
    ui.stop()


@pytest.fixture(scope="session")
def secured_kafbat(kafbat_image, kafka, network):
    """kafbat with auth.type LOGIN_FORM; the same Spring chain LDAP uses."""
    ui = start_kafbat(
        kafbat_image,
        network,
        AUTH_TYPE="LOGIN_FORM",
        SPRING_SECURITY_USER_NAME=USERNAME,
        SPRING_SECURITY_USER_PASSWORD=PASSWORD,
    )
    yield url_of(ui)
    ui.stop()


async def call(base: str, tool: str, args: dict | None = None, **cfg_kwargs):
    """One MCP session per call: the client's lifespan closes the HTTP client on exit."""
    cfg = Config(url=base, keepalive_seconds=0, **{"auth": "none", "auto_open": False, **cfg_kwargs})
    async with Client(build_server(Kafbat(cfg))) as client:
        result = await client.call_tool(tool, args or {})
    return result, result.content[0].text


# --- reads ---------------------------------------------------------------------------------------


async def test_reads_a_real_cluster(open_kafbat):
    result, text = await call(open_kafbat, "list_clusters")
    assert not result.is_error, text
    assert json.loads(text)[0]["name"] == CLUSTER

    result, text = await call(open_kafbat, "cluster_health", {"cluster": CLUSTER})
    assert not result.is_error, text
    health = json.loads(text)
    assert health["stats"]["brokerCount"] == 1
    assert health["brokers"][0]["id"] == 1


async def test_whoami_reports_rbac_disabled(open_kafbat):
    result, text = await call(open_kafbat, "whoami")
    assert not result.is_error, text
    assert json.loads(text)["rbacEnabled"] is False


async def test_describe_broker(open_kafbat):
    result, text = await call(open_kafbat, "describe_broker", {"cluster": CLUSTER, "broker_id": 1})
    assert not result.is_error, text
    out = json.loads(text)
    assert out["brokerId"] == 1
    assert out["nonDefaultConfig"]  # cp-kafka sets plenty through env vars
    assert all("password" not in k for k in out["nonDefaultConfig"])  # sensitive entries dropped
    log_dir = out["logDirs"][0]
    assert log_dir["topicCount"] >= 1
    assert "error" not in log_dir  # a healthy disk reports no error, and prune drops the null


async def test_list_consumer_groups(open_kafbat, kafka, name):
    topic, group = name("orders.listed"), name("listing-group")
    await call(open_kafbat, "create_topic", {"cluster": CLUSTER, "topic": topic}, read_only=False)
    await call(open_kafbat, "produce_message", {"cluster": CLUSTER, "topic": topic, "value": "m"}, read_only=False)
    commit_offsets(kafka, topic, group, 1)

    result, text = await call(open_kafbat, "list_consumer_groups", {"cluster": CLUSTER, "search": group})
    assert not result.is_error, text
    listed = json.loads(text)["consumerGroups"]
    assert [g["groupId"] for g in listed] == [group]
    assert listed[0]["state"] == "EMPTY"


async def test_schema_registry_reads(open_kafbat):
    result, text = await call(open_kafbat, "list_schemas", {"cluster": CLUSTER})
    assert not result.is_error, text
    assert SUBJECT in [s["subject"] for s in json.loads(text)["schemas"]]

    result, text = await call(open_kafbat, "get_schema", {"cluster": CLUSTER, "subject": SUBJECT})
    assert not result.is_error, text
    latest = json.loads(text)
    assert latest["version"] == "2" and latest["schemaType"] == "AVRO"
    assert sorted(latest["availableVersions"]) == ["1", "2"]  # so `version` never has to be guessed
    assert "note" in latest["schema"]

    result, text = await call(open_kafbat, "get_schema", {"cluster": CLUSTER, "subject": SUBJECT, "version": 1})
    assert not result.is_error, text
    assert json.loads(text)["version"] == "1"


async def test_connect_reads(open_kafbat):
    result, text = await call(open_kafbat, "list_connectors", {"cluster": CLUSTER})
    assert not result.is_error, text
    listed = json.loads(text)["connectors"]
    assert [(c["connect"], c["name"]) for c in listed] == [(CONNECT, CONNECTOR)]

    args = {"cluster": CLUSTER, "connect": CONNECT, "connector": CONNECTOR}
    result, text = await call(open_kafbat, "describe_connector", args)
    assert not result.is_error, text
    info = json.loads(text)
    assert info["status"]["state"] == "RUNNING"
    assert info["config"]["source.cluster.ssl.truststore.password"] == "***"
    assert "hunter2" not in text  # the whole payload, not just that one field
    assert info["tasks"][0]["status"]["state"] == "RUNNING"


# --- writes --------------------------------------------------------------------------------------


async def test_create_produce_consume_round_trip(open_kafbat, name):
    """The one test that proves our request bodies are right: kafbat has to accept every step."""
    topic = name("orders.roundtrip")
    write = {"read_only": False}

    result, text = await call(
        open_kafbat,
        "create_topic",
        {"cluster": CLUSTER, "topic": topic, "partitions": 2, "configs": {"retention.ms": "600000"}},
        **write,
    )
    assert not result.is_error, text

    result, text = await call(open_kafbat, "describe_topic", {"cluster": CLUSTER, "topic": topic})
    assert not result.is_error, text
    described = json.loads(text)
    assert described["partitionCount"] == 2
    assert described["nonDefaultConfig"]["retention.ms"] == "600000"
    assert described["consumerGroups"] == []  # nobody reads it yet

    # a dict value exercises the SDK's JSON pre-parsing, the way a model would actually call this
    produced = {"orderId": 42, "note": None}
    result, text = await call(
        open_kafbat,
        "produce_message",
        {"cluster": CLUSTER, "topic": topic, "partition": 0, "key": "k1", "value": produced},
        **write,
    )
    assert not result.is_error, text

    result, text = await call(open_kafbat, "consume_messages", {"cluster": CLUSTER, "topic": topic, "mode": "EARLIEST"})
    assert not result.is_error, text
    messages = json.loads(text)["messages"]
    assert len(messages) == 1
    assert messages[0]["key"] == "k1"
    assert json.loads(messages[0]["value"]) == produced  # null survived the round trip


async def test_smart_filter_is_evaluated_by_kafbat(open_kafbat, name):
    """CEL is compiled server-side, so only a real kafbat can prove the expression and bindings are right."""
    topic = name("orders.filtered")
    await call(open_kafbat, "create_topic", {"cluster": CLUSTER, "topic": topic}, read_only=False)
    for order_id in (1, 2, 3):
        result, text = await call(
            open_kafbat,
            "produce_message",
            {"cluster": CLUSTER, "topic": topic, "partition": 0, "value": {"orderId": order_id}},
            read_only=False,
        )
        assert not result.is_error, text

    # the bindings the tool's docstring promises the model
    for expression in ("record.value.orderId == 2", 'record.valueAsText.contains("\\"orderId\\":2")'):
        args = {"cluster": CLUSTER, "topic": topic, "mode": "EARLIEST", "smart_filter": expression}
        result, text = await call(open_kafbat, "consume_messages", args)
        assert not result.is_error, f"{expression}: {text}"
        messages = json.loads(text)["messages"]
        assert [json.loads(m["value"])["orderId"] for m in messages] == [2], expression


def commit_offsets(kafka, topic: str, group: str, count: int) -> None:
    """Create a real, then inactive, consumer group with committed offsets.

    kafbat refuses to reset a group that still has members, so this has to be a consumer that exits:
    the console consumer stops after `--max-messages` and its close() flushes the auto-commit.
    Done through the broker's own CLI so the suite needs no Kafka client library.
    """
    result = kafka.exec(
        [
            "kafka-console-consumer",
            "--bootstrap-server", "localhost:9092",  # the BROKER listener, from inside the container
            "--topic", topic,
            "--group", group,
            "--from-beginning",
            "--max-messages", str(count),
            "--timeout-ms", "30000",
            "--consumer-property", "enable.auto.commit=true",
            "--consumer-property", "auto.commit.interval.ms=100",
        ]
    )
    assert b"Processed a total of" in result.output, result.output.decode()[-800:]


async def offsets_of(base: str, group: str) -> dict[int, tuple]:
    """{partition: (committed, end)} — asserting on these beats a summed lag, which hides a missing field."""
    result, text = await call(base, "describe_consumer_group", {"cluster": CLUSTER, "group_id": group})
    assert not result.is_error, text
    return {p["partition"]: (p.get("currentOffset"), p.get("endOffset")) for p in json.loads(text)["partitions"]}



async def topic_consumer_groups(base: str, topic: str) -> list[str]:
    """An EMPTY group reaches describe_topic only once the statistics scraper has run again."""
    for _ in range(20):
        _, text = await call(base, "describe_topic", {"cluster": CLUSTER, "topic": topic})
        groups = [g["groupId"] for g in json.loads(text).get("consumerGroups", [])]
        if groups:
            return groups
        await asyncio.sleep(1)
    return []


async def test_reset_consumer_group_offsets(open_kafbat, kafka, name):
    topic, group = name("orders.resettable"), name("billing")
    write = {"read_only": False}
    destructive = {"read_only": False, "allow_destructive": True}
    await call(open_kafbat, "create_topic", {"cluster": CLUSTER, "topic": topic}, **write)
    for n in range(3):
        result, text = await call(
            open_kafbat, "produce_message", {"cluster": CLUSTER, "topic": topic, "value": f"m{n}"}, **write
        )
        assert not result.is_error, text

    commit_offsets(kafka, topic, group, 3)
    assert await offsets_of(open_kafbat, group) == {0: (3, 3)}  # the group read everything

    assert await topic_consumer_groups(open_kafbat, topic) == [group]

    # no `partitions`: the tool has to resolve them, or kafbat answers 200 and resets nothing
    args = {"cluster": CLUSTER, "group_id": group, "topic": topic, "reset_type": "EARLIEST"}
    result, text = await call(open_kafbat, "reset_consumer_group_offsets", args, **destructive)
    assert not result.is_error, text
    assert await offsets_of(open_kafbat, group) == {0: (0, 3)}  # rewound to the start

    args = {
        "cluster": CLUSTER,
        "group_id": group,
        "topic": topic,
        "reset_type": "OFFSET",
        "partitions": [0],
        "offset": 2,
    }
    result, text = await call(open_kafbat, "reset_consumer_group_offsets", args, **destructive)
    assert not result.is_error, text
    assert await offsets_of(open_kafbat, group) == {0: (2, 3)}  # moved to the explicit offset


async def test_update_connector_state_pauses_and_resumes(open_kafbat):
    """Runs last of the Connect tests: it leaves the connector paused until it resumes it."""

    async def state() -> str:
        args = {"cluster": CLUSTER, "connect": CONNECT, "connector": CONNECTOR}
        _, text = await call(open_kafbat, "describe_connector", args)
        return json.loads(text)["status"]["state"]

    async def await_state(expected: str) -> str:
        for _ in range(20):
            if (actual := await state()) == expected:
                return actual
            await asyncio.sleep(1)
        return actual

    args = {"cluster": CLUSTER, "connect": CONNECT, "connector": CONNECTOR, "action": "PAUSE"}
    result, text = await call(open_kafbat, "update_connector_state", args, read_only=False)
    assert not result.is_error, text
    assert await await_state("PAUSED") == "PAUSED"

    args["action"] = "RESUME"
    result, text = await call(open_kafbat, "update_connector_state", args, read_only=False)
    assert not result.is_error, text
    assert await await_state("RUNNING") == "RUNNING"


async def test_delete_topic_removes_it(open_kafbat, name):
    topic = name("orders.doomed")
    destructive = {"read_only": False, "allow_destructive": True}
    await call(open_kafbat, "create_topic", {"cluster": CLUSTER, "topic": topic}, **destructive)

    result, text = await call(open_kafbat, "delete_topic", {"cluster": CLUSTER, "topic": topic}, **destructive)
    assert not result.is_error, text

    result, text = await call(open_kafbat, "list_topics", {"cluster": CLUSTER, "search": topic})
    assert not result.is_error, text
    assert json.loads(text)["topics"] == []


async def test_read_only_tier_really_cannot_write(open_kafbat):
    result, text = await call(open_kafbat, "delete_topic", {"cluster": CLUSTER, "topic": "whatever"})
    assert result.is_error and "Unknown tool" in text


# --- auth ----------------------------------------------------------------------------------------


def firefox_profile(directory: Path, host: str, cookie: str) -> str:
    """A minimal Firefox profile yt-dlp can read.

    Firefox keeps cookies in a plain sqlite table with no OS-keyring encryption, so the real extraction path
    can be exercised without a browser. Only the columns yt-dlp selects have to exist.
    """
    directory.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(directory / "cookies.sqlite")
    con.execute(
        "CREATE TABLE IF NOT EXISTS moz_cookies (id INTEGER PRIMARY KEY, originAttributes TEXT DEFAULT '',"
        " name TEXT, value TEXT, host TEXT, path TEXT, expiry INTEGER, lastAccessed INTEGER,"
        " creationTime INTEGER, isSecure INTEGER, isHttpOnly INTEGER)"
    )
    con.execute("DELETE FROM moz_cookies")
    con.execute(
        "INSERT INTO moz_cookies (name, value, host, path, expiry, isSecure, isHttpOnly) VALUES (?,?,?,?,?,0,1)",
        ("SESSION", cookie, host, "/", int(time.time()) + 3600),
    )
    con.commit()
    con.close()
    return str(directory)


@pytest.fixture(scope="session")
def real_session_cookie(secured_kafbat) -> str:
    """A genuine kafbat SESSION cookie, minted by form login so no browser or IdP is needed."""
    r = httpx2.post(
        f"{secured_kafbat}/login",
        data={"username": USERNAME, "password": PASSWORD},
        follow_redirects=False,
        timeout=30,
    )
    cookie = r.cookies.get("SESSION")
    assert cookie, f"no SESSION cookie: HTTP {r.status_code}"
    return cookie


async def test_cookie_strategy_replays_a_real_session(secured_kafbat, real_session_cookie, tmp_path):
    """The production path for OAUTH2/SSO: extract the cookie from a browser profile and replay it.

    Everything except the OAuth login itself — yt-dlp extraction, host matching, replay, kafbat accepting it.
    """
    profile = firefox_profile(tmp_path / "ff", "localhost", real_session_cookie)
    result, text = await call(
        secured_kafbat, "list_clusters", auth="cookie", browser="firefox", browser_profile=profile
    )
    assert not result.is_error, text
    assert json.loads(text)[0]["name"] == CLUSTER


async def test_stale_cookie_is_reported_when_the_browser_cannot_be_opened(secured_kafbat, tmp_path):
    profile = firefox_profile(tmp_path / "ff-stale", "localhost", "definitely-not-a-session")
    result, text = await call(
        secured_kafbat,
        "list_clusters",
        auth="cookie",
        browser="firefox",
        browser_profile=profile,
        auto_open=False,
    )
    assert result.is_error and "No valid kafbat session" in text


async def test_expired_cookie_recovers_once_the_browser_logs_in_again(
    secured_kafbat, real_session_cookie, tmp_path, monkeypatch
):
    """Starts stale, and 'logging in' rewrites the profile — the real 302-to-login drives the refresh."""
    monkeypatch.setattr(kafbat_client, "LOGIN_POLL_SECONDS", 0.2)
    directory = tmp_path / "ff-relogin"
    profile = firefox_profile(directory, "localhost", "stale-session")
    cfg = Config(
        url=secured_kafbat,
        auth="cookie",
        browser="firefox",
        browser_profile=profile,
        keepalive_seconds=0,
        login_wait_seconds=15,
    )
    opened: list[str] = []

    def log_in(url: str) -> None:
        opened.append(url)
        firefox_profile(directory, "localhost", real_session_cookie)

    async with Client(build_server(Kafbat(cfg, opener=log_in))) as client:
        result = await client.call_tool("list_clusters", {})
    assert not result.is_error, result.content[0].text
    assert opened == [secured_kafbat]


async def test_oauth2_kafbat_redirects_and_we_notice(oauth_kafbat):
    """auth_failed() assumes kafbat redirects unauthenticated API calls rather than answering 401."""
    unauthenticated = httpx2.get(f"{oauth_kafbat}/api/clusters", follow_redirects=False, timeout=30)
    assert unauthenticated.is_redirect, unauthenticated.status_code
    assert "/oauth2/authorization/" in unauthenticated.headers["Location"]

    result, text = await call(oauth_kafbat, "list_clusters", auto_open=False)
    assert result.is_error and "KAFBAT_AUTH" in text


async def test_form_login_against_a_secured_kafbat(secured_kafbat):
    result, text = await call(
        secured_kafbat, "list_clusters", auth="form", username=USERNAME, password=PASSWORD
    )
    assert not result.is_error, text
    assert json.loads(text)[0]["name"] == CLUSTER


async def test_wrong_password_is_reported_clearly(secured_kafbat):
    result, text = await call(
        secured_kafbat, "list_clusters", auth="form", username=USERNAME, password="wrong"
    )
    assert result.is_error and "KAFBAT_USERNAME" in text


async def test_no_credentials_against_a_secured_kafbat(secured_kafbat):
    result, text = await call(secured_kafbat, "list_clusters")
    assert result.is_error and "KAFBAT_AUTH" in text
