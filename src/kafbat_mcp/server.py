"""MCP tools over kafbat's REST API.

Every tool declares MCP annotations, and those annotations are what the read-only tiers filter on, so a
tool's label and its gate can never disagree. Writes are off by default; see Config and build_server.tool().
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Awaitable
from contextlib import asynccontextmanager, suppress
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from kafbat_mcp import __version__
from kafbat_mcp.client import Kafbat
from kafbat_mcp.config import Config
from kafbat_mcp.formatting import (
    as_text,
    mask_secrets,
    non_default,
    parse_message_events,
    path_seg,
    pick,
    to_json,
    trim_trace,
    truncate,
)

MAX_MESSAGES = 500
MAX_ACL_CHARS = 20000

CLUSTER_FIELDS = (
    "name",
    "status",
    "defaultCluster",
    "readOnly",
    "version",
    "brokerCount",
    "topicCount",
    "onlinePartitionCount",
    "features",
)
TOPIC_FIELDS = (
    "name",
    "internal",
    "partitionCount",
    "replicationFactor",
    "inSyncReplicas",
    "underReplicatedPartitions",
    "cleanUpPolicy",
    "segmentSize",
)
GROUP_FIELDS = ("groupId", "state", "members", "topics", "consumerLag", "simple")
SCHEMA_FIELDS = ("subject", "version", "id", "schemaType", "compatibilityLevel")
BROKER_FIELDS = (
    "id",
    "host",
    "port",
    "partitions",
    "partitionsLeader",
    "inSyncPartitions",
    "partitionsSkew",
    "leadersSkew",
)
CONNECTOR_FIELDS = ("connect", "name", "connectorClass", "type", "topics", "status", "tasksCount", "failedTasksCount")
CONNECT_FIELDS = ("name", "address", "version", "connectorsCount", "failedConnectorsCount", "failedTasksCount")


async def optional(coro: Awaitable[Any]) -> Any:
    """A secondary lookup must not sink the whole tool: not every cluster has Connect, ACLs or log dirs."""
    try:
        return await coro
    except ToolError:
        return None


def build_server(kafbat: Kafbat) -> MCPServer:
    cfg = kafbat.cfg

    @asynccontextmanager
    async def lifespan(_server):
        task = asyncio.create_task(kafbat.keepalive()) if kafbat.cfg.keepalive_seconds > 0 else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await kafbat.http.aclose()

    mcp = MCPServer(
        "kafbat",
        version=__version__,
        instructions="Access to Kafka clusters through kafbat UI. Start with list_clusters to get cluster names. "
        "Reading messages never commits offsets. "
        + (
            "This server is in read-only mode: no tool can change cluster state."
            if cfg.read_only
            else "Write tools are enabled. Destructive tools (delete, offset reset) are "
            + ("also enabled." if cfg.allow_destructive else "NOT enabled.")
        ),
        lifespan=lifespan,
    )

    def permitted(name: str, read_only: bool, destructive: bool) -> bool:
        if read_only or name in cfg.enable_tools:  # a named tool is opted in explicitly, so it beats every tier
            return True
        if cfg.read_only:
            return False
        return cfg.allow_destructive or not destructive

    def tool(read_only: bool = True, destructive: bool = False):
        """Register a tool, but only if the configured tier admits it.

        The annotation is the gate — there is no second list of write-tool names to drift out of sync with
        the labels. A tool that is not registered is absent from `tools/list` *and* unknown to `tools/call`,
        because the SDK resolves calls through the same registry; hiding is the enforcement, not a veneer
        over it. structured_output=False everywhere: plain JSON text, no outputSchema.
        """
        annotations = ToolAnnotations(
            read_only_hint=read_only,
            destructive_hint=destructive,
            idempotent_hint=not destructive,
            open_world_hint=False,  # kafbat reaches only the clusters it is configured with
        )

        def register(fn):
            if not permitted(fn.__name__, read_only, destructive):
                return fn
            return mcp.tool(structured_output=False, annotations=annotations)(fn)

        return register

    async def paged(
        path: str,
        key: str,
        fields: tuple[str, ...],
        page: int,
        per_page: int,
        search: str | None = None,
        **extra: Any,
    ) -> str:
        params: dict[str, Any] = {"page": page, "perPage": per_page, **extra}
        if search:
            params["search"] = search
        data = await kafbat.get_json(path, params)
        return to_json(
            {
                "page": page,
                "pageCount": data.get("pageCount"),
                key: [pick(item, *fields) for item in data.get(key, [])],
            }
        )

    @tool()
    async def list_clusters() -> str:
        """List Kafka clusters configured in kafbat with status, versions and enabled features."""
        clusters = await kafbat.get_json("/api/clusters")
        return to_json([pick(c, *CLUSTER_FIELDS) for c in clusters])

    @tool()
    async def list_topics(
        cluster: str, search: str | None = None, show_internal: bool = False, page: int = 1, per_page: int = 100
    ) -> str:
        """List topics of a cluster (paged). `search` is a substring match on topic name."""
        return await paged(
            f"/api/clusters/{path_seg(cluster)}/topics",
            "topics",
            TOPIC_FIELDS,
            page,
            per_page,
            search,
            showInternal=str(show_internal).lower(),
        )

    @tool()
    async def describe_topic(cluster: str, topic: str) -> str:
        """Topic details: partitions with leader and min/max offsets, configs that differ from defaults,
        and the consumer groups reading this topic."""
        base = f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}"
        details, configs, groups = await asyncio.gather(
            kafbat.get_json(base),
            kafbat.get_json(f"{base}/config"),
            optional(kafbat.get_json(f"{base}/consumer-groups")),
        )
        details["nonDefaultConfig"] = non_default(configs)
        if groups is not None:  # an empty list is the useful answer "nobody reads this topic"
            details["consumerGroups"] = [pick(g, *GROUP_FIELDS) for g in groups]
        return to_json(details)

    @tool()
    async def list_consumer_groups(cluster: str, search: str | None = None, page: int = 1, per_page: int = 100) -> str:
        """List consumer groups of a cluster (paged) with state, member count and total lag."""
        return await paged(
            f"/api/clusters/{path_seg(cluster)}/consumer-groups/paged",
            "consumerGroups",
            GROUP_FIELDS,
            page,
            per_page,
            search,
        )

    @tool()
    async def describe_consumer_group(cluster: str, group_id: str) -> str:
        """Consumer group details: per-partition current offset, end offset, lag and assigned consumer."""
        return to_json(await kafbat.get_json(f"/api/clusters/{path_seg(cluster)}/consumer-groups/{path_seg(group_id)}"))

    @tool()
    async def consume_messages(
        cluster: str,
        topic: str,
        mode: Literal["LATEST", "EARLIEST", "FROM_OFFSET", "TO_OFFSET", "FROM_TIMESTAMP", "TO_TIMESTAMP"] = "LATEST",
        partitions: list[int] | None = None,
        offset: int | None = None,
        timestamp_ms: int | None = None,
        limit: int = 20,
        string_filter: str | None = None,
        smart_filter: str | None = None,
        key_serde: str | None = None,
        value_serde: str | None = None,
        max_value_chars: int = 2000,
        cursor: str | None = None,
    ) -> str:
        """Read messages from a topic without committing offsets.

        LATEST = newest `limit` messages, EARLIEST = oldest. FROM_/TO_OFFSET need `offset`,
        FROM_/TO_TIMESTAMP need `timestamp_ms` (epoch millis). Pass `cursor` from a previous
        `nextCursor` to get the next page (other params are then ignored). Long keys/values
        are truncated.

        Two filters, both applied by kafbat before sending anything back:
        `string_filter` keeps messages whose key/value/headers contain the string.
        `smart_filter` is a CEL boolean expression over a single binding, `record` — e.g.
        `record.value.orderId == 42`, `record.key == "k2"`. A JSON payload is addressed
        field by field through `record.value`; `record.keyAsText` and `record.valueAsText`
        give the raw strings. It is far more precise than `string_filter` and costs nothing
        extra in context.
        """
        if mode.endswith("_OFFSET") and offset is None and not cursor:
            raise ToolError(f"mode {mode} requires offset")
        if mode.endswith("_TIMESTAMP") and timestamp_ms is None and not cursor:
            raise ToolError(f"mode {mode} requires timestamp_ms")
        params: list[tuple[str, Any]] = [("mode", mode), ("limit", min(limit, MAX_MESSAGES))]
        params += [("partitions", p) for p in partitions or []]
        if smart_filter and not cursor:  # the cursor already carries the filter kafbat registered
            registered = await kafbat.post_json(
                f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}/smartfilters",
                {"filterCode": smart_filter},
            )
            params.append(("smartFilterId", registered["id"]))
        for name, value in (
            ("offset", offset),
            ("timestamp", timestamp_ms),
            ("stringFilter", string_filter),
            ("keySerde", key_serde),
            ("valueSerde", value_serde),
            ("cursor", cursor),
        ):
            if value is not None:
                params.append((name, value))
        path = f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}/messages/v2"
        resp = await kafbat.get(path, params)
        return to_json(parse_message_events(resp.text, max_value_chars))

    @tool()
    async def list_schemas(cluster: str, search: str | None = None, page: int = 1, per_page: int = 100) -> str:
        """List Schema Registry subjects (latest version metadata, without schema bodies)."""
        return await paged(
            f"/api/clusters/{path_seg(cluster)}/schemas", "schemas", SCHEMA_FIELDS, page, per_page, search
        )

    @tool()
    async def get_schema(cluster: str, subject: str, version: int | None = None) -> str:
        """Get a schema by subject: latest version by default, or a specific `version`.

        The response lists every registered version, so `version` never has to be guessed.
        """
        path = f"/api/clusters/{path_seg(cluster)}/schemas/{path_seg(subject)}"
        schema, versions = await asyncio.gather(
            kafbat.get_json(f"{path}/versions/{version}" if version is not None else f"{path}/latest"),
            optional(kafbat.get_json(f"{path}/versions")),
        )
        if versions is not None:
            schema["availableVersions"] = [v.get("version") for v in versions]
        return to_json(schema)

    @tool()
    async def cluster_health(cluster: str) -> str:
        """Cluster health: offline and under-replicated partition counts, the active controller, disk usage
        per broker, and each broker's partition load and skew. Start here when something looks cluster-wide."""
        base = f"/api/clusters/{path_seg(cluster)}"
        stats, brokers = await asyncio.gather(
            kafbat.get_json(f"{base}/stats"), optional(kafbat.get_json(f"{base}/brokers"))
        )
        return to_json({"stats": stats, "brokers": [pick(b, *BROKER_FIELDS) for b in brokers or []]})

    @tool()
    async def describe_broker(cluster: str, broker_id: int) -> str:
        """Broker config that differs from defaults, plus its log directories — a non-null `error` on a
        log dir means that disk has failed. Per-broker disk sizes live in `cluster_health`."""
        base = f"/api/clusters/{path_seg(cluster)}"
        configs, logdirs = await asyncio.gather(
            kafbat.get_json(f"{base}/brokers/{broker_id}/configs"),
            optional(kafbat.get_json(f"{base}/brokers/logdirs", {"broker": broker_id})),
        )
        return to_json(
            {
                "brokerId": broker_id,
                "nonDefaultConfig": non_default(configs),
                # Listing every topic-partition per log dir would dwarf the rest; the count and error suffice.
                "logDirs": [
                    {"name": d.get("name"), "error": d.get("error"), "topicCount": len(d.get("topics") or [])}
                    for d in logdirs or []
                ],
            }
        )

    @tool()
    async def list_connectors(cluster: str, search: str | None = None) -> str:
        """List Kafka Connect connectors across every Connect cluster, with state and failed-task counts."""
        base = f"/api/clusters/{path_seg(cluster)}"
        connectors = await kafbat.get_json(f"{base}/connectors", {"search": search} if search else None)
        out: dict[str, Any] = {"connectors": [pick(c, *CONNECTOR_FIELDS) for c in connectors]}
        if not connectors:
            # Empty means either "no connectors" or "no Connect configured"; the connects list tells them apart.
            connects = await optional(kafbat.get_json(f"{base}/connects"))
            out["connectClusters"] = [pick(c, *CONNECT_FIELDS) for c in connects or []]
        return to_json(out)

    @tool()
    async def describe_connector(cluster: str, connect: str, connector: str, max_trace_chars: int = 2000) -> str:
        """Connector state, config and per-task state, including the stack trace of a failed task — which is
        where the reason a connector stopped actually lives.

        Get `connect` (the Connect cluster) and `connector` from `list_connectors`. Config values whose key
        looks like a credential are masked.
        """
        base = f"/api/clusters/{path_seg(cluster)}/connects/{path_seg(connect)}/connectors/{path_seg(connector)}"
        # No separate /config call: kafbat's connector payload already embeds the config.
        info, tasks = await asyncio.gather(kafbat.get_json(base), optional(kafbat.get_json(f"{base}/tasks")))
        info["config"] = mask_secrets(info.get("config"))
        trim_trace(info.get("status"), max_trace_chars)
        for task in tasks or []:
            trim_trace(task.get("status"), max_trace_chars)
            task.pop("config", None)  # a copy of the connector config, per task
        info["tasks"] = tasks
        return to_json(info)

    @tool()
    async def list_acls(cluster: str) -> str:
        """Kafka ACLs as CSV (principal, host, resource, operation, permission) — kafbat's compact form."""
        resp = await kafbat.get(f"/api/clusters/{path_seg(cluster)}/acl/csv")
        return truncate(resp.text, MAX_ACL_CHARS) or ""

    @tool()
    async def whoami() -> str:
        """The kafbat user this server authenticates as, and its RBAC permissions per cluster and resource.
        Check here before concluding that a 403, or an empty list, is a kafbat fault."""
        return to_json(await kafbat.get_json("/api/authorization"))

    # ---- Additive writes: enabled by KAFBAT_READ_ONLY=false. Nothing here destroys existing data. ----

    @tool(read_only=False)
    async def produce_message(
        cluster: str,
        topic: str,
        partition: int = 0,
        key: str | dict | list | None = None,
        value: str | dict | list | None = None,
        headers: dict[str, str] | None = None,
        key_serde: str = "String",
        value_serde: str = "String",
    ) -> str:
        """Produce one message to a topic partition. `key` and `value` may be plain strings or JSON.

        Omit `value` to write a tombstone. Both serdes default to `String`, which covers text and JSON
        payloads; pass `SchemaRegistry` (or another serde kafbat offers) for a topic with a registered
        schema. kafbat rejects the request unless both are set, with the misleading "Value is undefined".
        """
        body: dict[str, Any] = {"partition": partition, "keySerde": key_serde, "valueSerde": value_serde}
        for name, val in (("key", as_text(key)), ("value", as_text(value)), ("headers", headers)):
            if val is not None:
                body[name] = val
        await kafbat.post(f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}/messages", body)
        return to_json({"produced": True, "topic": topic, "partition": partition})

    @tool(read_only=False)
    async def create_topic(
        cluster: str,
        topic: str,
        partitions: int = 1,
        replication_factor: int | None = None,
        configs: dict[str, str] | None = None,
    ) -> str:
        """Create a topic. `configs` takes raw Kafka topic settings, e.g. {"retention.ms": "604800000"}."""
        body: dict[str, Any] = {"name": topic, "partitions": partitions}
        if replication_factor is not None:
            body["replicationFactor"] = replication_factor
        if configs:
            body["configs"] = configs
        return to_json(await kafbat.post_json(f"/api/clusters/{path_seg(cluster)}/topics", body))

    @tool(read_only=False)
    async def update_connector_state(
        cluster: str,
        connect: str,
        connector: str,
        action: Literal["RESTART", "RESTART_ALL_TASKS", "RESTART_FAILED_TASKS", "PAUSE", "RESUME", "STOP"],
    ) -> str:
        """Restart, pause, resume or stop a connector. `RESTART_FAILED_TASKS` is the usual fix for the dead
        tasks that `describe_connector` reports."""
        base = f"/api/clusters/{path_seg(cluster)}/connects/{path_seg(connect)}/connectors/{path_seg(connector)}"
        await kafbat.post(f"{base}/action/{action}")
        return to_json({"connector": connector, "action": action, "applied": True})

    # ---- Destructive writes: additionally require KAFBAT_ALLOW_DESTRUCTIVE=true. These lose data. ----

    @tool(read_only=False, destructive=True)
    async def reset_consumer_group_offsets(
        cluster: str,
        group_id: str,
        topic: str,
        reset_type: Literal["EARLIEST", "LATEST", "TIMESTAMP", "OFFSET"],
        partitions: list[int] | None = None,
        offset: int | None = None,
        timestamp_ms: int | None = None,
    ) -> str:
        """Reset a consumer group's committed offsets for one topic. The group must have no active members,
        or kafbat rejects it. Omit `partitions` to reset every partition; OFFSET needs both `partitions`
        and `offset`, TIMESTAMP needs `timestamp_ms`."""
        if reset_type == "OFFSET" and (offset is None or not partitions):
            raise ToolError("reset_type OFFSET requires both partitions and offset")
        if reset_type == "TIMESTAMP" and timestamp_ms is None:
            raise ToolError("reset_type TIMESTAMP requires timestamp_ms")
        if not partitions:
            # kafbat accepts an absent `partitions` and then resets nothing, still answering 200, so the
            # list is always sent explicitly. Verified against v1.5.0 in the integration tests.
            details = await kafbat.get_json(f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}")
            partitions = sorted(p["partition"] for p in details.get("partitions") or [])
        body: dict[str, Any] = {"topic": topic, "resetType": reset_type, "partitions": partitions}
        if reset_type == "OFFSET":
            body["partitionsOffsets"] = [{"partition": p, "offset": offset} for p in partitions or []]
        if timestamp_ms is not None:
            body["resetToTimestamp"] = timestamp_ms
        path = f"/api/clusters/{path_seg(cluster)}/consumer-groups/{path_seg(group_id)}/offsets"
        await kafbat.post(path, body)
        return to_json({"group": group_id, "topic": topic, "resetType": reset_type, "reset": True})

    @tool(read_only=False, destructive=True)
    async def delete_topic(cluster: str, topic: str) -> str:
        """Permanently delete a topic and every message in it. This cannot be undone."""
        await kafbat.delete(f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}")
        return to_json({"topic": topic, "deleted": True})

    return mcp


def main() -> None:
    try:
        cfg = Config.from_env()
    except ValueError as e:
        print(f"kafbat-mcp: {e}", file=sys.stderr)
        sys.exit(2)
    logging.basicConfig(
        stream=sys.stderr, level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx2").setLevel(logging.WARNING)  # httpx2 logs every request at INFO
    build_server(Kafbat(cfg)).run("stdio")
