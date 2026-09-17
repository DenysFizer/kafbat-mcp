"""MCP tools over kafbat's REST API. Every tool is a GET; nothing here can change cluster state."""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager, suppress
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from kafbat_mcp import __version__
from kafbat_mcp.client import Kafbat
from kafbat_mcp.config import Config
from kafbat_mcp.formatting import parse_message_events, path_seg, pick, to_json

MAX_MESSAGES = 500

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


def build_server(kafbat: Kafbat) -> MCPServer:
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
        instructions="Read-only access to Kafka clusters through kafbat UI. "
        "Start with list_clusters to get cluster names.",
        lifespan=lifespan,
    )

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

    # structured_output=False everywhere: plain JSON text, no outputSchema.
    @mcp.tool(structured_output=False)
    async def list_clusters() -> str:
        """List Kafka clusters configured in kafbat with status, versions and enabled features."""
        clusters = await kafbat.get_json("/api/clusters")
        return to_json([pick(c, *CLUSTER_FIELDS) for c in clusters])

    @mcp.tool(structured_output=False)
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

    @mcp.tool(structured_output=False)
    async def describe_topic(cluster: str, topic: str) -> str:
        """Topic details: partitions with leader and min/max offsets, plus configs that differ from defaults."""
        base = f"/api/clusters/{path_seg(cluster)}/topics/{path_seg(topic)}"
        details, configs = await asyncio.gather(kafbat.get_json(base), kafbat.get_json(f"{base}/config"))
        details["nonDefaultConfig"] = {
            c["name"]: c.get("value")
            for c in configs
            if c.get("source") != "DEFAULT_CONFIG" and not c.get("isSensitive")
        }
        return to_json(details)

    @mcp.tool(structured_output=False)
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

    @mcp.tool(structured_output=False)
    async def describe_consumer_group(cluster: str, group_id: str) -> str:
        """Consumer group details: per-partition current offset, end offset, lag and assigned consumer."""
        return to_json(await kafbat.get_json(f"/api/clusters/{path_seg(cluster)}/consumer-groups/{path_seg(group_id)}"))

    @mcp.tool(structured_output=False)
    async def consume_messages(
        cluster: str,
        topic: str,
        mode: Literal["LATEST", "EARLIEST", "FROM_OFFSET", "TO_OFFSET", "FROM_TIMESTAMP", "TO_TIMESTAMP"] = "LATEST",
        partitions: list[int] | None = None,
        offset: int | None = None,
        timestamp_ms: int | None = None,
        limit: int = 20,
        string_filter: str | None = None,
        key_serde: str | None = None,
        value_serde: str | None = None,
        max_value_chars: int = 2000,
        cursor: str | None = None,
    ) -> str:
        """Read messages from a topic without committing offsets.

        LATEST = newest `limit` messages, EARLIEST = oldest. FROM_/TO_OFFSET need `offset`,
        FROM_/TO_TIMESTAMP need `timestamp_ms` (epoch millis). `string_filter` keeps messages
        whose key/value/headers contain the string. Pass `cursor` from a previous `nextCursor`
        to get the next page (other params are then ignored). Long keys/values are truncated.
        """
        if mode.endswith("_OFFSET") and offset is None and not cursor:
            raise ToolError(f"mode {mode} requires offset")
        if mode.endswith("_TIMESTAMP") and timestamp_ms is None and not cursor:
            raise ToolError(f"mode {mode} requires timestamp_ms")
        params: list[tuple[str, Any]] = [("mode", mode), ("limit", min(limit, MAX_MESSAGES))]
        params += [("partitions", p) for p in partitions or []]
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

    @mcp.tool(structured_output=False)
    async def list_schemas(cluster: str, search: str | None = None, page: int = 1, per_page: int = 100) -> str:
        """List Schema Registry subjects (latest version metadata, without schema bodies)."""
        return await paged(
            f"/api/clusters/{path_seg(cluster)}/schemas", "schemas", SCHEMA_FIELDS, page, per_page, search
        )

    @mcp.tool(structured_output=False)
    async def get_schema(cluster: str, subject: str, version: int | None = None) -> str:
        """Get a schema by subject: latest version by default, or a specific `version`."""
        path = f"/api/clusters/{path_seg(cluster)}/schemas/{path_seg(subject)}"
        return to_json(await kafbat.get_json(f"{path}/versions/{version}" if version is not None else f"{path}/latest"))

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
