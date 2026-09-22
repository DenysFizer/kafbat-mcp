# Contributing

Thanks for helping. Bug reports and feature requests go through the
[issue templates](https://github.com/DenysFizer/kafbat-mcp/issues/new/choose); security issues go to a
[private advisory](https://github.com/DenysFizer/kafbat-mcp/security/advisories/new) instead.

## Setup

Fork the repository on GitHub, then:

```bash
git clone https://github.com/<you>/kafbat-mcp && cd kafbat-mcp
uv sync
```

## Tests

```bash
uv run pytest                  # offline: a fake kafbat behind a mock transport, ~2s, no Docker

# integration: real kafbat UI + Kafka + Schema Registry + Connect in Docker, ~2 min once images are cached
KAFBAT_INTEGRATION=1 uv run pytest tests/test_integration.py

# the same suite against several kafbat releases (only the kafbat container varies, the rest is shared)
KAFBAT_INTEGRATION=1 KAFBAT_IMAGES=ghcr.io/kafbat/kafka-ui:v1.3.0,ghcr.io/kafbat/kafka-ui:main \
  uv run pytest tests/test_integration.py
```

CI runs `uv run ruff check`, the offline suite on Python 3.10 and 3.13, and the integration suite against
kafbat v1.3.0, v1.4.2, v1.5.0 and `main`.

## Pull requests

`main` is protected: changes land through a pull request — from a branch in your fork, or in this repository if
you have write access — and it merges once the required checks are green.
Add a line under `## [Unreleased]` in `CHANGELOG.md` for anything a user would notice.

## Adding or changing a tool

- **Verify against a real kafbat, not just the OpenAPI spec.** The spec on kafbat's `main` has disagreed with
  released servers more than once: `produce` needs both serdes or it fails with a misleading
  `Value is undefined`, an offset reset without `partitions` answers `200` and resets nothing, and smart
  filters are CEL over `record`, not Groovy. The offline suite pins how a request is shaped; only an
  integration test proves kafbat accepts it. Every new endpoint needs one.
- **Register tools through `tool()` in `server.py`, never `mcp.tool()` directly.** Its `read_only` and
  `destructive` arguments become the MCP annotations *and* decide whether the configured tier registers the
  tool at all. A tool that changes cluster state must say so, or it will be exposed in read-only mode.
- **Keep responses small.** Tool output goes into a model's context: pick the useful fields with `pick()`,
  let `to_json()` drop nulls, and truncate anything unbounded (payloads, stack traces, CSV).
- **Never return secrets.** Drop configs kafbat flags as sensitive, and mask credential-looking keys where
  kafbat has no flag, as `describe_connector` does.
