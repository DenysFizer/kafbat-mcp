# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-09-22

### Added
- Write tools, **off by default**, in two tiers: `KAFBAT_READ_ONLY=false` registers `produce_message`,
  `create_topic` and `update_connector_state`; `KAFBAT_ALLOW_DESTRUCTIVE=true` additionally registers
  `delete_topic` and `reset_consumer_group_offsets`. `KAFBAT_ENABLE_TOOLS` admits named tools regardless
  of tier, so read-only need not be abandoned to recover one tool.
- Every tool now declares MCP annotations, and the tiers filter on those annotations rather than on a
  separate list of tool names, so a tool's label and its gate cannot disagree. A tool the tier excludes is
  not registered, so it is absent from `tools/list` and unknown to `tools/call`.
- Brokers and cluster health: `cluster_health` (offline/under-replicated partitions, active controller, disk
  usage, per-broker load) and `describe_broker` (non-default config, log dirs with failed disks flagged).
- Kafka Connect: `list_connectors` and `describe_connector`, the latter carrying per-task failure traces.
- `list_acls` (kafbat's CSV form) and `whoami` (authenticated user and RBAC permissions).
- `describe_topic` now lists the consumer groups reading the topic; `get_schema` lists every available version,
  so `version` no longer has to be guessed.
- `consume_messages` gained `smart_filter`, a CEL predicate kafbat evaluates server-side over a `record`
  binding, e.g. `record.value.orderId == 42`.
- `KAFBAT_AUTH` selects the authentication strategy: `cookie` (the previous behaviour, for browser SSO),
  `none` (kafbat's `DISABLED`) and `form` (`KAFBAT_USERNAME` / `KAFBAT_PASSWORD`, serving both `LOGIN_FORM`
  and `LDAP` — they share one Spring chain). Bearer tokens are deliberately not supported: kafbat only accepts
  them with `auth.oauth2.resourceServer.*` configured, and with RBAC enabled they make `validateAccess` fail
  open while list endpoints return nothing.

### Fixed
- `produce_message` now always sends `keySerde`/`valueSerde` (default `String`). kafbat rejects the request
  with a misleading `500 Value is undefined` when either is absent — found by the new integration tests.
- `reset_consumer_group_offsets` now always sends an explicit `partitions` list, resolving it from the topic
  when the caller omits it. kafbat answers `200` to a reset with no `partitions` and then resets nothing, so
  the tool used to report success for work that never happened.
- `consume_messages` documented `smart_filter` as Groovy with `key`/`value` bindings, which is the old
  provectus dialect. kafbat evaluates **CEL** over a single `record` binding
  (`record.value.orderId == 42`, `record.keyAsText`, …); the old form failed to compile server-side.
- kafbat instances with `auth.type: DISABLED` — kafbat's default — are now reachable. Previously the server had
  no way to authenticate without a browser cookie, so it opened a browser and failed after the login timeout.

### Added (release)
- CI runs the integration suite against kafbat v1.3.0, v1.4.2, v1.5.0 and `main` on every pull request and
  weekly, one parallel job per release; a failure on upstream `main` is reported but does not block.
- Publishing a GitHub release uploads to PyPI through trusted publishing — no API token is stored anywhere.
  The job refuses to publish if the release tag does not match the `pyproject.toml` version.

### Added (testing)
- Integration tests against a real kafbat UI and Kafka in Docker via testcontainers, covering reads, the
  create/produce/consume round trip, smart filters, topic deletion, consumer-group offset resets, the write
  tiers and form login. The offset-reset test builds a real, then inactive, consumer group with the broker's
  own console consumer, so no Kafka client library is needed. Opt-in with `KAFBAT_INTEGRATION=1`; the default
  `pytest` run stays offline and takes ~2s. `KAFBAT_IMAGES` re-runs the suite against several kafbat
  releases; only the kafbat container is parametrised, so each extra release costs one boot rather than
  a whole stack. Verified green on v1.3.0, v1.4.2, v1.5.0 (= `latest`) and `main`.

### Changed
- Use `httpx2` (already required by `mcp`) instead of the unmaintained `httpx` 0.28 line — one HTTP client instead
  of two.
- Formatting helpers moved to `kafbat_mcp.formatting`; the paged tools share one helper. No behaviour change.
- README: logo, one-click install buttons for VS Code and Cursor, GitHub alerts, unified badges.
- `KAFBAT_LOGIN_WAIT_SECONDS` now defaults to 45s so a slow re-login fails with our own message instead of hitting
  the client's 60s tool timeout.

## [0.1.0]

### Added
- Read-only tools: `list_clusters`, `list_topics`, `describe_topic`, `list_consumer_groups`,
  `describe_consumer_group`, `consume_messages`, `list_schemas`, `get_schema`.
- Authentication with the kafbat session cookie read from a local browser profile (Chromium-based browsers,
  Firefox, Safari via yt-dlp), with automatic re-login through the browser and a keepalive.
- Login page opens in the configured Chromium profile (`--profile-directory`), overridable with `KAFBAT_OPEN_COMMAND`.
- Short error for non-JSON responses from proxies in front of kafbat (e.g. Cloudflare 403) instead of raw HTML.
- Configuration through `KAFBAT_*` environment variables.
