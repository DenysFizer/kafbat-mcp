# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Dependabot for Python dependencies (`uv.lock`) and GitHub Actions, weekly, minor and patch bumps grouped.
- Issue templates for bugs and feature requests, `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`.
- Dependency review on every pull request: one that adds a dependency with a known vulnerability of moderate
  severity or worse fails before it reaches `main`.
- A CI `build` job: the package must build, pass `twine check --strict`, install, and start.

### Changed
- GitHub Actions are pinned to commit SHAs instead of tags, which can be moved to other code; Dependabot
  keeps the pins current.
- README, `SECURITY.md` and `CONTRIBUTING.md` reviewed against the code: they no longer describe only the
  browser-cookie login, claim that every request is a `GET`, or misstate how kafbat's own MCP server handles
  RBAC; the tools table gives the real Connect paths; Limitations and Troubleshooting cover what the
  integration tests found.

## [0.2.0] - 2026-09-22

### Added
- Write tools, **off by default**, in two tiers: `KAFBAT_READ_ONLY=false` registers `produce_message`,
  `create_topic` and `update_connector_state`; `KAFBAT_ALLOW_DESTRUCTIVE=true` additionally registers
  `delete_topic` and `reset_consumer_group_offsets`. `KAFBAT_ENABLE_TOOLS` admits named tools regardless
  of tier, so read-only need not be abandoned to recover one tool. `produce_message` always sends both serdes
  (default `String`), and `reset_consumer_group_offsets` resolves the topic's partitions when none are given:
  without them kafbat rejects the first and silently resets nothing in the second.
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
- Integration tests against a real kafbat UI, Kafka, Schema Registry and Kafka Connect in Docker via
  testcontainers, covering every tool except `list_acls`, all three auth modes and the write tiers. Opt-in with
  `KAFBAT_INTEGRATION=1`; the default `pytest` run stays offline and takes ~2s. `KAFBAT_IMAGES` re-runs the suite
  against several kafbat releases, sharing Kafka, Schema Registry and Connect between them. Verified on v1.3.0,
  v1.4.2, v1.5.0 (= `latest`) and `main`.
- CI runs the integration suite against kafbat v1.3.0, v1.4.2, v1.5.0 and `main` on every pull request and
  weekly, one parallel job per release; a failure on upstream `main` is reported but does not block.
- Publishing a GitHub release uploads to PyPI through trusted publishing — no API token is stored anywhere.
  The job refuses to publish if the release tag does not match the `pyproject.toml` version.

### Fixed
- kafbat instances with `auth.type: DISABLED` — kafbat's default — are now reachable. Previously the server had
  no way to authenticate without a browser cookie, so it opened a browser and failed after the login timeout.

### Changed
- Use `httpx2` (already required by `mcp`) instead of the unmaintained `httpx` 0.28 line — one HTTP client instead
  of two.
- Formatting helpers moved to `kafbat_mcp.formatting`; the paged tools share one helper. No behaviour change.
- README: logo, one-click install buttons for VS Code and Cursor, GitHub alerts, unified badges.
- `KAFBAT_LOGIN_WAIT_SECONDS` now defaults to 45s so a slow re-login fails with our own message instead of hitting
  the client's 60s tool timeout.

## [0.1.0] - 2026-09-18

### Added
- Read-only tools: `list_clusters`, `list_topics`, `describe_topic`, `list_consumer_groups`,
  `describe_consumer_group`, `consume_messages`, `list_schemas`, `get_schema`.
- Authentication with the kafbat session cookie read from a local browser profile (Chromium-based browsers,
  Firefox, Safari via yt-dlp), with automatic re-login through the browser and a keepalive.
- Login page opens in the configured Chromium profile (`--profile-directory`), overridable with `KAFBAT_OPEN_COMMAND`.
- Short error for non-JSON responses from proxies in front of kafbat (e.g. Cloudflare 403) instead of raw HTML.
- Configuration through `KAFBAT_*` environment variables.
