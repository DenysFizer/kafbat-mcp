<div align="center">

<!-- absolute URL: PyPI renders this README too, and relative paths don't resolve there -->
<img src="https://raw.githubusercontent.com/DenysFizer/kafbat-mcp/main/assets/logo.svg" alt="" width="300">

# kafbat-mcp

**MCP server for kafbat UI — browse Kafka from your AI client,<br>using the session you already have in your browser. Read-only by default.**

[![CI](https://github.com/DenysFizer/kafbat-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/DenysFizer/kafbat-mcp/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python&logoColor=white)
[![MCP](https://img.shields.io/badge/MCP-server-orange?style=flat-square)](https://modelcontextprotocol.io)

</div>

Browse topics, consumer group lag, messages and schemas through [kafbat UI](https://github.com/kafbat/kafka-ui) (the
maintained fork of Kafka UI) from any [MCP](https://modelcontextprotocol.io) client — Claude Code, Claude Desktop,
Cursor, VS Code and others. No broker credentials, no service account, nothing to change on the kafbat side: the
server reuses the kafbat session from your own browser, so it sees exactly what you can see.

> [!NOTE]
> Not affiliated with or endorsed by the kafbat project.

## Contents

[Why kafbat-mcp](#why-kafbat-mcp) · [Quick start](#quick-start) · [Tools](#tools) ·
[Write access](#write-access) · [Authentication](#authentication) · [How it works](#how-it-works) ·
[Configuration](#configuration) · [Security](#security) · [Requirements](#requirements-and-platform-support) ·
[Troubleshooting](#troubleshooting) · [Limitations](#limitations) · [Development](#development) · [License](#license)

## Why kafbat-mcp

kafbat ships its own MCP server, but it is **off by default** (`mcp.enabled`), so enabling it needs a config change
and a redeploy from whoever runs kafbat. It also runs server-side with kafbat's own Kafka credentials, so it cannot
reflect *your* RBAC permissions, and forwarding a browser session into it is not supported for SSO setups.

It is also all-or-nothing (as of v1.5.0). Its tool generator filters on exactly one thing — `@Deprecated` — so
every controller method becomes a tool, including `deleteTopic`, `deleteTopicMessages`, `executeKsql` and
`resetConsumerGroupOffsets`.
There is no read-only switch and no `readOnlyHint` annotations. The per-cluster `readOnly` flag does not help
either: it is enforced by a Spring `WebFilter`, and MCP calls reach the controllers by reflection without passing
through the filter chain. That was fixed on `main` in [#1766](https://github.com/kafbat/kafka-ui/pull/1766), but it
is not in any release as of v1.5.0 — so on released kafbat, `mcp.enabled=true` grants any connected model
unrestricted deletes, on read-only clusters too, unless RBAC happens to be configured.

kafbat-mcp runs on your machine instead. If you can open kafbat in your browser, your MCP client can use it within
your own kafbat permissions — and it is **read-only by default**, with writes behind two explicit opt-in flags.

## Quick start

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/). Not on PyPI yet, so `uvx` installs it
straight from GitHub, pinned to a release tag — change the tag to upgrade.

[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_server-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=kafbat&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22--from%22%2C%22git%2Bhttps%3A%2F%2Fgithub.com%2FDenysFizer%2Fkafbat-mcp%40v0.2.0%22%2C%22kafbat-mcp%22%5D%2C%22env%22%3A%7B%22KAFBAT_URL%22%3A%22https%3A%2F%2Fkafka.example.com%22%2C%22KAFBAT_BROWSER%22%3A%22chrome%22%7D%7D)
[![Install in Cursor](https://img.shields.io/badge/Cursor-Install_server-000000?style=flat-square&logo=cursor&logoColor=white)](https://cursor.com/en/install-mcp?name=kafbat&config=eyJjb21tYW5kIjoidXZ4IiwiYXJncyI6WyItLWZyb20iLCJnaXQraHR0cHM6Ly9naXRodWIuY29tL0RlbnlzRml6ZXIva2FmYmF0LW1jcEB2MC4yLjAiLCJrYWZiYXQtbWNwIl0sImVudiI6eyJLQUZCQVRfVVJMIjoiaHR0cHM6Ly9rYWZrYS5leGFtcGxlLmNvbSIsIktBRkJBVF9CUk9XU0VSIjoiY2hyb21lIn19)

Both buttons install with the placeholder `https://kafka.example.com` — change `KAFBAT_URL` to your instance
afterwards. For Claude Code:

```bash
claude mcp add -s user kafbat \
  -e KAFBAT_URL=https://kafka.example.com \
  -e KAFBAT_BROWSER=chrome \
  -- uvx --from git+https://github.com/DenysFizer/kafbat-mcp@v0.2.0 kafbat-mcp
```

Then ask your client something like *"which consumer groups on DEV have lag?"* or *"show the last 5 messages on
orders.v1"*.

> [!TIP]
> The default install is read-only, so you can add `"mcp__kafbat__*"` to `permissions.allow` in
> `~/.claude/settings.json` to skip approval prompts. Narrow that rule first if you enable [write
> access](#write-access) — Claude Code matches on tool names, not on MCP annotations.

### Other MCP clients

<details>
<summary><b>Claude Desktop</b> — <code>claude_desktop_config.json</code></summary>

```json
{
  "mcpServers": {
    "kafbat": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp@v0.2.0", "kafbat-mcp"],
      "env": { "KAFBAT_URL": "https://kafka.example.com", "KAFBAT_BROWSER": "chrome" }
    }
  }
}
```
</details>

<details>
<summary><b>Cursor</b> — <code>~/.cursor/mcp.json</code></summary>

```json
{
  "mcpServers": {
    "kafbat": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp@v0.2.0", "kafbat-mcp"],
      "env": { "KAFBAT_URL": "https://kafka.example.com", "KAFBAT_BROWSER": "chrome" }
    }
  }
}
```
</details>

<details>
<summary><b>VS Code</b> — <code>.vscode/mcp.json</code></summary>

```json
{
  "servers": {
    "kafbat": {
      "type": "stdio",
      "command": "uvx",
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp@v0.2.0", "kafbat-mcp"],
      "env": { "KAFBAT_URL": "https://kafka.example.com", "KAFBAT_BROWSER": "chrome" }
    }
  }
}
```
</details>

---

## Tools

| Tool | What it returns | kafbat endpoint |
|---|---|---|
| `list_clusters` | Clusters with status, broker/topic counts, features | `GET /api/clusters` |
| `list_topics` | Paged topic list, optional name search | `GET /api/clusters/{cluster}/topics` |
| `describe_topic` | Partitions, offsets, non-default configs | `GET …/topics/{topic}` + `…/config` |
| `list_consumer_groups` | Paged groups with state, members, total lag | `GET …/consumer-groups/paged` |
| `describe_consumer_group` | Per-partition offsets and lag | `GET …/consumer-groups/{id}` |
| `consume_messages` | Messages by `LATEST`/`EARLIEST`/offset/timestamp, with string or CEL smart filters and cursor paging; long values truncated | `GET …/topics/{topic}/messages/v2` |
| `list_schemas` | Schema Registry subjects (metadata only) | `GET …/schemas` |
| `get_schema` | Latest or a specific schema version, plus every available version | `GET …/schemas/{subject}/latest` + `…/versions` |
| `cluster_health` | Offline/under-replicated partitions, active controller, disk usage, per-broker load | `GET …/stats` + `…/brokers` |
| `describe_broker` | Non-default broker config and log dirs, with failed disks flagged | `GET …/brokers/{id}/configs` + `…/brokers/logdirs` |
| `list_connectors` | Connectors across all Connect clusters with state and failed-task counts | `GET …/connectors` |
| `describe_connector` | Connector state, masked config, and per-task failure traces | `GET …/connectors/{name}` + `…/tasks` |
| `list_acls` | ACLs as CSV | `GET …/acl/csv` |
| `whoami` | The authenticated user and its RBAC permissions | `GET /api/authorization` |

**Write tools**, off by default — see [Write access](#write-access):

| Tool | Tier | kafbat endpoint |
|---|---|---|
| `produce_message` | additive | `POST …/topics/{topic}/messages` |
| `create_topic` | additive | `POST …/topics` |
| `update_connector_state` | additive | `POST …/connectors/{name}/action/{action}` |
| `reset_consumer_group_offsets` | destructive | `POST …/consumer-groups/{id}/offsets` |
| `delete_topic` | destructive | `DELETE …/topics/{topic}` |

<i>Responses are compact JSON with <code>null</code> fields removed, to keep context small.</i>

## Write access

Three tiers. **The default is read-only** — a fresh install cannot change anything in your clusters.

| | `KAFBAT_READ_ONLY` | `KAFBAT_ALLOW_DESTRUCTIVE` | Tools registered |
|---|---|---|---|
| Read-only *(default)* | `true` | `false` | reads only |
| Additive writes | `false` | `false` | + produce, create topic, connector actions |
| Everything | `false` | `true` | + delete topic, reset offsets |

A tool the tier does not admit is **not registered**: it is absent from `tools/list`, and calling it by name
fails with `Unknown tool`, because the MCP SDK resolves calls through the same registry. There is no second
list of write-tool names — the tier filters on each tool's own `readOnlyHint` / `destructiveHint` annotation,
so a tool's label and its gate cannot disagree.

`KAFBAT_ENABLE_TOOLS` is the escape hatch: a comma-separated list of tool names admitted whatever the tier, so
you need not abandon read-only mode to recover one tool.

```bash
KAFBAT_READ_ONLY=false                      # allow produce/create/restart
KAFBAT_READ_ONLY=false KAFBAT_ALLOW_DESTRUCTIVE=true
KAFBAT_ENABLE_TOOLS=produce_message         # otherwise still read-only
```

> [!IMPORTANT]
> These flags are a guardrail, not a security boundary — they live in the same process as the tools they gate.
> The real boundary is the credential: kafbat's RBAC and the cluster's `readOnly` flag. `whoami` reports what
> your session is actually allowed to do.

## Authentication

kafbat has four mutually exclusive `auth.type` settings. Pick the matching `KAFBAT_AUTH` — there is no
autodetection, because an env var never guesses wrong.

| kafbat `auth.type` | `KAFBAT_AUTH` | What kafbat-mcp sends | Also set |
|---|---|---|---|
| `OAUTH2` (browser SSO) | `cookie` *(default)* | `SESSION` cookie lifted from your browser | `KAFBAT_BROWSER`, `KAFBAT_BROWSER_PROFILE` |
| `DISABLED` | `none` | nothing | — |
| `LOGIN_FORM` | `form` | `POST /login`, then the `SESSION` cookie it returns | `KAFBAT_USERNAME`, `KAFBAT_PASSWORD` |
| `LDAP` / Active Directory | `form` | same — kafbat uses one Spring `formLogin` chain for both | `KAFBAT_USERNAME`, `KAFBAT_PASSWORD` |

For an `OAUTH2` deployment, `cookie` is the only supported path. Bearer tokens are not: kafbat accepts them only
when the operator sets `auth.oauth2.resourceServer.*`, and even then it wraps principals into `RbacUser` solely in
the `oauth2Login` flow — so with RBAC on, a bearer request carries no user, list endpoints come back empty and
`validateAccess` fails *open*. Until that upstream bug is fixed, a bearer token is not a safe credential here.

kafbat never configures `httpBasic()`, so `Authorization: Basic` cannot work with any auth type. There is no
API-key, service-account or personal-access-token mechanism upstream.

## How it works

1. On the first tool call the server obtains credentials for the configured `KAFBAT_AUTH` strategy and validates
   them against `GET /api/clusters`. For `cookie` that means reading kafbat's `SESSION` cookie from your browser
   profile with [yt-dlp](https://github.com/yt-dlp/yt-dlp)'s extraction, which decrypts it via your OS keyring.
2. It calls kafbat's REST API with those credentials — `GET` only, redirects not followed.
3. On a 401 or a redirect to the login page it re-authenticates. In `cookie` mode, if the browser's cookie is
   stale too, it opens kafbat in the configured browser profile, waits for the new cookie (an SSO login usually
   completes on its own) and retries.
4. While your MCP client is running, a periodic request keeps the session from idling out.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFBAT_URL` | **required** | Base URL of kafbat, e.g. `https://kafka.example.com` |
| `KAFBAT_READ_ONLY` | `true` | `false` registers the additive write tools |
| `KAFBAT_ALLOW_DESTRUCTIVE` | `false` | `true` also registers delete/reset tools; needs `KAFBAT_READ_ONLY=false` |
| `KAFBAT_ENABLE_TOOLS` | — | Comma-separated tool names admitted regardless of the tier |
| `KAFBAT_AUTH` | `cookie` | `cookie`, `none`, `form` — see [Authentication](#authentication) |
| `KAFBAT_USERNAME` / `KAFBAT_PASSWORD` | — | Required for `KAFBAT_AUTH=form` |
| `KAFBAT_BROWSER` | `chrome` | `chrome`, `chromium`, `brave`, `edge`, `opera`, `vivaldi`, `whale`, `firefox`, `safari` |
| `KAFBAT_BROWSER_PROFILE` | browser default | Profile name (`Profile 1`) or path to the profile directory |
| `KAFBAT_KEYRING` | auto-detect | Linux only: `GNOMEKEYRING`, `KWALLET`, `KWALLET5`, `KWALLET6`, `BASICTEXT` |
| `KAFBAT_COOKIE_NAME` | `SESSION` | Session cookie name (Spring's default) |
| `KAFBAT_KEEPALIVE_SECONDS` | `600` | Keepalive interval; `0` disables it |
| `KAFBAT_AUTO_OPEN` | `true` | Open kafbat in the browser when the session is gone |
| `KAFBAT_OPEN_COMMAND` | auto | Command used to open the login page; the URL is appended |
| `KAFBAT_LOGIN_WAIT_SECONDS` | `45` | How long to wait for a fresh cookie after opening the browser |
| `KAFBAT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` (logs go to stderr) |

## Security

Credentials — the session cookie or your `KAFBAT_PASSWORD` — stay in memory and are sent
only to `KAFBAT_URL`; redirects are not followed, so nothing leaks to an identity provider you did not configure.

> [!IMPORTANT]
> By default no tool can change cluster state; see [Write access](#write-access) before enabling the write
> tiers. Either way, message payloads, configs and ACLs do reach your MCP client and model. Read
> [SECURITY.md](SECURITY.md) before pointing this at clusters with sensitive data.

Sensitive config values are withheld: broker and topic entries kafbat flags as sensitive are dropped, and Connect
configs — a free-form map with no such flag — have values masked when the key mentions a password, secret, token
or credential.

## Requirements and platform support

- [uv](https://docs.astral.sh/uv/getting-started/installation/), Python 3.10+
- A kafbat instance you can log in to in a local browser
- **Linux:** tested (GNOME Keyring, Firefox). **macOS:** should work (Keychain), untested.
  **Windows:** not supported for Chromium browsers (App-Bound Encryption).
- **kafbat v1.3.0 through `main`.** CI runs the integration suite against v1.3.0, v1.4.2, v1.5.0 and `main`
  every week, so an upstream change shows up here before it reaches you.

---

## Troubleshooting

<details>
<summary><code>No valid kafbat session</code> / <code>No kafbat session after 45s</code></summary>

Log in to kafbat in the browser and profile you configured. Chromium browsers write cookies to disk about every
30 seconds, so a fresh login can take up to half a minute to be picked up. If the login tab opened in the wrong
browser or profile, see the next section.

The first call after a long break is the one that pays for the re-login, and MCP clients cap a single tool call
(Claude Code: `MCP_TOOL_TIMEOUT`, 60s by default). If a login needs longer than that, simply repeat the request —
the session obtained in the background is reused. To wait it out inside one call instead, raise both
`MCP_TOOL_TIMEOUT` and `KAFBAT_LOGIN_WAIT_SECONDS`.
</details>

<details>
<summary>Multiple browser profiles (e.g. personal and work Chrome windows)</summary>

Cookies are read only from `KAFBAT_BROWSER_PROFILE`, so other open profiles don't interfere. For the login page:

- If `KAFBAT_BROWSER_PROFILE` is a profile *name* and the browser executable is on `PATH` (`google-chrome`,
  `chromium`, `brave-browser`, `microsoft-edge`, `vivaldi`, `opera`), the page opens with
  `--profile-directory=<profile>`, i.e. in that profile even if another one was used last.
- Otherwise it falls back to the OS default handler (`xdg-open`/`open`), which uses whatever browser and profile were
  active last.
- For Flatpak/Snap installs, macOS, or a profile given as a path, set the command explicitly:
  ```bash
  KAFBAT_OPEN_COMMAND="flatpak run com.google.Chrome --profile-directory='Profile 2'"
  KAFBAT_OPEN_COMMAND="open -na 'Google Chrome' --args --profile-directory='Profile 2'"   # macOS
  ```

The command used is logged at `INFO` level. To list which Chromium profile belongs to which account:

```bash
python3 -c "import json,os; s=json.load(open(os.path.expanduser('~/.config/google-chrome/Local State'))); [print(repr(k), v.get('user_name')) for k,v in s['profile']['info_cache'].items()]"
```
</details>

<details>
<summary><code>failed to decrypt cookie</code> warnings (Linux)</summary>

The keyring couldn't be read. Make sure a desktop keyring (GNOME Keyring or KWallet) is unlocked, or set
`KAFBAT_KEYRING` explicitly. Run with `KAFBAT_LOG_LEVEL=DEBUG` to see which keyring was chosen.
</details>

<details>
<summary>Firefox: session never found</summary>

Firefox may keep session cookies (no expiry) out of `cookies.sqlite`, so the session may not be found. Use a
Chromium-based browser if that happens.
</details>

<details>
<summary><code>kafbat unreachable</code> or <code>HTTP 403 … non-JSON text/html</code></summary>

The instance isn't reachable from your machine, or a proxy in front of kafbat (e.g. Cloudflare) blocked the request.
Check your VPN or network.
</details>

<details>
<summary><code>spawn uvx ENOENT</code> / the server fails to start</summary>

`uvx` isn't on the `PATH` your MCP client sees — use the absolute path (`which uvx`) as the command. The first start
also downloads dependencies; if your client times out, run the `uvx --from …` command once in a terminal first.
Logs go to stderr: in Claude Code use `claude --debug`.
</details>

## Limitations

- Writes cover the common operations only; ksqlDB, ACL and schema mutation are not exposed.
- `TAILING` (live) message mode isn't exposed, because MCP tool calls must finish.
- Built-in kafbat serdes only. Custom serde names can be passed via `key_serde`/`value_serde`.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the pull-request flow and how to add a tool.

```bash
git clone https://github.com/DenysFizer/kafbat-mcp && cd kafbat-mcp
uv sync
uv run pytest                  # offline tests against a mocked kafbat, ~2s, no Docker
uv run ruff check

# integration tests: real kafbat UI + Kafka + Schema Registry + Connect in Docker, ~1min once cached
KAFBAT_INTEGRATION=1 uv run pytest tests/test_integration.py

# run the whole suite against several kafbat releases (Kafka/Schema Registry/Connect are shared,
# so each extra release costs only one more kafbat boot, ~25s)
KAFBAT_INTEGRATION=1 KAFBAT_IMAGES=ghcr.io/kafbat/kafka-ui:v1.3.0,ghcr.io/kafbat/kafka-ui:main \
  uv run pytest tests/test_integration.py

# run your working copy in an MCP client
claude mcp add kafbat -e KAFBAT_URL=https://kafka.example.com -- uv run --directory "$PWD" kafbat-mcp

# or poke at it with the MCP Inspector
npx @modelcontextprotocol/inspector -e KAFBAT_URL=https://kafka.example.com uv run --directory "$PWD" kafbat-mcp
```

## License

[MIT](LICENSE)
