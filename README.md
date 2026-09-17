<div align="center">

<!-- switch to the raw.githubusercontent.com URL when publishing to PyPI; relative paths don't resolve there -->
<img src="assets/logo.svg" alt="" width="300">

# kafbat-mcp

**Read-only MCP server for kafbat UI — browse Kafka from your AI client,<br>using the session you already have in your browser.**

<!-- native Actions badge: shields.io can't read a private repo's workflow status -->
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

[Why kafbat-mcp](#why-kafbat-mcp) · [Quick start](#quick-start) · [Tools](#tools) · [How it works](#how-it-works) ·
[Configuration](#configuration) · [Security](#security) · [Requirements](#requirements-and-platform-support) ·
[Troubleshooting](#troubleshooting) · [Limitations](#limitations) · [Development](#development) · [License](#license)

## Why kafbat-mcp

kafbat ships its own MCP server, but it is **off by default** (`mcp.enabled`), so enabling it needs a config change
and a redeploy from whoever runs kafbat. It also runs server-side with kafbat's own Kafka credentials, so it cannot
reflect *your* RBAC permissions, and forwarding a browser session into it is not supported for SSO setups.

kafbat-mcp runs on your machine instead. If you can open kafbat in your browser, your MCP client can read from it,
within your kafbat read permissions — and every tool is a `GET`, so nothing in the cluster can be changed.

## Quick start

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/). Not on PyPI yet, so `uvx` installs it
straight from GitHub.

[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_server-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=kafbat&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22--from%22%2C%22git%2Bhttps%3A%2F%2Fgithub.com%2FDenysFizer%2Fkafbat-mcp%22%2C%22kafbat-mcp%22%5D%2C%22env%22%3A%7B%22KAFBAT_URL%22%3A%22https%3A%2F%2Fkafka.example.com%22%2C%22KAFBAT_BROWSER%22%3A%22chrome%22%7D%7D)
[![Install in Cursor](https://img.shields.io/badge/Cursor-Install_server-000000?style=flat-square&logo=cursor&logoColor=white)](https://cursor.com/en/install-mcp?name=kafbat&config=eyJjb21tYW5kIjoidXZ4IiwiYXJncyI6WyItLWZyb20iLCJnaXQraHR0cHM6Ly9naXRodWIuY29tL0RlbnlzRml6ZXIva2FmYmF0LW1jcCIsImthZmJhdC1tY3AiXSwiZW52Ijp7IktBRkJBVF9VUkwiOiJodHRwczovL2thZmthLmV4YW1wbGUuY29tIiwiS0FGQkFUX0JST1dTRVIiOiJjaHJvbWUifX0%3D)

Both buttons install with the placeholder `https://kafka.example.com` — change `KAFBAT_URL` to your instance
afterwards. For Claude Code:

```bash
claude mcp add -s user kafbat \
  -e KAFBAT_URL=https://kafka.example.com \
  -e KAFBAT_BROWSER=chrome \
  -- uvx --from git+https://github.com/DenysFizer/kafbat-mcp kafbat-mcp
```

Then ask your client something like *"which consumer groups on DEV have lag?"* or *"show the last 5 messages on
orders.v1"*.

> [!TIP]
> All tools are read-only, so you can add `"mcp__kafbat__*"` to `permissions.allow` in `~/.claude/settings.json`
> to skip approval prompts.

### Other MCP clients

<details>
<summary><b>Claude Desktop</b> — <code>claude_desktop_config.json</code></summary>

```json
{
  "mcpServers": {
    "kafbat": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp", "kafbat-mcp"],
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
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp", "kafbat-mcp"],
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
      "args": ["--from", "git+https://github.com/DenysFizer/kafbat-mcp", "kafbat-mcp"],
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
| `consume_messages` | Messages by `LATEST`/`EARLIEST`/offset/timestamp, with filter and cursor paging; long values truncated | `GET …/topics/{topic}/messages/v2` |
| `list_schemas` | Schema Registry subjects (metadata only) | `GET …/schemas` |
| `get_schema` | Latest or a specific schema version | `GET …/schemas/{subject}/latest` |

<i>Responses are compact JSON with <code>null</code> fields removed, to keep context small.</i>

## How it works

1. On the first tool call the server reads kafbat's session cookie (`SESSION`) from your browser profile, using
   [yt-dlp](https://github.com/yt-dlp/yt-dlp)'s cookie extraction, which decrypts it with your OS keyring.
2. It calls kafbat's REST API with that cookie — `GET` only, redirects not followed.
3. If the session has expired, it re-reads the cookie; if the browser's cookie is stale too, it opens kafbat in the
   configured browser profile, waits for the new cookie (an SSO login usually completes on its own) and retries.
4. While your MCP client is running, a periodic request keeps the session from idling out.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFBAT_URL` | **required** | Base URL of kafbat, e.g. `https://kafka.example.com` |
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

The session cookie stays in memory and is sent only to `KAFBAT_URL`; redirects are not followed.

> [!IMPORTANT]
> Every tool is a `GET`, so nothing in the cluster can be changed — but message payloads do reach your MCP client
> and model. Read [SECURITY.md](SECURITY.md) before pointing this at clusters with sensitive data.

## Requirements and platform support

- [uv](https://docs.astral.sh/uv/getting-started/installation/), Python 3.10+
- A kafbat instance you can log in to in a local browser
- **Linux:** tested (GNOME Keyring). **macOS:** should work (Keychain), untested.
  **Windows:** not supported for Chromium browsers (App-Bound Encryption).
- Tested against kafbat [`d6cbc7f`](https://github.com/kafbat/kafka-ui/commit/d6cbc7f) (Nov 2025) with Google OAuth2
  and RBAC.

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

- Read-only by design: no producing messages, resetting offsets, or managing topics.
- `TAILING` (live) message mode isn't exposed, because MCP tool calls must finish.
- Built-in kafbat serdes only. Custom serde names can be passed via `key_serde`/`value_serde`.

## Development

```bash
git clone https://github.com/DenysFizer/kafbat-mcp && cd kafbat-mcp
uv sync
uv run pytest                  # offline tests against a mocked kafbat
uv run ruff check

# run your working copy in an MCP client
claude mcp add kafbat -e KAFBAT_URL=https://kafka.example.com -- uv run --directory "$PWD" kafbat-mcp

# or poke at it with the MCP Inspector
npx @modelcontextprotocol/inspector -e KAFBAT_URL=https://kafka.example.com uv run --directory "$PWD" kafbat-mcp
```

## License

[MIT](LICENSE)
