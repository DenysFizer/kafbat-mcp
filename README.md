# kafbat-mcp

[![CI](https://github.com/DenysFizer/kafbat-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/DenysFizer/kafbat-mcp/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)

Read-only [MCP](https://modelcontextprotocol.io) server for [kafbat UI](https://github.com/kafbat/kafka-ui)
(the maintained fork of Kafka UI). It lets Claude Code, Claude Desktop, Cursor or VS Code browse topics, consumer groups,
lag, messages and schemas **through kafbat**, using the session you already have in your browser.

**Why this exists.** kafbat ships a built-in MCP server, but it is often disabled, and it doesn't work well behind
OAuth2/SSO. Service accounts need changes on the kafbat side. This server needs nothing from ops: if you can open
kafbat in your browser, your MCP client can read from it, with exactly your RBAC permissions.

> Not affiliated with or endorsed by the kafbat project.

## How it works

1. On the first tool call, the server reads kafbat's session cookie (`SESSION`) from your browser profile, using
   [yt-dlp](https://github.com/yt-dlp/yt-dlp)'s cookie extraction (it decrypts it with your OS keyring).
2. It calls kafbat's REST API (`/api/**`) with that cookie. Only `GET` requests.
3. **Session expired?** It re-reads the cookie. If the browser's cookie is stale too, it opens kafbat in your browser,
   in the configured profile. An SSO login usually completes on its own. The server waits for the new cookie and
   retries the call.
4. **Keepalive.** While the MCP client runs, a periodic request keeps the session from idling out.

## Requirements

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- A kafbat instance you can log in to in a local browser
- **Linux:** tested (GNOME Keyring). **macOS:** should work (Keychain), untested.
  **Windows:** not supported for Chromium browsers (App-Bound Encryption).

## Installation

The server isn't on PyPI yet; `uvx` installs it straight from GitHub. Replace `KAFBAT_URL` with your instance.

<details open>
<summary><b>Claude Code</b></summary>

```bash
claude mcp add -s user kafbat \
  -e KAFBAT_URL=https://kafka.example.com \
  -e KAFBAT_BROWSER=chrome \
  -- uvx --from git+https://github.com/DenysFizer/kafbat-mcp kafbat-mcp
```

All tools are read-only. To skip permission prompts, add `"mcp__kafbat__*"` to `permissions.allow` in
`~/.claude/settings.json`.
</details>

<details>
<summary><b>Claude Desktop</b> (<code>claude_desktop_config.json</code>)</summary>

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
<summary><b>Cursor</b> (<code>~/.cursor/mcp.json</code>)</summary>

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
<summary><b>VS Code</b> (<code>.vscode/mcp.json</code>)</summary>

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

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFBAT_URL` | **required** | Base URL of kafbat, e.g. `https://kafka.example.com` |
| `KAFBAT_BROWSER` | `chrome` | `chrome`, `chromium`, `brave`, `edge`, `opera`, `vivaldi`, `whale`, `firefox`, `safari` |
| `KAFBAT_BROWSER_PROFILE` | browser default | Profile name (`Profile 1`) or path to the profile directory. See [Troubleshooting](#troubleshooting) |
| `KAFBAT_KEYRING` | auto-detect | Linux only: `GNOMEKEYRING`, `KWALLET`, `KWALLET5`, `KWALLET6`, `BASICTEXT` |
| `KAFBAT_COOKIE_NAME` | `SESSION` | Session cookie name (Spring's default) |
| `KAFBAT_KEEPALIVE_SECONDS` | `600` | Keepalive interval; `0` disables it |
| `KAFBAT_AUTO_OPEN` | `true` | Open kafbat in the browser when the session is gone |
| `KAFBAT_OPEN_COMMAND` | auto | Command used to open the login page; the URL is appended. See [Multiple browser profiles](#multiple-browser-profiles) |
| `KAFBAT_LOGIN_WAIT_SECONDS` | `90` | How long to wait for a fresh cookie after opening the browser |
| `KAFBAT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` (logs go to stderr) |

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

Responses are compact JSON with `null` fields removed, to save context. Tested against kafbat
[`d6cbc7f`](https://github.com/kafbat/kafka-ui/commit/d6cbc7f) (Nov 2025) with Google OAuth2 and RBAC.

## Security

The session cookie stays in memory, is sent only to `KAFBAT_URL`, and no tool can modify anything.
Message payloads, however, do reach your MCP client and model. Read [SECURITY.md](SECURITY.md) before
pointing this at clusters with sensitive data.

## Troubleshooting

**`No valid kafbat session` / `No kafbat session after 90s`**
- Log in to kafbat in the browser and profile you configured. Chromium browsers write cookies to disk about every
  30 seconds, so a fresh login can take up to half a minute to be picked up.
- The login tab opened in a different browser or profile than the one configured: see the next section.

<a id="multiple-browser-profiles"></a>
**Multiple browser profiles (e.g. personal and work Chrome windows)**
Cookies are read only from `KAFBAT_BROWSER_PROFILE`, so other open profiles don't interfere. For the login page:
- If `KAFBAT_BROWSER_PROFILE` is a profile *name* and the browser executable is on `PATH` (`google-chrome`,
  `chromium`, `brave-browser`, `microsoft-edge`, `vivaldi`, `opera`), the page opens with
  `--profile-directory=<profile>`, i.e. in that profile's window even if another profile was used last.
- Otherwise it falls back to the OS default handler (`xdg-open`/`open`), which uses whatever browser and profile were
  active last.
- For Flatpak/Snap installs, macOS, or a profile given as a path, set the command explicitly:
  ```bash
  KAFBAT_OPEN_COMMAND="flatpak run com.google.Chrome --profile-directory='Profile 2'"
  KAFBAT_OPEN_COMMAND="open -na 'Google Chrome' --args --profile-directory='Profile 2'"   # macOS
  ```
The command used is logged at `INFO` level whenever the page is opened.

Chromium stores profiles as `Default`, `Profile 1`, `Profile 2`, … under e.g. `~/.config/google-chrome/`. To see
which profile is which account:
```bash
python3 -c "import json,os; s=json.load(open(os.path.expanduser('~/.config/google-chrome/Local State'))); [print(repr(k), v.get('user_name')) for k,v in s['profile']['info_cache'].items()]"
```

**`failed to decrypt cookie` warnings (Linux)**
The keyring couldn't be read. Make sure a desktop keyring (GNOME Keyring or KWallet) is unlocked, or set
`KAFBAT_KEYRING` explicitly. Run with `KAFBAT_LOG_LEVEL=DEBUG` to see which keyring was chosen.

**Firefox**
Firefox may keep session cookies (no expiry) out of `cookies.sqlite`, so the session may not be found. Use a
Chromium-based browser if that happens.

**`kafbat unreachable` / `HTTP 403 … non-JSON text/html`**
The instance isn't reachable from your machine, or a proxy in front of kafbat (e.g. Cloudflare) blocked the request.
Check your VPN or network.

**`spawn uvx ENOENT` / server fails to start**
`uvx` isn't on the `PATH` your MCP client sees. Use the absolute path (`which uvx`) as the command. The first start
also downloads dependencies. If your client times out, run the `uvx --from …` command once in a terminal first.

**Seeing logs**
Logs go to stderr. In Claude Code use `claude --debug`; Claude Desktop writes MCP logs to its log directory.

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
