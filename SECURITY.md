# Security

## What this server touches

kafbat-mcp runs locally, as your user. How it authenticates depends on `KAFBAT_AUTH`:

| `KAFBAT_AUTH` | Credential | Where it comes from |
|---|---|---|
| `cookie` *(default)* | kafbat `SESSION` cookie | your browser's cookie store |
| `form` | `KAFBAT_USERNAME` / `KAFBAT_PASSWORD` | your MCP client's config, as env vars |
| `none` | nothing | — |

- **`cookie` reads your browser's cookie database.** On every session refresh it copies the browser's cookie store
  to a temporary directory and reads it the way
  [yt-dlp's `--cookies-from-browser`](https://github.com/yt-dlp/yt-dlp#filesystem-options) does — decrypting it
  with the OS keyring for Chromium-based browsers (GNOME Keyring / KWallet on Linux, Keychain on macOS). Only the
  cookie named `KAFBAT_COOKIE_NAME` for the host in `KAFBAT_URL` is kept; everything else is discarded.
- **`form` keeps your password in the environment of the server process.** It is sent only in the `POST /login`
  to `KAFBAT_URL`. Treat the MCP client config that holds it like any other file containing a password.
- **Credentials stay in memory.** The session cookie is never written to disk, logged, or returned by any tool.
- **Credentials are sent only to `KAFBAT_URL`.** HTTP redirects are not followed, so a cookie or password never
  reaches your identity provider or any other host.

## What it can change

**By default, nothing.** `KAFBAT_READ_ONLY` defaults to `true`, and in that mode no tool can create, change or
delete anything in your clusters. The single non-`GET` request is `registerFilter`, which stores a smart-filter
expression in kafbat so it can apply it while *reading* messages.

Write tools exist, behind explicit opt-in:

| Setting | Adds |
|---|---|
| `KAFBAT_READ_ONLY=false` | `produce_message`, `create_topic`, `update_connector_state` |
| `KAFBAT_ALLOW_DESTRUCTIVE=true` *(needs the above)* | `delete_topic`, `reset_consumer_group_offsets` — these lose data |
| `KAFBAT_ENABLE_TOOLS=<names>` | the named tools only, whatever the tier |

A tool the configured tier excludes is **not registered**: it is absent from the tool list, and calling it by name
fails with `Unknown tool`, because the MCP SDK resolves calls through the same registry.

These flags are a guardrail, not a security boundary — they run in the same process as the tools they gate. The
real boundary is kafbat's own RBAC for the user you authenticate as, and the cluster's `readOnly` setting. The
`whoami` tool reports what your session is actually permitted to do.

## What it hides

- **Sensitive broker and topic configs.** `describe_topic` and `describe_broker` drop every entry kafbat flags as
  sensitive.
- **Credentials in Kafka Connect configs.** Connect configs are a free-form map with no sensitivity flag, so
  `describe_connector` masks values whose key contains `password`, `secret`, `token` or `credential`. This is
  a name heuristic: a secret stored under a key like `dsn` or `connection.url` is **not** masked.

## What it does not protect against

- **Message payloads go to your MCP client and the model.** `consume_messages` returns keys, values and headers.
  If your topics carry personal data or secrets, that data ends up in the model context. Use `max_value_chars`
  and the filters accordingly, or don't enable the server for such clusters.
- **Any process running as your user can already read your browser cookies.** This server does not add new access.
  It does let an MCP client act as you in kafbat, within your kafbat permissions — including writes, if you enable
  them.
- **Your MCP client's approval rules.** Some clients auto-approve tools by name pattern. If you allow
  `mcp__kafbat__*` and then enable write access, destructive tools are auto-approved too. Narrow the rule first.
- **Session keepalive.** While your MCP client is running, the server keeps your kafbat session from idling out.
  Set `KAFBAT_KEEPALIVE_SECONDS=0` if your organisation relies on idle timeouts.

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/DenysFizer/kafbat-mcp/security/advisories/new) rather than a public
issue.
