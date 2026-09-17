# Security

## What this server touches

kafbat-mcp runs locally, as your user, and reuses the kafbat session you already have in your browser.

- **Reads your browser's cookie database.** On every session refresh it copies the browser's cookie store to a
  temporary directory and decrypts it with the OS keyring (GNOME Keyring / KWallet on Linux, Keychain on macOS),
  the same way [yt-dlp's `--cookies-from-browser`](https://github.com/yt-dlp/yt-dlp#filesystem-options) does.
  Only the cookie named `KAFBAT_COOKIE_NAME` for the host in `KAFBAT_URL` is kept; everything else is discarded.
- **Keeps the session cookie in memory only.** It is never written to disk, logged, or returned by any tool.
- **Sends the cookie only to `KAFBAT_URL`.** HTTP redirects are not followed, so the cookie never reaches your
  identity provider or any other host.
- **Only issues `GET` requests.** No tool can create, change, or delete topics, consumer groups, offsets, schemas,
  ACLs, or connectors. What you can *read* is still limited by kafbat's RBAC for your user.
- **Hides sensitive topic configs.** `describe_topic` drops entries that kafbat marks as sensitive.

## What it does not protect against

- **Message payloads go to your MCP client and the model.** `consume_messages` returns keys, values and headers.
  If your topics carry personal data or secrets, that data ends up in the model context. Use `max_value_chars`
  and `string_filter` accordingly, or don't enable the server for such clusters.
- **Any process running as your user can already read your browser cookies.** This server does not add new access.
  It does let an MCP client act as you in kafbat, within your read permissions.
- **Session keepalive.** While your MCP client is running, the server keeps your kafbat session from idling out.
  Set `KAFBAT_KEEPALIVE_SECONDS=0` if your organisation relies on idle timeouts.

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/DenysFizer/kafbat-mcp/security/advisories/new) rather than a public
issue.
