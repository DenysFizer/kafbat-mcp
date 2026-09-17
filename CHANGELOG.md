# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0]

### Added
- Read-only tools: `list_clusters`, `list_topics`, `describe_topic`, `list_consumer_groups`,
  `describe_consumer_group`, `consume_messages`, `list_schemas`, `get_schema`.
- Authentication with the kafbat session cookie read from a local browser profile (Chromium-based browsers,
  Firefox, Safari via yt-dlp), with automatic re-login through the browser and a keepalive.
- Login page opens in the configured Chromium profile (`--profile-directory`), overridable with `KAFBAT_OPEN_COMMAND`.
- Short error for non-JSON responses from proxies in front of kafbat (e.g. Cloudflare 403) instead of raw HTML.
- Configuration through `KAFBAT_*` environment variables.
