"""Read-only MCP server for kafbat UI, authenticated with the session cookie of your browser."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("kafbat-mcp")
except PackageNotFoundError:  # running from a source tree without installation
    __version__ = "0.0.0"
