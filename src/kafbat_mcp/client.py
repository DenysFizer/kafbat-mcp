"""kafbat HTTP client. Authenticates with one of kafbat's auth types (see Config.auth)."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

import httpx2
from mcp.server.mcpserver.exceptions import ToolError
from yt_dlp.cookies import YDLLogger, extract_cookies_from_browser

from kafbat_mcp.config import Config

log = logging.getLogger("kafbat-mcp")

LOGIN_POLL_SECONDS = 3.0  # Chromium flushes cookies to disk every ~30s
HTTP_TIMEOUT_SECONDS = 30.0

# Executables that accept --profile-directory, so a login tab lands in the profile we read cookies from.
CHROMIUM_EXECUTABLES = {
    "chrome": ("google-chrome", "google-chrome-stable"),
    "chromium": ("chromium", "chromium-browser"),
    "brave": ("brave-browser", "brave"),
    "edge": ("microsoft-edge", "microsoft-edge-stable"),
    "vivaldi": ("vivaldi", "vivaldi-stable"),
    "opera": ("opera",),
}

# `cookie` is missing on purpose: it raises its own message from _browser_cookie.
AUTH_HINTS = {
    "none": "kafbat requires authentication. Set KAFBAT_AUTH to form or cookie.",
    "form": "kafbat rejected the login. Check KAFBAT_USERNAME and KAFBAT_PASSWORD.",
}


class _CookieLog(YDLLogger):
    """Routes yt-dlp messages to stderr logging; stdout belongs to the MCP protocol."""

    def debug(self, message):
        log.debug("yt-dlp: %s", message)

    def info(self, message):
        log.debug("yt-dlp: %s", message)

    def warning(self, message, only_once=False):
        log.warning("yt-dlp: %s", message)

    def error(self, message, *, is_error=True):
        log.error("yt-dlp: %s", message)


def cookie_matches_host(cookie_domain: str, host: str) -> bool:
    domain = cookie_domain.lstrip(".").lower()
    return host == domain or host.endswith("." + domain)


def auth_failed(resp: httpx2.Response) -> bool:
    # kafbat redirects unauthenticated API calls to its login page (e.g. /oauth2/authorization/<provider>)
    return resp.status_code == 401 or resp.is_redirect


def checked(resp: httpx2.Response) -> httpx2.Response:
    if resp.is_success:
        return resp
    content_type = resp.headers.get("content-type", "").split(";")[0]
    if "json" in content_type:
        try:
            body = resp.json()
        except ValueError:
            body = None
        # kafbat puts a full stacktrace next to message
        detail = (body.get("message") if isinstance(body, dict) else None) or resp.text
    else:
        detail = f"non-JSON {content_type or 'response'}"  # a proxy/WAF answered, not kafbat
        if resp.status_code in (403, 429, 502, 503, 504):
            detail += " (likely blocked by a proxy in front of kafbat; check your network/VPN)"
    raise ToolError(f"kafbat HTTP {resp.status_code} for {resp.request.url.path}: {str(detail)[:500]}")


def open_command(cfg: Config, which: Callable[[str], str | None] = shutil.which) -> list[str] | None:
    """Command the login URL is appended to; None means the OS default handler (Windows)."""
    if cfg.open_command:
        return list(cfg.open_command)
    profile = cfg.browser_profile
    # A profile *name* ("Profile 2") maps to --profile-directory; a path means a custom user-data-dir, which we can't
    # reliably reconstruct, so fall back to the default handler (KAFBAT_OPEN_COMMAND covers that case).
    if profile and os.sep not in profile:
        for name in CHROMIUM_EXECUTABLES.get(cfg.browser, ()):
            if path := which(name):
                return [path, f"--profile-directory={profile}"]
    if sys.platform == "win32":
        return None
    return ["open" if sys.platform == "darwin" else "xdg-open"]


def open_login_page(cfg: Config, url: str) -> None:
    cmd = open_command(cfg)
    log.info("kafbat session expired, opening %s with %s", url, " ".join(cmd) if cmd else "the default browser")
    try:
        if cmd is None:
            os.startfile(url)  # type: ignore[attr-defined]
            return
        # Detached with no inherited stdout: anything a browser prints there would corrupt the MCP stream.
        subprocess.Popen(
            [*cmd, url],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as e:
        raise ToolError(f"Cannot open the browser ({e}). Set KAFBAT_OPEN_COMMAND or log in to {url} manually.") from e


class Kafbat:
    def __init__(
        self,
        cfg: Config,
        transport: httpx2.AsyncBaseTransport | None = None,
        cookie_reader: Callable[[], str | None] | None = None,
        opener: Callable[[str], None] | None = None,
    ):
        self.cfg = cfg
        self.http = httpx2.AsyncClient(
            base_url=cfg.url, timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=False, transport=transport
        )
        self._cookie_reader = cookie_reader or self._extract_browser_cookie
        self._open = opener or (lambda url: open_login_page(cfg, url))
        # Credential headers for the configured strategy; None until established. {} is valid (auth `none`).
        self._headers: dict[str, str] | None = None
        self._lock = asyncio.Lock()

    def _extract_browser_cookie(self) -> str | None:
        host = urlsplit(self.cfg.url).hostname or ""
        jar = extract_cookies_from_browser(
            self.cfg.browser, self.cfg.browser_profile, _CookieLog(), keyring=self.cfg.keyring
        )
        for c in jar:
            if c.name == self.cfg.cookie_name and cookie_matches_host(c.domain, host):
                return c.value
        return None

    async def _read_cookie(self) -> str | None:
        try:
            return await asyncio.to_thread(self._cookie_reader)
        except Exception as e:
            raise ToolError(f"Cannot read cookies from {self.cfg.browser}: {e}") from e

    async def _request(
        self,
        method: str,
        path: str,
        params: Any = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        json: Any = None,
    ) -> httpx2.Response:
        # Explicit headers: httpx2 then skips its own cookie jar, so Set-Cookie from redirects can't leak in.
        try:
            return await self.http.request(method, path, params=params, headers=headers, data=data, json=json)
        except httpx2.HTTPError as e:
            raise ToolError(f"kafbat unreachable ({type(e).__name__}: {e}). Is the network/VPN up?") from e

    async def _accepted(self, headers: dict[str, str] | None) -> bool:
        return headers is not None and not auth_failed(await self._request("GET", "/api/clusters", headers=headers))

    def _cookie_header(self, value: str) -> dict[str, str]:
        return {"Cookie": f"{self.cfg.cookie_name}={value}"}

    async def _form_login(self) -> dict[str, str]:
        """kafbat's LOGIN_FORM and LDAP share one Spring formLogin chain; CSRF is disabled, so no token needed."""
        resp = await self._request(
            "POST", "/login", data={"username": self.cfg.username, "password": self.cfg.password}
        )
        session = resp.cookies.get(self.cfg.cookie_name)
        # On bad credentials kafbat answers 302 to /login?error with no session cookie.
        return self._cookie_header(session) if session else {}

    async def _browser_cookie(self) -> dict[str, str]:
        stale = await self._read_cookie()
        if stale and await self._accepted(self._cookie_header(stale)):
            return self._cookie_header(stale)
        hint = f"Log in to {self.cfg.url} in {self.cfg.browser} and retry."
        if not self.cfg.auto_open:
            raise ToolError(f"No valid kafbat session. {hint}")
        self._open(self.cfg.url)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.cfg.login_wait_seconds
        while loop.time() < deadline:
            await asyncio.sleep(LOGIN_POLL_SECONDS)
            fresh = await self._read_cookie()
            if fresh and fresh != stale and await self._accepted(self._cookie_header(fresh)):
                return self._cookie_header(fresh)
        raise ToolError(f"No kafbat session after {self.cfg.login_wait_seconds:.0f}s. {hint}")

    async def _obtain(self) -> dict[str, str]:
        """Credential headers for the configured strategy, already validated against /api/clusters."""
        if self.cfg.auth == "cookie":
            return await self._browser_cookie()  # validates as it polls
        headers: dict[str, str] = {} if self.cfg.auth == "none" else await self._form_login()
        if not await self._accepted(headers):
            raise ToolError(AUTH_HINTS[self.cfg.auth])
        return headers

    async def _refresh(self) -> None:
        if await self._accepted(self._headers):  # refreshed concurrently while we waited for the lock
            return
        self._headers = None
        self._headers = await self._obtain()

    async def _send(self, method: str, path: str, params: Any = None, json: Any = None) -> httpx2.Response:
        """Re-authenticates once on a 401/login redirect. Safe to retry: such a response means nothing ran."""
        if self._headers is not None:
            resp = await self._request(method, path, params, self._headers, json=json)
            if not auth_failed(resp):
                return checked(resp)
        async with self._lock:
            await self._refresh()
        resp = await self._request(method, path, params, self._headers, json=json)
        if auth_failed(resp):
            raise ToolError("kafbat rejected freshly obtained credentials. Check RBAC permissions.")
        return checked(resp)

    async def get(self, path: str, params: Any = None) -> httpx2.Response:
        return await self._send("GET", path, params)

    async def get_json(self, path: str, params: Any = None) -> Any:
        return (await self.get(path, params)).json()

    async def post(self, path: str, json: Any = None) -> httpx2.Response:
        return await self._send("POST", path, json=json)

    async def post_json(self, path: str, json: Any = None) -> Any:
        return (await self.post(path, json)).json()

    async def delete(self, path: str) -> httpx2.Response:
        return await self._send("DELETE", path)

    async def keepalive(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.keepalive_seconds)
            try:
                if self._headers is not None and not await self._accepted(self._headers):
                    log.warning("kafbat session expired; will re-authenticate on next tool call")
                    self._headers = None
            except ToolError as e:
                log.warning("keepalive failed: %s", e)
