from __future__ import annotations

import os
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, get_args

from yt_dlp.cookies import SUPPORTED_BROWSERS, SUPPORTED_KEYRINGS

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

# kafbat's four auth.type values map onto these: DISABLED -> none, LOGIN_FORM and LDAP -> form (same Spring
# formLogin chain), OAUTH2 -> cookie (the browser flow).
AuthType = Literal["cookie", "none", "form"]
AUTH_TYPES = get_args(AuthType)


def flag(env: Mapping[str, str], name: str, default: str) -> bool:
    return env.get(name, default).lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class Config:
    url: str
    auth: AuthType = "cookie"
    username: str | None = None
    password: str | None = None
    # Three tiers: reads only / + additive writes / + destructive writes. See build_server.permitted().
    read_only: bool = True
    allow_destructive: bool = False
    enable_tools: frozenset[str] = frozenset()
    browser: str = "chrome"
    browser_profile: str | None = None
    keyring: str | None = None
    cookie_name: str = "SESSION"
    keepalive_seconds: float = 600
    auto_open: bool = True
    # stays under the 60s default tool timeout of MCP clients, so a slow login fails with our own message
    login_wait_seconds: float = 45
    open_command: tuple[str, ...] | None = None
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Config:
        url = env.get("KAFBAT_URL", "").rstrip("/")
        if not url.startswith(("http://", "https://")):
            raise ValueError("KAFBAT_URL must be set, e.g. https://kafka.example.com")
        auth = env.get("KAFBAT_AUTH", "cookie").lower()
        if auth not in AUTH_TYPES:
            raise ValueError(f"KAFBAT_AUTH must be one of {list(AUTH_TYPES)}")
        username, password = env.get("KAFBAT_USERNAME"), env.get("KAFBAT_PASSWORD")
        if auth == "form" and not (username and password):
            raise ValueError("KAFBAT_AUTH=form requires KAFBAT_USERNAME and KAFBAT_PASSWORD")
        read_only = flag(env, "KAFBAT_READ_ONLY", "true")
        allow_destructive = flag(env, "KAFBAT_ALLOW_DESTRUCTIVE", "false")
        if read_only and allow_destructive:
            raise ValueError("KAFBAT_ALLOW_DESTRUCTIVE needs KAFBAT_READ_ONLY=false, otherwise it does nothing")
        browser = env.get("KAFBAT_BROWSER", "chrome").lower()
        if browser not in SUPPORTED_BROWSERS:
            raise ValueError(f"KAFBAT_BROWSER must be one of {sorted(SUPPORTED_BROWSERS)}")
        keyring = env.get("KAFBAT_KEYRING", "").upper() or None
        if keyring and keyring not in SUPPORTED_KEYRINGS:
            raise ValueError(f"KAFBAT_KEYRING must be one of {sorted(SUPPORTED_KEYRINGS)}")
        log_level = env.get("KAFBAT_LOG_LEVEL", "INFO").upper()
        if log_level not in LOG_LEVELS:
            raise ValueError(f"KAFBAT_LOG_LEVEL must be one of {list(LOG_LEVELS)}")
        profile = env.get("KAFBAT_BROWSER_PROFILE")
        try:
            open_command = tuple(shlex.split(env.get("KAFBAT_OPEN_COMMAND", ""))) or None
        except ValueError as e:
            raise ValueError(f"KAFBAT_OPEN_COMMAND is not a valid command line: {e}") from e
        return cls(
            url=url,
            auth=auth,  # type: ignore[arg-type]
            username=username,
            password=password,
            read_only=read_only,
            allow_destructive=allow_destructive,
            enable_tools=frozenset(t.strip() for t in env.get("KAFBAT_ENABLE_TOOLS", "").split(",") if t.strip()),
            browser=browser,
            browser_profile=os.path.expanduser(profile) if profile else None,
            keyring=keyring,
            cookie_name=env.get("KAFBAT_COOKIE_NAME", "SESSION"),
            keepalive_seconds=float(env.get("KAFBAT_KEEPALIVE_SECONDS", "600")),
            auto_open=flag(env, "KAFBAT_AUTO_OPEN", "true"),
            login_wait_seconds=float(env.get("KAFBAT_LOGIN_WAIT_SECONDS", "45")),
            open_command=open_command,
            log_level=log_level,
        )
