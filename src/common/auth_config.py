from __future__ import annotations

"""OIDC environment variable contract, provider metadata, audit event names,
session-state key constants, and identity normalization rules.

Shared between ``launcher.py``, ``src/analytics/auth.py``, and
``src/analytics/audit.py`` so every module agrees on naming without
duplicating string literals.
"""

from typing import Final

# ---------------------------------------------------------------------------
# Environment variable contract
# ---------------------------------------------------------------------------
_ENV_PREFIX: Final[str] = "STREAMLIT_OIDC_"
_ENABLED_PROVIDERS_KEY: Final[str] = f"{_ENV_PREFIX}ENABLED_PROVIDERS"
_REDIRECT_URI_KEY: Final[str] = f"{_ENV_PREFIX}REDIRECT_URI"
_AUTH_MODE_KEY: Final[str] = "CLAIMOPS_AUTH_MODE"
_AUTH_BACKEND_KEY: Final[str] = "CLAIMOPS_AUTH_BACKEND"
_AUTH_FALLBACK_REASON_KEY: Final[str] = "CLAIMOPS_AUTH_FALLBACK_REASON"

AUTH_MODE_AUTO: Final[str] = "auto"
AUTH_MODE_OIDC_ONLY: Final[str] = "oidc_only"
AUTH_MODE_HEADERS_ONLY: Final[str] = "headers_only"
AUTH_MODES: Final[frozenset[str]] = frozenset(
    {AUTH_MODE_AUTO, AUTH_MODE_OIDC_ONLY, AUTH_MODE_HEADERS_ONLY}
)

AUTH_BACKEND_OIDC: Final[str] = "oidc"
AUTH_BACKEND_HEADERS: Final[str] = "headers"

_REQUIRED_PROVIDER_KEYS: Final[tuple[str, ...]] = (
    "CLIENT_ID",
    "CLIENT_SECRET",
    "REDIRECT_URI",
)

# ---------------------------------------------------------------------------
# Inactivity timeout
# ---------------------------------------------------------------------------
INACTIVITY_TIMEOUT_SECONDS: Final[int] = 15 * 60  # sliding 15 minutes

# ---------------------------------------------------------------------------
# Provider metadata
# ---------------------------------------------------------------------------
PROVIDER_GOOGLE: Final[str] = "google"
PROVIDER_MICROSOFT: Final[str] = "microsoft"
PROVIDER_OKTA: Final[str] = "okta"
PROVIDER_AUTH0: Final[str] = "auth0"
PROVIDER_DATABRICKS_HEADERS: Final[str] = "databricks_headers"

_PROVIDER_META: Final[dict[str, dict[str, str]]] = {
    PROVIDER_GOOGLE: {
        "display_name": "Google",
        "icon": "🔵",
        "server_metadata_url": "https://accounts.google.com/.well-known/openid-configuration",
    },
    PROVIDER_MICROSOFT: {
        "display_name": "Microsoft Entra ID",
        "icon": "🔷",
        "server_metadata_url": "",
    },
    PROVIDER_OKTA: {
        "display_name": "Okta",
        "icon": "🟢",
        "server_metadata_url": "",
    },
    PROVIDER_AUTH0: {
        "display_name": "Auth0",
        "icon": "🟠",
        "server_metadata_url": "",
    },
}

KNOWN_PROVIDERS: Final[frozenset[str]] = frozenset(_PROVIDER_META)

PROVIDER_LABELS: Final[dict[str, str]] = {
    k: v["display_name"] for k, v in _PROVIDER_META.items()
}
PROVIDER_LABELS[PROVIDER_DATABRICKS_HEADERS] = "Databricks Workspace"

# ---------------------------------------------------------------------------
# Databricks forwarded identity headers
# ---------------------------------------------------------------------------
HEADER_FORWARDED_USER: Final[str] = "x-forwarded-user"
HEADER_FORWARDED_EMAIL: Final[str] = "x-forwarded-email"
HEADER_FORWARDED_PREFERRED_USERNAME: Final[str] = "x-forwarded-preferred-username"

# ---------------------------------------------------------------------------
# Audit event names
# ---------------------------------------------------------------------------
EVENT_LOGIN_SUCCESS: Final[str] = "login_success"
EVENT_LOGOUT: Final[str] = "logout"
EVENT_SESSION_TIMEOUT: Final[str] = "session_timeout"
EVENT_ACCESS_DENIED: Final[str] = "access_denied"

OUTCOME_SUCCESS: Final[str] = "success"
OUTCOME_FAILURE: Final[str] = "failure"

# ---------------------------------------------------------------------------
# Session-state keys (used by st.session_state)
# ---------------------------------------------------------------------------
SESSION_SESSION_ID: Final[str] = "_auth_session_id"
SESSION_USER_SUB: Final[str] = "_auth_user_sub"
SESSION_USER_EMAIL: Final[str] = "_auth_user_email"
SESSION_USER_NAME: Final[str] = "_auth_user_name"
SESSION_PROVIDER: Final[str] = "_auth_provider"
SESSION_LAST_ACTIVITY: Final[str] = "_auth_last_activity_ts"
SESSION_IS_AUTHENTICATED: Final[str] = "_auth_is_authenticated"
SESSION_ACCESS_DENIED: Final[str] = "_auth_access_denied"
SESSION_AUTH_UNAVAILABLE: Final[str] = "_auth_unavailable"
SESSION_AUTH_BACKEND: Final[str] = "_auth_backend"
SESSION_HEADER_LOCKED: Final[str] = "_auth_header_locked"

# ---------------------------------------------------------------------------
# Identity normalization keys extracted from OIDC userinfo
# ---------------------------------------------------------------------------
_IDENTITY_KEYS: Final[tuple[str, ...]] = ("sub", "email", "name", "picture")

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def env_prefix() -> str:
    return _ENV_PREFIX


def enabled_providers_key() -> str:
    return _ENABLED_PROVIDERS_KEY


def redirect_uri_key() -> str:
    return _REDIRECT_URI_KEY


def auth_mode_key() -> str:
    return _AUTH_MODE_KEY


def auth_backend_key() -> str:
    return _AUTH_BACKEND_KEY


def auth_fallback_reason_key() -> str:
    return _AUTH_FALLBACK_REASON_KEY


def normalize_auth_mode(value: str | None) -> str:
    mode = (value or "").strip().lower()
    if mode in AUTH_MODES:
        return mode
    return AUTH_MODE_AUTO


def required_provider_keys() -> tuple[str, ...]:
    return _REQUIRED_PROVIDER_KEYS


def provider_meta(provider: str) -> dict[str, str]:
    return _PROVIDER_META.get(provider.lower(), {})


def known_providers() -> frozenset[str]:
    return KNOWN_PROVIDERS


def identity_keys() -> tuple[str, ...]:
    return _IDENTITY_KEYS


def provider_env_name(provider: str, key: str) -> str:
    return f"{_ENV_PREFIX}{provider.upper()}_{key.upper()}"
