"""Authentication module with resilient dual backends.

Primary backend: Streamlit native OIDC (``st.login`` / ``st.user`` / ``st.logout``).
Fallback backend: Databricks Apps forwarded identity headers from ``st.context.headers``.
"""
from __future__ import annotations

import html
import logging
import os
import time
import uuid
from collections.abc import Mapping
from typing import Any, Final

import streamlit as st

from src.common.auth_config import (
    AUTH_BACKEND_HEADERS,
    AUTH_BACKEND_OIDC,
    EVENT_ACCESS_DENIED,
    EVENT_LOGIN_SUCCESS,
    EVENT_LOGOUT,
    EVENT_SESSION_TIMEOUT,
    HEADER_FORWARDED_EMAIL,
    HEADER_FORWARDED_PREFERRED_USERNAME,
    HEADER_FORWARDED_USER,
    INACTIVITY_TIMEOUT_SECONDS,
    KNOWN_PROVIDERS,
    OUTCOME_FAILURE,
    OUTCOME_SUCCESS,
    PROVIDER_DATABRICKS_HEADERS,
    PROVIDER_GOOGLE,
    PROVIDER_LABELS,
    SESSION_ACCESS_DENIED,
    SESSION_AUTH_BACKEND,
    SESSION_AUTH_UNAVAILABLE,
    SESSION_HEADER_LOCKED,
    SESSION_IS_AUTHENTICATED,
    SESSION_LAST_ACTIVITY,
    SESSION_PROVIDER,
    SESSION_SESSION_ID,
    SESSION_USER_EMAIL,
    SESSION_USER_NAME,
    SESSION_USER_SUB,
    auth_backend_key,
    auth_fallback_reason_key,
)

logger: Final = logging.getLogger(__name__)


def _runtime_auth_backend() -> str:
    raw = os.getenv(auth_backend_key(), AUTH_BACKEND_OIDC).strip().lower()
    if raw == AUTH_BACKEND_HEADERS:
        return AUTH_BACKEND_HEADERS
    return AUTH_BACKEND_OIDC


def _fallback_reason() -> str:
    return os.getenv(auth_fallback_reason_key(), "").strip()


def _is_oidc_backend(backend: str) -> bool:
    return backend == AUTH_BACKEND_OIDC


# ---------------------------------------------------------------------------
# OIDC configuration helpers
# ---------------------------------------------------------------------------


def _get_auth_config() -> dict[str, Any]:
    """Return the ``[auth]`` section of Streamlit secrets, or empty dict."""
    try:
        section = st.secrets.get("auth")
        if isinstance(section, Mapping):
            return _mapping_to_plain_dict(section)
    except Exception:
        pass
    return {}


def _mapping_to_plain_dict(value: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively normalize Streamlit AttrDict-style mappings to plain dicts."""
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        normalized[key] = _mapping_to_plain_dict(item) if isinstance(item, Mapping) else item
    return normalized


def is_auth_available() -> bool:
    """Return True when OIDC config is present in secrets and not empty."""
    config = _get_auth_config()
    if not config:
        return False
    enabled = _get_enabled_providers(config)
    return len(enabled) > 0


def _get_enabled_providers(auth_config: dict[str, Any]) -> list[str]:
    """Discover which OIDC providers have configuration sections."""
    providers: list[str] = []
    for key, value in auth_config.items():
        if key in ("redirect_uri", "cookie_secret"):
            continue
        if isinstance(value, Mapping) and key in KNOWN_PROVIDERS:
            providers.append(key)
    return providers


# ---------------------------------------------------------------------------
# Databricks header identity helpers
# ---------------------------------------------------------------------------


def _get_request_headers() -> dict[str, str]:
    try:
        context = getattr(st, "context", None)
        headers = getattr(context, "headers", None)
        if headers is None:
            return {}
        if isinstance(headers, Mapping):
            return {str(k).lower(): str(v).strip() for k, v in headers.items()}
        if hasattr(headers, "items"):
            return {str(k).lower(): str(v).strip() for k, v in headers.items()}
    except Exception:
        pass
    return {}


def _get_forwarded_identity() -> tuple[str, str, str]:
    headers = _get_request_headers()
    user = headers.get(HEADER_FORWARDED_USER, "").strip()
    email = headers.get(HEADER_FORWARDED_EMAIL, "").strip()
    preferred_username = headers.get(HEADER_FORWARDED_PREFERRED_USERNAME, "").strip()
    return user, email, preferred_username


def _has_forwarded_identity() -> bool:
    user, _, _ = _get_forwarded_identity()
    return bool(user)


# ---------------------------------------------------------------------------
# Session state helpers
# ---------------------------------------------------------------------------


def init_auth_session(backend: str) -> None:
    """Initialise auth-related keys in ``st.session_state`` once per session."""
    now = time.time()
    unavailable = (not is_auth_available()) if _is_oidc_backend(backend) else (not _has_forwarded_identity())
    defaults: dict[str, Any] = {
        SESSION_SESSION_ID: "",
        SESSION_USER_SUB: "",
        SESSION_USER_EMAIL: "",
        SESSION_USER_NAME: "",
        SESSION_PROVIDER: "",
        SESSION_LAST_ACTIVITY: now,
        SESSION_IS_AUTHENTICATED: False,
        SESSION_ACCESS_DENIED: False,
        SESSION_AUTH_UNAVAILABLE: unavailable,
        SESSION_AUTH_BACKEND: backend,
        SESSION_HEADER_LOCKED: False,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default
    st.session_state[SESSION_AUTH_BACKEND] = backend
    if _is_oidc_backend(backend):
        st.session_state[SESSION_HEADER_LOCKED] = False


def get_session_id() -> str:
    """Return the app-level session UUID, generating one if needed."""
    sid = str(st.session_state.get(SESSION_SESSION_ID, ""))
    if not sid:
        sid = str(uuid.uuid4())
        st.session_state[SESSION_SESSION_ID] = sid
    return sid


def touch_activity() -> None:
    """Refresh the inactivity timer."""
    if st.session_state.get(SESSION_IS_AUTHENTICATED):
        st.session_state[SESSION_LAST_ACTIVITY] = time.time()


def _clear_auth_session() -> None:
    """Drop core auth keys from session state (logout / timeout / denied)."""
    for key in (
        SESSION_USER_SUB,
        SESSION_USER_EMAIL,
        SESSION_USER_NAME,
        SESSION_PROVIDER,
        SESSION_LAST_ACTIVITY,
        SESSION_IS_AUTHENTICATED,
        SESSION_ACCESS_DENIED,
    ):
        st.session_state.pop(key, None)


# ---------------------------------------------------------------------------
# User synchronisation
# ---------------------------------------------------------------------------


def _sync_st_user_to_session() -> bool:
    """Copy claims from ``st.user`` into session state on first successful login."""
    if not _st_user_is_logged_in():
        return False
    if st.session_state.get(SESSION_IS_AUTHENTICATED):
        return False

    user: Any = st.user  # type: ignore[assignment]
    sub = _safe_attr(user, "sub") or _safe_attr(user, "id") or ""
    email = _safe_attr(user, "email") or ""
    name = _safe_attr(user, "name") or ""

    st.session_state[SESSION_USER_SUB] = sub
    st.session_state[SESSION_USER_EMAIL] = email
    st.session_state[SESSION_USER_NAME] = name
    st.session_state[SESSION_PROVIDER] = _infer_provider()
    st.session_state[SESSION_IS_AUTHENTICATED] = True
    st.session_state[SESSION_LAST_ACTIVITY] = time.time()
    get_session_id()
    return True


def _sync_headers_to_session() -> bool:
    """Sync Databricks forwarded identity headers into session state."""
    if st.session_state.get(SESSION_HEADER_LOCKED):
        return False
    user_sub, email, preferred_username = _get_forwarded_identity()
    if not user_sub:
        return False
    if st.session_state.get(SESSION_IS_AUTHENTICATED):
        return False
    st.session_state[SESSION_USER_SUB] = user_sub
    st.session_state[SESSION_USER_EMAIL] = email
    st.session_state[SESSION_USER_NAME] = preferred_username or email or user_sub
    st.session_state[SESSION_PROVIDER] = PROVIDER_DATABRICKS_HEADERS
    st.session_state[SESSION_IS_AUTHENTICATED] = True
    st.session_state[SESSION_LAST_ACTIVITY] = time.time()
    get_session_id()
    return True


def _st_user_is_logged_in() -> bool:
    try:
        user = st.user
        if user is None:
            return False
        if hasattr(user, "is_logged_in"):
            return bool(user.is_logged_in)
        return bool(getattr(user, "sub", None) or getattr(user, "email", None))
    except Exception:
        return False


def _infer_provider() -> str:
    auth_config = _get_auth_config()
    enabled = _get_enabled_providers(auth_config)
    return enabled[0] if enabled else PROVIDER_GOOGLE


def _safe_attr(obj: Any, name: str) -> str:
    try:
        if isinstance(obj, dict):
            val = obj.get(name, "")
        else:
            val = getattr(obj, name, "")
        return str(val).strip()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Inactivity timeout
# ---------------------------------------------------------------------------


def check_inactivity_timeout(backend: str) -> bool:
    """Return True if active; enforce backend-specific timeout behavior."""
    if not st.session_state.get(SESSION_IS_AUTHENTICATED):
        return True
    last = float(st.session_state.get(SESSION_LAST_ACTIVITY, 0))
    elapsed = time.time() - last
    if elapsed <= INACTIVITY_TIMEOUT_SECONDS:
        return True

    _write_audit_event(
        EVENT_SESSION_TIMEOUT,
        OUTCOME_SUCCESS,
        reason=f"inactive {elapsed:.0f}s",
    )
    if _is_oidc_backend(backend):
        _clear_auth_session()
        try:
            st.logout()
        except Exception:
            logger.warning("st.logout() failed during timeout — clearing local state")
    else:
        _clear_auth_session()
        st.session_state[SESSION_HEADER_LOCKED] = True
    return False


# ---------------------------------------------------------------------------
# Access policy (v1: always allow)
# ---------------------------------------------------------------------------


def check_access_policy(_email: str = "") -> tuple[bool, str | None]:
    return True, None


# ---------------------------------------------------------------------------
# Auth screens
# ---------------------------------------------------------------------------


def render_login_screen() -> None:
    """Provider-config-driven login page."""
    auth_config = _get_auth_config()
    providers = _get_enabled_providers(auth_config)

    st.markdown("### Sign in")
    st.caption("Authentication required to access the Claim Denial Risk Analyzer.")

    if not providers:
        st.warning("No authentication providers are configured.")
        return

    st.markdown("---")
    for provider in providers:
        label = PROVIDER_LABELS.get(provider, provider.title())
        st.button(
            f"Sign in with {label}",
            key=f"auth_login_btn_{provider}",
            type="primary",
            on_click=lambda p=provider: st.login(p),
        )


def render_auth_unavailable_screen(backend: str) -> None:
    """Fail-closed screen shown when the selected auth backend is unavailable."""
    st.markdown("### Authentication Unavailable")
    if _is_oidc_backend(backend):
        st.caption(
            "The application authentication configuration is missing or incomplete. "
            "Please contact the workspace administrator."
        )
        st.info(
            "Expected environment variables with prefix `STREAMLIT_OIDC_` "
            "to be set via Databricks Apps managed secrets."
        )
    else:
        st.caption(
            "The app is running in Databricks fallback auth mode, but no forwarded "
            "workspace identity header was found."
        )
        st.info(
            "Expected header `x-forwarded-user` from Databricks Apps ingress. "
            "Check app authorization and workspace configuration."
        )


def render_access_denied_screen(reason: str | None = None) -> None:
    st.markdown("### Access Denied")
    st.caption("Your account is not authorized to access this application.")
    if reason:
        st.info(f"Reason: {reason}")


def render_header_mode_banner() -> None:
    reason = _fallback_reason()
    message = "Running with Databricks header fallback authentication."
    if reason:
        message = f"{message} Reason: `{reason}`"
    st.warning(message)


def render_timeout_lock_screen() -> None:
    st.markdown("### Session Locked")
    st.caption(
        "Your app session timed out due to inactivity. "
        "Resume to continue with your Databricks workspace identity."
    )
    if st.button("Resume session", key="auth_resume_session_btn", type="primary"):
        st.session_state[SESSION_HEADER_LOCKED] = False
        st.session_state[SESSION_IS_AUTHENTICATED] = False
        st.session_state[SESSION_LAST_ACTIVITY] = time.time()
        st.rerun()


# ---------------------------------------------------------------------------
# Identity UI
# ---------------------------------------------------------------------------


def render_identity_ui(backend: str) -> None:
    display = (
        st.session_state.get(SESSION_USER_NAME, "")
        or st.session_state.get(SESSION_USER_EMAIL, "")
        or "Unknown"
    )
    provider = str(st.session_state.get(SESSION_PROVIDER, ""))
    provider_label = html.escape(PROVIDER_LABELS.get(provider, provider.title()))

    cols = st.columns([1, 1, 1, 1, 1, 1])
    with cols[0]:
        st.caption(f"Signed in as **{html.escape(display)}** via {provider_label}")
    with cols[5]:
        if st.button("Sign out", key="auth_signout_btn"):
            _write_audit_event(EVENT_LOGOUT, OUTCOME_SUCCESS)
            _clear_auth_session()
            if _is_oidc_backend(backend):
                try:
                    st.logout()
                except Exception:
                    logger.warning("st.logout() failed — cleared local state")
            else:
                st.session_state[SESSION_HEADER_LOCKED] = True
                st.rerun()


# ---------------------------------------------------------------------------
# Audit helper
# ---------------------------------------------------------------------------


def _write_audit_event(
    event_name: str,
    outcome: str,
    reason: str | None = None,
) -> None:
    try:
        from src.analytics.audit import insert_audit_event

        insert_audit_event(
            session_id=get_session_id(),
            user_sub=str(st.session_state.get(SESSION_USER_SUB, "")),
            user_email=str(st.session_state.get(SESSION_USER_EMAIL, "")),
            provider=str(st.session_state.get(SESSION_PROVIDER, "")),
            event_name=event_name,
            outcome=outcome,
            reason=reason,
        )
    except Exception:
        logger.warning(
            "Audit write failed event=%s outcome=%s — continuing",
            event_name,
            outcome,
            exc_info=True,
        )


def write_login_success_audit() -> None:
    _write_audit_event(EVENT_LOGIN_SUCCESS, OUTCOME_SUCCESS)


def write_access_denied_audit(reason: str | None = None) -> None:
    _write_audit_event(EVENT_ACCESS_DENIED, OUTCOME_FAILURE, reason=reason)


# ---------------------------------------------------------------------------
# Top-level auth gate (called from main())
# ---------------------------------------------------------------------------


def apply_auth_gate() -> str:
    backend = _runtime_auth_backend()
    init_auth_session(backend)

    if st.session_state.get(SESSION_AUTH_UNAVAILABLE):
        render_auth_unavailable_screen(backend)
        return "unavailable"

    if not _is_oidc_backend(backend) and st.session_state.get(SESSION_HEADER_LOCKED):
        render_timeout_lock_screen()
        return "timeout"

    is_fresh_login = _sync_st_user_to_session() if _is_oidc_backend(backend) else _sync_headers_to_session()

    if not check_inactivity_timeout(backend):
        return "timeout"

    if not st.session_state.get(SESSION_IS_AUTHENTICATED):
        if _is_oidc_backend(backend):
            render_login_screen()
            return "login"
        render_auth_unavailable_screen(backend)
        return "unavailable"

    user_email = str(st.session_state.get(SESSION_USER_EMAIL, ""))
    allowed, deny_reason = check_access_policy(user_email)
    if not allowed:
        st.session_state[SESSION_ACCESS_DENIED] = True
        write_access_denied_audit(deny_reason)
        render_access_denied_screen(deny_reason)
        return "denied"

    if is_fresh_login:
        write_login_success_audit()

    touch_activity()
    render_identity_ui(backend)
    if not _is_oidc_backend(backend):
        render_header_mode_banner()

    return "allowed"


__all__ = sorted(
    [
        "apply_auth_gate",
        "check_access_policy",
        "check_inactivity_timeout",
        "init_auth_session",
        "is_auth_available",
        "render_access_denied_screen",
        "render_auth_unavailable_screen",
        "render_identity_ui",
        "render_login_screen",
        "touch_activity",
        "write_access_denied_audit",
        "write_login_success_audit",
    ]
)
