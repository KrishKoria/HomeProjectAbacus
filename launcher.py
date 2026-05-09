"""Startup launcher — generates ``.streamlit/secrets.toml`` from Databricks-managed
environment variables, validates OIDC configuration, then launches Streamlit.

Databricks Apps entrypoint: ``python launcher.py`` (configured in ``app.yaml``).

Signal handling: forwards SIGTERM to the Streamlit child process so the app
shuts down within the Databricks Apps 15-second grace period.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Final

from src.common.auth_config import (
    AUTH_BACKEND_HEADERS,
    AUTH_BACKEND_OIDC,
    AUTH_MODE_AUTO,
    AUTH_MODE_HEADERS_ONLY,
    AUTH_MODE_OIDC_ONLY,
    auth_backend_key,
    auth_fallback_reason_key,
    auth_mode_key,
    normalize_auth_mode,
    provider_meta,
)

_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
_SECRETS_DIR: Final[Path] = _PROJECT_ROOT / ".streamlit"
_SECRETS_PATH: Final[Path] = _SECRETS_DIR / "secrets.toml"

# ---------------------------------------------------------------------------
# OIDC environment variable contract (provider-config-driven)
# ---------------------------------------------------------------------------
# Shared (required for bootstrap):
#   STREAMLIT_OIDC_ENABLED_PROVIDERS — comma-separated provider keys, e.g. "google"
#
# Per-provider (example for "google"):
#   STREAMLIT_OIDC_GOOGLE_CLIENT_ID
#   STREAMLIT_OIDC_GOOGLE_CLIENT_SECRET
#   STREAMLIT_OIDC_GOOGLE_REDIRECT_URI
#   STREAMLIT_OIDC_GOOGLE_SERVER_METADATA_URL  (optional)
#
# The launcher maps every ``STREAMLIT_OIDC_<PROVIDER>_<KEY>`` env var into
# ``[auth.<provider>.<key_lower>]`` in secrets.toml so Streamlit native OIDC
# picks it up.
# ---------------------------------------------------------------------------

_ENV_PREFIX: Final[str] = "STREAMLIT_OIDC_"
_ENABLED_PROVIDERS_KEY: Final[str] = f"{_ENV_PREFIX}ENABLED_PROVIDERS"

# Shared keys — not provider-specific, parsed separately.
_SHARED_KEYS: Final[frozenset[str]] = frozenset({"COOKIE_SECRET", "REDIRECT_URI"})


def _is_shared_key(stripped: str) -> bool:
    return stripped.upper() in _SHARED_KEYS


def _collect_provider_env_vars() -> dict[str, dict[str, str]]:
    """Scan environment for ``STREAMLIT_OIDC_<PROVIDER>_<KEY>`` and group by provider.

    Shared (non-provider) keys like ``COOKIE_SECRET`` are skipped — they are read
    directly in ``_build_auth_config``.
    """
    providers: dict[str, dict[str, str]] = {}
    prefix_len = len(_ENV_PREFIX)
    for key, value in os.environ.items():
        if not key.startswith(_ENV_PREFIX) or key == _ENABLED_PROVIDERS_KEY:
            continue
        stripped = key[prefix_len:]
        if _is_shared_key(stripped):
            continue
        parts = stripped.split("_", 1)
        if len(parts) != 2:
            continue
        provider, attr = parts
        providers.setdefault(provider.lower(), {})[attr.lower()] = value
    return providers


def _validate_providers(providers: dict[str, dict[str, str]]) -> list[str]:
    """Ensure every enabled provider has at minimum ``client_id`` and ``client_secret``."""
    missing: list[str] = []
    for provider, attrs in providers.items():
        for required in ("client_id", "client_secret", "redirect_uri"):
            if required not in attrs or not attrs[required].strip():
                missing.append(f"STREAMLIT_OIDC_{provider.upper()}_{required.upper()}")
    return missing


def _auth_mode() -> str:
    return normalize_auth_mode(os.getenv(auth_mode_key()))


def _build_auth_config(providers: dict[str, dict[str, str]]) -> dict[str, object]:
    """Build the Streamlit-native ``[auth]`` config dict from provider env vars."""
    providers = _apply_provider_defaults(providers)
    first_provider = next(iter(providers.values()), {})
    config: dict[str, object] = {
        "redirect_uri": first_provider.get("redirect_uri", ""),
        "cookie_secret": os.getenv(f"{_ENV_PREFIX}COOKIE_SECRET")
        or first_provider.get("client_secret", ""),
    }
    for provider, attrs in providers.items():
        provider_config: dict[str, str] = {}
        for key, val in attrs.items():
            if key == "redirect_uri" and provider == list(providers.keys())[0]:
                continue
            provider_config[key] = val
        config[provider] = provider_config
    return config


def _apply_provider_defaults(
    providers: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Backfill known provider defaults required by Streamlit OIDC."""
    normalized: dict[str, dict[str, str]] = {}
    for provider, attrs in providers.items():
        merged = dict(attrs)
        if not merged.get("server_metadata_url"):
            default_metadata_url = provider_meta(provider).get("server_metadata_url", "").strip()
            if default_metadata_url:
                merged["server_metadata_url"] = default_metadata_url
        normalized[provider] = merged
    return normalized


def _fetch_json(url: str, timeout_seconds: float = 6.0) -> dict[str, object]:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"HTTP {response.status} for {url}")
        payload = response.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise RuntimeError(f"Expected JSON object from {url}")
        return data


def _check_endpoint_reachability(url: str, timeout_seconds: float = 5.0) -> tuple[bool, str]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return True, f"ok:{response.status}"
    except urllib.error.HTTPError as exc:
        # Non-2xx still proves DNS + TLS + routing work for this host.
        return True, f"http_error:{exc.code}"
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        return False, f"url_error:{reason}"
    except socket.timeout as exc:
        return False, f"timeout:{exc}"
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"error:{exc}"


def _oidc_preflight(providers: dict[str, dict[str, str]]) -> tuple[bool, str]:
    required_fields = ("authorization_endpoint", "token_endpoint", "jwks_uri")
    for provider, attrs in providers.items():
        metadata_url = str(attrs.get("server_metadata_url", "")).strip()
        if not metadata_url:
            return False, f"{provider}:missing_server_metadata_url"
        try:
            metadata = _fetch_json(metadata_url)
        except Exception as exc:
            return False, f"{provider}:discovery_unreachable:{exc}"
        for field in required_fields:
            endpoint_url = str(metadata.get(field, "")).strip()
            if not endpoint_url:
                return False, f"{provider}:missing_{field}"
            ok, detail = _check_endpoint_reachability(endpoint_url)
            if not ok:
                return False, f"{provider}:{field}_unreachable:{detail}"
    return True, "oidc_preflight_ok"


def _set_auth_backend(backend: str, reason: str = "") -> None:
    os.environ[auth_backend_key()] = backend
    os.environ[auth_fallback_reason_key()] = reason


def _start_headers_fallback(reason: str) -> None:
    _set_auth_backend(AUTH_BACKEND_HEADERS, reason)
    print(
        f"[launcher] Starting in Databricks header fallback mode: {reason}",
        file=sys.stderr,
    )
    _run_streamlit()


def _write_secrets(config: dict[str, object]) -> None:
    """Write ``.streamlit/secrets.toml`` with the ``[auth]`` section."""
    _SECRETS_DIR.mkdir(exist_ok=True)
    with open(_SECRETS_PATH, "w", encoding="utf-8") as fh:
        fh.write("# Generated by launcher.py — do not commit.\n")
        fh.write("[auth]\n")
        for key, value in config.items():
            if isinstance(value, dict):
                for sub_key, sub_val in value.items():
                    escaped = str(sub_val).replace("\\", "\\\\").replace('"', '\\"')
                    fh.write(f'{key}.{sub_key} = "{escaped}"\n')
            else:
                escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
                fh.write(f'{key} = "{escaped}"\n')


def _enabled_providers() -> list[str]:
    raw = os.getenv(_ENABLED_PROVIDERS_KEY, "")
    if not raw.strip():
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


def _run_streamlit() -> None:
    """Launch Streamlit as a child process and forward signals for clean shutdown."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app_streamlit.py"],
    )

    def _forward_signal(signum: int, _frame: object) -> None:
        proc.send_signal(signum)

    signal.signal(signal.SIGTERM, _forward_signal)
    proc.wait()


def main() -> None:
    mode = _auth_mode()

    if mode == AUTH_MODE_HEADERS_ONLY:
        _start_headers_fallback("headers_only_mode")
        return

    enabled = _enabled_providers()
    if not enabled:
        message = f"{_ENABLED_PROVIDERS_KEY}_empty"
        if mode == AUTH_MODE_OIDC_ONLY:
            print(
                f"[launcher] ERROR: {message} while auth mode is {AUTH_MODE_OIDC_ONLY}",
                file=sys.stderr,
            )
            sys.exit(1)
        _start_headers_fallback(message)
        return

    providers = _collect_provider_env_vars()
    missing = _validate_providers(providers)
    if missing:
        print(
            f"[launcher] ERROR: Missing required OIDC environment variables: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    providers = _apply_provider_defaults(providers)
    preflight_ok, preflight_detail = _oidc_preflight(providers)
    if not preflight_ok:
        if mode == AUTH_MODE_OIDC_ONLY:
            print(
                f"[launcher] ERROR: OIDC preflight failed in {AUTH_MODE_OIDC_ONLY}: {preflight_detail}",
                file=sys.stderr,
            )
            sys.exit(1)
        _start_headers_fallback(f"oidc_preflight_failed:{preflight_detail}")
        return

    _write_secrets(_build_auth_config(providers))
    _set_auth_backend(AUTH_BACKEND_OIDC, "")
    print(
        (
            f"[launcher] OIDC preflight passed. Wrote OIDC config for providers: "
            f"{', '.join(sorted(providers))}"
        ),
        file=sys.stderr,
    )
    _run_streamlit()


if __name__ == "__main__":
    main()
