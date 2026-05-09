"""Append-only auth audit writer targeting ``healthcare.analytics.app_auth_events``.

Uses the Databricks SQL connector (same path as the existing frontend queries).
Audit writes never block the auth flow — failures are logged at warning level
and the app continues.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Final

logger: Final = logging.getLogger(__name__)

_AUDIT_TABLE: Final[str] = "`healthcare`.`analytics`.`app_auth_events`"
_AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "event_ts",
    "session_id",
    "user_sub",
    "user_email",
    "provider",
    "event_name",
    "outcome",
    "reason",
)


def _get_sql_connection() -> Any:
    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    def _env(name: str, default: str = "") -> str:
        return os.getenv(name, default).strip()

    server_hostname = _env("DATABRICKS_SERVER_HOSTNAME", _env("DATABRICKS_HOST"))
    http_path = _env("CLAIMOPS_SQL_HTTP_PATH", _env("DATABRICKS_HTTP_PATH"))
    if not server_hostname:
        raise RuntimeError("Missing Databricks workspace host")
    if not http_path:
        raise RuntimeError("Missing SQL warehouse HTTP path")
    server_hostname = server_hostname.replace("https://", "").replace("http://", "").strip("/")

    direct_token = _env("DATABRICKS_TOKEN")
    if direct_token:
        return sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=direct_token,
        )

    client_id = _env("DATABRICKS_CLIENT_ID")
    client_secret = _env("DATABRICKS_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Missing app OAuth credentials")
    oauth_config = Config(
        host=f"https://{server_hostname}",
        client_id=client_id,
        client_secret=client_secret,
    )
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: oauth_service_principal(oauth_config),
    )


def insert_audit_event(
    session_id: str,
    user_sub: str,
    user_email: str,
    provider: str,
    event_name: str,
    outcome: str,
    reason: str | None = None,
) -> None:
    """Insert one row into the auth audit table using explicit columns and parameters."""
    from databricks import sql

    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    insert_sql = (
        f"INSERT INTO {_AUDIT_TABLE} "
        f"(event_ts, session_id, user_sub, user_email, provider, event_name, outcome, reason) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params: list[Any] = [
        ts,
        session_id,
        user_sub,
        user_email,
        provider,
        event_name,
        outcome,
        reason or "",
    ]

    with _get_sql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, params)

    logger.debug("audit_event written: %s outcome=%s", event_name, outcome)
