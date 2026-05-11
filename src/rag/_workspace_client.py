from __future__ import annotations

from typing import Any

_ws: Any = None


def get_workspace_client() -> Any:
    global _ws
    if _ws is None:
        from databricks.sdk import WorkspaceClient

        _ws = WorkspaceClient()
    return _ws


def reset_workspace_client() -> None:
    global _ws
    _ws = None
