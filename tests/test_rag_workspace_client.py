from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from src.rag._workspace_client import get_workspace_client, reset_workspace_client


class TestWorkspaceClient(unittest.TestCase):
    def tearDown(self) -> None:
        reset_workspace_client()

    @patch("databricks.sdk.WorkspaceClient")
    def test_get_creates_client_once(self, mock_ws: MagicMock) -> None:
        mock_ws.return_value = MagicMock()
        c1 = get_workspace_client()
        c2 = get_workspace_client()
        self.assertIs(c1, c2)
        mock_ws.assert_called_once()

    @patch("databricks.sdk.WorkspaceClient")
    def test_reset_creates_new_client(self, mock_ws: MagicMock) -> None:
        mock_ws.return_value = MagicMock()
        c1 = get_workspace_client()
        reset_workspace_client()
        mock_ws.reset_mock()
        mock_ws.return_value = MagicMock()
        c2 = get_workspace_client()
        self.assertIsNot(c1, c2)
        mock_ws.assert_called_once()


if __name__ == "__main__":
    unittest.main()
