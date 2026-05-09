"""Tests for launcher auth-mode selection and auth gateway behavior."""
from __future__ import annotations

import os
import time
import unittest
from collections.abc import Mapping
from types import SimpleNamespace
from unittest import mock


class LauncherAuthModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_patcher = mock.patch.dict(os.environ, {}, clear=True)
        self._env_patcher.start()

    def tearDown(self) -> None:
        self._env_patcher.stop()

    def _seed_google_oidc_env(self) -> None:
        os.environ["STREAMLIT_OIDC_ENABLED_PROVIDERS"] = "google"
        os.environ["STREAMLIT_OIDC_GOOGLE_CLIENT_ID"] = "gcid"
        os.environ["STREAMLIT_OIDC_GOOGLE_CLIENT_SECRET"] = "gcsec"
        os.environ["STREAMLIT_OIDC_GOOGLE_REDIRECT_URI"] = "https://example.com/oauth2callback"
        os.environ["STREAMLIT_OIDC_COOKIE_SECRET"] = "cookie"

    def test_headers_only_mode_starts_fallback_backend(self) -> None:
        import launcher

        os.environ["CLAIMOPS_AUTH_MODE"] = "headers_only"
        with mock.patch.object(launcher, "_run_streamlit") as run_mock:
            launcher.main()
        self.assertEqual(os.environ["CLAIMOPS_AUTH_BACKEND"], "headers")
        self.assertEqual(os.environ["CLAIMOPS_AUTH_FALLBACK_REASON"], "headers_only_mode")
        run_mock.assert_called_once()

    def test_auto_mode_falls_back_when_oidc_preflight_fails(self) -> None:
        import launcher

        self._seed_google_oidc_env()
        with (
            mock.patch.object(
                launcher,
                "_oidc_preflight",
                return_value=(False, "google:discovery_unreachable"),
            ),
            mock.patch.object(launcher, "_run_streamlit") as run_mock,
        ):
            launcher.main()
        self.assertEqual(os.environ["CLAIMOPS_AUTH_BACKEND"], "headers")
        self.assertIn("oidc_preflight_failed", os.environ["CLAIMOPS_AUTH_FALLBACK_REASON"])
        run_mock.assert_called_once()

    def test_oidc_only_mode_fails_closed_on_preflight_error(self) -> None:
        import launcher

        os.environ["CLAIMOPS_AUTH_MODE"] = "oidc_only"
        self._seed_google_oidc_env()
        with mock.patch.object(
            launcher,
            "_oidc_preflight",
            return_value=(False, "google:discovery_unreachable"),
        ):
            with self.assertRaises(SystemExit):
                launcher.main()

    def test_auto_mode_sets_oidc_backend_on_success(self) -> None:
        import launcher

        self._seed_google_oidc_env()
        with (
            mock.patch.object(launcher, "_oidc_preflight", return_value=(True, "ok")),
            mock.patch.object(launcher, "_write_secrets"),
            mock.patch.object(launcher, "_run_streamlit"),
        ):
            launcher.main()
        self.assertEqual(os.environ["CLAIMOPS_AUTH_BACKEND"], "oidc")
        self.assertEqual(os.environ["CLAIMOPS_AUTH_FALLBACK_REASON"], "")


class AuthConfigTests(unittest.TestCase):
    def test_normalize_auth_mode_defaults_to_auto(self) -> None:
        from src.common.auth_config import normalize_auth_mode

        self.assertEqual(normalize_auth_mode(None), "auto")
        self.assertEqual(normalize_auth_mode("unknown"), "auto")

    def test_normalize_auth_mode_accepts_known_modes(self) -> None:
        from src.common.auth_config import normalize_auth_mode

        self.assertEqual(normalize_auth_mode("auto"), "auto")
        self.assertEqual(normalize_auth_mode("oidc_only"), "oidc_only")
        self.assertEqual(normalize_auth_mode("headers_only"), "headers_only")


class AuthGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_patcher = mock.patch.dict(os.environ, {}, clear=True)
        self._env_patcher.start()
        import src.analytics.auth as auth_mod

        self.auth_mod = auth_mod

    def tearDown(self) -> None:
        self._env_patcher.stop()

    def _fake_st(self, headers: dict[str, str] | None = None) -> SimpleNamespace:
        return SimpleNamespace(
            context=SimpleNamespace(headers=headers or {}),
            secrets=SimpleNamespace(get=lambda _k: {}),
            session_state={},
            user=None,
            button=lambda *args, **kwargs: False,
            markdown=lambda *args, **kwargs: None,
            caption=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
            columns=lambda _layout: [SimpleNamespace(__enter__=lambda self: self, __exit__=lambda *a: None)] * 6,
            logout=lambda: None,
            rerun=lambda: None,
        )

    def test_get_auth_config_accepts_mapping_sections(self) -> None:
        class FakeMapping(Mapping[str, object]):
            def __init__(self, data: dict[str, object]) -> None:
                self._data = data

            def __getitem__(self, key: str) -> object:
                value = self._data[key]
                if isinstance(value, dict):
                    return FakeMapping(value)
                return value

            def __iter__(self):
                return iter(self._data)

            def __len__(self) -> int:
                return len(self._data)

            def items(self):
                for key in self._data:
                    yield key, self[key]

        section = FakeMapping(
            {
                "redirect_uri": "https://example.com/oauth2callback",
                "cookie_secret": "cookie",
                "google": {
                    "client_id": "gcid",
                    "client_secret": "gcsec",
                    "server_metadata_url": "https://accounts.google.com/.well-known/openid-configuration",
                },
            }
        )
        fake_st = self._fake_st()
        fake_st.secrets = SimpleNamespace(get=lambda _k: section)
        with mock.patch.object(self.auth_mod, "st", fake_st):
            config = self.auth_mod._get_auth_config()
            self.assertEqual(config["google"]["client_id"], "gcid")
            self.assertTrue(self.auth_mod.is_auth_available())

    def test_header_backend_bootstraps_session_from_forwarded_headers(self) -> None:
        os.environ["CLAIMOPS_AUTH_BACKEND"] = "headers"
        os.environ["CLAIMOPS_AUTH_FALLBACK_REASON"] = "oidc_preflight_failed:dns"
        fake_st = self._fake_st(
            headers={
                "x-forwarded-user": "u123",
                "x-forwarded-email": "a@b.com",
                "x-forwarded-preferred-username": "Analyst A",
            }
        )
        with (
            mock.patch.object(self.auth_mod, "st", fake_st),
            mock.patch.object(self.auth_mod, "check_access_policy", return_value=(True, None)),
            mock.patch.object(self.auth_mod, "write_login_success_audit"),
            mock.patch.object(self.auth_mod, "render_identity_ui"),
            mock.patch.object(self.auth_mod, "render_header_mode_banner"),
        ):
            result = self.auth_mod.apply_auth_gate()
        self.assertEqual(result, "allowed")
        self.assertEqual(fake_st.session_state[self.auth_mod.SESSION_USER_SUB], "u123")
        self.assertEqual(fake_st.session_state[self.auth_mod.SESSION_PROVIDER], "databricks_headers")

    def test_header_backend_unavailable_without_forwarded_identity(self) -> None:
        os.environ["CLAIMOPS_AUTH_BACKEND"] = "headers"
        fake_st = self._fake_st(headers={})
        with (
            mock.patch.object(self.auth_mod, "st", fake_st),
            mock.patch.object(self.auth_mod, "render_auth_unavailable_screen") as unavailable_mock,
        ):
            result = self.auth_mod.apply_auth_gate()
        self.assertEqual(result, "unavailable")
        unavailable_mock.assert_called()

    def test_timeout_headers_backend_sets_lock(self) -> None:
        fake_st = self._fake_st()
        fake_st.session_state = {
            self.auth_mod.SESSION_IS_AUTHENTICATED: True,
            self.auth_mod.SESSION_LAST_ACTIVITY: time.time() - 901,
            self.auth_mod.SESSION_HEADER_LOCKED: False,
        }
        with (
            mock.patch.object(self.auth_mod, "st", fake_st),
            mock.patch.object(self.auth_mod, "_write_audit_event"),
        ):
            result = self.auth_mod.check_inactivity_timeout("headers")
        self.assertFalse(result)
        self.assertTrue(fake_st.session_state[self.auth_mod.SESSION_HEADER_LOCKED])

    def test_timeout_oidc_backend_attempts_logout(self) -> None:
        fake_st = self._fake_st()
        fake_st.session_state = {
            self.auth_mod.SESSION_IS_AUTHENTICATED: True,
            self.auth_mod.SESSION_LAST_ACTIVITY: time.time() - 901,
        }
        logout = mock.Mock()
        fake_st.logout = logout
        with (
            mock.patch.object(self.auth_mod, "st", fake_st),
            mock.patch.object(self.auth_mod, "_write_audit_event"),
        ):
            result = self.auth_mod.check_inactivity_timeout("oidc")
        self.assertFalse(result)
        logout.assert_called_once()


if __name__ == "__main__":
    unittest.main()
