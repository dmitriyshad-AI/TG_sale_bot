import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import HTTPException

from sales_agent.sales_api.services.crm_dependencies import CrmDependencyService


class CrmDependencyServiceTests(unittest.TestCase):
    def _make_service(self, **settings_overrides):
        settings = SimpleNamespace(
            tallanto_read_only=True,
            tallanto_api_url="https://crm.example/api",
            tallanto_api_token="token-123",
            tallanto_api_key="legacy-key",
            enable_tallanto_enrichment=True,
            crm_provider="tallanto",
            tallanto_default_contact_module="contacts",
        )
        for key, value in settings_overrides.items():
            setattr(settings, key, value)

        return CrmDependencyService(
            settings=settings,
            database_path="/tmp/test.db",
            cache_ttl_seconds=3600,
            get_connection=Mock(name="get_connection"),
            get_crm_cache=Mock(name="get_crm_cache"),
            upsert_crm_cache=Mock(name="upsert_crm_cache"),
        )

    def test_require_tallanto_readonly_client_requires_readonly_mode(self) -> None:
        service = self._make_service(tallanto_read_only=False)

        with self.assertRaises(HTTPException) as ctx:
            service.require_tallanto_readonly_client()

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertIn("read-only mode is disabled", str(ctx.exception.detail).lower())

    def test_require_tallanto_readonly_client_requires_full_config(self) -> None:
        service = self._make_service(tallanto_api_url="", tallanto_api_token="", tallanto_api_key="")

        with self.assertRaises(HTTPException) as ctx:
            service.require_tallanto_readonly_client()

        self.assertEqual(ctx.exception.status_code, 503)
        self.assertIn("config is incomplete", str(ctx.exception.detail).lower())

    def test_require_tallanto_readonly_client_uses_token_precedence(self) -> None:
        client_cls = Mock(return_value={"ok": True})
        service = self._make_service()
        service.client_cls = client_cls

        result = service.require_tallanto_readonly_client()

        self.assertEqual(result, {"ok": True})
        client_cls.assert_called_once_with(base_url="https://crm.example/api", token="token-123")

    def test_crm_cache_key_delegates(self) -> None:
        service = self._make_service()
        crm_cache_key_fn = Mock(return_value="crm:key")
        service.crm_cache_key_fn = crm_cache_key_fn

        key = service.crm_cache_key("modules", {"x": 1})

        self.assertEqual(key, "crm:key")
        crm_cache_key_fn.assert_called_once_with("modules", {"x": 1})

    def test_read_crm_cache_delegates_with_wiring(self) -> None:
        service = self._make_service()
        read_fn = Mock(return_value={"cached": True})
        service.read_crm_cache_fn = read_fn

        payload = service.read_crm_cache("crm:modules")

        self.assertEqual(payload, {"cached": True})
        read_fn.assert_called_once_with(
            database_path="/tmp/test.db",
            key="crm:modules",
            max_age_seconds=3600,
            get_connection=service.get_connection,
            get_crm_cache=service.get_crm_cache,
        )

    def test_write_crm_cache_delegates_with_wiring(self) -> None:
        service = self._make_service()
        write_fn = Mock()
        service.write_crm_cache_fn = write_fn

        service.write_crm_cache("crm:modules", {"items": ["contacts"]})

        write_fn.assert_called_once_with(
            database_path="/tmp/test.db",
            key="crm:modules",
            payload={"items": ["contacts"]},
            get_connection=service.get_connection,
            upsert_crm_cache=service.upsert_crm_cache,
        )

    def test_map_tallanto_error_delegates(self) -> None:
        service = self._make_service()
        mapped = HTTPException(status_code=400, detail="boom")
        map_fn = Mock(return_value=mapped)
        service.map_tallanto_error_fn = map_fn

        result = service.map_tallanto_error(RuntimeError("boom"))

        self.assertIs(result, mapped)
        map_fn.assert_called_once()

    def test_build_thread_crm_context_uses_internal_read_write_functions(self) -> None:
        service = self._make_service()

        read_fn = Mock(return_value={"cached": "value"})
        write_fn = Mock()
        service.read_crm_cache_fn = read_fn
        service.write_crm_cache_fn = write_fn

        def fake_builder(user_item, *, settings, read_cache, write_cache):
            cached = read_cache("k1")
            write_cache("k2", {"ok": True})
            return {
                "enabled": True,
                "found": True,
                "cached": cached,
                "user": user_item,
                "provider": settings.crm_provider,
            }

        service.build_thread_crm_context_fn = fake_builder

        payload = service.build_thread_crm_context({"user_id": 10})

        self.assertTrue(payload["enabled"])
        self.assertTrue(payload["found"])
        self.assertEqual(payload["provider"], "tallanto")
        self.assertEqual(payload["cached"], {"cached": "value"})

        read_fn.assert_called_once_with(
            database_path="/tmp/test.db",
            key="k1",
            max_age_seconds=3600,
            get_connection=service.get_connection,
            get_crm_cache=service.get_crm_cache,
        )
        write_fn.assert_called_once_with(
            database_path="/tmp/test.db",
            key="k2",
            payload={"ok": True},
            get_connection=service.get_connection,
            upsert_crm_cache=service.upsert_crm_cache,
        )


if __name__ == "__main__":
    unittest.main()
