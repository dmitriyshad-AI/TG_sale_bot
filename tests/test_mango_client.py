import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from sales_agent.sales_core.mango_client import MangoClient, MangoClientError


class _FakeResponse:
    def __init__(self, body: dict, status: int = 200):
        self._body = json.dumps(body).encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeTextResponse:
    def __init__(self, body: str, status: int = 200):
        self._body = body.encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self) -> None:
        return None


class MangoClientTests(unittest.TestCase):
    def test_build_request_raises_when_not_configured(self) -> None:
        client = MangoClient(base_url="", token="")
        with self.assertRaises(MangoClientError):
            client._build_request(path="/calls", method="GET")

    def test_build_request_post_sets_json_headers(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        req = client._build_request(path="calls", method="POST", params={"a": "b"})
        self.assertEqual(req.get_method(), "POST")
        self.assertTrue(req.full_url.endswith("/calls"))
        self.assertEqual(req.headers.get("Authorization"), "Bearer token")
        self.assertEqual(req.get_header("Content-type"), "application/json")
        self.assertEqual(req.data, b'{"a":"b"}')

    def test_verify_webhook_signature(self) -> None:
        client = MangoClient(
            base_url="https://mango.example/api",
            token="token",
            webhook_secret="secret",
        )
        body = b'{"event":"call"}'
        import hmac
        import hashlib

        digest = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
        self.assertTrue(client.verify_webhook_signature(raw_body=body, signature=digest))
        self.assertFalse(client.verify_webhook_signature(raw_body=body, signature="bad-sign"))

    def test_verify_webhook_signature_allows_when_secret_empty(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token", webhook_secret="")
        self.assertTrue(client.verify_webhook_signature(raw_body=b"{}", signature=""))

    def test_verify_webhook_signature_rejects_empty_when_secret_required(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token", webhook_secret="s")
        self.assertFalse(client.verify_webhook_signature(raw_body=b"{}", signature=""))

    def test_parse_call_event_returns_none_for_non_call_event(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        event = client.parse_call_event({"event": "status_changed", "data": {"id": "x"}})
        self.assertIsNone(event)

    def test_parse_call_event_extracts_fields(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        payload = {
            "event": "call_recording_ready",
            "event_id": "evt-1",
            "data": {
                "call_id": "call-77",
                "phone": "+79990000000",
                "recording_url": "https://cdn.example/rec.mp3",
                "summary": "Клиент хочет консультацию",
            },
        }
        event = client.parse_call_event(payload)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.event_id, "evt-1")
        self.assertEqual(event.call_id, "call-77")
        self.assertEqual(event.phone, "+79990000000")
        self.assertEqual(event.recording_url, "https://cdn.example/rec.mp3")
        self.assertIn("консультацию", event.transcript_hint)

    def test_parse_call_event_supports_call_wrapper(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        payload = {
            "type": "voip_record",
            "call": {
                "id": "call-88",
                "audio_url": "https://cdn.example/rec88.mp3",
                "caller": "+79995554433",
            },
        }
        event = client.parse_call_event(payload)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.call_id, "call-88")
        self.assertEqual(event.recording_url, "https://cdn.example/rec88.mp3")
        self.assertEqual(event.phone, "+79995554433")

    def test_parse_call_event_generates_event_id_when_missing(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        payload = {
            "event": "call_recording_ready",
            "data": {
                "call_id": "call-99",
                "recording_url": "https://cdn.example/rec99.mp3",
            },
        }
        event = client.parse_call_event(payload)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertTrue(event.event_id)
        self.assertEqual(event.call_id, "call-99")

    def test_parse_call_event_returns_none_when_neither_call_nor_recording(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        self.assertIsNone(client.parse_call_event({"event": "call_recording_ready", "data": {"phone": "+7"}}))
        self.assertIsNone(client.parse_call_event("not-a-dict"))  # type: ignore[arg-type]

    def test_list_recent_calls(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token", calls_path="vpbx/calls")
        payload = {
            "items": [
                {
                    "event": "call_recording_ready",
                    "event_id": "evt-1",
                    "data": {
                        "call_id": "call-77",
                        "phone": "+79990000000",
                        "recording_url": "https://cdn.example/rec.mp3",
                    },
                }
            ]
        }
        with patch("urllib.request.urlopen", return_value=_FakeResponse(payload)) as mock_urlopen:
            events = client.list_recent_calls(since_iso="2026-01-01T00:00:00Z", limit=20)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].call_id, "call-77")
        req = mock_urlopen.call_args.args[0]
        self.assertIn("/vpbx/calls", req.full_url)
        self.assertIn("since=2026-01-01T00%3A00%3A00Z", req.full_url)
        self.assertIn("limit=20", req.full_url)

    def test_list_recent_calls_returns_empty_when_items_not_list(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        with patch("urllib.request.urlopen", return_value=_FakeResponse({"items": "oops"})):
            events = client.list_recent_calls()
        self.assertEqual(events, [])

    def test_list_recent_calls_ignores_non_dict_items(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        payload = {
            "items": [
                "raw",
                {"event": "call_recording_ready", "data": {"call_id": "c1", "recording_url": "https://a"}},
            ]
        }
        with patch("urllib.request.urlopen", return_value=_FakeResponse(payload)):
            events = client.list_recent_calls()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].call_id, "c1")

    def test_list_recent_calls_raises_on_http_error(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        http_error = HTTPError(
            url="https://mango.example/api/calls",
            code=500,
            msg="server error",
            hdrs=None,
            fp=_FakeTextResponse("boom"),
        )
        with patch("urllib.request.urlopen", side_effect=http_error):
            with self.assertRaises(MangoClientError):
                client.list_recent_calls()

    def test_list_recent_calls_raises_on_connection_error(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        with patch("urllib.request.urlopen", side_effect=URLError("dns down")):
            with self.assertRaises(MangoClientError):
                client.list_recent_calls(since_iso="", limit=10)

    def test_list_recent_calls_raises_on_invalid_json(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        with patch("urllib.request.urlopen", return_value=_FakeTextResponse("not-json")):
            with self.assertRaises(MangoClientError):
                client.list_recent_calls()

    def test_list_recent_calls_raises_on_non_object_json(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        with patch("urllib.request.urlopen", return_value=_FakeTextResponse("[]")):
            with self.assertRaises(MangoClientError):
                client.list_recent_calls()

    def test_list_recent_calls_raises_on_unexpected_status(self) -> None:
        client = MangoClient(base_url="https://mango.example/api", token="token")
        with patch("urllib.request.urlopen", return_value=_FakeResponse({"items": []}, status=301)):
            with self.assertRaises(MangoClientError):
                client.list_recent_calls()


if __name__ == "__main__":
    unittest.main()
