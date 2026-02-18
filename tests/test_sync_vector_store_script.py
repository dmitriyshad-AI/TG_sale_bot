import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from sales_agent.sales_core.config import Settings
from sales_agent.sales_core.vector_store import read_vector_store_meta
from scripts import sync_vector_store


class SyncVectorStoreScriptTests(unittest.TestCase):
    def _settings(
        self,
        root: Path,
        knowledge_path: Path,
        meta_path: Path,
        openai_api_key: str,
        openai_vector_store_id: str = "",
    ) -> Settings:
        return Settings(
            telegram_bot_token="",
            openai_api_key=openai_api_key,
            openai_model="gpt-4.1",
            tallanto_api_url="",
            tallanto_api_key="",
            brand_default="kmipt",
            database_path=root / "sales_agent.db",
            catalog_path=root / "products.yaml",
            knowledge_path=knowledge_path,
            vector_store_meta_path=meta_path,
            openai_vector_store_id=openai_vector_store_id,
            admin_user="",
            admin_pass="",
        )

    class _MockHTTPResponse:
        def __init__(self, body: str) -> None:
            self._body = body

        def read(self) -> bytes:
            return self._body.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    @patch("scripts.sync_vector_store.urlopen")
    def test_request_json_post_parses_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = self._MockHTTPResponse('{"id":"ok"}')
        payload = sync_vector_store._request_json("https://api.example/test", "k1", {"x": 1})
        self.assertEqual(payload["id"], "ok")
        sent_request = mock_urlopen.call_args.args[0]
        self.assertEqual(sent_request.get_method(), "POST")

    @patch("scripts.sync_vector_store.urlopen")
    def test_request_json_get_parses_response(self, mock_urlopen) -> None:
        mock_urlopen.return_value = self._MockHTTPResponse('{"data":[]}')
        payload = sync_vector_store._request_json_get("https://api.example/test", "k1")
        self.assertEqual(payload["data"], [])
        sent_request = mock_urlopen.call_args.args[0]
        self.assertEqual(sent_request.get_method(), "GET")

    def test_create_vector_store_raises_when_id_missing(self) -> None:
        with patch.object(sync_vector_store, "_request_json", return_value={"name": "test"}):
            with self.assertRaises(RuntimeError):
                sync_vector_store._create_vector_store(api_key="k1", name="sales")

    def test_upload_file_raises_when_response_has_no_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "x.md"
            file_path.write_text("hello", encoding="utf-8")
            with patch("scripts.sync_vector_store.urlopen", return_value=self._MockHTTPResponse('{"status":"ok"}')):
                with self.assertRaises(RuntimeError):
                    sync_vector_store._upload_file(api_key="k1", file_path=file_path)

    @patch("scripts.sync_vector_store.urlopen")
    def test_delete_file_from_vector_store_uses_delete_method(self, mock_urlopen) -> None:
        mock_urlopen.return_value = self._MockHTTPResponse("")
        sync_vector_store._delete_file_from_vector_store(
            api_key="k1",
            vector_store_id="vs_1",
            file_id="file_1",
        )
        sent_request = mock_urlopen.call_args.args[0]
        self.assertEqual(sent_request.get_method(), "DELETE")

    def test_main_fails_when_api_key_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            (knowledge_dir / "faq_general.md").write_text("FAQ", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="",
            )
            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ):
                result = sync_vector_store.main()
            self.assertEqual(result, 1)

    def test_main_fails_when_knowledge_dir_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing_knowledge = root / "knowledge_missing"
            meta_path = root / "data" / "vector_store.json"
            args = argparse.Namespace(
                knowledge_dir=missing_knowledge,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=missing_knowledge,
                meta_path=meta_path,
                openai_api_key="test-key",
            )
            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ):
                result = sync_vector_store.main()
            self.assertEqual(result, 1)

    def test_main_fails_when_knowledge_has_no_supported_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            (knowledge_dir / "ignore.jpg").write_text("x", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"
            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )
            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ):
                result = sync_vector_store.main()
            self.assertEqual(result, 1)

    def test_main_returns_error_on_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            (knowledge_dir / "faq_general.md").write_text("FAQ", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"
            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )
            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(
                sync_vector_store,
                "_create_vector_store",
                side_effect=HTTPError(
                    url="https://api.openai.com/v1/vector_stores",
                    code=500,
                    msg="error",
                    hdrs=None,
                    fp=io.BytesIO(b""),
                ),
            ):
                result = sync_vector_store.main()
            self.assertEqual(result, 1)

    def test_main_returns_error_on_url_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            (knowledge_dir / "faq_general.md").write_text("FAQ", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"
            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )
            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(sync_vector_store, "_create_vector_store", side_effect=URLError("timed out")):
                result = sync_vector_store.main()
            self.assertEqual(result, 1)

    def test_index_existing_files_only_returns_valid_items(self) -> None:
        meta = {
            "vector_store_id": "vs_1",
            "files": [
                {"name": "faq.md", "file_id": "file_1", "sha256": "abc"},
                {"name": "", "file_id": "file_bad", "sha256": "x"},
                {"name": "payments.md", "file_id": "", "sha256": "y"},
                {"name": "camp.md", "file_id": "file_3", "sha256": ""},
                "invalid",
            ],
        }
        indexed = sync_vector_store._index_existing_files(meta, vector_store_id="vs_1")
        self.assertEqual(indexed, {"faq.md": {"file_id": "file_1", "sha256": "abc"}})

    def test_list_vector_store_files_handles_pagination(self) -> None:
        responses = [
            {"data": [{"id": "file_1", "filename": "faq.md"}], "has_more": True, "last_id": "cursor_1"},
            {"data": [{"file_id": "file_2", "filename": "payments.md"}], "has_more": False},
        ]
        with patch.object(sync_vector_store, "_request_json_get", side_effect=responses) as get_mock:
            items = sync_vector_store._list_vector_store_files(
                api_key="key",
                vector_store_id="vs_1",
            )
        self.assertEqual(
            items,
            [
                {"file_id": "file_1", "name": "faq.md"},
                {"file_id": "file_2", "name": "payments.md"},
            ],
        )
        self.assertEqual(get_mock.call_count, 2)

    def test_main_reuses_unchanged_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            faq = knowledge_dir / "faq_general.md"
            payments = knowledge_dir / "payments.md"
            faq.write_text("FAQ V1", encoding="utf-8")
            payments.write_text("Payments V1", encoding="utf-8")

            faq_sha = sync_vector_store._file_sha256(faq)
            meta_path = root / "data" / "vector_store.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(
                json.dumps(
                    {
                        "vector_store_id": "vs_existing",
                        "files": [
                            {
                                "name": "faq_general.md",
                                "file_id": "file_existing_faq",
                                "sha256": faq_sha,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(sync_vector_store, "_create_vector_store") as create_mock, patch.object(
                sync_vector_store, "_upload_file", return_value="file_new_payments"
            ) as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            create_mock.assert_not_called()
            upload_mock.assert_called_once_with(api_key="test-key", file_path=payments)
            attach_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_existing",
                file_id="file_new_payments",
            )

            meta = read_vector_store_meta(meta_path)
            self.assertEqual(meta.get("vector_store_id"), "vs_existing")
            self.assertEqual(meta.get("stats"), {"uploaded": 1, "reused": 1, "removed": 0, "total": 2})
            file_map = {item["name"]: item for item in meta["files"]}
            self.assertEqual(file_map["faq_general.md"]["status"], "reused")
            self.assertEqual(file_map["faq_general.md"]["file_id"], "file_existing_faq")
            self.assertEqual(file_map["payments.md"]["status"], "uploaded")
            self.assertEqual(file_map["payments.md"]["file_id"], "file_new_payments")

    def test_main_creates_vector_store_when_id_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            faq = knowledge_dir / "faq_general.md"
            faq.write_text("FAQ", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=False,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(
                sync_vector_store, "_create_vector_store", return_value="vs_new"
            ) as create_mock, patch.object(
                sync_vector_store, "_upload_file", return_value="file_new_faq"
            ) as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            create_mock.assert_called_once_with(api_key="test-key", name="sales-agent-knowledge")
            upload_mock.assert_called_once_with(api_key="test-key", file_path=faq)
            attach_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_new",
                file_id="file_new_faq",
            )

            meta = read_vector_store_meta(meta_path)
            self.assertEqual(meta.get("vector_store_id"), "vs_new")
            self.assertEqual(meta.get("stats"), {"uploaded": 1, "reused": 0, "removed": 0, "total": 1})

    def test_main_dry_run_does_not_write_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            (knowledge_dir / "faq_general.md").write_text("FAQ", encoding="utf-8")
            meta_path = root / "data" / "vector_store.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text("{}", encoding="utf-8")

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id="vs_existing",
                name="sales-agent-knowledge",
                dry_run=True,
                prune_missing=True,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(sync_vector_store, "_upload_file") as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock, patch.object(
                sync_vector_store, "write_vector_store_meta"
            ) as write_meta_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            upload_mock.assert_not_called()
            attach_mock.assert_not_called()
            write_meta_mock.assert_not_called()

    def test_main_prunes_stale_files_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            faq = knowledge_dir / "faq_general.md"
            faq.write_text("FAQ", encoding="utf-8")
            faq_sha = sync_vector_store._file_sha256(faq)

            meta_path = root / "data" / "vector_store.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(
                json.dumps(
                    {
                        "vector_store_id": "vs_existing",
                        "files": [
                            {"name": "faq_general.md", "file_id": "file_faq", "sha256": faq_sha},
                            {"name": "legacy.md", "file_id": "file_legacy", "sha256": "legacy-sha"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=True,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(sync_vector_store, "_upload_file") as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock, patch.object(
                sync_vector_store, "_list_vector_store_files", return_value=[]
            ) as list_mock, patch.object(
                sync_vector_store, "_delete_file_from_vector_store"
            ) as delete_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            upload_mock.assert_not_called()
            attach_mock.assert_not_called()
            list_mock.assert_called_once_with(api_key="test-key", vector_store_id="vs_existing")
            delete_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_existing",
                file_id="file_legacy",
            )

            meta = read_vector_store_meta(meta_path)
            self.assertEqual(meta.get("stats"), {"uploaded": 0, "reused": 1, "removed": 1, "total": 1})
            names = {item["name"] for item in meta["files"]}
            self.assertEqual(names, {"faq_general.md"})

    def test_main_prunes_replaced_file_ids_when_content_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            faq = knowledge_dir / "faq_general.md"
            faq.write_text("FAQ NEW", encoding="utf-8")

            meta_path = root / "data" / "vector_store.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(
                json.dumps(
                    {
                        "vector_store_id": "vs_existing",
                        "files": [
                            {"name": "faq_general.md", "file_id": "file_old_faq", "sha256": "old-sha"}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=True,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(
                sync_vector_store, "_upload_file", return_value="file_new_faq"
            ) as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock, patch.object(
                sync_vector_store, "_list_vector_store_files", return_value=[{"file_id": "file_old_faq", "name": "faq_general.md"}]
            ), patch.object(
                sync_vector_store, "_delete_file_from_vector_store"
            ) as delete_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            upload_mock.assert_called_once_with(api_key="test-key", file_path=faq)
            attach_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_existing",
                file_id="file_new_faq",
            )
            delete_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_existing",
                file_id="file_old_faq",
            )

            meta = read_vector_store_meta(meta_path)
            self.assertEqual(meta.get("stats"), {"uploaded": 1, "reused": 0, "removed": 1, "total": 1})
            files = meta.get("files", [])
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0]["file_id"], "file_new_faq")

    def test_main_prunes_remote_files_not_in_local_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            knowledge_dir = root / "knowledge"
            knowledge_dir.mkdir(parents=True, exist_ok=True)
            faq = knowledge_dir / "faq_general.md"
            faq.write_text("FAQ", encoding="utf-8")
            faq_sha = sync_vector_store._file_sha256(faq)

            meta_path = root / "data" / "vector_store.json"
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(
                json.dumps(
                    {
                        "vector_store_id": "vs_existing",
                        "files": [
                            {"name": "faq_general.md", "file_id": "file_faq", "sha256": faq_sha}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            args = argparse.Namespace(
                knowledge_dir=knowledge_dir,
                meta_path=meta_path,
                vector_store_id=None,
                name="sales-agent-knowledge",
                dry_run=False,
                prune_missing=True,
            )
            settings = self._settings(
                root=root,
                knowledge_path=knowledge_dir,
                meta_path=meta_path,
                openai_api_key="test-key",
            )

            with patch.object(sync_vector_store, "parse_args", return_value=args), patch.object(
                sync_vector_store, "get_settings", return_value=settings
            ), patch.object(sync_vector_store, "_upload_file") as upload_mock, patch.object(
                sync_vector_store, "_attach_file_to_vector_store"
            ) as attach_mock, patch.object(
                sync_vector_store, "_list_vector_store_files", return_value=[{"file_id": "file_faq", "name": "faq_general.md"}, {"file_id": "file_unknown", "name": "other.md"}]
            ) as list_mock, patch.object(
                sync_vector_store, "_delete_file_from_vector_store"
            ) as delete_mock:
                result = sync_vector_store.main()

            self.assertEqual(result, 0)
            upload_mock.assert_not_called()
            attach_mock.assert_not_called()
            list_mock.assert_called_once_with(api_key="test-key", vector_store_id="vs_existing")
            delete_mock.assert_called_once_with(
                api_key="test-key",
                vector_store_id="vs_existing",
                file_id="file_unknown",
            )

            meta = read_vector_store_meta(meta_path)
            self.assertEqual(meta.get("stats"), {"uploaded": 0, "reused": 1, "removed": 1, "total": 1})


if __name__ == "__main__":
    unittest.main()
