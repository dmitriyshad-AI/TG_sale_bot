import tempfile
import unittest
from pathlib import Path

from sales_agent.sales_core.vector_store import (
    load_vector_store_id,
    read_vector_store_meta,
    write_vector_store_meta,
)


class VectorStoreMetaTests(unittest.TestCase):
    def test_read_returns_empty_for_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "vector_store.json"
            self.assertEqual(read_vector_store_meta(path), {})
            self.assertIsNone(load_vector_store_id(path))

    def test_write_and_load_vector_store_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "meta" / "vector_store.json"
            write_vector_store_meta(path, {"vector_store_id": "vs_abc123", "files": []})

            loaded = read_vector_store_meta(path)
            self.assertEqual(loaded.get("vector_store_id"), "vs_abc123")
            self.assertEqual(load_vector_store_id(path), "vs_abc123")

    def test_load_returns_none_for_invalid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "vector_store.json"
            write_vector_store_meta(path, {"vector_store_id": ""})
            self.assertIsNone(load_vector_store_id(path))

    def test_read_returns_empty_for_invalid_json_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "vector_store.json"
            path.write_text("{invalid-json", encoding="utf-8")
            self.assertEqual(read_vector_store_meta(path), {})
            self.assertIsNone(load_vector_store_id(path))

    def test_read_returns_empty_for_non_mapping_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "vector_store.json"
            path.write_text("[]", encoding="utf-8")
            self.assertEqual(read_vector_store_meta(path), {})
            self.assertIsNone(load_vector_store_id(path))


if __name__ == "__main__":
    unittest.main()
