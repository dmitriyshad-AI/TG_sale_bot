import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import create_app
    from sales_agent.sales_core.config import Settings

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


def _settings(db_path: Path, webapp_dist: Path) -> Settings:
    return Settings(
        telegram_bot_token="",
        openai_api_key="",
        openai_model="gpt-4.1",
        tallanto_api_url="",
        tallanto_api_key="",
        brand_default="kmipt",
        database_path=db_path,
        catalog_path=Path("catalog/products.yaml"),
        knowledge_path=Path("knowledge"),
        vector_store_meta_path=Path("data/vector_store.json"),
        openai_vector_store_id="",
        admin_user="admin",
        admin_pass="secret",
        webapp_dist_path=webapp_dist,
    )


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiUserWebappTests(unittest.TestCase):
    def test_user_webapp_placeholder_when_dist_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            app = create_app(_settings(root / "app.db", root / "missing_dist"))
            client = TestClient(app)

            root_response = client.get("/")
            self.assertEqual(root_response.status_code, 200)
            self.assertEqual(root_response.json()["user_miniapp"]["status"], "build-required")

            placeholder = client.get("/app")
            self.assertEqual(placeholder.status_code, 200)
            self.assertIn("User Mini App is not built yet", placeholder.text)

    def test_user_webapp_served_when_dist_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dist = root / "dist"
            dist.mkdir(parents=True, exist_ok=True)
            (dist / "index.html").write_text("<!doctype html><html><body>miniapp-ready</body></html>", encoding="utf-8")

            app = create_app(_settings(root / "app.db", dist))
            client = TestClient(app)

            root_response = client.get("/")
            self.assertEqual(root_response.status_code, 200)
            self.assertEqual(root_response.json()["user_miniapp"]["status"], "ready")

            webapp_response = client.get("/app", follow_redirects=True)
            self.assertEqual(webapp_response.status_code, 200)
            self.assertIn("miniapp-ready", webapp_response.text)


if __name__ == "__main__":
    unittest.main()
