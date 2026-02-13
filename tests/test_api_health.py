import unittest

try:
    from fastapi.testclient import TestClient

    from sales_agent.sales_api.main import app

    HAS_FASTAPI = True
except ModuleNotFoundError:
    HAS_FASTAPI = False


@unittest.skipUnless(HAS_FASTAPI, "fastapi dependencies are not installed")
class ApiHealthTests(unittest.TestCase):
    def test_health_endpoint_returns_ok_payload(self) -> None:
        client = TestClient(app)
        response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "service": "sales-agent"})


if __name__ == "__main__":
    unittest.main()
