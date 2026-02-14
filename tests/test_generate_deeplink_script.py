import subprocess
import sys
import unittest
from urllib.parse import parse_qs, urlparse

from sales_agent.sales_core.deeplink import parse_start_payload


class GenerateDeepLinkScriptTests(unittest.TestCase):
    def _extract_payload(self, link: str) -> str:
        query = parse_qs(urlparse(link).query)
        payload_list = query.get("start", [])
        self.assertEqual(len(payload_list), 1)
        return payload_list[0]

    def test_generates_default_link(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/generate_deeplink.py", "--bot-username", "SalesBot"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        link = result.stdout.strip()
        self.assertTrue(link.startswith("https://t.me/SalesBot?start="))
        payload = self._extract_payload(link)
        parsed = parse_start_payload(payload)
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")

    def test_generates_link_with_custom_params(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/generate_deeplink.py",
                "--bot-username",
                "SalesBot",
                "--brand",
                "kmipt",
                "--source",
                "site",
                "--page",
                "",
                "--utm-source",
                "g",
                "--utm-medium",
                "cpc",
                "--utm-campaign",
                "",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        payload = self._extract_payload(result.stdout.strip())
        parsed = parse_start_payload(payload)
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")
        self.assertNotIn("page", parsed)
        self.assertEqual(parsed.get("utm_source"), "g")
        self.assertEqual(parsed.get("utm_medium"), "cpc")
        self.assertNotIn("utm_campaign", parsed)


if __name__ == "__main__":
    unittest.main()
