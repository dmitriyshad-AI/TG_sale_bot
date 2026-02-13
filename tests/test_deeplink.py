import unittest

from sales_agent.sales_core.deeplink import (
    DeepLinkMeta,
    build_greeting_hint,
    encode_start_payload,
    parse_start_payload,
)


class DeepLinkTests(unittest.TestCase):
    def test_meta_to_dict_omits_empty_values(self) -> None:
        meta = DeepLinkMeta(brand="kmipt", source="site", page="/courses/ege", utm_source="")
        payload = meta.to_dict()
        self.assertEqual(payload["brand"], "kmipt")
        self.assertEqual(payload["source"], "site")
        self.assertEqual(payload["page"], "/courses/ege")
        self.assertNotIn("utm_source", payload)

    def test_encode_and_parse_payload_roundtrip(self) -> None:
        token = encode_start_payload(
            DeepLinkMeta(
                brand="kmipt",
                source="site",
                page="/courses/ege",
                utm_source="google",
                utm_medium="cpc",
            )
        )
        parsed = parse_start_payload(token)
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")
        self.assertEqual(parsed.get("page"), "/courses/ege")

    def test_parse_plain_query_payload(self) -> None:
        parsed = parse_start_payload("brand=foton&source=site&page=%2Fcamp&utm_source=vk")
        self.assertEqual(parsed.get("brand"), "foton")
        self.assertEqual(parsed.get("source"), "site")
        self.assertEqual(parsed.get("page"), "/camp")
        self.assertEqual(parsed.get("utm_source"), "vk")

    def test_build_greeting_hint_uses_page_and_source(self) -> None:
        hint = build_greeting_hint({"source": "site", "page": "/courses/camp/summer"})
        self.assertIsNotNone(hint)
        self.assertIn("лагер", hint.lower())

    def test_parse_returns_empty_for_invalid_token(self) -> None:
        self.assertEqual(parse_start_payload("dl_not-valid-base64"), {})

    def test_long_payload_remains_valid_and_decodable(self) -> None:
        token = encode_start_payload(
            DeepLinkMeta(
                brand="kmipt",
                source="site",
                page="/courses/very/long/path/that/should/be/truncated/safely",
                utm_source="verylongutmsourcevalue",
                utm_medium="verylongutmmediumvalue",
                utm_campaign="verylongutmcampaignvalue",
            )
        )
        self.assertLessEqual(len(token), 64)
        parsed = parse_start_payload(token)
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")

    def test_long_camp_page_keeps_page_hint_when_full_page_drops(self) -> None:
        token = encode_start_payload(
            DeepLinkMeta(
                brand="kmipt",
                source="site",
                page="/courses/camp/very/long/path/that/definitely/will/be/compressed",
                utm_source="a" * 24,
                utm_medium="b" * 24,
                utm_campaign="c" * 24,
            ),
            max_len=64,
        )
        parsed = parse_start_payload(token)
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")
        self.assertIn(parsed.get("page"), {"/camp", "/courses/camp"})

    def test_parse_plain_query_payload_uses_page_hint(self) -> None:
        parsed = parse_start_payload("brand=kmipt&source=site&page_hint=oge")
        self.assertEqual(parsed.get("brand"), "kmipt")
        self.assertEqual(parsed.get("source"), "site")
        self.assertEqual(parsed.get("page"), "/oge")

    def test_encode_payload_falls_back_to_minimal_when_max_len_too_small(self) -> None:
        token = encode_start_payload(
            DeepLinkMeta(
                brand="kmipt",
                source="site",
                page="/very/long/path",
                utm_source="verylongsource",
                utm_medium="verylongmedium",
                utm_campaign="verylongcampaign",
            ),
            max_len=8,
        )
        self.assertTrue(token.startswith("dl_"))
        parsed = parse_start_payload(token)
        self.assertIsInstance(parsed, dict)

    def test_parse_start_payload_handles_empty_values(self) -> None:
        self.assertEqual(parse_start_payload(""), {})
        self.assertEqual(parse_start_payload("   "), {})

    def test_build_greeting_hint_for_oge_and_none_case(self) -> None:
        oge_hint = build_greeting_hint({"page": "/courses/oge"})
        self.assertIsNotNone(oge_hint)
        self.assertIn("огэ", oge_hint.lower())

        no_hint = build_greeting_hint({"page": "/unknown", "source": "ads"})
        self.assertIsNone(no_hint)


if __name__ == "__main__":
    unittest.main()
