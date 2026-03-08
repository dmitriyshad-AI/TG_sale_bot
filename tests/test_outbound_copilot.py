import unittest

from sales_agent.sales_core.outbound_copilot import (
    build_outbound_proposal,
    evaluate_outbound_proposal_guard,
    parse_outbound_companies_csv,
    score_company_fit,
)


class OutboundCopilotTests(unittest.TestCase):
    def test_score_company_fit_detects_school_profile(self) -> None:
        company = {
            "company_name": "Лицей №1",
            "segment": "Школа с инженерным профилем",
            "city": "Москва",
            "website": "https://lyceum.example.edu",
            "note": "Интерес к олимпиадной подготовке",
        }
        fit = score_company_fit(company, campaign_tags=["school", "olympiad"])
        self.assertGreaterEqual(float(fit["score"]), 50.0)
        self.assertIn("school", fit["tags"])
        self.assertIn("moscow", fit["tags"])

    def test_build_outbound_proposal_contains_expected_sections(self) -> None:
        company = {
            "company_name": "Школа 57",
            "city": "Москва",
            "segment": "school",
        }
        fit = {"score": 76.5, "tags": ["school", "moscow"], "reason": "fit ok"}
        draft = build_outbound_proposal(company, fit=fit, offer_focus="ЕГЭ и олимпиадный трек")
        self.assertIn("Здравствуйте", draft["short_message"])
        self.assertIn("ЕГЭ и олимпиадный трек", draft["proposal_text"])
        self.assertIn("Школа 57", draft["proposal_text"])

    def test_parse_outbound_companies_csv_uses_name_fallback(self) -> None:
        csv_text = "name,website,city,segment\nШкола 179,https://179.example,Москва,school\n"
        items = parse_outbound_companies_csv(csv_text, source="csv_test")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["company_name"], "Школа 179")
        self.assertEqual(items[0]["source"], "csv_test")

    def test_score_company_fit_handles_sparse_and_non_string_tags(self) -> None:
        sparse = score_company_fit({"company_name": 123, "segment": None, "city": None, "note": None}, campaign_tags=[1, 2])
        self.assertGreaterEqual(float(sparse["score"]), 0.0)
        self.assertIn("insufficient explicit education markers", sparse["reason"])

        corporate = score_company_fit(
            {
                "company_name": "Business Corp",
                "segment": "corporate training",
                "city": "Moscow",
                "note": "",
                "website": "https://corp.example",
            }
        )
        self.assertIn("corporate", corporate["tags"])
        self.assertGreaterEqual(float(corporate["score"]), 30.0)

    def test_score_company_fit_generates_matched_tags_reason(self) -> None:
        fit = score_company_fit(
            {
                "company_name": "Гимназия 1",
                "segment": "school profile",
                "city": "Москва",
                "website": "",
            },
            campaign_tags=[],
        )
        self.assertIn("matched tags:", fit["reason"])

    def test_build_outbound_proposal_score_tiers_and_compaction(self) -> None:
        long_name = "Очень длинное название компании " * 20
        medium = build_outbound_proposal(
            {"company_name": long_name, "city": "Москва", "segment": "school"},
            fit={"score": 55, "tags": [], "reason": ""},
        )
        low = build_outbound_proposal(
            {"company_name": "Компания", "city": "", "segment": ""},
            fit={"score": 20, "tags": [], "reason": ""},
        )
        self.assertIn("средний fit", medium["proposal_text"])
        self.assertIn("дополнительная квалификация", low["proposal_text"])
        self.assertIn("...", medium["proposal_text"])

    def test_parse_outbound_companies_csv_skips_empty_rows(self) -> None:
        csv_text = "company_name,website,city,segment\n,https://empty.example,Москва,school\n"
        items = parse_outbound_companies_csv(csv_text, source="csv_test")
        self.assertEqual(items, [])

    def test_proposal_guard_covers_denied_and_allowed_paths(self) -> None:
        denied_inactive = evaluate_outbound_proposal_guard(
            company_status="archived",
            open_proposals=0,
            recent_touches=0,
        )
        self.assertFalse(denied_inactive.allowed)
        self.assertEqual(denied_inactive.reason_code, "company_inactive")

        denied_open = evaluate_outbound_proposal_guard(
            company_status="qualified",
            open_proposals=1,
            recent_touches=0,
            max_open_proposals=1,
        )
        self.assertFalse(denied_open.allowed)
        self.assertEqual(denied_open.reason_code, "open_proposal_exists")

        denied_touches = evaluate_outbound_proposal_guard(
            company_status="qualified",
            open_proposals=0,
            recent_touches=2,
            max_recent_touches=2,
        )
        self.assertFalse(denied_touches.allowed)
        self.assertEqual(denied_touches.reason_code, "touch_limit_reached")

        denied_won = evaluate_outbound_proposal_guard(
            company_status="won",
            open_proposals=0,
            recent_touches=0,
        )
        self.assertFalse(denied_won.allowed)
        self.assertEqual(denied_won.reason_code, "company_already_won")

        allowed = evaluate_outbound_proposal_guard(
            company_status="qualified",
            open_proposals=0,
            recent_touches=0,
        )
        self.assertTrue(allowed.allowed)
        self.assertEqual(allowed.reason_code, "ok")


if __name__ == "__main__":
    unittest.main()
