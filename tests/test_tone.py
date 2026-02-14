import tempfile
import unittest
from pathlib import Path

import yaml

from sales_agent.sales_core.tone import (
    DEFAULT_TONE_PROFILE,
    apply_tone_guardrails,
    assess_response_quality,
    load_tone_profile,
    tone_as_prompt_block,
)


class ToneTests(unittest.TestCase):
    def test_load_tone_profile_returns_default_for_missing_file(self) -> None:
        missing = Path("/tmp/definitely-missing-sales-tone.yaml")
        profile = load_tone_profile(missing)
        self.assertEqual(profile.persona, DEFAULT_TONE_PROFILE.persona)

    def test_load_tone_profile_reads_custom_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "tone.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "persona": "Test persona",
                        "principles": ["A", "B"],
                        "polite_markers": ["пожалуйста"],
                        "pressure_markers": ["срочно"],
                        "substitutions": {"Оставьте телефон": "Если удобно, оставьте телефон"},
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            profile = load_tone_profile(path)

        self.assertEqual(profile.persona, "Test persona")
        self.assertEqual(profile.principles, ["A", "B"])
        self.assertIn("Оставьте телефон", profile.substitutions)

    def test_tone_as_prompt_block_contains_principles(self) -> None:
        block = tone_as_prompt_block(DEFAULT_TONE_PROFILE)
        self.assertIn("Профиль тона", block)
        self.assertIn("Правила", block)

    def test_apply_tone_guardrails_replaces_pushy_phrases(self) -> None:
        text = "Оставьте телефон!!! Срочно."
        sanitized = apply_tone_guardrails(text, DEFAULT_TONE_PROFILE)
        self.assertIn("Если вам удобно, оставьте телефон", sanitized)
        self.assertNotIn("Срочно", sanitized)
        self.assertNotIn("!!!", sanitized)

    def test_assess_response_quality_detects_pressure(self) -> None:
        low_pressure = assess_response_quality("Спасибо! Если удобно, подскажите формат.")
        high_pressure = assess_response_quality("Срочно! Это последний шанс, оставьте телефон прямо сейчас.")
        self.assertLessEqual(low_pressure["pressure_score"], high_pressure["pressure_score"])
        self.assertGreaterEqual(low_pressure["friendliness_score"], high_pressure["friendliness_score"])


if __name__ == "__main__":
    unittest.main()
