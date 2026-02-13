import unittest

try:
    from sales_agent.sales_bot.bot import _sanitize_phone

    HAS_BOT_DEPS = True
except ModuleNotFoundError:
    HAS_BOT_DEPS = False


@unittest.skipUnless(HAS_BOT_DEPS, "bot dependencies are not installed")
class BotLeadTestHelpers(unittest.TestCase):
    def test_sanitize_phone_handles_plus7(self) -> None:
        self.assertEqual(_sanitize_phone("+7 (999) 123-45-67"), "+79991234567")

    def test_sanitize_phone_converts_8_prefix_to_plus7(self) -> None:
        self.assertEqual(_sanitize_phone("8 999 123 45 67"), "+79991234567")

    def test_sanitize_phone_assumes_plus7_for_ten_digits(self) -> None:
        self.assertEqual(_sanitize_phone("9991234567"), "+79991234567")

    def test_sanitize_phone_rejects_short_values(self) -> None:
        self.assertIsNone(_sanitize_phone("12345"))


if __name__ == "__main__":
    unittest.main()
