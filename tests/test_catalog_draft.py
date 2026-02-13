import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from sales_agent.sales_core.catalog_draft import (
    ProductCandidate,
    build_product_from_candidate,
    derive_category,
    derive_grade_range,
    extract_foton_candidates,
    extract_kmipt_candidates,
    extract_price_from_html,
    make_product_id,
    parse_ru_date_range,
    strip_html,
)


class _MockHTTPResponse:
    def __init__(self, body: str) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class CatalogDraftTests(unittest.TestCase):
    def test_extract_foton_candidates(self) -> None:
        html = """
        <div class="cart-course content_ajax_item">
          <div class="cart-course__content">
            <div class="cart-course__format _online">Онлайн</div>
            <a href="/courses/kursy-online-5-8/" class="cart-course__name">
              Онлайн-курсы для 5-8 классов
            </a>
            <div class="cart-course__desc _full">Занятия по математике и физике</div>
            <a href="/courses/kursy-online-5-8/" class="btn"
               onclick="dataLayer.push({'ecommerce':{'click':{'products':{'price':'36700'}}}});">Подробнее</a>
          </div>
        </div>
        """
        items = extract_foton_candidates(html)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].brand, "foton")
        self.assertIn("Онлайн-курсы", items[0].title)
        self.assertEqual(items[0].list_price, 36700)
        self.assertEqual(items[0].format_hint, "online")

    def test_extract_kmipt_candidates(self) -> None:
        html = """
        <h4><a class="name_title" href="/courses/EGE/Matematika_EGE/">Математика ЕГЭ</a></h4>
        <h4><a class="name_title" href="/courses/EGE/Fizika_EGE/">Физика ЕГЭ</a></h4>
        """
        items = extract_kmipt_candidates(html, source_url="https://kmipt.ru/courses/EGE/")
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].brand, "kmipt")
        self.assertIn("Matematika_EGE", items[0].url)

    def test_extract_price_from_html_variants(self) -> None:
        self.assertEqual(extract_price_from_html("<b class=\"price_value\">36 900</b>"), 36900)
        self.assertEqual(extract_price_from_html("'price':'11400'"), 11400)
        self.assertEqual(extract_price_from_html("Стоимость 82 000 ₽"), 82000)

    def test_strip_html_removes_scripts(self) -> None:
        html = "<div>Нормальный текст</div><script>window.BX = {x:1};</script>"
        self.assertEqual(strip_html(html), "Нормальный текст")

    def test_derive_category_and_grade_range(self) -> None:
        category = derive_category("Подготовка к ЕГЭ по математике")
        self.assertEqual(category, "ege")
        self.assertEqual(derive_grade_range("Курс для 5-8 классов", "base"), (5, 8))
        self.assertEqual(derive_grade_range("Подготовка к ОГЭ", "oge"), (8, 9))

    def test_parse_ru_date_range(self) -> None:
        parsed = parse_ru_date_range("Зимний лагерь с 3 по 11 января 2026 г.")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed[0].isoformat(), "2026-01-03")
        self.assertEqual(parsed[1].isoformat(), "2026-01-11")

    def test_make_product_id_uniqueness(self) -> None:
        used: set[str] = set()
        first = make_product_id("kmipt", "https://kmipt.ru/courses/EGE/Matematika_EGE/", used)
        second = make_product_id("kmipt", "https://kmipt.ru/courses/EGE/Matematika_EGE/", used)
        self.assertNotEqual(first, second)

    def test_build_product_from_candidate_creates_required_fields(self) -> None:
        candidate = ProductCandidate(
            brand="foton",
            title="Летняя Выездная школа 2026 для 5 — 10 кл.",
            url="https://cdpofoton.ru/courses/vyezdnye-shkoly/",
            description="Летняя смена",
            list_price=83300,
            format_hint="offline",
        )
        detail_html = """
        <h1>Летняя Выездная школа 2026 для 5 — 10 кл.</h1>
        <div>Смена проходит с 20 по 30 июня 2026</div>
        <div>Математика, физика, информатика</div>
        """
        product = build_product_from_candidate(candidate, detail_html=detail_html, used_ids=set())
        self.assertEqual(product["brand"], "foton")
        self.assertEqual(product["category"], "camp")
        self.assertGreaterEqual(len(product["usp"]), 3)
        self.assertEqual(product["sessions"][0]["start_date"], "2026-06-20")

    @patch("sales_agent.sales_core.catalog_draft.time.sleep")
    @patch("sales_agent.sales_core.catalog_draft.OPENER.open")
    def test_fetch_html_retries_after_503(self, mock_open, _mock_sleep) -> None:
        from sales_agent.sales_core.catalog_draft import fetch_html

        mock_open.side_effect = [
            HTTPError(url="https://kmipt.ru/x", code=503, msg="503", hdrs=None, fp=None),
            _MockHTTPResponse(""),
            _MockHTTPResponse("<html>ok</html>"),
        ]
        html = fetch_html("https://kmipt.ru/courses/EGE/Matematika_EGE/", timeout=1)
        self.assertIn("ok", html)
        self.assertEqual(mock_open.call_count, 3)


if __name__ == "__main__":
    unittest.main()
