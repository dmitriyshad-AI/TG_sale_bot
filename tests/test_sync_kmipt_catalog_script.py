import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from scripts import sync_kmipt_catalog


def _sitemap_xml(urls: list[str]) -> str:
    body = "".join(f"<url><loc>{url}</loc></url>" for url in urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}"
        "</urlset>"
    )


def _sitemap_index_xml(sitemap_urls: list[str]) -> str:
    body = "".join(f"<sitemap><loc>{url}</loc></sitemap>" for url in sitemap_urls)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}"
        "</sitemapindex>"
    )


def _course_html(*, title: str, description: str = "", price: str = "") -> str:
    description_meta = f'<meta name="description" content="{description}" />' if description else ""
    price_block = f'<div class="price_value">{price}</div>' if price else ""
    return f"<html><head>{description_meta}</head><body><h1>{title}</h1>{price_block}</body></html>"


class SyncKmiptCatalogScriptTests(unittest.TestCase):
    def test_parse_sitemap_urls_keeps_leaf_course_pages(self) -> None:
        xml = _sitemap_xml(
            [
                "https://kmipt.ru/courses/EGE/",
                "https://kmipt.ru/courses/EGE/Matematika_EGE/",
                "https://kmipt.ru/courses/online/online_11/",
                "https://example.com/courses/EGE/Fizika_EGE/",
            ]
        )
        urls = sync_kmipt_catalog.parse_sitemap_urls(xml, base_url="https://kmipt.ru")
        self.assertEqual(
            urls,
            [
                "https://kmipt.ru/courses/EGE/Matematika_EGE/",
                "https://kmipt.ru/courses/online/online_11/",
            ],
        )

    def test_collect_course_urls_from_sitemap_index(self) -> None:
        index_url = "https://kmipt.ru/sitemap.xml"
        part_a = "https://kmipt.ru/sitemap-part-a.xml"
        part_b = "https://kmipt.ru/sitemap-part-b.xml"
        sitemap_a = _sitemap_xml(
            [
                "https://kmipt.ru/courses/EGE/",
                "https://kmipt.ru/courses/EGE/Matematika_EGE/",
            ]
        )
        sitemap_b = _sitemap_xml(
            [
                "https://kmipt.ru/courses/online/online_11/",
                "https://kmipt.ru/courses/online/online_11/",
            ]
        )
        fetch_map = {
            index_url: _sitemap_index_xml([part_a, part_b]),
            part_a: sitemap_a,
            part_b: sitemap_b,
        }
        with patch.object(sync_kmipt_catalog, "fetch_url", side_effect=lambda url, timeout, retries: fetch_map[url]):
            urls = sync_kmipt_catalog.collect_course_urls_from_sitemaps(
                sitemap_url=index_url,
                base_url="https://kmipt.ru",
                timeout=1.0,
                retries=1,
            )
        self.assertEqual(
            urls,
            [
                "https://kmipt.ru/courses/EGE/Matematika_EGE/",
                "https://kmipt.ru/courses/online/online_11/",
            ],
        )

    def test_build_product_extracts_price_and_title(self) -> None:
        product = sync_kmipt_catalog.build_product(
            url="https://kmipt.ru/courses/EGE/Matematika_EGE/",
            html=_course_html(
                title="Математика ЕГЭ",
                description="Курс для 11 класса",
                price="36 900",
            ),
        )
        self.assertEqual(product["title"], "Математика ЕГЭ")
        self.assertEqual(product["category"], "ege")
        self.assertEqual(product["grade_min"], 11)
        self.assertEqual(product["grade_max"], 11)
        self.assertEqual(product["format"], "offline")
        self.assertIn("Цена на странице: 36900 руб.", product["usp"])

    def test_infer_category_prefers_base_for_school_paths_even_with_olympiad_text(self) -> None:
        category = sync_kmipt_catalog.infer_category(
            url="https://kmipt.ru/courses/School_5_8/Fizika_5_8/",
            title="Физика 7—8 класс",
            description="Преподаватели — эксперты ЕГЭ и олимпиад.",
        )
        self.assertEqual(category, "base")

    def test_infer_category_prefers_base_for_enrollment_pages(self) -> None:
        category = sync_kmipt_catalog.infer_category(
            url="https://kmipt.ru/courses/aktualnyi_nabor/Nabor_ochno/",
            title="Набор на очные курсы для 1—11 классов",
            description="Подготовка к ЕГЭ, ОГЭ и олимпиадам.",
        )
        self.assertEqual(category, "base")

    def test_main_sync_writes_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "products.yaml"
            sitemap_url = "https://kmipt.ru/sitemap-iblock-8.xml"
            course_a = "https://kmipt.ru/courses/EGE/Matematika_EGE/"
            course_b = "https://kmipt.ru/courses/online/online_11/"
            args = argparse.Namespace(
                output=output,
                sitemap_url=sitemap_url,
                base_url="https://kmipt.ru",
                timeout=1.0,
                retries=1,
                check_catalog=None,
            )
            pages = {
                sitemap_url: _sitemap_xml(
                    [
                        "https://kmipt.ru/courses/EGE/",
                        course_a,
                        course_b,
                    ]
                ),
                course_a: _course_html(
                    title="Математика ЕГЭ",
                    description="Курс для 11 класса",
                    price="36 900",
                ),
                course_b: _course_html(
                    title="Курсы ЕГЭ онлайн 11 класс",
                    description="Онлайн формат",
                ),
            }

            with patch.object(sync_kmipt_catalog, "parse_args", return_value=args), patch.object(
                sync_kmipt_catalog, "fetch_url", side_effect=lambda url, timeout, retries: pages[url]
            ):
                result = sync_kmipt_catalog.main()

            self.assertEqual(result, 0)
            self.assertTrue(output.exists())
            payload = yaml.safe_load(output.read_text(encoding="utf-8"))
            products = payload.get("products", [])
            self.assertEqual(len(products), 2)
            self.assertEqual({item["url"] for item in products}, {course_a, course_b})

    def test_main_check_catalog_reports_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_path = Path(tmpdir) / "products.yaml"
            catalog_path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            {
                                "id": "kmipt-test-1",
                                "brand": "kmipt",
                                "title": "Старый заголовок",
                                "url": "https://kmipt.ru/courses/EGE/Matematika_EGE/",
                                "category": "ege",
                                "grade_min": 11,
                                "grade_max": 11,
                                "subjects": ["math"],
                                "format": "offline",
                                "usp": ["u1", "u2", "u3"],
                            }
                        ]
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            sitemap_url = "https://kmipt.ru/sitemap-iblock-8.xml"
            course = "https://kmipt.ru/courses/EGE/Matematika_EGE/"
            args = argparse.Namespace(
                output=Path(tmpdir) / "unused.yaml",
                sitemap_url=sitemap_url,
                base_url="https://kmipt.ru",
                timeout=1.0,
                retries=1,
                check_catalog=catalog_path,
            )
            pages = {
                sitemap_url: _sitemap_xml([course]),
                course: _course_html(title="Математика ЕГЭ"),
            }

            with patch.object(sync_kmipt_catalog, "parse_args", return_value=args), patch.object(
                sync_kmipt_catalog, "fetch_url", side_effect=lambda url, timeout, retries: pages[url]
            ):
                result = sync_kmipt_catalog.main()

            self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
