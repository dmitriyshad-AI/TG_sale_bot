import argparse
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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
    def test_parse_args_accepts_custom_flags(self) -> None:
        with patch(
            "sys.argv",
            [
                "sync_kmipt_catalog.py",
                "--output",
                "/tmp/out.yaml",
                "--timeout",
                "11",
                "--retries",
                "3",
                "--check-catalog",
                "/tmp/catalog.yaml",
            ],
        ):
            args = sync_kmipt_catalog.parse_args()
        self.assertEqual(args.output, Path("/tmp/out.yaml"))
        self.assertEqual(args.timeout, 11.0)
        self.assertEqual(args.retries, 3)
        self.assertEqual(args.check_catalog, Path("/tmp/catalog.yaml"))

    def test_fetch_url_falls_back_to_curl(self) -> None:
        with patch.object(sync_kmipt_catalog, "urlopen", side_effect=RuntimeError("network down")), patch.object(
            sync_kmipt_catalog.time, "sleep", return_value=None
        ), patch.object(
            sync_kmipt_catalog.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=0, stdout="ok-body"),
        ):
            body = sync_kmipt_catalog.fetch_url("https://kmipt.ru/test", timeout=1.0, retries=1)
        self.assertEqual(body, "ok-body")

    def test_fetch_url_raises_when_network_and_curl_fail(self) -> None:
        with patch.object(sync_kmipt_catalog, "urlopen", side_effect=RuntimeError("network down")), patch.object(
            sync_kmipt_catalog.time, "sleep", return_value=None
        ), patch.object(
            sync_kmipt_catalog.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=7, stdout=""),
        ):
            with self.assertRaises(RuntimeError):
                sync_kmipt_catalog.fetch_url("https://kmipt.ru/test", timeout=1.0, retries=1)

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

    def test_parse_sitemap_urls_rejects_invalid_xml(self) -> None:
        with self.assertRaises(RuntimeError):
            sync_kmipt_catalog.parse_sitemap_urls("<not-xml", base_url="https://kmipt.ru")

    def test_parse_sitemap_urls_requires_urlset(self) -> None:
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></sitemapindex>'
        )
        with self.assertRaises(RuntimeError):
            sync_kmipt_catalog.parse_sitemap_urls(xml, base_url="https://kmipt.ru")

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

    def test_collect_course_urls_raises_for_invalid_nested_xml(self) -> None:
        with patch.object(
            sync_kmipt_catalog,
            "fetch_url",
            return_value="<xml><broken>",
        ):
            with self.assertRaises(RuntimeError):
                sync_kmipt_catalog.collect_course_urls_from_sitemaps(
                    sitemap_url="https://kmipt.ru/sitemap.xml",
                    base_url="https://kmipt.ru",
                    timeout=1.0,
                    retries=1,
                )

    def test_collect_course_urls_raises_when_empty(self) -> None:
        with patch.object(
            sync_kmipt_catalog,
            "fetch_url",
            return_value=_sitemap_xml(["https://kmipt.ru/courses/EGE/"]),
        ):
            with self.assertRaises(RuntimeError):
                sync_kmipt_catalog.collect_course_urls_from_sitemaps(
                    sitemap_url="https://kmipt.ru/sitemap.xml",
                    base_url="https://kmipt.ru",
                    timeout=1.0,
                    retries=1,
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

    def test_build_product_raises_when_h1_missing(self) -> None:
        with self.assertRaises(RuntimeError):
            sync_kmipt_catalog.build_product(
                url="https://kmipt.ru/courses/EGE/Matematika_EGE/",
                html="<html><head></head><body><p>without title</p></body></html>",
            )

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

    def test_infer_category_path_rules_and_text_fallback(self) -> None:
        self.assertEqual(
            sync_kmipt_catalog.infer_category(
                "https://kmipt.ru/courses/kanikuly/letnyaya/",
                title="Школа",
                description="",
            ),
            "camp",
        )
        self.assertEqual(
            sync_kmipt_catalog.infer_category(
                "https://kmipt.ru/courses/olimp/math/",
                title="Олимп курс",
                description="",
            ),
            "olympiad",
        )
        self.assertEqual(
            sync_kmipt_catalog.infer_category(
                "https://kmipt.ru/courses/custom/x/",
                title="Интенсив по физике",
                description="Короткий интенсив",
            ),
            "intensive",
        )

    def test_infer_grades_handles_special_paths_and_fallbacks(self) -> None:
        self.assertEqual(
            sync_kmipt_catalog.infer_grades(
                "https://kmipt.ru/courses/online_5_8/courses_7/",
                title="Курс 7 класс",
                description="",
                category="base",
            ),
            (7, 7),
        )
        self.assertEqual(
            sync_kmipt_catalog.infer_grades(
                "https://kmipt.ru/courses/Kanikuly/Letnyaya_vyezdnaya_fizikomatematicheskaya_shkola_8__11_kl/",
                title="Летняя школа",
                description="",
                category="camp",
            ),
            (8, 11),
        )
        self.assertEqual(
            sync_kmipt_catalog.infer_grades(
                "https://kmipt.ru/courses/other/track/",
                title="Общий курс",
                description="",
                category="oge",
            ),
            (8, 9),
        )

    def test_infer_subjects_and_format_cover_fallback_branches(self) -> None:
        subjects = sync_kmipt_catalog.infer_subjects(
            "https://kmipt.ru/courses/custom/",
            title="Программа развития",
            description="Без явных предметов",
        )
        self.assertEqual(subjects, ["general"])

        self.assertEqual(
            sync_kmipt_catalog.infer_format(
                url="https://kmipt.ru/courses/online/track/",
                title="Онлайн курс",
                description="Онлайн и очно, гибкий формат",
                category="base",
            ),
            "hybrid",
        )
        self.assertEqual(
            sync_kmipt_catalog.infer_format(
                url="https://kmipt.ru/courses/ege/track/",
                title="Курс ЕГЭ",
                description="",
                category="ege",
            ),
            "offline",
        )

    def test_make_product_id_and_dedupe_ids(self) -> None:
        product_id = sync_kmipt_catalog.make_product_id("https://kmipt.ru/courses/EGE/Matematika_EGE/")
        self.assertTrue(product_id.startswith("kmipt-courses-ege-matematika_ege"))

        deduped = sync_kmipt_catalog._dedupe_ids(
            [
                {"id": "kmipt-test", "url": "a"},
                {"id": "kmipt-test", "url": "b"},
            ]
        )
        self.assertEqual(deduped[0]["id"], "kmipt-test")
        self.assertTrue(str(deduped[1]["id"]).startswith("kmipt-test-"))

    def test_catalog_from_products_raises_on_validation_error(self) -> None:
        with self.assertRaises(RuntimeError):
            sync_kmipt_catalog._catalog_from_products([{"id": "broken"}])

    def test_check_catalog_against_site_reports_missing_and_extra(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            catalog_path = Path(tmpdir) / "products.yaml"
            catalog_path.write_text(
                yaml.safe_dump(
                    {
                        "products": [
                            {
                                "id": "kmipt-local-1",
                                "brand": "kmipt",
                                "title": "Local Only",
                                "url": "https://kmipt.ru/courses/local-only/",
                                "category": "base",
                                "grade_min": 5,
                                "grade_max": 11,
                                "subjects": ["general"],
                                "format": "hybrid",
                                "usp": ["u1", "u2", "u3"],
                            }
                        ]
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            result = sync_kmipt_catalog.check_catalog_against_site(
                catalog_path=catalog_path,
                products_from_site=[
                    {
                        "id": "kmipt-site-1",
                        "brand": "kmipt",
                        "title": "Site Only",
                        "url": "https://kmipt.ru/courses/site-only/",
                        "category": "base",
                        "grade_min": 5,
                        "grade_max": 11,
                        "subjects": ["general"],
                        "format": "hybrid",
                        "usp": ["u1", "u2", "u3"],
                    }
                ],
            )
        self.assertEqual(result, 1)

    def test_main_returns_error_when_scrape_fails(self) -> None:
        args = argparse.Namespace(
            output=Path("/tmp/out.yaml"),
            sitemap_url="https://kmipt.ru/sitemap.xml",
            base_url="https://kmipt.ru",
            timeout=1.0,
            retries=1,
            check_catalog=None,
        )
        with patch.object(sync_kmipt_catalog, "parse_args", return_value=args), patch.object(
            sync_kmipt_catalog, "scrape_products", side_effect=RuntimeError("boom")
        ):
            result = sync_kmipt_catalog.main()
        self.assertEqual(result, 1)

    def test_check_catalog_against_site_returns_error_for_missing_file(self) -> None:
        result = sync_kmipt_catalog.check_catalog_against_site(
            catalog_path=Path("/tmp/definitely-missing-catalog.yaml"),
            products_from_site=[],
        )
        self.assertEqual(result, 1)

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
