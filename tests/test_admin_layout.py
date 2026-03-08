import unittest

from sales_agent.sales_api.services.admin_layout import (
    inbox_workflow_badge,
    inbox_workflow_status_label,
    render_admin_page,
)


class AdminLayoutServiceTests(unittest.TestCase):
    def test_inbox_workflow_status_label_known_and_unknown(self) -> None:
        self.assertEqual(inbox_workflow_status_label("new"), "Новый")
        self.assertEqual(inbox_workflow_status_label("FAILED"), "Ошибка отправки")
        self.assertEqual(inbox_workflow_status_label("custom_status"), "custom_status")
        self.assertEqual(inbox_workflow_status_label(""), "new")

    def test_inbox_workflow_badge_escapes_unknown_label(self) -> None:
        badge = inbox_workflow_badge("<script>alert(1)</script>")
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", badge)
        self.assertNotIn("<script>", badge)

    def test_render_admin_page_renders_html_shell(self) -> None:
        response = render_admin_page("Title <unsafe>", "<section id='x'>content</section>")
        body = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.media_type)
        self.assertIn("<title>Title &lt;unsafe&gt;</title>", body)
        self.assertIn("<section id='x'>content</section>", body)
        self.assertIn('href="/admin/ui/inbox"', body)
        self.assertIn('href="/admin/ui/director"', body)
        self.assertIn('href="/admin/ui/outbound"', body)


if __name__ == "__main__":
    unittest.main()
