from __future__ import annotations

import html

from fastapi.responses import HTMLResponse


def inbox_workflow_status_label(status_value: str) -> str:
    normalized = (status_value or "").strip().lower()
    labels = {
        "new": "Новый",
        "needs_approval": "Нужен approve",
        "ready_to_send": "Готов к отправке",
        "sending": "Отправляется",
        "failed": "Ошибка отправки",
        "sent": "Отправлен",
        "rejected": "Отклонён",
        "manual_required": "Нужен ручной шаг",
    }
    return labels.get(normalized, status_value or "new")


def inbox_workflow_badge(status_value: str) -> str:
    normalized = (status_value or "").strip().lower()
    colors = {
        "new": "#e5e7eb",
        "needs_approval": "#fef3c7",
        "ready_to_send": "#dbeafe",
        "sending": "#bfdbfe",
        "failed": "#fecaca",
        "sent": "#dcfce7",
        "rejected": "#f3f4f6",
        "manual_required": "#ede9fe",
    }
    bg = colors.get(normalized, "#e5e7eb")
    label = html.escape(inbox_workflow_status_label(normalized))
    return f"<span class='badge' style='background:{bg};'>{label}</span>"


def render_admin_page(title: str, body_html: str) -> HTMLResponse:
    page = f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --fg: #0f172a;
      --muted: #475569;
      --line: #cbd5e1;
      --card: #f8fafc;
      --bg: #eef2ff;
      --nav: #0b1d35;
      --nav-link: #e2e8f0;
      --nav-link-active: #bfdbfe;
      --btn: #1d4ed8;
      --btn-text: #ffffff;
    }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: var(--fg); background: var(--bg); }}
    .shell {{ max-width: 1420px; margin: 0 auto; padding: 16px 20px 28px; }}
    h1, h2 {{ margin: 0 0 12px; line-height: 1.25; }}
    .muted {{ color: var(--muted); }}
    nav {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: var(--nav);
      border-bottom: 1px solid #0f2a49;
      padding: 10px 14px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin: -16px -20px 16px;
    }}
    nav a {{
      color: var(--nav-link);
      text-decoration: none;
      padding: 7px 10px;
      border-radius: 10px;
      font-weight: 600;
      font-size: 13px;
      line-height: 1;
      white-space: nowrap;
    }}
    nav a:hover {{ background: #16355a; color: #ffffff; }}
    nav a:focus {{ outline: 2px solid #93c5fd; outline-offset: 1px; }}
    .current {{ color: var(--nav-link-active); background: #16355a; }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 12px;
    }}
    .toolbar a {{
      text-decoration: none;
      border: 1px solid #bfdbfe;
      color: #1e3a8a;
      background: #eff6ff;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 600;
    }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; background: #fff; }}
    th, td {{ border: 1px solid var(--line); padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f1f5f9; }}
    .card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px; margin-bottom: 12px; background: var(--card); }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #dbeafe; color: #1e3a8a; font-weight: 600; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #e2e8f0; padding: 10px; border-radius: 6px; }}
    input, textarea, select, button {{ font-size: 14px; }}
    input, textarea, select {{ border: 1px solid #94a3b8; border-radius: 8px; padding: 7px 9px; }}
    button {{ padding: 8px 12px; cursor: pointer; background: var(--btn); color: var(--btn-text); border: 1px solid #1e40af; border-radius: 8px; font-weight: 600; }}
    button:hover {{ background: #1e40af; }}
    @media (max-width: 900px) {{
      .shell {{ padding: 12px; }}
      nav {{ margin: -12px -12px 12px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <nav>
      <a href="/admin">Dashboard</a>
      <a href="/admin/ui/inbox">Inbox</a>
      <a href="/admin/ui/business-inbox">Business Inbox</a>
      <a href="/admin/ui/followups">Followups</a>
      <a href="/admin/ui/director">Director</a>
      <a href="/admin/ui/outbound">Outbound</a>
      <a href="/admin/ui/calls">Calls</a>
      <a href="/admin/ui/faq-lab">FAQ Lab</a>
      <a href="/admin/ui/revenue-metrics">Revenue Metrics</a>
      <a href="/admin/ui/leads">Leads</a>
      <a href="/admin/ui/conversations">Conversations</a>
      <a href="/admin/ui/copilot">Copilot</a>
    </nav>
    <div class="toolbar">
      <a href="/admin/ui/inbox?status=new">Новые треды</a>
      <a href="/admin/ui/inbox?status=failed">Ошибки отправки</a>
      <a href="/admin/ui/followups?priority=hot&status=pending">Hot followups</a>
      <a href="/admin/ui/calls">Последние звонки</a>
      <a href="/admin/ui/director">Активные кампании</a>
      <a href="/admin/ui/outbound">B2B Outbound</a>
    </div>
    {body_html}
  </div>
</body>
</html>
"""
    return HTMLResponse(page)
