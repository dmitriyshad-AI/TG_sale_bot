(function () {
  const state = {
    initData: "",
    me: null,
  };

  function getTelegramInitData() {
    if (window.Telegram && window.Telegram.WebApp) {
      const webApp = window.Telegram.WebApp;
      webApp.ready();
      webApp.expand();
      return webApp.initData || "";
    }
    return "";
  }

  function setStatus(message, kind) {
    const status = document.getElementById("authStatus");
    status.textContent = message;
    status.classList.remove("error");
    if (kind === "error") {
      status.classList.add("error");
    }
  }

  function setUserStatus(text) {
    const status = document.getElementById("userStatus");
    status.textContent = text;
  }

  function renderEmpty(target, message) {
    target.innerHTML = `<div class="item item-empty">${message}</div>`;
  }

  function renderError(target, message) {
    target.innerHTML = `<div class="item item-empty item-error">${message}</div>`;
  }

  function itemMeta(parts) {
    return `<div class="item-meta">${parts.map((part) => `<span>${part}</span>`).join("")}</div>`;
  }

  async function apiGet(path) {
    const response = await fetch(path, {
      method: "GET",
      headers: {
        "X-Telegram-Init-Data": state.initData,
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = payload && payload.detail ? payload.detail : `HTTP ${response.status}`;
      throw new Error(detail);
    }
    return payload;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  async function loadMe() {
    const payload = await apiGet("/admin/miniapp/api/me");
    state.me = payload.user || {};
    const userLabel = state.me.username
      ? `@${state.me.username}`
      : [state.me.first_name, state.me.last_name].filter(Boolean).join(" ") || String(payload.user_id || "-");
    setUserStatus(`Пользователь: ${userLabel}`);
    setStatus("Авторизация подтверждена");
  }

  async function loadLeads() {
    const target = document.getElementById("leadsList");
    target.innerHTML = "";
    const payload = await apiGet("/admin/miniapp/api/leads?limit=50");
    const items = payload.items || [];
    if (!items.length) {
      renderEmpty(target, "Лидов пока нет.");
      return;
    }
    target.innerHTML = items
      .map((item) => {
        const contact = item.contact || {};
        const title = `Лид #${item.lead_id} · user ${item.user_id}`;
        const meta = itemMeta([
          `Статус: ${escapeHtml(item.status || "-")}`,
          `Телефон: ${escapeHtml(contact.phone || "-")}`,
          `Источник: ${escapeHtml(contact.source || "-")}`,
          `Создан: ${escapeHtml(item.created_at || "-")}`,
        ]);
        return `<article class="item"><h3 class="item-title">${title}</h3>${meta}</article>`;
      })
      .join("");
  }

  async function loadConversations() {
    const target = document.getElementById("conversationsList");
    target.innerHTML = "";
    const payload = await apiGet("/admin/miniapp/api/conversations?limit=60");
    const items = payload.items || [];
    if (!items.length) {
      renderEmpty(target, "Диалогов пока нет.");
      return;
    }
    target.innerHTML = items
      .map((item) => {
        const title = `User ${item.user_id} · ${escapeHtml(item.username || item.external_id || "без имени")}`;
        const meta = itemMeta([
          `Сообщений: ${escapeHtml(item.messages_count || 0)}`,
          `Последнее: ${escapeHtml(item.last_message_at || "-")}`,
          `Канал: ${escapeHtml(item.channel || "-")}`,
        ]);
        return (
          `<article class="item">` +
          `<h3 class="item-title">${title}</h3>` +
          `${meta}` +
          `<p class="item-text"><button class="glass-btn open-history" data-user-id="${item.user_id}">Открыть историю</button></p>` +
          `</article>`
        );
      })
      .join("");

    target.querySelectorAll(".open-history").forEach((button) => {
      button.addEventListener("click", () => {
        const userId = button.getAttribute("data-user-id");
        const field = document.getElementById("historyUserId");
        field.value = userId;
        switchTab("history");
        loadHistory();
      });
    });
  }

  async function loadHistory() {
    const target = document.getElementById("historyList");
    const userId = document.getElementById("historyUserId").value.trim();
    if (!userId) {
      renderEmpty(target, "Введите user_id и нажмите «Загрузить».");
      return;
    }

    target.innerHTML = "";
    const payload = await apiGet(`/admin/miniapp/api/conversations/${encodeURIComponent(userId)}?limit=500`);
    const items = payload.messages || [];
    if (!items.length) {
      renderEmpty(target, "Сообщения не найдены.");
      return;
    }

    target.innerHTML = items
      .map((item) => {
        const meta = itemMeta([
          `Направление: ${escapeHtml(item.direction || "-")}`,
          `Время: ${escapeHtml(item.created_at || "-")}`,
        ]);
        return (
          `<article class="item">` +
          `${meta}` +
          `<p class="item-text">${escapeHtml(item.text || "")}</p>` +
          `</article>`
        );
      })
      .join("");
  }

  function switchTab(tabId) {
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.classList.toggle("active", tab.dataset.tab === tabId);
    });
    document.querySelectorAll(".panel").forEach((panel) => {
      panel.classList.toggle("active", panel.id === `panel-${tabId}`);
    });
  }

  async function bootstrap() {
    state.initData = getTelegramInitData();
    if (!state.initData) {
      setStatus("Откройте miniapp из Telegram", "error");
      renderError(document.getElementById("leadsList"), "initData не найден. Запустите miniapp через команду /adminapp.");
      return;
    }

    try {
      await loadMe();
      await loadLeads();
      await loadConversations();
      renderEmpty(document.getElementById("historyList"), "Введите user_id и нажмите «Загрузить».");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Неизвестная ошибка";
      setStatus("Ошибка авторизации", "error");
      renderError(document.getElementById("leadsList"), message);
      renderError(document.getElementById("conversationsList"), message);
      renderError(document.getElementById("historyList"), message);
    }
  }

  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => switchTab(tab.dataset.tab));
  });
  document.getElementById("refreshLeads").addEventListener("click", () => {
    loadLeads().catch((error) => renderError(document.getElementById("leadsList"), error.message));
  });
  document.getElementById("refreshConversations").addEventListener("click", () => {
    loadConversations().catch((error) => renderError(document.getElementById("conversationsList"), error.message));
  });
  document.getElementById("loadHistory").addEventListener("click", () => {
    loadHistory().catch((error) => renderError(document.getElementById("historyList"), error.message));
  });

  bootstrap();
})();
