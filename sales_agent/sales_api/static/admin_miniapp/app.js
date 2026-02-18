(function () {
  const state = {
    initData: "",
    me: null,
    tabHistory: ["leads"],
    meta: {
      advisorName: "Гид",
      managerLabel: "Менеджер",
      managerChatUrl: "",
      userMiniappUrl: "/app",
    },
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

  function setQuickActionLabels() {
    const guideBtn = document.getElementById("quickGuide");
    const managerBtn = document.getElementById("quickManager");
    if (guideBtn) {
      guideBtn.textContent = `Спросить ${state.meta.advisorName}`;
    }
    if (managerBtn) {
      managerBtn.textContent = `Написать ${String(state.meta.managerLabel || "менеджеру").toLowerCase()}`;
    }
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

  function openExternal(url) {
    const target = String(url || "").trim();
    if (!target) {
      return false;
    }
    const webApp = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    try {
      if (target.startsWith("https://t.me/") || target.startsWith("tg://")) {
        if (webApp && typeof webApp.openTelegramLink === "function") {
          webApp.openTelegramLink(target);
          return true;
        }
      } else if (webApp && typeof webApp.openLink === "function") {
        webApp.openLink(target);
        return true;
      }
    } catch (_error) {
      // Fall through to window.open fallback below.
    }
    try {
      window.open(target, "_blank", "noopener,noreferrer");
      return true;
    } catch (_windowError) {
      return false;
    }
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

  async function loadMiniappMeta() {
    try {
      const payload = await apiGet("/api/miniapp/meta");
      if (payload && payload.ok) {
        if (payload.advisor_name) {
          state.meta.advisorName = String(payload.advisor_name).trim() || "Гид";
        }
        if (payload.manager_label) {
          state.meta.managerLabel = String(payload.manager_label).trim() || "Менеджер";
        }
        if (typeof payload.manager_chat_url === "string") {
          state.meta.managerChatUrl = payload.manager_chat_url.trim();
        }
        if (payload.user_miniapp_url) {
          state.meta.userMiniappUrl = String(payload.user_miniapp_url).trim() || "/app";
        }
      }
    } catch (_error) {
      // keep defaults
    }
    setQuickActionLabels();
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

  function switchTab(tabId, pushHistory) {
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.classList.toggle("active", tab.dataset.tab === tabId);
    });
    document.querySelectorAll(".mobile-tab[data-tab-target]").forEach((tab) => {
      tab.classList.toggle("active", tab.dataset.tabTarget === tabId);
    });
    document.querySelectorAll(".panel").forEach((panel) => {
      panel.classList.toggle("active", panel.id === `panel-${tabId}`);
    });
    if (pushHistory !== false) {
      const last = state.tabHistory[state.tabHistory.length - 1];
      if (last !== tabId) {
        state.tabHistory.push(tabId);
      }
    }
  }

  function goBack() {
    if (state.tabHistory.length <= 1) {
      switchTab("leads", false);
      return;
    }
    state.tabHistory.pop();
    const prev = state.tabHistory[state.tabHistory.length - 1] || "leads";
    switchTab(prev, false);
  }

  function openGuide() {
    const target = String(state.meta.userMiniappUrl || "/app").trim();
    if (target.startsWith("http://") || target.startsWith("https://") || target.startsWith("tg://")) {
      if (!openExternal(target)) {
        setStatus("Не удалось открыть Гида", "error");
      }
      return;
    }
    window.location.href = target.startsWith("/") ? target : `/${target}`;
  }

  function openManager() {
    if (!state.meta.managerChatUrl) {
      setStatus("Ссылка менеджера не настроена в окружении", "error");
      return;
    }
    if (!openExternal(state.meta.managerChatUrl)) {
      setStatus("Не удалось открыть чат менеджера", "error");
    }
  }

  async function bootstrap() {
    state.initData = getTelegramInitData();
    if (!state.initData) {
      setStatus("Откройте miniapp из Telegram", "error");
      renderError(document.getElementById("leadsList"), "initData не найден. Запустите miniapp через команду /adminapp.");
      return;
    }

    try {
      await loadMiniappMeta();
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
    tab.addEventListener("click", () => switchTab(tab.dataset.tab, true));
  });
  document.querySelectorAll(".mobile-tab[data-tab-target]").forEach((tab) => {
    tab.addEventListener("click", () => switchTab(tab.dataset.tabTarget, true));
  });
  document.querySelectorAll(".mobile-tab[data-action='guide']").forEach((tab) => {
    tab.addEventListener("click", () => openGuide());
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
  document.getElementById("quickBack").addEventListener("click", () => goBack());
  document.getElementById("quickGuide").addEventListener("click", () => openGuide());
  document.getElementById("quickManager").addEventListener("click", () => openManager());

  bootstrap();
})();
