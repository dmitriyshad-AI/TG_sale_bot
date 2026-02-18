import "./styles/tokens.css";
import "./styles/components.css";
import "./styles/app.css";
import defaultBrandLogoUrl from "./assets/brand-kmipt.svg";
import {
  buildAuthHeaders,
  initTelegramContext,
  openExternalLink,
  triggerHaptic,
  type TelegramWebApp,
  type TelegramWebAppUser
} from "./tg";

type HomeAction = {
  key: "pick" | "ask" | "consult";
  title: string;
  subtitle: string;
  emoji: string;
};

type ChoiceOption = {
  label: string;
  value: string;
};

type SearchCriteria = {
  brand: string;
  grade: number | null;
  goal: string | null;
  subject: string | null;
  format: string | null;
};

type CatalogItem = {
  id: string;
  title: string;
  url: string;
  usp: string[];
  price_text: string;
  next_start_text: string;
  why_match: string;
};

type CatalogResponse = {
  ok: boolean;
  count: number;
  items: CatalogItem[];
  match_quality?: "strong" | "limited" | "none";
  manager_recommended?: boolean;
  manager_message?: string;
  manager_call_to_action?: string;
};

type AssistantRecommendedItem = {
  id: string;
  title: string;
  url: string;
  why_match: string;
};

type AssistantResponse = {
  ok: boolean;
  request_id?: string;
  mode: "knowledge" | "consultative" | "general";
  answer_text: string;
  sources: string[];
  used_fallback: boolean;
  match_quality: "strong" | "limited" | "none";
  recommended_products: AssistantRecommendedItem[];
  manager_offer: {
    recommended: boolean;
    message: string;
    call_to_action: string;
  };
  processing_note: string;
};

type AssistantErrorDetail = {
  code?: string;
  message?: string;
  user_message?: string;
  request_id?: string;
};

type MiniappMetaResponse = {
  ok: boolean;
  brand_name?: string;
  advisor_name?: string;
  manager_label?: string;
  manager_chat_url?: string;
  user_miniapp_url?: string;
};

type MiniAppPayload = {
  flow: "catalog" | "consultation_request";
  criteria: SearchCriteria;
  top: Array<{ id: string; title: string; url: string }>;
  question?: string;
  note?: string;
};

type AuthResponse =
  | { ok: true; user: TelegramWebAppUser }
  | { ok: false; reason: string; user: null };

type AppView = "home" | "picker" | "results" | "chat";

type ChatMessage = {
  role: "user" | "assistant";
  text: string;
  sources?: string[];
  meta?: string;
};

class AssistantApiError extends Error {
  readonly userMessage: string;
  readonly requestId: string | null;

  constructor(userMessage: string, requestId: string | null) {
    super(userMessage);
    this.name = "AssistantApiError";
    this.userMessage = userMessage;
    this.requestId = requestId;
  }
}

type ManagerOffer = {
  recommended: boolean;
  message: string;
  callToAction: string;
};

type AppState = {
  view: AppView;
  criteria: SearchCriteria;
  results: CatalogItem[];
  matchQuality: "strong" | "limited" | "none";
  managerRecommended: boolean;
  managerMessage: string;
  managerCallToAction: string;
  loading: boolean;
  error: string | null;
  statusLine: string;
  initData: string;
  user: TelegramWebAppUser | null;
  coachmarkStep: number;
  chatInput: string;
  chatMessages: ChatMessage[];
  chatLoading: boolean;
  chatProgressText: string;
  chatElapsedSec: number;
  lastManagerOffer: ManagerOffer | null;
  brandName: string;
  advisorName: string;
  managerLabel: string;
  managerChatUrl: string;
  userMiniappUrl: string;
};

const HOME_ACTIONS: HomeAction[] = [
  {
    key: "pick",
    title: "Быстрый подбор",
    subtitle: "4 шага и готовые варианты",
    emoji: "🎯"
  },
  {
    key: "ask",
    title: "Спросить Гида",
    subtitle: "Любой вопрос про учебу и поступление",
    emoji: "💬"
  },
  {
    key: "consult",
    title: "Связаться с менеджером",
    subtitle: "Личный разбор с менеджером",
    emoji: "📞"
  }
];

const GOAL_OPTIONS: ChoiceOption[] = [
  { label: "ЕГЭ", value: "ege" },
  { label: "ОГЭ", value: "oge" },
  { label: "Олимпиады", value: "olympiad" },
  { label: "Лагерь", value: "camp" },
  { label: "Успеваемость", value: "base" }
];

const SUBJECT_OPTIONS: ChoiceOption[] = [
  { label: "Математика", value: "math" },
  { label: "Физика", value: "physics" },
  { label: "Информатика", value: "informatics" }
];

const FORMAT_OPTIONS: ChoiceOption[] = [
  { label: "Онлайн", value: "online" },
  { label: "Очно", value: "offline" },
  { label: "Гибрид", value: "hybrid" }
];

const CHAT_PROMPTS = [
  "План поступления в МФТИ (10 класс)",
  "Как подтянуть математику в 8 классе"
];

const CHAT_PROGRESS_STEPS = [
  "Смотрю ваш контекст…",
  "Собираю ответ…",
  "Проверяю рекомендации…"
];

const VIEW_TITLES: Record<AppView, string> = {
  home: "Главная",
  picker: "Подбор",
  results: "Варианты",
  chat: "Чат с гидом",
};

const COACHMARK_STORAGE_KEY = "kmipt_sales_miniapp_coachmarks_v2";
const COACHMARKS = [
  "1/3 Выберите класс.",
  "2/3 Укажите цель и предмет.",
  "3/3 Получите варианты или задайте вопрос Гиду."
];
const CUSTOM_BRAND_LOGO_URL = "/brand-kmipt.png";
const DEFAULT_MANAGER_TELEGRAM_USERNAME = "unpk_mipt";
const MAX_MANAGER_CONTEXT_LENGTH = 900;

const rootNode = document.getElementById("app");
if (!rootNode) {
  throw new Error("App root not found");
}
const appRoot: HTMLElement = rootNode;

const telegram = initTelegramContext();
const webApp = telegram.webApp;
let mainButtonHandler: (() => void) | null = null;
let chatProgressTimer: number | null = null;

function shouldShowCoachmarks(): boolean {
  try {
    return localStorage.getItem(COACHMARK_STORAGE_KEY) !== "1";
  } catch (_error) {
    return false;
  }
}

function markCoachmarksComplete(): void {
  try {
    localStorage.setItem(COACHMARK_STORAGE_KEY, "1");
  } catch (_error) {
    // no-op in private mode
  }
  state.coachmarkStep = -1;
}

function isLowEndDevice(): boolean {
  const hardware = navigator.hardwareConcurrency;
  const memory = (navigator as Navigator & { deviceMemory?: number }).deviceMemory;
  return (typeof hardware === "number" && hardware > 0 && hardware <= 4) || (typeof memory === "number" && memory <= 4);
}

if (isLowEndDevice()) {
  document.body.classList.add("low-end-device");
}

const state: AppState = {
  view: "home",
  criteria: {
    brand: "kmipt",
    grade: null,
    goal: null,
    subject: null,
    format: null
  },
  results: [],
  matchQuality: "none",
  managerRecommended: false,
  managerMessage: "",
  managerCallToAction: "",
  loading: false,
  error: null,
  statusLine: "Проверяю подключение к Telegram…",
  initData: telegram.initData,
  user: telegram.user,
  coachmarkStep: shouldShowCoachmarks() ? 0 : -1,
  chatInput: "",
  chatMessages: [],
  chatLoading: false,
  chatProgressText: CHAT_PROGRESS_STEPS[0],
  chatElapsedSec: 0,
  lastManagerOffer: null,
  brandName: "УНПК МФТИ",
  advisorName: "Гид",
  managerLabel: "Менеджер",
  managerChatUrl: "",
  userMiniappUrl: "/app"
};

function clearChatProgressTimer(): void {
  if (chatProgressTimer !== null) {
    window.clearInterval(chatProgressTimer);
    chatProgressTimer = null;
  }
}

function startChatProgress(): void {
  clearChatProgressTimer();
  state.chatElapsedSec = 0;
  state.chatProgressText = CHAT_PROGRESS_STEPS[0];
  chatProgressTimer = window.setInterval(() => {
    state.chatElapsedSec += 1;
    const index = Math.min(CHAT_PROGRESS_STEPS.length - 1, Math.floor(state.chatElapsedSec / 3));
    state.chatProgressText = CHAT_PROGRESS_STEPS[index];
    render();
  }, 1000);
}

function stopChatProgress(): void {
  clearChatProgressTimer();
  state.chatElapsedSec = 0;
  state.chatProgressText = CHAT_PROGRESS_STEPS[0];
}

function navigateTo(view: AppView): void {
  state.error = null;
  state.view = view;
  render();
}

function canGoBack(): boolean {
  return state.view !== "home";
}

function goBack(): void {
  state.error = null;
  if (state.view === "chat") {
    state.view = state.results.length > 0 ? "results" : "picker";
  } else if (state.view === "results") {
    state.view = "picker";
  } else if (state.view === "picker") {
    state.view = "home";
  } else {
    state.view = "home";
  }
  render();
}

function guideActionText(): string {
  return `Спросить ${state.advisorName}`;
}

function managerActionText(): string {
  return `Связаться с ${state.managerLabel.toLowerCase()}`;
}

function attachBrandLogoFallback(img: HTMLImageElement | null): void {
  if (!img) {
    return;
  }
  img.src = CUSTOM_BRAND_LOGO_URL;
  img.onerror = () => {
    img.onerror = null;
    img.src = defaultBrandLogoUrl;
  };
}

function createActionCard(action: HomeAction): HTMLButtonElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "glassCard actionCard";
  button.dataset.action = action.key;

  const title = document.createElement("h3");
  title.className = "sectionTitle";
  const titleText =
    action.key === "ask" ? `${action.emoji} ${guideActionText()}` : `${action.emoji} ${action.title}`;
  title.textContent = titleText;

  const subtitle = document.createElement("p");
  subtitle.className = "actionSubtitle";
  subtitle.textContent =
    action.key === "ask"
      ? "Пишите свободно, отвечу по делу."
      : action.subtitle;

  const chip = document.createElement("span");
  chip.className = "chip";
  chip.textContent = "Перейти";

  button.append(title, subtitle, chip);
  return button;
}

function renderHeader(statusText: string): HTMLElement {
  const hero = document.createElement("header");
  hero.className = "hero glassCard";
  const name = state.user?.first_name ? `, ${state.user.first_name}` : "";
  hero.innerHTML = `
    <div class="heroBrandLine">
      <img src="${defaultBrandLogoUrl}" alt="Логотип ${state.brandName}" class="heroLogo">
      <p class="eyebrow">${state.brandName} • Sales Agent</p>
    </div>
    <h1 class="heroTitle">Помогаю выбрать обучение${name}</h1>
    <p class="heroSubtitle">${statusText}</p>
    <p class="heroHint">Кнопка «Спросить Гида» всегда внизу.</p>
  `;
  attachBrandLogoFallback(hero.querySelector("img.heroLogo"));
  return hero;
}

function createBrandMark(): HTMLElement {
  const brand = document.createElement("div");
  brand.className = "brandMark";
  brand.innerHTML = `
    <img src="${defaultBrandLogoUrl}" alt="Логотип ${state.brandName}" class="brandLogo">
    <span class="brandText">${state.brandName}</span>
  `;
  attachBrandLogoFallback(brand.querySelector("img.brandLogo"));
  return brand;
}

function openManagerChat(): void {
  const contextText = buildManagerContextSummary();
  const encoded = encodeURIComponent(contextText);
  const username = resolveManagerUsername();
  const preferredLinks = [
    `tg://resolve?domain=${username}&text=${encoded}`,
    `https://t.me/${username}?text=${encoded}`,
  ];

  for (const link of preferredLinks) {
    if (openExternalLink(webApp, link)) {
      return;
    }
  }

  sendConsultationRequestToChat();
  state.error = "Не удалось открыть чат менеджера напрямую. Контекст отправлен в бот.";
  render();
}

function resolveManagerUsername(): string {
  const direct = DEFAULT_MANAGER_TELEGRAM_USERNAME.trim();
  if (direct) {
    return direct;
  }
  const fromMeta = state.managerChatUrl.trim().replace(/^https?:\/\/t\.me\//i, "").replace(/^@/, "");
  return fromMeta || "unpk_mipt";
}

function compactValue(value: string | null | undefined, fallback = "не указано"): string {
  const normalized = (value || "").trim();
  return normalized || fallback;
}

function compactCriteriaSummary(): string {
  const grade = state.criteria.grade ? `${state.criteria.grade} кл.` : "класс не указан";
  const goalMap: Record<string, string> = {
    ege: "ЕГЭ",
    oge: "ОГЭ",
    olympiad: "олимпиады",
    camp: "лагерь",
    base: "успеваемость",
  };
  const subjectMap: Record<string, string> = {
    math: "математика",
    physics: "физика",
    informatics: "информатика",
  };
  const formatMap: Record<string, string> = {
    online: "онлайн",
    offline: "очно",
    hybrid: "гибрид",
  };
  const goal = goalMap[state.criteria.goal || ""] || compactValue(state.criteria.goal);
  const subject = subjectMap[state.criteria.subject || ""] || compactValue(state.criteria.subject);
  const mode = formatMap[state.criteria.format || ""] || compactValue(state.criteria.format);
  return `${grade}; цель: ${goal}; предмет: ${subject}; формат: ${mode}`;
}

function compactProductsSummary(): string {
  if (!state.results.length) {
    return "подбор пока без финального совпадения";
  }
  const titles = state.results.slice(0, 3).map((item) => item.title.trim()).filter(Boolean);
  return titles.length ? titles.join(" | ") : "подбор есть, названия уточняются";
}

function compactDialogueSummary(): string {
  const userMessages = state.chatMessages.filter((item) => item.role === "user").slice(-2);
  if (!userMessages.length) {
    return "в Mini App еще не было свободного вопроса";
  }
  return userMessages
    .map((item, index) => `${index + 1}) ${item.text.replace(/\s+/g, " ").trim()}`)
    .join(" ");
}

function trimForManager(text: string): string {
  const normalized = text.replace(/\s+\n/g, "\n").trim();
  if (normalized.length <= MAX_MANAGER_CONTEXT_LENGTH) {
    return normalized;
  }
  return `${normalized.slice(0, MAX_MANAGER_CONTEXT_LENGTH - 1).trimEnd()}…`;
}

function buildManagerContextSummary(): string {
  const personParts: string[] = [];
  if (state.user?.first_name) {
    personParts.push(state.user.first_name);
  }
  if (state.user?.username) {
    personParts.push(`@${state.user.username}`);
  }
  if (typeof state.user?.id === "number") {
    personParts.push(`id:${state.user.id}`);
  }
  const who = personParts.length ? personParts.join(" ") : "пользователь Mini App";

  const lines = [
    "Здравствуйте! Это запрос из клиентского Mini App УНПК МФТИ.",
    `Клиент: ${who}.`,
    `Контекст запроса: ${compactCriteriaSummary()}.`,
    `Подобранные программы: ${compactProductsSummary()}.`,
    `Последние вопросы клиента: ${compactDialogueSummary()}.`,
    "Просьба: связаться и дать персональную траекторию обучения.",
  ];

  return trimForManager(lines.join("\n"));
}

function createTopNav(): HTMLElement {
  const nav = document.createElement("section");
  nav.className = "glassCard topNav";

  const left = document.createElement("div");
  left.className = "topNavLeft";
  if (canGoBack()) {
    const back = document.createElement("button");
    back.type = "button";
    back.className = "glassButton navBackButton";
    back.textContent = "Назад";
    back.addEventListener("click", () => {
      triggerHaptic(webApp, "light");
      goBack();
    });
    left.appendChild(back);
  }
  left.append(createBrandMark());

  const tabs = document.createElement("div");
  tabs.className = "topNavTabs";
  const routes: Array<{ view: AppView; label: string }> = [
    { view: "home", label: "Домой" },
    { view: "picker", label: "Подбор" },
    { view: "chat", label: "Гид" },
  ];
  for (const route of routes) {
    const tab = document.createElement("button");
    tab.type = "button";
    tab.className = "chipButton topNavTab";
    tab.textContent = route.label;
    if (state.view === route.view || (route.view === "picker" && state.view === "results")) {
      tab.classList.add("isActive");
    }
    tab.addEventListener("click", () => {
      triggerHaptic(webApp, "light");
      navigateTo(route.view);
    });
    tabs.appendChild(tab);
  }

  nav.append(left, tabs);
  return nav;
}

function createChipGroup(
  title: string,
  options: ChoiceOption[],
  selectedValue: string | null,
  onSelect: (value: string) => void
): HTMLElement {
  const section = document.createElement("section");
  section.className = "glassCard pickerSection";

  const label = document.createElement("h3");
  label.className = "sectionTitle sectionTitleCompact";
  label.textContent = title;

  const chips = document.createElement("div");
  chips.className = "chipGrid";

  for (const option of options) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "chipButton";
    if (selectedValue === option.value) {
      button.classList.add("isActive");
    }
    button.textContent = option.label;
    button.addEventListener("click", () => {
      triggerHaptic(webApp, "light");
      state.error = null;
      onSelect(option.value);
      updateCoachmarkProgress();
      render();
    });
    chips.appendChild(button);
  }

  section.append(label, chips);
  return section;
}

function createGradeGroup(): HTMLElement {
  const options: ChoiceOption[] = Array.from({ length: 11 }, (_unused, index) => ({
    label: String(index + 1),
    value: String(index + 1)
  }));
  return createChipGroup("Шаг 1. Класс ученика", options, state.criteria.grade ? String(state.criteria.grade) : null, (value) => {
    state.criteria.grade = Number(value);
  });
}

function createHomeView(): HTMLElement {
  const section = document.createElement("section");
  section.className = "actions";

  HOME_ACTIONS.forEach((action, index) => {
    const card = createActionCard(action);
    card.style.setProperty("--index", String(index));
    card.addEventListener("click", () => {
      triggerHaptic(webApp, "light");
      state.error = null;
      if (action.key === "pick") {
        navigateTo("picker");
        return;
      }
      if (action.key === "ask") {
        navigateTo("chat");
        return;
      }
      openManagerChat();
    });
    section.appendChild(card);
  });

  return section;
}

function createPickerView(): HTMLElement {
  const container = document.createElement("section");
  container.className = "pickerStack";

  const doneCount = [state.criteria.grade, state.criteria.goal, state.criteria.subject, state.criteria.format].filter(
    (item) => item !== null && item !== ""
  ).length;
  const intro = document.createElement("article");
  intro.className = "glassCard pickerIntro";
  intro.innerHTML = `
    <h3 class="sectionTitle sectionTitleCompact">Подбор за 4 шага</h3>
    <p class="actionSubtitle">Готовность: ${doneCount}/4. После этого покажем лучшие варианты.</p>
  `;
  container.appendChild(intro);

  container.appendChild(createGradeGroup());
  container.appendChild(
    createChipGroup("Шаг 2. Цель подготовки", GOAL_OPTIONS, state.criteria.goal, (value) => {
      state.criteria.goal = value;
    })
  );
  container.appendChild(
    createChipGroup("Шаг 3. Предмет", SUBJECT_OPTIONS, state.criteria.subject, (value) => {
      state.criteria.subject = value;
    })
  );
  container.appendChild(
    createChipGroup("Шаг 4. Формат занятий", FORMAT_OPTIONS, state.criteria.format, (value) => {
      state.criteria.format = value;
    })
  );

  const controls = document.createElement("div");
  controls.className = "pickerControls";

  const askBtn = document.createElement("button");
  askBtn.type = "button";
  askBtn.className = "glassButton";
  askBtn.textContent = guideActionText();
  askBtn.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("chat");
  });

  const submit = document.createElement("button");
  submit.type = "button";
  submit.className = "glassButton glassButtonPrimary";
  submit.textContent = state.loading ? "Подбираю…" : "Получить подбор";
  submit.disabled = !isCriteriaComplete() || state.loading;
  submit.addEventListener("click", () => {
    triggerHaptic(webApp, "medium");
    void loadCatalogResults();
  });

  controls.append(askBtn, submit);
  container.appendChild(controls);

  return container;
}

function createResultSummaryCard(): HTMLElement {
  const card = document.createElement("article");
  card.className = "glassCard resultSummaryCard";

  const title = document.createElement("h3");
  title.className = "sectionTitle sectionTitleCompact";
  title.textContent = "Результат подбора";

  const text = document.createElement("p");
  text.className = "actionSubtitle";

  if (state.matchQuality === "strong" && state.results.length > 0) {
    text.textContent = `Есть сильное совпадение: ${state.results[0].title}.`;
  } else if (state.results.length > 0) {
    text.textContent =
      state.managerMessage ||
      "Есть хорошие варианты. Для финального выбора подключим менеджера.";
  } else {
    text.textContent =
      "Идеального совпадения нет, но у нас есть подходящие решения под ваш запрос.";
  }

  const cta = document.createElement("p");
  cta.className = "resultSupportText";
  cta.textContent =
    state.managerCallToAction ||
    "Оставьте контакт, и менеджер подберет вариант под цель и сроки.";

  card.append(title, text, cta);
  return card;
}

function createResultsView(): HTMLElement {
  const section = document.createElement("section");
  section.className = "resultsGrid";

  section.appendChild(createResultSummaryCard());

  if (state.results.length === 0) {
    const empty = document.createElement("article");
    empty.className = "glassCard resultCard";
    empty.innerHTML = `
      <h3 class="sectionTitle sectionTitleCompact">Подбор требует ручной точной настройки</h3>
      <p class="actionSubtitle">Оставьте контакт или задайте вопрос в чате. Подберем персонально.</p>
    `;
    section.appendChild(empty);
  } else {
    for (const item of state.results) {
      const card = document.createElement("article");
      card.className = "glassCard resultCard";

      const title = document.createElement("h3");
      title.className = "sectionTitle sectionTitleCompact";
      title.textContent = item.title;

      const why = document.createElement("p");
      why.className = "actionSubtitle";
      why.textContent = item.why_match;

      const meta = document.createElement("p");
      meta.className = "resultMeta";
      meta.textContent = `${item.price_text} • Ближайший старт: ${item.next_start_text}`;

      const uspList = document.createElement("ul");
      uspList.className = "uspList";
      for (const bullet of item.usp) {
        const li = document.createElement("li");
        li.textContent = bullet;
        uspList.appendChild(li);
      }

      const link = document.createElement("a");
      link.className = "glassButton resultLink";
      link.href = item.url;
      link.target = "_blank";
      link.rel = "noreferrer";
      link.textContent = "Подробнее о программе";

      card.append(title, why, meta, uspList, link);
      section.appendChild(card);
    }
  }

  const actions = document.createElement("div");
  actions.className = "resultsActions";

  const askButton = document.createElement("button");
  askButton.type = "button";
  askButton.className = "glassButton";
  askButton.textContent = guideActionText();
  askButton.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("chat");
  });

  const contactButton = document.createElement("button");
  contactButton.type = "button";
  contactButton.className = "glassButton glassButtonPrimary";
  contactButton.textContent = managerActionText();
  contactButton.addEventListener("click", () => {
    openManagerChat();
  });

  actions.append(askButton, contactButton);
  section.appendChild(actions);
  return section;
}

function createChatMessage(item: ChatMessage): HTMLElement {
  const bubble = document.createElement("article");
  bubble.className = `glassCard chatBubble ${item.role === "user" ? "chatBubbleUser" : "chatBubbleAssistant"}`;

  const role = document.createElement("p");
  role.className = "chatRole";
  role.textContent = item.role === "user" ? "Вы" : state.advisorName;

  const text = document.createElement("p");
  text.className = "chatText";
  text.textContent = item.text;

  bubble.append(role, text);

  if (item.meta) {
    const meta = document.createElement("p");
    meta.className = "chatMeta";
    meta.textContent = item.meta;
    bubble.appendChild(meta);
  }

  if (item.sources && item.sources.length > 0) {
    const sourcesWrap = document.createElement("div");
    sourcesWrap.className = "chatSources";
    for (const source of item.sources.slice(0, 3)) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = source;
      sourcesWrap.appendChild(chip);
    }
    bubble.appendChild(sourcesWrap);
  }

  return bubble;
}

function createChatQuickPrompts(): HTMLElement {
  const row = document.createElement("div");
  row.className = "chatQuickRow";
  for (const prompt of CHAT_PROMPTS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "chipButton";
    button.textContent = prompt;
    button.addEventListener("click", () => {
      triggerHaptic(webApp, "light");
      state.chatInput = prompt;
      render();
      void askAssistantQuestion(prompt);
    });
    row.appendChild(button);
  }
  return row;
}

function createChatView(): HTMLElement {
  const container = document.createElement("section");
  container.className = "chatStack";

  const intro = document.createElement("article");
  intro.className = "glassCard chatIntro";
  intro.innerHTML = `
    <h3 class="sectionTitle sectionTitleCompact">Чат с ${state.advisorName}</h3>
    <p class="actionSubtitle">Пишите свободно. Отвечу по стратегии, предметам и программам.</p>
  `;
  container.appendChild(intro);
  container.appendChild(createChatQuickPrompts());

  const messages = document.createElement("div");
  messages.className = "chatMessages";
  if (state.chatMessages.length === 0) {
    const empty = document.createElement("article");
    empty.className = "glassCard chatBubble chatBubbleAssistant";
    empty.innerHTML = `
      <p class="chatRole">${state.advisorName}</p>
      <p class="chatText">Напишите вопрос в 1-2 фразах. Например: «Как начать подготовку к ЕГЭ в 10 классе?»</p>
    `;
    messages.appendChild(empty);
  } else {
    for (const item of state.chatMessages) {
      messages.appendChild(createChatMessage(item));
    }
  }

  if (state.chatLoading) {
    const progress = document.createElement("article");
    progress.className = "glassCard chatProgress";
    progress.innerHTML = `
      <p class="chatRole">${state.advisorName}</p>
      <p class="chatText progressPulse">${state.chatProgressText}</p>
      <p class="chatMeta">Прошло: ${state.chatElapsedSec} сек</p>
    `;
    messages.appendChild(progress);
  }

  container.appendChild(messages);

  if (state.lastManagerOffer?.recommended) {
    const managerCard = document.createElement("article");
    managerCard.className = "glassCard managerOfferCard";
    managerCard.innerHTML = `
      <h3 class="sectionTitle sectionTitleCompact">Персональный подбор с менеджером</h3>
      <p class="actionSubtitle">${state.lastManagerOffer.message}</p>
      <p class="resultSupportText">${state.lastManagerOffer.callToAction}</p>
    `;
    const managerButton = document.createElement("button");
    managerButton.type = "button";
    managerButton.className = "glassButton glassButtonPrimary";
    managerButton.textContent = managerActionText();
    managerButton.addEventListener("click", () => {
      openManagerChat();
    });
    managerCard.appendChild(managerButton);
    container.appendChild(managerCard);
  }

  const composer = document.createElement("div");
  composer.className = "glassCard chatComposer";

  const textarea = document.createElement("textarea");
  textarea.className = "chatTextarea";
  textarea.rows = 4;
  textarea.maxLength = 2000;
  textarea.placeholder = "Напишите вопрос. Например: «Как подготовиться к поступлению в МФТИ?»";
  textarea.value = state.chatInput;
  textarea.disabled = state.chatLoading;
  textarea.addEventListener("input", () => {
    state.chatInput = textarea.value;
    render();
  });

  const controls = document.createElement("div");
  controls.className = "chatControls";

  const back = document.createElement("button");
  back.type = "button";
  back.className = "glassButton";
  back.textContent = "К подбору";
  back.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("picker");
  });

  const send = document.createElement("button");
  send.type = "button";
  send.className = "glassButton glassButtonPrimary";
  send.textContent = state.chatLoading ? "Обрабатываю…" : "Отправить вопрос";
  send.disabled = state.chatLoading || state.chatInput.trim().length === 0;
  send.addEventListener("click", () => {
    triggerHaptic(webApp, "medium");
    void askAssistantQuestion();
  });

  controls.append(back, send);
  composer.append(textarea, controls);
  container.appendChild(composer);

  return container;
}

function createBottomDock(): HTMLElement {
  const bottom = document.createElement("footer");
  bottom.className = "bottomDock glassCard";

  const label = document.createElement("span");
  label.className = "dockLabel";
  if (state.view === "chat") {
    label.textContent = `${state.advisorName} онлайн. Отвечаю в формате диалога.`;
  } else if (state.view === "results") {
    label.textContent = "Варианты готовы. Можем подключить менеджера.";
  } else {
    label.textContent = "Задайте вопрос Гиду или начните подбор.";
  }

  const actions = document.createElement("div");
  actions.className = "dockActions";

  const ask = document.createElement("button");
  ask.className = "glassButton";
  ask.type = "button";
  ask.textContent = state.view === "chat" ? "К подбору" : guideActionText();
  ask.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    if (state.view === "chat") {
      navigateTo("picker");
      return;
    }
    navigateTo("chat");
  });

  const primary = document.createElement("button");
  primary.className = "glassButton glassButtonPrimary";
  primary.type = "button";

  if (state.view === "results") {
    primary.textContent = managerActionText();
    primary.addEventListener("click", () => {
      triggerHaptic(webApp, "medium");
      openManagerChat();
    });
  } else if (state.view === "chat") {
    primary.textContent = state.chatLoading ? "Обрабатываю…" : "Отправить вопрос";
    primary.disabled = state.chatLoading || state.chatInput.trim().length === 0;
    primary.addEventListener("click", () => {
      triggerHaptic(webApp, "medium");
      void askAssistantQuestion();
    });
  } else {
    primary.textContent = "Открыть подбор";
    primary.addEventListener("click", () => {
      triggerHaptic(webApp, "medium");
      navigateTo("picker");
    });
  }

  actions.append(ask, primary);
  bottom.append(label, actions);
  return bottom;
}

function createCoachmark(): HTMLElement | null {
  if (state.coachmarkStep < 0 || state.view === "home" || state.view === "chat") {
    return null;
  }

  const box = document.createElement("section");
  box.className = "glassCard coachmark";
  const content = document.createElement("p");
  content.className = "coachmarkText";

  if (state.coachmarkStep >= 2 && state.view === "results") {
    content.textContent = "Готово. Можете задать вопрос или сразу оставить контакт для менеджера.";
  } else {
    const index = Math.min(state.coachmarkStep, COACHMARKS.length - 1);
    content.textContent = COACHMARKS[index];
  }

  const actions = document.createElement("div");
  actions.className = "coachmarkActions";

  const skip = document.createElement("button");
  skip.type = "button";
  skip.className = "glassButton";
  skip.textContent = "Скрыть";
  skip.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    markCoachmarksComplete();
    render();
  });

  const next = document.createElement("button");
  next.type = "button";
  next.className = "glassButton glassButtonPrimary";
  next.textContent = state.coachmarkStep >= 2 ? "Понятно" : "Далее";
  next.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    if (state.coachmarkStep >= 2) {
      markCoachmarksComplete();
    } else {
      state.coachmarkStep += 1;
    }
    render();
  });

  actions.append(skip, next);
  box.append(content, actions);
  return box;
}

function renderError(): HTMLElement | null {
  if (!state.error) {
    return null;
  }
  const box = document.createElement("div");
  box.className = "glassCard errorBox";
  box.textContent = state.error;
  return box;
}

function isCriteriaComplete(): boolean {
  return Boolean(state.criteria.grade && state.criteria.goal && state.criteria.subject && state.criteria.format);
}

function updateCoachmarkProgress(): void {
  if (state.coachmarkStep < 0) {
    return;
  }
  if (state.coachmarkStep === 0 && state.criteria.grade) {
    state.coachmarkStep = 1;
  }
  if (state.coachmarkStep === 1 && state.criteria.goal) {
    state.coachmarkStep = 2;
  }
}

function clearTelegramMainButtonHandler(target: TelegramWebApp | null): void {
  if (!target?.MainButton || !mainButtonHandler) {
    return;
  }
  try {
    target.MainButton.offClick(mainButtonHandler);
  } catch (_error) {
    // ignore
  }
  mainButtonHandler = null;
}

function buildMiniAppPayload(flow: "catalog" | "consultation_request", note?: string): string | null {
  const payload: MiniAppPayload = {
    flow,
    criteria: state.criteria,
    top: state.results.slice(0, 3).map((item) => ({ id: item.id, title: item.title, url: item.url })),
    note
  };
  const serialized = JSON.stringify(payload);
  if (serialized.length >= 4096) {
    return null;
  }
  return serialized;
}

function sendPayloadToChat(payload: string, successText: string): void {
  if (!webApp?.sendData) {
    state.error = "Отправка в чат доступна только внутри Telegram Mini App.";
    render();
    return;
  }
  try {
    webApp.sendData(payload);
    webApp.close?.();
  } catch (_error) {
    state.error = "Не удалось отправить данные в чат. Попробуйте еще раз.";
    render();
    return;
  }
  state.error = successText;
  render();
}

function sendCatalogSelectionToChat(): void {
  triggerHaptic(webApp, "medium");
  const payload = buildMiniAppPayload("catalog", "Пользователь отправил подбор из miniapp.");
  if (!payload) {
    state.error = "Подбор слишком большой для отправки. Попробуйте снова после уточнения параметров.";
    render();
    return;
  }
  sendPayloadToChat(payload, "Подбор отправлен в чат. Продолжим диалог в Telegram.");
}

function sendConsultationRequestToChat(): void {
  triggerHaptic(webApp, "medium");
  const payload = buildMiniAppPayload(
    "consultation_request",
    "Пользователь просит менеджера сделать персональный подбор и связаться."
  );
  if (!payload) {
    state.error = "Не удалось подготовить запрос менеджеру. Сформулируйте коротко запрос в чате.";
    render();
    return;
  }
  sendPayloadToChat(payload, "Запрос менеджеру отправлен. В чате попросим контакт и продолжим.");
}

function syncTelegramMainButton(): void {
  if (!webApp?.MainButton) {
    return;
  }
  clearTelegramMainButtonHandler(webApp);
  const button = webApp.MainButton;

  if (state.view === "picker") {
    button.setText(state.loading ? "Подбираю…" : "Получить подбор");
    if (!isCriteriaComplete() || state.loading) {
      button.disable();
    } else {
      button.enable();
    }
    mainButtonHandler = () => {
      if (isCriteriaComplete() && !state.loading) {
        triggerHaptic(webApp, "medium");
        void loadCatalogResults();
      }
    };
    button.onClick(mainButtonHandler);
    button.show();
    return;
  }

  if (state.view === "results") {
    button.setText(managerActionText());
    button.enable();
    mainButtonHandler = () => openManagerChat();
    button.onClick(mainButtonHandler);
    button.show();
    return;
  }

  if (state.view === "chat") {
    button.setText(state.chatLoading ? "Обрабатываю…" : "Отправить вопрос");
    if (state.chatLoading || state.chatInput.trim().length === 0) {
      button.disable();
    } else {
      button.enable();
    }
    mainButtonHandler = () => {
      if (!state.chatLoading && state.chatInput.trim()) {
        triggerHaptic(webApp, "medium");
        void askAssistantQuestion();
      }
    };
    button.onClick(mainButtonHandler);
    button.show();
    return;
  }

  button.hide();
}

async function loadCatalogResults(): Promise<void> {
  state.loading = true;
  state.error = null;
  state.statusLine = "Собираю лучшие варианты под ваш запрос…";
  render();

  const params = new URLSearchParams({
    brand: state.criteria.brand,
    grade: String(state.criteria.grade),
    goal: String(state.criteria.goal),
    subject: String(state.criteria.subject),
    format: String(state.criteria.format)
  });

  try {
    const response = await fetch(`/api/catalog/search?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Catalog request failed: ${response.status}`);
    }

    const payload = (await response.json()) as CatalogResponse;
    state.results = Array.isArray(payload.items) ? payload.items : [];
    state.matchQuality = payload.match_quality || (state.results.length > 0 ? "limited" : "none");
    state.managerRecommended = Boolean(payload.manager_recommended);
    state.managerMessage = payload.manager_message || "";
    state.managerCallToAction = payload.manager_call_to_action || "";
    state.lastManagerOffer = {
      recommended: state.managerRecommended,
      message: state.managerMessage,
      callToAction: state.managerCallToAction
    };
    state.view = "results";
    state.statusLine = "Подбор готов • можно уточнить вопрос или подключить менеджера";
    if (state.coachmarkStep >= 2) {
      state.coachmarkStep = 2;
    }
  } catch (_error) {
    state.error = "Не удалось получить подбор. Попробуйте еще раз через несколько секунд.";
  } finally {
    state.loading = false;
    render();
  }
}

function toCatalogItem(item: AssistantRecommendedItem): CatalogItem {
  return {
    id: item.id,
    title: item.title,
    url: item.url,
    usp: [],
    price_text: "Цена уточняется у менеджера",
    next_start_text: "Уточним под ваш график",
    why_match: item.why_match || "Подобрано по вашему запросу"
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

async function readAssistantApiError(response: Response): Promise<AssistantApiError> {
  const fallbackMessage = "Не удалось получить ответ. Попробуйте еще раз через несколько секунд.";
  let userMessage = fallbackMessage;
  let requestId = (response.headers.get("X-Request-ID") || "").trim() || null;

  try {
    const payload = (await response.json()) as unknown;
    if (isRecord(payload)) {
      if (typeof payload.user_message === "string" && payload.user_message.trim()) {
        userMessage = payload.user_message.trim();
      }
      if (!requestId && typeof payload.request_id === "string" && payload.request_id.trim()) {
        requestId = payload.request_id.trim();
      }

      const detail = payload.detail;
      if (typeof detail === "string") {
        if (detail.trim() && response.status < 500) {
          userMessage = detail.trim();
        }
      } else if (isRecord(detail)) {
        const typedDetail = detail as AssistantErrorDetail;
        if (typeof typedDetail.user_message === "string" && typedDetail.user_message.trim()) {
          userMessage = typedDetail.user_message.trim();
        } else if (typeof typedDetail.message === "string" && typedDetail.message.trim() && response.status < 500) {
          userMessage = typedDetail.message.trim();
        }
        if (!requestId && typeof typedDetail.request_id === "string" && typedDetail.request_id.trim()) {
          requestId = typedDetail.request_id.trim();
        }
      }
    }
  } catch (_error) {
    // Keep fallback message for non-JSON API failures.
  }

  return new AssistantApiError(userMessage, requestId);
}

function normalizeAssistantApiError(error: unknown): AssistantApiError {
  if (error instanceof AssistantApiError) {
    return error;
  }
  return new AssistantApiError("Не удалось получить ответ. Попробуйте еще раз через несколько секунд.", null);
}

async function askAssistantQuestion(questionOverride?: string): Promise<void> {
  const question = (questionOverride || state.chatInput).trim();
  if (!question || state.chatLoading) {
    return;
  }

  state.error = null;
  state.chatMessages.push({ role: "user", text: question });
  state.chatInput = "";
  state.chatLoading = true;
  startChatProgress();
  state.view = "chat";
  render();

  try {
    const headers: HeadersInit = {
      "Content-Type": "application/json",
      ...buildAuthHeaders(state.initData)
    };
    const response = await fetch("/api/assistant/ask", {
      method: "POST",
      headers,
      body: JSON.stringify({
        question,
        criteria: state.criteria
      })
    });
    if (!response.ok) {
      throw await readAssistantApiError(response);
    }

    const payload = (await response.json()) as AssistantResponse;
    if (!payload.ok) {
      throw new AssistantApiError(
        "Сервис ответа вернул некорректный результат. Попробуйте еще раз через несколько секунд.",
        payload.request_id || null
      );
    }

    const responseRequestId = payload.request_id || (response.headers.get("X-Request-ID") || "").trim() || "";
    const metaText = payload.processing_note || "";
    const metaWithRequestId = responseRequestId ? `${metaText} Код запроса: ${responseRequestId}` : metaText;
    state.chatMessages.push({
      role: "assistant",
      text: payload.answer_text,
      sources: Array.isArray(payload.sources) ? payload.sources : [],
      meta: metaWithRequestId || undefined
    });

    if (Array.isArray(payload.recommended_products) && payload.recommended_products.length > 0) {
      state.results = payload.recommended_products.map(toCatalogItem);
    }

    if (payload.manager_offer) {
      state.lastManagerOffer = {
        recommended: Boolean(payload.manager_offer.recommended),
        message: payload.manager_offer.message || "",
        callToAction: payload.manager_offer.call_to_action || ""
      };
      state.matchQuality = payload.match_quality || state.matchQuality;
      state.managerRecommended = Boolean(payload.manager_offer.recommended);
      state.managerMessage = payload.manager_offer.message || state.managerMessage;
      state.managerCallToAction = payload.manager_offer.call_to_action || state.managerCallToAction;
    }
  } catch (error) {
    const apiError = normalizeAssistantApiError(error);
    state.error = apiError.requestId
      ? `${apiError.userMessage} Код запроса: ${apiError.requestId}`
      : apiError.userMessage;
    const assistantErrorText = apiError.requestId
      ? `Я на связи, но сейчас не удалось завершить обработку. Попробуйте повторить вопрос. Код запроса: ${apiError.requestId}.`
      : "Я на связи, просто сейчас не удалось завершить обработку запроса. Попробуйте повторить вопрос.";
    state.chatMessages.push({
      role: "assistant",
      text: assistantErrorText
    });
  } finally {
    stopChatProgress();
    state.chatLoading = false;
    render();
  }
}

async function loadMiniappMeta(): Promise<void> {
  try {
    const response = await fetch("/api/miniapp/meta");
    if (!response.ok) {
      return;
    }
    const payload = (await response.json()) as MiniappMetaResponse;
    if (!payload.ok) {
      return;
    }
    if (typeof payload.brand_name === "string" && payload.brand_name.trim()) {
      state.brandName = payload.brand_name.trim();
    }
    if (typeof payload.advisor_name === "string" && payload.advisor_name.trim()) {
      state.advisorName = payload.advisor_name.trim();
    }
    if (typeof payload.manager_label === "string" && payload.manager_label.trim()) {
      state.managerLabel = payload.manager_label.trim();
    }
    if (typeof payload.manager_chat_url === "string") {
      state.managerChatUrl = payload.manager_chat_url.trim();
    }
    if (typeof payload.user_miniapp_url === "string" && payload.user_miniapp_url.trim()) {
      state.userMiniappUrl = payload.user_miniapp_url.trim();
    }
  } catch (_error) {
    // Keep defaults when metadata endpoint is unavailable.
  }
}

async function loadWhoAmI(): Promise<void> {
  try {
    const headers = buildAuthHeaders(state.initData);
    const response = await fetch("/api/auth/whoami", { headers });
    if (!response.ok) {
      if (response.status === 401) {
        state.statusLine = "Откройте Mini App из Telegram, чтобы включить персонализацию.";
        return;
      }
      throw new Error(`whoami failed: ${response.status}`);
    }
    const payload = (await response.json()) as AuthResponse;
    if (!payload.ok) {
      state.statusLine = "Демо-режим в браузере • Откройте в Telegram для персонализации";
      return;
    }
    state.user = payload.user;
    state.statusLine = "Онлайн • Подбор за 60 сек и ответы на любые вопросы";
  } catch (_error) {
    state.statusLine = "Подключение к Telegram недоступно • Можно работать в демо-режиме";
  }
}

function render(): void {
  const container = document.createElement("main");
  container.className = "appShell";
  container.appendChild(renderHeader(state.statusLine));
  container.appendChild(createTopNav());

  const error = renderError();
  if (error) {
    container.appendChild(error);
  }

  const coachmark = createCoachmark();
  if (coachmark) {
    container.appendChild(coachmark);
  }

  if (state.view === "home") {
    container.appendChild(createHomeView());
  } else if (state.view === "picker") {
    container.appendChild(createPickerView());
  } else if (state.view === "results") {
    container.appendChild(createResultsView());
  } else {
    container.appendChild(createChatView());
  }

  container.appendChild(createBottomDock());
  appRoot.replaceChildren(container);
  syncTelegramMainButton();
}

render();
void Promise.all([loadMiniappMeta(), loadWhoAmI()]).then(() => render());
