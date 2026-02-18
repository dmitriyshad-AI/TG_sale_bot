import "./styles/tokens.css";
import "./styles/components.css";
import "./styles/app.css";
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
    title: "Подобрать курс",
    subtitle: "Найдём лучший старт под класс, цель и формат",
    emoji: "🎯"
  },
  {
    key: "ask",
    title: "Задать вопрос в любой момент",
    subtitle: "По стратегии, поступлению, предметам и обучению",
    emoji: "💬"
  },
  {
    key: "consult",
    title: "Связаться с менеджером",
    subtitle: "Поможем с персональным подбором и следующим шагом",
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
  "Как построить стратегию поступления в МФТИ для 10 класса?",
  "Что делать, если у ребёнка проседает математика в 8 классе?",
  "С чего начать подготовку к ЕГЭ по физике без перегруза?"
];

const CHAT_PROGRESS_STEPS = [
  "Собираю контекст запроса…",
  "Проверяю, какие варианты подойдут лучше всего…",
  "Готовлю полезный и точный ответ без шаблонов…"
];

const VIEW_TITLES: Record<AppView, string> = {
  home: "Главная",
  picker: "Подбор",
  results: "Варианты",
  chat: "Гид",
};

const COACHMARK_STORAGE_KEY = "kmipt_sales_miniapp_coachmarks_v2";
const COACHMARKS = [
  "1/3 Выберите класс, чтобы отсечь лишние варианты.",
  "2/3 Выберите цель и предмет, чтобы подобрать точнее.",
  "3/3 Если хотите, в любой момент можно перейти к вопросу и общению."
];

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

function createActionCard(action: HomeAction): HTMLButtonElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "glassCard actionCard";
  button.dataset.action = action.key;

  const title = document.createElement("h3");
  title.className = "sectionTitle";
  const titleText =
    action.key === "ask" ? `${action.emoji} Спросить ${state.advisorName}` : `${action.emoji} ${action.title}`;
  title.textContent = titleText;

  const subtitle = document.createElement("p");
  subtitle.className = "actionSubtitle";
  subtitle.textContent =
    action.key === "ask"
      ? "Быстрый диалог по поступлению, стратегии и выбору программы."
      : action.subtitle;

  const chip = document.createElement("span");
  chip.className = "chip";
  chip.textContent = "Открыть";

  button.append(title, subtitle, chip);
  return button;
}

function renderHeader(statusText: string): HTMLElement {
  const hero = document.createElement("header");
  hero.className = "hero glassCard";
  const name = state.user?.first_name ? `, ${state.user.first_name}` : "";
  hero.innerHTML = `
    <p class="eyebrow">${state.brandName} • Sales Agent</p>
    <h1 class="heroTitle">Подбор и консультации без давления${name}</h1>
    <p class="heroSubtitle">${statusText}</p>
    <p class="heroHint">В любой момент можно задать вопрос и продолжить диалог по образованию.</p>
  `;
  return hero;
}

function createBrandMark(): HTMLElement {
  const brand = document.createElement("div");
  brand.className = "brandMark";
  brand.innerHTML = `
    <span class="brandOrb" aria-hidden="true">K</span>
    <span class="brandText">${state.brandName}</span>
  `;
  return brand;
}

function openManagerChat(): void {
  const target = state.managerChatUrl.trim();
  if (target) {
    const opened = openExternalLink(webApp, target);
    if (!opened) {
      state.error = "Не удалось открыть чат менеджера. Попробуйте снова.";
      render();
    }
    return;
  }
  sendConsultationRequestToChat();
}

function createTopNav(): HTMLElement {
  const nav = document.createElement("section");
  nav.className = "glassCard topNav";

  const left = document.createElement("div");
  left.className = "topNavLeft";
  const back = document.createElement("button");
  back.type = "button";
  back.className = "glassButton navBackButton";
  back.textContent = "← Назад";
  back.disabled = !canGoBack();
  back.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    goBack();
  });
  left.append(back, createBrandMark());

  const right = document.createElement("div");
  right.className = "topNavActions";

  const askGuide = document.createElement("button");
  askGuide.type = "button";
  askGuide.className = "glassButton";
  askGuide.textContent = `Спросить ${state.advisorName}`;
  askGuide.addEventListener("click", () => {
    triggerHaptic(webApp, "medium");
    navigateTo("chat");
  });

  const manager = document.createElement("button");
  manager.type = "button";
  manager.className = "glassButton glassButtonPrimary";
  manager.textContent = `Написать ${state.managerLabel.toLowerCase()}`;
  manager.addEventListener("click", () => {
    triggerHaptic(webApp, "medium");
    openManagerChat();
  });

  right.append(askGuide, manager);

  const tabs = document.createElement("div");
  tabs.className = "topNavTabs";
  const routes: Array<{ view: AppView; label: string }> = [
    { view: "home", label: "Главная" },
    { view: "picker", label: "Подбор" },
    { view: "chat", label: state.advisorName },
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

  const viewBadge = document.createElement("span");
  viewBadge.className = "chip";
  viewBadge.textContent = `Раздел: ${VIEW_TITLES[state.view]}`;
  tabs.appendChild(viewBadge);

  nav.append(left, right, tabs);
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
  return createChipGroup("1. Класс", options, state.criteria.grade ? String(state.criteria.grade) : null, (value) => {
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

  container.appendChild(createGradeGroup());
  container.appendChild(
    createChipGroup("2. Цель", GOAL_OPTIONS, state.criteria.goal, (value) => {
      state.criteria.goal = value;
    })
  );
  container.appendChild(
    createChipGroup("3. Предмет", SUBJECT_OPTIONS, state.criteria.subject, (value) => {
      state.criteria.subject = value;
    })
  );
  container.appendChild(
    createChipGroup("4. Формат", FORMAT_OPTIONS, state.criteria.format, (value) => {
      state.criteria.format = value;
    })
  );

  const controls = document.createElement("div");
  controls.className = "pickerControls";

  const askBtn = document.createElement("button");
  askBtn.type = "button";
  askBtn.className = "glassButton";
  askBtn.textContent = "Задать вопрос";
  askBtn.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("chat");
  });

  const submit = document.createElement("button");
  submit.type = "button";
  submit.className = "glassButton glassButtonPrimary";
  submit.textContent = state.loading ? "Подбираю…" : "Показать варианты";
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
  title.textContent = "Что нашли по вашему запросу";

  const text = document.createElement("p");
  text.className = "actionSubtitle";

  if (state.matchQuality === "strong" && state.results.length > 0) {
    text.textContent = `Есть очень подходящий вариант: ${state.results[0].title}. При желании менеджер дополнительно сверит график и нагрузку.`;
  } else if (state.results.length > 0) {
    text.textContent =
      state.managerMessage ||
      "Есть хорошие предложения под ваш запрос. Чтобы выбрать самый точный вариант, лучше подключить менеджера.";
  } else {
    text.textContent =
      "Автоматический фильтр не нашёл идеальный вариант, но это не тупик: у нас широкая линейка под разные цели, уровни и форматы.";
  }

  const cta = document.createElement("p");
  cta.className = "resultSupportText";
  cta.textContent =
    state.managerCallToAction ||
    "Оставьте контакт, и менеджер предложит подходящие варианты под вашу задачу и сроки.";

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
      <p class="actionSubtitle">Оставьте контакт или задайте вопрос в чате: подберём персонально без шаблонных ответов.</p>
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
      link.textContent = "Открыть программу";

      card.append(title, why, meta, uspList, link);
      section.appendChild(card);
    }
  }

  const actions = document.createElement("div");
  actions.className = "resultsActions";

  const askButton = document.createElement("button");
  askButton.type = "button";
  askButton.className = "glassButton";
  askButton.textContent = "Уточнить вопросом";
  askButton.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("chat");
  });

  const contactButton = document.createElement("button");
  contactButton.type = "button";
  contactButton.className = "glassButton glassButtonPrimary";
  contactButton.textContent = `Написать ${state.managerLabel.toLowerCase()}`;
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
    <h3 class="sectionTitle sectionTitleCompact">Можно просто пообщаться с ${state.advisorName} и получить пользу</h3>
    <p class="actionSubtitle">Задавайте вопросы про стратегию поступления, подготовку по предметам, выбор программы и формат обучения.</p>
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
      <p class="chatText">Можете начать с любого вопроса. Например: «Ученик 10 класса, как распределить подготовку к ЕГЭ и олимпиадам?»</p>
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
    managerButton.textContent = `Написать ${state.managerLabel.toLowerCase()}`;
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
  textarea.placeholder = "Напишите вопрос. Например: «Как подготовиться к поступлению в МФТИ без перегруза?»";
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
  send.textContent = state.chatLoading ? "Обрабатываю…" : `Спросить ${state.advisorName}`;
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
    label.textContent = `${state.advisorName} онлайн. Можно спрашивать про стратегию, курсы и поступление.`;
  } else if (state.view === "results") {
    label.textContent = "Видите варианты. Если нужен точный подбор под детали, подключим менеджера.";
  } else {
    label.textContent = "Сначала польза и понятная рекомендация, затем только релевантные предложения.";
  }

  const actions = document.createElement("div");
  actions.className = "dockActions";

  const ask = document.createElement("button");
  ask.className = "glassButton";
  ask.type = "button";
  ask.textContent = `Спросить ${state.advisorName}`;
  ask.addEventListener("click", () => {
    triggerHaptic(webApp, "light");
    navigateTo("chat");
  });

  const primary = document.createElement("button");
  primary.className = "glassButton glassButtonPrimary";
  primary.type = "button";

  if (state.view === "results") {
    primary.textContent = `Написать ${state.managerLabel.toLowerCase()}`;
    primary.addEventListener("click", () => {
      triggerHaptic(webApp, "medium");
      openManagerChat();
    });
  } else if (state.view === "chat") {
    primary.textContent = state.chatLoading ? "Обрабатываю…" : `Спросить ${state.advisorName}`;
    primary.disabled = state.chatLoading || state.chatInput.trim().length === 0;
    primary.addEventListener("click", () => {
      triggerHaptic(webApp, "medium");
      void askAssistantQuestion();
    });
  } else {
    primary.textContent = "Показать подбор";
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
    button.setText(state.loading ? "Подбираю…" : "Показать результаты");
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
    button.setText(`Написать ${state.managerLabel.toLowerCase()}`);
    button.enable();
    mainButtonHandler = () => openManagerChat();
    button.onClick(mainButtonHandler);
    button.show();
    return;
  }

  if (state.view === "chat") {
    button.setText(state.chatLoading ? "Обрабатываю…" : `Спросить ${state.advisorName}`);
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
      throw new Error(`assistant ask failed: ${response.status}`);
    }

    const payload = (await response.json()) as AssistantResponse;
    if (!payload.ok) {
      throw new Error("assistant returned not ok");
    }

    state.chatMessages.push({
      role: "assistant",
      text: payload.answer_text,
      sources: Array.isArray(payload.sources) ? payload.sources : [],
      meta: payload.processing_note || undefined
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
  } catch (_error) {
    state.error = "Не удалось получить ответ. Попробуйте еще раз через несколько секунд.";
    state.chatMessages.push({
      role: "assistant",
      text: "Я на связи, просто сейчас не удалось завершить обработку запроса. Попробуйте повторить вопрос."
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
