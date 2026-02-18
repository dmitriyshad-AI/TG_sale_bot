import "./styles/tokens.css";
import "./styles/components.css";
import "./styles/app.css";
import { buildAuthHeaders, initTelegramContext, type TelegramWebAppUser } from "./tg";

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
};

type AuthResponse =
  | { ok: true; user: TelegramWebAppUser }
  | { ok: false; reason: string; user: null };

type AppView = "home" | "picker" | "results";

type AppState = {
  view: AppView;
  criteria: SearchCriteria;
  results: CatalogItem[];
  loading: boolean;
  error: string | null;
  statusLine: string;
  initData: string;
  user: TelegramWebAppUser | null;
};

const HOME_ACTIONS: HomeAction[] = [
  {
    key: "pick",
    title: "Подобрать курс",
    subtitle: "3 варианта под цель и класс за 60 секунд",
    emoji: "🎯"
  },
  {
    key: "ask",
    title: "Задать вопрос",
    subtitle: "Ответ на условия, документы и формат обучения",
    emoji: "💬"
  },
  {
    key: "consult",
    title: "Записаться на консультацию",
    subtitle: "Свяжем с методистом и соберем персональный план",
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

const rootNode = document.getElementById("app");
if (!rootNode) {
  throw new Error("App root not found");
}
const appRoot: HTMLElement = rootNode;

const telegram = initTelegramContext();
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
  loading: false,
  error: null,
  statusLine: "Проверяю подключение к Telegram…",
  initData: telegram.initData,
  user: telegram.user
};

function createActionCard(action: HomeAction): HTMLButtonElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "glassCard actionCard";
  button.dataset.action = action.key;

  const title = document.createElement("h3");
  title.className = "sectionTitle";
  title.textContent = `${action.emoji} ${action.title}`;

  const subtitle = document.createElement("p");
  subtitle.className = "actionSubtitle";
  subtitle.textContent = action.subtitle;

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
    <p class="eyebrow">KMIPT • Sales Agent</p>
    <h1 class="heroTitle">Подбор программ без давления${name}</h1>
    <p class="heroSubtitle">${statusText}</p>
  `;
  return hero;
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
      state.error = null;
      onSelect(option.value);
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
      state.error = null;
      state.view = "picker";
      render();
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

  const back = document.createElement("button");
  back.type = "button";
  back.className = "glassButton";
  back.textContent = "Назад";
  back.addEventListener("click", () => {
    state.view = "home";
    state.error = null;
    render();
  });

  const submit = document.createElement("button");
  submit.type = "button";
  submit.className = "glassButton glassButtonPrimary";
  submit.textContent = state.loading ? "Подбираю…" : "Показать варианты";
  submit.disabled = !isCriteriaComplete() || state.loading;
  submit.addEventListener("click", () => {
    void loadCatalogResults();
  });

  controls.append(back, submit);
  container.appendChild(controls);

  return container;
}

function createResultsView(): HTMLElement {
  const section = document.createElement("section");
  section.className = "resultsGrid";

  if (state.results.length === 0) {
    const empty = document.createElement("article");
    empty.className = "glassCard resultCard";
    empty.innerHTML = `
      <h3 class="sectionTitle sectionTitleCompact">Пока не нашёл точных совпадений</h3>
      <p class="actionSubtitle">Измените 1-2 параметра, и я покажу ближайшие варианты.</p>
    `;
    section.appendChild(empty);
    return section;
  }

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
  return section;
}

function createBottomDock(): HTMLElement {
  const bottom = document.createElement("footer");
  bottom.className = "bottomDock glassCard";

  const label = document.createElement("span");
  label.className = "dockLabel";
  label.textContent =
    state.view === "results"
      ? "Если удобно, напишите в чат: «Хочу консультацию»."
      : "Без спама • Сначала польза, потом рекомендации.";

  const action = document.createElement("button");
  action.className = "glassButton";
  action.type = "button";
  action.textContent = state.view === "results" ? "Уточнить подбор" : "Продолжить";
  action.addEventListener("click", () => {
    state.error = null;
    state.view = state.view === "results" ? "picker" : "picker";
    render();
  });

  bottom.append(label, action);
  return bottom;
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

async function loadCatalogResults(): Promise<void> {
  state.loading = true;
  state.error = null;
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
    state.view = "results";
  } catch (_error) {
    state.error = "Не удалось получить подбор. Попробуйте еще раз через несколько секунд.";
  } finally {
    state.loading = false;
    render();
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
    state.statusLine = "Онлайн • Подбор за 60 сек";
  } catch (_error) {
    state.statusLine = "Подключение к Telegram недоступно • Можно работать в демо-режиме";
  }
}

function render(): void {
  const container = document.createElement("main");
  container.className = "appShell";
  container.appendChild(renderHeader(state.statusLine));

  const error = renderError();
  if (error) {
    container.appendChild(error);
  }

  if (state.view === "home") {
    container.appendChild(createHomeView());
  } else if (state.view === "picker") {
    container.appendChild(createPickerView());
  } else {
    container.appendChild(createResultsView());
  }

  container.appendChild(createBottomDock());
  appRoot.replaceChildren(container);
}

render();
void loadWhoAmI().then(() => render());
