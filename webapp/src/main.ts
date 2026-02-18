import "./styles/tokens.css";
import "./styles/components.css";
import "./styles/app.css";

type QuickAction = {
  title: string;
  subtitle: string;
  emoji: string;
  callbackData: string;
};

const ACTIONS: QuickAction[] = [
  {
    title: "Подобрать курс",
    subtitle: "3 варианта под цель и класс за 60 секунд",
    emoji: "🎯",
    callbackData: "action:pick"
  },
  {
    title: "Задать вопрос",
    subtitle: "Ответ на условия, документы и формат обучения",
    emoji: "💬",
    callbackData: "action:ask"
  },
  {
    title: "Записаться на консультацию",
    subtitle: "Свяжем с методистом и соберем персональный план",
    emoji: "📞",
    callbackData: "action:consult"
  }
];

function createActionCard(action: QuickAction): HTMLElement {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "glassCard actionCard";
  button.dataset.action = action.callbackData;

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

function mountApp(root: HTMLElement): void {
  const container = document.createElement("main");
  container.className = "appShell";

  const hero = document.createElement("header");
  hero.className = "hero glassCard";
  hero.innerHTML = `
    <p class="eyebrow">KMIPT • Sales Agent</p>
    <h1 class="heroTitle">Умный подбор программ в 1 касание</h1>
    <p class="heroSubtitle">Онлайн • Подбор за 60 сек</p>
  `;

  const section = document.createElement("section");
  section.className = "actions";

  ACTIONS.forEach((action, index) => {
    const card = createActionCard(action);
    card.style.setProperty("--index", String(index));
    section.appendChild(card);
  });

  const bottom = document.createElement("footer");
  bottom.className = "bottomDock glassCard";
  bottom.innerHTML = `
    <span class="dockLabel">Без спама • Мягкая консультация</span>
    <button class="glassButton" type="button">Продолжить</button>
  `;

  container.append(hero, section, bottom);
  root.replaceChildren(container);
}

const root = document.getElementById("app");
if (!root) {
  throw new Error("App root not found");
}

mountApp(root);
