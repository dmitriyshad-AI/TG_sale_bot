export type TelegramWebAppUser = {
  id: number;
  first_name?: string;
  last_name?: string;
  username?: string;
  language_code?: string;
};

type MainButton = {
  setText(text: string): void;
  show(): void;
  hide(): void;
  onClick(callback: () => void): void;
  offClick(callback: () => void): void;
};

type HapticFeedback = {
  impactOccurred(style: "light" | "medium" | "heavy" | "rigid" | "soft"): void;
};

export type TelegramWebApp = {
  initData?: string;
  initDataUnsafe?: {
    user?: TelegramWebAppUser;
  };
  MainButton?: MainButton;
  HapticFeedback?: HapticFeedback;
  ready(): void;
  expand(): void;
};

type TelegramGlobal = {
  WebApp?: TelegramWebApp;
};

declare global {
  interface Window {
    Telegram?: TelegramGlobal;
  }
}

export type TelegramContext = {
  webApp: TelegramWebApp | null;
  initData: string;
  user: TelegramWebAppUser | null;
};

export function initTelegramContext(): TelegramContext {
  const webApp = window.Telegram?.WebApp ?? null;
  if (!webApp) {
    return { webApp: null, initData: "", user: null };
  }

  try {
    webApp.ready();
    webApp.expand();
  } catch (_error) {
    return { webApp, initData: "", user: null };
  }

  const initData = (webApp.initData ?? "").trim();
  const user = webApp.initDataUnsafe?.user ?? null;
  return { webApp, initData, user };
}

export function buildAuthHeaders(initData: string): HeadersInit {
  const value = initData.trim();
  if (!value) {
    return {};
  }
  return { "X-Tg-Init-Data": value };
}
