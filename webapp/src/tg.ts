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
  enable(): void;
  disable(): void;
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
  sendData?(data: string): void;
  openTelegramLink?(url: string): void;
  openLink?(url: string): void;
  close?(): void;
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

export function triggerHaptic(webApp: TelegramWebApp | null, style: "light" | "medium" | "heavy" = "light"): void {
  try {
    webApp?.HapticFeedback?.impactOccurred(style);
  } catch (_error) {
    // Telegram API can be unavailable in browser preview.
  }
}

export function openExternalLink(webApp: TelegramWebApp | null, url: string): boolean {
  const target = url.trim();
  if (!target) {
    return false;
  }
  try {
    if (target.startsWith("https://t.me/") || target.startsWith("tg://")) {
      if (typeof webApp?.openTelegramLink === "function") {
        webApp.openTelegramLink(target);
        return true;
      }
    } else if (typeof webApp?.openLink === "function") {
      webApp.openLink(target);
      return true;
    }
  } catch (_error) {
    // Fallback to window.open below.
  }
  try {
    window.open(target, "_blank", "noopener,noreferrer");
    return true;
  } catch (_windowError) {
    return false;
  }
}
