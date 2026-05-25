/**
 * PWA install controller — in-page experience.
 *
 * Goals:
 *  - Make the install request unmistakable IN THE PAGE (not the omnibox).
 *  - Surface a persistent FAB users can tap any time + an auto card on first visit.
 *  - Gracefully handle: Chromium (beforeinstallprompt), iOS Safari (manual steps),
 *    other browsers / unmet criteria (helpful fallback instructions).
 *  - Session-scoped dismissal (the FAB stays even after the user closes the card).
 *  - Hide everything once installed.
 *
 * Public API:
 *  - window.PWAInstall.show()          → open the install card
 *  - window.PWAInstall.hide()          → close the install card
 *  - window.PWAInstall.canInstall()    → boolean
 *  - [data-pwa-install] elements       → auto-bound to open the card
 */
(function () {
  "use strict";

  const SESSION_KEY = "pwa-install-dismissed-session";
  const LEGACY_KEY = "pwa-install-dismiss-until";
  const AUTO_SHOW_DELAY = 900;
  const FAB_REVEAL_DELAY = 1800;

  try {
    localStorage.removeItem(LEGACY_KEY);
  } catch (e) {
    /* ignore */
  }

  function isStandalone() {
    try {
      if (window.matchMedia && window.matchMedia("(display-mode: standalone)").matches) return true;
      if (window.matchMedia && window.matchMedia("(display-mode: fullscreen)").matches) return true;
      if (window.matchMedia && window.matchMedia("(display-mode: minimal-ui)").matches) return true;
    } catch (e) {
      /* ignore */
    }
    if (window.navigator.standalone === true) return true;
    if (document.referrer && document.referrer.startsWith("android-app://")) return true;
    return false;
  }

  function isIos() {
    const ua = window.navigator.userAgent || "";
    const isAppleDevice = /iPad|iPhone|iPod/.test(ua) && !window.MSStream;
    const isIpadOs =
      ua.includes("Macintosh") && navigator.maxTouchPoints && navigator.maxTouchPoints > 1;
    return isAppleDevice || isIpadOs;
  }

  function isAndroid() {
    return /Android/i.test(window.navigator.userAgent || "");
  }

  function isFirefox() {
    return /Firefox\//.test(window.navigator.userAgent || "");
  }

  function isInPosPage() {
    return /\/shops\/\d+\/shop-pos\b/.test(window.location.pathname);
  }

  function dismissedThisSession() {
    try {
      return sessionStorage.getItem(SESSION_KEY) === "1";
    } catch (e) {
      return false;
    }
  }

  function rememberSessionDismiss() {
    try {
      sessionStorage.setItem(SESSION_KEY, "1");
    } catch (e) {
      /* ignore */
    }
  }

  function appName() {
    const meta =
      document.querySelector('meta[name="application-name"]') ||
      document.querySelector('meta[name="apple-mobile-web-app-title"]');
    if (meta && meta.content) return meta.content.trim();
    return "this app";
  }

  function appIconUrl() {
    const link = document.querySelector('link[rel="apple-touch-icon"][sizes="192x192"]')
      || document.querySelector('link[rel="apple-touch-icon"]')
      || document.querySelector('link[rel="icon"][type="image/png"]')
      || document.querySelector('link[rel="icon"]');
    return link && link.href ? link.href : "/static/icons/app-icon-192.png";
  }

  function detectBrowserLabel() {
    if (isIos()) return "ios";
    if (isFirefox()) return "firefox";
    return "chromium";
  }

  /* ---------------------------------------------------------------------- */

  let deferredPrompt = null;
  let card = null;
  let fab = null;
  let cardOpen = false;

  /* ---------- Card markup ----------------------------------------------- */

  function manualHelpHtml(browser) {
    if (browser === "ios") {
      return `
        <ol class="pwa-install__steps">
          <li>
            Tap the
            <span class="pwa-install__inline-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 3v12" />
                <path d="m7 8 5-5 5 5" />
                <path d="M5 12v7a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-7" />
              </svg>
            </span>
            Share button in Safari
          </li>
          <li>Choose <strong>Add to Home Screen</strong></li>
          <li>Tap <strong>Add</strong> in the top-right</li>
        </ol>
      `;
    }
    if (browser === "firefox") {
      return `
        <ol class="pwa-install__steps">
          <li>Open the browser <strong>menu</strong> (three lines)</li>
          <li>Tap <strong>Install</strong> or <strong>Add to Home Screen</strong></li>
          <li>Confirm to add the app</li>
        </ol>
      `;
    }
    return `
      <ol class="pwa-install__steps">
        <li>Open the browser <strong>menu</strong> (︙ or ⋯)</li>
        <li>Choose <strong>Install app</strong> or <strong>Add to Home Screen</strong></li>
        <li>Confirm to install</li>
      </ol>
    `;
  }

  function buildCardHtml(mode) {
    const name = appName();
    const icon = appIconUrl();
    const desc = "Quick launch, full-screen workspace, faster reloads and offline-aware.";

    if (mode === "manual") {
      const browser = detectBrowserLabel();
      return `
        <div class="pwa-install__icon" aria-hidden="true">
          <img src="${icon}" alt="" loading="lazy" decoding="async" />
        </div>
        <div class="pwa-install__body">
          <p class="pwa-install__eyebrow">Install ${name}</p>
          <h3 class="pwa-install__title">${browser === "ios" ? "Add to Home Screen" : "Install the app"}</h3>
          <p class="pwa-install__desc">${browser === "ios"
              ? "iOS doesn't auto-prompt — just a couple of taps:"
              : "Your browser will let you install it from the menu:"}</p>
          ${manualHelpHtml(browser)}
          <div class="pwa-install__actions pwa-install__actions--single">
            <button type="button" class="pwa-install__btn pwa-install__btn--ghost" data-pwa-dismiss>Got it</button>
          </div>
        </div>
        <button type="button" class="pwa-install__close" data-pwa-dismiss aria-label="Dismiss install prompt">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M18 6 6 18M6 6l12 12" />
          </svg>
        </button>
      `;
    }

    return `
      <div class="pwa-install__icon" aria-hidden="true">
        <img src="${icon}" alt="" loading="lazy" decoding="async" />
      </div>
      <div class="pwa-install__body">
        <p class="pwa-install__eyebrow">Install app</p>
        <h3 class="pwa-install__title">Install ${name}</h3>
        <p class="pwa-install__desc">${desc}</p>
        <div class="pwa-install__features" aria-hidden="true">
          <span class="pwa-install__feature">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m5 12 5 5L20 7"/></svg>
            One-tap launch
          </span>
          <span class="pwa-install__feature">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l3-8 4 16 3-8h4"/></svg>
            Faster &amp; smoother
          </span>
          <span class="pwa-install__feature">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 4 5v6c0 5 3.5 9.5 8 11 4.5-1.5 8-6 8-11V5l-8-3z"/></svg>
            Works offline
          </span>
        </div>
        <div class="pwa-install__actions">
          <button type="button" class="pwa-install__btn pwa-install__btn--primary" data-pwa-confirm>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M12 3v12"/><path d="m7 12 5 5 5-5"/><path d="M5 21h14"/>
            </svg>
            Install now
          </button>
          <button type="button" class="pwa-install__btn pwa-install__btn--ghost" data-pwa-later>
            Not now
          </button>
        </div>
      </div>
      <button type="button" class="pwa-install__close" data-pwa-dismiss aria-label="Dismiss install prompt">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M18 6 6 18M6 6l12 12" />
        </svg>
      </button>
    `;
  }

  function ensureCard(mode) {
    if (card && card.dataset.mode === mode) return card;
    if (card) {
      card.remove();
      card = null;
    }
    card = document.createElement("aside");
    card.className = "pwa-install" + (mode === "manual" ? " pwa-install--manual" : "");
    card.setAttribute("role", "dialog");
    card.setAttribute("aria-live", "polite");
    card.setAttribute("aria-label", "Install application");
    card.dataset.mode = mode;
    card.hidden = true;
    card.innerHTML = buildCardHtml(mode);
    document.body.appendChild(card);

    card.addEventListener("click", (e) => {
      const t = e.target.closest("[data-pwa-confirm], [data-pwa-later], [data-pwa-dismiss]");
      if (!t) return;
      if (t.matches("[data-pwa-confirm]")) {
        triggerNativePrompt();
      } else if (t.matches("[data-pwa-later]") || t.matches("[data-pwa-dismiss]")) {
        hideCard(true);
      }
    });
    return card;
  }

  function showCard(mode) {
    if (isStandalone()) return;
    const el = ensureCard(mode);
    el.hidden = false;
    requestAnimationFrame(() => el.classList.add("pwa-install--show"));
    document.documentElement.classList.add("pwa-install-open");
    cardOpen = true;
    hideFab();
  }

  function hideCard(remember) {
    cardOpen = false;
    if (card) {
      card.classList.remove("pwa-install--show");
      const node = card;
      setTimeout(() => {
        if (node && node.parentNode) node.hidden = true;
      }, 250);
    }
    document.documentElement.classList.remove("pwa-install-open");
    if (remember) rememberSessionDismiss();
    if (!isStandalone()) revealFab();
  }

  async function triggerNativePrompt() {
    if (deferredPrompt) {
      try {
        deferredPrompt.prompt();
        const choice = await deferredPrompt.userChoice;
        if (choice && choice.outcome === "accepted") {
          hideCard(false);
          destroyFab();
        } else {
          hideCard(true);
        }
      } catch (e) {
        showCard("manual");
      } finally {
        deferredPrompt = null;
        refreshInstallButtons();
      }
      return;
    }
    showCard("manual");
  }

  /* ---------- Floating button -------------------------------------------- */

  function buildFab() {
    if (fab) return fab;
    fab = document.createElement("button");
    fab.type = "button";
    fab.className = "pwa-install-fab";
    fab.setAttribute("aria-label", "Install app");
    fab.innerHTML = `
      <span class="pwa-install-fab__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 3v12"/><path d="m7 12 5 5 5-5"/><path d="M5 21h14"/>
        </svg>
      </span>
      <span class="pwa-install-fab__label">Install app</span>
      <span class="pwa-install-fab__pulse" aria-hidden="true"></span>
    `;
    fab.addEventListener("click", () => api.show());
    document.body.appendChild(fab);
    return fab;
  }

  function revealFab() {
    if (isStandalone()) return;
    if (isInPosPage()) return;
    const el = buildFab();
    el.hidden = false;
    requestAnimationFrame(() => el.classList.add("pwa-install-fab--show"));
  }

  function hideFab() {
    if (!fab) return;
    fab.classList.remove("pwa-install-fab--show");
  }

  function destroyFab() {
    if (!fab) return;
    fab.classList.remove("pwa-install-fab--show");
    const node = fab;
    setTimeout(() => {
      if (node && node.parentNode) node.parentNode.removeChild(node);
      if (fab === node) fab = null;
    }, 300);
  }

  /* ---------- Inline buttons (data-pwa-install) -------------------------- */

  function refreshInstallButtons() {
    const buttons = Array.from(document.querySelectorAll("[data-pwa-install]"));
    buttons.forEach((btn) => {
      btn.hidden = isStandalone();
      if (!btn.dataset.pwaBound) {
        btn.dataset.pwaBound = "1";
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          api.show();
        });
      }
    });
  }

  /* ---------- Public API ------------------------------------------------- */

  const api = {
    show() {
      if (isStandalone()) return;
      if (deferredPrompt) {
        showCard("prompt");
      } else {
        showCard("manual");
      }
    },
    hide() {
      hideCard(false);
    },
    canInstall() {
      return !!deferredPrompt;
    },
    isStandalone,
    isIos,
  };

  window.PWAInstall = api;

  /* ---------- Events ----------------------------------------------------- */

  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    deferredPrompt = e;
    refreshInstallButtons();
    if (isStandalone() || isInPosPage()) return;
    if (!cardOpen && !dismissedThisSession()) {
      setTimeout(() => {
        if (deferredPrompt && !isStandalone() && !cardOpen && !dismissedThisSession()) {
          showCard("prompt");
        }
      }, AUTO_SHOW_DELAY);
    } else {
      revealFab();
    }
  });

  window.addEventListener("appinstalled", () => {
    deferredPrompt = null;
    hideCard(false);
    destroyFab();
    try {
      sessionStorage.removeItem(SESSION_KEY);
    } catch (e) {
      /* ignore */
    }
    refreshInstallButtons();
  });

  function bootstrap() {
    refreshInstallButtons();
    if (isStandalone()) return;
    if (isInPosPage()) return;

    if (dismissedThisSession()) {
      setTimeout(revealFab, FAB_REVEAL_DELAY);
      return;
    }

    if (isIos()) {
      setTimeout(() => {
        if (!isStandalone() && !dismissedThisSession() && !cardOpen) showCard("manual");
      }, AUTO_SHOW_DELAY);
      return;
    }

    setTimeout(() => {
      if (isStandalone() || dismissedThisSession() || cardOpen) return;
      if (deferredPrompt) {
        showCard("prompt");
      } else {
        revealFab();
      }
    }, AUTO_SHOW_DELAY + 800);

    setTimeout(revealFab, FAB_REVEAL_DELAY);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
  } else {
    bootstrap();
  }
})();
