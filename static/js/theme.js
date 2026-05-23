/**
 * Global light/dark theme for the whole site.
 * - html[data-theme="light"|"dark"] + class "dark" on <html> (Tailwind darkMode: class)
 * - Palette tokens come from CSS (data-theme-preset + data-theme) — never left inline
 * - Persists to localStorage (richcom-theme)
 * - data-theme-default may be dark | light | system
 */
(function () {
  var PALETTE_VARS = [
    "--rc-page-bg",
    "--rc-page-fg",
    "--rc-muted",
    "--rc-border",
    "--rc-surface",
    "--rc-surface-2",
  ];

  function storageKey() {
    return document.documentElement.getAttribute("data-theme-key") || "richcom-theme";
  }

  function defaultTheme() {
    return document.documentElement.getAttribute("data-theme-default") || "dark";
  }

  function resolveDefault(def) {
    if (def === "system") {
      try {
        return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
      } catch (_) {
        return "dark";
      }
    }
    return def === "light" || def === "dark" ? def : "dark";
  }

  function normalize(theme) {
    return theme === "light" || theme === "dark" ? theme : "dark";
  }

  /** Inline palette vars block CSS mode switching — clear on every theme apply. */
  function clearPaletteInlineVars() {
    var style = document.documentElement.style;
    PALETTE_VARS.forEach(function (name) {
      style.removeProperty(name);
    });
  }

  function apply(theme) {
    theme = normalize(theme);
    clearPaletteInlineVars();
    var root = document.documentElement;
    root.dataset.theme = theme;
    root.classList.toggle("dark", theme === "dark");
    root.style.colorScheme = theme === "dark" ? "dark" : "light";
    try {
      localStorage.setItem(storageKey(), theme);
    } catch (_) {}
  }

  function readStored() {
    try {
      return localStorage.getItem(storageKey());
    } catch (_) {
      return null;
    }
  }

  function init() {
    apply(normalize(readStored() || resolveDefault(defaultTheme())));
  }

  function toggle() {
    apply(document.documentElement.classList.contains("dark") ? "light" : "dark");
  }

  function onDocumentClick(e) {
    if (!e.target.closest("[data-theme-toggle]")) return;
    e.preventDefault();
    toggle();
  }

  function watchSystemDefault() {
    if (defaultTheme() !== "system") return;
    try {
      var mq = window.matchMedia("(prefers-color-scheme: dark)");
      mq.addEventListener("change", function () {
        if (!readStored()) apply(resolveDefault("system"));
      });
    } catch (_) {}
  }

  init();
  watchSystemDefault();
  document.addEventListener("click", onDocumentClick);

  window.RichcomTheme = {
    apply: apply,
    toggle: toggle,
    init: init,
    clearPaletteInlineVars: clearPaletteInlineVars,
    resolveDefault: resolveDefault,
    get: function () {
      return document.documentElement.dataset.theme || "dark";
    },
  };
})();
