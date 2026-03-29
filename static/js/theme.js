/**
 * Global light/dark theme for the whole site.
 * - html[data-theme="light"|"dark"] + class "dark" on <html> (Tailwind darkMode: class)
 * - Persists to localStorage (richcom-theme)
 * - Toggle: any element with data-theme-toggle
 */
(function () {
  function storageKey() {
    return document.documentElement.getAttribute("data-theme-key") || "richcom-theme";
  }

  function defaultTheme() {
    return document.documentElement.getAttribute("data-theme-default") || "dark";
  }

  function normalize(theme) {
    return theme === "light" || theme === "dark" ? theme : "dark";
  }

  function apply(theme) {
    theme = normalize(theme);
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
    apply(normalize(readStored() || defaultTheme()));
  }

  function toggle() {
    apply(document.documentElement.classList.contains("dark") ? "light" : "dark");
  }

  function onDocumentClick(e) {
    if (!e.target.closest("[data-theme-toggle]")) return;
    e.preventDefault();
    toggle();
  }

  init();
  document.addEventListener("click", onDocumentClick);

  window.RichcomTheme = {
    apply: apply,
    toggle: toggle,
    init: init,
    get: function () {
      return document.documentElement.dataset.theme || "dark";
    },
  };
})();
