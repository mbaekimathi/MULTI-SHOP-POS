/**
 * Marketing site: optional scroll reveals, theme toggle. Kept minimal on purpose.
 */
(function () {
  "use strict";

  function initReveals() {
    var nodes = document.querySelectorAll(".mk-reveal");
    if (!nodes.length || !("IntersectionObserver" in window)) {
      nodes.forEach(function (el) {
        el.classList.add("mk-reveal--visible");
      });
      return;
    }
    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("mk-reveal--visible");
            io.unobserve(entry.target);
          }
        });
      },
      { root: null, rootMargin: "0px 0px -8% 0px", threshold: 0.08 }
    );
    nodes.forEach(function (el) {
      io.observe(el);
    });
  }

  function applyMarketingTheme(theme) {
    var root = document.documentElement;
    root.setAttribute("data-marketing-theme", theme);
    root.style.colorScheme = theme === "dark" ? "dark" : "light";
  }

  function hasStoredMarketingTheme(cfgKey, cfg) {
    try {
      var storedCfg = localStorage.getItem(cfgKey);
      var stored = localStorage.getItem("marketing-theme");
      return !!(storedCfg && cfg && storedCfg === cfg && (stored === "dark" || stored === "light"));
    } catch (e) {
      return false;
    }
  }

  function initMarketingThemeToggle() {
    var key = "marketing-theme";
    var cfgKey = "marketing-theme-config";
    var root = document.documentElement;
    var cfg = root.getAttribute("data-marketing-theme-config") || "";
    var storedCfg = null;
    var stored = null;
    try {
      storedCfg = localStorage.getItem(cfgKey);
      stored = localStorage.getItem(key);
    } catch (e) {}
    if (storedCfg && cfg && storedCfg === cfg && (stored === "dark" || stored === "light")) {
      applyMarketingTheme(stored);
    }

    document.querySelectorAll("[data-mk-theme-toggle]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var next = root.getAttribute("data-marketing-theme") === "dark" ? "light" : "dark";
        applyMarketingTheme(next);
        try {
          localStorage.setItem(key, next);
          if (cfg) localStorage.setItem(cfgKey, cfg);
        } catch (e) {}
      });
    });

    var themeDefault = root.getAttribute("data-marketing-theme-default") || "system";
    if (themeDefault !== "system") return;
    try {
      var mq = window.matchMedia("(prefers-color-scheme: dark)");
      mq.addEventListener("change", function () {
        if (!hasStoredMarketingTheme(cfgKey, cfg)) {
          applyMarketingTheme(mq.matches ? "dark" : "light");
        }
      });
    } catch (e) {}
  }

  document.addEventListener("DOMContentLoaded", function () {
    initReveals();
    initMarketingThemeToggle();
  });
})();
