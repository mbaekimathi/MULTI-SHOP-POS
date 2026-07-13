/**
 * Marketing site — scroll reveals, theme toggle, tabs, FAQ, counters.
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

  function initHeaderScroll() {
    var header = document.querySelector("[data-mk-header]");
    if (!header) return;
    var onScroll = function () {
      header.classList.toggle("is-scrolled", window.scrollY > 12);
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  function initTabs(root) {
    if (!root) return;
    var tabs = root.querySelectorAll("[data-mk-tab]");
    var panels = root.querySelectorAll("[data-mk-panel]");
    if (!tabs.length) return;

    tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        var id = tab.getAttribute("data-mk-tab");
        tabs.forEach(function (t) {
          t.classList.toggle("is-active", t === tab);
          t.setAttribute("aria-selected", t === tab ? "true" : "false");
        });
        panels.forEach(function (panel) {
          var match = panel.getAttribute("data-mk-panel") === id;
          panel.classList.toggle("is-active", match);
          panel.hidden = !match;
        });
      });
    });
  }

  function initBillingToggle() {
    var wrap = document.querySelector("[data-mk-billing]");
    if (!wrap) return;
    var buttons = wrap.querySelectorAll("button[data-mk-billing-mode]");
    var prices = document.querySelectorAll("[data-mk-price-monthly]");
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var yearly = btn.getAttribute("data-mk-billing-mode") === "yearly";
        buttons.forEach(function (b) {
          b.classList.toggle("is-active", b === btn);
        });
        prices.forEach(function (el) {
          var monthly = el.getAttribute("data-mk-price-monthly");
          var yearlyPrice = el.getAttribute("data-mk-price-yearly");
          if (monthly && yearlyPrice) {
            el.textContent = yearly ? yearlyPrice : monthly;
          }
        });
      });
    });
  }

  function initFaq() {
    document.querySelectorAll("[data-mk-faq-item]").forEach(function (item) {
      var btn = item.querySelector("[data-mk-faq-q]");
      if (!btn) return;
      btn.addEventListener("click", function () {
        var open = item.classList.contains("is-open");
        document.querySelectorAll("[data-mk-faq-item].is-open").forEach(function (other) {
          if (other !== item) {
            other.classList.remove("is-open");
            var ob = other.querySelector("[data-mk-faq-q]");
            if (ob) ob.setAttribute("aria-expanded", "false");
          }
        });
        item.classList.toggle("is-open", !open);
        btn.setAttribute("aria-expanded", !open ? "true" : "false");
      });
    });
  }

  function initCounters() {
    var nodes = document.querySelectorAll("[data-mk-count]");
    if (!nodes.length || !("IntersectionObserver" in window)) return;
    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var el = entry.target;
          var target = parseInt(el.getAttribute("data-mk-count") || "0", 10);
          var suffix = el.getAttribute("data-mk-count-suffix") || "";
          if (!target) return;
          var start = 0;
          var duration = 900;
          var t0 = null;
          function step(ts) {
            if (!t0) t0 = ts;
            var p = Math.min((ts - t0) / duration, 1);
            var eased = 1 - Math.pow(1 - p, 3);
            el.textContent = Math.round(start + (target - start) * eased) + suffix;
            if (p < 1) requestAnimationFrame(step);
          }
          requestAnimationFrame(step);
          io.unobserve(el);
        });
      },
      { threshold: 0.4 }
    );
    nodes.forEach(function (el) {
      io.observe(el);
    });
  }

  function initDashPreview() {
    var root = document.querySelector("[data-mk-dash]");
    if (!root) return;
    var tabs = root.querySelectorAll("[data-mk-tab]");
    var panels = root.querySelectorAll("[data-mk-panel]");
    tabs.forEach(function (tab) {
      tab.addEventListener("click", function () {
        var id = tab.getAttribute("data-mk-tab");
        tabs.forEach(function (t) {
          t.classList.toggle("is-active", t === tab);
          t.setAttribute("aria-selected", t === tab ? "true" : "false");
        });
        panels.forEach(function (panel) {
          var match = panel.getAttribute("data-mk-panel") === id;
          panel.classList.toggle("is-active", match);
          panel.hidden = !match;
        });
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initReveals();
    initMarketingThemeToggle();
    initHeaderScroll();
    initBillingToggle();
    initFaq();
    initCounters();
    initDashPreview();
    document.querySelectorAll("[data-mk-tabs]").forEach(initTabs);
  });
})();
