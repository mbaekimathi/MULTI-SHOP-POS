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

  function initMarketingThemeToggle() {
    var key = "marketing-theme";
    var root = document.documentElement;
    var stored = null;
    try {
      stored = localStorage.getItem(key);
    } catch (e) {}
    if (stored === "dark" || stored === "light") {
      root.setAttribute("data-marketing-theme", stored);
    }

    document.querySelectorAll("[data-mk-theme-toggle]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var next = root.getAttribute("data-marketing-theme") === "dark" ? "light" : "dark";
        root.setAttribute("data-marketing-theme", next);
        try {
          localStorage.setItem(key, next);
        } catch (e) {}
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initReveals();
    initMarketingThemeToggle();
  });
})();
