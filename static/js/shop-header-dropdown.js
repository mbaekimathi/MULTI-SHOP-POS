/**
 * Shop account dropdown — native <details> toggle + fixed panel on POS header.
 */
(function () {
  "use strict";

  function clearPanelStyles(panel) {
    panel.classList.remove("is-floating");
    panel.style.position = "";
    panel.style.top = "";
    panel.style.right = "";
    panel.style.left = "";
    panel.style.zIndex = "";
    panel.style.minWidth = "";
    panel.style.display = "";
  }

  function mountPanel(wrap, summary, panel) {
    var rect = summary.getBoundingClientRect();
    if (panel.parentElement !== document.body) {
      document.body.appendChild(panel);
    }
    panel.classList.add("is-floating");
    panel.style.display = "block";
    panel.style.position = "fixed";
    panel.style.top = Math.round(rect.bottom + 6) + "px";
    panel.style.right = Math.round(Math.max(8, window.innerWidth - rect.right)) + "px";
    panel.style.left = "auto";
    panel.style.zIndex = "9999";
    panel.style.minWidth = "13rem";
  }

  function unmountPanel(wrap, panel) {
    clearPanelStyles(panel);
    if (panel.parentElement === document.body) {
      wrap.appendChild(panel);
    }
  }

  function init(wrap) {
    if (!wrap || wrap.getAttribute("data-shop-dd-ready") === "1") return;
    wrap.setAttribute("data-shop-dd-ready", "1");

    var summary = wrap.querySelector("#shop-header-dd-btn");
    var panel = wrap.querySelector("#shop-header-dd-panel");
    if (!summary || !panel) return;

    var useFloating = !!wrap.closest(".pos-pos-header-shell");

    function syncPanel() {
      if (!wrap.open) {
        unmountPanel(wrap, panel);
        return;
      }
      if (useFloating) mountPanel(wrap, summary, panel);
    }

    wrap.addEventListener("toggle", syncPanel);

    window.addEventListener("resize", syncPanel);
    window.addEventListener("scroll", syncPanel, true);

    document.addEventListener("click", function (e) {
      if (!wrap.open) return;
      if (summary.contains(e.target) || panel.contains(e.target)) return;
      wrap.open = false;
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && wrap.open) wrap.open = false;
    });
  }

  function boot() {
    document.querySelectorAll("[data-shop-header-dd]").forEach(init);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
