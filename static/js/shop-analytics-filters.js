/**
 * Shop portal analytics — period/scope filters (live HTML swap).
 */
(function () {
  var form = document.getElementById("it-analytics-filter-form");
  if (!form || !window.PortalLiveFilters) return;

  var scopeInput = document.getElementById("it-analytics-scope-input");
  var scopeNav = form.querySelector(".rev-tabs--scope");
  var scopeCluster = form.querySelector(".rev-toolbar__cluster--scope");
  var filterApi = window.itSupportAnalyticsFilter || {};

  function normalizeScope(raw) {
    if (typeof filterApi.normalizeScope === "function") return filterApi.normalizeScope(raw);
    return String(raw || "general").trim().toLowerCase() === "actual" ? "actual" : "general";
  }

  function scopeButtons() {
    return form.querySelectorAll(".it-scope-btn, .rev-scope-btn");
  }

  function setScopeUi(scope) {
    var next = normalizeScope(scope);
    if (scopeInput) scopeInput.value = next;
    scopeButtons().forEach(function (btn) {
      var sc = btn.getAttribute("data-it-scope") || btn.getAttribute("data-rev-scope") || "general";
      var on = normalizeScope(sc) === next;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    });
    if (scopeNav) scopeNav.setAttribute("data-active-scope", next);
    if (scopeCluster) scopeCluster.setAttribute("data-active-scope", next);
  }

  function buildParams() {
    var params = new URLSearchParams(new FormData(form));
    if (scopeInput) {
      var next = normalizeScope(scopeInput.value);
      if (next === "actual") params.set("analytics_scope", "actual");
      else params.delete("analytics_scope");
    }
    return params;
  }

  function onScopeClick(btn) {
    var scope = btn.getAttribute("data-it-scope") || btn.getAttribute("data-rev-scope") || "general";
    var next = normalizeScope(scope);
    if (scopeInput && normalizeScope(scopeInput.value) === next && btn.classList.contains("is-active")) {
      return;
    }
    setScopeUi(next);
    window.PortalLiveFilters.fetchAndSwap(form, {
      buildParams: buildParams,
      target: document.getElementById("it-analytics-live-root"),
    });
  }

  window.PortalLiveFilters.wireForm(form, {
    buildParams: buildParams,
    target: document.getElementById("it-analytics-live-root"),
  });

  if (scopeNav) {
    scopeNav.addEventListener("click", function (e) {
      var btn = e.target.closest(".it-scope-btn, .rev-scope-btn");
      if (!btn || !scopeNav.contains(btn)) return;
      e.preventDefault();
      onScopeClick(btn);
    });
  } else {
    scopeButtons().forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        onScopeClick(btn);
      });
    });
  }

  if (scopeInput) setScopeUi(scopeInput.value || "general");
})();
