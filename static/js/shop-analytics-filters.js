/**
 * Shop portal analytics — period/scope filters (full page reload, no live JSON).
 */
(function () {
  var form = document.getElementById("it-analytics-filter-form");
  if (!form) return;

  var modeEl = document.getElementById("analytics-filter-mode");
  var filterDay = document.getElementById("filter-single-day");
  var filterPeriod = document.getElementById("filter-period");
  var filterMonth = document.getElementById("filter-month");
  var filterYear = document.getElementById("filter-year");
  var scopeInput = document.getElementById("it-analytics-scope-input");
  var scopeNav = form.querySelector(".rev-tabs--scope");
  var scopeCluster = form.querySelector(".rev-toolbar__cluster--scope");
  var debounceTimer = null;
  var filterApi = window.itSupportAnalyticsFilter || {};

  function normalizeScope(raw) {
    if (typeof filterApi.normalizeScope === "function") return filterApi.normalizeScope(raw);
    return String(raw || "general").trim().toLowerCase() === "actual" ? "actual" : "general";
  }

  function applyScopeToParams(params, scope) {
    if (typeof filterApi.applyScopeToParams === "function") return filterApi.applyScopeToParams(params, scope);
    var next = normalizeScope(scope);
    if (next === "actual") params.set("analytics_scope", "actual");
    else params.delete("analytics_scope");
    return next;
  }

  function scopeButtons() {
    return form.querySelectorAll(".it-scope-btn");
  }

  function syncModeFields() {
    if (!modeEl) return;
    var m = modeEl.value || "single_day";
    if (filterDay) filterDay.classList.toggle("hidden", m !== "single_day");
    if (filterPeriod) filterPeriod.classList.toggle("hidden", m !== "period");
    if (filterMonth) filterMonth.classList.toggle("hidden", m !== "month");
    if (filterYear) filterYear.classList.toggle("hidden", m !== "year");
  }

  function setScopeUi(scope) {
    var next = normalizeScope(scope);
    if (scopeInput) scopeInput.value = next;
    scopeButtons().forEach(function (btn) {
      var sc = btn.getAttribute("data-it-scope") || "general";
      var on = normalizeScope(sc) === next;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    });
    if (scopeNav) scopeNav.setAttribute("data-active-scope", next);
    if (scopeCluster) scopeCluster.setAttribute("data-active-scope", next);
  }

  function buildNavigateParams(scopeOverride) {
    var params = new URLSearchParams(new FormData(form));
    var next = applyScopeToParams(
      params,
      scopeOverride != null ? scopeOverride : scopeInput ? scopeInput.value : "general"
    );
    if (scopeInput) scopeInput.value = next;
    return params;
  }

  function persistAndNavigate(scopeOverride) {
    var params = buildNavigateParams(scopeOverride);
    if (typeof filterApi.save === "function") {
      filterApi.save(params);
    }
    var qs = params.toString();
    var action = form.getAttribute("action") || window.location.pathname;
    window.location.assign(qs ? action + "?" + qs : action);
  }

  function scheduleNavigate() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(persistAndNavigate, 280);
  }

  if (modeEl) {
    modeEl.addEventListener("change", function () {
      syncModeFields();
      scheduleNavigate();
    });
  }

  form.querySelectorAll("input[type='date'], input[type='month'], input[type='number']").forEach(function (el) {
    el.addEventListener("change", scheduleNavigate);
  });

  function onScopeClick(btn) {
    var scope = btn.getAttribute("data-it-scope") || "general";
    var next = normalizeScope(scope);
    if (scopeInput && normalizeScope(scopeInput.value) === next && btn.classList.contains("is-active")) {
      return;
    }
    setScopeUi(next);
    persistAndNavigate(next);
  }

  if (scopeNav) {
    scopeNav.addEventListener("click", function (e) {
      var btn = e.target.closest(".it-scope-btn");
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

  syncModeFields();
  if (scopeInput) {
    setScopeUi(scopeInput.value || "general");
  }
})();
