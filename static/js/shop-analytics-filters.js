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
  var scopeBtns = form.querySelectorAll(".it-scope-btn");
  var debounceTimer = null;

  function syncModeFields() {
    if (!modeEl) return;
    var m = modeEl.value || "single_day";
    if (filterDay) filterDay.classList.toggle("hidden", m !== "single_day");
    if (filterPeriod) filterPeriod.classList.toggle("hidden", m !== "period");
    if (filterMonth) filterMonth.classList.toggle("hidden", m !== "month");
    if (filterYear) filterYear.classList.toggle("hidden", m !== "year");
  }

  function persistAndSubmit() {
    if (window.itSupportAnalyticsFilter && typeof window.itSupportAnalyticsFilter.save === "function") {
      window.itSupportAnalyticsFilter.save(new URLSearchParams(new FormData(form)));
    }
    form.submit();
  }

  function scheduleSubmit() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(persistAndSubmit, 280);
  }

  if (modeEl) {
    modeEl.addEventListener("change", function () {
      syncModeFields();
      scheduleSubmit();
    });
  }

  form.querySelectorAll("input[type='date'], input[type='month'], input[type='number']").forEach(function (el) {
    el.addEventListener("change", scheduleSubmit);
  });

  scopeBtns.forEach(function (btn) {
    btn.addEventListener("click", function () {
      var scope = btn.getAttribute("data-it-scope") || "general";
      if (scopeInput) scopeInput.value = scope;
      scopeBtns.forEach(function (b) {
        b.classList.toggle("is-active", b === btn);
      });
      persistAndSubmit();
    });
  });

  syncModeFields();
})();
