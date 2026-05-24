/**
 * IT Support credit payments — live GET filters with clean analytics-aligned URLs.
 */
(function () {
  function init() {
    var form = document.getElementById("it-analytics-filter-form");
    var toolbar = document.getElementById("cp-credit-toolbar");
    if (!form || form.getAttribute("data-live-filter") !== "credit-payments") return;

    var modeEl = document.getElementById("analytics-filter-mode");
    var filterDay = document.getElementById("filter-single-day");
    var filterPeriod = document.getElementById("filter-period");
    var filterMonth = document.getElementById("filter-month");
    var filterYear = document.getElementById("filter-year");
    var debounceTimer = null;
    var searchTimer = null;

    function syncModeFields() {
      if (!modeEl) return;
      var m = modeEl.value || "single_day";
      if (filterDay) filterDay.classList.toggle("hidden", m !== "single_day");
      if (filterPeriod) filterPeriod.classList.toggle("hidden", m !== "period");
      if (filterMonth) filterMonth.classList.toggle("hidden", m !== "month");
      if (filterYear) filterYear.classList.toggle("hidden", m !== "year");
    }

    function buildCleanParams() {
      var params = new URLSearchParams();
      var shopEl = document.getElementById("cp-filter-shop");
      if (shopEl && shopEl.value) params.set("shop_id", shopEl.value);

      var customerEl = document.getElementById("cp-customer-q");
      var cq = customerEl ? (customerEl.value || "").trim() : "";
      if (cq) params.set("customer_q", cq);

      var mode = modeEl ? modeEl.value || "single_day" : "single_day";
      params.set("mode", mode);

      if (mode === "single_day") {
        var sd = document.getElementById("analytics-single-day");
        if (sd && sd.value) params.set("single_day", sd.value);
      } else if (mode === "period") {
        var start = document.getElementById("analytics-start-date");
        var end = document.getElementById("analytics-end-date");
        if (start && start.value) params.set("start_date", start.value);
        if (end && end.value) params.set("end_date", end.value);
      } else if (mode === "month") {
        var month = document.getElementById("analytics-month");
        if (month && month.value) params.set("month", month.value);
      } else if (mode === "year") {
        var year = document.getElementById("analytics-year");
        if (year && year.value) params.set("year", year.value);
      }

      return params;
    }

    function setLoading(loading) {
      if (!toolbar) return;
      toolbar.classList.toggle("cp-credit-toolbar--loading", loading);
      toolbar.setAttribute("aria-busy", loading ? "true" : "false");
    }

    function persistAndSubmit() {
      var params = buildCleanParams();
      if (window.itSupportAnalyticsFilter && typeof window.itSupportAnalyticsFilter.save === "function") {
        window.itSupportAnalyticsFilter.save(params);
      }
      setLoading(true);
      var base = form.getAttribute("action") || form.action || window.location.pathname;
      var qs = params.toString();
      window.location.href = qs ? base + "?" + qs : base;
    }

    function scheduleSubmit(delay) {
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(function () {
        debounceTimer = null;
        persistAndSubmit();
      }, delay == null ? 280 : delay);
    }

    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      persistAndSubmit();
    });

    if (modeEl) {
      modeEl.addEventListener("change", function () {
        syncModeFields();
        scheduleSubmit(80);
      });
    }

    form.querySelectorAll("select").forEach(function (el) {
      if (el === modeEl) return;
      el.addEventListener("change", function () {
        scheduleSubmit(120);
      });
    });

    form.querySelectorAll('input[type="date"], input[type="month"]').forEach(function (el) {
      el.addEventListener("change", function () {
        scheduleSubmit();
      });
    });

    var yearInput = document.getElementById("analytics-year");
    if (yearInput) {
      yearInput.addEventListener("change", function () {
        scheduleSubmit();
      });
      yearInput.addEventListener("input", function () {
        if (searchTimer) clearTimeout(searchTimer);
        searchTimer = setTimeout(function () {
          searchTimer = null;
          var y = parseInt(yearInput.value, 10);
          if (y >= 2000 && y <= 2100) scheduleSubmit(0);
        }, 450);
      });
    }

    form.querySelectorAll('input[type="search"]').forEach(function (el) {
      el.addEventListener("input", function () {
        if (searchTimer) clearTimeout(searchTimer);
        searchTimer = setTimeout(function () {
          searchTimer = null;
          scheduleSubmit(200);
        }, 500);
      });
    });

    syncModeFields();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
