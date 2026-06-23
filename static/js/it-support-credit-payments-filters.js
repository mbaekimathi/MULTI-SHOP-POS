/**
 * IT Support credit hub — live GET filters (no full page reload).
 */
(function () {
  function creditBuildParams(form) {
    var liveKey = form.getAttribute("data-live-filter");
    var params = new URLSearchParams();

    if (liveKey === "credit-due") {
      var daysEl = document.getElementById("cp-due-days");
      if (daysEl && daysEl.value) params.set("days", daysEl.value);
      var shopEl = document.getElementById("cp-due-shop");
      if (shopEl && shopEl.value) params.set("shop_id", shopEl.value);
      var customerEl = document.getElementById("cp-due-customer-q");
      var cq = customerEl ? (customerEl.value || "").trim() : "";
      if (cq) params.set("customer_q", cq);
      return params;
    }

    if (liveKey === "credit-audit") {
      var allTime = document.getElementById("cp-audit-all-time");
      if (allTime && allTime.checked) {
        params.set("all_time", "1");
        return params;
      }
      var shopElA = document.getElementById("cp-audit-shop");
      if (shopElA && shopElA.value) params.set("shop_id", shopElA.value);
      var customerElA = document.getElementById("cp-audit-customer-q");
      var cqa = customerElA ? (customerElA.value || "").trim() : "";
      if (cqa) params.set("customer_q", cqa);
      var scopeEl = document.getElementById("cp-audit-payment-scope");
      if (scopeEl && scopeEl.value) params.set("payment_scope", scopeEl.value);
      var modeElA = document.getElementById("cp-audit-mode");
      var modeA = modeElA ? modeElA.value || "single_day" : "single_day";
      params.set("mode", modeA);
      if (modeA === "single_day") {
        var sdA = document.getElementById("cp-audit-day");
        if (sdA && sdA.value) params.set("single_day", sdA.value);
      } else if (modeA === "period") {
        var startA = document.getElementById("cp-audit-start");
        var endA = document.getElementById("cp-audit-end");
        if (startA && startA.value) params.set("start_date", startA.value);
        if (endA && endA.value) params.set("end_date", endA.value);
      } else if (modeA === "month") {
        var monthA = document.getElementById("cp-audit-month-input");
        if (monthA && monthA.value) params.set("month", monthA.value);
      } else if (modeA === "year") {
        var yearA = document.getElementById("cp-audit-year-input");
        if (yearA && yearA.value) params.set("year", yearA.value);
      }
      return params;
    }

    var shopEl = document.getElementById("cp-filter-shop");
    if (shopEl && shopEl.value) params.set("shop_id", shopEl.value);
    var customerEl = document.getElementById("cp-customer-q");
    var cq = customerEl ? (customerEl.value || "").trim() : "";
    if (cq) params.set("customer_q", cq);
    var modeEl = document.getElementById("analytics-filter-mode");
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

  function initCreditForm(form) {
    if (!form || !window.PortalLiveFilters) return;
    window.PortalLiveFilters.wireForm(form, {
      buildParams: function () {
        return creditBuildParams(form);
      },
    });
  }

  function init() {
    initCreditForm(document.getElementById("it-analytics-filter-form"));
    initCreditForm(document.getElementById("credit-audit-filter-form"));
    initCreditForm(document.getElementById("credit-due-filter-form"));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
