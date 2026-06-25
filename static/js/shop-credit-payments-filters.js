/**
 * Shop credit payments — live GET filters (clean params per period mode).
 */
(function () {
  function shopCreditBuildParams() {
    var params = new URLSearchParams();
    var modeEl = document.getElementById("shop-credit-filter-mode");
    var mode = modeEl ? (modeEl.value || "all").trim() : "all";

    if (mode === "all") {
      var cqAll = document.getElementById("shop-credit-customer-q");
      var qAll = cqAll ? (cqAll.value || "").trim() : "";
      if (qAll) params.set("customer_q", qAll);
      return params;
    }

    params.set("mode", mode);

    if (mode === "single_day") {
      var sd = document.getElementById("shop-credit-single-day");
      if (sd && sd.value) params.set("single_day", sd.value);
    } else if (mode === "period") {
      var start = document.getElementById("shop-credit-start-date");
      var end = document.getElementById("shop-credit-end-date");
      if (start && start.value) params.set("start_date", start.value);
      if (end && end.value) params.set("end_date", end.value);
    } else if (mode === "month") {
      var month = document.getElementById("shop-credit-month");
      if (month && month.value) params.set("month", month.value);
    } else if (mode === "year") {
      var year = document.getElementById("shop-credit-year");
      if (year && year.value) params.set("year", year.value);
    }

    var cq = document.getElementById("shop-credit-customer-q");
    var q = cq ? (cq.value || "").trim() : "";
    if (q) params.set("customer_q", q);
    return params;
  }

  function init() {
    var form = document.getElementById("shop-credit-filter-form");
    if (!form || !window.PortalLiveFilters) return;
    window.PortalLiveFilters.wireForm(form, {
      buildParams: shopCreditBuildParams,
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
