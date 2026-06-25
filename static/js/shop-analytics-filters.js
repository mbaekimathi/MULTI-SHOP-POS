/**
 * Shop portal analytics — period/scope filters (live HTML swap).
 */
(function (global) {
  var wired = false;

  function normalizeScope(raw) {
    var filterApi = global.itSupportAnalyticsFilter || {};
    if (typeof filterApi.normalizeScope === "function") return filterApi.normalizeScope(raw);
    return String(raw || "general").trim().toLowerCase() === "actual" ? "actual" : "general";
  }

  function resolveLiveTarget(form) {
    if (!form) return null;
    var key = form.getAttribute("data-analytics-key") || "";
    if (key === "receipts" || document.getElementById("receipt-register-root")) {
      return null;
    }
    var root = document.getElementById("it-analytics-live-root");
    if (root) return root;
    return document.getElementById("analytics-view-raw");
  }

  function isReceiptFilterForm(form) {
    return (form.getAttribute("data-analytics-key") || "") === "receipts" || !!document.getElementById("receipt-register-root");
  }

  function buildParams(form) {
    var params = new URLSearchParams();
    var modeEl = form.querySelector("[name='mode']");
    var mode = modeEl ? (modeEl.value || "single_day").trim() : "single_day";
    params.set("mode", mode);

    var scopeInput = form.querySelector("#it-analytics-scope-input");
    if (scopeInput) {
      if (normalizeScope(scopeInput.value) === "actual") params.set("analytics_scope", "actual");
    }

    var shopViewInput = form.querySelector("#shop-analytics-view-input");
    if (shopViewInput && shopViewInput.value) params.set("shop_view", shopViewInput.value);

    ["customer_name", "customer_phone"].forEach(function (name) {
      var el = form.querySelector('[name="' + name + '"]');
      if (el && el.value) params.set(name, el.value);
    });

    if (mode === "single_day") {
      var sd = form.querySelector('[name="single_day"]');
      if (sd && sd.value) params.set("single_day", sd.value);
    } else if (mode === "period") {
      var start = form.querySelector('[name="start_date"]');
      var end = form.querySelector('[name="end_date"]');
      if (start && start.value) params.set("start_date", start.value);
      if (end && end.value) params.set("end_date", end.value);
    } else if (mode === "month") {
      var month = form.querySelector('[name="month"]');
      if (month && month.value) params.set("month", month.value);
    } else if (mode === "year") {
      var year = form.querySelector('[name="year"]');
      if (year && year.value) params.set("year", year.value);
    }

    return params;
  }

  function syncAnalyticsJson(doc) {
    var jsonEl = document.getElementById("it-support-analytics-json");
    if (!jsonEl) return;
    var freshJson = doc.getElementById("it-support-analytics-json");
    if (!freshJson || !freshJson.textContent) return;
    jsonEl.textContent = freshJson.textContent;
    try {
      var parsed = JSON.parse(freshJson.textContent);
      var visual = document.getElementById("analytics-view-visual");
      if (
        visual &&
        !visual.classList.contains("hidden") &&
        typeof global.itSupportAnalyticsChartsRender === "function"
      ) {
        if (typeof global.itSupportAnalyticsChartsDestroy === "function") {
          global.itSupportAnalyticsChartsDestroy();
        }
        requestAnimationFrame(function () {
          global.itSupportAnalyticsChartsRender(parsed);
        });
      }
    } catch (e) {}
  }

  function syncCustomerDetailJson(doc) {
    var cfgEl = document.getElementById("shop-customer-detail-cfg");
    if (!cfgEl) return;
    var freshCfg = doc.getElementById("shop-customer-detail-cfg");
    if (!freshCfg || !freshCfg.textContent) return;
    cfgEl.textContent = freshCfg.textContent;
    global.dispatchEvent(
      new CustomEvent("shop-customer-detail-data-updated", {
        detail: { cfgText: freshCfg.textContent },
      })
    );
  }

  function afterSwap(doc) {
    syncAnalyticsJson(doc);
    syncCustomerDetailJson(doc);
    if (typeof global.shopReceiptsRefresh === "function") {
      global.shopReceiptsRefresh();
    }
  }

  function wireScopeButtons(form, options) {
    var scopeInput = form.querySelector("#it-analytics-scope-input");
    var scopeNav = form.querySelector(".rev-tabs--scope");
    if (!scopeInput && !scopeNav) return;

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
      var scopeCluster = form.querySelector(".rev-toolbar__cluster--scope");
      if (scopeNav) scopeNav.setAttribute("data-active-scope", next);
      if (scopeCluster) scopeCluster.setAttribute("data-active-scope", next);
    }

    function onScopeClick(btn) {
      var scope = btn.getAttribute("data-it-scope") || btn.getAttribute("data-rev-scope") || "general";
      var next = normalizeScope(scope);
      if (scopeInput && normalizeScope(scopeInput.value) === next && btn.classList.contains("is-active")) {
        return;
      }
      setScopeUi(next);
      global.PortalLiveFilters.fetchAndSwap(form, options);
    }

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
  }

  function initForm() {
    if (wired) return true;
    var form = document.getElementById("it-analytics-filter-form");
    if (!form || form.dataset.portalLiveWired === "true") return false;
    if (!global.PortalLiveFilters) return false;

    var target = resolveLiveTarget(form);
    if (!target && !isReceiptFilterForm(form)) return false;

    var options = {
      buildParams: function () {
        return buildParams(form);
      },
      target: target,
      urlOnly: isReceiptFilterForm(form),
      afterSwap: afterSwap,
    };

    global.PortalLiveFilters.wireForm(form, options);
    wireScopeButtons(form, options);
    wired = true;
    return true;
  }

  function tryInit(attempt) {
    if (initForm()) return;
    if ((attempt || 0) < 40) {
      setTimeout(function () {
        tryInit((attempt || 0) + 1);
      }, 50);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      tryInit(0);
    });
  } else {
    tryInit(0);
  }
})(window);
