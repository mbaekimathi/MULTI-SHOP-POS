/**
 * Persist IT Support analytics period/scope across pages (sessionStorage + nav links).
 */
(function (global) {
  var STORAGE_KEY = "it-support-analytics-filter";
  var KEYS = [
    "analytics_scope",
    "mode",
    "single_day",
    "start_date",
    "end_date",
    "month",
    "year",
    "shop_id",
    "shop_view",
    "all_time",
    "customer_q",
  ];

  function fromSearchParams(params) {
    var out = {};
    KEYS.forEach(function (key) {
      if (params.has(key)) {
        var v = params.get(key);
        if (v != null && String(v).trim() !== "") out[key] = String(v);
      }
    });
    return out;
  }

  function normalizeScope(raw) {
    return String(raw || "general").trim().toLowerCase() === "actual" ? "actual" : "general";
  }

  /** Set or remove analytics_scope on query params (general omits the param — server default). */
  function applyScopeToParams(params, scope) {
    var next = normalizeScope(scope);
    if (!params || typeof params.set !== "function") return next;
    if (next === "actual") params.set("analytics_scope", "actual");
    else params.delete("analytics_scope");
    return next;
  }

  function toSearchString(obj) {
    var p = new URLSearchParams();
    KEYS.forEach(function (key) {
      if (key === "analytics_scope") {
        if (normalizeScope(obj.analytics_scope) === "actual") {
          p.set("analytics_scope", "actual");
        }
        return;
      }
      if (obj[key] != null && String(obj[key]).trim() !== "") {
        p.set(key, String(obj[key]));
      }
    });
    return p.toString();
  }

  function save(params) {
    var obj;
    if (params instanceof URLSearchParams) {
      obj = fromSearchParams(params);
    } else if (params && typeof params === "object") {
      obj = {};
      KEYS.forEach(function (key) {
        if (params[key] != null && String(params[key]).trim() !== "") {
          obj[key] = String(params[key]);
        }
      });
    } else {
      return;
    }
    if (!obj.mode && !obj.all_time) return;
    obj.analytics_scope = normalizeScope(obj.analytics_scope);
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(obj));
    } catch (e) {}
    patchNavLinks(obj);
  }

  function load() {
    try {
      var raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      var parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  /** Keys synced onto every analytics nav link (period + scope + optional shop). */
  var FILTER_KEYS = [
    "analytics_scope",
    "mode",
    "single_day",
    "start_date",
    "end_date",
    "month",
    "year",
    "shop_id",
  ];

  function isShopAnalyticsHref(base) {
    return /\/it_support\/analytics\/shop\/?$/i.test(String(base || "").replace(/\/+$/, ""));
  }

  function isReceiptsHref(base) {
    return /\/it_support\/receipts\/?$/i.test(String(base || "").replace(/\/+$/, ""));
  }

  function isAnalyticsHref(base) {
    return /\/it_support\/analytics\//i.test(String(base || ""));
  }

  function isCreditHubHref(base) {
    return /\/it_support\/credit-payments/i.test(String(base || ""));
  }

  function applyShopIdToParams(existing, q, base) {
    if (isShopAnalyticsHref(base) || isAnalyticsHref(base) || isReceiptsHref(base) || isCreditHubHref(base)) {
      if (q.shop_id != null && String(q.shop_id).trim() !== "") {
        existing.set("shop_id", String(q.shop_id));
      } else {
        existing.delete("shop_id");
      }
      return;
    }
    existing.delete("shop_id");
  }

  /**
   * Update filter query params on nav links without clobbering per-link values
   * (e.g. shop_view=item on the Item sidebar link while viewing Revenue).
   */
  function patchNavLinks(q) {
    if (!q || (!q.mode && !q.all_time)) return;
    document.querySelectorAll("a[data-it-analytics-nav]").forEach(function (a) {
      var href = a.getAttribute("href") || "";
      var qIdx = href.indexOf("?");
      var base = qIdx >= 0 ? href.slice(0, qIdx) : href;
      var existing = new URLSearchParams(qIdx >= 0 ? href.slice(qIdx + 1) : "");
      var creditHub = isCreditHubHref(base);

      if (q.all_time) {
        existing.set("all_time", "1");
        ["mode", "single_day", "start_date", "end_date", "month", "year"].forEach(function (key) {
          existing.delete(key);
        });
      } else {
        existing.delete("all_time");
        FILTER_KEYS.forEach(function (key) {
          if (creditHub && key === "analytics_scope") return;
          if (key === "analytics_scope") {
            if (normalizeScope(q.analytics_scope) === "actual") {
              existing.set("analytics_scope", "actual");
            } else {
              existing.delete("analytics_scope");
            }
            return;
          }
          if (q[key] != null && String(q[key]).trim() !== "") {
            existing.set(key, String(q[key]));
          }
        });
      }

      if (creditHub) {
        existing.delete("analytics_scope");
      }

      applyShopIdToParams(existing, q, base);

      if (isShopAnalyticsHref(base)) {
        /* shop_view stays on per-link hrefs */
      } else if (creditHub) {
        if (q.customer_q != null && String(q.customer_q).trim() !== "") {
          existing.set("customer_q", String(q.customer_q));
        } else {
          existing.delete("customer_q");
        }
        existing.delete("shop_view");
      } else if (isAnalyticsHref(base) || isReceiptsHref(base)) {
        existing.delete("shop_view");
        existing.delete("customer_q");
        existing.delete("all_time");
      } else {
        existing.delete("shop_view");
        existing.delete("customer_q");
        existing.delete("all_time");
      }
      var qs = existing.toString();
      a.setAttribute("href", qs ? base + "?" + qs : base);
    });
  }

  function urlHasFilterParams() {
    var p = new URLSearchParams(window.location.search);
    return p.has("mode") || p.has("all_time");
  }

  /** Redirect once if this page loaded without filter query but we have a saved filter. */
  function restoreUrlFromStorage() {
    if (urlHasFilterParams()) return false;
    var stored = load();
    if (!stored || !stored.mode) return false;
    var qs = toSearchString(stored);
    if (!qs) return false;
    global.location.replace(global.location.pathname + "?" + qs);
    return true;
  }

  function initNavLinks() {
    var fromUrl = fromSearchParams(new URLSearchParams(global.location.search));
    if (fromUrl.mode) {
      save(fromUrl);
      return;
    }
    var stored = load();
    if (stored) patchNavLinks(stored);
  }

  global.itSupportAnalyticsFilter = {
    KEYS: KEYS,
    save: save,
    load: load,
    patchNavLinks: patchNavLinks,
    restoreUrlFromStorage: restoreUrlFromStorage,
    initNavLinks: initNavLinks,
    normalizeScope: normalizeScope,
    applyScopeToParams: applyScopeToParams,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      if (!restoreUrlFromStorage()) initNavLinks();
    });
  } else if (!restoreUrlFromStorage()) {
    initNavLinks();
  }
})(window);
