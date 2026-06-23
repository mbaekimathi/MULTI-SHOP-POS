/**
 * IT Support revenue analytics — live filters with staged loading for bulk data.
 */
(function () {
  var CFG = window.IT_SUPPORT_ANALYTICS || {};
  var revCfg = window.IT_REVENUE_ANALYTICS || {};
  var pageKey = CFG.key || "revenue";
  if (pageKey !== "revenue") return;

  var form =
    document.getElementById("it-analytics-filter-form") ||
    document.getElementById("rev-analytics-filter-form");
  var dataUrl = CFG.dataUrl || revCfg.dataUrl;
  if (!form || !dataUrl) return;
  var TX_PAGE = Number(CFG.pageSize || revCfg.txPageSize) || 120;
  var root =
    document.getElementById("it-analytics-live-root") || document.getElementById("rev-live-root");
  var scopeInput =
    document.getElementById("it-analytics-scope-input") ||
    document.getElementById("rev-analytics-scope-input");
  var periodLabel = document.getElementById("rev-period-label");
  var scopeChip = document.getElementById("rev-scope-chip");
  var jsonEl = document.getElementById("it-support-analytics-json");
  var modeEl = document.getElementById("analytics-filter-mode");
  var filterDay = document.getElementById("filter-single-day");
  var filterPeriod = document.getElementById("filter-period");
  var filterMonth = document.getElementById("filter-month");
  var filterYear = document.getElementById("filter-year");
  var scopeNav = form.querySelector(".rev-tabs--scope");
  var scopeCluster = form.querySelector(".rev-toolbar__cluster--scope");
  var filterApi = window.itSupportAnalyticsFilter || {};
  var loadMoreBtn = document.getElementById("rev-tx-load-more");
  var debounceTimer = null;
  var fetchSeq = 0;
  var inFlight = null;
  var txInFlight = null;
  var lastSummary = null;

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
    return form.querySelectorAll(".it-scope-btn, .rev-scope-btn");
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function fmtMoney(n) {
    var x = Number(n) || 0;
    return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  function syncModeFields() {
    if (!modeEl) return;
    var m = modeEl.value || "single_day";
    if (filterDay) filterDay.classList.toggle("hidden", m !== "single_day");
    if (filterPeriod) filterPeriod.classList.toggle("hidden", m !== "period");
    if (filterMonth) filterMonth.classList.toggle("hidden", m !== "month");
    if (filterYear) filterYear.classList.toggle("hidden", m !== "year");
  }
  function buildQueryParams() {
    var params = new URLSearchParams();
    applyScopeToParams(params, scopeInput ? scopeInput.value : "general");
    if (modeEl) params.set("mode", modeEl.value || "single_day");
    var mode = modeEl ? modeEl.value : "single_day";
    var sd = document.getElementById("analytics-single-day");
    var start = document.getElementById("analytics-start-date");
    var end = document.getElementById("analytics-end-date");
    var month = document.getElementById("analytics-month");
    var year = document.getElementById("analytics-year");
    if (mode === "single_day" && sd && sd.value) params.set("single_day", sd.value);
    if (mode === "period") {
      if (start && start.value) params.set("start_date", start.value);
      if (end && end.value) params.set("end_date", end.value);
    }
    if (mode === "month" && month && month.value) params.set("month", month.value);
    if (mode === "year" && year && year.value) params.set("year", year.value);
    var shopEl =
      document.getElementById("it-analytics-shop-select") ||
      document.getElementById("shop-analytics-select");
    if (shopEl && shopEl.value) params.set("shop_id", shopEl.value);
    return params;
  }
  function setLoading(loading) {
    if (root) {
      root.classList.toggle("rev-live-loading", loading);
      root.setAttribute("aria-busy", loading ? "true" : "false");
    }
    form.querySelectorAll("select, input:not([type='hidden'])").forEach(function (el) {
      el.disabled = loading;
    });
    if (loadMoreBtn && loading) loadMoreBtn.disabled = true;
  }
  function setTxLoading(loading) {
    var txBody = document.getElementById("rev-tx-body");
    if (txBody) txBody.classList.toggle("rev-tx-body--loading", loading);
    if (loadMoreBtn && !loading && loadMoreBtn.hidden === false) {
      loadMoreBtn.disabled = false;
      var def = loadMoreBtn.getAttribute("data-label-default");
      if (def) loadMoreBtn.textContent = def;
    }
  }
  function updateScopeUi(scope) {
    var next = normalizeScope(scope);
    if (scopeInput) scopeInput.value = next;
    if (scopeChip) scopeChip.textContent = next === "actual" ? "Actual" : "General";
    scopeButtons().forEach(function (btn) {
      var sc = btn.getAttribute("data-it-scope") || btn.getAttribute("data-rev-scope") || "general";
      var active = normalizeScope(sc) === next;
      btn.classList.toggle("is-active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
    if (scopeNav) scopeNav.setAttribute("data-active-scope", next);
    if (scopeCluster) scopeCluster.setAttribute("data-active-scope", next);
  }
  function updateKpis(rd) {
    var el;
    el = document.getElementById("rev-kpi-total");
    if (el) el.textContent = fmtMoney(rd.total_amount);
    el = document.getElementById("rev-kpi-credit");
    if (el) el.textContent = fmtMoney(rd.credit_amount);
    el = document.getElementById("rev-kpi-cash");
    if (el) el.textContent = fmtMoney(rd.cash_paid_total);
    el = document.getElementById("rev-kpi-mpesa");
    if (el) el.textContent = fmtMoney(rd.mpesa_paid_total);
    el = document.getElementById("rev-kpi-tx");
    if (el) el.textContent = String(rd.tx_count || 0);
  }
  function updateTxMeta(meta) {
    meta = meta || {};
    var loaded = meta.loaded_count != null ? meta.loaded_count : 0;
    var total = meta.total_count != null ? meta.total_count : 0;
    var txCount = document.getElementById("rev-tx-count");
    if (txCount) txCount.textContent = loaded + " / " + total;
    if (loadMoreBtn) {
      loadMoreBtn.hidden = !meta.has_more;
      if (!meta.has_more) loadMoreBtn.disabled = false;
    }
  }
  function renderShopsTable(shops) {
    if (!shops || !shops.length) {
      return '<p class="rev-empty">No shop revenue for this period.</p>';
    }
    var rows = shops
      .map(function (row) {
        return (
          "<tr>" +
          '<td class="cell-strong">' +
          esc(row.shop_name) +
          "</td>" +
          '<td class="num cell-sale">' +
          fmtMoney(row.sale_amount) +
          "</td>" +
          '<td class="num cell-credit">' +
          fmtMoney(row.credit_amount) +
          "</td>" +
          '<td class="num">' +
          fmtMoney(row.cash_paid) +
          "</td>" +
          '<td class="num">' +
          fmtMoney(row.mpesa_paid) +
          "</td>" +
          '<td class="num cell-strong">' +
          fmtMoney(row.total_amount) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="rev-scroll rev-scroll--hint"><table class="rev-data rev-data--wide rev-data--shops"><thead><tr>' +
      '<th scope="col">Shop</th><th scope="col" class="num">Sales</th><th scope="col" class="num">Credit</th>' +
      '<th scope="col" class="num">Cash</th><th scope="col" class="num">M-Pesa</th><th scope="col" class="num">Total</th>' +
      "</tr></thead><tbody>" +
      rows +
      "</tbody></table></div>"
    );
  }
  function renderTxRow(tx) {
    var saleType = (tx.sale_type || "").toLowerCase();
    var tagClass = saleType === "sale" ? "rev-tag--sale" : "rev-tag--credit";
    var pay = (tx.payment_method || "—").toUpperCase();
    return (
      "<tr>" +
      '<td class="cell-muted whitespace-nowrap">' +
      esc(tx.created_at) +
      "</td>" +
      '<td class="whitespace-nowrap">' +
      esc(tx.shop_name) +
      "</td>" +
      "<td><span class=\"rev-tag " +
      tagClass +
      '">' +
      esc((tx.sale_type || "").toUpperCase()) +
      "</span></td>" +
      '<td class="num cell-strong">' +
      fmtMoney(tx.total_amount) +
      "</td>" +
      '<td class="cell-muted whitespace-nowrap">' +
      esc(pay) +
      "</td>" +
      '<td class="num">' +
      fmtMoney(tx.cash_amount) +
      "</td>" +
      '<td class="num">' +
      fmtMoney(tx.mpesa_amount) +
      "</td>" +
      '<td class="num cell-muted">' +
      esc(tx.item_count) +
      "</td>" +
      '<td class="cell-ellipsis" title="' +
      esc(tx.customer_name) +
      '">' +
      esc(tx.customer_name || "—") +
      "</td>" +
      '<td class="cell-muted whitespace-nowrap">' +
      esc(tx.customer_phone || "—") +
      "</td>" +
      '<td class="cell-ellipsis" title="' +
      esc(tx.employee_name) +
      '">' +
      esc(tx.employee_name || "—") +
      "</td>" +
      "</tr>"
    );
  }
  function renderTxRows(transactions) {
    return (transactions || []).map(renderTxRow).join("");
  }
  function renderTxTable(transactions) {
    if (!transactions || !transactions.length) {
      return '<p class="rev-empty">No transactions for this period.</p>';
    }
    return (
      '<div class="rev-scroll rev-scroll--lg rev-scroll--hint"><table class="rev-data rev-data--wide rev-data--tx"><thead><tr>' +
      '<th scope="col">Time</th><th scope="col">Shop</th><th scope="col">Type</th><th scope="col" class="num">Amount</th>' +
      '<th scope="col">Payment</th><th scope="col" class="num">Cash</th><th scope="col" class="num">M-Pesa</th>' +
      '<th scope="col" class="num">Items</th><th scope="col">Customer</th><th scope="col">Phone</th><th scope="col">Employee</th>' +
      "</tr></thead><tbody id=\"rev-tx-tbody\">" +
      renderTxRows(transactions) +
      "</tbody></table></div>"
    );
  }
  function updateShopsPanel(rd) {
    var shops = rd.shops || [];
    var shopsBody = document.getElementById("rev-shops-body");
    var shopsCount = document.getElementById("rev-shops-count");
    if (shopsBody) shopsBody.innerHTML = renderShopsTable(shops);
    if (shopsCount) {
      shopsCount.textContent = shops.length + " " + (shops.length === 1 ? "shop" : "shops");
    }
  }
  function updateTxPanel(rd, append) {
    var txs = rd.transactions || [];
    var meta = rd.transactions_meta || {};
    var txBody = document.getElementById("rev-tx-body");
    if (!txBody) return;
    if (append) {
      var tbody = document.getElementById("rev-tx-tbody");
      if (tbody && txs.length) {
        tbody.insertAdjacentHTML("beforeend", renderTxRows(txs));
      }
    } else {
      txBody.innerHTML = renderTxTable(txs);
    }
    updateTxMeta(meta);
  }
  function updateJsonPayload(rd) {
    if (!jsonEl) return;
    var payload = { key: "revenue", revenue: rd };
    jsonEl.textContent = JSON.stringify(payload);
    var visual = document.getElementById("analytics-view-visual");
    if (
      visual &&
      !visual.classList.contains("hidden") &&
      typeof window.itSupportAnalyticsChartsRender === "function"
    ) {
      if (typeof window.itSupportAnalyticsChartsDestroy === "function") {
        window.itSupportAnalyticsChartsDestroy();
      }
      requestAnimationFrame(function () {
        window.itSupportAnalyticsChartsRender(payload);
      });
    }
  }
  function syncUrl(params) {
    var qs = params.toString();
    var path = window.location.pathname;
    var next = qs ? path + "?" + qs : path;
    try {
      history.replaceState(null, "", next);
    } catch (e) {}
    if (window.itSupportAnalyticsFilter) {
      window.itSupportAnalyticsFilter.save(params);
    }
  }
  function fetchJson(url, signal) {
    return fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" },
      signal: signal,
    }).then(function (res) {
      if (!res.ok) throw new Error("HTTP " + res.status);
      return res.json();
    });
  }
  function applySummary(body) {
    if (!body || !body.ok) return;
    var rd = body.revenue || {};
    lastSummary = rd;
    if (periodLabel && body.period_label) periodLabel.textContent = body.period_label;
    var clientScope = scopeInput ? normalizeScope(scopeInput.value) : normalizeScope(body.analytics_scope);
    updateScopeUi(clientScope);
    updateKpis(rd);
    updateShopsPanel(rd);
    var chartRd = Object.assign({}, rd, { transactions: [] });
    updateJsonPayload(chartRd);
  }
  function applyTransactions(body, append) {
    if (!body || !body.ok) return;
    var rd = body.revenue || {};
    updateTxPanel(rd, append);
    if (lastSummary) {
      var prevTx = append ? lastSummary.transactions || [] : [];
      var mergedTx = append ? prevTx.concat(rd.transactions || []) : rd.transactions || [];
      lastSummary = Object.assign({}, lastSummary, {
        transactions: mergedTx,
        transactions_meta: rd.transactions_meta || lastSummary.transactions_meta,
      });
    } else {
      lastSummary = rd;
    }
  }
  function fetchTransactions(params, offset, append, seq) {
    var txParams = new URLSearchParams(params.toString());
    txParams.set("include_transactions", "1");
    txParams.set("tx_limit", String(TX_PAGE));
    txParams.set("tx_offset", String(offset));
    var url = dataUrl + "?" + txParams.toString();
    if (txInFlight && typeof txInFlight.abort === "function") {
      try {
        txInFlight.abort();
      } catch (e) {}
    }
    txInFlight = new AbortController();
    setTxLoading(true);
    return fetchJson(url, txInFlight.signal)
      .then(function (body) {
        if (seq !== fetchSeq) return;
        applyTransactions(body, append);
      })
      .catch(function (err) {
        if (err && err.name === "AbortError") return;
        console.warn("Revenue transactions load failed:", err);
      })
      .finally(function () {
        if (seq === fetchSeq) setTxLoading(false);
      });
  }
  function fetchLive() {
    var params = buildQueryParams();
    syncUrl(params);
    var seq = ++fetchSeq;
    setLoading(true);
    if (inFlight && typeof inFlight.abort === "function") {
      try {
        inFlight.abort();
      } catch (e) {}
    }
    inFlight = new AbortController();
    var summaryParams = new URLSearchParams(params.toString());
    summaryParams.set("include_transactions", "0");
    var summaryUrl = dataUrl + "?" + summaryParams.toString();
    fetchJson(summaryUrl, inFlight.signal)
      .then(function (body) {
        if (seq !== fetchSeq) return;
        applySummary(body);
        setLoading(false);
        return fetchTransactions(params, 0, false, seq);
      })
      .catch(function (err) {
        if (err && err.name === "AbortError") return;
        console.warn("Revenue analytics live update failed:", err);
      })
      .finally(function () {
        if (seq === fetchSeq) setLoading(false);
      });
  }
  function loadMoreTransactions() {
    if (!lastSummary) return;
    var meta = lastSummary.transactions_meta || {};
    if (!meta.has_more) return;
    var params = buildQueryParams();
    var offset = meta.loaded_count != null ? meta.loaded_count : 0;
    if (loadMoreBtn) {
      loadMoreBtn.disabled = true;
      loadMoreBtn.textContent = "Loading…";
    }
    fetchTransactions(params, offset, true, fetchSeq);
  }
  function scheduleFetch(delay) {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(fetchLive, delay == null ? 450 : delay);
  }
  form.addEventListener("submit", function (e) {
    e.preventDefault();
    clearTimeout(debounceTimer);
    fetchLive();
  });
  if (modeEl) {
    modeEl.addEventListener("change", function () {
      syncModeFields();
      scheduleFetch(300);
    });
  }
  ["analytics-single-day", "analytics-start-date", "analytics-end-date", "analytics-month", "analytics-year"].forEach(
    function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("change", function () {
        scheduleFetch(450);
      });
    }
  );
  function onScopeClick(btn) {
    var scope = btn.getAttribute("data-it-scope") || btn.getAttribute("data-rev-scope") || "general";
    var next = normalizeScope(scope);
    if (scopeInput && normalizeScope(scopeInput.value) === next && btn.classList.contains("is-active")) {
      return;
    }
    updateScopeUi(next);
    clearTimeout(debounceTimer);
    fetchLive();
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
  if (loadMoreBtn) {
    loadMoreBtn.addEventListener("click", loadMoreTransactions);
  }

  var revenueShopSelect = document.getElementById("it-analytics-shop-select");
  if (revenueShopSelect) {
    revenueShopSelect.addEventListener("change", function () {
      clearTimeout(debounceTimer);
      fetchLive();
    });
  }

  if (jsonEl && jsonEl.textContent) {
    try {
      var initialPayload = JSON.parse(jsonEl.textContent);
      if (initialPayload && initialPayload.revenue) {
        lastSummary = initialPayload.revenue;
      }
    } catch (e) {}
  }

  syncModeFields();
  if (scopeInput) {
    updateScopeUi(scopeInput.value || "general");
  }
})();

/**
 * Other IT Support analytics pages — live filters via HTML fragment + bulk load more.
 */
(function () {
  var CFG = window.IT_SUPPORT_ANALYTICS || {};
  var pageKey = CFG.key;
  if (!pageKey || pageKey === "revenue") return;
  var fragmentOnly = !CFG.dataUrl;

  var form = document.getElementById("it-analytics-filter-form");
  if (!form) return;

  var dataUrl = CFG.dataUrl;
  var PAGE_SIZE = Number(CFG.pageSize) || 120;
  var bulkKind = CFG.bulkKind;
  var scopeInput = document.getElementById("it-analytics-scope-input");
  var modeEl = document.getElementById("analytics-filter-mode");
  var filterDay = document.getElementById("filter-single-day");
  var filterPeriod = document.getElementById("filter-period");
  var filterMonth = document.getElementById("filter-month");
  var filterYear = document.getElementById("filter-year");
  var scopeNav = form.querySelector(".rev-tabs--scope");
  var scopeCluster = form.querySelector(".rev-toolbar__cluster--scope");
  var filterApi = window.itSupportAnalyticsFilter || {};
  var jsonEl = document.getElementById("it-support-analytics-json");
  var debounceTimer = null;
  var fetchSeq = 0;
  var inFlight = null;
  var bulkInFlight = null;
  var lastData = null;

  function liveRoot() {
    return document.getElementById("it-analytics-live-root");
  }

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

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtMoney(n) {
    var x = Number(n) || 0;
    return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function syncModeFields() {
    if (!modeEl) return;
    var m = modeEl.value || "single_day";
    if (filterDay) filterDay.classList.toggle("hidden", m !== "single_day");
    if (filterPeriod) filterPeriod.classList.toggle("hidden", m !== "period");
    if (filterMonth) filterMonth.classList.toggle("hidden", m !== "month");
    if (filterYear) filterYear.classList.toggle("hidden", m !== "year");
  }

  function buildQueryParams() {
    var params = new URLSearchParams();
    applyScopeToParams(params, scopeInput ? scopeInput.value : "general");
    if (modeEl) params.set("mode", modeEl.value || "single_day");
    var mode = modeEl ? modeEl.value : "single_day";
    var sd = document.getElementById("analytics-single-day");
    var start = document.getElementById("analytics-start-date");
    var end = document.getElementById("analytics-end-date");
    var month = document.getElementById("analytics-month");
    var year = document.getElementById("analytics-year");
    if (mode === "single_day" && sd && sd.value) params.set("single_day", sd.value);
    if (mode === "period") {
      if (start && start.value) params.set("start_date", start.value);
      if (end && end.value) params.set("end_date", end.value);
    }
    if (mode === "month" && month && month.value) params.set("month", month.value);
    if (mode === "year" && year && year.value) params.set("year", year.value);
    if (pageKey === "shop") {
      var shopSelect = document.getElementById("shop-analytics-select");
      var shopView = document.getElementById("shop-analytics-view-input");
      var sid = shopSelect && shopSelect.value ? shopSelect.value : "";
      if (sid) params.set("shop_id", sid);
      if (shopView && shopView.value) params.set("shop_view", shopView.value);
    } else {
      var filterShop = document.getElementById("it-analytics-shop-select");
      if (filterShop && filterShop.value) params.set("shop_id", filterShop.value);
    }
    return params;
  }

  function setLoading(loading) {
    var root = liveRoot();
    if (root) {
      root.classList.toggle("rev-live-loading", loading);
      root.setAttribute("aria-busy", loading ? "true" : "false");
    }
    form.querySelectorAll("select, input:not([type='hidden'])").forEach(function (el) {
      el.disabled = loading;
    });
  }

  function setBulkLoading(loading) {
    var body = document.getElementById("it-bulk-body-" + bulkKind);
    if (body) body.classList.toggle("rev-tx-body--loading", loading);
  }

  function updateScopeUi(scope) {
    var next = normalizeScope(scope);
    if (scopeInput) scopeInput.value = next;
    scopeButtons().forEach(function (btn) {
      var sc = btn.getAttribute("data-it-scope") || "general";
      var active = normalizeScope(sc) === next;
      btn.classList.toggle("is-active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
    if (scopeNav) scopeNav.setAttribute("data-active-scope", next);
    if (scopeCluster) scopeCluster.setAttribute("data-active-scope", next);
  }

  function syncUrl(params) {
    var qs = params.toString();
    var path = window.location.pathname;
    var next = qs ? path + "?" + qs : path;
    try {
      history.replaceState(null, "", next);
    } catch (e) {}
    if (window.itSupportAnalyticsFilter) {
      window.itSupportAnalyticsFilter.save(params);
    }
  }

  function updateJsonFromData(data, extra) {
    if (!jsonEl || !data) return;
    var payload = { key: pageKey };
    if (pageKey === "shop") {
      payload.key = "shop";
      payload.shop_data = data;
      var viewInput = document.getElementById("shop-analytics-view-input");
      payload.shop_view =
        (extra && extra.shop_view) || (viewInput && viewInput.value) || "revenue";
    } else {
      payload[pageKey] = data;
      if (pageKey === "item") payload.item = data;
      if (pageKey === "customer") payload.customer = data;
      if (pageKey === "sales") payload.sales = data;
      if (pageKey === "credit") payload.credit = data;
      if (pageKey === "period") payload.period = data;
      if (pageKey === "employee") payload.employee = data;
    }
    jsonEl.textContent = JSON.stringify(payload);
    var visual = document.getElementById("analytics-view-visual");
    if (
      visual &&
      !visual.classList.contains("hidden") &&
      typeof window.itSupportAnalyticsChartsRender === "function"
    ) {
      if (typeof window.itSupportAnalyticsChartsDestroy === "function") {
        window.itSupportAnalyticsChartsDestroy();
      }
      requestAnimationFrame(function () {
        window.itSupportAnalyticsChartsRender(payload);
      });
    }
  }

  function reloadFragment(params, seq) {
    var url = window.location.pathname + (params.toString() ? "?" + params.toString() : "");
    return fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: { "X-Requested-With": "XMLHttpRequest" },
      signal: inFlight ? inFlight.signal : undefined,
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.text();
      })
      .then(function (html) {
        if (seq !== fetchSeq) return;
        var doc = new DOMParser().parseFromString(html, "text/html");
        var fresh = doc.getElementById("it-analytics-live-root");
        var root = liveRoot();
        if (fresh && root) {
          root.innerHTML = fresh.innerHTML;
          bindBulkLoadMore();
        }
        if (jsonEl) {
          var freshJson = doc.getElementById("it-support-analytics-json");
          if (freshJson && freshJson.textContent) {
            jsonEl.textContent = freshJson.textContent;
            try {
              var parsed = JSON.parse(freshJson.textContent);
              if (pageKey === "shop") {
                lastData = parsed.shop_data || null;
              } else {
                lastData = parsed[pageKey] || parsed.data || null;
              }
              if (
                pageKey === "shop" &&
                typeof window.itSupportAnalyticsChartsRender === "function" &&
                document.getElementById("analytics-view-visual") &&
                !document.getElementById("analytics-view-visual").classList.contains("hidden")
              ) {
                if (typeof window.itSupportAnalyticsChartsDestroy === "function") {
                  window.itSupportAnalyticsChartsDestroy();
                }
                requestAnimationFrame(function () {
                  window.itSupportAnalyticsChartsRender(parsed);
                });
              }
            } catch (e) {}
          }
        }
        var requestedScope = params.get("analytics_scope")
          ? normalizeScope(params.get("analytics_scope"))
          : scopeInput
            ? normalizeScope(scopeInput.value)
            : "general";
        updateScopeUi(requestedScope);
      });
  }

  function renderItemLineRows(lines) {
    return (lines || [])
      .map(function (row) {
        var st = (row.sale_type || "").toLowerCase();
        var cls = st === "sale" ? "text-emerald-400" : "text-amber-400";
        var cust = row.customer_name || "-";
        if (row.customer_phone) cust += " (" + row.customer_phone + ")";
        return (
          "<tr>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))] whitespace-nowrap">' +
          esc(row.created_at) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-page-fg))] whitespace-nowrap">' +
          esc(row.shop_name) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-page-fg))]">' +
          esc(row.item_name) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          esc(row.qty) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          fmtMoney(row.unit_price) +
          "</td>" +
          '<td class="px-3 py-2 text-emerald-400">' +
          fmtMoney(row.line_total) +
          "</td>" +
          '<td class="px-3 py-2 whitespace-nowrap ' +
          cls +
          '">' +
          esc((row.sale_type || "").toUpperCase()) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          esc(row.employee_name || "-") +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          esc(cust) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function renderCustomerRows(customers, txBase) {
    var filterQs = buildQueryParams().toString();
    return (customers || [])
      .map(function (row) {
        var href =
          txBase +
          "?customer_name=" +
          encodeURIComponent(row.customer_name || "") +
          "&customer_phone=" +
          encodeURIComponent(row.customer_phone || "") +
          (filterQs ? "&" + filterQs : "");
        return (
          "<tr>" +
          '<td class="px-3 py-2 font-semibold text-[rgb(var(--rc-page-fg))]">' +
          esc(row.customer_name) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          esc(row.customer_phone) +
          "</td>" +
          '<td class="px-3 py-2 text-[rgb(var(--rc-muted))]">' +
          esc(row.tx_count) +
          "</td>" +
          '<td class="px-3 py-2 text-emerald-400">' +
          fmtMoney(row.sale_amount) +
          "</td>" +
          '<td class="px-3 py-2 text-amber-400">' +
          fmtMoney(row.credit_amount) +
          "</td>" +
          '<td class="px-3 py-2 font-semibold text-[rgb(var(--rc-page-fg))]">' +
          fmtMoney(row.total_amount) +
          "</td>" +
          '<td class="px-3 py-2"><a href="' +
          esc(href) +
          '" class="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))] text-[rgb(var(--rc-page-fg))] hover:bg-[rgb(var(--rc-surface))]" title="View customer transactions" aria-label="View customer transactions"><svg class="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" /><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5s8.268 2.943 9.542 7c-1.274 4.057-5.065 7-9.542 7S3.732 16.057 2.458 12z" /></svg></a></td>' +
          "</tr>"
        );
      })
      .join("");
  }

  function updateBulkMeta(meta) {
    if (!bulkKind || !meta) return;
    var countEl = document.getElementById("it-bulk-count-" + bulkKind);
    if (countEl) {
      countEl.textContent =
        (meta.loaded_count != null ? meta.loaded_count : 0) +
        " / " +
        (meta.total_count != null ? meta.total_count : 0);
    }
    var btn = document.getElementById("it-bulk-load-more-" + bulkKind);
    if (btn) btn.hidden = !meta.has_more;
  }

  function fetchBulk(params, offset, append, seq) {
    var bulkParams = new URLSearchParams(params.toString());
    if (bulkKind === "lines") bulkParams.set("include_lines", "1");
    if (bulkKind === "customers") bulkParams.set("include_customers", "1");
    bulkParams.set("bulk_limit", String(PAGE_SIZE));
    bulkParams.set("bulk_offset", String(offset));
    var url = dataUrl + "?" + bulkParams.toString();
    if (bulkInFlight && typeof bulkInFlight.abort === "function") {
      try {
        bulkInFlight.abort();
      } catch (e) {}
    }
    bulkInFlight = new AbortController();
    setBulkLoading(true);
    return fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" },
      signal: bulkInFlight.signal,
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then(function (body) {
        if (seq !== fetchSeq || !body || !body.ok) return;
        var data = body.data || body[pageKey] || {};
        var metaKey = bulkKind === "lines" ? "lines_meta" : "customers_meta";
        var meta = data[metaKey] || {};
        var tbody = document.getElementById("it-bulk-tbody-" + bulkKind);
        if (tbody) {
          if (bulkKind === "lines") {
            if (append) tbody.insertAdjacentHTML("beforeend", renderItemLineRows(data.lines));
            else tbody.innerHTML = renderItemLineRows(data.lines);
          } else if (bulkKind === "customers") {
            var txBase = form.getAttribute("data-customer-tx-url") || "";
            if (append) tbody.insertAdjacentHTML("beforeend", renderCustomerRows(data.customers, txBase));
            else tbody.innerHTML = renderCustomerRows(data.customers, txBase);
          }
        }
        updateBulkMeta(meta);
        if (lastData) {
          if (bulkKind === "lines") lastData.lines = append ? (lastData.lines || []).concat(data.lines || []) : data.lines || [];
          lastData[metaKey] = meta;
        }
      })
      .catch(function (err) {
        if (err && err.name === "AbortError") return;
        console.warn("Analytics bulk load failed:", err);
      })
      .finally(function () {
        if (seq === fetchSeq) setBulkLoading(false);
      });
  }

  function bindBulkLoadMore() {
    if (!bulkKind) return;
    var btn = document.getElementById("it-bulk-load-more-" + bulkKind);
    if (!btn || btn._bound) return;
    btn._bound = true;
    btn.addEventListener("click", function () {
      var metaKey = bulkKind === "lines" ? "lines_meta" : "customers_meta";
      var meta = (lastData && lastData[metaKey]) || {};
      if (!meta.has_more) return;
      btn.disabled = true;
      btn.textContent = "Loading…";
      var offset = meta.loaded_count != null ? meta.loaded_count : 0;
      fetchBulk(buildQueryParams(), offset, true, fetchSeq).finally(function () {
        btn.disabled = false;
        btn.textContent = btn.getAttribute("data-label-default") || "Load more";
      });
    });
  }

  function fetchLive() {
    var params = buildQueryParams();
    syncUrl(params);
    var seq = ++fetchSeq;
    setLoading(true);
    if (inFlight && typeof inFlight.abort === "function") {
      try {
        inFlight.abort();
      } catch (e) {}
    }
    inFlight = new AbortController();

    if (!fragmentOnly && (bulkKind === "lines" || bulkKind === "customers")) {
      var summaryParams = new URLSearchParams(params.toString());
      if (bulkKind === "lines") summaryParams.set("include_lines", "0");
      if (bulkKind === "customers") summaryParams.set("include_customers", "0");
      fetch(dataUrl + "?" + summaryParams.toString(), {
        method: "GET",
        credentials: "same-origin",
        headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" },
        signal: inFlight.signal,
      })
        .then(function (res) {
          return res.json();
        })
        .then(function (body) {
          if (seq !== fetchSeq || !body || !body.ok) return;
          lastData = body.data || body[pageKey] || {};
          updateJsonFromData(lastData);
          return reloadFragment(params, seq).then(function () {
            return fetchBulk(params, 0, false, seq);
          });
        })
        .catch(function (err) {
          if (err && err.name !== "AbortError") console.warn("Analytics live update failed:", err);
        })
        .finally(function () {
          if (seq === fetchSeq) setLoading(false);
        });
      return;
    }

    reloadFragment(params, seq)
      .catch(function (err) {
        if (err && err.name !== "AbortError") console.warn("Analytics live update failed:", err);
      })
      .finally(function () {
        if (seq === fetchSeq) setLoading(false);
      });
  }

  function scheduleFetch(delay) {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(fetchLive, delay == null ? 450 : delay);
  }

  form.addEventListener("submit", function (e) {
    e.preventDefault();
    clearTimeout(debounceTimer);
    fetchLive();
  });

  if (modeEl) {
    modeEl.addEventListener("change", function () {
      syncModeFields();
      scheduleFetch(300);
    });
  }

  ["analytics-single-day", "analytics-start-date", "analytics-end-date", "analytics-month", "analytics-year"].forEach(
    function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("change", function () {
        scheduleFetch(450);
      });
    }
  );

  function onScopeClick(btn) {
    var scope = btn.getAttribute("data-it-scope") || "general";
    var next = normalizeScope(scope);
    if (scopeInput && normalizeScope(scopeInput.value) === next && btn.classList.contains("is-active")) {
      return;
    }
    updateScopeUi(next);
    clearTimeout(debounceTimer);
    fetchLive();
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

  var shopSelect =
    document.getElementById("it-analytics-shop-select") ||
    (pageKey === "shop" ? document.getElementById("shop-analytics-select") : null);
  if (shopSelect) {
    shopSelect.addEventListener("change", function () {
      clearTimeout(debounceTimer);
      fetchLive();
    });
  }

  if (jsonEl && jsonEl.textContent) {
    try {
      var initial = JSON.parse(jsonEl.textContent);
      if (pageKey === "shop") lastData = initial.shop_data || null;
      else lastData = initial[pageKey] || initial.data || null;
    } catch (e) {}
  }

  bindBulkLoadMore();
  syncModeFields();
  if (scopeInput) {
    updateScopeUi(scopeInput.value || "general");
  }
})();
