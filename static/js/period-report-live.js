(function () {
  function pad2(n) {
    return n < 10 ? "0" + n : String(n);
  }

  function todayIso() {
    var d = new Date();
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  function fmtMoney(v) {
    try {
      return Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } catch (e) {
      return String(Number(v || 0).toFixed(2));
    }
  }

  function setLiveText(key, value) {
    document.querySelectorAll('[data-report-live="' + key + '"]').forEach(function (el) {
      el.textContent = value;
    });
  }

  function renderSoldRows(tableBody, items) {
    if (!tableBody) return;
    var html = "";
    (items || []).forEach(function (row) {
      var name = row.name || "Item";
      var category = row.category || "";
      var search = (name + " " + category).toLowerCase();
      html +=
        '<tr class="cr-row" data-search="' +
        search.replace(/"/g, "&quot;") +
        '">' +
        '<td class="sticky left-0 z-10 bg-[rgb(var(--rc-surface))] px-4 py-3 shadow-[1px_0_0_rgb(var(--rc-border))]">' +
        '<div class="font-medium text-[rgb(var(--rc-page-fg))]">' +
        name +
        "</div>" +
        (category ? '<div class="text-xs text-[rgb(var(--rc-muted))]">' + category + "</div>" : "") +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums">' +
        (row.stock_sold || 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums text-[rgb(var(--rc-page-fg))]">' +
        fmtMoney(row.revenue || 0) +
        "</td>" +
        "</tr>";
    });
    tableBody.innerHTML = html || '<tr><td colspan="3" class="px-4 py-8 text-center text-sm text-[rgb(var(--rc-muted))]">No items sold yet for this period.</td></tr>';
  }

  function renderExpenditureRows(tableBody, items) {
    if (!tableBody) return;
    var html = "";
    (items || []).forEach(function (row) {
      var name = row.name || "Item";
      var category = row.category || "";
      var supplier = row.supplier || "—";
      var search = (name + " " + category + " " + supplier).toLowerCase();
      html +=
        '<tr class="exp-row" data-search="' +
        search.replace(/"/g, "&quot;") +
        '">' +
        '<td class="whitespace-nowrap px-4 py-3 text-xs tabular-nums text-[rgb(var(--rc-muted))]">' +
        (row.created_at || "—") +
        "</td>" +
        '<td class="px-4 py-3"><div class="font-medium text-[rgb(var(--rc-page-fg))]">' +
        name +
        "</div>" +
        (category ? '<div class="text-xs text-[rgb(var(--rc-muted))]">' + category + "</div>" : "") +
        "</td>" +
        '<td class="px-4 py-3 text-sm text-[rgb(var(--rc-muted))]">' +
        supplier +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums">' +
        (row.qty != null ? row.qty : 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums">' +
        fmtMoney(row.buying_price || 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums text-amber-700 dark:text-amber-300">' +
        fmtMoney(row.total_cost || 0) +
        "</td>" +
        "</tr>";
    });
    tableBody.innerHTML =
      html ||
      '<tr><td colspan="6" class="px-4 py-10 text-center text-sm text-[rgb(var(--rc-muted))]">No stock purchases recorded for this period.</td></tr>';
  }

  function applyReportPayload(data) {
    var rd = (data && data.report) || data || {};
    var till = rd.till_summary || {};
    setLiveText("total_revenue", fmtMoney(rd.total_revenue));
    setLiveText("sale_revenue", fmtMoney(rd.sale_revenue));
    setLiveText("credit_revenue", fmtMoney(rd.credit_revenue));
    setLiveText("cash_revenue", fmtMoney(rd.cash_revenue));
    setLiveText("mpesa_revenue", fmtMoney(rd.mpesa_revenue));
    setLiveText("total_expenditure", fmtMoney(rd.total_expenditure));
    setLiveText("net_profit", fmtMoney(rd.net_profit));
    setLiveText("opening_cash", fmtMoney(till.opening_cash));
    setLiveText("opening_mpesa", fmtMoney(till.opening_mpesa));
    setLiveText("opening_total", fmtMoney(till.opening_total));
    setLiveText("closing_cash", fmtMoney(till.closing_cash));
    setLiveText("closing_mpesa", fmtMoney(till.closing_mpesa));
    setLiveText("closing_total", fmtMoney(till.closing_total));
    var badge = document.getElementById("report-live-badge");
    if (badge && data.generated_at) badge.textContent = "Updated " + data.generated_at;
    renderSoldRows(document.getElementById("report-items-sold-body"), rd.items_sold || []);
    renderExpenditureRows(document.getElementById("report-expenditure-body"), rd.expenditure_rows || []);
  }

  window.initPeriodReportLive = function (opts) {
    opts = opts || {};
    var jsonUrl = opts.jsonUrl || "";
    var pollMs = opts.pollMs || 45000;
    var enabled = !!opts.liveEnabled;
    if (!enabled || !jsonUrl) return;

    function refresh() {
      fetch(jsonUrl, { headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" } })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          if (!j || j.ok === false) return;
          applyReportPayload(j);
        })
        .catch(function () {});
    }

    refresh();
    setInterval(refresh, pollMs);
  };
})();
