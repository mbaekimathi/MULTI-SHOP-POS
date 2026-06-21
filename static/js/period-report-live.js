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
      if (el.getAttribute("data-report-live-signed")) return;
      el.textContent = value;
    });
  }

  function setLiveSignedMoney(key, value) {
    var n = Number(value || 0);
    document.querySelectorAll('[data-report-live="' + key + '"][data-report-live-signed]').forEach(function (el) {
      el.textContent = fmtMoney(n);
      el.classList.remove(
        "text-emerald-600",
        "dark:text-emerald-400",
        "text-rose-600",
        "dark:text-rose-400"
      );
      if (n >= 0) {
        el.classList.add("text-emerald-600", "dark:text-emerald-400");
      } else {
        el.classList.add("text-rose-600", "dark:text-rose-400");
      }
    });
  }

  function creditPaymentStatusBadge(status) {
    var s = String(status || "not_paid").toLowerCase();
    if (s === "paid") {
      return (
        '<span class="inline-flex rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-emerald-700 dark:text-emerald-300">Paid</span>'
      );
    }
    if (s === "partially_paid") {
      return (
        '<span class="inline-flex rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-700 dark:text-amber-300">Partial</span>'
      );
    }
    return (
      '<span class="inline-flex rounded-full bg-rose-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-rose-700 dark:text-rose-300">Not paid</span>'
    );
  }

  function renderSoldCreditRows(tableBody, items) {
    if (!tableBody) return;
    var html = "";
    (items || []).forEach(function (row) {
      var name = row.name || "Item";
      var category = row.category || "";
      var ps = String(row.payment_status || "not_paid").toLowerCase();
      var search = (name + " " + category + " " + ps).toLowerCase();
      html +=
        '<tr class="cr-credit-row" data-search="' +
        search.replace(/"/g, "&quot;") +
        '">' +
        '<td class="px-4 py-3">' +
        '<div class="font-medium text-[rgb(var(--rc-page-fg))]">' +
        name +
        "</div>" +
        (category ? '<div class="text-xs text-[rgb(var(--rc-muted))]">' + category + "</div>" : "") +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums">' +
        (row.qty != null ? row.qty : 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums text-sky-700 dark:text-sky-300">' +
        fmtMoney(row.amount || 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums text-violet-700 dark:text-violet-300">' +
        fmtMoney(row.stock_cost || 0) +
        "</td>" +
        '<td class="px-4 py-3">' +
        creditPaymentStatusBadge(ps) +
        "</td>" +
        "</tr>";
    });
    tableBody.innerHTML =
      html ||
      '<tr><td colspan="5" class="px-4 py-10 text-center text-sm text-[rgb(var(--rc-muted))]">No credit sales for this period yet.</td></tr>';
  }

  function renderSoldSaleRows(tableBody, items) {
    if (!tableBody) return;
    var html = "";
    (items || []).forEach(function (row) {
      var name = row.name || "Item";
      var category = row.category || "";
      var search = (name + " " + category).toLowerCase();
      html +=
        '<tr class="cr-sale-row" data-search="' +
        search.replace(/"/g, "&quot;") +
        '">' +
        '<td class="px-4 py-3">' +
        '<div class="font-medium text-[rgb(var(--rc-page-fg))]">' +
        name +
        "</div>" +
        (category ? '<div class="text-xs text-[rgb(var(--rc-muted))]">' + category + "</div>" : "") +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums">' +
        (row.qty != null ? row.qty : 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums text-emerald-700 dark:text-emerald-300">' +
        fmtMoney(row.amount || 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right tabular-nums text-violet-700 dark:text-violet-300">' +
        fmtMoney(row.stock_cost || 0) +
        "</td>" +
        '<td class="px-4 py-3">' +
        creditPaymentStatusBadge("paid") +
        "</td>" +
        "</tr>";
    });
    tableBody.innerHTML =
      html ||
      '<tr><td colspan="5" class="px-4 py-10 text-center text-sm text-[rgb(var(--rc-muted))]">No cash sales for this period yet.</td></tr>';
  }

  function renderSoldRows(tableBody, items) {
    renderSoldSaleRows(tableBody, items);
  }

  function paymentStatusBadge(status) {
    var s = String(status || "pending_payment").toLowerCase();
    if (s === "paid") {
      return (
        '<span class="mt-1 inline-flex rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-emerald-700 dark:text-emerald-300">Paid</span>'
      );
    }
    if (s === "partially_paid") {
      return (
        '<span class="mt-1 inline-flex rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-700 dark:text-amber-300">Partial</span>'
      );
    }
    if (s === "partially_refunded") {
      return (
        '<span class="mt-1 inline-flex rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-700 dark:text-amber-300">Partially refunded</span>'
      );
    }
    if (s === "cancelled_out") {
      return (
        '<span class="mt-1 inline-flex rounded-full bg-slate-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-slate-700 dark:text-slate-300">Cancelled out</span>'
      );
    }
    return (
      '<span class="mt-1 inline-flex rounded-full bg-rose-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-rose-700 dark:text-rose-300">Pending</span>'
    );
  }

  function expenditureKindLabel(kind) {
    if (kind === "operational") return "Expense";
    if (kind === "stock_out") return "Stock out";
    return "Stock";
  }

  function renderExpenditureRows(tableBody, items) {
    if (!tableBody) return;
    var html = "";
    (items || []).forEach(function (row) {
      var name = row.name || "Item";
      var category = row.category || "";
      var supplier = row.supplier || "—";
      var kind = expenditureKindLabel(row.expense_kind);
      var ps = String(row.payment_status || "pending_payment").toLowerCase();
      var total = Number(row.total_cost || row.total_amount || 0);
      var paid = Number(row.amount_paid || 0);
      var bal = row.balance != null ? Number(row.balance) : Math.max(0, total - paid);
      var search = (name + " " + category + " " + supplier + " " + kind + " " + ps + " " + (row.shop_name || "")).toLowerCase();
      var balClass = "text-[rgb(var(--rc-muted))]";
      if (ps === "partially_refunded") balClass = "text-sky-700 dark:text-sky-300";
      else if (ps === "pending_payment") balClass = "text-rose-700 dark:text-rose-300";
      else if ((ps === "paid" || ps === "partially_paid") && bal > 0.009) {
        balClass = "text-amber-700 dark:text-amber-300";
      }
      var balText = "—";
      if (ps === "partially_refunded") balText = fmtMoney(bal);
      else if (ps === "paid" || ps === "partially_paid" || ps === "pending_payment") balText = fmtMoney(bal);
      html +=
        '<tr class="exp-row" data-search="' +
        search.replace(/"/g, "&quot;") +
        '">' +
        '<td class="px-4 py-3">' +
        '<div class="text-[10px] font-bold uppercase text-[rgb(var(--rc-page-fg))]">' +
        kind +
        "</div>" +
        paymentStatusBadge(ps) +
        "</td>" +
        '<td class="whitespace-nowrap px-4 py-3 text-xs tabular-nums text-[rgb(var(--rc-muted))]">' +
        (row.created_at || "—") +
        "</td>" +
        (row.shop_name
          ? '<td class="px-4 py-3 text-sm">' + row.shop_name + "</td>"
          : "") +
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
        fmtMoney(row.buying_price || row.unit_price || 0) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums text-amber-700 dark:text-amber-300">' +
        fmtMoney(total) +
        "</td>" +
        '<td class="px-4 py-3 text-right font-semibold tabular-nums ' +
        balClass +
        '">' +
        balText +
        "</td>" +
        "</tr>";
    });
    var emptyColspan = tableBody.closest("table") && tableBody.closest("table").querySelector("thead tr")
      ? tableBody.closest("table").querySelectorAll("thead th").length
      : 8;
    tableBody.innerHTML =
      html ||
      '<tr><td colspan="' +
      emptyColspan +
      '" class="px-4 py-10 text-center text-sm text-[rgb(var(--rc-muted))]">No expenditure recorded for this period.</td></tr>';
  }

  function applyReportPayload(data) {
    var rd = (data && data.report) || data || {};
    var till = rd.till_summary || {};
    setLiveText("collected_revenue", fmtMoney(rd.collected_revenue));
    setLiveText("total_revenue", fmtMoney(rd.total_revenue));
    setLiveText("accrual_cogs", fmtMoney(rd.accrual_cogs));
    setLiveText("accrual_cogs_sale", fmtMoney(rd.accrual_cogs_sale));
    setLiveText("accrual_cogs_credit", fmtMoney(rd.accrual_cogs_credit));
    setLiveText("accrual_cogs_stock_out", fmtMoney(rd.accrual_cogs_stock_out));
    setLiveText("accrual_operating_expenses", fmtMoney(rd.accrual_operating_expenses));
    setLiveSignedMoney("accrual_gross_profit", rd.accrual_gross_profit);
    setLiveSignedMoney("accrual_net_profit", rd.accrual_net_profit);
    setLiveText("sale_revenue", fmtMoney(rd.sale_revenue));
    setLiveText("credit_revenue", fmtMoney(rd.credit_revenue));
    setLiveText("cash_revenue", fmtMoney(rd.cash_revenue));
    setLiveText("mpesa_revenue", fmtMoney(rd.mpesa_revenue));
    setLiveText("total_expenditure", fmtMoney(rd.total_expenditure));
    setLiveText("paid_expenditure", fmtMoney(rd.paid_expenditure));
    setLiveText("paid_credit", fmtMoney(rd.paid_credit));
    setLiveText("unpaid_credit", fmtMoney(rd.unpaid_credit));
    setLiveText("balance_expenditure", fmtMoney(rd.balance_expenditure));
    setLiveText("stock_cost_sold", fmtMoney(rd.stock_cost_sold));
    setLiveText("stock_cost_stock_out", fmtMoney(rd.stock_cost_stock_out));
    setLiveText("stock_cost_total", fmtMoney(rd.stock_cost_total));
    setLiveText("stock_cost_sale_only", fmtMoney(rd.stock_cost_sale_only));
    setLiveText("stock_cost_credit_total", fmtMoney(rd.stock_cost_credit_total));
    setLiveSignedMoney("estimated_sale_gross_profit", rd.estimated_sale_gross_profit);
    setLiveText("opening_cash", fmtMoney(till.opening_cash));
    setLiveText("opening_mpesa", fmtMoney(till.opening_mpesa));
    setLiveText("opening_total", fmtMoney(till.opening_total));
    setLiveText("closing_cash", fmtMoney(till.closing_cash));
    setLiveText("closing_mpesa", fmtMoney(till.closing_mpesa));
    setLiveText("closing_total", fmtMoney(till.closing_total));
    var badge = document.getElementById("report-live-badge");
    if (badge && data.generated_at) badge.textContent = "Updated " + data.generated_at;
    renderSoldCreditRows(document.getElementById("report-items-credit-body"), rd.items_sold_credit || []);
    renderSoldSaleRows(document.getElementById("report-items-sale-body"), rd.items_sold_sale || []);
    renderSoldRows(document.getElementById("report-items-sold-body"), rd.items_sold || []);
    renderExpenditureRows(document.getElementById("report-expenditure-body"), rd.expenditure_rows || []);
  }

  window.initPeriodReportLive = function (opts) {
    opts = opts || {};
    var jsonUrl = opts.jsonUrl || "";
    var pollMs = opts.pollMs || 45000;
    var enabled = !!opts.liveEnabled;
    if (!jsonUrl) return;

    function refresh() {
      var sep = jsonUrl.indexOf("?") >= 0 ? "&" : "?";
      fetch(jsonUrl + sep + "_=" + Date.now(), {
        headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest", "Cache-Control": "no-cache" },
      })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          if (!j || j.ok === false) return;
          applyReportPayload(j);
        })
        .catch(function () {});
    }

    if (enabled) {
      setInterval(refresh, pollMs);
      document.addEventListener("visibilitychange", function () {
        if (!document.hidden) refresh();
      });
    }
  };
})();
