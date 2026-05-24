/**
 * Shop customer detail — credit note / analytics tables / charts.
 */
(function () {
  var cfgEl = document.getElementById("shop-customer-detail-cfg");
  if (!cfgEl) return;
  var cfg;
  try {
    cfg = JSON.parse(cfgEl.textContent || "{}");
  } catch (e) {
    cfg = {};
  }

  var charts = {};
  var chartsReady = false;

  function fmtMoney(n) {
    var x = Number(n) || 0;
    return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function sortAsc(rows, key) {
    return (rows || []).slice().sort(function (a, b) {
      return String((a && a[key]) || "").localeCompare(String((b && b[key]) || ""));
    });
  }

  function chartTheme() {
    var dark = document.documentElement.dataset.theme === "dark";
    return {
      fg: dark ? "#e2e8f0" : "#0f172a",
      muted: dark ? "#94a3b8" : "#64748b",
      grid: dark ? "rgba(148,163,184,0.12)" : "rgba(100,116,139,0.18)",
    };
  }

  function baseOptions() {
    var t = chartTheme();
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: t.fg, font: { size: 11 } } },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.92)",
          titleColor: "#f8fafc",
          bodyColor: "#e2e8f0",
          borderColor: "rgba(148,163,184,0.3)",
          borderWidth: 1,
          padding: 10,
        },
      },
      scales: {},
    };
  }

  function moneyScales() {
    var t = chartTheme();
    return {
      x: { ticks: { color: t.muted }, grid: { color: t.grid } },
      y: {
        ticks: { color: t.muted, callback: function (v) { return fmtMoney(v); } },
        grid: { color: t.grid },
      },
    };
  }

  function destroyCharts() {
    Object.keys(charts).forEach(function (k) {
      if (charts[k]) {
        charts[k].destroy();
        charts[k] = null;
      }
    });
    charts = {};
    chartsReady = false;
  }

  function initCharts() {
    if (chartsReady || typeof Chart === "undefined") return;
    chartsReady = true;
    var a = cfg.analytics || {};

    function lineChart(id, labels, data, label, color, fill) {
      var el = document.getElementById(id);
      if (!el || !labels.length) return;
      var o = baseOptions();
      o.scales = moneyScales();
      charts[id] = new Chart(el, {
        type: "line",
        data: {
          labels: labels,
          datasets: [{ label: label, data: data, borderColor: color, backgroundColor: fill, fill: true, tension: 0.35 }],
        },
        options: o,
      });
    }

    function barChart(id, labels, data, label, color, horizontal) {
      var el = document.getElementById(id);
      if (!el || !labels.length) return;
      var t = chartTheme();
      var o = baseOptions();
      if (horizontal) {
        o.indexAxis = "y";
        o.scales = {
          x: { ticks: { color: t.muted }, grid: { display: false } },
          y: { ticks: { color: t.muted }, grid: { display: false } },
        };
      } else {
        o.scales = {
          x: { ticks: { color: t.muted }, grid: { display: false } },
          y: { ticks: { color: t.muted, callback: function (v) { return fmtMoney(v); } }, grid: { color: t.grid } },
        };
      }
      charts[id] = new Chart(el, {
        type: "bar",
        data: { labels: labels, datasets: [{ label: label, data: data, backgroundColor: color }] },
        options: o,
      });
    }

    var dailySale = sortAsc(a.daily_sale, "day");
    lineChart(
      "customer-chart-sale-day",
      dailySale.map(function (r) { return String(r.day || ""); }),
      dailySale.map(function (r) { return Number(r.amount) || 0; }),
      "Cash sales",
      "rgba(52, 211, 153, 1)",
      "rgba(52, 211, 153, 0.12)"
    );

    var hourlySale = sortAsc(a.hourly_sale, "hour");
    barChart(
      "customer-chart-sale-hour",
      hourlySale.map(function (r) { return String(r.hour || ""); }),
      hourlySale.map(function (r) { return Number(r.amount) || 0; }),
      "Cash sales",
      "rgba(52, 211, 153, 0.65)",
      false
    );

    var dailyCredit = sortAsc(a.daily_credit, "day");
    lineChart(
      "customer-chart-credit-day",
      dailyCredit.map(function (r) { return String(r.day || ""); }),
      dailyCredit.map(function (r) { return Number(r.amount) || 0; }),
      "Credit",
      "rgba(251, 191, 36, 1)",
      "rgba(251, 191, 36, 0.15)"
    );

    var hourlyCredit = sortAsc(a.hourly_credit, "hour");
    barChart(
      "customer-chart-credit-hour",
      hourlyCredit.map(function (r) { return String(r.hour || ""); }),
      hourlyCredit.map(function (r) { return Number(r.amount) || 0; }),
      "Credit",
      "rgba(251, 191, 36, 0.65)",
      false
    );

    var topItems = (a.top_items || []).slice(0, 12);
    barChart(
      "customer-chart-items",
      topItems.map(function (r) { return String(r.item_name || "").slice(0, 32); }),
      topItems.map(function (r) { return Number(r.qty_sold) || 0; }),
      "Qty",
      "rgba(56, 189, 248, 0.55)",
      true
    );

    var daily = sortAsc(a.daily, "day");
    lineChart(
      "shop-cust-chart-daily",
      daily.map(function (r) { return String(r.day || ""); }),
      daily.map(function (r) { return Number(r.amount) || 0; }),
      "Total",
      "rgba(249, 115, 22, 1)",
      "rgba(249, 115, 22, 0.1)"
    );

    var emps = (a.employees || []).slice(0, 12);
    barChart(
      "shop-cust-chart-emp",
      emps.map(function (r) { return String(r.employee_name || "").slice(0, 24); }),
      emps.map(function (r) { return Number(r.amount) || 0; }),
      "Revenue",
      "rgba(167, 139, 250, 0.6)",
      true
    );
  }

  var note = document.getElementById("analytics-view-credit-note");
  var raw = document.getElementById("analytics-view-raw");
  var visual = document.getElementById("analytics-view-visual");
  var btnNote = document.getElementById("it-analytics-btn-note");
  var btnRaw = document.getElementById("it-analytics-btn-raw");
  var btnVis = document.getElementById("it-analytics-btn-visual");
  var toolbar = document.getElementById("it-support-analytics-toolbar");
  function setActiveBtn(btn) {
    [btnNote, btnRaw, btnVis].forEach(function (b) {
      if (!b) return;
      var on = b === btn;
      b.setAttribute("aria-pressed", on ? "true" : "false");
      b.classList.toggle("it-analytics-active", on);
    });
  }

  function setView(view) {
    var v = view === "visual" ? "visual" : view === "raw" ? "raw" : "note";
    if (!toolbar) return;
    toolbar.setAttribute("data-it-analytics-view", v);
    if (note) note.classList.toggle("hidden", v !== "note");
    if (raw) raw.classList.toggle("hidden", v !== "raw");
    if (visual) visual.classList.toggle("hidden", v !== "visual");
    if (v === "note") setActiveBtn(btnNote);
    if (v === "raw") setActiveBtn(btnRaw);
    if (v === "visual") setActiveBtn(btnVis);

    if (v === "visual") {
      requestAnimationFrame(initCharts);
    } else {
      destroyCharts();
    }
  }

  if (btnNote) btnNote.addEventListener("click", function () { setView("note"); });
  if (btnRaw) btnRaw.addEventListener("click", function () { setView("raw"); });
  if (btnVis) btnVis.addEventListener("click", function () { setView("visual"); });

  var initial = "note";
  try {
    var qv = new URLSearchParams(window.location.search).get("view");
    if (qv === "raw" || qv === "visual") initial = qv;
  } catch (e) {}
  setView(initial);

  if (window.location.hash === "#shop-credit-payments") {
    setView("note");
    var paySection = document.getElementById("shop-credit-payments");
    if (paySection) {
      requestAnimationFrame(function () {
        paySection.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }
  }

  document.querySelectorAll(".shop-cust-tx-items-toggle").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var id = btn.getAttribute("data-target");
      if (!id) return;
      var row = document.getElementById(id);
      if (!row) return;
      var open = row.classList.contains("hidden");
      row.classList.toggle("hidden", !open);
      btn.setAttribute("aria-expanded", open ? "true" : "false");
    });
  });
})();
