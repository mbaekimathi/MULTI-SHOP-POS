/**
 * Portal dashboard — simple Chart.js visuals backed by company / shop period report.
 */
(function () {
  var charts = [];

  function destroyCharts() {
    charts.forEach(function (c) {
      try {
        c.destroy();
      } catch (e) {}
    });
    charts = [];
  }

  function theme() {
    var dark = document.documentElement.dataset.theme === "dark";
    return {
      fg: dark ? "#e2e8f0" : "#0f172a",
      muted: dark ? "#94a3b8" : "#64748b",
      grid: dark ? "rgba(148,163,184,0.12)" : "rgba(100,116,139,0.18)",
    };
  }

  function fmtMoney(n) {
    var x = Number(n) || 0;
    try {
      return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } catch (e) {
      return x.toFixed(2);
    }
  }

  function readBoot() {
    var el = document.getElementById("portal-dashboard-boot");
    if (!el) return { scope: "company", report: {}, shops: [] };
    try {
      return JSON.parse(el.textContent || "{}");
    } catch (e) {
      return { scope: "company", report: {}, shops: [] };
    }
  }

  function topItemsFromReport(rd) {
    var map = {};
    function addRows(rows) {
      (rows || []).forEach(function (row) {
        var name = (row && row.name) || "Item";
        if (!map[name]) map[name] = { name: name, amount: 0 };
        map[name].amount += Number((row && row.amount) || 0);
      });
    }
    addRows(rd.items_sold_sale);
    addRows(rd.items_sold_credit);
    if (!Object.keys(map).length) addRows(rd.items_sold);
    return Object.values(map)
      .sort(function (a, b) {
        return b.amount - a.amount;
      })
      .slice(0, 5);
  }

  function moneyOverview(rd) {
    return {
      labels: ["Revenue", "Spending", "Stock cost", "Net profit"],
      values: [
        Number(rd.summary_revenue_total || rd.collected_revenue || 0),
        Number(rd.summary_expenditure_total || rd.total_expenditure || 0),
        Number(rd.accrual_cogs || 0),
        Number(rd.accrual_net_profit || 0),
      ],
      colors: [
        "rgba(52, 211, 153, 0.88)",
        "rgba(251, 113, 133, 0.88)",
        "rgba(167, 139, 250, 0.88)",
        "rgba(56, 189, 248, 0.88)",
      ],
    };
  }

  function shopRowsFromBoot(boot) {
    return (boot && boot.shops) || [];
  }

  function renderAll(ChartJS, boot) {
    destroyCharts();
    var t = theme();
    var rd = (boot && boot.report) || {};
    var scope = (boot && boot.scope) || "company";

    var overviewEl = document.getElementById("pd-chart-overview");
    if (overviewEl) {
      var ov = moneyOverview(rd);
      charts.push(
        new ChartJS(overviewEl, {
          type: "bar",
          data: {
            labels: ov.labels,
            datasets: [
              {
                data: ov.values,
                backgroundColor: ov.colors,
                borderRadius: 8,
                borderSkipped: false,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: function (ctx) {
                    return fmtMoney(ctx.parsed.y != null ? ctx.parsed.y : ctx.parsed.x);
                  },
                },
              },
            },
            scales: {
              x: {
                ticks: { color: t.fg, font: { size: 11 } },
                grid: { color: t.grid },
              },
              y: {
                ticks: {
                  color: t.muted,
                  font: { size: 10 },
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            },
          },
        })
      );
    }

    var itemsEl = document.getElementById("pd-chart-top-items");
    if (itemsEl) {
      var items = topItemsFromReport(rd);
      charts.push(
        new ChartJS(itemsEl, {
          type: "bar",
          data: {
            labels: items.map(function (r) {
              return r.name;
            }),
            datasets: [
              {
                label: "Sales",
                data: items.map(function (r) {
                  return r.amount;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.82)",
                borderRadius: 6,
              },
            ],
          },
          options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: function (ctx) {
                    return fmtMoney(ctx.parsed.x);
                  },
                },
              },
            },
            scales: {
              x: {
                ticks: {
                  color: t.muted,
                  font: { size: 10 },
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: {
                ticks: { color: t.fg, font: { size: 11 } },
                grid: { display: false },
              },
            },
          },
        })
      );
    }

    var sideEl = document.getElementById("pd-chart-side");
    if (sideEl) {
      if (scope === "company") {
        var shops = shopRowsFromBoot(boot).filter(function (s) {
          return Number(s.total_amount || 0) > 0;
        });
        if (shops.length > 6) shops = shops.slice(0, 6);
        charts.push(
          new ChartJS(sideEl, {
            type: "bar",
            data: {
              labels: shops.map(function (s) {
                return s.shop_name || "Shop";
              }),
              datasets: [
                {
                  label: "Revenue",
                  data: shops.map(function (s) {
                    return Number(s.total_amount || 0);
                  }),
                  backgroundColor: "rgba(99, 102, 241, 0.82)",
                  borderRadius: 6,
                },
              ],
            },
            options: {
              indexAxis: "y",
              responsive: true,
              maintainAspectRatio: false,
              plugins: {
                legend: { display: false },
                tooltip: {
                  callbacks: {
                    label: function (ctx) {
                      return fmtMoney(ctx.parsed.x);
                    },
                  },
                },
              },
              scales: {
                x: {
                  ticks: {
                    color: t.muted,
                    font: { size: 10 },
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
                y: {
                  ticks: { color: t.fg, font: { size: 11 } },
                  grid: { display: false },
                },
              },
            },
          })
        );
      } else {
        var cash = Number(rd.cash_revenue || 0);
        var mpesa = Number(rd.mpesa_revenue || 0);
        if (cash <= 0 && mpesa <= 0) {
          var total = Number(rd.summary_cash_mpesa || 0);
          cash = total * 0.5;
          mpesa = total * 0.5;
        }
        charts.push(
          new ChartJS(sideEl, {
            type: "doughnut",
            data: {
              labels: ["Cash", "M-Pesa"],
              datasets: [
                {
                  data: [cash, mpesa],
                  backgroundColor: ["rgba(52, 211, 153, 0.88)", "rgba(56, 189, 248, 0.88)"],
                  borderWidth: 0,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              cutout: "62%",
              plugins: {
                legend: { position: "bottom", labels: { color: t.fg, padding: 14 } },
                tooltip: {
                  callbacks: {
                    label: function (ctx) {
                      return ctx.label + ": " + fmtMoney(ctx.parsed);
                    },
                  },
                },
              },
            },
          })
        );
      }
    }
  }

  function loadChartJs(cb) {
    if (window.Chart) {
      cb(window.Chart);
      return;
    }
    var s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js";
    s.onload = function () {
      cb(window.Chart);
    };
    document.head.appendChild(s);
  }

  window.initPortalDashboardCharts = function () {
    var boot = readBoot();
    loadChartJs(function (ChartJS) {
      renderAll(ChartJS, boot);
    });

    document.addEventListener("period-report:updated", function (ev) {
      var detail = (ev && ev.detail) || {};
      var rd = detail.report || detail;
      boot.report = rd;
      if (Array.isArray(detail.shops)) {
        boot.shops = detail.shops;
      }
      loadChartJs(function (ChartJS) {
        renderAll(ChartJS, boot);
      });
    });
  };
})();
