/**
 * IT Support analytics — Chart.js visualizations (raw JSON payload from template).
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

  function pushChart(chart) {
    charts.push(chart);
  }

  function commonOpts(t) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: t.fg, font: { size: 11 } },
        },
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

  function sortDailyAsc(rows) {
    if (!rows || !rows.length) return [];
    return rows
      .slice()
      .sort(function (a, b) {
        return String(a.day || "").localeCompare(String(b.day || ""));
      });
  }

  function fmtMoney(n) {
    var x = Number(n) || 0;
    return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function renderRevenue(payload, ChartJS) {
    var t = theme();
    var rd = payload.revenue || {};
    var mix = document.getElementById("it-chart-rev-mix");
    if (mix) {
      var sale = Number(rd.sale_amount) || 0;
      var credit = Number(rd.credit_amount) || 0;
      if (sale > 0 || credit > 0) {
      pushChart(
        new ChartJS(mix, {
          type: "doughnut",
          data: {
            labels: ["Cash sales", "Credit sales"],
            datasets: [
              {
                data: [sale, credit],
                backgroundColor: ["rgba(52, 211, 153, 0.85)", "rgba(251, 191, 36, 0.88)"],
                borderWidth: 0,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { position: "bottom", labels: { color: t.fg, padding: 16 } },
              tooltip: {
                callbacks: {
                  label: function (ctx) {
                    return ctx.label + ": " + fmtMoney(ctx.parsed);
                  },
                },
              },
            },
            cutout: "58%",
          },
        })
      );
      }
    }

    var dailyCanvas = document.getElementById("it-chart-rev-daily");
    var dailyRows = sortDailyAsc(rd.daily || []);
    if (dailyCanvas && dailyRows.length) {
      var labels = dailyRows.map(function (r) {
        return String(r.day || "");
      });
      pushChart(
        new ChartJS(dailyCanvas, {
          type: "line",
          data: {
            labels: labels,
            datasets: [
              {
                label: "Cash",
                data: dailyRows.map(function (r) {
                  return Number(r.sale_amount) || 0;
                }),
                borderColor: "rgba(52, 211, 153, 1)",
                backgroundColor: "rgba(52, 211, 153, 0.12)",
                fill: true,
                tension: 0.35,
              },
              {
                label: "Credit",
                data: dailyRows.map(function (r) {
                  return Number(r.credit_amount) || 0;
                }),
                borderColor: "rgba(251, 191, 36, 1)",
                backgroundColor: "rgba(251, 191, 36, 0.1)",
                fill: true,
                tension: 0.35,
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: {
                ticks: { color: t.muted, maxRotation: 45 },
                grid: { color: t.grid },
              },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var shopCanvas = document.getElementById("it-chart-rev-shops");
    var shops = (rd.shops || []).slice(0, 12);
    if (shopCanvas && shops.length) {
      pushChart(
        new ChartJS(shopCanvas, {
          type: "bar",
          data: {
            labels: shops.map(function (s) {
              return String(s.shop_name || s.shop_code || "");
            }),
            datasets: [
              {
                label: "Cash",
                data: shops.map(function (s) {
                  return Number(s.sale_amount) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.75)",
              },
              {
                label: "Credit",
                data: shops.map(function (s) {
                  return Number(s.credit_amount) || 0;
                }),
                backgroundColor: "rgba(251, 191, 36, 0.8)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { stacked: true, ticks: { color: t.muted, maxRotation: 40 }, grid: { display: false } },
              y: {
                stacked: true,
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderItem(payload, ChartJS) {
    var t = theme();
    var d = payload.item || {};
    var top = (d.top_items || []).slice(0, 15);
    var c1 = document.getElementById("it-chart-item-top");
    if (c1 && top.length) {
      pushChart(
        new ChartJS(c1, {
          type: "bar",
          data: {
            labels: top.map(function (r) {
              return String(r.item_name || "").slice(0, 42);
            }),
            datasets: [
              {
                label: "Revenue",
                data: top.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.75)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }

    var shops = (d.shops || []).slice(0, 12);
    var c2 = document.getElementById("it-chart-item-shops");
    if (c2 && shops.length) {
      pushChart(
        new ChartJS(c2, {
          type: "bar",
          data: {
            labels: shops.map(function (s) {
              return String(s.shop_name || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: shops.map(function (s) {
                  return Number(s.total_revenue) || 0;
                }),
                backgroundColor: "rgba(249, 115, 22, 0.75)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted, maxRotation: 35 }, grid: { display: false } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderPeriod(payload, ChartJS) {
    var t = theme();
    var d = payload.period || {};
    var dailyRows = sortDailyAsc(d.daily || []);

    var c1 = document.getElementById("it-chart-per-daily");
    if (c1 && dailyRows.length) {
      pushChart(
        new ChartJS(c1, {
          type: "bar",
          data: {
            labels: dailyRows.map(function (r) {
              return String(r.day || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: dailyRows.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(56, 189, 248, 0.65)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted, maxRotation: 45 }, grid: { display: false } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var hourly = d.hourly || [];
    var c2 = document.getElementById("it-chart-per-hourly");
    if (c2 && hourly.length) {
      pushChart(
        new ChartJS(c2, {
          type: "line",
          data: {
            labels: hourly.map(function (r) {
              return String(r.hour != null ? r.hour : "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: hourly.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                borderColor: "rgba(167, 139, 250, 1)",
                backgroundColor: "rgba(167, 139, 250, 0.15)",
                fill: true,
                tension: 0.35,
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted }, grid: { color: t.grid } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var emps = (d.employees || []).slice(0, 12);
    var c3 = document.getElementById("it-chart-per-emp");
    if (c3 && emps.length) {
      pushChart(
        new ChartJS(c3, {
          type: "bar",
          data: {
            labels: emps.map(function (r) {
              return String(r.employee_name || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: emps.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.72)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }

    var shops = (d.shops || []).slice(0, 12);
    var c4 = document.getElementById("it-chart-per-shop");
    if (c4 && shops.length) {
      pushChart(
        new ChartJS(c4, {
          type: "bar",
          data: {
            labels: shops.map(function (r) {
              return String(r.shop_name || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: shops.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(251, 191, 36, 0.78)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderEmployee(payload, ChartJS) {
    var t = theme();
    var d = payload.employee || {};
    var rows = (d.employees || []).slice(0, 15);
    var c = document.getElementById("it-chart-emp-rank");
    if (c && rows.length) {
      pushChart(
        new ChartJS(c, {
          type: "bar",
          data: {
            labels: rows.map(function (r) {
              return String(r.employee_name || "");
            }),
            datasets: [
              {
                label: "Cash",
                data: rows.map(function (r) {
                  return Number(r.sale_amount) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.75)",
              },
              {
                label: "Credit",
                data: rows.map(function (r) {
                  return Number(r.credit_amount) || 0;
                }),
                backgroundColor: "rgba(251, 191, 36, 0.78)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                stacked: true,
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { stacked: true, ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderSales(payload, ChartJS) {
    var t = theme();
    var d = payload.sales || {};
    var dailyRows = sortDailyAsc(d.daily || []);

    var c1 = document.getElementById("it-chart-sales-daily");
    if (c1 && dailyRows.length) {
      pushChart(
        new ChartJS(c1, {
          type: "bar",
          data: {
            labels: dailyRows.map(function (r) {
              return String(r.day || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: dailyRows.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.72)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted, maxRotation: 45 }, grid: { display: false } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var hourly = d.hourly || [];
    var c2 = document.getElementById("it-chart-sales-hour");
    if (c2 && hourly.length) {
      pushChart(
        new ChartJS(c2, {
          type: "bar",
          data: {
            labels: hourly.map(function (r) {
              return String(r.hour != null ? r.hour : "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: hourly.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(34, 197, 94, 0.55)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted }, grid: { display: false } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var shops = (d.shops || []).slice(0, 12);
    var c3 = document.getElementById("it-chart-sales-shop");
    if (c3 && shops.length) {
      pushChart(
        new ChartJS(c3, {
          type: "bar",
          data: {
            labels: shops.map(function (r) {
              return String(r.shop_name || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: shops.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(249, 115, 22, 0.72)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }

    var emps = (d.employees || []).slice(0, 12);
    var c4 = document.getElementById("it-chart-sales-emp");
    if (c4 && emps.length) {
      pushChart(
        new ChartJS(c4, {
          type: "bar",
          data: {
            labels: emps.map(function (r) {
              return String(r.employee_name || "");
            }),
            datasets: [
              {
                label: "Revenue",
                data: emps.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(56, 189, 248, 0.72)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderCredit(payload, ChartJS) {
    var t = theme();
    var d = payload.credit || {};
    var dailyRows = sortDailyAsc(d.daily || []);

    var c1 = document.getElementById("it-chart-cr-daily");
    if (c1 && dailyRows.length) {
      pushChart(
        new ChartJS(c1, {
          type: "line",
          data: {
            labels: dailyRows.map(function (r) {
              return String(r.day || "");
            }),
            datasets: [
              {
                label: "Credit",
                data: dailyRows.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                borderColor: "rgba(251, 191, 36, 1)",
                backgroundColor: "rgba(251, 191, 36, 0.15)",
                fill: true,
                tension: 0.35,
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted, maxRotation: 45 }, grid: { color: t.grid } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var hourly = d.hourly || [];
    var c2 = document.getElementById("it-chart-cr-hour");
    if (c2 && hourly.length) {
      pushChart(
        new ChartJS(c2, {
          type: "bar",
          data: {
            labels: hourly.map(function (r) {
              return String(r.hour != null ? r.hour : "");
            }),
            datasets: [
              {
                label: "Credit",
                data: hourly.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(251, 191, 36, 0.65)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.scales = {
              x: { ticks: { color: t.muted }, grid: { display: false } },
              y: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
            };
            return o;
          })(),
        })
      );
    }

    var shops = (d.shops || []).slice(0, 10);
    var c3 = document.getElementById("it-chart-cr-shop");
    if (c3 && shops.length) {
      pushChart(
        new ChartJS(c3, {
          type: "doughnut",
          data: {
            labels: shops.map(function (r) {
              return String(r.shop_name || "");
            }),
            datasets: [
              {
                data: shops.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: [
                  "rgba(251, 191, 36, 0.85)",
                  "rgba(252, 211, 77, 0.8)",
                  "rgba(245, 158, 11, 0.82)",
                  "rgba(217, 119, 6, 0.78)",
                  "rgba(180, 83, 9, 0.75)",
                  "rgba(146, 64, 14, 0.72)",
                  "rgba(120, 53, 15, 0.7)",
                  "rgba(251, 191, 36, 0.55)",
                  "rgba(253, 224, 71, 0.65)",
                  "rgba(234, 179, 8, 0.7)",
                ],
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { position: "right", labels: { color: t.fg, boxWidth: 12 } },
            },
          },
        })
      );
    }

    var custs = (d.customers || []).slice(0, 12);
    var c4 = document.getElementById("it-chart-cr-cust");
    if (c4 && custs.length) {
      pushChart(
        new ChartJS(c4, {
          type: "bar",
          data: {
            labels: custs.map(function (r) {
              return String(r.customer_name || "").slice(0, 28);
            }),
            datasets: [
              {
                label: "Credit",
                data: custs.map(function (r) {
                  return Number(r.revenue) || 0;
                }),
                backgroundColor: "rgba(251, 191, 36, 0.75)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderCustomer(payload, ChartJS) {
    var t = theme();
    var d = payload.customer || {};
    var rows = (d.customers || []).slice(0, 16);
    var c = document.getElementById("it-chart-cust-bar");
    if (c && rows.length) {
      pushChart(
        new ChartJS(c, {
          type: "bar",
          data: {
            labels: rows.map(function (r) {
              return String(r.customer_name || "—").slice(0, 36);
            }),
            datasets: [
              {
                label: "Total spent",
                data: rows.map(function (r) {
                  return Number(r.total_amount) || 0;
                }),
                backgroundColor: "rgba(52, 211, 153, 0.72)",
              },
            ],
          },
          options: (function () {
            var o = commonOpts(t);
            o.indexAxis = "y";
            o.scales = {
              x: {
                ticks: {
                  color: t.muted,
                  callback: function (v) {
                    return fmtMoney(v);
                  },
                },
                grid: { color: t.grid },
              },
              y: { ticks: { color: t.muted }, grid: { display: false } },
            };
            return o;
          })(),
        })
      );
    }
  }

  function renderShop(payload, ChartJS) {
    var t = theme();
    var d = payload.shop_data || {};
    var sv = String(payload.shop_view || "revenue").toLowerCase();

    if (sv === "revenue") {
      var saleAmt = Number((d.sale && d.sale.amount) || 0) || 0;
      var creditAmt = Number((d.credit && d.credit.amount) || 0) || 0;
      var mixEl = document.getElementById("it-chart-shop-rev-mix");
      if (mixEl && (saleAmt > 0 || creditAmt > 0)) {
        pushChart(
          new ChartJS(mixEl, {
            type: "doughnut",
            data: {
              labels: ["Cash sales", "Credit sales"],
              datasets: [
                {
                  data: [saleAmt, creditAmt],
                  backgroundColor: ["rgba(52, 211, 153, 0.85)", "rgba(251, 191, 36, 0.88)"],
                  borderWidth: 0,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: {
                legend: { position: "bottom", labels: { color: t.fg, padding: 16 } },
                tooltip: {
                  callbacks: {
                    label: function (ctx) {
                      return ctx.label + ": " + fmtMoney(ctx.parsed);
                    },
                  },
                },
              },
              cutout: "58%",
            },
          })
        );
      }
      var dailyRows = sortDailyAsc(d.daily || []);
      var dailyEl = document.getElementById("it-chart-shop-rev-daily");
      if (dailyEl && dailyRows.length) {
        pushChart(
          new ChartJS(dailyEl, {
            type: "line",
            data: {
              labels: dailyRows.map(function (r) {
                return String(r.day || "");
              }),
              datasets: [
                {
                  label: "Cash",
                  data: dailyRows.map(function (r) {
                    return Number(r.sale_amount) || 0;
                  }),
                  borderColor: "rgba(52, 211, 153, 1)",
                  backgroundColor: "rgba(52, 211, 153, 0.12)",
                  fill: true,
                  tension: 0.35,
                },
                {
                  label: "Credit",
                  data: dailyRows.map(function (r) {
                    return Number(r.credit_amount) || 0;
                  }),
                  borderColor: "rgba(251, 191, 36, 1)",
                  backgroundColor: "rgba(251, 191, 36, 0.1)",
                  fill: true,
                  tension: 0.35,
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.scales = {
                x: {
                  ticks: { color: t.muted, maxRotation: 45 },
                  grid: { color: t.grid },
                },
                y: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
              };
              return o;
            })(),
          })
        );
      }
      return;
    }

    if (sv === "item") {
      var top = (d.top_items || []).slice(0, 15);
      var c1 = document.getElementById("it-chart-shop-item-top");
      if (c1 && top.length) {
        pushChart(
          new ChartJS(c1, {
            type: "bar",
            data: {
              labels: top.map(function (r) {
                return String(r.item_name || "").slice(0, 42);
              }),
              datasets: [
                {
                  label: "Revenue",
                  data: top.map(function (r) {
                    return Number(r.revenue) || 0;
                  }),
                  backgroundColor: "rgba(52, 211, 153, 0.75)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.indexAxis = "y";
              o.scales = {
                x: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
                y: { ticks: { color: t.muted }, grid: { display: false } },
              };
              return o;
            })(),
          })
        );
      }
      return;
    }

    if (sv === "sales" || sv === "credit" || sv === "period") {
      var isCredit = sv === "credit";
      var labelY = isCredit ? "Credit" : "Revenue";
      var colorBar = isCredit ? "rgba(251, 191, 36, 0.7)" : "rgba(52, 211, 153, 0.65)";
      var colorLine = isCredit ? "rgba(251, 191, 36, 1)" : "rgba(56, 189, 248, 1)";

      var dailyRows = sortDailyAsc(d.daily || []);
      var cD = document.getElementById("it-chart-shop-trend-daily");
      if (cD && dailyRows.length) {
        pushChart(
          new ChartJS(cD, {
            type: "bar",
            data: {
              labels: dailyRows.map(function (r) {
                return String(r.day || "");
              }),
              datasets: [
                {
                  label: labelY,
                  data: dailyRows.map(function (r) {
                    return Number(r.revenue) || 0;
                  }),
                  backgroundColor: colorBar,
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.scales = {
                x: { ticks: { color: t.muted, maxRotation: 45 }, grid: { display: false } },
                y: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
              };
              return o;
            })(),
          })
        );
      }

      var hourly = d.hourly || [];
      var cH = document.getElementById("it-chart-shop-trend-hourly");
      if (cH && hourly.length) {
        pushChart(
          new ChartJS(cH, {
            type: "line",
            data: {
              labels: hourly.map(function (r) {
                return String(r.hour != null ? r.hour : "");
              }),
              datasets: [
                {
                  label: labelY,
                  data: hourly.map(function (r) {
                    return Number(r.revenue) || 0;
                  }),
                  borderColor: colorLine,
                  backgroundColor: isCredit ? "rgba(251, 191, 36, 0.12)" : "rgba(56, 189, 248, 0.12)",
                  fill: true,
                  tension: 0.35,
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.scales = {
                x: { ticks: { color: t.muted }, grid: { color: t.grid } },
                y: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
              };
              return o;
            })(),
          })
        );
      }

      var emps = (d.employees || []).slice(0, 14);
      var cE = document.getElementById("it-chart-shop-emp");
      if (cE && emps.length) {
        pushChart(
          new ChartJS(cE, {
            type: "bar",
            data: {
              labels: emps.map(function (r) {
                return String(r.employee_name || "");
              }),
              datasets: [
                {
                  label: labelY,
                  data: emps.map(function (r) {
                    return Number(r.revenue) || 0;
                  }),
                  backgroundColor: "rgba(249, 115, 22, 0.72)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.indexAxis = "y";
              o.scales = {
                x: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
                y: { ticks: { color: t.muted }, grid: { display: false } },
              };
              return o;
            })(),
          })
        );
      }

      if (isCredit) {
        var custs = (d.customers || []).slice(0, 12);
        var cC = document.getElementById("it-chart-shop-cr-cust");
        if (cC && custs.length) {
          pushChart(
            new ChartJS(cC, {
              type: "bar",
              data: {
                labels: custs.map(function (r) {
                  return String(r.customer_name || "").slice(0, 28);
                }),
                datasets: [
                  {
                    label: "Credit",
                    data: custs.map(function (r) {
                      return Number(r.credit_total != null ? r.credit_total : r.revenue) || 0;
                    }),
                    backgroundColor: "rgba(251, 191, 36, 0.75)",
                  },
                ],
              },
              options: (function () {
                var o = commonOpts(t);
                o.indexAxis = "y";
                o.scales = {
                  x: {
                    ticks: {
                      color: t.muted,
                      callback: function (v) {
                        return fmtMoney(v);
                      },
                    },
                    grid: { color: t.grid },
                  },
                  y: { ticks: { color: t.muted }, grid: { display: false } },
                };
                return o;
              })(),
            })
          );
        }
      }
      return;
    }

    if (sv === "customer") {
      var rows = (d.customers || []).slice(0, 16);
      var c = document.getElementById("it-chart-shop-cust-bar");
      if (c && rows.length) {
        pushChart(
          new ChartJS(c, {
            type: "bar",
            data: {
              labels: rows.map(function (r) {
                return String(r.customer_name || "—").slice(0, 36);
              }),
              datasets: [
                {
                  label: "Total",
                  data: rows.map(function (r) {
                    return Number(r.total_amount) || 0;
                  }),
                  backgroundColor: "rgba(52, 211, 153, 0.72)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.indexAxis = "y";
              o.scales = {
                x: {
                  ticks: {
                    color: t.muted,
                    callback: function (v) {
                      return fmtMoney(v);
                    },
                  },
                  grid: { color: t.grid },
                },
                y: { ticks: { color: t.muted }, grid: { display: false } },
              };
              return o;
            })(),
          })
        );
      }
      return;
    }

    if (sv === "stock") {
      var ins = (d.top_in_items || []).slice(0, 10);
      var outs = (d.top_out_items || []).slice(0, 10);
      var cIn = document.getElementById("it-chart-shop-stock-in");
      if (cIn && ins.length) {
        pushChart(
          new ChartJS(cIn, {
            type: "bar",
            data: {
              labels: ins.map(function (r) {
                return String(r.name || "").slice(0, 32);
              }),
              datasets: [
                {
                  label: "Qty in",
                  data: ins.map(function (r) {
                    return Number(r.qty) || 0;
                  }),
                  backgroundColor: "rgba(52, 211, 153, 0.72)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.indexAxis = "y";
              o.scales = {
                x: { ticks: { color: t.muted }, grid: { color: t.grid } },
                y: { ticks: { color: t.muted }, grid: { display: false } },
              };
              return o;
            })(),
          })
        );
      }
      var cOut = document.getElementById("it-chart-shop-stock-out");
      if (cOut && outs.length) {
        pushChart(
          new ChartJS(cOut, {
            type: "bar",
            data: {
              labels: outs.map(function (r) {
                return String(r.name || "").slice(0, 32);
              }),
              datasets: [
                {
                  label: "Qty out",
                  data: outs.map(function (r) {
                    return Number(r.qty) || 0;
                  }),
                  backgroundColor: "rgba(244, 63, 94, 0.7)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.indexAxis = "y";
              o.scales = {
                x: { ticks: { color: t.muted }, grid: { color: t.grid } },
                y: { ticks: { color: t.muted }, grid: { display: false } },
              };
              return o;
            })(),
          })
        );
      }
      var stockDaily = sortDailyAsc(d.daily || []);
      var cSd = document.getElementById("it-chart-shop-stock-daily");
      if (cSd && stockDaily.length) {
        pushChart(
          new ChartJS(cSd, {
            type: "bar",
            data: {
              labels: stockDaily.map(function (r) {
                return String(r.day || "");
              }),
              datasets: [
                {
                  label: "In",
                  data: stockDaily.map(function (r) {
                    return Number(r.qty_in) || 0;
                  }),
                  backgroundColor: "rgba(52, 211, 153, 0.65)",
                },
                {
                  label: "Out",
                  data: stockDaily.map(function (r) {
                    return Number(r.qty_out) || 0;
                  }),
                  backgroundColor: "rgba(244, 63, 94, 0.55)",
                },
              ],
            },
            options: (function () {
              var o = commonOpts(t);
              o.scales = {
                x: { stacked: true, ticks: { color: t.muted, maxRotation: 45 }, grid: { display: false } },
                y: {
                  stacked: true,
                  ticks: { color: t.muted },
                  grid: { color: t.grid },
                },
              };
              return o;
            })(),
          })
        );
      }
    }
  }

  window.itSupportAnalyticsChartsDestroy = destroyCharts;

  window.itSupportAnalyticsChartsRender = function (payload) {
    destroyCharts();
    if (!payload || typeof Chart === "undefined") return;
    var key = payload.key;
    switch (key) {
      case "revenue":
        renderRevenue(payload, Chart);
        break;
      case "item":
        renderItem(payload, Chart);
        break;
      case "period":
        renderPeriod(payload, Chart);
        break;
      case "employee":
        renderEmployee(payload, Chart);
        break;
      case "sales":
        renderSales(payload, Chart);
        break;
      case "credit":
        renderCredit(payload, Chart);
        break;
      case "customer":
        renderCustomer(payload, Chart);
        break;
      case "shop":
        renderShop(payload, Chart);
        break;
      default:
        break;
    }
  };
})();
