/**
 * Portal live filters — update page regions via fetch (no full reload).
 */
(function (global) {
  var FETCH_HEADER = "X-Portal-Live-Filter";
  var debounceByForm = new WeakMap();

  function qs(form, selector) {
    return form.querySelector(selector);
  }

  function resolveContentTarget(form) {
    var selector =
      form.getAttribute("data-portal-live-content") ||
      form.dataset.portalLiveContent ||
      "#portal-live-content";
    var el = document.querySelector(selector);
    if (el) return el;
    var root = form.closest("[data-portal-live-root]");
    if (root) {
      el = root.querySelector("[data-portal-live-content]");
      if (el) return el;
    }
    return null;
  }

  function resolveToolbar(form) {
    return (
      form.closest(".rev-toolbar") ||
      form.closest(".cp-credit-toolbar") ||
      form.closest(".portal-filter-toolbar") ||
      form
    );
  }

  function buildParams(form, extraBuilder) {
    if (typeof extraBuilder === "function") {
      return extraBuilder(form);
    }
    var params = new URLSearchParams();
    var data = new FormData(form);
    data.forEach(function (value, key) {
      if (value == null) return;
      var str = String(value).trim();
      if (str === "") return;
      if (key === "all_time" && !form.querySelector('[name="all_time"]:checked')) return;
      params.set(key, str);
    });
    var allTime = form.querySelector('[name="all_time"]');
    if (allTime && allTime.checked) {
      params.set("all_time", "1");
      ["mode", "single_day", "start_date", "end_date", "month", "year"].forEach(function (k) {
        params.delete(k);
      });
    }
    return params;
  }

  function syncModePanels(form) {
    var modeEl = qs(form, "[data-portal-filter-mode], [name='mode']");
    var rangeEl = qs(form, "[name='range']");

    function syncStockPanels() {
      if (!modeEl) return;
      var mode = modeEl.value || "single_day";
      form.querySelectorAll("[data-stock-filter]").forEach(function (panel) {
        var panelMode = panel.getAttribute("data-stock-filter") || "";
        if (mode === "all") {
          panel.classList.add("hidden");
          return;
        }
        panel.classList.toggle("hidden", panelMode !== mode);
      });
    }

    function syncLeadsPanels() {
      if (!rangeEl) return;
      var v = rangeEl.value || "all";
      form.querySelectorAll(".lead-filt-it, .lead-filt").forEach(function (el) {
        el.classList.add("hidden");
      });
      if (v === "day") {
        form.querySelectorAll(".lead-filt-it-day, .lead-filt-day").forEach(function (el) {
          el.classList.remove("hidden");
        });
      }
      if (v === "period") {
        form.querySelectorAll(".lead-filt-it-period, .lead-filt-period").forEach(function (el) {
          el.classList.remove("hidden");
        });
      }
      if (v === "month") {
        form.querySelectorAll(".lead-filt-it-month, .lead-filt-month").forEach(function (el) {
          el.classList.remove("hidden");
        });
      }
      if (v === "year") {
        form.querySelectorAll(".lead-filt-it-year, .lead-filt-year").forEach(function (el) {
          el.classList.remove("hidden");
        });
      }
    }

    function sync() {
      syncLeadsPanels();
      syncStockPanels();
      if (!modeEl) return;

      var mode = modeEl.value;
      if (!mode && modeEl.querySelector('option[value="all"]')) {
        mode = "all";
      } else if (!mode) {
        mode = "single_day";
      }

      if (modeEl.name === "mode" && mode === "") {
        [
          "#suppliers-filter-single-day",
          "#suppliers-filter-period",
          "#suppliers-filter-month",
          "#suppliers-filter-year",
        ].forEach(function (sel) {
          form.querySelectorAll(sel).forEach(function (el) {
            el.classList.add("hidden");
          });
        });
      }

      form.querySelectorAll("[data-portal-filter-panel]").forEach(function (panel) {
        var panelMode = panel.getAttribute("data-portal-filter-panel") || "";
        var show =
          panelMode === mode ||
          (mode === "period" && panelMode.indexOf("period") === 0);
        panel.classList.toggle("hidden", !show);
      });

      var legacyMap = {
        single_day: [
          "#filter-single-day",
          "#cp-audit-single-day",
          "#expenses-filter-single-day",
          "#incomes-filter-single-day",
          "#suppliers-filter-single-day",
          "#it-reports-f-day",
          "#ia-filter-single-day",
          "#sr-filter-single-day",
          "[data-portal-filter-panel='single_day']",
        ],
        period: [
          "#filter-period",
          "#cp-audit-period",
          "#expenses-filter-period",
          "#expenses-filter-period-end",
          "#incomes-filter-period",
          "#incomes-filter-period-end",
          "#suppliers-filter-period",
          "#it-reports-f-start",
          "#it-reports-f-end",
          "#ia-filter-period",
          "#sr-filter-period-start",
          "#sr-filter-period-end",
          "[data-portal-filter-panel='period']",
          "[data-portal-filter-panel='period_start']",
          "[data-portal-filter-panel='period_end']",
        ],
        month: [
          "#filter-month",
          "#cp-audit-month",
          "#expenses-filter-month",
          "#incomes-filter-month",
          "#suppliers-filter-month",
          "#it-reports-f-month",
          "#ia-filter-month",
          "#sr-filter-month",
          "[data-portal-filter-panel='month']",
        ],
        year: [
          "#filter-year",
          "#cp-audit-year",
          "#expenses-filter-year",
          "#incomes-filter-year",
          "#suppliers-filter-year",
          "#it-reports-f-year",
          "#ia-filter-single-day",
          "#ia-filter-period",
          "#ia-filter-month",
          "#ia-filter-year",
          "#sr-filter-year",
          "[data-portal-filter-panel='year']",
        ],
      };

      Object.keys(legacyMap).forEach(function (key) {
        var show = key === mode;
        legacyMap[key].forEach(function (sel) {
          form.querySelectorAll(sel).forEach(function (el) {
            if (el === modeEl) return;
            el.classList.toggle("hidden", !show);
          });
        });
      });
    }

    if (modeEl) modeEl.addEventListener("change", sync);
    if (rangeEl) rangeEl.addEventListener("change", sync);
    sync();
  }

  function setLoading(form, target, loading) {
    var toolbar = resolveToolbar(form);
    if (toolbar) toolbar.classList.toggle("portal-live-toolbar--loading", loading);
    if (target) {
      target.classList.toggle("portal-live-content--loading", loading);
      target.setAttribute("aria-busy", loading ? "true" : "false");
    }
  }

  function resolveResponseForm(form, doc) {
    if (!form || !doc) return null;
    return (
      doc.querySelector("form#" + form.id) ||
      doc.querySelector('form[data-live-filter="' + (form.getAttribute("data-live-filter") || "") + '"]')
    );
  }

  function swapLiveContent(target, fresh) {
    var nodes = Array.from(fresh.childNodes);
    if (typeof target.replaceChildren === "function") {
      target.replaceChildren.apply(target, nodes);
      return;
    }
    target.innerHTML = "";
    nodes.forEach(function (node) {
      target.appendChild(node);
    });
  }

  function mergeToolbarState(form, doc) {
    var newForm = resolveResponseForm(form, doc);
    if (!newForm || newForm.id !== form.id) return;
    form.querySelectorAll("select, input:not([type='hidden'])").forEach(function (el) {
      if (!el.name) return;
      var fresh = newForm.querySelector("[name='" + el.name + "']");
      if (!fresh || fresh === el) return;
      if (fresh.type === "checkbox") {
        el.checked = fresh.checked;
      } else {
        el.value = fresh.value;
      }
    });
    var chipHost = form.querySelector(".portal-filter-actions");
    var newChipHost = newForm.querySelector(".portal-filter-actions");
    if (chipHost && newChipHost) {
      chipHost.innerHTML = newChipHost.innerHTML;
    }
    syncModePanels(form);
  }

  function fetchAndSwap(form, options) {
    options = options || {};
    var params = buildParams(form, options.buildParams);
    if (global.itSupportAnalyticsFilter && typeof global.itSupportAnalyticsFilter.save === "function") {
      global.itSupportAnalyticsFilter.save(params);
    }

    var action = form.getAttribute("action") || global.location.pathname;
    var url = params.toString() ? action + "?" + params.toString() : action;
    var seq = (form._portalLiveSeq || 0) + 1;
    form._portalLiveSeq = seq;

    if (options.urlOnly) {
      global.history.replaceState({ portalLive: true }, "", url);
      if (typeof options.afterSwap === "function") {
        options.afterSwap(null, null, url);
      }
      document.dispatchEvent(
        new CustomEvent("portal-live-content-updated", {
          detail: { form: form, target: null, url: url },
        })
      );
      return Promise.resolve(true);
    }

    var target = options.target || resolveContentTarget(form);
    if (!target) {
      if (typeof form.requestSubmit === "function") form.requestSubmit();
      else form.submit();
      return Promise.resolve(false);
    }

    setLoading(form, target, true);

    return fetch(url, {
      credentials: "same-origin",
      headers: {
        Accept: "text/html",
        [FETCH_HEADER]: "1",
      },
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.text();
      })
      .then(function (html) {
        if (form._portalLiveSeq !== seq) return false;
        var doc = new DOMParser().parseFromString(html, "text/html");
        var selector =
          target.getAttribute("data-portal-live-content-id") ||
          target.id ||
          form.getAttribute("data-portal-live-content") ||
          "[data-portal-live-content]";
        var fresh =
          (target.id && doc.getElementById(target.id)) ||
          doc.querySelector(selector) ||
          doc.querySelector("[data-portal-live-content]");
        if (!fresh) throw new Error("Missing live content in response");
        swapLiveContent(target, fresh);
        mergeToolbarState(form, doc);
        if (typeof options.afterSwap === "function") {
          options.afterSwap(doc, html, url);
        }
        if (options.updateTitle !== false) {
          var newTitle = doc.querySelector("title");
          if (newTitle && newTitle.textContent) document.title = newTitle.textContent;
        }
        global.history.replaceState({ portalLive: true }, "", url);
        document.dispatchEvent(
          new CustomEvent("portal-live-content-updated", {
            detail: { form: form, target: target, url: url },
          })
        );
        return true;
      })
      .catch(function () {
        global.location.assign(url);
        return false;
      })
      .finally(function () {
        if (form._portalLiveSeq === seq) setLoading(form, target, false);
      });
  }

  function scheduleFetch(form, delay, options) {
    var timer = debounceByForm.get(form);
    if (timer) clearTimeout(timer);
    debounceByForm.set(
      form,
      setTimeout(function () {
        debounceByForm.delete(form);
        fetchAndSwap(form, options);
      }, delay == null ? 280 : delay)
    );
  }

  var ICON_FILTER =
    '<svg class="portal-filter-header__svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">' +
    '<path d="M4 6h16M7 12h10M10 18h4"/></svg>';
  var ICON_LIVE =
    '<svg class="portal-live-chip__svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M13 2L3 14h8l-1 8 10-12h-8l1-8z"/></svg>';

  function liveChipHtml() {
    return (
      '<span class="portal-live-chip" title="Filters apply automatically">' +
      ICON_LIVE +
      '<span class="portal-live-chip__text">Live</span>' +
      '<span class="portal-live-chip__dot" aria-hidden="true"></span>' +
      "</span>"
    );
  }

  function shouldSkipFilterHeader(form) {
    if (form.closest(".rev-toolbar")) return true;
    var prev = form.previousElementSibling;
    if (prev && prev.matches && prev.matches("h3.rc-section-title, .rc-section-title")) return true;
    var cardTitle = form.parentElement && form.parentElement.querySelector("h3.rc-section-title");
    if (cardTitle && form.parentElement.querySelector("h3.rc-section-title + form") === form) return true;
    return false;
  }

  function injectFilterHeader(form) {
    if (form.querySelector(".portal-filter-header") || shouldSkipFilterHeader(form)) return;
    var header = document.createElement("div");
    header.className = "portal-filter-header";
    header.innerHTML =
      '<div class="portal-filter-header__brand">' +
      '<span class="portal-filter-header__icon">' +
      ICON_FILTER +
      "</span>" +
      '<div class="portal-filter-header__copy">' +
      '<span class="portal-filter-header__title">Filters</span>' +
      '<span class="portal-filter-header__hint">Updates instantly as you change values</span>' +
      "</div></div>";
    var actions = form.querySelector(".portal-filter-actions");
    if (actions) {
      header.appendChild(actions);
    } else {
      var actionsWrap = document.createElement("div");
      actionsWrap.className = "portal-filter-actions";
      actionsWrap.innerHTML = liveChipHtml();
      header.appendChild(actionsWrap);
    }
    form.insertBefore(header, form.firstChild);
  }

  function upgradeLiveChips(root) {
    var chips = [];
    try {
      chips = Array.from(
        (root || document).querySelectorAll(".portal-live-chip:not(:has(.portal-live-chip__svg))")
      );
    } catch (e) {
      chips = Array.from((root || document).querySelectorAll(".portal-live-chip")).filter(function (chip) {
        return !chip.querySelector(".portal-live-chip__svg");
      });
    }
    chips.forEach(function (chip) {
      var dot = chip.querySelector(".portal-live-chip__dot");
      if (!chip.querySelector(".portal-live-chip__text")) {
        var text = document.createElement("span");
        text.className = "portal-live-chip__text";
        text.textContent = (chip.textContent || "Live").replace(/\s+/g, " ").trim() || "Live";
        chip.textContent = "";
        chip.insertAdjacentHTML("afterbegin", ICON_LIVE);
        chip.appendChild(text);
        if (dot) chip.appendChild(dot);
        else {
          var d = document.createElement("span");
          d.className = "portal-live-chip__dot";
          d.setAttribute("aria-hidden", "true");
          chip.appendChild(d);
        }
      }
    });
    (root || document).querySelectorAll(".rev-chip--live:not(.portal-live-chip)").forEach(function (chip) {
      chip.classList.add("portal-live-chip", "rev-chip--live");
      if (!chip.querySelector(".portal-live-chip__svg")) {
        var label = chip.textContent.replace(/\s+/g, " ").trim() || "Live";
        chip.innerHTML =
          ICON_LIVE +
          '<span class="portal-live-chip__text">' +
          label +
          '</span><span class="portal-live-chip__dot" aria-hidden="true"></span>';
      }
    });
  }

  function enhanceFilterChrome(form) {
    if (form.closest(".rev-toolbar")) {
      upgradeLiveChips(form.closest(".rev-toolbar"));
      return;
    }
    form.classList.add("portal-filter-shell--colorful");
    injectFilterHeader(form);
    upgradeLiveChips(form);
  }

  function shouldSkipForm(form) {
    if (!form || form.method.toLowerCase() !== "get") return true;
    if (form.dataset.portalLiveSkip === "true") return true;
    if (form.id === "it-analytics-filter-form" && document.getElementById("it-analytics-live-root") && global.IT_SUPPORT_ANALYTICS && global.IT_SUPPORT_ANALYTICS.dataUrl) {
      return true;
    }
    if (form.id === "receipt-scope-form" && document.getElementById("receipt-register-root")) return true;
    return false;
  }

  function modernizeLegacyForm(form) {
    if (form.closest(".rev-toolbar")) return;
    if (!form.classList.contains("portal-filter-toolbar--legacy")) {
      form.classList.add("portal-live-filter", "portal-filter-toolbar--legacy");
    }
    form.querySelectorAll('[type="submit"]').forEach(function (btn) {
      btn.classList.add("portal-live-hide-submit");
    });
    if (!form.querySelector(".portal-live-chip") && !form.querySelector(".portal-filter-header .portal-live-chip")) {
      var actions = document.createElement("div");
      actions.className = "portal-filter-actions";
      actions.innerHTML = liveChipHtml();
      form.appendChild(actions);
    }
    enhanceFilterChrome(form);
  }

  function wireForm(form, options) {
    if (shouldSkipForm(form)) return;
    if (form.dataset.portalLiveWired === "true") return;
    options = options || {};

    modernizeLegacyForm(form);
    if (form.closest(".rev-toolbar")) {
      enhanceFilterChrome(form);
    }
    syncModePanels(form);

    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      fetchAndSwap(form, options);
    });

    var modeEl = qs(form, "[data-portal-filter-mode], [name='mode']");
    var rangeEl = qs(form, "[name='range']");
    if (modeEl) {
      modeEl.addEventListener("change", function () {
        scheduleFetch(form, 80, options);
      });
    }
    if (rangeEl && rangeEl !== modeEl) {
      rangeEl.addEventListener("change", function () {
        scheduleFetch(form, 80, options);
      });
    }

    form.querySelectorAll("select").forEach(function (el) {
      if (el === modeEl || el === rangeEl) return;
      el.addEventListener("change", function () {
        scheduleFetch(form, 120, options);
      });
    });

    form.querySelectorAll('input[type="date"], input[type="month"], input[type="checkbox"]').forEach(function (el) {
      el.addEventListener("change", function () {
        scheduleFetch(form, el.type === "checkbox" ? 80 : undefined, options);
      });
    });

    form.querySelectorAll('input[type="number"]').forEach(function (el) {
      el.addEventListener("change", function () {
        scheduleFetch(form, undefined, options);
      });
      el.addEventListener("input", function () {
        scheduleFetch(form, 450, options);
      });
    });

    form.querySelectorAll(
      'input[type="search"], input[type="text"][name*="q"], input[type="text"][name*="search"], input[name="moved_by"], input[name="supplier_q"]'
    ).forEach(function (el) {
      if (el.getAttribute("data-portal-live-ignore") === "1") return;
      el.addEventListener("input", function () {
        scheduleFetch(form, 500, options);
      });
    });

    form.querySelectorAll("input[type='hidden'][data-portal-live-watch]").forEach(function (el) {
      el.addEventListener("change", function () {
        scheduleFetch(form, 80, options);
      });
    });

    form.querySelectorAll("[data-portal-set-mode]").forEach(function (btn) {
      btn.addEventListener("click", function (ev) {
        ev.preventDefault();
        if (!modeEl) return;
        modeEl.value = btn.getAttribute("data-portal-set-mode") || "single_day";
        syncModePanels(form);
        scheduleFetch(form, 80, options);
      });
    });

    form.dataset.portalLiveWired = "true";
  }

  function initAll() {
    var selectors = [
      'form[data-portal-live-filter]:not([data-live-filter])',
      ".sa-stock-live-filter",
      "#expenses-page form[method='get']",
      "#incomes-page form[method='get']",
      "#salaries-payroll-shell form[method='get']",
      "#leads-filter-form",
      "#leads-filter-form-it",
      "#suppliers-filter-form",
      "#item-analytics-filter-form",
      "#it-stock-reports-filter-form",
      "#hr-analytics-filter-form",
      "#ka-filter-form",
      ".kitchen-analytics-toolbar",
      "#stock-profitability-filter-form",
      "#stock-requests-audit-filter-form",
      "#stock-movement-filter-form",
      "#customer-tx-filter-form",
      "#supplier-payment-tx-filter-form",
      "#suppliers-grouped-filter-form",
      "#company-expenses-filter-form",
    ];

    var seen = new WeakSet();
    selectors.forEach(function (sel) {
      document.querySelectorAll(sel).forEach(function (form) {
        if (!(form instanceof HTMLFormElement) || seen.has(form)) return;
        seen.add(form);
        wireForm(form);
      });
    });
    upgradeLiveChips(document);
  }

  global.PortalLiveFilters = {
    wireForm: wireForm,
    fetchAndSwap: fetchAndSwap,
    scheduleFetch: scheduleFetch,
    syncModePanels: syncModePanels,
    upgradeLiveChips: upgradeLiveChips,
    enhanceFilterChrome: enhanceFilterChrome,
    initAll: initAll,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll);
  } else {
    initAll();
  }
})(window);
