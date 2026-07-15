/**
 * Credit note statement filter — fast live apply (fragment + short debounce).
 */
(function () {
  var FETCH_HEADER = "X-Portal-Live-Filter";
  var debounceTimer = null;
  var fetchSeq = 0;

  function syncStatementPanels(form) {
    if (!form) return;
    var scopeEl = form.querySelector("[data-cn-statement-scope], [name='statement']");
    var modeEl = form.querySelector("[data-cn-filter-mode], [name='mode']");
    var periodWrap = form.querySelector("[data-cn-period-fields]");
    var isFull = !scopeEl || scopeEl.value === "full";

    if (periodWrap) {
      periodWrap.classList.toggle("hidden", isFull);
      if (isFull) periodWrap.setAttribute("hidden", "hidden");
      else periodWrap.removeAttribute("hidden");
    }
    if (modeEl) modeEl.disabled = isFull;

    var mode = (modeEl && modeEl.value) || "single_day";
    form.querySelectorAll("[data-cn-panel], [data-portal-filter-panel]").forEach(function (panel) {
      var panelMode =
        panel.getAttribute("data-cn-panel") ||
        panel.getAttribute("data-portal-filter-panel") ||
        "";
      var show = !isFull && panelMode === mode;
      panel.classList.toggle("hidden", !show);
      panel.querySelectorAll("input, select").forEach(function (inp) {
        if (inp === modeEl) return;
        inp.disabled = !show;
      });
    });
  }

  function buildParams(form) {
    var params = new URLSearchParams();
    ["customer_name", "customer_phone", "shop_id", "back_shop_id"].forEach(function (name) {
      var el = form.querySelector('[name="' + name + '"]');
      var val = el ? String(el.value || "").trim() : "";
      if (val) params.set(name, val);
    });

    var scopeEl = form.querySelector("[name='statement']");
    var isFull = !scopeEl || scopeEl.value === "full";
    params.set("statement", isFull ? "full" : "partial");
    params.set("live", "1");
    if (isFull) return params;

    var modeEl = form.querySelector("[name='mode']");
    var mode = modeEl ? String(modeEl.value || "single_day").trim() : "single_day";
    params.set("mode", mode);

    if (mode === "period") {
      var start = form.querySelector("[name='start_date']");
      var end = form.querySelector("[name='end_date']");
      if (start && start.value) params.set("start_date", start.value);
      if (end && end.value) params.set("end_date", end.value);
    } else if (mode === "month") {
      var month = form.querySelector("[name='month']");
      if (month && month.value) params.set("month", month.value);
    } else if (mode === "year") {
      var year = form.querySelector("[name='year']");
      if (year && year.value) params.set("year", year.value);
    } else {
      var day = form.querySelector("[name='single_day']");
      if (day && day.value) params.set("single_day", day.value);
    }
    return params;
  }

  function historyUrl(params) {
    var clean = new URLSearchParams(params.toString());
    clean.delete("live");
    var action = (document.getElementById("credit-note-statement-filter") || {}).getAttribute
      ? document.getElementById("credit-note-statement-filter").getAttribute("action")
      : window.location.pathname;
    var qs = clean.toString();
    return qs ? action + "?" + qs : action;
  }

  function setLoading(form, target, on) {
    if (form) form.classList.toggle("portal-live-toolbar--loading", on);
    if (target) {
      target.classList.toggle("portal-live-content--loading", on);
      target.setAttribute("aria-busy", on ? "true" : "false");
    }
  }

  function mergeFilterMeta(form, doc) {
    var fresh =
      doc.querySelector("form#credit-note-statement-filter") ||
      doc.querySelector('form[data-live-filter="credit-note-statement"]');
    if (!fresh) return;
    var chipHost = form.querySelector(".portal-filter-actions");
    var freshChips = fresh.querySelector(".portal-filter-actions");
    if (chipHost && freshChips) chipHost.innerHTML = freshChips.innerHTML;
    var hint = form.querySelector("[data-cn-statement-hint]");
    var freshHint = fresh.querySelector("[data-cn-statement-hint]");
    if (hint && freshHint) hint.innerHTML = freshHint.innerHTML;
    form.querySelectorAll("select, input:not([type='hidden'])").forEach(function (el) {
      if (!el.name) return;
      var src = fresh.querySelector("[name='" + el.name + "']");
      if (!src) return;
      if (el.type === "checkbox") el.checked = src.checked;
      else el.value = src.value;
    });
  }

  function preserveQr(target, fresh) {
    if (!target || !fresh) return;
    var oldQr = target.querySelector(".shop-credit-note__qr img");
    var newQr = fresh.querySelector(".shop-credit-note__qr img");
    if (!oldQr || !newQr) return;
    if ((oldQr.getAttribute("src") || "") === (newQr.getAttribute("src") || "")) {
      newQr.setAttribute("src", oldQr.currentSrc || oldQr.getAttribute("src") || "");
    }
  }

  function swapContent(target, fresh) {
    preserveQr(target, fresh);
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

  function fetchLive(form) {
    var target = document.getElementById("credit-note-live-content");
    if (!form || !target) return;
    syncStatementPanels(form);
    var params = buildParams(form);
    var action = form.getAttribute("action") || window.location.pathname;
    var url = action + "?" + params.toString();
    var pretty = historyUrl(params);
    var seq = ++fetchSeq;
    setLoading(form, target, true);

    fetch(url, {
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
        if (seq !== fetchSeq) return;
        var doc = new DOMParser().parseFromString(html, "text/html");
        var fresh = doc.getElementById("credit-note-live-content");
        if (!fresh) throw new Error("Missing live content");
        swapContent(target, fresh);
        mergeFilterMeta(form, doc);
        syncStatementPanels(form);
        history.replaceState({ creditNoteLive: true }, "", pretty);
      })
      .catch(function () {
        if (seq !== fetchSeq) return;
        window.location.assign(pretty);
      })
      .finally(function () {
        if (seq === fetchSeq) setLoading(form, target, false);
      });
  }

  function scheduleFetch(form, delay) {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(function () {
      debounceTimer = null;
      fetchLive(form);
    }, delay == null ? 90 : delay);
  }

  function wire(form) {
    if (!form || form.dataset.cnLiveWired === "true") return;
    form.dataset.cnLiveWired = "true";
    form.dataset.portalLiveWired = "true";
    form.dataset.portalLiveSkip = "true";

    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      scheduleFetch(form, 0);
    });

    form.addEventListener(
      "change",
      function (ev) {
        var t = ev.target;
        if (!t) return;
        if (t.name === "statement" || t.name === "mode") syncStatementPanels(form);
        scheduleFetch(form, t.name === "year" ? 250 : 70);
      },
      true
    );

    form.querySelectorAll('input[type="number"][name="year"]').forEach(function (el) {
      el.addEventListener("input", function () {
        scheduleFetch(form, 320);
      });
    });

    syncStatementPanels(form);
  }

  function init() {
    wire(document.getElementById("credit-note-statement-filter"));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
