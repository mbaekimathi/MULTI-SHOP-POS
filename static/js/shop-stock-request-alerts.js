(function () {
  var shopId = window.__SHOP_STOCK_ALERT_SHOP_ID;
  var API = window.__SHOP_STOCK_ALERTS_API;
  if (!shopId || !API) return;

  var LS_KEY = "shop_sr_outcome_seen_" + shopId;
  var SEEDED_KEY = "shop_sr_outcome_seeded_" + shopId;
  var POLL_MS = 8000;
  var modal = document.getElementById("shop-sr-outcome-modal");
  var titleEl = document.getElementById("shop-sr-outcome-title");
  var messageEl = document.getElementById("shop-sr-outcome-message");
  var iconEl = document.getElementById("shop-sr-outcome-icon");
  var linkEl = document.getElementById("shop-sr-outcome-link");
  var dismissBtn = document.getElementById("shop-sr-outcome-dismiss");
  var backdrop = document.getElementById("shop-sr-outcome-backdrop");

  var queue = [];
  var current = null;
  var seeded = false;

  function getSeenId() {
    try {
      return parseInt(localStorage.getItem(LS_KEY) || "0", 10) || 0;
    } catch (e) {
      return 0;
    }
  }

  function setSeenId(id) {
    try {
      localStorage.setItem(LS_KEY, String(id));
    } catch (e) {}
  }

  function hasSeeded() {
    if (seeded) return true;
    try {
      seeded = localStorage.getItem(SEEDED_KEY) === "1";
    } catch (e) {
      seeded = false;
    }
    return seeded;
  }

  function markSeeded() {
    seeded = true;
    try {
      localStorage.setItem(SEEDED_KEY, "1");
    } catch (e) {}
  }

  function setOpen(on) {
    if (!modal) return;
    modal.classList.toggle("hidden", !on);
    modal.setAttribute("aria-hidden", on ? "false" : "true");
  }

  function applyKind(kind) {
    if (!iconEl) return;
    var bad = kind === "cancelled" || kind === "expired";
    iconEl.className =
      "flex h-10 w-10 shrink-0 items-center justify-center rounded-xl " +
      (bad ? "bg-rose-500/15 text-rose-400" : "bg-emerald-500/15 text-emerald-400");
    iconEl.innerHTML = bad
      ? '<svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>'
      : '<svg class="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>';
  }

  function playAlertTone() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      var ctx = new Ctx();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.type = "sine";
      o.frequency.value = 660;
      g.gain.value = 0.08;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      o.stop(ctx.currentTime + 0.14);
    } catch (e) {}
  }

  function showNext() {
    if (current) return;
    if (!queue.length) {
      setOpen(false);
      return;
    }
    current = queue.shift();
    if (titleEl) titleEl.textContent = current.title || "Stock request update";
    if (messageEl) messageEl.textContent = current.message || "";
    applyKind(current.kind);
    if (linkEl) {
      if (current.link_url) {
        linkEl.href = current.link_url;
        linkEl.classList.remove("hidden");
      } else {
        linkEl.classList.add("hidden");
      }
    }
    setOpen(true);
    playAlertTone();
  }

  function dismissCurrent() {
    if (current && current.id > getSeenId()) {
      setSeenId(current.id);
    }
    current = null;
    showNext();
  }

  function enqueueNew(alerts) {
    var list = (alerts || [])
      .filter(function (a) {
        return a && a.id;
      })
      .sort(function (a, b) {
        return a.id - b.id;
      });
    if (!list.length) {
      if (!hasSeeded()) markSeeded();
      return;
    }

    // First visit with no prior history: remember the latest id so old alerts
    // do not flood. Later polls only surface newer ids.
    if (!hasSeeded() && getSeenId() <= 0) {
      var maxId = list[list.length - 1].id;
      setSeenId(maxId);
      markSeeded();
      return;
    }
    if (!hasSeeded()) markSeeded();

    var seen = getSeenId();
    var fresh = list.filter(function (a) {
      return a.id > seen;
    });
    if (!fresh.length) return;

    var queuedIds = {};
    queue.forEach(function (q) {
      queuedIds[q.id] = true;
    });
    if (current) queuedIds[current.id] = true;

    fresh.forEach(function (a) {
      if (!queuedIds[a.id]) queue.push(a);
    });
    showNext();
  }

  function poll() {
    fetch(API, { credentials: "same-origin", headers: { Accept: "application/json" }, cache: "no-store" })
      .then(function (res) {
        if (!res.ok) return null;
        return res.json();
      })
      .then(function (data) {
        if (!data || !data.ok) return;
        enqueueNew(data.alerts || []);
      })
      .catch(function () {});
  }

  dismissBtn && dismissBtn.addEventListener("click", dismissCurrent);
  backdrop && backdrop.addEventListener("click", dismissCurrent);

  document.addEventListener("keydown", function (e) {
    if (e.key !== "Escape") return;
    if (!modal || modal.classList.contains("hidden")) return;
    dismissCurrent();
  });

  setTimeout(poll, 800);
  setInterval(poll, POLL_MS);

  // Resume quickly when cashier returns to the POS tab.
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") poll();
  });
})();
