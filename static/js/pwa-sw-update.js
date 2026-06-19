/**
 * Shop PWA — service worker registration + "update available" refresh prompt.
 */
(function () {
  "use strict";

  if (!("serviceWorker" in navigator)) return;
  if (window.__rcSwRegistered) return;
  window.__rcSwRegistered = true;

  var pendingReload = false;
  var waitingWorker = null;
  var registrationRef = null;

  function isShopSession() {
    return document.documentElement.getAttribute("data-pwa-install") === "shop";
  }

  function isPosPage() {
    return /\/shops\/\d+\/shop-pos\b/.test(window.location.pathname);
  }

  function activateWaitingWorker() {
    if (!waitingWorker) return;
    pendingReload = true;
    try {
      waitingWorker.postMessage("SKIP_WAITING");
    } catch (e) {
      /* ignore */
    }
  }

  function hideUpdateBanner() {
    var bar = document.getElementById("pwa-update-banner");
    if (!bar) return;
    bar.classList.remove("pwa-update-banner--show");
    setTimeout(function () {
      if (bar.parentNode) bar.parentNode.removeChild(bar);
    }, 280);
  }

  function showUpdateBanner() {
    if (!isShopSession() || document.getElementById("pwa-update-banner")) return;

    var bar = document.createElement("div");
    bar.id = "pwa-update-banner";
    bar.className = "pwa-update-banner";
    bar.setAttribute("role", "status");
    bar.setAttribute("aria-live", "polite");

    var title = isPosPage()
      ? "Update ready"
      : "New version available";
    var lead = isPosPage()
      ? "Refresh when your current sale is finished to load the latest till version."
      : "Refresh to load the latest shop app version.";

    bar.innerHTML =
      '<div class="pwa-update-banner__icon" aria-hidden="true">' +
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h5M20 20v-5h-5"/>' +
          '<path stroke-linecap="round" stroke-linejoin="round" d="M20.49 9A9 9 0 0 0 5.64 5.64L4 4m16 16l-1.64-1.64A9 9 0 0 1 3.51 15"/>' +
        "</svg>" +
      "</div>" +
      '<div class="pwa-update-banner__body">' +
        '<p class="pwa-update-banner__title">' + title + "</p>" +
        '<p class="pwa-update-banner__lead">' + lead + "</p>" +
      "</div>" +
      '<div class="pwa-update-banner__actions">' +
        '<button type="button" class="pwa-update-banner__btn pwa-update-banner__btn--primary" data-pwa-update-refresh>Refresh now</button>' +
        '<button type="button" class="pwa-update-banner__btn pwa-update-banner__btn--ghost" data-pwa-update-dismiss>Later</button>' +
      "</div>";

    bar.addEventListener("click", function (e) {
      if (e.target.closest("[data-pwa-update-refresh]")) {
        hideUpdateBanner();
        activateWaitingWorker();
        return;
      }
      if (e.target.closest("[data-pwa-update-dismiss]")) {
        hideUpdateBanner();
      }
    });

    document.body.appendChild(bar);
    requestAnimationFrame(function () {
      bar.classList.add("pwa-update-banner--show");
    });
  }

  function noteWaitingWorker(worker) {
    if (!worker || !navigator.serviceWorker.controller) return;
    waitingWorker = worker;
    showUpdateBanner();
  }

  function watchRegistration(reg) {
    registrationRef = reg;

    if (reg.waiting && navigator.serviceWorker.controller) {
      noteWaitingWorker(reg.waiting);
    }

    reg.addEventListener("updatefound", function () {
      var installing = reg.installing;
      if (!installing) return;
      installing.addEventListener("statechange", function () {
        if (installing.state !== "installed") return;
        if (navigator.serviceWorker.controller) {
          noteWaitingWorker(reg.waiting || installing);
          return;
        }
        try {
          installing.postMessage("SKIP_WAITING");
        } catch (e) {
          /* ignore first-install activation */
        }
      });
    });
  }

  navigator.serviceWorker.addEventListener("controllerchange", function () {
    if (!pendingReload) return;
    window.location.reload();
  });

  function register() {
    navigator.serviceWorker
      .register("/pos-sw.js", { scope: "/" })
      .then(function (reg) {
        if (!reg) return;
        watchRegistration(reg);
        return reg.update();
      })
      .catch(function () {});
  }

  if (document.readyState === "loading") {
    window.addEventListener("load", register, { once: true });
  } else {
    register();
  }

  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState !== "visible" || !registrationRef) return;
    registrationRef.update().catch(function () {});
  });
})();
