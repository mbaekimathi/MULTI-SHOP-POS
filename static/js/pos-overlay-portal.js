/**
 * Move POS fixed overlays to a top-level portal on <body> so dialogs are not
 * trapped behind catalog/header stacking contexts.
 */
(function () {
  "use strict";

  var OVERLAY_SELECTORS = [
    "#pos-toast",
    "#pos-sync-toast",
    "#pos-day-opening-modal-backdrop",
    "#pos-day-opening-modal",
    "#pos-compulsory-printer-modal-backdrop",
    "#pos-compulsory-printer-modal",
    "#pos-cart-backdrop",
    "#pos-cart-drawer",
    "#pos-printer-backdrop",
    "#pos-printer-dialog",
    "#pos-hold-modal",
    "#pos-hold-void-modal",
    "#pos-hold-saved-modal",
    "#pos-stockin-modal",
    "#pos-refill-portions-modal",
    "#pos-receipts-modal",
    "#pos-incoming-sr-modal",
    "#shop-sr-outcome-modal",
    "#shop-day-closing-reminder-modal",
  ];

  function init() {
    var portal = document.getElementById("pos-overlay-root");
    if (!portal) {
      portal = document.createElement("div");
      portal.id = "pos-overlay-root";
      portal.className = "pos-overlay-root";
      portal.setAttribute("aria-hidden", "true");
      document.body.appendChild(portal);
    }

    OVERLAY_SELECTORS.forEach(function (selector) {
      var node = document.querySelector(selector);
      if (node && node.parentNode !== portal) {
        portal.appendChild(node);
      }
    });

    var cartBackdrop = document.getElementById("pos-cart-backdrop");
    if (cartBackdrop && !cartBackdrop.classList.contains("is-visible")) {
      cartBackdrop.style.pointerEvents = "none";
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
