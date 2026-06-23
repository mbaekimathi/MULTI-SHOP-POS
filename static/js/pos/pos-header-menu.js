/**
 * POS header — legacy menu hook (toolbar now always visible).
 * Keeps printer/network in the toolbar system group.
 */
(function () {
  "use strict";

  function ensureSystemGroup() {
    var systemGroup = document.querySelector(".pos-header-toolbar__group--system");
    var printer = document.getElementById("pos-printer-open");
    var network = document.getElementById("pos-network-state-indicator");
    if (!systemGroup) return;
    if (printer && printer.parentElement !== systemGroup) systemGroup.appendChild(printer);
    if (network && network.parentElement !== systemGroup) systemGroup.appendChild(network);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensureSystemGroup);
  } else {
    ensureSystemGroup();
  }
})();
