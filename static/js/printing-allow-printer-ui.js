(function (global) {
  function syncAllowVsCompulsory(allows, comp) {
    if (!comp) return;
    var any = false;
    allows.forEach(function (el) {
      if (el.checked) any = true;
    });
    comp.disabled = !any;
    comp.title = any ? "" : "Turn on at least one allowed printer type to enable this option.";
    if (!any) comp.checked = false;
  }

  /**
   * Bluetooth (direct BLE) and USB (browser print dialog) are mutually exclusive.
   * Network may be enabled alongside either.
   */
  function initPrintingAllowPrinterUi(root) {
    root = root || document;
    var bt = root.querySelector('input[name="printing_allow_bluetooth"]');
    var usb = root.querySelector('input[name="printing_allow_usb"]');
    var comp = root.querySelector("input.js-printing-compulsory-sale");
    var allows = root.querySelectorAll("input.js-printing-allow-printer");
    if (!allows.length) return;

    function syncBtUsbExclusive(changedEl) {
      if (bt && usb) {
        if (changedEl === bt && bt.checked) {
          usb.checked = false;
        } else if (changedEl === usb && usb.checked) {
          bt.checked = false;
        } else if (bt.checked && usb.checked) {
          if (changedEl === usb) bt.checked = false;
          else usb.checked = false;
        }
        var btOn = bt.checked;
        var usbOn = usb.checked;
        bt.disabled = usbOn;
        usb.disabled = btOn;
        bt.title = usbOn
          ? "Turn off USB to enable Bluetooth (direct BLE printing)."
          : "";
        usb.title = btOn
          ? "Turn off Bluetooth to enable USB (browser print dialog)."
          : "";
      }
      syncAllowVsCompulsory(allows, comp);
    }

    allows.forEach(function (el) {
      el.addEventListener("change", function () {
        syncBtUsbExclusive(el);
      });
    });
    syncBtUsbExclusive(null);
  }

  global.initPrintingAllowPrinterUi = initPrintingAllowPrinterUi;
})(window);
