(function () {
  window.POS_RECEIPT_PRINTER_CONFIGURED = false;
  try {
    if (window.__updatePosCompulsoryPrinterWorkspaceLock) window.__updatePosCompulsoryPrinterWorkspaceLock();
  } catch (eLock0) {}
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var PRINTER_API = BOOT.apis.printer;
  var SCAN_API = BOOT.apis.printerScanNetwork;
  var SCAN_STREAM_API = BOOT.apis.printerScanNetworkStream;
  var PRINT_AGENT_STATUS_API = BOOT.apis.printAgentStatus;
  var PRINT_AGENT_CONFIGURE_API = BOOT.apis.printAgentConfigure;
  var TCP_REACHABLE_API = BOOT.apis.printerTcpReachable;
  var LS_KEY = "richcom-shop-pos-printer-local-" + SHOP_ID;
var dlg = document.getElementById("pos-printer-dialog");
      var backdrop = document.getElementById("pos-printer-backdrop");
      var closeBtn = document.getElementById("pos-printer-close");
      var stepType = document.getElementById("pos-printer-step-type");
      var panelBt = document.getElementById("pos-printer-panel-bluetooth");
      var panelNet = document.getElementById("pos-printer-panel-network");
      var panelUsb = document.getElementById("pos-printer-panel-usb");
      var savedBox = document.getElementById("pos-printer-saved");
      var msgEl = document.getElementById("pos-printer-msg");

      var btPending = null;
      var usbPending = null;
      var netScanPort = 9100;
      var printerUiOpen = false;

      var USB_FILTERS = [
        { classCode: 7 },
        { vendorId: 0x04b8 },
        { vendorId: 0x04e8 },
        { vendorId: 0x0519 },
        { vendorId: 0x0fe6 },
        { vendorId: 0x0483 },
        { vendorId: 0x0416 },
        { vendorId: 0x2730 },
        { vendorId: 0x28e9 },
        { vendorId: 0x154f },
        { vendorId: 0x0922 },
        { vendorId: 0x067b },
        { vendorId: 0x1a86 },
        { vendorId: 0x0403 },
        { vendorId: 0x1fc9 },
        { vendorId: 0x6868 },
        { vendorId: 0x0dd4 },
        { vendorId: 0x0525 },
        { vendorId: 0x1504 },
      ];

      /** Matches IT / shop printing settings: which receipt connection types staff may use. */
      function posPrinterAllowedKindsInSettings() {
        var out = [];
        if (window.posPrinterTypeAllowed(null, "bluetooth")) out.push("bluetooth");
        if (window.posPrinterTypeAllowed(null, "network")) out.push("network");
        if (window.posPrinterTypeAllowed(null, "usb")) out.push("usb");
        return out;
      }

      /** IT toggles only — never infer allowed types from DOM or a saved USB/BT profile. */
      function printerKindsForSetupNavigation() {
        return posPrinterAllowedKindsInSettings();
      }

      function firstAllowedPrinterKind() {
        var kinds = printerKindsForSetupNavigation();
        return kinds.length ? kinds[0] : "";
      }

      function msg(t, tone) {
        if (!msgEl) return;
        msgEl.textContent = t || "";
        msgEl.classList.remove("text-rose-600", "dark:text-rose-400", "text-emerald-700", "dark:text-emerald-300");
        if (tone === "error") {
          msgEl.classList.add("text-rose-600", "dark:text-rose-400");
        } else if (tone === "ok") {
          msgEl.classList.add("text-emerald-700", "dark:text-emerald-300");
        }
      }

      function applyPrinterTypeAllowList() {
        document.querySelectorAll(".pos-printer-type-btn").forEach(function (btn) {
          var t = btn.getAttribute("data-printer-type");
          if (!t) return;
          var allow = window.posPrinterTypeAllowed(null, t);
          btn.classList.toggle("hidden", !allow);
          btn.classList.toggle("pos-printer-type-blocked", !allow);
          btn.disabled = !allow;
          if (allow) {
            btn.removeAttribute("aria-hidden");
            btn.removeAttribute("tabindex");
          } else {
            btn.setAttribute("aria-hidden", "true");
            btn.setAttribute("tabindex", "-1");
          }
        });
        if (stepType) {
          var kinds = posPrinterAllowedKindsInSettings();
          stepType.classList.toggle("hidden", kinds.length <= 1);
        }
        if (panelUsb) {
          panelUsb.classList.toggle("hidden", !window.posPrinterTypeAllowed(null, "usb"));
        }
        syncPrinterIntroCopy();
        syncPrinterSingleModeBackLinks();
      }

      function syncPrinterIntroCopy() {
        var el = document.getElementById("pos-printer-intro-checkout");
        if (!el) return;
        var kinds = posPrinterAllowedKindsInSettings();
        if (kinds.length === 1 && kinds[0] === "usb") {
          el.innerHTML =
            '<strong class="font-semibold text-[rgb(var(--rc-page-fg))]">USB is enabled for this shop.</strong> ' +
            "Plug in the printer, tap <strong class=\"font-semibold text-[rgb(var(--rc-page-fg))]\">Choose USB printer</strong> below, and allow Chrome access. " +
            "Receipts print silently over USB when pairing succeeds; otherwise the browser print dialog opens (pick <strong class=\"font-semibold text-[rgb(var(--rc-page-fg))]\">POS-80C</strong> / 80&nbsp;mm).";
          return;
        }
        if (kinds.length === 1 && kinds[0] === "network") {
          el.innerHTML =
            '<strong class="font-semibold text-[rgb(var(--rc-page-fg))]">Network printing is enabled.</strong> ' +
            "Save the printer IP below; receipts are sent as ESC/POS on port 9100 from this server.";
          return;
        }
        if (kinds.length === 1 && kinds[0] === "bluetooth") {
          el.innerHTML =
            '<strong class="font-semibold text-[rgb(var(--rc-page-fg))]">Bluetooth is enabled.</strong> ' +
            "Use <strong class=\"font-semibold text-[rgb(var(--rc-page-fg))]\">Choose printer</strong> and <strong class=\"font-semibold text-[rgb(var(--rc-page-fg))]\">Connect printer</strong> until the status shows linked.";
          return;
        }
      }

      function savedPrinterTypeAllowed(p) {
        if (!p) return false;
        var t = String(p.printer_type || "").toLowerCase();
        return posPrinterAllowedKindsInSettings().indexOf(t) >= 0;
      }

      function normalizePrinterConfig(cfg) {
        if (!cfg) return {};
        if (typeof cfg === "object") return cfg;
        if (typeof cfg === "string") {
          try {
            return JSON.parse(cfg) || {};
          } catch (e) {
            return {};
          }
        }
        return {};
      }

      function openPrinterPanelForType(t, opts) {
        opts = opts || {};
        t = String(t || "").toLowerCase();
        if (!window.posPrinterTypeAllowed(null, t)) {
          msg("That printer type is disabled in IT settings.");
          return;
        }
        if (t === "bluetooth") {
          showPanel("bluetooth");
          if (!opts.fromSaved) resetBluetoothPanel();
          reflectBluetoothPrinterModal();
          if (!opts.fromSaved && opts.autoPair) runBluetoothPairOrResumeFromUi();
          else if (!opts.fromSaved) msg("");
        } else if (t === "network") {
          showPanel("network");
          if (opts.autoScan !== false) runNetworkScanAuto();
        } else if (t === "usb") {
          showPanel("usb");
          reflectUsbPrinterModal();
        }
      }

      var compulsoryPrinterConnectFlow = false;

      /**
       * Compulsory printing on + printer not ready: lock catalog, show gate, open printer setup.
       * opts.fromLoad — when true, re-runs after refreshSaved even if already queued once.
       */
      function runCompulsoryPrinterConnectPrompt(opts) {
        opts = opts || {};
        if (typeof window.__posPrintingCompulsoryOnSaleEnabled !== "function" || !window.__posPrintingCompulsoryOnSaleEnabled()) {
          return;
        }
        if (window.POS_RECEIPT_PRINTER_CONFIGURED) {
          window.__posCompulsorySetupPromptQueued = false;
          window.__posCompulsorySetupOpened = false;
          document.body.classList.remove("pos-compulsory-setup-open");
          return;
        }
        try {
          if (window.__updatePosCompulsoryPrinterWorkspaceLock) window.__updatePosCompulsoryPrinterWorkspaceLock();
        } catch (eLock) {}
        if (!opts.force) {
          if (printerUiOpen) return;
          if (window.__posCompulsorySetupOpened && !opts.fromLoad) return;
          if (window.__posCompulsorySetupPromptQueued && !opts.fromLoad) return;
        }
        window.__posCompulsorySetupPromptQueued = true;
        clearTimeout(window.__posCompulsorySetupPromptTimer);
        var delayMs = opts.fromLoad ? 400 : 300;
        window.__posCompulsorySetupPromptTimer = setTimeout(function () {
          if (window.POS_RECEIPT_PRINTER_CONFIGURED) return;
          if (!window.__posPrintingCompulsoryOnSaleEnabled()) return;
          if (!opts.force && printerUiOpen) return;
          window.__posCompulsorySetupOpened = true;
          compulsoryPrinterConnectFlow = true;
          document.body.classList.add("pos-compulsory-setup-open");
          try {
            if (window.__posCompulsoryPrinterModalHide) window.__posCompulsoryPrinterModalHide();
          } catch (eHide) {}
          setPrinterOpen(true);
        }, delayMs);
      }

      function ensureCompulsoryPrinterPromptAfterLoad() {
        runCompulsoryPrinterConnectPrompt({ fromLoad: true, force: true });
      }

      function navigatePrinterSetupAfterRefresh() {
        if (!printerUiOpen) return;
        applyPrinterTypeAllowList();
        var kinds = printerKindsForSetupNavigation();
        var p = window.__POS_LAST_HEADER_PRINTER;
        var forceConnect = !!compulsoryPrinterConnectFlow;
        if (p && savedPrinterTypeAllowed(p)) {
          var t = String(p.printer_type || "").toLowerCase();
          var sgBt = window.__POS_BT || {};
          var autoPairBt = t === "bluetooth" && (forceConnect || !sgBt.gattConnected);
          openPrinterPanelForType(t, { fromSaved: true, autoScan: t === "network", autoPair: autoPairBt });
          compulsoryPrinterConnectFlow = false;
          return;
        }
        if (p && !savedPrinterTypeAllowed(p)) {
          msg(
            "Saved printer is " +
              String(p.printer_type || "unknown") +
              ", but IT settings only allow " +
              (kinds.length ? kinds.join(" / ") : "none") +
              ". Tap Forget saved printer, then set up an allowed type."
          );
        }
        if (!kinds.length) {
          msg("No printer connection types are enabled in IT settings.", "error");
          showTypeChooser();
          compulsoryPrinterConnectFlow = false;
          return;
        }
        if (kinds.length !== 1) {
          showTypeChooser();
          compulsoryPrinterConnectFlow = false;
          return;
        }
        openPrinterPanelForType(kinds[0], {
          fromSaved: false,
          autoPair: kinds[0] === "bluetooth",
        });
        compulsoryPrinterConnectFlow = false;
      }

      function setPrinterOpen(on) {
        printerUiOpen = !!on;
        if (!dlg || !backdrop) return;
        if (on) {
          var compulsoryNeedsPrinter =
            typeof window.__posPrintingCompulsoryOnSaleEnabled === "function" &&
            window.__posPrintingCompulsoryOnSaleEnabled() &&
            !window.POS_RECEIPT_PRINTER_CONFIGURED;
          if (compulsoryNeedsPrinter) {
            document.body.classList.add("pos-compulsory-setup-open");
            try {
              if (window.__posCompulsoryPrinterModalHide) window.__posCompulsoryPrinterModalHide();
            } catch (eHide2) {}
          }
          applyPrinterTypeAllowList();
          backdrop.classList.remove("hidden");
          dlg.classList.remove("hidden");
          document.body.style.overflow = "hidden";
          refreshSaved().then(function () {
            navigatePrinterSetupAfterRefresh();
          });
        } else {
          stopNetScanStream();
          if (usbPending && navigator.usb && usbPending.opened) {
            usbPending.close().catch(function () {});
            usbPending = null;
          }
          backdrop.classList.add("hidden");
          dlg.classList.add("hidden");
          document.body.classList.remove("pos-compulsory-setup-open");
          msg("");
          try {
            if (window.__updatePosCompulsoryPrinterWorkspaceLock) window.__updatePosCompulsoryPrinterWorkspaceLock();
          } catch (eRelock) {}
        }
      }

      function showTypeChooser() {
        applyPrinterTypeAllowList();
        if (stepType) {
          var kinds = printerKindsForSetupNavigation();
          if (kinds.length <= 1) {
            if (kinds.length === 1) openPrinterPanelForType(kinds[0], { fromSaved: false });
            return;
          }
          stepType.classList.remove("hidden");
        }
        if (panelBt) panelBt.classList.add("hidden");
        if (panelNet) panelNet.classList.add("hidden");
        if (panelUsb) panelUsb.classList.add("hidden");
        syncPrinterSingleModeBackLinks();
      }

      function syncPrinterSingleModeBackLinks() {
        var hideBack = printerKindsForSetupNavigation().length <= 1;
        document.querySelectorAll(".pos-printer-back").forEach(function (btn) {
          btn.classList.toggle("hidden", !!hideBack);
        });
      }

      function showPanel(kind) {
        kind = String(kind || "").toLowerCase();
        if (!window.posPrinterTypeAllowed(null, kind)) {
          var fallback = firstAllowedPrinterKind();
          if (!fallback) {
            showTypeChooser();
            return;
          }
          kind = fallback;
        }
        if (stepType) stepType.classList.add("hidden");
        if (panelBt) panelBt.classList.toggle("hidden", kind !== "bluetooth");
        if (panelNet) panelNet.classList.toggle("hidden", kind !== "network");
        if (panelUsb) {
          panelUsb.classList.toggle("hidden", kind !== "usb" || !window.posPrinterTypeAllowed(null, "usb"));
        }
        if (kind === "network") {
          refreshPrintAgentStatus();
        }
        if (kind === "bluetooth") reflectBluetoothPrinterModal();
        if (kind === "usb") reflectUsbPrinterModal();
        syncPrinterSingleModeBackLinks();
      }

      function resetBluetoothPanel() {
        var picked = document.getElementById("pos-printer-bt-picked");
        if (picked) {
          picked.classList.add("hidden");
          picked.textContent = "";
        }
      }

      function resetUsbPanel() {
        var retry = document.getElementById("pos-printer-usb-retry");
        var picked = document.getElementById("pos-printer-usb-picked");
        if (retry) retry.classList.add("hidden");
        if (picked) {
          picked.classList.add("hidden");
          picked.textContent = "";
        }
        reflectUsbPrinterModal();
      }

      function reflectUsbPrinterModal() {
        var dot = document.getElementById("pos-printer-usb-status-dot");
        var label = document.getElementById("pos-printer-usb-status-label");
        var pickBtn = document.getElementById("pos-printer-usb-pick");
        var pickedEl = document.getElementById("pos-printer-usb-picked");
        var retryEl = document.getElementById("pos-printer-usb-retry");
        var disabledHint = document.getElementById("pos-printer-usb-disabled-hint");
        var usbAllowed = window.posPrinterTypeAllowed(null, "usb");

        if (disabledHint) disabledHint.classList.toggle("hidden", usbAllowed);
        if (pickBtn) {
          pickBtn.disabled = !usbAllowed;
          pickBtn.classList.toggle("opacity-50", !usbAllowed);
          pickBtn.classList.toggle("cursor-not-allowed", !usbAllowed);
        }

        function greyState(text) {
          if (!dot || !label) return;
          dot.className =
            "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-slate-400 ring-2 ring-[rgb(var(--rc-surface))]";
          label.textContent = text;
        }

        if (!usbAllowed) {
          greyState("USB printing is disabled in settings.");
          if (pickBtn) pickBtn.textContent = "USB not allowed";
          return;
        }

        if (!window.isSecureContext) {
          greyState("USB needs HTTPS or localhost (127.0.0.1).");
          if (pickBtn) pickBtn.textContent = "Choose USB printer…";
          return;
        }
        if (!navigator.usb) {
          greyState("WebUSB is not available — use Chrome or Edge.");
          if (pickBtn) pickBtn.textContent = "Choose USB printer…";
          return;
        }

        var pRow = window.__POS_LAST_HEADER_PRINTER || null;
        var hasSavedUsb =
          !!(
            window.__POS_LAST_HEADER_CONNECTED &&
            pRow &&
            String(pRow.printer_type || "").toLowerCase() === "usb"
          );

        if (pickBtn) {
          pickBtn.textContent = hasSavedUsb ? "Re-select USB printer…" : "Choose USB printer…";
        }

        if (!hasSavedUsb) {
          greyState("No USB printer saved for this shop yet.");
          if (pickedEl) {
            pickedEl.classList.add("hidden");
            pickedEl.textContent = "";
          }
          return;
        }

        var cfg = normalizePrinterConfig(pRow.config);
        var vid = parseInt(cfg.vendorId, 10);
        var pid = parseInt(cfg.productId, 10);
        var name = ((pRow.device_label || "") + "").trim() || "USB printer";
        var idLine =
          !isNaN(vid) && !isNaN(pid)
            ? "VID " + vid.toString(16) + " · PID " + pid.toString(16)
            : "";

        if (pickedEl) {
          pickedEl.textContent = "Saved: " + name + (idLine ? " (" + idLine + ")" : "");
          pickedEl.classList.remove("hidden");
        }
        if (retryEl) retryEl.classList.add("hidden");

        if (isNaN(vid) || isNaN(pid)) {
          greyState("Saved USB profile is invalid — choose the printer again.");
          return;
        }

        navigator.usb
          .getDevices()
          .then(function (devices) {
            var plugged = devices.some(function (d) {
              return d && d.vendorId === vid && d.productId === pid;
            });
            if (!dot || !label) return;
            if (plugged) {
              dot.className =
                "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-emerald-500 shadow-[0_0_10px_-3px_rgba(34,197,94,0.9)] ring-2 ring-emerald-400/65";
              label.textContent =
                'Saved — “' +
                name +
                '” is plugged in. Checkout uses browser print (POS-80C) unless silent USB is available.';
            } else {
              dot.className =
                "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-amber-500 ring-2 ring-[rgb(var(--rc-surface))]";
              label.textContent =
                'Saved — “' +
                name +
                '” not detected. Plug in USB and tap Re-select USB printer to grant access again.';
            }
          })
          .catch(function () {
            greyState("Could not check USB devices.");
          });
      }

      function pickBluetoothAndSave() {
        var pickedEl = document.getElementById("pos-printer-bt-picked");
        function requestBluetoothPrinterDeviceCompat() {
          if (!navigator.bluetooth || typeof navigator.bluetooth.requestDevice !== "function") {
            return Promise.reject(new Error("Web Bluetooth picker unavailable"));
          }
          // Keep requestDevice synchronous inside the user click gesture.
          try {
            var svcList = (window.KNOWN_BLE_PRINT_SERVICE_UUIDS || []).slice();
            return navigator.bluetooth.requestDevice({
              acceptAllDevices: true,
              optionalServices: ["generic_access", "device_information"].concat(svcList),
            });
          } catch (e) {
            // Fallback for browsers that reject large/strict service lists.
            return navigator.bluetooth.requestDevice({
              acceptAllDevices: true,
              optionalServices: ["generic_access", "device_information"],
            });
          }
        }
        msg("");
        if (!window.isSecureContext) {
          msg("Open this POS with HTTPS or localhost to use Bluetooth.");
          return;
        }
        if (!navigator.bluetooth || typeof navigator.bluetooth.requestDevice !== "function") {
          msg("Use the latest Chrome or Edge for Bluetooth printers.");
          return;
        }
        msg("Open your printer's Bluetooth pairing mode, then pick it in the browser dialog.");
        var pickerPromise = requestBluetoothPrinterDeviceCompat();
        pickerPromise.finally(function () {
          if (typeof window.posSuppressCatalogGhostClicks === "function") window.posSuppressCatalogGhostClicks(600);
        });
        pickerPromise
          .then(function (device) {
            btPending = device;
            if (pickedEl) {
              pickedEl.textContent = (device.name || device.id || "Device") + " — connecting…";
              pickedEl.classList.remove("hidden");
            }
            msg("Saving paired printer…");
            var label = device.name || "Bluetooth printer";
            var baseCfg = {
              bluetoothId: device.id,
              name: device.name || "",
            };
            // Save immediately after picker selection so the app always has a printer row.
            return apiSave("bluetooth", label, baseCfg).then(function () {
              // Best-effort: enrich saved config with BLE endpoint for faster prints.
              // Do not fail setup if endpoint discovery fails here.
              if (!device.gatt) return;
              var btConn =
                typeof window.bluetoothGattConnect === "function" ? window.bluetoothGattConnect : null;
              var connectP =
                device.gatt.connected ? Promise.resolve() : btConn ? btConn(device) : Promise.resolve();
              return connectP
                .catch(function () {})
                .then(function () {
                  if (!device.gatt || !device.gatt.connected) return;
                  var discoverWritable =
                    typeof window.findWritableEscPosCharacteristic === "function"
                      ? window.findWritableEscPosCharacteristic
                      : null;
                  if (!discoverWritable) return;
                  return discoverWritable(device.gatt).then(function (endpoint) {
                    if (!endpoint || !endpoint.characteristic) return;
                    var serviceUuid = endpoint.service && endpoint.service.uuid ? String(endpoint.service.uuid) : "";
                    var characteristicUuid =
                      endpoint.characteristic && endpoint.characteristic.uuid
                        ? String(endpoint.characteristic.uuid)
                        : "";
                    var richCfg = {
                      bluetoothId: device.id,
                      name: device.name || "",
                      serviceUuid: serviceUuid,
                      characteristicUuid: characteristicUuid,
                    };
                    return apiSave("bluetooth", label, richCfg).catch(function () {});
                  }).catch(function () {});
                });
            });
          })
          .then(function () {
            var d = btPending;
            btPending = null;
            if (pickedEl && d) {
              pickedEl.textContent = "Saved: " + (d.name || d.id || "Bluetooth printer");
              pickedEl.classList.remove("hidden");
            }

            function afterRefresh(note, closeModal) {
              reflectBluetoothPrinterModal();
              msg(note);
              refreshSaved();
              if (closeModal) setPrinterOpen(false);
            }

            if (!d) {
              afterRefresh("Printer saved.", true);
              return;
            }
            if (!d.gatt) {
              afterRefresh(
                "Profile saved, but this hardware has no BLE session for Chrome. Use USB or network, or print via the browser Print dialog.",
                false
              );
              return;
            }

            var attach = typeof window.posBtAttachPairedThermal === "function" ? window.posBtAttachPairedThermal(d) : Promise.resolve(d);
            return Promise.resolve(attach).then(
              function () {
                var sg = window.__POS_BT || {};
                if (typeof reevaluatePosReceiptPrinterReady === "function") {
                  reevaluatePosReceiptPrinterReady();
                }
                if (sg.gattConnected) {
                  afterRefresh("Printer saved and linked for silent receipts.", true);
                } else {
                  afterRefresh(
                    "Printer saved. BLE link did not complete — keep the printer on and tap Connect printer.",
                    false
                  );
                }
              },
              function () {
                afterRefresh(
                  "Printer saved. Bluetooth link failed — stay close to the printer and tap Connect printer, or switch to USB/network.",
                  false
                );
              }
            );
          })
          .catch(function (e) {
            btPending = null;
            if (pickedEl) pickedEl.classList.add("hidden");
            if (e.name === "NotFoundError") {
              msg("");
            } else {
              msg((e.message || String(e)) + " Tap Choose printer to retry.");
            }
            reflectBluetoothPrinterModal();
          });
      }

      /** Same as the blue Bluetooth button: silent resume if paused, otherwise browser pairing dialog. */
      function runBluetoothPairOrResumeFromUi() {
        // Always open the native browser Bluetooth picker when cashier explicitly taps Pair.
        // Silent reconnect paths can make it seem like nothing happened on some browser builds.
        pickBluetoothAndSave();
      }

      function pickUsbAndSave() {
        var pickedEl = document.getElementById("pos-printer-usb-picked");
        var retryEl = document.getElementById("pos-printer-usb-retry");
        if (!window.posPrinterTypeAllowed(null, "usb")) {
          msg("USB printing is turned off in IT settings.", "error");
          return;
        }
        msg("Select your USB printer…");
        if (!navigator.usb) {
          msg("WebUSB is not available in this browser.");
          if (retryEl) retryEl.classList.remove("hidden");
          return;
        }
        if (retryEl) retryEl.classList.add("hidden");
        var usbPickPromise = navigator.usb.requestDevice({ filters: USB_FILTERS });
        usbPickPromise.finally(function () {
          if (typeof window.posSuppressCatalogGhostClicks === "function") window.posSuppressCatalogGhostClicks(600);
        });
        usbPickPromise
          .then(function (device) {
            usbPending = device;
            var label = device.productName || "USB printer";
            var cfg = {
              vendorId: device.vendorId,
              productId: device.productId,
              serialNumber: device.serialNumber || "",
            };
            if (pickedEl) {
              pickedEl.textContent =
                "Saving VID " +
                device.vendorId.toString(16) +
                " · PID " +
                device.productId.toString(16) +
                (device.productName ? " — " + device.productName : "") +
                "…";
              pickedEl.classList.remove("hidden");
            }
            function finishSave(warnText) {
              return apiSave("usb", label, cfg).then(function () {
                return warnText || "";
              });
            }
            /* requestDevice() grants access; open() often fails on Windows when the spooler/driver owns the printer. */
            if (!device.opened) {
              return device
                .open()
                .then(function () {
                  return device.close().catch(function () {});
                })
                .then(function () {
                  return finishSave("");
                })
                .catch(function (openErr) {
                  if (
                    typeof window.posIsUsbAccessDeniedError === "function" &&
                    window.posIsUsbAccessDeniedError(openErr)
                  ) {
                    return finishSave("__usb_driver__");
                  }
                  return Promise.reject(openErr);
                });
            }
            return finishSave("");
          })
          .then(function (warnText) {
            usbPending = null;
            if (warnText === "__usb_driver__") {
              msg(
                "USB printer saved. " +
                  (typeof window.posUsbAccessDeniedShort === "function"
                    ? window.posUsbAccessDeniedShort()
                    : "Use browser print (POS-80C)."),
                "ok"
              );
            } else if (warnText) {
              msg(warnText, "error");
            } else {
              msg("USB printer saved.", "ok");
            }
            if (pickedEl) {
              pickedEl.textContent = "USB printer saved.";
              pickedEl.classList.remove("hidden");
            }
            if (retryEl) retryEl.classList.add("hidden");
            refreshSaved().then(function () {
              reflectUsbPrinterModal();
            });
            setPrinterOpen(false);
          })
          .catch(function (e) {
            usbPending = null;
            if (pickedEl) pickedEl.classList.add("hidden");
            if (e.name === "NotFoundError") {
              msg("No device selected.");
            } else {
              msg(
                typeof window.posFormatUsbError === "function"
                  ? window.posFormatUsbError(e)
                  : e.message || String(e)
              );
            }
            if (retryEl) retryEl.classList.remove("hidden");
            reflectUsbPrinterModal();
          });
      }

      function saveNetworkHost(host, port) {
        host = (host || "").trim();
        if (!/^(\d{1,3}\.){3}\d{1,3}$/.test(host)) {
          msg("Enter a valid IPv4 address.");
          return Promise.reject(new Error("bad host"));
        }
        if (port < 1 || port > 65535) {
          msg("Invalid port.");
          return Promise.reject(new Error("bad port"));
        }
        var label = "Network " + host + ":" + port;
        return fetch(PRINTER_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) {
            return r.json();
          })
          .then(function (j) {
            var prev = j && j.ok && j.printer && j.printer.config && typeof j.printer.config === "object" ? j.printer.config : {};
            var cfg = Object.assign({}, prev, { host: host, port: port });
            return apiSave("network", label, cfg);
          })
          .then(function () {
            msg("Network printer saved.");
            refreshSaved();
            setPrinterOpen(false);
          })
          .catch(function (e) {
            msg(e.message || String(e));
            throw e;
          });
      }

      function getNetworkScanPort() {
        var port = parseInt((document.getElementById("pos-printer-port") || {}).value || "9100", 10);
        if (isNaN(port) || port < 1 || port > 65535) return NaN;
        return port;
      }

      var netScanStreamAbort = null;

      function stopNetScanStream() {
        if (netScanStreamAbort) {
          try {
            netScanStreamAbort.abort();
          } catch (e) {}
          netScanStreamAbort = null;
        }
      }

      /** Show live log container; returns the scrollable log element. */
      function openNetScanLiveLog(title) {
        var box = document.getElementById("pos-printer-net-results");
        if (!box) return null;
        stopNetScanStream();
        box.classList.remove("hidden");
        box.innerHTML =
          '<p class="text-[10px] font-semibold uppercase tracking-wide text-[rgb(var(--rc-muted))]">' +
          (title || "Scanning") +
          '</p><div id="pos-printer-net-live" class="mt-1 max-h-36 space-y-0.5 overflow-y-auto font-mono text-[10px] leading-snug text-[rgb(var(--rc-page-fg))]/90"></div>';
        return document.getElementById("pos-printer-net-live");
      }

      function appendNetLiveLine(liveEl, text) {
        if (!liveEl) return;
        var row = document.createElement("div");
        row.className =
          "border-b border-[rgb(var(--rc-border))]/25 pb-0.5 text-[rgb(var(--rc-muted))] last:border-0";
        row.textContent = text;
        liveEl.appendChild(row);
        liveEl.scrollTop = liveEl.scrollHeight;
      }

      /**
       * POST NDJSON scan stream; updates live log then applies final result like the JSON API.
       */
      function runNdjsonPrinterScan(body, liveTitle, emptyDetailHtml) {
        var p = body && body.port != null && body.port !== "" ? parseInt(body.port, 10) : getNetworkScanPort();
        if (isNaN(p) || p < 1 || p > 65535) {
          msg("Invalid port.");
          return Promise.resolve();
        }
        body = body || {};
        body.port = p;
        var live = openNetScanLiveLog(liveTitle);
        msg("Scanning… progress below.");
        var finalDone = null;
        netScanStreamAbort = new AbortController();

        return fetch(SCAN_STREAM_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/x-ndjson" },
          body: JSON.stringify(body),
          signal: netScanStreamAbort.signal,
        })
          .then(function (r) {
            if (!r.ok) {
              return r.text().then(function (txt) {
                var j = null;
                try {
                  var line = (txt || "").trim().split("\n")[0];
                  if (line) j = JSON.parse(line);
                } catch (e) {}
                throw new Error((j && j.error) || txt || r.statusText);
              });
            }
            if (!r.body || !r.body.getReader) {
              throw new Error("Streaming is not supported in this browser.");
            }
            var reader = r.body.getReader();
            var dec = new TextDecoder();
            var buf = "";

            function pump() {
              return reader.read().then(function (res) {
                buf += dec.decode(res.value || new Uint8Array(), { stream: !res.done });
                var lines = buf.split("\n");
                buf = lines.pop() || "";
                lines.forEach(function (line) {
                  line = line.trim();
                  if (!line) return;
                  var ev;
                  try {
                    ev = JSON.parse(line);
                  } catch (e) {
                    return;
                  }
                  if (ev.type === "start" && live) {
                    var prefs = (ev.prefixes || []).join(", ");
                    var sp = (ev.scan_ports || []).join(", ");
                    appendNetLiveLine(
                      live,
                      "Primary port " + ev.port + " · thermal TCP: " + (sp || "—")
                    );
                    appendNetLiveLine(
                      live,
                      (ev.mode === "subnet" ? "One subnet" : "LAN") + " · " + (prefs || "—")
                    );
                    if (ev.client_hint_ip) {
                      appendNetLiveLine(live, "Client IP hint: " + ev.client_hint_ip);
                    }
                  } else if (ev.type === "batch") {
                    var foundList = "";
                    if (ev.found && ev.found.length) {
                      foundList = ev.found
                        .map(function (x) {
                          if (x && typeof x === "object" && x.host) return x.host + ":" + x.port;
                          return String(x);
                        })
                        .join(", ");
                    }
                    var cumN = ev.hosts_cumulative ? ev.hosts_cumulative.length : 0;
                    appendNetLiveLine(
                      live,
                      ev.prefix +
                        " · ." +
                        ev.lo +
                        "–." +
                        ev.hi +
                        " · batch " +
                        ev.batch +
                        "/" +
                        ev.batches +
                        " · +" +
                        (ev.found ? ev.found.length : 0) +
                        " · " +
                        cumN +
                        " cum" +
                        (foundList ? " · " + foundList : "")
                    );
                  } else if (ev.type === "subnet" && ev.status === "start") {
                    appendNetLiveLine(live, "→ Subnet " + ev.index + "/" + ev.total + ": " + ev.prefix + ".x");
                  } else if (ev.type === "done") {
                    finalDone = ev;
                  }
                });
                if (res.done) {
                  netScanStreamAbort = null;
                  if (buf.trim()) {
                    try {
                      var evLast = JSON.parse(buf.trim());
                      if (evLast.type === "done") finalDone = evLast;
                    } catch (e2) {}
                  }
                  if (!finalDone) throw new Error("Scan ended without a result.");
                  if (finalDone.ok === false) throw new Error(finalDone.error || "Scan failed");
                  var spList = (finalDone.scan_ports || []).join(", ");
                  var hintAuto =
                    finalDone.scanned_prefixes && finalDone.scanned_prefixes.length
                      ? '<p class="text-xs text-[rgb(var(--rc-muted))]">Searched subnets: ' +
                        finalDone.scanned_prefixes.join(", ") +
                        ".x — no open TCP on scanned ports (" +
                        (spList || String(p)) +
                        ").</p>"
                      : "";
                  var emptyHtml = emptyDetailHtml;
                  if (emptyHtml === "" || emptyHtml == null) {
                    emptyHtml = hintAuto;
                  }
                  return applyNetworkScanResults(finalDone, p, emptyHtml);
                }
                return pump();
              });
            }
            return pump();
          })
          .catch(function (e) {
            netScanStreamAbort = null;
            if (e && e.name === "AbortError") return;
            msg(e.message || String(e));
          });
      }

      /** Apply scan API JSON: hosts list, optional empty message. Uses ``endpoints`` when present (multi-port thermal). */
      function applyNetworkScanResults(j, port, emptyDetailHtml) {
        var box = document.getElementById("pos-printer-net-results");
        if (!j.ok) throw new Error(j.error || "Scan failed");
        netScanPort = port;
        if (!box) return Promise.resolve();
        box.innerHTML = "";
        var eps = j.endpoints && j.endpoints.length ? j.endpoints : null;
        var spFallback = (j.scan_ports || []).join(", ");
        var hasTargets = eps ? eps.length : j.hosts && j.hosts.length;
        if (!hasTargets) {
          box.innerHTML =
            emptyDetailHtml ||
            '<p class="text-xs text-[rgb(var(--rc-muted))]">No thermal/raw TCP response on scanned ports (' +
              (spFallback || String(port)) +
              ").</p>";
          box.classList.remove("hidden");
          msg("Scan finished — no printers found.");
          return Promise.resolve();
        }
        if (eps && eps.length === 1) {
          msg("Found 1 printer — saving…");
          return saveNetworkHost(eps[0].host, eps[0].port).then(function () {
            box.innerHTML =
              '<p class="text-xs text-[rgb(var(--rc-muted))]">Saved ' +
              eps[0].host +
              ":" +
              eps[0].port +
              ".</p>";
            box.classList.remove("hidden");
          });
        }
        if (!eps && j.hosts.length === 1) {
          msg("Found 1 printer — saving…");
          return saveNetworkHost(j.hosts[0], port).then(function () {
            box.innerHTML =
              '<p class="text-xs text-[rgb(var(--rc-muted))]">Saved ' +
              j.hosts[0] +
              ":" +
              port +
              ".</p>";
            box.classList.remove("hidden");
          });
        }
        var list = eps || j.hosts.map(function (h) {
          return { host: h, port: port };
        });
        list.forEach(function (ep, i) {
          var lab = document.createElement("label");
          lab.className =
            "flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1.5 hover:bg-[rgb(var(--rc-surface-2))]/80";
          var inp = document.createElement("input");
          inp.type = "radio";
          inp.name = "pos-printer-net-host";
          inp.value = ep.host + "\t" + ep.port;
          inp.className = "pos-printer-net-radio";
          if (i === 0) inp.checked = true;
          lab.appendChild(inp);
          lab.appendChild(document.createTextNode(ep.host + ":" + ep.port));
          box.appendChild(lab);
        });
        box.classList.remove("hidden");
        msg("Found " + list.length + " endpoint(s) — tap one to save.");
        return Promise.resolve();
      }

      /** Match server ``_normalize_subnet_scan_prefix``: ``a.b.c`` or full IPv4 / CIDR → first three octets. */
      function normalizeSubnetPrefixForScan(s) {
        s = (s || "").trim();
        if (!s) return "";
        if (s.indexOf("/") >= 0) s = s.split("/")[0].trim();
        var parts = s.split(".").filter(function (p) {
          return p !== "";
        });
        if (parts.length >= 4) parts = parts.slice(0, 3);
        if (parts.length !== 3) return "";
        var out = parts.join(".");
        if (!/^\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(out)) return "";
        for (var i = 0; i < 3; i++) {
          var n = parseInt(parts[i], 10);
          if (isNaN(n) || n > 255) return "";
        }
        return out;
      }

      /** Auto: client’s /24 + common subnets (server must share the LAN). Live NDJSON progress. */
      function runNetworkScanAuto() {
        var port = getNetworkScanPort();
        if (isNaN(port)) {
          msg("Invalid port.");
          return;
        }
        runNdjsonPrinterScan({ port: port }, "Live scan · automatic", "");
      }

      /** Scan only the subnet field (.1–.254). */
      function runNetworkScanSubnet() {
        var subEl = document.getElementById("pos-printer-subnet");
        var raw = ((subEl && subEl.value) || "").trim();
        var sub = normalizeSubnetPrefixForScan(raw);
        if (!sub) {
          if (!raw) {
            sub = "192.168.1";
            if (subEl) subEl.value = sub;
          } else {
            msg("Enter a subnet (e.g. 192.168.1) or a printer IP — we scan the .1–.254 range on that subnet.");
            return;
          }
        } else if (subEl && raw !== sub) {
          subEl.value = sub;
        }
        var port = getNetworkScanPort();
        if (isNaN(port)) {
          msg("Invalid port.");
          return;
        }
        var emptyMsg =
          '<p class="text-xs text-[rgb(var(--rc-muted))]">No thermal/raw TCP on subnet ' +
          sub +
          ".x for the scanned port list.</p>";
        runNdjsonPrinterScan({ subnet_prefix: sub, port: port }, "Live scan · " + sub + ".x", emptyMsg);
      }

      /** Probe a single IP or hostname; save if port is open. */
      function probeManualNetworkPrinter() {
        var host = (((document.getElementById("pos-printer-host") || {}).value) || "").trim();
        var port = getNetworkScanPort();
        if (isNaN(port)) {
          msg("Invalid port.");
          return;
        }
        if (!host) {
          msg("Enter a printer IP or hostname.");
          return;
        }
        msg("Checking " + host + "…");
        var box = document.getElementById("pos-printer-net-results");
        if (box) {
          box.innerHTML = "";
          box.classList.add("hidden");
        }
        fetch(SCAN_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ probe_host: host, port: port }),
        })
          .then(function (r) {
            return r.json();
          })
          .then(function (j) {
            if (!j.ok) throw new Error(j.error || "Check failed");
            var epsProbe = j.endpoints && j.endpoints.length ? j.endpoints : null;
            if (!epsProbe && (!j.hosts || !j.hosts.length)) {
              if (box) {
                var spb = (j.scan_ports || []).join(", ");
                box.innerHTML =
                  '<p class="text-xs text-[rgb(var(--rc-muted))]">No response on thermal ports (' +
                  (spb || String(port)) +
                  ") at <strong>" +
                  host.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;") +
                  "</strong>.</p>";
                box.classList.remove("hidden");
              }
              msg("Nothing found at that address.");
              return;
            }
            var ep0 = epsProbe ? epsProbe[0] : { host: j.hosts[0], port: port };
            var ip = ep0.host;
            var usePort = ep0.port;
            var label = j.probe_target && j.resolved && j.probe_target !== j.resolved ? ip + " (" + j.probe_target + ")" : ip;
            msg("Found printer at " + label + ":" + usePort + " — saving…");
            return saveNetworkHost(ip, usePort).then(function () {
              if (box) {
                box.innerHTML =
                  '<p class="text-xs text-[rgb(var(--rc-muted))]">Saved network printer ' + ip + ":" + usePort + ".</p>";
                box.classList.remove("hidden");
              }
            });
          })
          .catch(function (e) {
            msg(e.message || String(e));
          });
      }

      if (panelNet && !panelNet.dataset.netHostDelegate) {
        panelNet.dataset.netHostDelegate = "1";
        panelNet.addEventListener("change", function (e) {
          var t = e.target;
          if (!t || t.name !== "pos-printer-net-host" || !t.checked) return;
          var parts = (t.value || "").split("\t");
          if (parts.length === 2) {
            msg("Saving " + parts[0].trim() + ":" + parts[1].trim() + "…");
            saveNetworkHost(parts[0].trim(), parseInt(parts[1].trim(), 10)).catch(function () {});
          } else {
            msg("Saving " + t.value + "…");
            saveNetworkHost(t.value, netScanPort).catch(function () {});
          }
        });
      }

      function apiSave(printerType, deviceLabel, config) {
        return fetch(PRINTER_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({
            printer_type: printerType,
            device_label: deviceLabel,
            config: config,
          }),
        }).then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) throw new Error((j && j.error) || r.statusText);
            return j;
          });
        });
      }

      function apiClear() {
        return fetch(PRINTER_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ clear: true }),
        }).then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) throw new Error((j && j.error) || r.statusText);
            return j;
          });
        });
      }

      function formatPrinterHeaderSummary(p) {
        var t = (p && p.printer_type ? p.printer_type : "").toLowerCase();
        var typeLabel =
          t === "network" ? "Network" : t === "bluetooth" ? "Bluetooth" : t === "usb" ? "USB" : "Printer";
        var cfg = {};
        if (p && p.config != null) {
          if (typeof p.config === "object") cfg = p.config;
          else if (typeof p.config === "string") {
            try {
              cfg = JSON.parse(p.config) || {};
            } catch (e) {
              cfg = {};
            }
          }
        }
        var detail = "";
        if (t === "network") {
          var h = (cfg.host || "").trim();
          var pr = cfg.port != null && cfg.port !== "" ? String(cfg.port) : "9100";
          detail = h ? h + ":" + pr : (p.device_label || "").trim();
        } else if (t === "bluetooth") {
          detail = (p.device_label || "").trim();
          if (!detail && cfg.bluetoothId) {
            var bid = String(cfg.bluetoothId);
            detail = bid.length > 14 ? bid.slice(0, 12) + "…" : bid;
          }
          if (!detail) detail = "Bluetooth";
        } else if (t === "usb") {
          detail = (p.device_label || "").trim() || "USB";
        }
        var sm = detail ? typeLabel + " · " + detail : typeLabel;
        if (sm.length > 48) sm = sm.slice(0, 45) + "…";
        var xs = detail || typeLabel;
        if (xs.length > 14) xs = xs.slice(0, 12) + "…";
        var aria =
          typeLabel +
          " receipt printer" +
          (detail ? ", " + detail : "") +
          ". Open printer settings to change.";
        return { sm: sm, xs: xs, aria: aria };
      }

      /** Re-check compulsory gate after BLE link (publishReceiptPrinterReadyFlag can run before GATT connects). */
      function reevaluatePosReceiptPrinterReady() {
        if (
          typeof window.__posPrintingCompulsoryOnSaleEnabled !== "function" ||
          !window.__posPrintingCompulsoryOnSaleEnabled()
        ) {
          return;
        }
        var row = window.__POS_LAST_HEADER_PRINTER;
        if (!row || !savedPrinterTypeAllowed(row)) {
          return;
        }
        var pt = String(row.printer_type || "").toLowerCase();
        if (pt !== "bluetooth") {
          return;
        }
        var sg = window.__POS_BT || {};
        var ready = !!(sg.gattConnected && !sg.userMuted);
        if (ready === !!window.POS_RECEIPT_PRINTER_CONFIGURED) {
          return;
        }
        window.POS_RECEIPT_PRINTER_CONFIGURED = ready;
        if (ready) {
          window.__posCompulsorySetupPromptQueued = false;
          window.__posCompulsorySetupOpened = false;
          clearTimeout(window.__posCompulsorySetupPromptTimer);
          document.body.classList.remove("pos-compulsory-setup-open");
          if (printerUiOpen) {
            setPrinterOpen(false);
          }
        }
        try {
          if (window.__posPrinterConnectionChanged) window.__posPrinterConnectionChanged();
        } catch (eRc) {}
      }

      function syncPrinterHeaderButton(connected, printer) {
        var rawPrinter = printer || null;
        window.__POS_LAST_HEADER_PRINTER = rawPrinter;
        window.__POS_LAST_HEADER_STALE_PRINTER =
          rawPrinter && !savedPrinterTypeAllowed(rawPrinter) ? rawPrinter : null;
        var effPrinter =
          rawPrinter && savedPrinterTypeAllowed(rawPrinter) ? rawPrinter : null;
        var effConnected = !!(connected && effPrinter);
        window.__POS_LAST_HEADER_CONNECTED = !!effConnected;

        function publishReceiptPrinterReadyFlag() {
          var baseReady = effConnected && !!effPrinter;
          window.__posReceiptReadyProbeGen = (window.__posReceiptReadyProbeGen || 0) + 1;
          var myGen = window.__posReceiptReadyProbeGen;

          function compulsoryOn() {
            return (
              typeof window.__posPrintingCompulsoryOnSaleEnabled === "function" &&
              window.__posPrintingCompulsoryOnSaleEnabled()
            );
          }

          function applyReady(rv) {
            if (myGen !== window.__posReceiptReadyProbeGen) return;
            window.POS_RECEIPT_PRINTER_CONFIGURED = !!rv;
            if (rv) {
              window.__posCompulsorySetupPromptQueued = false;
              window.__posCompulsorySetupOpened = false;
              clearTimeout(window.__posCompulsorySetupPromptTimer);
              document.body.classList.remove("pos-compulsory-setup-open");
              if (printerUiOpen) {
                setPrinterOpen(false);
              }
            }
            try {
              if (window.__posPrinterConnectionChanged) window.__posPrinterConnectionChanged();
            } catch (eCb) {}
            try {
              var c0 = compulsoryOn();
              var ob2 = document.getElementById("pos-printer-open");
              if (!ob2 || !effPrinter || !c0) return;
              var pt0 = String(effPrinter.printer_type || "").toLowerCase();
              if (pt0 === "network" || pt0 === "usb") {
                var showOk = !!rv && baseReady;
                ob2.classList.toggle("pos-printer-header-btn--connected", showOk);
                ob2.classList.toggle("pos-printer-header-btn--disconnected", !showOk);
              }
            } catch (eHdr) {}
          }

          if (!baseReady || !effPrinter) {
            applyReady(false);
            return;
          }

          var pt = String(effPrinter.printer_type || "").toLowerCase();
          var compulsory = compulsoryOn();

          if (pt === "bluetooth") {
            var sg = window.__POS_BT || {};
            applyReady(!!(sg.gattConnected && !sg.userMuted));
            return;
          }

          if (!compulsory) {
            applyReady(true);
            return;
          }

          if (pt === "network") {
            applyReady(false);
            fetch(TCP_REACHABLE_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
              .then(function (r) {
                return r.json();
              })
              .then(function (j) {
                applyReady(!!(j && j.ok && j.reachable));
              })
              .catch(function () {
                applyReady(false);
              });
            return;
          }

          if (pt === "usb") {
            if (typeof window.posPrinterTypeAllowed === "function" && !window.posPrinterTypeAllowed(null, "usb")) {
              applyReady(false);
              return;
            }
            applyReady(false);
            if (!window.isSecureContext || !navigator.usb) {
              return;
            }
            var cfg = effPrinter.config;
            if (typeof cfg === "string") {
              try {
                cfg = JSON.parse(cfg) || {};
              } catch (eParse) {
                cfg = {};
              }
            }
            if (!cfg || typeof cfg !== "object") cfg = {};
            var vid = parseInt(cfg.vendorId, 10);
            var pid = parseInt(cfg.productId, 10);
            if (isNaN(vid) || isNaN(pid)) {
              return;
            }
            navigator.usb
              .getDevices()
              .then(function (devices) {
                var ok = devices.some(function (d) {
                  return d && d.vendorId === vid && d.productId === pid;
                });
                applyReady(ok);
              })
              .catch(function () {
                applyReady(false);
              });
            return;
          }

          applyReady(false);
        }

        var ob = document.getElementById("pos-printer-open");
        if (!ob) {
          publishReceiptPrinterReadyFlag();
          return;
        }
        var smEl = document.getElementById("pos-printer-open-label-sm");
        var xsEl = document.getElementById("pos-printer-open-label-xs");
        var isBtThermal =
          !!(effConnected && effPrinter && String(effPrinter.printer_type || "").toLowerCase() === "bluetooth");
        ob.classList.remove("pos-printer-header-btn--bt-standby", "pos-printer-header-btn--bt-busy");

        var sg = window.__POS_BT || {};
        var gattLive = !!(sg.gattConnected);
        var muted = !!(sg.userMuted);
        var busyBt = !!(sg.busyConnecting);

        if (!isBtThermal) {
          var compulsoryHdr =
            typeof window.__posPrintingCompulsoryOnSaleEnabled === "function" &&
            window.__posPrintingCompulsoryOnSaleEnabled();
          var ptHdr =
            effConnected && effPrinter ? String(effPrinter.printer_type || "").toLowerCase() : "";
          if (compulsoryHdr && effConnected && effPrinter && (ptHdr === "network" || ptHdr === "usb")) {
            ob.classList.toggle("pos-printer-header-btn--connected", false);
            ob.classList.toggle("pos-printer-header-btn--disconnected", true);
          } else {
            ob.classList.toggle("pos-printer-header-btn--connected", !!effConnected);
            ob.classList.toggle("pos-printer-header-btn--disconnected", !effConnected);
          }
        } else {
          ob.classList.remove("pos-printer-header-btn--connected", "pos-printer-header-btn--disconnected");
          ob.classList.toggle("pos-printer-header-btn--connected", !!(gattLive && !muted));
          ob.classList.toggle("pos-printer-header-btn--bt-standby", !gattLive || muted);
          ob.classList.toggle("pos-printer-header-btn--disconnected", false);
          ob.classList.toggle("pos-printer-header-btn--bt-busy", busyBt && !muted);
        }

        if (window.__POS_LAST_HEADER_STALE_PRINTER) {
          var staleKinds = printerKindsForSetupNavigation();
          var staleNeed =
            staleKinds.indexOf("bluetooth") >= 0
              ? "Bluetooth"
              : staleKinds.length
                ? staleKinds[0].charAt(0).toUpperCase() + staleKinds[0].slice(1)
                : "printer";
          var staleSm = "Connect " + staleNeed;
          var staleXs = staleNeed;
          if (smEl) smEl.textContent = staleSm;
          if (xsEl) xsEl.textContent = staleXs.length > 14 ? staleXs.slice(0, 12) + "…" : staleXs;
          ob.classList.remove("pos-printer-header-btn--bt-standby", "pos-printer-header-btn--bt-busy");
          ob.classList.toggle("pos-printer-header-btn--connected", false);
          ob.classList.toggle("pos-printer-header-btn--disconnected", true);
          ob.setAttribute(
            "aria-label",
            "Saved " +
              String(window.__POS_LAST_HEADER_STALE_PRINTER.printer_type || "printer") +
              " printer is not allowed in IT settings. Open setup to connect " +
              staleNeed +
              "."
          );
          publishReceiptPrinterReadyFlag();
          return;
        }

        if (effConnected && effPrinter) {
          var s = formatPrinterHeaderSummary(effPrinter);
          var headlineSm = s.sm;
          var headlineXs = s.xs;
          var suffix = "";

          if (isBtThermal) {
            if (muted) {
              headlineSm = "Paused · " + s.sm;
              headlineXs = "Paused";
              suffix = " Bluetooth paused — use Connect printer below to reconnect.";
            } else if (!gattLive) {
              if (busyBt) headlineSm = "Connecting… · " + s.sm;
              else headlineSm = "Off · " + s.sm;
              headlineXs = busyBt ? "…" : "Off";
              suffix = busyBt ? " Bluetooth connecting." : " Bluetooth not linked; opens when the printer is nearby.";
            }
          }

          if (smEl) smEl.textContent = headlineSm.length > 56 ? headlineSm.slice(0, 53) + "…" : headlineSm;
          if (xsEl) xsEl.textContent = headlineXs.length > 14 ? headlineXs.slice(0, 12) + "…" : headlineXs;
          ob.setAttribute("aria-label", s.aria + suffix);
        } else {
          var allowedKinds =
            typeof printerKindsForSetupNavigation === "function"
              ? printerKindsForSetupNavigation()
              : [];
          var onlyOne = allowedKinds.length === 1;
          var kindLabels = allowedKinds
            .map(function (k) {
              if (k === "bluetooth") return "Bluetooth";
              if (k === "network") return "network";
              if (k === "usb") return "USB";
              return k;
            })
            .join(", ");
          if (smEl) smEl.textContent = onlyOne ? "Connect printer" : "Set up printer";
          if (xsEl) xsEl.textContent = onlyOne ? "Connect" : "Printer";
          ob.setAttribute(
            "aria-label",
            onlyOne
              ? "No receipt printer configured. Open to connect the allowed printer."
              : "No receipt printer configured. Open to choose " + (kindLabels || "a printer type") + "."
          );
        }

        publishReceiptPrinterReadyFlag();
      }

      function refreshPrintAgentStatus() {
        var st = document.getElementById("pos-print-agent-status");
        var cb = document.getElementById("pos-print-agent-enabled");
        if (!st || !cb) return;
        fetch(PRINT_AGENT_STATUS_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) {
            return r.json();
          })
          .then(function (j) {
            if (!j || !j.ok) return;
            if (!j.available) {
              st.textContent = "Save a network printer above to configure the agent.";
              cb.disabled = true;
              cb.checked = false;
              return;
            }
            cb.disabled = false;
            cb.checked = !!j.enabled;
            st.textContent = j.has_token
              ? "Token …" + (j.token_hint || "----") + " — set RICHCOM_AGENT_TOKEN on the on-site PC."
              : "Enable agent and save to generate a token.";
          })
          .catch(function () {});
      }

      function reflectBluetoothPrinterModal() {
        var dot = document.getElementById("pos-printer-bt-status-dot");
        var label = document.getElementById("pos-printer-bt-status-label");
        var pairBtn = document.getElementById("pos-printer-bt-pair");
        if (!dot || !label) return;

        var pRow = window.__POS_LAST_HEADER_PRINTER || null;
        var hasSavedBt =
          !!(
            window.__POS_LAST_HEADER_CONNECTED &&
            pRow &&
            String(pRow.printer_type || "").toLowerCase() === "bluetooth"
          );

        function greyState(text) {
          dot.className =
            "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-slate-400 ring-2 ring-[rgb(var(--rc-surface))]";
          label.textContent = text;
        }

        function setPairLabel(savedBt) {
          if (!pairBtn) return;
          pairBtn.textContent = savedBt ? "Connect printer…" : "Choose printer…";
        }

        if (!window.isSecureContext || !navigator.bluetooth) {
          greyState("Bluetooth printing needs Chrome or Edge with HTTPS — or localhost for testing.");
          setPairLabel(hasSavedBt);
          return;
        }

        if (!hasSavedBt) {
          greyState("Tap Choose printer to add one.");
          setPairLabel(false);
          return;
        }

        setPairLabel(hasSavedBt);

        var p = pRow;
        var sg = window.__POS_BT || {};
        var busy = !!sg.busyConnecting;
        var live = !!sg.gattConnected;
        var name = ((p.device_label || "") + "").trim() || "this printer";

        if (busy) {
          dot.className =
            "mt-1.5 h-2.5 w-2.5 shrink-0 animate-pulse rounded-full bg-sky-500 ring-2 ring-sky-300/55";
          label.textContent = 'Connecting to “' + name + '"…';
          return;
        }

        if (live) {
          dot.className =
            "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-emerald-500 shadow-[0_0_10px_-3px_rgba(34,197,94,0.9)] ring-2 ring-emerald-400/65";
          label.textContent = 'Ready — “' + name + '” is linked; receipts print without prompts.';
          return;
        }

        dot.className =
          "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full bg-amber-500 ring-2 ring-[rgb(var(--rc-surface))]";
        label.textContent =
          'Saved — no BLE link yet. Tap Connect printer (printer on, nearby). OS pairing alone is not enough.';
      }

      document.addEventListener("richcom-pos-bluetooth-changed", function () {
        var row = window.__POS_LAST_HEADER_PRINTER || null;
        var allowed = !!(row && savedPrinterTypeAllowed(row));
        syncPrinterHeaderButton(allowed, row);
        reflectBluetoothPrinterModal();
        reevaluatePosReceiptPrinterReady();
      });

      function printerCacheFingerprintRow(p) {
        if (!p || !p.printer_type) return "";
        var t = (p.printer_type || "").toLowerCase();
        var c = p.config || {};
        if (typeof c === "string") {
          try {
            c = JSON.parse(c) || {};
          } catch (e) {
            c = {};
          }
        }
        if (t === "bluetooth") return "bt:" + String(c.bluetoothId || "");
        if (t === "network") {
          var h = String(c.host || "").trim().toLowerCase();
          var po = c.port != null && c.port !== "" ? String(c.port) : "9100";
          return "net:" + h + ":" + po;
        }
        if (t === "usb") {
          var vid = parseInt(c.vendorId, 10);
          var pid = parseInt(c.productId, 10);
          if (isNaN(vid) || isNaN(pid)) return "usb:invalid";
          return "usb:" + vid + ":" + pid + ":" + String(c.serialNumber || "").trim();
        }
        return t;
      }

      function refreshSaved() {
        var testBtn = document.getElementById("pos-printer-test-print");
        return fetch(PRINTER_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) {
            return r.json();
          })
          .then(function (j) {
            if (typeof window.__posApplyPrinterProfileJson === "function") {
              window.__posApplyPrinterProfileJson(j);
            }
            if (typeof window.__posWarmBluetoothPrinter === "function") {
              window.__posWarmBluetoothPrinter();
            }
            var p = j && j.ok ? j.printer : null;
            var stale = j && j.ok && j.stale_printer ? j.stale_printer : null;
            var activeRow = p && savedPrinterTypeAllowed(p) ? p : null;
            if (typeof window.posBtOnSavedPrinterRowChanged === "function") {
              window.posBtOnSavedPrinterRowChanged(activeRow);
            }
            syncPrinterHeaderButton(!!activeRow, activeRow);
            if (stale && !activeRow) {
              window.__POS_LAST_HEADER_STALE_PRINTER = stale;
            }
            reflectBluetoothPrinterModal();
            if (panelUsb && !panelUsb.classList.contains("hidden")) {
              reflectUsbPrinterModal();
            }
            if (!j || !j.ok || !savedBox) return;
            var bannerRow = activeRow || stale;
            if (!bannerRow) {
              savedBox.classList.add("hidden");
              savedBox.textContent = "";
              if (testBtn) {
                testBtn.disabled = true;
                testBtn.title = "Save a printer first";
              }
              refreshPrintAgentStatus();
              return;
            }
            var line =
              "<strong>Saved:</strong> " +
              (bannerRow.device_label || bannerRow.printer_type) +
              " (" +
              bannerRow.printer_type +
              ")";
            if (!savedPrinterTypeAllowed(bannerRow)) {
              line =
                '<strong class="text-amber-800 dark:text-amber-200">Not active:</strong> ' +
                (p.device_label || p.printer_type) +
                " (" +
                p.printer_type +
                ") — disabled in IT settings.";
              line +=
                '<br><span class="mt-1 block text-xs font-semibold text-amber-700 dark:text-amber-300">Only ' +
                posPrinterAllowedKindsInSettings().join(", ") +
                ' allowed. Tap <strong>Forget saved printer</strong>, then connect ' +
                (posPrinterAllowedKindsInSettings().indexOf("bluetooth") >= 0 ? "Bluetooth" : "an allowed printer") +
                ".</span>";
              if (testBtn) {
                testBtn.disabled = true;
                testBtn.title = "Forget the saved printer and set up an allowed type";
              }
            }
            savedBox.innerHTML = line;
            savedBox.classList.remove("hidden");
            if (testBtn && savedPrinterTypeAllowed(bannerRow) && activeRow) {
              testBtn.disabled = false;
              testBtn.title = "Send a one-line test receipt to your saved printer";
            }
            try {
              if (activeRow) {
                var fp = printerCacheFingerprintRow(activeRow);
                localStorage.setItem(
                  LS_KEY,
                  JSON.stringify(Object.assign({}, activeRow, { shop_id: SHOP_ID, printer_fingerprint: fp }))
                );
              } else {
                localStorage.removeItem(PRINTER_LS_KEY);
              }
            } catch (e) {}
            if (activeRow && (activeRow.printer_type || "").toLowerCase() === "network") {
              refreshPrintAgentStatus();
            }
            return j;
          })
          .catch(function () {
            if (typeof window.posBtOnSavedPrinterRowChanged === "function") {
              window.posBtOnSavedPrinterRowChanged(null);
            }
            syncPrinterHeaderButton(false, null);
            reflectBluetoothPrinterModal();
            if (panelUsb && !panelUsb.classList.contains("hidden")) {
              reflectUsbPrinterModal();
            }
            if (savedBox) {
              savedBox.classList.add("hidden");
            }
            var tb = document.getElementById("pos-printer-test-print");
            if (tb) {
              tb.disabled = true;
              tb.title = "Save a printer first";
            }
          });
      }

      document.querySelectorAll(".pos-printer-type-btn").forEach(function (btn) {
        btn.addEventListener("click", function () {
          if (btn.disabled || btn.classList.contains("pos-printer-type-blocked")) return;
          var t = btn.getAttribute("data-printer-type");
          openPrinterPanelForType(t, { fromSaved: false, autoPair: false });
        });
      });

      document.querySelectorAll(".pos-printer-back").forEach(function (btn) {
        btn.addEventListener("click", function () {
          showTypeChooser();
        });
      });

      /** Header button: open setup, load saved profile, show the matching connection panel. */
      function openPrinterSetupFromHeader() {
        setPrinterOpen(true);
      }

      try {
        window.__posOpenPrinterSetupFromHeader = openPrinterSetupFromHeader;
        window.__posRunCompulsoryPrinterConnectPrompt = runCompulsoryPrinterConnectPrompt;
        window.__posEnsureCompulsoryPrinterPromptAfterLoad = ensureCompulsoryPrinterPromptAfterLoad;
        if (window.__posCompulsoryPrinterOnLoadPending) {
          ensureCompulsoryPrinterPromptAfterLoad();
        }
      } catch (eExposeComp) {}

      document.addEventListener(
        "click",
        function (ev) {
          var el = ev.target && ev.target.closest && ev.target.closest("#pos-printer-open");
          if (!el) return;
          ev.preventDefault();
          openPrinterSetupFromHeader();
        },
        true
      );
      if (closeBtn) closeBtn.addEventListener("click", function () {
        setPrinterOpen(false);
      });
      if (backdrop)
        backdrop.addEventListener("click", function () {
          setPrinterOpen(false);
        });

      document.getElementById("pos-printer-bt-pair") &&
        document.getElementById("pos-printer-bt-pair").addEventListener("click", function () {
          runBluetoothPairOrResumeFromUi();
        });

      document.getElementById("pos-printer-net-scan") &&
        document.getElementById("pos-printer-net-scan").addEventListener("click", function () {
          runNetworkScanSubnet();
        });

      document.getElementById("pos-printer-net-save-manual") &&
        document.getElementById("pos-printer-net-save-manual").addEventListener("click", function () {
          probeManualNetworkPrinter();
        });

      document.getElementById("pos-print-agent-apply") &&
        document.getElementById("pos-print-agent-apply").addEventListener("click", function () {
          var cb = document.getElementById("pos-print-agent-enabled");
          var hint = document.getElementById("pos-print-agent-token-hint");
          fetch(PRINT_AGENT_CONFIGURE_API, {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify({ enabled: !!(cb && cb.checked) }),
          })
            .then(function (r) {
              return r.json();
            })
            .then(function (j) {
              if (!j.ok) throw new Error(j.error || "Save failed");
              msg(j.message || "Agent settings saved.");
              if (j.token && hint) {
                hint.textContent = "RICHCOM_AGENT_TOKEN=" + j.token;
                hint.classList.remove("hidden");
              }
              refreshPrintAgentStatus();
            })
            .catch(function (e) {
              msg(e.message || String(e));
            });
        });

      document.getElementById("pos-print-agent-rotate") &&
        document.getElementById("pos-print-agent-rotate").addEventListener("click", function () {
          var hint = document.getElementById("pos-print-agent-token-hint");
          fetch(PRINT_AGENT_CONFIGURE_API, {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify({ enabled: true, rotate_token: true }),
          })
            .then(function (r) {
              return r.json();
            })
            .then(function (j) {
              if (!j.ok) throw new Error(j.error || "Could not rotate token");
              msg("Copy the new token into the agent on the shop PC.");
              if (j.token && hint) {
                hint.textContent = "RICHCOM_AGENT_TOKEN=" + j.token;
                hint.classList.remove("hidden");
              }
              refreshPrintAgentStatus();
            })
            .catch(function (e) {
              msg(e.message || String(e));
            });
        });

      document.getElementById("pos-printer-usb-pick") &&
        document.getElementById("pos-printer-usb-pick").addEventListener("click", function () {
          pickUsbAndSave();
        });
      document.getElementById("pos-printer-usb-retry") &&
        document.getElementById("pos-printer-usb-retry").addEventListener("click", function () {
          pickUsbAndSave();
        });

      document.getElementById("pos-printer-test-print") &&
        document.getElementById("pos-printer-test-print").addEventListener("click", function () {
          if (typeof window.richcomPosPrinterRunTestPrint !== "function") {
            msg("Could not start test print. Refresh the page and try again.");
            return;
          }
          var btn = document.getElementById("pos-printer-test-print");
          if (btn) btn.disabled = true;
          msg("Sending test receipt…");
          window
            .richcomPosPrinterRunTestPrint()
            .then(function () {
              msg("Test print sent. Check the printer or the browser print dialog.");
            })
            .catch(function (e) {
              msg((e && e.message) || String(e));
            })
            .finally(function () {
              refreshSaved();
            });
        });

      document.getElementById("pos-printer-clear-saved") &&
        document.getElementById("pos-printer-clear-saved").addEventListener("click", function () {
          apiClear()
            .then(function () {
              try {
                localStorage.removeItem(LS_KEY);
              } catch (e) {}
              btPending = null;
              usbPending = null;
              if (typeof window.posBtForgetSavedThermal === "function") {
                window.posBtForgetSavedThermal();
              }
              msg("Saved printer cleared.");
              refreshSaved();
            })
            .catch(function (e) {
              msg(e.message || String(e));
            });
        });

      document.addEventListener("pos:printer-auto-scan", function (e) {
        var kind = ((e && e.detail && e.detail.kind) || "").toLowerCase();
        if (kind === "bluetooth") {
          setPrinterOpen(true);
          return;
        }
        if (kind === "network") {
          setPrinterOpen(true);
          return;
        }
        if (kind === "usb") {
          if (typeof window.posPrinterTypeAllowed === "function" && !window.posPrinterTypeAllowed(null, "usb")) {
            return;
          }
          setPrinterOpen(true);
        }
      });

      document.addEventListener("keydown", function (e) {
        if (e.key !== "Escape") return;
        if (stockInModal && !stockInModal.classList.contains("hidden")) {
          setStockInModalOpen(false);
          return;
        }
        if (!printerUiOpen) return;
        setPrinterOpen(false);
      });

      document.addEventListener("visibilitychange", function () {
        if (document.visibilityState !== "visible") return;
        if (typeof window.__posPrintingCompulsoryOnSaleEnabled !== "function" || !window.__posPrintingCompulsoryOnSaleEnabled()) return;
        refreshSaved();
      });
      if (navigator.usb && typeof navigator.usb.addEventListener === "function") {
        try {
          navigator.usb.addEventListener("connect", function () {
            refreshSaved().then(function () {
              reflectUsbPrinterModal();
            });
          });
          navigator.usb.addEventListener("disconnect", function () {
            refreshSaved().then(function () {
              reflectUsbPrinterModal();
            });
          });
        } catch (eUsbEv) {}
      }

      applyPrinterTypeAllowList();
      refreshSaved().finally(function () {
        ensureCompulsoryPrinterPromptAfterLoad();
      });
      try {
        window.applyPrinterTypeAllowList = applyPrinterTypeAllowList;
      } catch (eExpose) {}
    })();