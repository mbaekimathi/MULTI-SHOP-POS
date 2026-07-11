(function () {
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var HOLD_SAVE_API = BOOT.apis.holdSave;
  var HOLD_LIST_API = BOOT.apis.holdList;
  var HOLD_GET_BASE = BOOT.apis.holdGetBase;
  var CART_LS_KEY = "richcom-shop-pos-cart-" + SHOP_ID;
  var HOLD_STATE_KEY = "richcom-shop-pos-hold-" + SHOP_ID;
  var CHECKOUT_PATH_KEY = "richcom-shop-pos-checkout-path-" + SHOP_ID;
function loadCheckoutPathPreference() {
        try {
          var v = String(localStorage.getItem(CHECKOUT_PATH_KEY) || "").trim().toLowerCase();
          if (v === "hold" || v === "withhold") return "hold";
          return "direct";
        } catch (e) {
          return "direct";
        }
      }

      function saveCheckoutPathPreference(path) {
        var p = path === "hold" ? "hold" : "direct";
        try {
          localStorage.setItem(CHECKOUT_PATH_KEY, p);
        } catch (e) {}
        window.__POS_CHECKOUT_PATH = p;
      }

      function applyCheckoutToolbar() {
        var st = loadHoldState();
        window.__POS_HELD_ORDER_ID = st && st.hold_id ? Number(st.hold_id) || null : null;
        var pathPref = loadCheckoutPathPreference();
        // Linked hold state does not force the Hold tab if the cashier chose Direct sale
        // (e.g. after "Load to cart" from the held-orders modal).
        var forcedHold = !!st && pathPref !== "direct";
        var pref = forcedHold ? "hold" : pathPref;
        window.__POS_CHECKOUT_PATH = pref;

        var btnD = document.getElementById("pos-cart-path-direct");
        var btnH = document.getElementById("pos-cart-path-hold");
        var hint = document.getElementById("pos-cart-path-hint");
        var row = document.getElementById("pos-cart-checkout-actions-row");
        var holdSave = document.getElementById("pos-cart-hold-save");

        function setPathBtnActive(btn, on) {
          if (!btn) return;
          btn.setAttribute("aria-pressed", on ? "true" : "false");
          // Drop any older inline utility classes that previous builds toggled;
          // colors now come from CSS keyed off aria-pressed.
          btn.classList.remove(
            "bg-[rgb(var(--rc-primary))]/18",
            "ring-1",
            "ring-[rgb(var(--rc-primary))]/45"
          );
        }
        setPathBtnActive(btnD, pref === "direct");
        setPathBtnActive(btnH, pref === "hold");
        if (btnD) btnD.disabled = !!forcedHold;
        if (btnH) btnH.disabled = false;

        if (hint) {
          if (forcedHold) {
            hint.textContent =
              "This cart is linked to an open held order — use Save & hold or Authorize, or tap Unlink to return to a normal direct sale.";
          } else if (st && pref === "direct") {
            hint.textContent =
              "Direct sale: cart is linked to a held order — Authorize & proceed to finalize payment; switch to Hold tab if you need Save & hold for more stock edits.";
          } else if (pref === "hold") {
            hint.textContent =
              "Hold tab: use Save & hold to commit lines to stock, then reopen this tab or finalize when the customer pays.";
          } else {
            hint.textContent = "Direct sale: normal checkout — stock moves when you authorize.";
          }
        }
        if (row && holdSave) {
          if (pref === "hold") {
            holdSave.classList.remove("hidden");
            row.className = "grid grid-cols-1 gap-2";
          } else {
            holdSave.classList.add("hidden");
            row.className = "grid grid-cols-1 gap-2";
          }
        }
        var headerProceed = document.getElementById("pos-cart-proceed");
        if (headerProceed) {
          if (pref === "hold" && !forcedHold) {
            headerProceed.classList.add("hidden");
          } else {
            headerProceed.classList.remove("hidden");
          }
        }
        try {
          if (typeof window.applyPosCartUiSettings === "function") {
            window.applyPosCartUiSettings();
          }
        } catch (eCartUi) {}
        try {
          if (typeof window.updateProceedState === "function") {
            window.updateProceedState();
          }
        } catch (eUpd) {}
      }

      function loadHoldState() {
        try {
          var raw = localStorage.getItem(HOLD_STATE_KEY);
          if (!raw) return null;
          var obj = JSON.parse(raw);
          if (!obj || typeof obj !== "object") return null;
          var hid = parseInt(obj.hold_id, 10);
          if (!hid || hid <= 0) return null;
          return {
            hold_id: hid,
            saves_count: parseInt(obj.saves_count || 0, 10) || 0,
            committed: obj.committed && typeof obj.committed === "object" ? obj.committed : {},
            table_label: normalizeHoldTableLabel(obj.table_label || ""),
          };
        } catch (e) {
          return null;
        }
      }

      function saveHoldState(state) {
        if (!state || !state.hold_id) {
          try { localStorage.removeItem(HOLD_STATE_KEY); } catch (e) {}
          window.__POS_HELD_ORDER_ID = null;
          return;
        }
        try {
          localStorage.setItem(HOLD_STATE_KEY, JSON.stringify(state));
        } catch (e) {}
        window.__POS_HELD_ORDER_ID = Number(state.hold_id) || null;
      }

      function holdTableInputEl() {
        return document.getElementById("pos-cart-hold-table-number");
      }

      function normalizeHoldTableLabel(v) {
        return String(v == null ? "" : v).trim().slice(0, 40);
      }

      function readHoldTableLabelFromInput() {
        var el = holdTableInputEl();
        return normalizeHoldTableLabel(el ? el.value : "");
      }

      function writeHoldTableLabelToInput(v) {
        var el = holdTableInputEl();
        if (!el) return;
        el.value = normalizeHoldTableLabel(v);
      }

      function readLocalCart() {
        try {
          var raw = localStorage.getItem(CART_LS_KEY);
          if (!raw) return [];
          var arr = JSON.parse(raw);
          return Array.isArray(arr) ? arr : [];
        } catch (e) {
          return [];
        }
      }

      function writeLocalCart(lines) {
        try {
          localStorage.setItem(CART_LS_KEY, JSON.stringify(lines || []));
        } catch (e) {}
      }

      function toastSay(msg) {
        if (typeof window.showToast === "function") {
          window.showToast(msg);
          return;
        }
        try { console.log("[POS]", msg); } catch (e) {}
      }

      function totalsForLines(lines) {
        var subtotal = 0;
        var items = 0;
        (lines || []).forEach(function (l) {
          var qty = parseFloat((l && l.qty) || 0) || 0;
          var unit = parseFloat((l && l.price) || 0) || 0;
          subtotal += qty * unit;
          items += qty;
        });
        var grand = subtotal;
        try {
          if (typeof window.computePosTax === "function") {
            grand = window.computePosTax(subtotal).grand;
          }
        } catch (e) {}
        return { subtotal: Math.round(subtotal * 100) / 100, grand: Math.round(grand * 100) / 100, item_count: items };
      }

      function readAuthorizedEmployee() {
        var emp = (window && window.authorizedEmployee) || null;
        if (emp && emp.employee_code) {
          return {
            id: emp.id || null,
            employee_code: String(emp.employee_code || "").trim(),
            full_name: emp.full_name || "",
          };
        }
        var inp = document.getElementById("pos-auth-code");
        var code = inp ? String(inp.value || "").trim() : "";
        if (/^\d{6}$/.test(code)) {
          return { id: null, employee_code: code, full_name: "" };
        }
        return null;
      }

      function applyHoldCountBadges(count) {
        var dot = document.getElementById("pos-hold-count-dot");
        var fabBadge = document.getElementById("pos-hold-fab-badge");
        var fab = document.getElementById("pos-hold-toggle-fab");
        var n = Math.max(0, parseInt(count, 10) || 0);
        var text = n > 9 ? "9+" : String(n);
        if (dot) {
          if (n <= 0) {
            dot.classList.add("hidden");
            dot.classList.remove("inline-flex");
          } else {
            dot.textContent = text;
            dot.classList.remove("hidden");
            dot.classList.add("inline-flex");
          }
        }
        if (fabBadge) {
          if (n <= 0) {
            fabBadge.classList.add("hidden");
            fabBadge.classList.remove("flex");
          } else {
            fabBadge.textContent = text;
            fabBadge.classList.remove("hidden");
            fabBadge.classList.add("flex");
          }
        }
        if (fab) {
          fab.classList.toggle("pos-hold-btn--live", n > 0);
          fab.setAttribute(
            "aria-label",
            n > 0 ? ("Open held orders (" + n + " open)") : "Open held orders"
          );
        }
      }

      function refreshHoldCountBadge() {
        fetch(HOLD_LIST_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            var arr = (j && j.ok && Array.isArray(j.held_orders)) ? j.held_orders : [];
            applyHoldCountBadges(arr.length);
          })
          .catch(function () {});
      }

      // Read the current open-held-orders count off the FAB badge text and
      // bump it by `delta` immediately. This gives the cashier instant feedback
      // after saving a new held order — the authoritative count from the
      // server arrives a moment later via refreshHoldCountBadge().
      function bumpHoldCountBadgeOptimistically(delta) {
        var d = parseInt(delta, 10);
        if (!isFinite(d) || d === 0) return;
        var fabBadge = document.getElementById("pos-hold-fab-badge");
        var dot = document.getElementById("pos-hold-count-dot");
        var srcEl = fabBadge && !fabBadge.classList.contains("hidden")
          ? fabBadge
          : (dot && !dot.classList.contains("hidden") ? dot : null);
        var cur = 0;
        if (srcEl) {
          var raw = String(srcEl.textContent || "").trim();
          if (/^\d+$/.test(raw)) cur = parseInt(raw, 10);
          else if (raw === "9+") cur = 9;
        }
        applyHoldCountBadges(Math.max(0, cur + d));
      }

      // Briefly highlight the held-orders FAB so the cashier sees that the
      // save landed. The animation is keyframe-driven (see .pos-hold-flash CSS)
      // and self-removes so it can be retriggered on every save.
      function flashHoldFab() {
        var fab = document.getElementById("pos-hold-toggle-fab");
        if (!fab) return;
        try {
          fab.classList.remove("pos-hold-flash");
          // Force a reflow so re-adding the class restarts the animation.
          // eslint-disable-next-line no-unused-expressions
          fab.offsetWidth;
          fab.classList.add("pos-hold-flash");
          setTimeout(function () {
            try { fab.classList.remove("pos-hold-flash"); } catch (eR) {}
          }, 1100);
        } catch (e) {}
      }

      function holdSaveNeedsReductionApproval(cartLines, committed) {
        if (!committed || typeof committed !== "object") return false;
        var byId = {};
        (cartLines || []).forEach(function (l) {
          if (!l || l.id == null) return;
          byId[String(l.id)] = parseFloat(l.qty) || 0;
        });
        var k;
        for (k in committed) {
          if (!Object.prototype.hasOwnProperty.call(committed, k)) continue;
          var prev = parseFloat(committed[k] || 0) || 0;
          if (prev <= 0) continue;
          var cur = Object.prototype.hasOwnProperty.call(byId, k) ? byId[k] : 0;
          if (cur + 1e-9 < prev) return true;
        }
        return false;
      }

      function updateHoldReductionApprovalVisibility() {
        var wrap = document.getElementById("pos-cart-hold-reduction-wrap");
        var inp = document.getElementById("pos-cart-hold-reduction-approver");
        if (!wrap) return;
        if (window.__POS_CHECKOUT_PATH !== "hold") {
          wrap.classList.add("hidden");
          if (inp) inp.value = "";
          return;
        }
        var st = loadHoldState();
        if (!st || !st.hold_id || !st.committed) {
          wrap.classList.add("hidden");
          if (inp) inp.value = "";
          return;
        }
        var lines = readLocalCart();
        var mapped = lines.map(function (l) {
          return { id: l && l.id, qty: parseFloat((l && l.qty) || 0) || 0 };
        });
        var need = holdSaveNeedsReductionApproval(mapped, st.committed);
        wrap.classList.toggle("hidden", !need);
        if (!need && inp) inp.value = "";
      }

      function syncHoldEditReductionApprovalUI(panel) {
        var wrap = panel && panel.querySelector ? panel.querySelector("[data-role='edit-reduction-approval']") : null;
        var inp = panel && panel.querySelector ? panel.querySelector("[data-role='edit-reduction-approver']") : null;
        if (!wrap || !inp) return;
        var lines = (panel.__lines || []).map(function (ln) {
          return { id: ln && ln.id, qty: parseFloat((ln && ln.qty) || 0) || 0 };
        });
        var need = holdSaveNeedsReductionApproval(lines, panel.__committed || {});
        wrap.classList.toggle("hidden", !need);
        if (!need) inp.value = "";
      }

      function updateBanner() {
        var banner = document.getElementById("pos-cart-hold-banner");
        var idEl = document.getElementById("pos-cart-hold-banner-id");
        var metaEl = document.getElementById("pos-cart-hold-banner-meta");
        if (banner) {
          var st = loadHoldState();
          if (!st) {
            banner.classList.add("hidden");
            window.__POS_HELD_ORDER_ID = null;
          } else {
            window.__POS_HELD_ORDER_ID = Number(st.hold_id) || null;
            var committedQty = 0;
            try {
              Object.keys(st.committed || {}).forEach(function (k) {
                committedQty += parseFloat(st.committed[k] || 0) || 0;
              });
            } catch (e) {}
            if (idEl) idEl.textContent = formatHoldOrderNumber(st.hold_id);
            if (metaEl) metaEl.textContent = "Items committed: " + String(committedQty) + " · Saves: " + String(st.saves_count || 0);
            banner.classList.remove("hidden");
          }
        } else {
          window.__POS_HELD_ORDER_ID = null;
        }
        var stx = loadHoldState();
        if (!stx || !stx.hold_id) {
          writeHoldTableLabelToInput("");
        } else {
          writeHoldTableLabelToInput(stx.table_label || "");
        }
        applyCheckoutToolbar();
        try {
          updateHoldReductionApprovalVisibility();
        } catch (eW) {}
      }

      function recomputeEditPanelTotals(panel) {
        var lines = panel.__lines || [];
        var totalAmt = 0;
        var totalQty = 0;
        lines.forEach(function (ln) {
          var qty = parseFloat(ln.qty || 0) || 0;
          var price = parseFloat(ln.price || 0) || 0;
          totalAmt += qty * price;
          totalQty += qty;
        });
        var amtEl = panel.querySelector("[data-role='edit-total-amt']");
        var qtyEl = panel.querySelector("[data-role='edit-total-qty']");
        if (amtEl) amtEl.textContent = totalAmt.toFixed(2);
        if (qtyEl) qtyEl.textContent = String(Math.round(totalQty * 1000) / 1000);
        panel.__totals = { amount: totalAmt, qty: totalQty };
        if (typeof panel.__refreshSaveBtnState === "function") {
          try { panel.__refreshSaveBtnState(); } catch (e) {}
        }
      }

      function rerenderEditLines(panel) {
        var lineHost = panel.querySelector("[data-role='edit-lines']");
        var empty = panel.querySelector("[data-role='edit-empty']");
        var committed = panel.__committed || {};
        if (!lineHost) return;
        // Drop lines at qty 0 with nothing committed — avoids stale rows until refresh.
        panel.__lines = (panel.__lines || []).filter(function (ln) {
          if (!ln || !ln.id) return false;
          var q = parseFloat(ln.qty || 0) || 0;
          var c = parseFloat(committed[String(ln.id)] || 0) || 0;
          return q > 0 || c > 0;
        });
        var lines = panel.__lines || [];
        lineHost.innerHTML = "";
        if (!lines.length) {
          if (empty) empty.classList.remove("hidden");
          recomputeEditPanelTotals(panel);
          return;
        }
        if (empty) empty.classList.add("hidden");
        lines.forEach(function (ln, idx) {
          var committedQty = parseFloat(committed[String(ln.id)] || 0) || 0;
          var row = document.createElement("div");
          row.className = "pos-hold-line";

          var nameBox = document.createElement("div");
          nameBox.className = "min-w-0";
          var nameEl = document.createElement("p");
          nameEl.className = "truncate text-[13px] font-bold text-[rgb(var(--rc-page-fg))]";
          nameEl.textContent = String(ln.name || "Item");
          var priceEl = document.createElement("p");
          priceEl.className = "truncate text-[11px] font-semibold text-[rgb(var(--rc-primary))]";
          priceEl.textContent = Number(ln.price || 0).toFixed(2);
          nameBox.appendChild(nameEl);
          nameBox.appendChild(priceEl);

          var trailing = document.createElement("div");
          trailing.className = "pos-hold-line__lock flex items-center";

          var qtyBox = document.createElement("div");
          qtyBox.className = "pos-hold-line__qty pos-hold-stepper" + (committedQty > 0 ? " pos-hold-stepper--committed" : "");

          var minus = document.createElement("button");
          minus.type = "button";
          minus.setAttribute("aria-label", "Decrease quantity");
          minus.innerHTML =
            '<svg class="h-3 w-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
            '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="3" d="M5 12h14" />' +
            '</svg>';

          var qtyIn = document.createElement("input");
          qtyIn.type = "number";
          qtyIn.step = "any";
          qtyIn.min = "0";
          qtyIn.value = String(parseFloat(ln.qty || 0) || 0);
          qtyIn.setAttribute("aria-label", "Quantity for " + (ln.name || "item"));

          var plus = document.createElement("button");
          plus.type = "button";
          plus.setAttribute("aria-label", "Increase quantity");
          plus.innerHTML =
            '<svg class="h-3 w-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
            '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="3" d="M12 5v14M5 12h14" />' +
            '</svg>';

          function setQty(newVal) {
            var v = parseFloat(newVal);
            if (!isFinite(v) || v < 0) v = 0;
            if (v <= 0 && committedQty <= 0) {
              panel.__lines.splice(idx, 1);
              rerenderEditLines(panel);
              return;
            }
            ln.qty = v;
            ln.total = (parseFloat(ln.price || 0) || 0) * v;
            qtyIn.value = String(v);
            recomputeEditPanelTotals(panel);
            syncHoldEditReductionApprovalUI(panel);
            if (typeof panel.__scheduleAutoSave === "function") {
              try { panel.__scheduleAutoSave(); } catch (eAuto) {}
            }
          }
          minus.addEventListener("click", function () { setQty((parseFloat(qtyIn.value || 0) || 0) - 1); });
          plus.addEventListener("click", function () { setQty((parseFloat(qtyIn.value || 0) || 0) + 1); });
          qtyIn.addEventListener("input", function () {
            var raw = String(qtyIn.value || "").trim();
            if (raw === "" || raw === "." || raw === "-" || raw === "-.") return;
            setQty(raw);
          });
          qtyIn.addEventListener("change", function () { setQty(qtyIn.value); });
          qtyIn.addEventListener("blur", function () { setQty(qtyIn.value); });

          qtyBox.appendChild(minus);
          qtyBox.appendChild(qtyIn);
          qtyBox.appendChild(plus);

          if (committedQty > 0) {
            var lock = document.createElement("span");
            lock.className =
              "inline-flex items-center justify-center rounded-md text-rose-500/80 dark:text-rose-300/80";
            lock.setAttribute("title", committedQty + " committed to stock — use manager code on save to reduce (returns stock).");
            lock.setAttribute("aria-label", "Committed qty " + committedQty + " — manager approval required to reduce");
            lock.innerHTML =
              '<svg class="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
              '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.25" d="M12 11c-.6 0-1 .4-1 1v2c0 .6.4 1 1 1s1-.4 1-1v-2c0-.6-.4-1-1-1zm6-4V6a6 6 0 10-12 0v1H4v13h16V7h-2zm-8 0V6a4 4 0 118 0v1H10z" />' +
              '</svg>';
            trailing.appendChild(lock);
          } else {
            var rm = document.createElement("button");
            rm.type = "button";
            rm.className =
              "inline-flex h-6 w-6 items-center justify-center rounded-md text-rose-500/80 transition hover:bg-rose-500/12 hover:text-rose-600 dark:text-rose-300/80";
            rm.setAttribute("aria-label", "Remove line");
            rm.setAttribute("title", "Remove");
            rm.innerHTML =
              '<svg class="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
              '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.25" d="M6 18L18 6M6 6l12 12" />' +
              '</svg>';
            rm.addEventListener("click", function () {
              panel.__lines.splice(idx, 1);
              rerenderEditLines(panel);
            });
            trailing.appendChild(rm);
          }

          row.appendChild(nameBox);
          row.appendChild(qtyBox);
          row.appendChild(trailing);
          lineHost.appendChild(row);
        });
        recomputeEditPanelTotals(panel);
        syncHoldEditReductionApprovalUI(panel);
        if (typeof panel.__scheduleAutoSave === "function") {
          try { panel.__scheduleAutoSave(); } catch (eAutoAll) {}
        }
      }

      function openHoldEditPanel(panel, holdId) {
        panel.classList.remove("hidden");
        panel.innerHTML =
          '<p class="text-[11px] italic text-[rgb(var(--rc-muted))]">Loading items…</p>';
        fetch(HOLD_GET_BASE + String(holdId), { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok || !j.hold) {
              panel.innerHTML =
                '<p class="text-[11px] text-rose-600 dark:text-rose-400">' +
                String((j && j.error) || "Could not load this held order.") +
                "</p>";
              return;
            }
            var hold = j.hold;
            panel.__holdId = Number(hold.id);
            panel.__customerName = hold.customer_name || "";
            panel.__customerPhone = hold.customer_phone || "";
            panel.__committed = hold.committed || {};
            panel.__createdEmployeeCode = String(hold.employee_code || "").trim();
            panel.__createdEmployeeName = String(hold.employee_name || "").trim();
            panel.__lines = (Array.isArray(hold.lines) ? hold.lines : []).map(function (ln) {
              return {
                id: parseInt(ln.id, 10),
                name: String(ln.name || "Item"),
                qty: parseFloat(ln.qty || 0) || 0,
                price: parseFloat(ln.price || 0) || 0,
                total: parseFloat(ln.total || 0) || 0,
              };
            }).filter(function (ln) { return ln.id && ln.id > 0; });

            panel.innerHTML =
              '<div class="space-y-2.5">' +
              '  <div class="flex items-center justify-end gap-2">' +
              '    <span class="pos-hold-chip pos-hold-chip--amt tabular-nums" data-role="edit-total-amt-wrap"><span data-role="edit-total-amt">0.00</span></span>' +
              '    <span class="pos-hold-chip"><span class="tabular-nums" data-role="edit-total-qty">0</span> qty</span>' +
              "  </div>" +
              '  <div data-role="edit-lines" class="space-y-1.5"></div>' +
              '  <p data-role="edit-empty" class="hidden py-2 text-center text-xs text-[rgb(var(--rc-muted))]">No items yet — tap Add items.</p>' +
              '  <div class="flex justify-center">' +
              '    <button type="button" data-role="edit-add-from-pos" class="pos-hold-btn-primary w-full max-w-[12rem]">' +
              '      <svg class="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
              '        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4" />' +
              '      </svg>' +
              '      <span>Add items</span>' +
              "    </button>" +
              "  </div>" +
              '  <div class="pos-hold-edit-footer">' +
              '    <div class="flex flex-wrap items-center gap-2">' +
              '      <p class="min-w-0 flex-1 text-[11px] leading-snug text-[rgb(var(--rc-muted))]">Auto-saves under your code.</p>' +
              '      <span data-role="edit-code-state" class="hidden items-center rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wide"></span>' +
              '      <button type="button" data-role="edit-save-btn" class="pos-hold-btn-load shrink-0 disabled:cursor-not-allowed disabled:opacity-50" aria-label="Save changes">' +
              '        <svg class="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
              '          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />' +
              '        </svg>' +
              '        <span>Save</span>' +
              '      </button>' +
              "    </div>" +
              '    <p data-role="edit-status" id="pos-hold-edit-status" class="mt-1.5 hidden text-[11px] text-rose-600 dark:text-rose-400"></p>' +
              '    <div data-role="edit-reduction-approval" class="hidden mt-2.5 space-y-1.5 rounded-lg bg-amber-500/10 px-2.5 py-2">' +
              '      <label class="block text-[10px] font-bold uppercase tracking-wide text-amber-800 dark:text-amber-200">Manager approval</label>' +
              '      <p class="text-[10px] leading-snug text-[rgb(var(--rc-muted))]">Needed when reducing committed stock.</p>' +
              '      <input type="password" maxlength="6" inputmode="numeric" autocomplete="one-time-code" data-role="edit-reduction-approver" class="w-full rounded-xl border-2 border-amber-500/35 bg-[rgb(var(--rc-surface))] px-3 py-2 text-center font-mono text-sm tracking-[0.35em] text-[rgb(var(--rc-page-fg))] focus:border-amber-500 focus:outline-none" placeholder="••••••" />' +
              "    </div>" +
              "  </div>" +
              "</div>";

            rerenderEditLines(panel);

            var addFromPosBtn = panel.querySelector("[data-role='edit-add-from-pos']");
            if (addFromPosBtn) {
              addFromPosBtn.addEventListener("click", function () {
                enterHoldPickMode(panel);
              });
            }

            // The held-orders modal is gated by a single 6-digit code (HOLD_SESSION).
            // Saves reuse that session — no per-action retype — and the live "Save"
            // button (plus an auto-save after edits) writes through whenever there's
            // a delta and the session is the order's creator.
            var saveBtn = panel.querySelector("[data-role='edit-save-btn']");
            var codeEl = null;
            var statusEl = panel.querySelector("[data-role='edit-status']");
            var codeStateEl = panel.querySelector("[data-role='edit-code-state']");

            function setStatus(msg, kind) {
              if (!statusEl) return;
              if (!msg) {
                statusEl.classList.add("hidden");
                statusEl.textContent = "";
                statusEl.classList.remove("text-emerald-600", "dark:text-emerald-300");
                statusEl.classList.remove("text-rose-600", "dark:text-rose-400");
                return;
              }
              statusEl.textContent = msg;
              statusEl.classList.remove("hidden");
              if (kind === "ok") {
                statusEl.classList.remove("text-rose-600", "dark:text-rose-400");
                statusEl.classList.add("text-emerald-600", "dark:text-emerald-300");
              } else {
                statusEl.classList.remove("text-emerald-600", "dark:text-emerald-300");
                statusEl.classList.add("text-rose-600", "dark:text-rose-400");
              }
            }

            function setCodeChip(text, kind) {
              if (!codeStateEl) return;
              if (!text) {
                codeStateEl.classList.add("hidden");
                codeStateEl.classList.remove("inline-flex");
                codeStateEl.textContent = "";
                return;
              }
              codeStateEl.textContent = text;
              codeStateEl.classList.remove("hidden");
              codeStateEl.classList.add("inline-flex");
              codeStateEl.classList.remove(
                "border-emerald-500/55",
                "bg-emerald-500/15",
                "text-emerald-700",
                "dark:text-emerald-300"
              );
              codeStateEl.classList.remove(
                "border-rose-500/55",
                "bg-rose-500/12",
                "text-rose-700",
                "dark:text-rose-300"
              );
              codeStateEl.classList.remove(
                "border-amber-500/50",
                "bg-amber-500/10",
                "text-amber-800",
                "dark:text-amber-200"
              );
              if (kind === "ok") {
                codeStateEl.classList.add(
                  "border-emerald-500/55",
                  "bg-emerald-500/15",
                  "text-emerald-700",
                  "dark:text-emerald-300"
                );
              } else if (kind === "bad") {
                codeStateEl.classList.add(
                  "border-rose-500/55",
                  "bg-rose-500/12",
                  "text-rose-700",
                  "dark:text-rose-300"
                );
              } else if (kind === "pending") {
                codeStateEl.classList.add(
                  "border-amber-500/50",
                  "bg-amber-500/10",
                  "text-amber-800",
                  "dark:text-amber-200"
                );
              }
            }

            function sessionCode() {
              return (HOLD_SESSION && HOLD_SESSION.employee_code) || "";
            }

            function sessionIsCreator() {
              var code = sessionCode();
              if (!code) return false;
              var creatorCode = String(panel.__createdEmployeeCode || "").trim();
              if (!creatorCode) return true;
              return code === creatorCode;
            }

            panel.__autoSaveSig = "";
            panel.__autoSaveTimer = null;

            function collectSaveDraft() {
              var comm = panel.__committed || {};
              var lines = (panel.__lines || []).filter(function (ln) {
                if (!ln || !ln.id) return false;
                var q = parseFloat(ln.qty || 0) || 0;
                var c = parseFloat(comm[String(ln.id)] || 0) || 0;
                return q > 0 || c > 0;
              });
              var saveLines = lines.map(function (ln) {
                return {
                  id: ln.id,
                  name: ln.name,
                  qty: parseFloat(ln.qty) || 0,
                  price: parseFloat(ln.price) || 0,
                  total: (parseFloat(ln.qty) || 0) * (parseFloat(ln.price) || 0),
                };
              });
              var needAppr = holdSaveNeedsReductionApproval(saveLines, panel.__committed || {});
              var apprEl = panel.querySelector("[data-role='edit-reduction-approver']");
              var apprCode = apprEl ? String(apprEl.value || "").trim() : "";
              return { lines: lines, saveLines: saveLines, needAppr: needAppr, apprEl: apprEl, apprCode: apprCode };
            }

            function buildSaveSig(saveLines, needAppr, apprCode) {
              var slim = saveLines.map(function (l) {
                return [Number(l.id || 0), Number(l.qty || 0), Number(l.price || 0)];
              });
              return JSON.stringify([slim, !!needAppr, needAppr ? apprCode : ""]);
            }

            function refreshSaveBtnState() {
              if (!saveBtn) return;
              var d = collectSaveDraft();
              if (!sessionCode()) {
                setCodeChip("Not signed in", "bad");
                saveBtn.disabled = true;
                return;
              }
              if (!sessionIsCreator()) {
                setCodeChip("Not your order", "bad");
                saveBtn.disabled = true;
                return;
              }
              if (!d.lines.length) {
                setCodeChip("Empty", "");
                saveBtn.disabled = true;
                return;
              }
              if (d.needAppr) {
                if (!/^\d{6}$/.test(d.apprCode)) setCodeChip("Approval needed", "bad");
                else setCodeChip("Ready", "ok");
              } else {
                setCodeChip("Ready", "ok");
              }
              saveBtn.disabled = panel.__saveInFlight === true;
            }
            panel.__refreshSaveBtnState = refreshSaveBtnState;

            function scheduleAutoSave() {
              if (panel.__saveInFlight) return;
              var d = collectSaveDraft();
              if (!sessionCode() || !sessionIsCreator() || !d.lines.length) return;
              var sig = buildSaveSig(d.saveLines, d.needAppr, d.apprCode);
              if (panel.__autoSaveSig === sig) return;
              if (panel.__autoSaveTimer) {
                clearTimeout(panel.__autoSaveTimer);
                panel.__autoSaveTimer = null;
              }
              panel.__autoSaveTimer = setTimeout(function () {
                panel.__autoSaveTimer = null;
                doSave({ auto: true });
              }, 900);
            }
            panel.__scheduleAutoSave = scheduleAutoSave;

            function doSave(opts) {
              opts = opts || {};
              var auto = !!opts.auto;
              if (!saveBtn) return;
              if (panel.__saveInFlight) return;
              var code = sessionCode();
              if (!/^\d{6}$/.test(code)) {
                if (!auto) setStatus("Not signed in — re-enter your 6-digit code.", "bad");
                setCodeChip("Not signed in", "bad");
                return;
              }
              if (!sessionIsCreator()) {
                if (!auto) {
                  setStatus(
                    "Not your order — only " +
                      (panel.__createdEmployeeName || "the cashier who created it") +
                      " can update this order.",
                    "bad"
                  );
                }
                setCodeChip("Not your order", "bad");
                return;
              }
              var d = collectSaveDraft();
              if (!d.lines.length) {
                if (!auto) setStatus("Add at least one item before saving.", "bad");
                return;
              }
              var proceed = function () {
                if (typeof window.posSalesAllowed === "function" && !window.posSalesAllowed()) {
                  var closedMsg = typeof window.__posShopClosedMessage === "function" ? window.__posShopClosedMessage() : "Shop is closed.";
                  if (!auto) setStatus(closedMsg, "bad");
                  else {
                    try { toastSay(closedMsg); } catch (eToastClosed) {}
                  }
                  return;
                }
                setStatus(auto ? "Auto-saving…" : "Saving…", "ok");
                panel.__saveInFlight = true;
                saveBtn.disabled = true;
                var postBody = {
                  hold_id: panel.__holdId,
                  lines: d.saveLines,
                  employee: { employee_code: code },
                  total_amount: panel.__totals ? panel.__totals.amount : 0,
                  item_count: panel.__totals ? panel.__totals.qty : 0,
                  customer_name: panel.__customerName || "",
                  customer_phone: panel.__customerPhone || "",
                };
                if (d.needAppr) postBody.reduction_approver_code = d.apprCode;
                fetch(HOLD_SAVE_API, {
                  method: "POST",
                  credentials: "same-origin",
                  headers: { "Content-Type": "application/json", Accept: "application/json" },
                  body: JSON.stringify(postBody),
                })
                  .then(function (r) { return r.json().catch(function () { return {}; }); })
                  .then(function (j) {
                    if (!j || !j.ok) {
                      var msg = (j && j.error) || "Could not save changes.";
                      if (!auto) setStatus(msg, "bad");
                      else {
                        try { toastSay(msg); } catch (eToastErr) {}
                      }
                      setCodeChip(/not your order/i.test(msg) ? "Not your order" : "Error", "bad");
                      return;
                    }
                    panel.__autoSaveSig = buildSaveSig(d.saveLines, d.needAppr, d.apprCode);
                    setCodeChip(auto ? "Auto-saved" : "Saved", "ok");
                    if (j && j.ok && Array.isArray(j.delta_lines) && j.delta_lines.length) {
                      try { printHoldCompanyCopy(j.delta_lines, panel.__holdId); } catch (ePrint) {}
                    }
                    if (j.committed) panel.__committed = j.committed;
                    var cur = loadHoldState();
                    if (cur && Number(cur.hold_id) === Number(panel.__holdId)) {
                      saveHoldState({
                        hold_id: cur.hold_id,
                        saves_count: (cur.saves_count || 0) + 1,
                        committed: j.committed || {},
                      });
                      updateBanner();
                    }
                    var statParts = [auto ? "Auto-saved" : "Saved"];
                    if (j.delta_lines && j.delta_lines.length) statParts.push(j.delta_lines.length + " new line(s) deducted from stock");
                    if (j.reduction_lines && j.reduction_lines.length) statParts.push(j.reduction_lines.length + " line(s) returned to stock");
                    setStatus(statParts.join(" · ") + ".", "ok");
                    if (!auto) {
                      var toastParts = ["Saved changes to " + formatHoldOrderNumber(panel.__holdId)];
                      if (j.delta_lines && j.delta_lines.length) toastParts.push(j.delta_lines.length + " new line(s) deducted from stock");
                      if (j.reduction_lines && j.reduction_lines.length) toastParts.push(j.reduction_lines.length + " line(s) returned to stock");
                      toastSay(toastParts.join(" · ") + ".");
                    }
                    fetchAndRenderList();
                    populateCartHoldPicker();
                    rerenderEditLines(panel);
                  })
                  .catch(function () {
                    if (!auto) setStatus("Could not reach server.", "bad");
                  })
                  .finally(function () {
                    panel.__saveInFlight = false;
                    refreshSaveBtnState();
                  });
              };

              if (d.needAppr) {
                if (!/^\d{6}$/.test(d.apprCode)) {
                  if (!auto) {
                    setStatus(
                      "Reducing committed quantities: enter a manager, admin, company manager, IT support, or super admin 6-digit approval code.",
                      "bad"
                    );
                    setCodeChip("Approval needed", "bad");
                    if (d.apprEl) {
                      try { d.apprEl.focus(); } catch (eF) {}
                    }
                  }
                  return;
                }
              }
              proceed();
            }
            panel.__doSave = doSave;

            refreshSaveBtnState();
            if (saveBtn) {
              saveBtn.addEventListener("click", function () { doSave(); });
            }
            var apprInputEl = panel.querySelector("[data-role='edit-reduction-approver']");
            if (apprInputEl) {
              apprInputEl.addEventListener("input", function () {
                apprInputEl.value = String(apprInputEl.value || "").replace(/[^\d]/g, "").slice(0, 6);
                refreshSaveBtnState();
                scheduleAutoSave();
              });
              apprInputEl.addEventListener("blur", function () {
                refreshSaveBtnState();
              });
            }
          })
          .catch(function () {
            panel.innerHTML =
              '<p class="text-[11px] text-rose-600 dark:text-rose-400">Could not reach the server.</p>';
          });
      }

      // Floating "pick from POS catalog" mode bar — visible only while the user is staging
      // adds onto an held order's edit panel by clicking item cards on the POS page directly.
      function setHoldPickBarHostVisible(show) {
        var host = document.getElementById("pos-hold-pick-bar-host");
        if (host) {
          host.classList.add("hidden");
          host.setAttribute("aria-hidden", "true");
        }
        var bar = document.getElementById("pos-hold-pick-bar");
        if (!bar) return;
        bar.classList.toggle("hidden", !show);
        bar.classList.toggle("pos-hold-pick-bar--floating", !!show);
        bar.setAttribute("aria-hidden", show ? "false" : "true");
        if (show) {
          document.body.appendChild(bar);
        }
      }

      function ensureHoldPickBar() {
        var host = document.getElementById("pos-hold-pick-bar-host");
        var bar = document.getElementById("pos-hold-pick-bar");
        if (bar) return bar;
        bar = document.createElement("div");
        bar.id = "pos-hold-pick-bar";
        bar.className = "pos-hold-pick-bar pos-hold-pick-bar--floating hidden";
        bar.setAttribute("role", "toolbar");
        bar.setAttribute("aria-label", "Pick items into held order");
        bar.innerHTML =
          '<div class="pos-hold-pick-bar__inner">' +
          '  <span class="pos-hold-pick-bar__icon" aria-hidden="true">' +
          '    <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">' +
          '      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.25" d="M12 4v16m8-8H4" />' +
          '    </svg>' +
          '  </span>' +
          '  <div class="pos-hold-pick-bar__meta min-w-0">' +
          '    <span class="pos-hold-pick-bar__label">Adding to held order</span>' +
          '    <span class="pos-hold-pick-bar__id truncate" data-role="pick-bar-id">ORD-0000</span>' +
          '    <span class="pos-hold-pick-bar__hint">Tap items on the catalog below</span>' +
          '  </div>' +
          '  <div class="pos-hold-pick-bar__qty-wrap">' +
          '    <span class="pos-hold-pick-bar__qty-label">Per tap</span>' +
          '    <input type="number" data-role="pick-bar-qty" min="0.0001" step="any" value="1" class="pos-hold-pick-bar__qty" aria-label="Quantity per tap" title="Qty per tap" />' +
          '  </div>' +
          '  <div class="pos-hold-pick-bar__added-wrap">' +
          '    <span class="pos-hold-pick-bar__added-label">Added</span>' +
          '    <span data-role="pick-bar-added" class="pos-hold-pick-bar__counter">0</span>' +
          '  </div>' +
          '  <button type="button" data-role="pick-bar-done" class="pos-hold-pick-bar__done">' +
          '    <svg class="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
          '      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7" />' +
          '    </svg>' +
          '    <span>Done</span>' +
          '  </button>' +
          '</div>';
        document.body.appendChild(bar);
        return bar;
      }

      function enterHoldPickMode(panel) {
        if (!panel || !panel.__holdId) return;
        window.__POS_ACTIVE_HOLD_PANEL = panel;
        var bar = ensureHoldPickBar();
        var idEl = bar.querySelector("[data-role='pick-bar-id']");
        var qtyInput = bar.querySelector("[data-role='pick-bar-qty']");
        var addedEl = bar.querySelector("[data-role='pick-bar-added']");
        var doneBtn = bar.querySelector("[data-role='pick-bar-done']");
        if (idEl) idEl.textContent = formatHoldOrderNumber(panel.__holdId);
        if (qtyInput) qtyInput.value = "1";
        var addedCount = 0;
        function setAdded(n) {
          addedCount = n;
          if (addedEl) {
            addedEl.textContent = String(n);
            addedEl.classList.toggle("pos-hold-pick-bar__counter--active", n > 0);
          }
        }
        setAdded(0);

        // Hide the modal (keep cashier session) so the catalogue is usable for picking.
        try {
          setHoldModalOpen(false, { keepSession: true });
        } catch (eClose) {
          var modal = document.getElementById("pos-hold-modal");
          if (modal) {
            modal.classList.add("hidden");
            modal.setAttribute("aria-hidden", "true");
          }
        }
        bar.classList.remove("hidden");
        setHoldPickBarHostVisible(true);
        document.body.classList.add("pos-hold-pick-active");
        try {
          toastSay("Adding to " + formatHoldOrderNumber(panel.__holdId) + " — tap items on the POS, then Done.");
        } catch (eToast) {}

        window.__posHoldPickHandler = function (btn) {
          if (typeof window.posSalesAllowed === "function" && !window.posSalesAllowed()) {
            try {
              toastSay(typeof window.__posShopClosedMessage === "function" ? window.__posShopClosedMessage() : "Shop is closed.");
            } catch (eClosedPick) {}
            return true;
          }
          try {
            if (!panel || !panel.__holdId) return false;
            var id = parseInt(btn.getAttribute("data-item-id"), 10);
            if (!id || id <= 0) return false;
            var name = btn.getAttribute("data-name") || "";
            var price = parseFloat(btn.getAttribute("data-price") || "0");
            if (!isFinite(price) || price < 0) price = 0;
            var qty = parseFloat((qtyInput && qtyInput.value) || "1");
            if (!isFinite(qty) || qty <= 0) qty = 1;
            qty = Math.round(qty * 1000) / 1000;
            var stockCap =
              typeof posSellingCapFromItemCard === "function" ? posSellingCapFromItemCard(btn) : 999999999;
            if (!panel.__lines || !Array.isArray(panel.__lines)) panel.__lines = [];
            var existing = panel.__lines.find(function (ln) { return ln && ln.id === id; });
            var curQty = existing ? parseFloat(existing.qty || 0) || 0 : 0;
            if (stockCap === 0 && curQty <= 0) {
              toastSay("Out of stock — cannot add " + (name || "item") + ".");
              return true;
            }
            var nextQty = curQty + qty;
            if (typeof stockCap === "number" && stockCap > 0 && stockCap < 999999998 && nextQty > stockCap) {
              if (curQty >= stockCap) {
                toastSay("Stock limit reached for " + (name || "item") + ".");
                return true;
              }
              nextQty = stockCap;
              qty = Math.round((nextQty - curQty) * 1000) / 1000;
              if (qty <= 0) return true;
            }
            if (existing) {
              existing.qty = nextQty;
              existing.total = (parseFloat(existing.price || 0) || 0) * nextQty;
            } else {
              panel.__lines.push({
                id: id,
                name: name,
                qty: qty,
                price: price,
                total: price * qty,
              });
            }
            panel.__pickDirty = true;
            setAdded(addedCount + 1);
            try {
              btn.classList.add("pos-hold-pick-ping");
              setTimeout(function () { btn.classList.remove("pos-hold-pick-ping"); }, 420);
            } catch (eP) {}
            toastSay("Added " + qty + " × " + (name || "item") + " to " + formatHoldOrderNumber(panel.__holdId));
            return true;
          } catch (eHandler) {
            return false;
          }
        };

        function done() {
          try { doneBtn.removeEventListener("click", done); } catch (eRm) {}
          exitHoldPickMode(panel);
        }
        if (doneBtn) {
          // Replace any previous listener by overwriting via a fresh closure each time we enter pick mode.
          doneBtn.onclick = done;
        }
      }

      function exitHoldPickMode(panel) {
        window.__posHoldPickHandler = null;
        setHoldPickBarHostVisible(false);
        document.body.classList.remove("pos-hold-pick-active");
        var target = panel || window.__POS_ACTIVE_HOLD_PANEL || null;
        // Restore the held-orders modal; session was kept while picking.
        var modal = document.getElementById("pos-hold-modal");
        if (modal) {
          modal.classList.remove("hidden");
          modal.removeAttribute("aria-hidden");
          try { applyHoldGateState(); } catch (eGate) {}
        }
        if (target) {
          try { rerenderEditLines(target); } catch (eR) {}
          if (target.__pickDirty) {
            target.__pickDirty = false;
            if (HOLD_SESSION && HOLD_SESSION.employee_code && typeof target.__doSave === "function") {
              try { target.__doSave({ auto: true }); } catch (eSave) {}
            } else {
              try {
                toastSay("Items added — sign in with your 6-digit code, then tap Save.");
              } catch (eT) {}
            }
          }
        }
        window.__POS_ACTIVE_HOLD_PANEL = null;
      }

      function renderHoldList(rows) {
        var list = document.getElementById("pos-hold-list");
        var status = document.getElementById("pos-hold-list-status");
        var empty = document.getElementById("pos-hold-list-empty");
        if (!list) return;
        list.innerHTML = "";
        if (!rows || !rows.length) {
          if (status) status.classList.add("hidden");
          if (empty) empty.classList.remove("hidden");
          return;
        }
        if (status) status.classList.add("hidden");
        if (empty) empty.classList.add("hidden");
        rows.forEach(function (r) {
          var li = document.createElement("li");
          li.className = "pos-hold-row";
          li.setAttribute("data-open", "false");

          var amt = (r.total_amount != null ? Number(r.total_amount).toFixed(2) : "0.00");
          var items = (r.item_count != null ? Number(r.item_count) : 0);
          var tableLabel = normalizeHoldTableLabel(r.label || "");

          var summary = document.createElement("div");
          summary.className = "pos-hold-row-summary flex items-center gap-2 px-3 py-3 sm:py-3.5";

          var meta = document.createElement("div");
          meta.className = "min-w-0 flex-1 pl-1";
          var title = document.createElement("p");
          title.className = "truncate text-sm font-extrabold tracking-tight text-[rgb(var(--rc-page-fg))] sm:text-[15px]";
          title.textContent =
            formatHoldOrderNumber(r.id) +
            (tableLabel ? " · Table " + tableLabel : "") +
            (r.customer_name ? " · " + r.customer_name : "");
          var sub = document.createElement("p");
          sub.className = "mt-0.5 truncate text-[11px] text-[rgb(var(--rc-muted))]";
          sub.textContent = String(items) + (items === 1 ? " item" : " items");
          var amtBadge = document.createElement("span");
          amtBadge.className = "pos-hold-amount-badge";
          amtBadge.textContent = amt;
          meta.appendChild(title);
          meta.appendChild(sub);
          meta.appendChild(amtBadge);

          var actions = document.createElement("div");
          actions.className = "flex shrink-0 items-center gap-1";

          var editBtn = document.createElement("button");
          editBtn.type = "button";
          editBtn.className = "pos-hold-btn-ghost !px-2.5";
          editBtn.setAttribute("aria-expanded", "false");
          editBtn.setAttribute("aria-label", "Edit items");
          editBtn.innerHTML =
            '<svg class="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
            '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.25" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />' +
            '</svg>';

          var openBtn = document.createElement("button");
          openBtn.type = "button";
          openBtn.className = "pos-hold-btn-load";
          openBtn.setAttribute("aria-label", "Load to cart");
          openBtn.innerHTML =
            '<svg class="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">' +
            '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.25" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-1.7 6.7M17 13l4 6.7M9 21a1 1 0 100-2 1 1 0 000 2zm10 0a1 1 0 100-2 1 1 0 000 2z" />' +
            '</svg><span>Load</span>';
          openBtn.addEventListener("click", function () { reopenHold(r.id); });

          actions.appendChild(editBtn);
          actions.appendChild(openBtn);
          summary.appendChild(meta);
          summary.appendChild(actions);

          var panel = document.createElement("div");
          panel.className = "pos-hold-edit-panel hidden px-3 pb-3 pt-1 sm:px-3.5 sm:pb-3.5";
          editBtn.addEventListener("click", function () {
            var opening = panel.classList.contains("hidden");
            if (opening) {
              li.setAttribute("data-open", "true");
              editBtn.setAttribute("aria-expanded", "true");
              openHoldEditPanel(panel, r.id);
            } else {
              li.setAttribute("data-open", "false");
              editBtn.setAttribute("aria-expanded", "false");
              panel.classList.add("hidden");
              panel.innerHTML = "";
            }
          });
          li.appendChild(summary);
          li.appendChild(panel);
          list.appendChild(li);
        });
      }

      // Held-orders modal session state. Once the cashier types a valid 6-digit code in the
      // gate, ``HOLD_SESSION`` carries their employee identity for the rest of the modal's
      // lifetime — list filter, save-edits authorization, and badge fetches all reuse it
      // without forcing a per-action retype. ``setHoldModalOpen(false)`` clears it.
      var HOLD_SESSION = null;

      function setHoldSession(emp) {
        if (!emp || !emp.employee_code) {
          HOLD_SESSION = null;
          return;
        }
        HOLD_SESSION = {
          id: parseInt(emp.id || 0, 10) || 0,
          employee_code: String(emp.employee_code || "").trim(),
          full_name: String(emp.full_name || "").trim(),
        };
      }

      function holdListUrlWithSession() {
        if (HOLD_SESSION && HOLD_SESSION.employee_code) {
          var sep = HOLD_LIST_API.indexOf("?") >= 0 ? "&" : "?";
          return HOLD_LIST_API + sep + "employee_code=" + encodeURIComponent(HOLD_SESSION.employee_code);
        }
        return HOLD_LIST_API;
      }

      function applyHoldGateState() {
        var gate = document.getElementById("pos-hold-gate");
        var listWrap = document.getElementById("pos-hold-list-wrap");
        var bar = document.getElementById("pos-hold-session-bar");
        var nameEl = document.getElementById("pos-hold-session-name");
        var signedIn = !!(HOLD_SESSION && HOLD_SESSION.employee_code);
        if (gate) gate.classList.toggle("hidden", signedIn);
        if (gate) gate.classList.toggle("flex", !signedIn);
        if (listWrap) {
          listWrap.classList.toggle("hidden", !signedIn);
        }
        if (bar) {
          bar.classList.toggle("hidden", !signedIn);
          bar.classList.toggle("flex", signedIn);
        }
        if (nameEl) {
          nameEl.textContent = signedIn
            ? (HOLD_SESSION.full_name || ("Code " + HOLD_SESSION.employee_code))
            : "—";
        }
      }

      function fetchAndRenderList() {
        if (!(HOLD_SESSION && HOLD_SESSION.employee_code)) {
          applyHoldGateState();
          return Promise.resolve();
        }
        var status = document.getElementById("pos-hold-list-status");
        var list = document.getElementById("pos-hold-list");
        var empty = document.getElementById("pos-hold-list-empty");
        if (list) list.innerHTML = "";
        if (empty) empty.classList.add("hidden");
        if (status) {
          status.textContent = "Loading…";
          status.classList.remove("hidden");
        }
        return fetch(holdListUrlWithSession(), { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) {
              if (status) status.textContent = (j && j.error) || "Could not load held orders.";
              return;
            }
            renderHoldList(j.held_orders || []);
          })
          .catch(function () {
            if (status) status.textContent = "Could not reach server.";
          })
          .finally(function () {
            refreshHoldCountBadge();
          });
      }

      // Tries to sign the cashier in using the 6-digit code from the gate input.
      // Reuses /hold/list?employee_code=... — the server validates and returns the
      // employee + filtered list in one round-trip.
      function tryHoldGateLogin(code) {
        var gateStatus = document.getElementById("pos-hold-gate-status");
        var codeEl = document.getElementById("pos-hold-gate-code");
        function showErr(msg) {
          if (gateStatus) {
            gateStatus.textContent = msg;
            gateStatus.classList.remove("hidden");
          }
          if (codeEl) {
            codeEl.classList.add("ring-2", "ring-rose-500/55");
            setTimeout(function () { codeEl.classList.remove("ring-2", "ring-rose-500/55"); }, 600);
          }
        }
        if (!/^\d{6}$/.test(String(code || ""))) {
          showErr("Enter a 6-digit code.");
          return Promise.resolve(false);
        }
        var url = HOLD_LIST_API + (HOLD_LIST_API.indexOf("?") >= 0 ? "&" : "?") + "employee_code=" + encodeURIComponent(code);
        return fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok || !j.employee) {
              showErr((j && j.error) || "Invalid code.");
              return false;
            }
            if (gateStatus) gateStatus.classList.add("hidden");
            setHoldSession(j.employee);
            applyHoldGateState();
            renderHoldList(j.held_orders || []);
            if (codeEl) codeEl.value = "";
            return true;
          })
          .catch(function () {
            showErr("Could not reach server.");
            return false;
          });
      }

      function holdGateSignOut() {
        setHoldSession(null);
        applyHoldGateState();
        var codeEl = document.getElementById("pos-hold-gate-code");
        if (codeEl) {
          codeEl.value = "";
          setTimeout(function () { try { codeEl.focus(); } catch (e) {} }, 60);
        }
      }

      function setHoldModalOpen(open, opts) {
        opts = opts || {};
        var modal = document.getElementById("pos-hold-modal");
        if (!modal) return;
        if (open) {
          modal.classList.remove("hidden");
          modal.removeAttribute("aria-hidden");
          applyHoldGateState();
          if (HOLD_SESSION && HOLD_SESSION.employee_code) {
            fetchAndRenderList();
          } else {
            var codeEl = document.getElementById("pos-hold-gate-code");
            if (codeEl) setTimeout(function () { try { codeEl.focus(); } catch (e) {} }, 80);
          }
        } else {
          modal.classList.add("hidden");
          modal.setAttribute("aria-hidden", "true");
          // Drop the session so a different cashier cannot inherit it on next open.
          // Pick-from-catalog mode hides the modal but must keep the session for save.
          if (!opts.keepSession) {
            setHoldSession(null);
          }
          applyHoldGateState();
        }
      }

      function reopenHold(holdId) {
        fetch(HOLD_GET_BASE + String(holdId), { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok || !j.hold) {
              toastSay((j && j.error) || "Could not reopen held order.");
              return;
            }
            var hold = j.hold;
            writeHoldTableLabelToInput(hold.label || "");
            var lines = Array.isArray(hold.lines) ? hold.lines.map(function (l) {
              return {
                id: l && l.id,
                name: (l && l.name) || "Item",
                qty: parseFloat((l && l.qty) || 0) || 0,
                price: parseFloat((l && l.price) || 0) || 0,
                total: parseFloat((l && l.total) || 0) || 0,
                listPrice: parseFloat((l && l.price) || 0) || 0,
                originalSellingPrice: parseFloat((l && l.price) || 0) || 0,
              };
            }) : [];
            saveCheckoutPathPreference("direct");
            writeLocalCart(lines);
            saveHoldState({
              hold_id: Number(hold.id),
              saves_count: Number(hold.saves_count || 0),
              committed: hold.committed || {},
              table_label: normalizeHoldTableLabel(hold.label || ""),
            });
            try {
              var nEl = document.getElementById("pos-customer-name");
              var pEl = document.getElementById("pos-customer-phone");
              if (nEl && hold.customer_name) nEl.value = hold.customer_name;
              if (pEl && hold.customer_phone) pEl.value = hold.customer_phone;
            } catch (eFill) {}
            try { if (typeof window.render === "function") window.render(); } catch (eRender) {}
            updateBanner();
            setHoldModalOpen(false);
            try {
              var trigger = document.querySelector(".pos-cart-open-trigger");
              if (trigger) trigger.click();
            } catch (eOpenCart) {}
            try { populateCartHoldPicker(); } catch (ePickerSync) {}
            toastSay("Order " + formatHoldOrderNumber(hold.id) + " loaded into cart. Enter the 6-digit code to finalize.");
          })
          .catch(function () {
            toastSay("Could not reach server.");
          });
      }

      function openVoidDialog(holdId) {
        var modal = document.getElementById("pos-hold-void-modal");
        var idEl = document.getElementById("pos-hold-void-id");
        var codeEl = document.getElementById("pos-hold-void-code");
        var statusEl = document.getElementById("pos-hold-void-status");
        var confirmBtn = document.getElementById("pos-hold-void-confirm");
        if (!modal || !idEl || !codeEl || !confirmBtn) return;
        idEl.textContent = formatHoldOrderNumber(holdId);
        codeEl.value = "";
        if (statusEl) {
          statusEl.classList.add("hidden");
          statusEl.textContent = "";
        }
        modal.classList.remove("hidden");
        modal.removeAttribute("aria-hidden");
        confirmBtn.onclick = function () {
          var code = String(codeEl.value || "").trim();
          if (!/^\d{6}$/.test(code)) {
            if (statusEl) {
              statusEl.textContent = "Enter a valid 6-digit code.";
              statusEl.classList.remove("hidden");
            }
            return;
          }
          confirmBtn.disabled = true;
          fetch(HOLD_GET_BASE + String(holdId) + "/void", {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify({ employee: { employee_code: code } }),
          })
            .then(function (r) { return r.json().catch(function () { return {}; }); })
            .then(function (j) {
              if (!j || !j.ok) {
                if (statusEl) {
                  statusEl.textContent = (j && j.error) || "Could not void.";
                  statusEl.classList.remove("hidden");
                }
                return;
              }
              modal.classList.add("hidden");
              modal.setAttribute("aria-hidden", "true");
              toastSay("Order " + formatHoldOrderNumber(holdId) + " voided.");
              var cur = loadHoldState();
              if (cur && Number(cur.hold_id) === Number(holdId)) {
                saveHoldState(null);
                updateBanner();
              }
              fetchAndRenderList();
              try { populateCartHoldPicker(); } catch (eP) {}
            })
            .catch(function () {
              if (statusEl) {
                statusEl.textContent = "Could not reach server.";
                statusEl.classList.remove("hidden");
              }
            })
            .finally(function () {
              confirmBtn.disabled = false;
            });
        };
      }

      function closeVoidDialog() {
        var modal = document.getElementById("pos-hold-void-modal");
        if (!modal) return;
        modal.classList.add("hidden");
        modal.setAttribute("aria-hidden", "true");
      }

      /**
       * After a successful held-order save: same printer routing as checkout, **company slip only**
       * (no customer/cashier copies regardless of receipt_copies).
       */
      function printHoldCompanyCopy(cartLines, holdId) {
        if (!cartLines || !cartLines.length) return;
        try {
          var pseudoLines = cartLines.map(function (l) {
            var q = parseFloat((l && l.qty) || 0) || 0;
            var p = parseFloat((l && l.price) || 0) || 0;
            var t = parseFloat((l && l.total) || 0) || 0;
            if (!t && (q || p)) t = q * p;
            return {
              id: l && l.id,
              name: (l && l.name) || "Item",
              qty: q,
              price: p,
              total: t,
            };
          });
          var payload = makeReceiptPayload(pseudoLines, "sale", {
            persistedReceiptNo: "HOLD-" + String(holdId),
          });
          runConfiguredPrinterAction(payload, { receiptVariants: ["company"] }).catch(function () {});
        } catch (e) {}
      }

      function formatHoldOrderNumber(holdId) {
        var n = Math.max(0, parseInt(holdId, 10) || 0);
        return "ORD-" + String(n).padStart(4, "0");
      }
      window.__posFormatHoldOrderNumber = formatHoldOrderNumber;

      function showHoldSavedDialog(holdId, deltaLines) {
        var modal = document.getElementById("pos-hold-saved-modal");
        if (!modal) return;
        var idEl = document.getElementById("pos-hold-saved-id");
        var noteEl = document.getElementById("pos-hold-saved-note");
        if (idEl) idEl.textContent = formatHoldOrderNumber(holdId);
        if (noteEl) {
          var newCount = Array.isArray(deltaLines) ? deltaLines.length : 0;
          noteEl.textContent =
            newCount > 0
              ? newCount + " new line(s) committed to stock for this order."
              : "Cart saved. No new stock movement this save.";
        }
        modal.classList.remove("hidden");
        modal.removeAttribute("aria-hidden");
      }

      function hideHoldSavedDialog() {
        var modal = document.getElementById("pos-hold-saved-modal");
        if (!modal) return;
        modal.classList.add("hidden");
        modal.setAttribute("aria-hidden", "true");
      }

      function populateCartHoldPicker() {
        var sel = document.getElementById("pos-cart-hold-resume-select");
        if (!sel) return;
        fetch(HOLD_LIST_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            var rows = (j && j.ok && Array.isArray(j.held_orders)) ? j.held_orders : [];
            var st = loadHoldState();
            var currentId = st ? String(st.hold_id) : "";
            var prev = sel.value;
            var parts = ['<option value="">Resume a saved order…</option>'];
            rows.forEach(function (r) {
              var num = formatHoldOrderNumber(r.id);
              var label = num;
              var tbl = normalizeHoldTableLabel(r.label || "");
              if (tbl) label += " · Table " + tbl;
              if (r.customer_name) label += " · " + r.customer_name;
              var amt = r.total_amount != null ? Number(r.total_amount).toFixed(2) : "0.00";
              label += " · " + amt;
              parts.push('<option value="' + String(r.id) + '">' + label.replace(/</g, "&lt;") + '</option>');
            });
            sel.innerHTML = parts.join("");
            if (currentId && rows.some(function (r) { return String(r.id) === currentId; })) {
              sel.value = currentId;
            } else if (prev && rows.some(function (r) { return String(r.id) === String(prev); })) {
              sel.value = prev;
            } else {
              sel.value = "";
            }
            var emptyHint = document.getElementById("pos-cart-hold-resume-empty");
            if (emptyHint) emptyHint.classList.toggle("hidden", rows.length > 0);
          })
          .catch(function () {});
      }

      function saveCurrentCartToHold() {
        if (typeof window.posSalesAllowed === "function" && !window.posSalesAllowed()) {
          toastSay(typeof window.__posShopClosedMessage === "function" ? window.__posShopClosedMessage() : "Shop is closed.");
          return;
        }
        if (window.__POS_CHECKOUT_PATH !== "hold") {
          toastSay('Switch checkout mode to "Hold tab" to save a held order.');
          return;
        }
        var btn = document.getElementById("pos-cart-hold-save");
        var lines = readLocalCart();
        if (!lines.length) {
          toastSay("Cart is empty — add items first.");
          return;
        }
        var emp = readAuthorizedEmployee();
        if (!emp || !emp.employee_code) {
          toastSay("Enter a 6-digit employee code in the cart to save the held order.");
          var inp = document.getElementById("pos-auth-code");
          if (inp) { try { inp.focus(); } catch (e) {} }
          return;
        }
        var totals = totalsForLines(lines);
        var st = loadHoldState();
        var tableLabel = readHoldTableLabelFromInput();
        var body = {
          hold_id: st ? st.hold_id : null,
          lines: lines.map(function (l) {
            return {
              id: l && l.id,
              name: (l && l.name) || "Item",
              qty: parseFloat((l && l.qty) || 0) || 0,
              price: parseFloat((l && l.price) || 0) || 0,
              total: parseFloat((l && l.total) || 0) || 0,
            };
          }),
          total_amount: totals.grand,
          item_count: totals.item_count,
          customer_name: ((document.getElementById("pos-customer-name") || {}).value || "").trim(),
          customer_phone: ((document.getElementById("pos-customer-phone") || {}).value || "").trim(),
          label: tableLabel,
          employee: { employee_code: emp.employee_code },
        };
        var needAppr = st && st.committed && holdSaveNeedsReductionApproval(body.lines, st.committed);
        if (needAppr) {
          var apprInp = document.getElementById("pos-cart-hold-reduction-approver");
          var apprC = apprInp ? String(apprInp.value || "").trim() : "";
          if (!/^\d{6}$/.test(apprC)) {
            toastSay(
              "To reduce quantities on this held order, enter a manager, admin, company manager, or super admin 6-digit approval code."
            );
            var wrap = document.getElementById("pos-cart-hold-reduction-wrap");
            if (wrap) wrap.classList.remove("hidden");
            if (apprInp) {
              try {
                apprInp.focus();
              } catch (eF) {}
            }
            if (btn) btn.disabled = false;
            return;
          }
          body.reduction_approver_code = apprC;
        }
        if (btn) btn.disabled = true;
        fetch(HOLD_SAVE_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(body),
        })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) {
              toastSay((j && j.error) || "Could not save the held order.");
              return;
            }
            var holdId = Number(j.hold_id || 0);
            saveHoldState({
              hold_id: holdId,
              saves_count: ((st && st.saves_count) || 0) + 1,
              committed: j.committed || {},
              table_label: tableLabel,
            });
            if (j && j.ok && Array.isArray(j.delta_lines) && j.delta_lines.length) {
              printHoldCompanyCopy(j.delta_lines, holdId);
            }
            writeLocalCart([]);
            try { if (typeof window.render === "function") window.render(); } catch (e) {}
            try { if (typeof window.refreshPosCatalogStock === "function") window.refreshPosCatalogStock(); } catch (e) {}
            saveHoldState(null);
            updateBanner();
            // Mirror the same reset the main "Clear cart" / finalize-sale path uses so
            // the next sale starts from a clean slate: drop payment selection, employee
            // authorization, known customer, quote flag, credit due date and the
            // customer detail inputs. Without this, lingering auth/payment state from
            // the just-saved hold leaks into the next transaction.
            try { if (typeof setPaymentMethod === "function") setPaymentMethod(""); } catch (eP) {}
            try { if (typeof clearAuthorization === "function") clearAuthorization(); } catch (eA) {}
            try { if (typeof clearKnownCustomer === "function") clearKnownCustomer(); } catch (eK) {}
            try {
              if (typeof setCustomerLookupStatus === "function") {
                setCustomerLookupStatus("", "muted");
              }
            } catch (eS) {}
            try {
              var qOnlyEl = document.getElementById("pos-quote-only");
              if (qOnlyEl) qOnlyEl.checked = false;
            } catch (eQ) {}
            try {
              var nEl = document.getElementById("pos-customer-name");
              var pEl = document.getElementById("pos-customer-phone");
              if (nEl) nEl.value = "";
              if (pEl) pEl.value = "";
              var dueEl = document.getElementById("pos-credit-due-date");
              if (dueEl) dueEl.value = "";
              var authEl = document.getElementById("pos-auth-code");
              if (authEl) authEl.value = "";
            } catch (eClear) {}
            try {
              if (typeof window.applyPosCartUiSettings === "function") window.applyPosCartUiSettings();
            } catch (eU) {}
            try { applyCheckoutToolbar(); } catch (eT) {}
            // Close the cart drawer reliably. Prefer the exposed setter
            // (window.__posSetCartOpen) so we don't rely on the close button's
            // click listener firing inside an async success callback. Fall back
            // to a synthetic click on the close button, then to clearing the
            // drawer's `is-open` class as a last resort.
            try {
              if (typeof window.__posSetCartOpen === "function") {
                window.__posSetCartOpen(false);
              } else {
                var closeBtn = document.getElementById("pos-cart-close");
                if (closeBtn) {
                  closeBtn.click();
                } else {
                  var drawerEl = document.getElementById("pos-cart-drawer");
                  if (drawerEl) {
                    drawerEl.classList.remove("is-open");
                    setTimeout(function () { drawerEl.hidden = true; }, 360);
                  }
                  var bd = document.getElementById("pos-cart-backdrop");
                  if (bd) bd.classList.remove("is-visible");
                  document.body.style.overflow = "";
                }
              }
            } catch (eCart) {}
            // Optimistic FAB icon update so the cashier sees the held-orders
            // badge tick up *immediately* after the save (the authoritative
            // count from the server follows via refreshHoldCountBadge below).
            // Only bump on a brand-new hold; updates to an existing hold don't
            // change the open-count.
            try {
              var wasNewHold = !st;
              if (wasNewHold) {
                bumpHoldCountBadgeOptimistically(1);
              }
              flashHoldFab();
            } catch (eOpt) {}
            refreshHoldCountBadge();
            try { showHoldSavedDialog(holdId, j.delta_lines || []); } catch (eDlg) {}
            toastSay("Order #" + formatHoldOrderNumber(holdId) + " saved" + (j.delta_lines && j.delta_lines.length ? " · " + j.delta_lines.length + " new line(s) deducted from stock" : "") + ".");
          })
          .catch(function () {
            toastSay("Could not reach server.");
          })
          .finally(function () {
            if (btn) btn.disabled = false;
          });
      }

      // Sale-finalize hook (called by proceedSale on success). Clears any active held order.
      var prevFinalizeHook = window.__posOnSaleFinalized;
      window.__posOnSaleFinalized = function (result, ctx) {
        try { if (typeof prevFinalizeHook === "function") prevFinalizeHook(result, ctx); } catch (e) {}
        if (result && result.held_order_finalized_id) {
          saveCheckoutPathPreference("direct");
        }
        var st = loadHoldState();
        var serverFinalizedId = result && result.held_order_finalized_id ? Number(result.held_order_finalized_id) : null;
        if (st || serverFinalizedId) {
          saveHoldState(null);
          updateBanner();
          refreshHoldCountBadge();
          populateCartHoldPicker();
        }
      };

      // Public bridge so the main cart's proceedSale() can divert to a hold-save when
      // the user authorizes an unsaved cart while in "Hold tab" mode.
      window.__posSaveCurrentCartToHold = function () {
        try { saveCurrentCartToHold(); } catch (e) {}
      };

      function bind() {
        var openBtn = document.getElementById("pos-hold-open");
        if (openBtn) openBtn.addEventListener("click", function () { setHoldModalOpen(true); });
        var fabBtn = document.getElementById("pos-hold-toggle-fab");
        if (fabBtn) fabBtn.addEventListener("click", function () { setHoldModalOpen(true); });
        var closeBtn = document.getElementById("pos-hold-close");
        if (closeBtn) closeBtn.addEventListener("click", function () { setHoldModalOpen(false); });
        var backdrop = document.getElementById("pos-hold-backdrop");
        if (backdrop) backdrop.addEventListener("click", function () { setHoldModalOpen(false); });
        var refresh = document.getElementById("pos-hold-refresh");
        if (refresh) {
          refresh.addEventListener("click", function () {
            if (HOLD_SESSION && HOLD_SESSION.employee_code) {
              fetchAndRenderList();
            } else {
              applyHoldGateState();
            }
          });
        }
        var gateInput = document.getElementById("pos-hold-gate-code");
        if (gateInput) {
          var commitTimer = null;
          gateInput.addEventListener("input", function () {
            var clean = String(gateInput.value || "").replace(/\D/g, "").slice(0, 6);
            if (gateInput.value !== clean) gateInput.value = clean;
            var statusEl = document.getElementById("pos-hold-gate-status");
            if (statusEl) statusEl.classList.add("hidden");
            if (commitTimer) clearTimeout(commitTimer);
            if (clean.length === 6) {
              commitTimer = setTimeout(function () {
                tryHoldGateLogin(clean);
              }, 60);
            }
          });
          gateInput.addEventListener("keydown", function (e) {
            if (e.key === "Enter") {
              e.preventDefault();
              var clean = String(gateInput.value || "").replace(/\D/g, "").slice(0, 6);
              tryHoldGateLogin(clean);
            }
          });
        }
        var switchBtn = document.getElementById("pos-hold-session-switch");
        if (switchBtn) switchBtn.addEventListener("click", function () { holdGateSignOut(); });
        var saveBtn = document.getElementById("pos-cart-hold-save");
        if (saveBtn) saveBtn.addEventListener("click", saveCurrentCartToHold);
        var unlinkBtn = document.getElementById("pos-cart-hold-banner-clear");
        if (unlinkBtn) {
          unlinkBtn.addEventListener("click", function () {
            saveHoldState(null);
            updateBanner();
            toastSay("Unlinked. The cart is no longer tied to a held order.");
          });
        }
        var pathDirect = document.getElementById("pos-cart-path-direct");
        var pathHold = document.getElementById("pos-cart-path-hold");
        if (pathDirect) {
          pathDirect.addEventListener("click", function () {
            saveCheckoutPathPreference("direct");
            applyCheckoutToolbar();
          });
        }
        if (pathHold) {
          pathHold.addEventListener("click", function () {
            saveCheckoutPathPreference("hold");
            applyCheckoutToolbar();
          });
        }
        document.querySelectorAll(".pos-cart-open-trigger").forEach(function (el) {
          el.addEventListener("click", function () {
            setTimeout(applyCheckoutToolbar, 80);
            setTimeout(populateCartHoldPicker, 80);
          });
        });
        var resumeSel = document.getElementById("pos-cart-hold-resume-select");
        if (resumeSel) {
          resumeSel.addEventListener("change", function () {
            var v = String(resumeSel.value || "").trim();
            if (!v) return;
            var idn = parseInt(v, 10);
            if (!idn) return;
            try { reopenHold(idn); } catch (e) {}
            try { setHoldModalOpen(false); } catch (e) {}
            setTimeout(populateCartHoldPicker, 250);
          });
        }
        var resumeRefresh = document.getElementById("pos-cart-hold-resume-refresh");
        if (resumeRefresh) {
          resumeRefresh.addEventListener("click", function () {
            populateCartHoldPicker();
            toastSay("Refreshed held-order list.");
          });
        }
        var savedClose = document.getElementById("pos-hold-saved-close");
        if (savedClose) savedClose.addEventListener("click", hideHoldSavedDialog);
        var savedBackdrop = document.getElementById("pos-hold-saved-backdrop");
        if (savedBackdrop) savedBackdrop.addEventListener("click", hideHoldSavedDialog);
        var savedNew = document.getElementById("pos-hold-saved-new");
        if (savedNew) {
          savedNew.addEventListener("click", function () {
            hideHoldSavedDialog();
            try {
              var closeBtn = document.getElementById("pos-cart-close");
              if (closeBtn) closeBtn.click();
            } catch (e) {}
          });
        }
        var voidCancel = document.getElementById("pos-hold-void-cancel");
        if (voidCancel) voidCancel.addEventListener("click", closeVoidDialog);
        var voidBackdrop = document.getElementById("pos-hold-void-backdrop");
        if (voidBackdrop) voidBackdrop.addEventListener("click", closeVoidDialog);
        document.addEventListener("keydown", function (e) {
          if (e.key !== "Escape") return;
          var pickBar = document.getElementById("pos-hold-pick-bar");
          if (pickBar && !pickBar.classList.contains("hidden")) {
            exitHoldPickMode(null);
            e.preventDefault();
            return;
          }
          var savedModal = document.getElementById("pos-hold-saved-modal");
          if (savedModal && !savedModal.classList.contains("hidden")) {
            hideHoldSavedDialog();
            e.preventDefault();
            return;
          }
          var modal = document.getElementById("pos-hold-modal");
          if (modal && !modal.classList.contains("hidden")) {
            setHoldModalOpen(false);
            e.preventDefault();
            return;
          }
          var voidM = document.getElementById("pos-hold-void-modal");
          if (voidM && !voidM.classList.contains("hidden")) {
            closeVoidDialog();
            e.preventDefault();
          }
        });
        updateBanner();
        refreshHoldCountBadge();
        populateCartHoldPicker();
        setInterval(refreshHoldCountBadge, 45000);
        setInterval(populateCartHoldPicker, 45000);
      }

      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bind);
      } else {
        bind();
      }
    })();