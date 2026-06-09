(function () {
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var KEY = "richcom-shop-pos-cart-" + SHOP_ID;
  if (typeof window.__POS_CHECKOUT_PATH === "undefined") {
    window.__POS_CHECKOUT_PATH = "direct";
  }
  var OFFLINE_SALES_KEY = "richcom-shop-pos-offline-sales-" + SHOP_ID;
  var OFFLINE_CATALOG_KEY = "richcom-shop-pos-catalog-snapshot-" + SHOP_ID;
  var OFFLINE_STOCKIN_KEY = "richcom-shop-pos-offline-stockins-" + SHOP_ID;
  var OFFLINE_REFILL_KEY = "richcom-shop-pos-offline-refills-" + SHOP_ID;
  var OFFLINE_DB_NAME = "richcom-pos-offline-db-v2";
  var OFFLINE_DB_VERSION = 4;
  var OFFLINE_DB_STORE = "offline_sales";
  var OFFLINE_DB_CATALOG_STORE = "catalog_snapshot";
  var OFFLINE_DB_STOCKIN_STORE = "offline_stockins";
  var OFFLINE_DB_REFILL_STORE = "offline_portion_refills";
  var OFFLINE_CATALOG_DOC_ID = "shop-" + SHOP_ID;
  var OFFLINE_STOCK_IN_API = BOOT.apis.stockIn;
  var OFFLINE_REFILL_PORTIONS_API = BOOT.apis.refillPortions || "";
  var OFFLINE_CATALOG_STALE_MS = 6 * 60 * 60 * 1000;
  var AUTH_API = BOOT.apis.authorizeEmployee;
  var EMPLOYEE_CACHE_API = BOOT.apis.employeeAuthCache;
  var EMPLOYEE_AUTH_CACHE_KEY = "richcom-shop-pos-employee-auth-cache-" + SHOP_ID;
  var CUSTOMER_LOOKUP_API = BOOT.apis.customerLookup;
  var CUSTOMER_UPSERT_API = BOOT.apis.customerUpsert;
  var RECORD_SALE_API = BOOT.apis.recordSale;
  var RECORD_QUOTE_API = BOOT.apis.recordQuote;
  var SELLER_LOOKUP_API = BOOT.apis.sellerLookup;
  var CATALOG_STOCK_API = BOOT.apis.catalogStock;
  var RECEIPTS_LIST_API = BOOT.apis.receiptsList;
  var RECEIPTS_MARK_API = BOOT.apis.receiptsMark;
  var RECEIPTS_DETAIL_API = BOOT.apis.receiptsDetail;
  var RECEIPTS_RETURN_LINES_API = BOOT.apis.receiptsReturnLines;
  var POS_INVENTORY_MODE = BOOT.inventoryMode;
  window.POS_INVENTORY_MODE = POS_INVENTORY_MODE;
  var PRINTER_API = BOOT.apis.printer;
  var QR_ESC_POS_API = BOOT.apis.printerQrEscpos;
  var NETWORK_ESC_POS_API = BOOT.apis.printerSendEscpos;
  var PRINTER_LS_KEY = "richcom-shop-pos-printer-local-" + SHOP_ID;
  window.POS_PRINTING = BOOT.printing;
  (function () {
    var p = window.POS_PRINTING;
    if (!p || typeof p !== "object") return;
    function triPrinterAllow(v) {
      if (v === true || v === 1 || v === "1") return true;
      if (typeof v === "string") {
        var s = v.trim().toLowerCase();
        if (s === "true" || s === "1" || s === "yes" || s === "on") return true;
      }
      return false;
    }
    p.printer_allow_bluetooth = triPrinterAllow(p.printer_allow_bluetooth);
    p.printer_allow_network = triPrinterAllow(p.printer_allow_network);
    p.printer_allow_usb = triPrinterAllow(p.printer_allow_usb);
    window.posPrinterTypeAllowed = function (pp, type) {
      pp = pp || window.POS_PRINTING || {};
      type = String(type || "").toLowerCase();
      if (type === "bluetooth") return pp.printer_allow_bluetooth === true;
      if (type === "network") return pp.printer_allow_network === true;
      if (type === "usb") return pp.printer_allow_usb === true;
      return false;
    };
    function triBoolSetting(v) {
      if (v === true || v === 1 || v === "1") return true;
      if (typeof v === "string") {
        var s = v.trim().toLowerCase();
        if (s === "true" || s === "1" || s === "yes" || s === "on") return true;
      }
      return false;
    }
    p.print_compulsory_sale = triBoolSetting(p.print_compulsory_sale);
  })();
  window.POS_RECEIPT_SETTINGS = BOOT.receiptSettings;
  window.POS_SITE = BOOT.site;
var saleType = "sale";
      var salePaymentMethod = "";
      var authorizedEmployee = null;
      var authVerifyInFlight = false;
      var lastVerifiedAuthCode = "";
      var knownCustomer = null;
      var customerLookupTimer = null;
      var customerLookupInFlight = false;
      var customerRegisterInFlight = false;
      var lastSplitEdited = "cash";
      var stockInModal = document.getElementById("pos-stockin-modal");
      var openReceiptsBtn = document.getElementById("pos-receipts-open");
      var receiptsModal = document.getElementById("pos-receipts-modal");
      var receiptsBackdrop = document.getElementById("pos-receipts-backdrop");
      var receiptsCloseBtn = document.getElementById("pos-receipts-close");
      var receiptsSubtitle = document.getElementById("pos-receipts-subtitle");
      var receiptsListBody = document.getElementById("pos-receipts-list-body");
      var receiptsMsg = document.getElementById("pos-receipts-msg");
      var receiptsReprintBtn = document.getElementById("pos-receipts-reprint");
      var receiptsCancelBtn = document.getElementById("pos-receipts-cancel");
      var receiptsReturnBtn = document.getElementById("pos-receipts-return");
      var selectedReceiptId = 0;
      var receiptsRowsCache = [];
      var networkStateIndicatorEl = document.getElementById("pos-network-state-indicator");
      var offlineSyncBadgeEl = document.getElementById("pos-offline-sync-badge");
      var offlineSyncNowBtn = document.getElementById("pos-offline-sync-now");
      var offlineSyncDiagBtn = document.getElementById("pos-offline-sync-diagnostics");
      var catalogStaleBannerEl = document.getElementById("pos-catalog-stale-banner");
      var offlineSyncBusy = false;
      var offlineSyncTimer = null;
      var offlineSyncRecentLog = [];
      var offlineSyncToastHideTimer = null;
      var OFFLINE_SYNC_LOG_CAP = 30;
      var OFFLINE_SYNC_LOG_LS_KEY = "richcom-shop-pos-offline-sync-log-day-" + SHOP_ID;

      function pad2(n) {
        return (n < 10 ? "0" : "") + n;
      }

      function localDayKeyFromMs(ms) {
        var d = new Date(ms);
        return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
      }

      function isIsoLocalToday(iso) {
        var t = Date.parse(String(iso || ""));
        if (!isFinite(t)) return false;
        return localDayKeyFromMs(t) === localDayKeyFromMs(Date.now());
      }

      function hydrateOfflineSyncLogFromStorage() {
        try {
          var raw = localStorage.getItem(OFFLINE_SYNC_LOG_LS_KEY);
          if (!raw) return;
          var doc = JSON.parse(raw);
          var today = localDayKeyFromMs(Date.now());
          if (!doc || typeof doc !== "object") return;
          if (doc.day !== today) {
            localStorage.removeItem(OFFLINE_SYNC_LOG_LS_KEY);
            return;
          }
          if (!Array.isArray(doc.entries)) return;
          offlineSyncRecentLog = doc.entries.slice(0, OFFLINE_SYNC_LOG_CAP);
        } catch (e) {
          try {
            localStorage.removeItem(OFFLINE_SYNC_LOG_LS_KEY);
          } catch (e2) {}
        }
      }

      function persistOfflineSyncLogDay() {
        try {
          var today = localDayKeyFromMs(Date.now());
          localStorage.setItem(
            OFFLINE_SYNC_LOG_LS_KEY,
            JSON.stringify({ day: today, entries: offlineSyncRecentLog.slice(0, 50) })
          );
        } catch (e) {}
      }

      hydrateOfflineSyncLogFromStorage();

      if ("serviceWorker" in navigator) {
        window.addEventListener("load", function () {
          navigator.serviceWorker.register("/pos-sw.js").catch(function () {});
        });
      }
      function todayIso() {
        var d = new Date();
        var y = d.getFullYear();
        var m = String(d.getMonth() + 1).padStart(2, "0");
        var day = String(d.getDate()).padStart(2, "0");
        return y + "-" + m + "-" + day;
      }
      function setReceiptsMsg(text, tone) {
        if (!receiptsMsg) return;
        receiptsMsg.textContent = text || "";
        receiptsMsg.className = "text-xs " + (tone === "error"
          ? "text-rose-500"
          : tone === "ok"
            ? "text-emerald-600 dark:text-emerald-300"
            : "text-[rgb(var(--rc-muted))]");
      }
      function receiptStatusChip(status) {
        var s = String(status || "pending").toLowerCase();
        if (s === "confirmed") return '<span class="inline-flex rounded-md bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700 dark:text-emerald-300">confirmed</span>';
        if (s === "cancelled") return '<span class="inline-flex rounded-md bg-rose-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-rose-700 dark:text-rose-300">cancelled</span>';
        if (s === "partial_return") return '<span class="inline-flex rounded-md bg-orange-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-orange-700 dark:text-orange-300">partial return</span>';
        if (s === "returned") return '<span class="inline-flex rounded-md bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-amber-700 dark:text-amber-300">returned</span>';
        return '<span class="inline-flex rounded-md bg-slate-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-slate-700 dark:text-slate-300">pending</span>';
      }
      function selectedReceiptRow() {
        var sid = selectedReceiptId;
        if (!sid) return null;
        for (var i = 0; i < receiptsRowsCache.length; i++) {
          if (parseInt(receiptsRowsCache[i].sale_id || 0, 10) === sid) return receiptsRowsCache[i];
        }
        return null;
      }
      function renderReceiptsRows(rows) {
        receiptsRowsCache = Array.isArray(rows) ? rows : [];
        if (!receiptsListBody) return;
        if (!receiptsRowsCache.length) {
          receiptsListBody.innerHTML =
            "<tr><td colspan='5' class='px-4 py-10 text-center text-sm text-[rgb(var(--rc-muted))]'>No receipts found for today.</td></tr>";
          selectedReceiptId = 0;
          return;
        }
        receiptsListBody.innerHTML = receiptsRowsCache
          .map(function (row) {
            var sid = parseInt(row.sale_id || 0, 10) || 0;
            var rec = row.receipt_number || ("#" + String(sid));
            var when = row.created_at || row.created_at_iso || "-";
            var amount = Number(row.total_amount || 0);
            var emp = row.employee_name || "-";
            var active = sid === selectedReceiptId;
            return (
              "<tr class='cursor-pointer " + (active ? "bg-[rgb(var(--rc-primary))]/10" : "hover:bg-[rgb(var(--rc-surface-2))]/40") + "' data-pos-receipt-row data-sale-id='" + String(sid) + "'>" +
              "<td class='px-3 py-2.5 font-mono font-bold text-[rgb(var(--rc-page-fg))]'>" + receiptEsc(rec) + "</td>" +
              "<td class='px-3 py-2.5 text-[rgb(var(--rc-muted))]'>" + receiptEsc(when) + "</td>" +
              "<td class='px-3 py-2.5 text-right tabular-nums font-semibold text-[rgb(var(--rc-page-fg))]'>" + receiptEsc(amount.toFixed(2)) + "</td>" +
              "<td class='px-3 py-2.5 text-[rgb(var(--rc-muted))]'>" + receiptEsc(emp) + "</td>" +
              "<td class='px-3 py-2.5'>" + receiptStatusChip(row.receipt_mark_status) + "</td>" +
              "</tr>"
            );
          })
          .join("");
        if (!selectedReceiptId && receiptsRowsCache.length) {
          selectedReceiptId = parseInt(receiptsRowsCache[0].sale_id || 0, 10) || 0;
          renderReceiptsRows(receiptsRowsCache);
        }
      }
      function loadPosReceiptsList() {
        var q = "?mode=single_day&single_day=" + encodeURIComponent(todayIso());
        if (receiptsSubtitle) receiptsSubtitle.textContent = "Loading today's receipts...";
        setReceiptsMsg("", "muted");
        return fetch(RECEIPTS_LIST_API + q, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Could not load receipts.");
            renderReceiptsRows(j.rows || []);
            if (receiptsSubtitle) {
              receiptsSubtitle.textContent = (j.range_label ? String(j.range_label) + " · " : "") + String((j.rows || []).length) + " receipt(s).";
            }
          })
          .catch(function (err) {
            renderReceiptsRows([]);
            setReceiptsMsg((err && err.message) ? err.message : "Could not load receipts.", "error");
          });
      }
      function showReceiptsModal() {
        if (!receiptsModal) return;
        receiptsModal.classList.remove("hidden");
        receiptsModal.setAttribute("aria-hidden", "false");
        loadPosReceiptsList();
      }
      function hideReceiptsModal() {
        if (!receiptsModal) return;
        receiptsModal.classList.add("hidden");
        receiptsModal.setAttribute("aria-hidden", "true");
      }
      function withSelectedReceiptDetail(fn) {
        if (!selectedReceiptId) {
          setReceiptsMsg("Select a receipt first.", "error");
          return Promise.resolve();
        }
        return fetch(RECEIPTS_DETAIL_API + "?sale_id=" + encodeURIComponent(selectedReceiptId), {
          credentials: "same-origin",
          headers: { Accept: "application/json" },
        })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Could not load receipt detail.");
            return fn(j.sale || {}, j.items || []);
          })
          .catch(function (err) {
            setReceiptsMsg((err && err.message) ? err.message : "Could not load receipt detail.", "error");
          });
      }
      function buildPersistedReceiptPayload(sale, items) {
        var saleObj = sale || {};
        var rows = (items || []).map(function (it) {
          var qty = parseFloat(it.qty || 0);
          if (isNaN(qty) || qty < 0) qty = 0;
          var total = Number(it.line_total || 0);
          var unit = Number(it.unit_price || 0);
          return {
            id: it.item_id,
            name: it.item_name || "Item",
            qty: qty,
            price: unit,
            listPrice: unit,
            total: total,
            listTotal: total,
            lineDiscount: 0,
            discounted: false,
          };
        });
        var subtotal = rows.reduce(function (sum, it) { return sum + Number(it.total || 0); }, 0);
        var tx = computePosTax(subtotal);
        var pm = String(saleObj.payment_method || "").toLowerCase();
        var paymentTypeLabel = "Payment";
        var paymentDetailText = "";
        if (pm === "cash") {
          paymentTypeLabel = "Payment: Cash";
          paymentDetailText = "Cash: " + fmt(saleObj.cash_amount || saleObj.total_amount || 0);
        } else if (pm === "mpesa") {
          paymentTypeLabel = "Payment: M-Pesa";
          paymentDetailText = "M-Pesa: " + fmt(saleObj.mpesa_amount || saleObj.total_amount || 0);
        } else if (pm === "both") {
          paymentTypeLabel = "Payment: Cash + M-Pesa";
          paymentDetailText = "Cash: " + fmt(saleObj.cash_amount || 0) + " | M-Pesa: " + fmt(saleObj.mpesa_amount || 0);
        } else if (String(saleObj.sale_type || "") === "credit") {
          paymentTypeLabel = "Payment: Credit";
          paymentDetailText = "Pending payment";
        }
        var createdAt = saleObj.created_at ? String(saleObj.created_at) : new Date().toLocaleString();
        var site = window.POS_SITE || {};
        var rs = receiptSettings();
        return {
          receiptNo: String(saleObj.receipt_number || ("#" + String(saleObj.id || ""))),
          printedAt: createdAt,
          shopName: String(saleObj.shop_name || site.shop_name || "Point of Sale"),
          companyName: String(site.company_name || ""),
          shopCode: String(saleObj.shop_code || site.shop_code || ""),
          shopLocation: String(site.shop_location || ""),
          receiptLogoUrl: String(site.receipt_logo_url || site.app_icon_url || ""),
          mode: String(saleObj.sale_type || "sale") === "credit" ? "Credit" : "Sale",
          isQuotation: false,
          isReprint: true,
          customerName: String(saleObj.customer_name || "Walk-in customer"),
          customerPhone: String(saleObj.customer_phone || "-"),
          employeeName: String(saleObj.employee_name || "Unknown"),
          employeeCode: String(saleObj.employee_code || "-"),
          lines: rows,
          subtotal: subtotal,
          discountTotal: 0,
          hasDiscount: false,
          taxAmount: tx.taxAmt,
          taxPercent: tx.taxPct,
          grandTotal: Number(saleObj.total_amount || tx.grand || subtotal),
          includeTax: tx.includeTax,
          paymentTypeLabel: paymentTypeLabel,
          paymentDetailText: paymentDetailText,
          receiptHeader: (rs.receipt_header || "").trim(),
          receiptFooter: (rs.receipt_footer || "").trim(),
          creditDueDate: "",
        };
      }
      function escHtml(s) {
        return String(s == null ? "" : s)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      }
      function employeeAuthCacheLoad() {
        try {
          var raw = localStorage.getItem(EMPLOYEE_AUTH_CACHE_KEY);
          if (!raw) return [];
          var j = JSON.parse(raw) || {};
          var rows = Array.isArray(j.employees) ? j.employees : [];
          return rows.filter(function (r) {
            return r && /^\d{6}$/.test(String(r.employee_code || ""));
          });
        } catch (e) {
          return [];
        }
      }
      function employeeAuthCacheSave(rows) {
        try {
          var clean = (Array.isArray(rows) ? rows : []).filter(function (r) {
            return r && /^\d{6}$/.test(String(r.employee_code || ""));
          });
          localStorage.setItem(
            EMPLOYEE_AUTH_CACHE_KEY,
            JSON.stringify({ updated_at: Date.now(), employees: clean })
          );
        } catch (e) {}
      }
      function employeeAuthCacheUpsert(employee) {
        if (!employee || !/^\d{6}$/.test(String(employee.employee_code || ""))) return;
        var rows = employeeAuthCacheLoad();
        var code = String(employee.employee_code);
        var next = rows.filter(function (r) {
          return String((r && r.employee_code) || "") !== code;
        });
        next.unshift({
          id: employee.id,
          full_name: employee.full_name || "Employee",
          employee_code: code,
          role: employee.role || "employee",
          status: employee.status || "active",
        });
        if (next.length > 400) next = next.slice(0, 400);
        employeeAuthCacheSave(next);
      }
      function employeeAuthCacheFindByCode(code) {
        var needle = String(code || "").replace(/\D/g, "").slice(0, 6);
        if (!/^\d{6}$/.test(needle)) return null;
        var rows = employeeAuthCacheLoad();
        for (var i = 0; i < rows.length; i++) {
          if (String(rows[i].employee_code || "") === needle) return rows[i];
        }
        return null;
      }
      function refreshEmployeeAuthCacheFromServer() {
        if (!navigator.onLine) return Promise.resolve();
        return fetch(EMPLOYEE_CACHE_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) {
            return r.json().catch(function () {
              return {};
            });
          })
          .then(function (j) {
            if (!j || !j.ok) return;
            employeeAuthCacheSave(j.employees || []);
          })
          .catch(function () {});
      }
      /** Run non-critical POS startup work after first paint / when the browser is idle. */
      function schedulePosIdleWork(fn, delayMs) {
        if (typeof fn !== "function") return;
        var wait = typeof delayMs === "number" && delayMs > 0 ? delayMs : 0;
        var run = function () {
          try {
            fn();
          } catch (e) {}
        };
        if (wait > 0) {
          setTimeout(run, wait);
          return;
        }
        if (typeof requestIdleCallback === "function") {
          requestIdleCallback(run, { timeout: 3000 });
        } else {
          setTimeout(run, 1);
        }
      }
      function looksLikeOfflineNetworkError(err) {
        if (!err) return false;
        if (!navigator.onLine) return true;
        var t = String((err && err.message) || err).toLowerCase();
        return (
          t.indexOf("failed to fetch") !== -1 ||
          t.indexOf("networkerror") !== -1 ||
          t.indexOf("network error") !== -1 ||
          t.indexOf("load failed") !== -1
        );
      }
      function verifyReceiptActionCode(rawCode) {
        var code = String(rawCode || "").replace(/\D/g, "").slice(0, 6);
        if (!/^\d{6}$/.test(code)) {
          return Promise.reject(new Error("Enter a valid 6-digit code."));
        }
        return fetch(AUTH_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ employee_code: code }),
        })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Authorization failed.");
            employeeAuthCacheUpsert(j.employee || null);
            var role = String((j.employee && j.employee.role) || "").trim().toLowerCase();
            if (role !== "manager" && role !== "admin" && role !== "super_admin") {
              throw new Error("Only manager, admin, or super admin can do this action.");
            }
            return j.employee || {};
          })
          .catch(function (err) {
            if (!looksLikeOfflineNetworkError(err)) throw err;
            var cached = employeeAuthCacheFindByCode(code);
            if (!cached) throw new Error("Offline: employee code not in local cache. Connect once to sync employees.");
            var role = String(cached.role || "").trim().toLowerCase();
            if (role !== "manager" && role !== "admin" && role !== "super_admin") {
              throw new Error("Only manager, admin, or super admin can do this action.");
            }
            return cached;
          });
      }
      function ensureReceiptActionModal() {
        var existing = document.getElementById("pos-receipt-action-modal");
        if (existing) return existing;
        var wrap = document.createElement("div");
        wrap.id = "pos-receipt-action-modal";
        wrap.className = "fixed inset-0 z-[130] hidden items-end justify-center bg-black/55 p-0 sm:items-center sm:p-4";
        wrap.innerHTML =
          '<div class="w-full max-w-2xl overflow-hidden rounded-t-2xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface))] shadow-2xl sm:rounded-2xl">' +
          '<div class="flex items-start justify-between gap-3 border-b border-[rgb(var(--rc-border))]/70 bg-[rgb(var(--rc-surface))]/95 px-4 py-3">' +
          '<div class="min-w-0">' +
          '<h3 id="pos-receipt-action-title" class="truncate text-sm font-black uppercase tracking-[0.12em] text-[rgb(var(--rc-page-fg))]">Receipt action</h3>' +
          '<p id="pos-receipt-action-subtitle" class="mt-1 text-xs text-[rgb(var(--rc-muted))]"></p>' +
          "</div>" +
          '<button type="button" id="pos-receipt-action-close" class="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-[rgb(var(--rc-border))] text-[rgb(var(--rc-muted))] transition hover:bg-[rgb(var(--rc-surface-2))]" aria-label="Close">✕</button>' +
          "</div>" +
          '<div id="pos-receipt-action-body" class="max-h-[68vh] overflow-auto p-4 text-sm text-[rgb(var(--rc-page-fg))]"></div>' +
          '<div class="border-t border-[rgb(var(--rc-border))]/70 bg-[rgb(var(--rc-surface))]/95 px-4 py-3">' +
          '<p id="pos-receipt-action-msg" class="text-xs text-[rgb(var(--rc-muted))]"></p>' +
          "</div>" +
          "</div>";
        document.body.appendChild(wrap);
        wrap.addEventListener("click", function (e) {
          if (e.target === wrap) wrap.classList.add("hidden");
        });
        var closeBtn = document.getElementById("pos-receipt-action-close");
        if (closeBtn) closeBtn.addEventListener("click", function () { wrap.classList.add("hidden"); });
        return wrap;
      }
      function openCancelReceiptModal(saleId, receiptNo) {
        var modal = ensureReceiptActionModal();
        var title = document.getElementById("pos-receipt-action-title");
        var subtitle = document.getElementById("pos-receipt-action-subtitle");
        var body = document.getElementById("pos-receipt-action-body");
        var msg = document.getElementById("pos-receipt-action-msg");
        if (title) title.textContent = "Cancel receipt";
        if (subtitle) subtitle.textContent = "Receipt " + String(receiptNo || ("#" + String(saleId)));
        if (body) {
          body.innerHTML =
            '<p class="mb-3 text-xs text-[rgb(var(--rc-muted))]">Enter 6-digit employee code for manager/admin/super admin verification.</p>' +
            '<label class="mb-1 block text-[10px] font-bold uppercase tracking-wider text-[rgb(var(--rc-muted))]">Employee code</label>' +
            '<input id="pos-receipt-cancel-code" type="password" inputmode="numeric" maxlength="6" pattern="[0-9]{6}" placeholder="••••••" class="w-full rounded-xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))] px-3 py-2.5 text-sm tracking-[0.2em] text-[rgb(var(--rc-page-fg))] outline-none" />' +
            '<div class="mt-3 flex justify-end gap-2">' +
            '<button type="button" id="pos-receipt-cancel-submit" class="btn-rc btn-rc-danger rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Verify and cancel</button>' +
            "</div>";
        }
        if (msg) msg.textContent = "";
        modal.classList.remove("hidden");
        var submit = document.getElementById("pos-receipt-cancel-submit");
        var input = document.getElementById("pos-receipt-cancel-code");
        if (input) input.focus();
        if (submit) {
          submit.onclick = function () {
            var code = (input && input.value) || "";
            submit.disabled = true;
            if (msg) msg.textContent = "Verifying code...";
            verifyReceiptActionCode(code)
              .then(function () {
                if (msg) msg.textContent = "Updating receipt...";
                return fetch(RECEIPTS_MARK_API, {
                  method: "POST",
                  credentials: "same-origin",
                  headers: { "Content-Type": "application/json", Accept: "application/json" },
                  body: JSON.stringify({ sale_ids: [saleId], mark_status: "cancelled" }),
                });
              })
              .then(function (r) { return r.json().catch(function () { return {}; }); })
              .then(function (j) {
                if (!j || !j.ok) throw new Error((j && j.error) || "Could not cancel receipt.");
                modal.classList.add("hidden");
                setReceiptsMsg("Receipt cancelled.", "ok");
                loadPosReceiptsList();
              })
              .catch(function (err) {
                if (msg) msg.textContent = (err && err.message) ? err.message : "Could not cancel receipt.";
                submit.disabled = false;
              });
          };
        }
      }
      function openReturnReceiptModal(sale, items) {
        var saleId = parseInt((sale && sale.id) || 0, 10) || 0;
        var recNo = (sale && sale.receipt_number) || ("#" + String(saleId));
        var modal = ensureReceiptActionModal();
        var title = document.getElementById("pos-receipt-action-title");
        var subtitle = document.getElementById("pos-receipt-action-subtitle");
        var body = document.getElementById("pos-receipt-action-body");
        var msg = document.getElementById("pos-receipt-action-msg");
        if (title) title.textContent = "Return items";
        if (subtitle) subtitle.textContent = "Receipt " + String(recNo) + " · select items then enter 6-digit code";
        var rows = (items || []).map(function (it) {
          var lineId = parseInt(it.line_id || 0, 10) || 0;
          var qty = parseFloat(it.qty || 0);
          if (isNaN(qty) || qty < 0) qty = 0;
          var qtyDisplay = fmtQty(qty);
          var total = Number(it.line_total || 0);
          return (
            "<tr class='border-t border-[rgb(var(--rc-border))]/50 hover:bg-[rgb(var(--rc-surface-2))]/40'>" +
            "<td class='py-3 px-2 text-center'><input type='checkbox' class='pos-return-line h-4 w-4 rounded border-[rgb(var(--rc-border))]' value='" + escHtml(lineId) + "'" + (qty <= 0 ? " disabled" : "") + " /></td>" +
            "<td class='py-3 px-2 text-[rgb(var(--rc-page-fg))]'><div class='font-semibold'>" + escHtml(it.item_name || "Item") + "</div></td>" +
            "<td class='py-3 px-2 text-right tabular-nums font-semibold'>" + escHtml(qtyDisplay) + "</td>" +
            "<td class='py-3 px-2 text-right tabular-nums'>" + escHtml(total.toFixed(2)) + "</td>" +
            "</tr>"
          );
        }).join("");
        if (body) {
          body.innerHTML =
            '<div class="space-y-3 rounded-2xl border border-[rgb(var(--rc-border))]/80 bg-gradient-to-br from-[rgb(var(--rc-surface-2))]/65 to-[rgb(var(--rc-surface))] p-3 sm:p-4">' +
            '<div class="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-[rgb(var(--rc-border))]/80 bg-[rgb(var(--rc-surface))]/90 px-3 py-2.5">' +
            '<p class="text-[11px] font-semibold text-[rgb(var(--rc-muted))]">Choose specific lines to return.</p>' +
            '<label class="inline-flex items-center gap-2 rounded-lg border border-[rgb(var(--rc-border))]/80 bg-[rgb(var(--rc-surface-2))]/60 px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider text-[rgb(var(--rc-page-fg))]"><input id="pos-return-select-all" type="checkbox" class="h-4 w-4 rounded border-[rgb(var(--rc-border))]" />All</label>' +
            "</div>" +
            '<div class="overflow-x-auto rounded-xl border border-[rgb(var(--rc-border))]/80 bg-[rgb(var(--rc-surface))] shadow-sm">' +
            "<table class='min-w-full text-xs sm:text-sm'><thead><tr class='text-left text-[rgb(var(--rc-muted))]'>" +
            "<th class='py-2.5 px-2 text-center font-bold uppercase tracking-wider'>Pick</th>" +
            "<th class='py-2.5 px-2 font-bold uppercase tracking-wider'>Item</th>" +
            "<th class='py-2.5 px-2 text-right font-bold uppercase tracking-wider'>Qty</th>" +
            "<th class='py-2.5 px-2 text-right font-bold uppercase tracking-wider'>Total</th>" +
            "</tr></thead><tbody>" + rows + "</tbody></table></div>" +
            '<div class="rounded-xl border border-[rgb(var(--rc-primary))]/35 bg-[rgb(var(--rc-surface))] p-3 shadow-sm">' +
            '<label class="mb-1 block text-[10px] font-bold uppercase tracking-wider text-[rgb(var(--rc-primary))]">6-digit verification code</label>' +
            '<div class="relative">' +
            '<input id="pos-receipt-return-code" type="password" inputmode="numeric" maxlength="6" pattern="[0-9]{6}" placeholder="Enter 6 digits" class="w-full rounded-xl border border-[rgb(var(--rc-primary))]/45 bg-[rgb(var(--rc-surface-2))] px-3 py-2.5 pr-11 text-sm font-semibold tracking-[0.26em] text-[rgb(var(--rc-page-fg))] outline-none transition focus:border-[rgb(var(--rc-primary))]/70 focus:ring-2 focus:ring-[rgb(var(--rc-primary))]/20" />' +
            '<button type="button" id="pos-receipt-return-code-toggle" class="absolute inset-y-0 right-1 inline-flex h-full items-center justify-center px-2 text-[rgb(var(--rc-muted))] transition hover:text-[rgb(var(--rc-page-fg))]" aria-label="Show code" title="Show code">' +
            '<svg id="pos-receipt-return-code-eye-open" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z"></path><circle cx="12" cy="12" r="3"></circle></svg>' +
            '<svg id="pos-receipt-return-code-eye-closed" class="hidden h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M3 3l18 18"></path><path d="M10.58 10.58A2 2 0 0012 14a2 2 0 001.42-.58"></path><path d="M9.36 5.37A10.94 10.94 0 0112 5c6.5 0 10 7 10 7a14.57 14.57 0 01-4.08 4.92"></path><path d="M6.23 6.23A14.54 14.54 0 002 12s3.5 7 10 7a10.94 10.94 0 005.27-1.37"></path></svg>' +
            "</button>" +
            "</div>" +
            '<p id="pos-return-picked-hint" class="mt-1.5 text-[11px] text-[rgb(var(--rc-muted))]">0 item lines selected.</p>' +
            '<p id="pos-return-code-hint" class="mt-0.5 text-[11px] text-[rgb(var(--rc-muted))]">Verification runs automatically on the 6th digit.</p>' +
            "</div>" +
            '<div class="sticky bottom-0 flex items-center justify-between gap-2 rounded-xl border border-[rgb(var(--rc-border))]/70 bg-[rgb(var(--rc-surface))]/90 px-3 py-2">' +
            '<span class="text-[11px] font-semibold text-[rgb(var(--rc-muted))]">Auto submit after valid code</span>' +
            '<button type="button" id="pos-receipt-return-submit" class="btn-rc btn-rc-danger rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Return now</button>' +
            "</div>" +
            "</div>";
        }
        if (msg) msg.textContent = "Select lines, then enter 6-digit code.";
        modal.classList.remove("hidden");
        var submit = document.getElementById("pos-receipt-return-submit");
        var codeInput = document.getElementById("pos-receipt-return-code");
        var codeToggle = document.getElementById("pos-receipt-return-code-toggle");
        var codeEyeOpen = document.getElementById("pos-receipt-return-code-eye-open");
        var codeEyeClosed = document.getElementById("pos-receipt-return-code-eye-closed");
        var selectAll = document.getElementById("pos-return-select-all");
        var pickedHint = document.getElementById("pos-return-picked-hint");
        var codeHint = document.getElementById("pos-return-code-hint");
        var returnAuthInFlight = false;
        var returnLast6 = "";
        function selectedLineIds() {
          return Array.prototype.slice
            .call(document.querySelectorAll(".pos-return-line:checked"))
            .map(function (cb) { return parseInt(cb.value || "0", 10) || 0; })
            .filter(function (n) { return n > 0; });
        }
        function updatePickedHint() {
          var count = selectedLineIds().length;
          if (pickedHint) {
            pickedHint.textContent = String(count) + " item line" + (count === 1 ? "" : "s") + " selected.";
          }
        }
        function runAutoReturnFromCode(source) {
          if (returnAuthInFlight) return;
          var picks = selectedLineIds();
          if (!picks.length) {
            if (msg) msg.textContent = "Select at least one item to return.";
            return;
          }
          var code = ((codeInput && codeInput.value) || "").replace(/\D/g, "").slice(0, 6);
          if (!/^\d{6}$/.test(code)) {
            if (msg) msg.textContent = "Enter a valid 6-digit code.";
            return;
          }
          if (source === "auto" && code === returnLast6) return;
          returnLast6 = code;
          returnAuthInFlight = true;
          if (submit) submit.disabled = true;
          if (codeInput) codeInput.disabled = true;
          if (msg) msg.textContent = "Verifying code...";
          verifyReceiptActionCode(code)
            .then(function () {
              if (msg) msg.textContent = "Code valid. Returning selected items...";
              return fetch(RECEIPTS_RETURN_LINES_API, {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json", Accept: "application/json" },
                body: JSON.stringify({ sale_id: saleId, line_ids: picks }),
              });
            })
            .then(function (r) { return r.json().catch(function () { return {}; }); })
            .then(function (j) {
              if (!j || !j.ok) throw new Error((j && j.error) || "Could not return receipt.");
              modal.classList.add("hidden");
              setReceiptsMsg("Return processed.", "ok");
              loadPosReceiptsList();
            })
            .catch(function (err) {
              if (msg) msg.textContent = (err && err.message) ? err.message : "Could not process return.";
              if (codeHint) codeHint.textContent = "Fix the code and retry.";
              returnAuthInFlight = false;
              if (submit) submit.disabled = false;
              if (codeInput) {
                codeInput.disabled = false;
                codeInput.focus();
              }
            });
        }
        if (selectAll) {
          selectAll.addEventListener("change", function () {
            Array.prototype.slice.call(document.querySelectorAll(".pos-return-line")).forEach(function (cb) {
              if (!cb.disabled) cb.checked = !!selectAll.checked;
            });
            updatePickedHint();
          });
        }
        Array.prototype.slice.call(document.querySelectorAll(".pos-return-line")).forEach(function (cb) {
          cb.addEventListener("change", updatePickedHint);
        });
        updatePickedHint();
        if (codeInput) {
          codeInput.focus();
          codeInput.select();
          codeInput.addEventListener("input", function () {
            var val = (codeInput.value || "").replace(/\D/g, "").slice(0, 6);
            codeInput.value = val;
            if (codeHint) {
              codeHint.textContent = val.length < 6
                ? "Verification runs automatically on the 6th digit."
                : "Verifying now...";
            }
            if (val.length === 6) {
              setTimeout(function () { runAutoReturnFromCode("auto"); }, 120);
            } else {
              returnLast6 = "";
            }
          });
        }
        if (codeToggle && codeInput) {
          codeToggle.addEventListener("click", function () {
            var show = codeInput.type === "password";
            codeInput.type = show ? "text" : "password";
            codeToggle.setAttribute("aria-label", show ? "Hide code" : "Show code");
            codeToggle.setAttribute("title", show ? "Hide code" : "Show code");
            if (codeEyeOpen) codeEyeOpen.classList.toggle("hidden", show);
            if (codeEyeClosed) codeEyeClosed.classList.toggle("hidden", !show);
            codeInput.focus();
          });
        }
        if (submit) {
          submit.onclick = function () {
            runAutoReturnFromCode("manual");
          };
        }
      }
      if (openReceiptsBtn) {
        openReceiptsBtn.addEventListener("click", function () {
          showReceiptsModal();
        });
      }
      if (receiptsCloseBtn) receiptsCloseBtn.addEventListener("click", hideReceiptsModal);
      if (receiptsBackdrop) receiptsBackdrop.addEventListener("click", hideReceiptsModal);
      if (receiptsListBody) {
        receiptsListBody.addEventListener("click", function (e) {
          var row = e.target && e.target.closest ? e.target.closest("[data-pos-receipt-row]") : null;
          if (!row) return;
          selectedReceiptId = parseInt(row.getAttribute("data-sale-id") || "0", 10) || 0;
          renderReceiptsRows(receiptsRowsCache);
          setReceiptsMsg("", "muted");
        });
      }
      if (receiptsCancelBtn) {
        receiptsCancelBtn.addEventListener("click", function () {
          var row = selectedReceiptRow();
          if (!row || !selectedReceiptId) {
            setReceiptsMsg("Select a receipt first.", "error");
            return;
          }
          openCancelReceiptModal(selectedReceiptId, row.receipt_number || ("#" + String(selectedReceiptId)));
        });
      }
      if (receiptsReturnBtn) {
        receiptsReturnBtn.addEventListener("click", function () {
          withSelectedReceiptDetail(function (sale, items) {
            openReturnReceiptModal(sale || {}, items || []);
          });
        });
      }
      if (receiptsReprintBtn) {
        receiptsReprintBtn.addEventListener("click", function () {
          withSelectedReceiptDetail(function (sale, items) {
            setReceiptsMsg("Sending receipt to printer...", "muted");
            var payload = buildPersistedReceiptPayload(sale, items);
            return runConfiguredPrinterAction(payload, { receiptVariants: receiptVariantsForCheckout() })
              .then(function () {
                setReceiptsMsg("Reprint sent.", "ok");
              })
              .catch(function () {
                setReceiptsMsg("Could not print receipt.", "error");
              });
          });
        });
      }
      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape" && receiptsModal && !receiptsModal.classList.contains("hidden")) {
          hideReceiptsModal();
        }
      });
      var stockInOpenBtn = document.getElementById("pos-buy-open");
      var stockInCloseBtn = document.getElementById("pos-stockin-close");
      var stockInCancelBtn = document.getElementById("pos-stockin-cancel");
      var stockInBackdrop = document.getElementById("pos-stockin-backdrop");
      var stockInSearch = document.getElementById("pos-stockin-search");
      var stockInSelect = document.getElementById("pos-stockin-item-id");
      var stockInForm = document.getElementById("pos-stockin-form");
      var stockInSellerPhone = document.getElementById("pos-stockin-seller-phone");
      var stockInSellerName = document.getElementById("pos-stockin-seller-name");
      var stockInSellerHint = document.getElementById("pos-stockin-seller-hint");
      var stockInFeedback = document.getElementById("pos-stockin-feedback");
      var stockInPrintBtn = document.getElementById("pos-stockin-print");
      var stockInAuthCode = document.getElementById("pos-stockin-auth-code");
      var stockInAuthStatus = document.getElementById("pos-stockin-auth-status");
      var stockInAuthorizedEmployee = null;
      var stockInAuthVerifyInFlight = false;
      var stockInAuthSixTimer = null;
      var stockInLastSixCode = "";
      var sellerLookupTimer = null;
      var stockInReceiptUrl = "";

      function setStockInAuthStatus(msg, tone) {
        if (!stockInAuthStatus) return;
        stockInAuthStatus.textContent = msg || "";
        stockInAuthStatus.classList.remove("pos-auth-status--ok", "pos-auth-status--error", "pos-auth-status--muted");
        stockInAuthStatus.classList.add(
          tone === "ok" ? "pos-auth-status--ok" : tone === "error" ? "pos-auth-status--error" : "pos-auth-status--muted"
        );
      }

      function resetStockInEmployeeAuth() {
        stockInAuthorizedEmployee = null;
        stockInLastSixCode = "";
        clearTimeout(stockInAuthSixTimer);
        if (stockInAuthCode) {
          stockInAuthCode.value = "";
          stockInAuthCode.disabled = false;
        }
        setStockInAuthStatus("Fill the form above, then enter the employee code to save.", "muted");
      }

      function attemptStockInVerifyAndSubmit(six) {
        if (!six || !/^\d{6}$/.test(six)) return;
        if (stockInAuthVerifyInFlight) return;
        if (!stockInForm) return;
        if (!stockInForm.checkValidity()) {
          setStockInAuthStatus("Fill all required fields above, then enter the 6-digit code again.", "error");
          if (stockInAuthCode) {
            stockInAuthCode.value = "";
            stockInLastSixCode = "";
            stockInAuthCode.focus();
          }
          return;
        }
        stockInAuthVerifyInFlight = true;
        if (stockInAuthCode) stockInAuthCode.disabled = true;
        setStockInAuthStatus("Verifying…", "muted");
        fetch(AUTH_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ employee_code: six }),
        })
          .then(function (r) {
            return r.json().then(function (j) {
              return { ok: r.ok, j: j };
            });
          })
          .then(function (x) {
            if (!x.ok || !x.j || !x.j.ok) {
              stockInAuthorizedEmployee = null;
              setStockInAuthStatus((x.j && x.j.error) || "Invalid employee code.", "error");
              if (stockInAuthCode) {
                stockInAuthCode.value = "";
                stockInLastSixCode = "";
                stockInAuthCode.disabled = false;
                stockInAuthCode.focus();
              }
              stockInAuthVerifyInFlight = false;
              return;
            }
            stockInAuthorizedEmployee = x.j.employee || null;
            employeeAuthCacheUpsert(stockInAuthorizedEmployee);
            var nm =
              stockInAuthorizedEmployee && stockInAuthorizedEmployee.full_name
                ? stockInAuthorizedEmployee.full_name
                : "Employee";
            setStockInAuthStatus("Authorized: " + nm + ". Saving…", "ok");
            /* Disabled inputs are not submitted — must include employee_code in POST. */
            if (stockInAuthCode) stockInAuthCode.disabled = false;
            stockInForm.requestSubmit();
          })
          .catch(function (err) {
            if (looksLikeOfflineNetworkError(err)) {
              var cached = employeeAuthCacheFindByCode(six);
              if (cached) {
                stockInAuthorizedEmployee = cached;
                setStockInAuthStatus(
                  "Authorized offline: " + (cached.full_name || "Employee") + ". Saving…",
                  "ok"
                );
                if (stockInAuthCode) stockInAuthCode.disabled = false;
                stockInForm.requestSubmit();
                return;
              }
              stockInAuthorizedEmployee = null;
              setStockInAuthStatus("Offline: code not in local cache. Connect once, then retry.", "error");
            } else {
              stockInAuthorizedEmployee = null;
              setStockInAuthStatus("Could not verify code. Try again.", "error");
            }
            if (stockInAuthCode) {
              stockInAuthCode.value = "";
              stockInLastSixCode = "";
              stockInAuthCode.disabled = false;
              stockInAuthCode.focus();
            }
            stockInAuthVerifyInFlight = false;
          });
      }

      function onStockInAuthCodeInput() {
        if (!stockInAuthCode) return;
        var v = String(stockInAuthCode.value || "").replace(/\D/g, "").slice(0, 6);
        if (stockInAuthCode.value !== v) stockInAuthCode.value = v;
        stockInAuthorizedEmployee = null;
        if (v.length < 6) {
          stockInLastSixCode = "";
          if (!v.length) {
            setStockInAuthStatus("Fill the form above, then enter the employee code to save.", "muted");
          } else {
            setStockInAuthStatus("Entering code… " + v.length + "/6", "muted");
          }
          return;
        }
        if (v === stockInLastSixCode) return;
        stockInLastSixCode = v;
        clearTimeout(stockInAuthSixTimer);
        stockInAuthSixTimer = setTimeout(function () {
          attemptStockInVerifyAndSubmit(v);
        }, 50);
      }

      function setStockInModalOpen(open) {
        if (!stockInModal) return;
        stockInModal.classList.toggle("hidden", !open);
        stockInModal.setAttribute("aria-hidden", open ? "false" : "true");
        if (open) {
          resetStockInEmployeeAuth();
          if (stockInSearch) {
            setTimeout(function () {
              stockInSearch.focus();
            }, 30);
          }
        }
      }

      function filterStockInOptions() {
        if (!stockInSelect) return;
        var q = ((stockInSearch && stockInSearch.value) || "").trim().toLowerCase();
        var firstVisible = "";
        Array.prototype.forEach.call(stockInSelect.options, function (opt, idx) {
          if (idx === 0) {
            opt.hidden = false;
            return;
          }
          var hay = (opt.getAttribute("data-search") || "").toLowerCase();
          var show = !q || hay.indexOf(q) !== -1;
          opt.hidden = !show;
          if (show && !firstVisible) firstVisible = opt.value;
        });
        if (firstVisible) stockInSelect.value = firstVisible;
      }

      function lookupSellerByPhone() {
        if (!stockInSellerPhone || !stockInSellerName) return;
        var phone = (stockInSellerPhone.value || "").trim();
        if (phone.indexOf("+254") === 0) {
          phone = "254" + phone.slice(4);
        }
        phone = phone.replace(/\s+/g, "");
        if (stockInSellerPhone.value !== phone) stockInSellerPhone.value = phone;
        var digits = phone.replace(/\D/g, "");
        if (!phone) {
          if (stockInSellerHint) stockInSellerHint.textContent = "";
          return;
        }
        if (digits.length < 10) {
          if (stockInSellerHint) stockInSellerHint.textContent = "Continue typing seller phone (10 digits)...";
          return;
        }
        var valid10 = /^((07|01)\d{8})$/.test(digits);
        var valid12 = /^(254\d{9})$/.test(digits);
        if (!valid10 && !valid12) {
          if (stockInSellerHint) stockInSellerHint.textContent = "Use 07… or 01… (10 digits) or 254… (12 digits).";
          return;
        }
        var fd = new FormData();
        fd.append("seller_phone", phone);
        fetch(SELLER_LOOKUP_API, { method: "POST", body: fd })
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (!data || !data.ok || !data.registered || !data.seller) {
              if (stockInSellerHint) stockInSellerHint.textContent = "New seller phone. Enter seller name to register.";
              return;
            }
            stockInSellerName.value = (data.seller.seller_name || "").toUpperCase();
            if (stockInSellerHint) stockInSellerHint.textContent = "Registered seller found. Name auto-filled.";
          })
          .catch(function () {
            if (stockInSellerHint) stockInSellerHint.textContent = "";
          });
      }

      if (stockInOpenBtn) {
        stockInOpenBtn.addEventListener("click", function () {
          setStockInModalOpen(true);
          filterStockInOptions();
        });
      }
      if (stockInCloseBtn) stockInCloseBtn.addEventListener("click", function () { setStockInModalOpen(false); });
      if (stockInCancelBtn) stockInCancelBtn.addEventListener("click", function () { setStockInModalOpen(false); });
      if (stockInBackdrop) stockInBackdrop.addEventListener("click", function () { setStockInModalOpen(false); });
      if (stockInAuthCode) {
        stockInAuthCode.addEventListener("input", onStockInAuthCodeInput);
      }
      if (stockInSearch) stockInSearch.addEventListener("input", filterStockInOptions);
      if (stockInForm) {
        var stockInQtyInput = stockInForm.querySelector('input[name="qty"]');
        if (stockInQtyInput) {
          stockInQtyInput.addEventListener("focus", function () {
            if (!stockInSearch || !stockInSelect) return;
            if ((stockInSearch.value || "").trim() === "") return;
            var selected = stockInSelect.value || "";
            stockInSearch.value = "";
            filterStockInOptions();
            if (selected) stockInSelect.value = selected;
            stockInSelect.classList.add("ring-2", "ring-brand-400/60");
            setTimeout(function () {
              stockInSelect.classList.remove("ring-2", "ring-brand-400/60");
            }, 1200);
          });
        }
      }
      if (stockInSellerPhone) {
        stockInSellerPhone.addEventListener("input", function () {
          if (sellerLookupTimer) clearTimeout(sellerLookupTimer);
          sellerLookupTimer = setTimeout(lookupSellerByPhone, 220);
        });
      }
      if (stockInPrintBtn) {
        stockInPrintBtn.addEventListener("click", function () {
          if (!stockInReceiptUrl) return;
          triggerStockInReceiptPrint();
        });
      }
      function triggerStockInReceiptPrint() {
        if (!stockInReceiptUrl) return;
        var frame = document.createElement("iframe");
        frame.style.position = "fixed";
        frame.style.right = "0";
        frame.style.bottom = "0";
        frame.style.width = "0";
        frame.style.height = "0";
        frame.style.border = "0";
        frame.setAttribute("aria-hidden", "true");
        frame.src = stockInReceiptUrl;
        frame.onload = function () {
          try {
            var w = frame.contentWindow;
            if (w) {
              w.focus();
              w.print();
            }
          } catch (e) {}
          setTimeout(function () {
            try { frame.remove(); } catch (e) {}
          }, 1500);
        };
        document.body.appendChild(frame);
      }
      if (stockInForm) {
        stockInForm.addEventListener("submit", function (ev) {
          ev.preventDefault();
          var submitBtn = stockInForm.querySelector('button[type="submit"]');
          var oldText = submitBtn ? submitBtn.textContent : "";
          if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.textContent = "Saving...";
          }
          if (stockInFeedback) {
            stockInFeedback.classList.add("hidden");
            stockInFeedback.textContent = "";
          }
          if (stockInPrintBtn) stockInPrintBtn.classList.add("hidden");
          var fd = new FormData(stockInForm);
          fetch(stockInForm.action, {
            method: "POST",
            body: fd,
            credentials: "same-origin",
            headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
          })
            .then(function (r) { return r.json().catch(function () { return {}; }); })
            .then(function (j) {
              if (!j || !j.ok) throw new Error((j && j.error) || "Could not stock in item.");
              if (stockInFeedback) {
                stockInFeedback.textContent = j.message || "Item stocked in successfully.";
                stockInFeedback.classList.remove("hidden");
              }
              stockInReceiptUrl = (j.receipt_url || "").trim();
              if (stockInReceiptUrl && stockInPrintBtn) stockInPrintBtn.classList.remove("hidden");
              stockInForm.reset();
              resetStockInEmployeeAuth();
              if (stockInReceiptUrl) {
                setTimeout(function () { triggerStockInReceiptPrint(); }, 120);
              }
              if (typeof window.refreshPosCatalogStock === "function") {
                setTimeout(function () { window.refreshPosCatalogStock(); }, 350);
              }
            })
            .catch(function (e) {
              // Phase 3 — offline fallback: queue the stock-in locally and bump the cached catalog.
              if (looksLikeOfflineNetworkError(e)) {
                var payloadSi = {
                  item_id: fd.get("item_id"),
                  qty: fd.get("qty"),
                  buying_price: fd.get("buying_price"),
                  seller_phone: fd.get("seller_phone"),
                  seller_name: fd.get("seller_name"),
                  note: fd.get("note"),
                  employee_code: fd.get("employee_code"),
                };
                return putOfflineStockIn(payloadSi).then(function () {
                  var idNum = parseInt(payloadSi.item_id, 10);
                  var qtyNum = parseFloat(payloadSi.qty);
                  // In "both" mode the stock-in form targets store_stock_items (backroom)
                  // which lives in a separate table from the POS catalog (shop_items).
                  // Skip the local delta to avoid an accidental id collision bumping the
                  // wrong shop_item; the backroom stock isn't displayed in the POS grid anyway.
                  var modeNow = String(window.POS_INVENTORY_MODE || "shop").toLowerCase();
                  var skipLocalDelta = modeNow === "both";
                  if (!skipLocalDelta && isFinite(idNum) && isFinite(qtyNum) && qtyNum > 0) {
                    var d = {}; d[idNum] = qtyNum;
                    // Always target shop_stock_qty: server's shop_manual_stock_in writes
                    // shop_stock_qty regardless of inventory_mode. In kitchen mode the
                    // change is invisible in the POS grid (kitchen_portions is shown),
                    // matching the existing online behavior — no drift on sync.
                    try { applyLocalCatalogDelta(d, "shop_stock_qty"); } catch (eApply) {}
                  }
                  if (stockInFeedback) {
                    stockInFeedback.textContent =
                      "Offline: stock-in queued. It will sync automatically when internet returns.";
                    stockInFeedback.classList.remove("hidden");
                  }
                  stockInForm.reset();
                  resetStockInEmployeeAuth();
                  updateOfflineSyncBadge();
                });
              }
              if (stockInFeedback) {
                stockInFeedback.textContent = (e && e.message) ? e.message : "Could not stock in item.";
                stockInFeedback.classList.remove("hidden");
              }
            })
            .finally(function () {
              stockInAuthVerifyInFlight = false;
              if (stockInAuthCode) stockInAuthCode.disabled = false;
              if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.textContent = oldText || "Save";
              }
            });
        });
      }

      // ===== Kitchen portion refill modal (kitchen / both mode only) =====
      (function initRefillPortionsModal() {
        var modal = document.getElementById("pos-refill-portions-modal");
        var openBtn = document.getElementById("pos-refill-portions-open");
        if (!modal || !openBtn) return; // not rendered in shop mode
        var backdrop = document.getElementById("pos-refill-portions-backdrop");
        var closeBtn = document.getElementById("pos-refill-portions-close");
        var cancelBtn = document.getElementById("pos-refill-portions-cancel");
        var form = document.getElementById("pos-refill-portions-form");
        var search = document.getElementById("pos-refill-portions-search");
        var select = document.getElementById("pos-refill-portions-item-id");
        var qtyInput = document.getElementById("pos-refill-portions-qty");
        var authCode = document.getElementById("pos-refill-portions-auth-code");
        var authStatus = document.getElementById("pos-refill-portions-auth-status");
        var feedback = document.getElementById("pos-refill-portions-feedback");

        function setAuthStatus(msg, tone) {
          if (!authStatus) return;
          authStatus.textContent = msg || "";
          authStatus.classList.remove(
            "pos-auth-status--muted",
            "pos-auth-status--ok",
            "pos-auth-status--error"
          );
          authStatus.classList.add("pos-auth-status--" + (tone || "muted"));
        }
        function resetAuth() {
          if (authCode) {
            authCode.value = "";
            authCode.disabled = false;
          }
          setAuthStatus("Fill the form above, then enter the employee code to save.", "muted");
        }
        function setOpen(open) {
          modal.classList.toggle("hidden", !open);
          modal.setAttribute("aria-hidden", open ? "false" : "true");
          if (open) {
            resetAuth();
            if (feedback) {
              feedback.classList.add("hidden");
              feedback.textContent = "";
            }
            setTimeout(function () { if (search) search.focus(); }, 30);
          }
        }

        function filterOptions() {
          if (!select) return;
          var q = ((search && search.value) || "").trim().toLowerCase();
          var firstVisible = "";
          Array.prototype.forEach.call(select.options, function (opt, idx) {
            if (idx === 0) { opt.hidden = false; return; }
            var hay = (opt.getAttribute("data-search") || "").toLowerCase();
            var show = !q || hay.indexOf(q) !== -1;
            opt.hidden = !show;
            if (show && !firstVisible) firstVisible = opt.value;
          });
          if (firstVisible) select.value = firstVisible;
        }

        var lastSixCode = "";
        var verifyTimer = null;
        var verifyInFlight = false;

        function onAuthCodeInput() {
          if (!authCode) return;
          var v = String(authCode.value || "").replace(/\D/g, "").slice(0, 6);
          if (authCode.value !== v) authCode.value = v;
          if (v.length < 6) {
            lastSixCode = "";
            if (!v.length) {
              setAuthStatus("Fill the form above, then enter the employee code to save.", "muted");
            } else {
              setAuthStatus("Entering code… " + v.length + "/6", "muted");
            }
            return;
          }
          if (v === lastSixCode) return;
          lastSixCode = v;
          clearTimeout(verifyTimer);
          verifyTimer = setTimeout(function () { triggerSubmit(); }, 50);
        }

        function triggerSubmit() {
          if (!form) return;
          if (verifyInFlight) return;
          var itemIdRaw = (select && select.value) || "";
          var qtyRaw = (qtyInput && qtyInput.value) || "";
          var itemId = parseInt(itemIdRaw, 10);
          var qty = parseInt(qtyRaw, 10);
          if (!isFinite(itemId) || itemId <= 0) {
            setAuthStatus("Pick a kitchen-tracked item first.", "error");
            return;
          }
          if (!isFinite(qty) || qty <= 0) {
            setAuthStatus("Enter portions to add (1 or more).", "error");
            return;
          }
          form.requestSubmit();
        }

        if (openBtn) {
          openBtn.addEventListener("click", function () { setOpen(true); filterOptions(); });
        }
        if (closeBtn) closeBtn.addEventListener("click", function () { setOpen(false); });
        if (cancelBtn) cancelBtn.addEventListener("click", function () { setOpen(false); });
        if (backdrop) backdrop.addEventListener("click", function () { setOpen(false); });
        if (search) search.addEventListener("input", filterOptions);
        if (authCode) authCode.addEventListener("input", onAuthCodeInput);

        if (form) {
          form.addEventListener("submit", function (ev) {
            ev.preventDefault();
            if (verifyInFlight) return;
            verifyInFlight = true;
            setAuthStatus("Saving…", "muted");
            if (feedback) {
              feedback.classList.add("hidden");
              feedback.textContent = "";
            }
            var fd = new FormData(form);
            var itemId = parseInt(fd.get("item_id") || "0", 10);
            var qty = parseInt(fd.get("qty") || "0", 10);
            fetch(form.action, {
              method: "POST",
              body: fd,
              credentials: "same-origin",
              headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
            })
              .then(function (r) { return r.json().catch(function () { return {}; }); })
              .then(function (j) {
                if (!j || !j.ok) throw new Error((j && j.error) || "Could not refill kitchen portions.");
                if (feedback) {
                  feedback.textContent = j.message || ("Added " + qty + " portion(s).");
                  feedback.classList.remove("hidden");
                }
                // Apply locally so the cashier sees the change immediately even before refresh.
                if (isFinite(itemId) && isFinite(qty) && qty > 0) {
                  try { applyLocalKitchenPortionDelta(itemId, qty); } catch (eApplyOn) {}
                }
                form.reset();
                resetAuth();
                if (typeof window.refreshPosCatalogStock === "function") {
                  setTimeout(function () { window.refreshPosCatalogStock(); }, 350);
                }
              })
              .catch(function (e) {
                if (looksLikeOfflineNetworkError(e)) {
                  var payloadRf = {
                    item_id: fd.get("item_id"),
                    qty: fd.get("qty"),
                    note: fd.get("note"),
                    employee_code: fd.get("employee_code"),
                  };
                  return putOfflinePortionRefill(payloadRf).then(function () {
                    if (isFinite(itemId) && isFinite(qty) && qty > 0) {
                      try { applyLocalKitchenPortionDelta(itemId, qty); } catch (eApplyOff) {}
                    }
                    if (feedback) {
                      feedback.textContent =
                        "Offline: portion refill queued. It will sync automatically when internet returns.";
                      feedback.classList.remove("hidden");
                    }
                    form.reset();
                    resetAuth();
                    updateOfflineSyncBadge();
                  });
                }
                if (feedback) {
                  feedback.textContent = (e && e.message) ? e.message : "Could not refill kitchen portions.";
                  feedback.classList.remove("hidden");
                }
                setAuthStatus("Could not save. Try again.", "error");
              })
              .finally(function () {
                verifyInFlight = false;
                if (authCode) authCode.disabled = false;
              });
          });
        }

        // Refresh dropdown when item filter changes elsewhere (best-effort, harmless).
        if (qtyInput) {
          qtyInput.addEventListener("focus", function () {
            if (!search || !select) return;
            if ((search.value || "").trim() === "") return;
            var selected = select.value || "";
            search.value = "";
            filterOptions();
            if (selected) select.value = selected;
          });
        }
      })();

      function normalizeCartLine(l) {
        if (!l || typeof l !== "object") return l;
        var p = parseFloat(l.price);
        if (isNaN(p)) p = 0;
        l.price = p;
        var lp = l.listPrice;
        if (lp == null || lp === "" || isNaN(parseFloat(lp))) {
          l.listPrice = p;
        } else {
          l.listPrice = parseFloat(lp);
        }
        var op = l.originalSellingPrice;
        if (op == null || op === "" || isNaN(parseFloat(op))) {
          l.originalSellingPrice = p;
        } else {
          l.originalSellingPrice = parseFloat(op);
        }
        return l;
      }

      function load() {
        try {
          var raw = localStorage.getItem(KEY);
          if (!raw) return [];
          var arr = JSON.parse(raw);
          if (!Array.isArray(arr)) return [];
          return arr.map(function (l) {
            return normalizeCartLine(l);
          });
        } catch (e) {
          return [];
        }
      }

      function save(lines) {
        localStorage.setItem(KEY, JSON.stringify(lines));
      }

      function idbSupported() {
        return typeof window !== "undefined" && !!window.indexedDB;
      }

      function openOfflineDb() {
        return new Promise(function (resolve, reject) {
          if (!idbSupported()) {
            reject(new Error("IndexedDB not supported"));
            return;
          }
          var req = window.indexedDB.open(OFFLINE_DB_NAME, OFFLINE_DB_VERSION);
          req.onupgradeneeded = function (ev) {
            var db = ev.target.result;
            if (!db.objectStoreNames.contains(OFFLINE_DB_STORE)) {
              db.createObjectStore(OFFLINE_DB_STORE, { keyPath: "client_txn_id" });
            }
            if (!db.objectStoreNames.contains(OFFLINE_DB_CATALOG_STORE)) {
              db.createObjectStore(OFFLINE_DB_CATALOG_STORE, { keyPath: "id" });
            }
            if (!db.objectStoreNames.contains(OFFLINE_DB_STOCKIN_STORE)) {
              db.createObjectStore(OFFLINE_DB_STOCKIN_STORE, { keyPath: "local_id" });
            }
            if (!db.objectStoreNames.contains(OFFLINE_DB_REFILL_STORE)) {
              db.createObjectStore(OFFLINE_DB_REFILL_STORE, { keyPath: "local_id" });
            }
          };
          req.onsuccess = function () { resolve(req.result); };
          req.onerror = function () { reject(req.error || new Error("Could not open offline DB")); };
        });
      }

      function listOfflineSales() {
        if (!idbSupported()) {
          try {
            var raw = localStorage.getItem(OFFLINE_SALES_KEY);
            var arr = raw ? JSON.parse(raw) : [];
            return Promise.resolve(Array.isArray(arr) ? arr : []);
          } catch (e) {
            return Promise.resolve([]);
          }
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STORE, "readonly");
            var req = tx.objectStore(OFFLINE_DB_STORE).getAll();
            req.onsuccess = function () {
              var rows = Array.isArray(req.result) ? req.result : [];
              rows.sort(function (a, b) {
                return String((a && a.queued_at) || "").localeCompare(String((b && b.queued_at) || ""));
              });
              resolve(rows);
            };
            req.onerror = function () { reject(req.error || new Error("Could not read queue")); };
          }).finally(function () {
            try { db.close(); } catch (e) {}
          });
        }).catch(function () {
          try {
            var raw = localStorage.getItem(OFFLINE_SALES_KEY);
            var arr = raw ? JSON.parse(raw) : [];
            return Array.isArray(arr) ? arr : [];
          } catch (e) {
            return [];
          }
        });
      }

      function putOfflineSale(entry) {
        if (!idbSupported()) {
          return listOfflineSales().then(function (rows) {
            rows.push(entry);
            localStorage.setItem(OFFLINE_SALES_KEY, JSON.stringify(rows));
            return entry;
          });
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STORE, "readwrite");
            tx.oncomplete = function () { resolve(entry); };
            tx.onerror = function () { reject(tx.error || new Error("Could not store offline sale")); };
            tx.objectStore(OFFLINE_DB_STORE).put(entry);
          }).finally(function () {
            try { db.close(); } catch (e) {}
          });
        }).catch(function () {
          return listOfflineSales().then(function (rows) {
            rows.push(entry);
            localStorage.setItem(OFFLINE_SALES_KEY, JSON.stringify(rows));
            return entry;
          });
        });
      }

      function deleteOfflineSale(clientTxnId) {
        var txid = String(clientTxnId || "");
        if (!txid) return Promise.resolve();
        if (!idbSupported()) {
          return listOfflineSales().then(function (rows) {
            var next = rows.filter(function (r) { return String(r.client_txn_id || "") !== txid; });
            localStorage.setItem(OFFLINE_SALES_KEY, JSON.stringify(next));
          });
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STORE, "readwrite");
            tx.oncomplete = function () { resolve(); };
            tx.onerror = function () { reject(tx.error || new Error("Could not delete offline sale")); };
            tx.objectStore(OFFLINE_DB_STORE).delete(txid);
          }).finally(function () {
            try { db.close(); } catch (e) {}
          });
        }).catch(function () {
          return listOfflineSales().then(function (rows) {
            var next = rows.filter(function (r) { return String(r.client_txn_id || "") !== txid; });
            localStorage.setItem(OFFLINE_SALES_KEY, JSON.stringify(next));
          });
        });
      }

      function getPendingOfflineSalesCount() {
        return listOfflineSales().then(function (rows) { return rows.length; }).catch(function () { return 0; });
      }

      function updateNetworkStateIndicator() {
        if (!networkStateIndicatorEl) return;
        var online = navigator.onLine;
        var labelEl = networkStateIndicatorEl.querySelector(".pos-header-action__text");
        networkStateIndicatorEl.classList.remove(
          "border-emerald-500/40",
          "bg-emerald-500/15",
          "text-emerald-700",
          "dark:text-emerald-300",
          "border-rose-500/40",
          "bg-rose-500/15",
          "text-rose-700",
          "dark:text-rose-300"
        );
        networkStateIndicatorEl.setAttribute("data-network-state", online ? "online" : "offline");
        if (online) {
          networkStateIndicatorEl.classList.add("border-emerald-500/40", "bg-emerald-500/15", "text-emerald-700", "dark:text-emerald-300");
          networkStateIndicatorEl.setAttribute("title", "Online — tap for today's offline queue");
          networkStateIndicatorEl.setAttribute("aria-label", "Online. Open today's offline queue.");
          if (labelEl) labelEl.textContent = "Online";
        } else {
          networkStateIndicatorEl.classList.add("border-rose-500/40", "bg-rose-500/15", "text-rose-700", "dark:text-rose-300");
          networkStateIndicatorEl.setAttribute("title", "Offline — tap for today's offline queue");
          networkStateIndicatorEl.setAttribute("aria-label", "Offline. Open today's offline queue.");
          if (labelEl) labelEl.textContent = "Offline";
        }
      }

      function appendOfflineSyncLogEntry(entry) {
        var rec = String((entry && entry.receipt) || "").trim() || "Sale";
        offlineSyncRecentLog.unshift({
          at: new Date().toISOString(),
          receipt: rec,
          ok: !!(entry && entry.ok),
          detail: String((entry && entry.detail) || "").trim(),
          kind: String((entry && entry.kind) || "").trim(),
          client_txn_id: String((entry && entry.client_txn_id) || "").trim(),
        });
        while (offlineSyncRecentLog.length > OFFLINE_SYNC_LOG_CAP) offlineSyncRecentLog.pop();
        persistOfflineSyncLogDay();
      }

      function showOfflineSyncResultToast(stats) {
        var toast = document.getElementById("pos-sync-toast");
        var iconEl = document.getElementById("pos-sync-toast-icon");
        var titleEl = document.getElementById("pos-sync-toast-title");
        var detailEl = document.getElementById("pos-sync-toast-detail");
        if (!toast || !iconEl || !titleEl || !detailEl || !stats || stats.skipped) return;
        if ((stats.synced || 0) + (stats.failed || 0) === 0) return;
        clearTimeout(offlineSyncToastHideTimer);
        var tickSvg =
          '<svg class="h-6 w-6 shrink-0 text-emerald-600 dark:text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/></svg>';
        var xSvg =
          '<svg class="h-6 w-6 shrink-0 text-rose-600 dark:text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M6 18L18 6M6 6l12 12"/></svg>';
        var ok = (stats.failed || 0) === 0 && (stats.synced || 0) > 0;
        var partialOk = (stats.synced || 0) > 0 && (stats.failed || 0) > 0;
        var allFail = (stats.synced || 0) === 0 && (stats.failed || 0) > 0;
        toast.classList.remove("pos-sync-toast--ok", "pos-sync-toast--err");
        if (ok) {
          iconEl.innerHTML = tickSvg;
          titleEl.textContent = "Offline queue synced";
          detailEl.textContent = String(stats.synced) + " checkout(s) saved to the server.";
          toast.classList.add("pos-sync-toast--ok");
        } else if (partialOk) {
          iconEl.innerHTML = xSvg;
          titleEl.textContent = "Sync partly failed";
          var lines = (stats.errors || []).map(function (e) {
            return String((e && e.receipt) || "Sale") + ": " + String((e && e.message) || "");
          });
          detailEl.textContent = lines.slice(0, 3).join(" · ") + ((stats.failed || 0) > 3 ? " …" : "");
          toast.classList.add("pos-sync-toast--err");
        } else if (allFail) {
          iconEl.innerHTML = xSvg;
          titleEl.textContent = "Sync failed";
          var e0 = (stats.errors && stats.errors[0]) || {};
          detailEl.textContent =
            String(e0.message || "Could not upload offline sales.") +
            ((stats.failed || 0) > 1 ? " (" + String(stats.failed) + " pending)" : "");
          toast.classList.add("pos-sync-toast--err");
        } else {
          return;
        }
        toast.classList.remove("hidden");
        toast.classList.add("pos-toast-show");
        var hideMs = ok ? 4200 : 6800;
        offlineSyncToastHideTimer = setTimeout(function () {
          toast.classList.add("hidden");
          toast.classList.remove("pos-toast-show");
        }, hideMs);
      }

      function mergeOfflineSyncStats(a, b) {
        a = a || {};
        b = b || {};
        return {
          skipped: false,
          synced: (parseInt(a.synced, 10) || 0) + (parseInt(b.synced, 10) || 0),
          failed: (parseInt(a.failed, 10) || 0) + (parseInt(b.failed, 10) || 0),
          errors: (a.errors || []).concat(b.errors || []),
          attempted: (parseInt(a.attempted, 10) || 0) + (parseInt(b.attempted, 10) || 0),
        };
      }

      function updateOfflineSyncBadge() {
        updateNetworkStateIndicator();
        if (!offlineSyncBadgeEl) return Promise.resolve();
        return Promise.all([
          getPendingOfflineSalesCount(),
          getPendingOfflineStockInsCount(),
          getPendingOfflinePortionRefillsCount(),
        ]).then(function (counts) {
          var saleCount = counts[0] || 0;
          var stockInCount = counts[1] || 0;
          var refillCount = counts[2] || 0;
          var count = saleCount + stockInCount + refillCount;
          var online = navigator.onLine;
          if (offlineSyncNowBtn) offlineSyncNowBtn.classList.toggle("hidden", count === 0);
          if (offlineSyncDiagBtn) offlineSyncDiagBtn.classList.toggle("hidden", count === 0);
          if (offlineSyncNowBtn) offlineSyncNowBtn.disabled = offlineSyncBusy || count === 0;
          if (offlineSyncDiagBtn) offlineSyncDiagBtn.disabled = count === 0;
          if (!count && online) {
            offlineSyncBadgeEl.classList.add("hidden");
            offlineSyncBadgeEl.textContent = "Sync · 0";
            return;
          }
          offlineSyncBadgeEl.classList.remove("hidden");
          if (count > 0) {
            var parts = [];
            if (saleCount) parts.push(saleCount + " sale" + (saleCount === 1 ? "" : "s"));
            if (stockInCount) parts.push(stockInCount + " stock-in" + (stockInCount === 1 ? "" : "s"));
            if (refillCount) parts.push(refillCount + " portion refill" + (refillCount === 1 ? "" : "s"));
            var detail = parts.length > 1 ? " (" + parts.join(" + ") + ")" : "";
            offlineSyncBadgeEl.textContent = "Sync · " + String(count) + detail + (online ? "" : " offline");
          } else {
            offlineSyncBadgeEl.textContent = "Offline";
          }
        }).catch(function () {});
      }

      function fmtDiagDate(iso) {
        var s = String(iso || "").trim();
        if (!s) return "-";
        var t = Date.parse(s);
        if (!isFinite(t)) return s;
        return new Date(t).toLocaleString();
      }

      function offlineKindLabel(kind, row) {
        var k = String(kind || "").toLowerCase();
        if (k === "quote") return "Quote";
        if (k === "credit") return "Credit";
        if (k === "sale") return "Sale";
        if (row) {
          var rk = String(row.pos_offline_record_kind || "").toLowerCase();
          if (rk === "quote") return "Quote";
          if (rk === "sale") return "Sale";
          if (String(row.sale_type || "").toLowerCase() === "credit") return "Credit";
        }
        return "Sale";
      }

      function ensureOfflineSyncDiagnosticsModal() {
        var existing = document.getElementById("pos-offline-diag-modal");
        if (existing) return existing;
        var wrap = document.createElement("div");
        wrap.id = "pos-offline-diag-modal";
        wrap.className = "fixed inset-0 z-[145] hidden items-end justify-center bg-black/55 p-0 sm:items-center sm:p-4";
        wrap.innerHTML =
          '<div class="w-full max-w-4xl overflow-hidden rounded-t-2xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface))] shadow-2xl sm:rounded-2xl">' +
          '<div class="flex items-center justify-between gap-3 border-b border-[rgb(var(--rc-border))]/70 px-4 py-3">' +
          '<div class="min-w-0"><h3 class="truncate text-sm font-black uppercase tracking-[0.12em] text-[rgb(var(--rc-page-fg))]">Offline queue diagnostics</h3>' +
          '<p id="pos-offline-diag-sub" class="mt-1 text-xs text-[rgb(var(--rc-muted))]">Loading...</p></div>' +
          '<button type="button" id="pos-offline-diag-close" class="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-[rgb(var(--rc-border))] text-[rgb(var(--rc-muted))] hover:bg-[rgb(var(--rc-surface-2))]" aria-label="Close">✕</button>' +
          "</div>" +
          '<div class="max-h-[68vh] overflow-auto p-4">' +
          '<div id="pos-offline-diag-list" class="space-y-2 text-xs text-[rgb(var(--rc-page-fg))]"></div>' +
          "</div>" +
          '<div class="flex flex-wrap items-center justify-end gap-2 border-t border-[rgb(var(--rc-border))]/70 px-4 py-3">' +
          '<button type="button" id="pos-offline-diag-refresh" class="btn-rc btn-rc-ghost rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Refresh</button>' +
          '<button type="button" id="pos-offline-diag-sync" class="btn-rc btn-rc-primary rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Sync now</button>' +
          "</div>" +
          "</div>";
        document.body.appendChild(wrap);
        wrap.addEventListener("click", function (e) {
          if (e.target === wrap) wrap.classList.add("hidden");
        });
        var closeBtn = document.getElementById("pos-offline-diag-close");
        if (closeBtn) closeBtn.addEventListener("click", function () { wrap.classList.add("hidden"); });
        return wrap;
      }

      function renderOfflineDiagnosticsRows(rows) {
        var listEl = document.getElementById("pos-offline-diag-list");
        var subEl = document.getElementById("pos-offline-diag-sub");
        if (!listEl) return;
        var arr = Array.isArray(rows) ? rows : [];
        var tickMini =
          '<svg class="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/></svg>';
        var xMini =
          '<svg class="h-4 w-4 shrink-0 text-rose-600 dark:text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M6 18L18 6M6 6l12 12"/></svg>';
        var pendingMini =
          '<svg class="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-300" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3"/></svg>';
        if (subEl) {
          subEl.textContent =
            String(arr.length) +
            " pending · " +
            String(offlineSyncRecentLog.length) +
            " recent sync row(s)." +
            (navigator.onLine ? " Online." : " Offline.");
        }
        var recentHtml = "";
        if (offlineSyncRecentLog.length) {
          recentHtml =
            '<div class="mb-4">' +
            '<p class="mb-2 text-[11px] font-black uppercase tracking-wider text-[rgb(var(--rc-muted))]">Recent sync</p>' +
            '<div class="space-y-2">' +
            offlineSyncRecentLog
              .map(function (log) {
                var ic = log.ok ? tickMini : xMini;
                var det = escapeHtml(String(log.detail || "").trim() || (log.ok ? "OK" : "Failed"));
                var when = escapeHtml(fmtDiagDate(log.at));
                var rc = escapeHtml(String(log.receipt || ""));
                return (
                  '<div class="flex gap-2 rounded-xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))]/30 px-3 py-2">' +
                  '<span class="mt-0.5 shrink-0" title="' +
                  (log.ok ? "Synced" : "Failed") +
                  '">' +
                  ic +
                  "</span>" +
                  '<div class="min-w-0 flex-1">' +
                  '<p class="font-bold text-[rgb(var(--rc-page-fg))]">' +
                  rc +
                  "</p>" +
                  '<p class="mt-0.5 text-[rgb(var(--rc-muted))]">' +
                  det +
                  "</p>" +
                  '<p class="mt-0.5 text-[10px] text-[rgb(var(--rc-muted))]">' +
                  when +
                  "</p>" +
                  "</div></div>"
                );
              })
              .join("") +
            "</div></div>";
        }
        if (!arr.length && !offlineSyncRecentLog.length) {
          listEl.innerHTML =
            '<div class="rounded-xl border border-emerald-500/35 bg-emerald-500/10 px-3 py-2 font-semibold text-emerald-800 dark:text-emerald-200">No pending offline sales.</div>';
          return;
        }
        var pendingHtml = "";
        if (arr.length) {
          pendingHtml =
            '<p class="mb-2 text-[11px] font-black uppercase tracking-wider text-[rgb(var(--rc-muted))]">Pending upload</p>' +
            '<div class="space-y-2">' +
            arr
              .map(function (row, idx) {
                var dueAt = fmtDiagDate(row && row.next_retry_at);
                var queuedAt = fmtDiagDate(row && row.queued_at);
                var err = String((row && row.last_error) || "").trim() || "-";
                var attempts = parseInt((row && row.attempts) || 0, 10) || 0;
                var total = Number((row && row.total_amount) || 0).toFixed(2);
                var pendingIcon = attempts > 0 && err !== "-" ? xMini : pendingMini;
                var pendingTitle = attempts > 0 && err !== "-" ? "Last upload failed" : "Waiting to sync";
                return (
                  '<div class="rounded-xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))]/40 p-3">' +
                  '<div class="flex flex-wrap items-start justify-between gap-2">' +
                  '<div class="flex min-w-0 flex-1 items-start gap-2">' +
                  '<span class="mt-0.5 shrink-0" title="' +
                  pendingTitle +
                  '">' +
                  pendingIcon +
                  "</span>" +
                  '<p class="min-w-0 font-bold text-[rgb(var(--rc-page-fg))]">#' +
                  String(idx + 1) +
                  " · " +
                  escapeHtml(String((row && row.local_receipt_number) || (row && row.client_txn_id) || "txn")) +
                  "</p></div>" +
                  '<p class="tabular-nums font-semibold text-[rgb(var(--rc-muted))]">' +
                  total +
                  "</p></div>" +
                  '<p class="mt-1 pl-6 text-[rgb(var(--rc-muted))]">Queued: ' +
                  queuedAt +
                  " · Attempts: " +
                  String(attempts) +
                  "</p>" +
                  '<p class="mt-1 pl-6 text-[rgb(var(--rc-muted))]">Next retry: ' +
                  dueAt +
                  "</p>" +
                  '<p class="mt-1 pl-6 text-[rgb(var(--rc-muted))]">Last error: ' +
                  escapeHtml(err) +
                  "</p>" +
                  "</div>"
                );
              })
              .join("") +
            "</div>";
        }
        listEl.innerHTML = recentHtml + pendingHtml;
      }

      function refreshOfflineDiagnostics() {
        return listOfflineSales().then(function (rows) {
          renderOfflineDiagnosticsRows(rows || []);
          updateOfflineSyncBadge();
        });
      }

      function ensureTodayOfflineQueueModal() {
        var existing = document.getElementById("pos-today-offline-modal");
        if (existing) return existing;
        var wrap = document.createElement("div");
        wrap.id = "pos-today-offline-modal";
        /* Same pattern as pos-compulsory-printer-modal: keep `flex` with `hidden` so removing hidden reliably shows a flex overlay (Tailwind hidden uses !important). */
        wrap.className =
          "fixed inset-0 z-[146] hidden flex items-end justify-center bg-black/55 p-0 sm:items-center sm:p-4";
        wrap.setAttribute("role", "dialog");
        wrap.setAttribute("aria-modal", "true");
        wrap.setAttribute("aria-labelledby", "pos-today-offline-title");
        wrap.innerHTML =
          '<div class="w-full max-w-lg overflow-hidden rounded-t-2xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface))] shadow-2xl sm:max-w-md sm:rounded-2xl">' +
          '<div class="flex items-center justify-between gap-3 border-b border-[rgb(var(--rc-border))]/70 px-4 py-3">' +
          '<div class="min-w-0">' +
          '<h3 id="pos-today-offline-title" class="truncate text-sm font-black uppercase tracking-[0.12em] text-[rgb(var(--rc-page-fg))]">Today\'s offline queue</h3>' +
          '<p id="pos-today-offline-sub" class="mt-1 text-xs text-[rgb(var(--rc-muted))]">Loading…</p>' +
          "</div>" +
          '<button type="button" id="pos-today-offline-close" class="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-[rgb(var(--rc-border))] text-[rgb(var(--rc-muted))] hover:bg-[rgb(var(--rc-surface-2))]" aria-label="Close">✕</button>' +
          "</div>" +
          '<div class="max-h-[min(70vh,28rem)] overflow-auto p-4">' +
          '<p class="mb-3 text-[11px] leading-relaxed text-[rgb(var(--rc-muted))]">Checked = saved to the database today. Unchecked = still waiting or last attempt failed.</p>' +
          '<div id="pos-today-offline-list" class="space-y-2 text-xs text-[rgb(var(--rc-page-fg))]"></div>' +
          "</div>" +
          '<div class="flex flex-wrap items-center justify-between gap-2 border-t border-[rgb(var(--rc-border))]/70 px-4 py-3">' +
          '<button type="button" id="pos-today-offline-open-diag" class="btn-rc btn-rc-ghost rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Full diagnostics</button>' +
          '<div class="flex flex-wrap gap-2">' +
          '<button type="button" id="pos-today-offline-refresh" class="btn-rc btn-rc-ghost rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Refresh</button>' +
          '<button type="button" id="pos-today-offline-sync" class="btn-rc btn-rc-primary rounded-lg px-3 py-2 text-[11px] font-bold uppercase tracking-wider">Sync now</button>' +
          "</div></div></div>";
        document.body.appendChild(wrap);
        wrap.addEventListener("click", function (e) {
          var inl = e.target && e.target.closest && e.target.closest("#pos-today-offline-sync-inline");
          if (inl) {
            e.preventDefault();
            if (offlineSyncBusy) return;
            setAuthStatus(
              navigator.onLine ? "Syncing offline queue…" : "Trying to sync (connection may be limited)…",
              "muted"
            );
            syncOfflineSalesQueue({ notify: true }).then(function () {
              return refreshTodayOfflineQueueModal();
            });
            return;
          }
          if (e.target === wrap) wrap.classList.add("hidden");
        });
        var closeBtn = document.getElementById("pos-today-offline-close");
        if (closeBtn) closeBtn.addEventListener("click", function () { wrap.classList.add("hidden"); });
        var diagJump = document.getElementById("pos-today-offline-open-diag");
        if (diagJump) {
          diagJump.addEventListener("click", function () {
            wrap.classList.add("hidden");
            if (offlineSyncDiagBtn) offlineSyncDiagBtn.click();
          });
        }
        var rf = document.getElementById("pos-today-offline-refresh");
        if (rf) rf.addEventListener("click", function () { refreshTodayOfflineQueueModal(); });
        var sy = document.getElementById("pos-today-offline-sync");
        if (sy) {
          sy.addEventListener("click", function () {
            if (offlineSyncBusy) return;
            setAuthStatus(
              navigator.onLine ? "Syncing offline queue…" : "Trying to sync (connection may be limited)…",
              "muted"
            );
            syncOfflineSalesQueue({ notify: true }).then(function () {
              return refreshTodayOfflineQueueModal();
            });
          });
        }
        return wrap;
      }

      function renderTodayOfflineQueue(rows) {
        var listEl = document.getElementById("pos-today-offline-list");
        var subEl = document.getElementById("pos-today-offline-sub");
        if (!listEl) return;
        rows = Array.isArray(rows) ? rows : [];
        var pendingToday = rows.filter(function (r) {
          return isIsoLocalToday(r && r.queued_at);
        });
        var pendingOlder = rows.length - pendingToday.length;
        var logsToday = offlineSyncRecentLog.filter(function (l) {
          return isIsoLocalToday(l && l.at);
        });

        if (subEl) {
          var d = new Date();
          subEl.textContent =
            d.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric", year: "numeric" }) +
            " · " +
            (navigator.onLine ? "Online" : "Offline") +
            " · " +
            String(pendingToday.length) +
            " pending today · " +
            String(logsToday.length) +
            " sync event(s) today";
        }

        function cbHtml(on, title) {
          return (
            '<span class="inline-flex shrink-0 items-center pt-0.5" title="' +
            escapeHtml(title) +
            '">' +
            '<input type="checkbox" disabled class="pointer-events-none h-4 w-4 rounded border-[rgb(var(--rc-border))] accent-emerald-600" ' +
            (on ? "checked " : "") +
            "/></span>"
          );
        }

        var parts = [];
        var pendingTotal = rows.length;
        if (pendingTotal > 0) {
          parts.push(
            '<div class="mb-3 rounded-xl border border-sky-500/40 bg-sky-500/10 px-3 py-3 dark:border-sky-500/35 dark:bg-sky-500/15">' +
              '<p class="text-[11px] font-black uppercase tracking-wider text-sky-950 dark:text-sky-100">Manual sync</p>' +
              '<p class="mt-1 text-xs leading-snug text-[rgb(var(--rc-muted))]">' +
              escapeHtml(String(pendingTotal)) +
              " checkout(s) are queued on this device and still need to be saved to the database. Tap below when you have a connection.</p>" +
              '<button type="button" id="pos-today-offline-sync-inline" class="btn-rc btn-rc-primary mt-2 w-full rounded-lg px-3 py-2.5 text-[11px] font-bold uppercase tracking-wider">Insert queued orders into database</button>' +
              "</div>"
          );
        }

        if (pendingOlder > 0) {
          parts.push(
            '<div class="mb-3 rounded-xl border border-amber-500/35 bg-amber-500/10 px-3 py-2 text-[11px] font-semibold leading-snug text-amber-900 dark:text-amber-100">' +
              escapeHtml(String(pendingOlder)) +
              " older checkout(s) are still in the queue (queued before today). Use Full diagnostics to see them." +
              "</div>"
          );
        }

        if (pendingToday.length) {
          parts.push(
            '<p class="mb-2 text-[11px] font-black uppercase tracking-wider text-[rgb(var(--rc-muted))]">Waiting to upload (today)</p>' +
              '<div class="mb-4 space-y-2">' +
              pendingToday
                .map(function (row) {
                  var ref = escapeHtml(String((row && row.local_receipt_number) || (row && row.client_txn_id) || "—"));
                  var total = Number((row && row.total_amount) || 0).toFixed(2);
                  var kl = offlineKindLabel(row && row.pos_offline_record_kind, row);
                  return (
                    '<div class="flex gap-3 rounded-xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))]/40 px-3 py-2">' +
                    cbHtml(false, "Not saved to the database yet") +
                    '<div class="min-w-0 flex-1">' +
                    '<p class="font-bold text-[rgb(var(--rc-page-fg))]">' +
                    ref +
                    ' · <span class="font-semibold text-[rgb(var(--rc-muted))]">' +
                    escapeHtml(kl) +
                    "</span></p>" +
                    '<p class="mt-0.5 text-[rgb(var(--rc-muted))]">Queued ' +
                    escapeHtml(fmtDiagDate(row && row.queued_at)) +
                    " · Total " +
                    total +
                    "</p>" +
                    "</div></div>"
                  );
                })
                .join("") +
              "</div>"
          );
        }

        if (logsToday.length) {
          parts.push(
            '<p class="mb-2 text-[11px] font-black uppercase tracking-wider text-[rgb(var(--rc-muted))]">Upload results (today)</p>' +
              '<div class="space-y-2">' +
              logsToday
                .map(function (log) {
                  var ref = escapeHtml(String(log.receipt || "").trim() || "—");
                  var ok = !!log.ok;
                  var kl = offlineKindLabel(log.kind, null);
                  return (
                    '<div class="flex gap-3 rounded-xl border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))]/30 px-3 py-2">' +
                    cbHtml(ok, ok ? "Saved to the database" : "Not saved — sync failed") +
                    '<div class="min-w-0 flex-1">' +
                    '<p class="font-bold text-[rgb(var(--rc-page-fg))]">' +
                    ref +
                    ' · <span class="font-semibold text-[rgb(var(--rc-muted))]">' +
                    escapeHtml(kl) +
                    "</span></p>" +
                    '<p class="mt-0.5 text-[rgb(var(--rc-muted))]">' +
                    escapeHtml(String(log.detail || "").trim() || (ok ? "OK" : "Failed")) +
                    "</p>" +
                    '<p class="mt-0.5 text-[10px] text-[rgb(var(--rc-muted))]">' +
                    escapeHtml(fmtDiagDate(log.at)) +
                    "</p>" +
                    "</div></div>"
                  );
                })
                .join("") +
              "</div>"
          );
        }

        if (!parts.length) {
          listEl.innerHTML =
            '<div class="rounded-xl border border-emerald-500/35 bg-emerald-500/10 px-3 py-3 text-sm font-semibold text-emerald-800 dark:text-emerald-200">No offline queue activity for today.</div>';
          return;
        }

        listEl.innerHTML = parts.join("");
      }

      function refreshTodayOfflineQueueModal() {
        return listOfflineSales().then(function (rows) {
          renderTodayOfflineQueue(rows || []);
          updateOfflineSyncBadge();
        });
      }

      function refreshTodayOfflineQueueModalIfOpen() {
        var m = document.getElementById("pos-today-offline-modal");
        if (m && !m.classList.contains("hidden")) refreshTodayOfflineQueueModal();
      }

      function formatCatalogAge(ms) {
        var mins = Math.max(1, Math.round(ms / 60000));
        if (mins < 60) return String(mins) + "m ago";
        var hrs = Math.round(mins / 60);
        if (hrs < 48) return String(hrs) + "h ago";
        return String(Math.round(hrs / 24)) + "d ago";
      }

      function setCatalogStaleBanner(text, tone) {
        if (!catalogStaleBannerEl) return;
        var msg = String(text || "").trim();
        if (!msg) {
          catalogStaleBannerEl.classList.add("hidden");
          catalogStaleBannerEl.textContent = "";
          return;
        }
        catalogStaleBannerEl.classList.remove("hidden");
        catalogStaleBannerEl.textContent = msg;
        catalogStaleBannerEl.classList.remove("border-amber-500/35", "bg-amber-500/10", "text-amber-900", "dark:text-amber-200");
        catalogStaleBannerEl.classList.remove("border-rose-500/35", "bg-rose-500/10", "text-rose-900", "dark:text-rose-200");
        if (tone === "error") {
          catalogStaleBannerEl.classList.add("border-rose-500/35", "bg-rose-500/10", "text-rose-900", "dark:text-rose-200");
        } else {
          catalogStaleBannerEl.classList.add("border-amber-500/35", "bg-amber-500/10", "text-amber-900", "dark:text-amber-200");
        }
      }

      function saveCatalogSnapshot(snapshot) {
        var doc = Object.assign({}, snapshot || {});
        doc.id = OFFLINE_CATALOG_DOC_ID;
        doc.saved_at = doc.saved_at || new Date().toISOString();
        if (!idbSupported()) {
          localStorage.setItem(OFFLINE_CATALOG_KEY, JSON.stringify(doc));
          return Promise.resolve(doc);
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_CATALOG_STORE, "readwrite");
            tx.oncomplete = function () { resolve(doc); };
            tx.onerror = function () { reject(tx.error || new Error("Could not save catalog snapshot")); };
            tx.objectStore(OFFLINE_DB_CATALOG_STORE).put(doc);
          }).finally(function () {
            try { db.close(); } catch (e) {}
          });
        }).catch(function () {
          localStorage.setItem(OFFLINE_CATALOG_KEY, JSON.stringify(doc));
          return doc;
        });
      }

      function loadCatalogSnapshot() {
        if (!idbSupported()) {
          try {
            var raw = localStorage.getItem(OFFLINE_CATALOG_KEY);
            return Promise.resolve(raw ? JSON.parse(raw) : null);
          } catch (e) {
            return Promise.resolve(null);
          }
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_CATALOG_STORE, "readonly");
            var req = tx.objectStore(OFFLINE_DB_CATALOG_STORE).get(OFFLINE_CATALOG_DOC_ID);
            req.onsuccess = function () { resolve(req.result || null); };
            req.onerror = function () { reject(req.error || new Error("Could not load catalog snapshot")); };
          }).finally(function () {
            try { db.close(); } catch (e) {}
          });
        }).catch(function () {
          try {
            var raw = localStorage.getItem(OFFLINE_CATALOG_KEY);
            return raw ? JSON.parse(raw) : null;
          } catch (e) {
            return null;
          }
        });
      }

      // ====================================================================
      // OFFLINE STOCK HELPERS — local catalog delta + offline stock-in queue
      // ====================================================================
      function effectiveStockFieldForCatalogRow(row, mode) {
        var m = String(mode || window.POS_INVENTORY_MODE || "shop").toLowerCase();
        if (m === "none") return null;
        if (m === "kitchen" || m === "both") {
          var en = Number(row && row.stock_update_enabled);
          var tracked = !isFinite(en) || en === 1;
          if (!tracked) return null;
          return "kitchen_portions";
        }
        return "shop_stock_qty";
      }

      /** Apply a signed delta map ({itemId: signedNumber}) to the cached catalog snapshot
       *  and refresh the DOM cards.
       *
       *  ``fieldOverride`` forces a specific catalog field to be updated instead of the
       *  "effective" field for the current inventory mode. Use cases:
       *    - sale decrement: omit (use effective field — kitchen_portions in kitchen/both, shop_stock_qty in shop)
       *    - stock-in increment: pass "shop_stock_qty" (server's shop_manual_stock_in always targets shop_stock_qty regardless of mode)
       *    - kitchen portion refill: pass "kitchen_portions" (server's add_shop_kitchen_portions always targets kitchen_portions) */
      function applyLocalCatalogDelta(delta, fieldOverride) {
        if (!delta || typeof delta !== "object") return Promise.resolve();
        var anything = false;
        for (var k0 in delta) {
          if (Object.prototype.hasOwnProperty.call(delta, k0) && Number(delta[k0]) !== 0) {
            anything = true; break;
          }
        }
        if (!anything) return Promise.resolve();
        var override = fieldOverride ? String(fieldOverride) : "";
        return loadCatalogSnapshot().then(function (snap) {
          if (!snap || !Array.isArray(snap.items) || !snap.items.length) return null;
          var mode = String(snap.inventory_mode || window.POS_INVENTORY_MODE || "shop").toLowerCase();
          var touched = [];
          var idMap = {};
          snap.items.forEach(function (it) {
            if (it && it.id != null) idMap[Number(it.id)] = it;
          });
          Object.keys(delta).forEach(function (idKey) {
            var id = Number(idKey);
            var row = idMap[id];
            if (!row) return;
            var field;
            if (override) {
              field = override;
              // For untracked kitchen items, suppress kitchen_portions writes so we don't
              // create misleading numbers for items the shop chose not to count.
              if (override === "kitchen_portions") {
                var enKp = Number(row.stock_update_enabled);
                if (isFinite(enKp) && enKp !== 1) return;
              }
            } else {
              field = effectiveStockFieldForCatalogRow(row, mode);
              if (!field) return;
            }
            var prev = parseFloat(row[field]);
            if (!isFinite(prev)) prev = 0;
            var next = prev + Number(delta[idKey]);
            if (next < 0) next = 0;
            row[field] = next;
            touched.push(row);
          });
          if (!touched.length) return null;
          snap.saved_at = new Date().toISOString();
          snap.source = "local-delta";
          return saveCatalogSnapshot(snap).then(function () { return touched; });
        }).then(function (touched) {
          if (touched && typeof window.posApplyCatalogRows === "function") {
            try { window.posApplyCatalogRows(touched); } catch (e) {}
          }
        }).catch(function () {});
      }

      function buildSaleDeltaFromLines(lines) {
        var d = {};
        if (!Array.isArray(lines)) return d;
        lines.forEach(function (l) {
          if (!l || l.id == null) return;
          var q = parseFloat(l.qty);
          if (!isFinite(q) || q <= 0) return;
          var id = Number(l.id);
          d[id] = (d[id] || 0) - q;
        });
        return d;
      }

      /** Inspect cart lines vs the catalog snapshot to find offline shortfalls.
       *  Returns [{ id, name, needed, available }] for any line that would oversell. */
      function assessOfflineStockShortfall(lines) {
        if (!Array.isArray(lines) || !lines.length) return Promise.resolve([]);
        // Combine demand per item (cart may have same item across multiple lines).
        var demand = {};
        var names = {};
        lines.forEach(function (l) {
          if (!l || l.id == null) return;
          var q = parseFloat(l.qty);
          if (!isFinite(q) || q <= 0) return;
          var id = Number(l.id);
          demand[id] = (demand[id] || 0) + q;
          if (l.name) names[id] = String(l.name);
        });
        var ids = Object.keys(demand);
        if (!ids.length) return Promise.resolve([]);
        return loadCatalogSnapshot().then(function (snap) {
          var issues = [];
          var idMap = {};
          if (snap && Array.isArray(snap.items)) {
            snap.items.forEach(function (it) {
              if (it && it.id != null) idMap[Number(it.id)] = it;
            });
          }
          var mode = String((snap && snap.inventory_mode) || window.POS_INVENTORY_MODE || "shop").toLowerCase();
          ids.forEach(function (idKey) {
            var id = Number(idKey);
            var needed = demand[id];
            var row = idMap[id];
            // Fall back to DOM card if no snapshot row found.
            var available = null;
            if (row) {
              var field = effectiveStockFieldForCatalogRow(row, mode);
              if (field == null) return; // untracked → no shortfall to warn about
              var v = parseFloat(row[field]);
              available = isFinite(v) ? v : 0;
            } else {
              var card = document.querySelector('.pos-item-card[data-item-id="' + id + '"]');
              if (!card) return;
              var dv = parseFloat(card.getAttribute("data-stock"));
              available = isFinite(dv) ? dv : 0;
              if (available > 100000000) return; // untracked sentinel from applyCatalogStockRows
            }
            if (needed > available) {
              issues.push({
                id: id,
                name: names[id] || (row && row.name) || "Item",
                needed: needed,
                available: available,
              });
            }
          });
          return issues;
        }).catch(function () { return []; });
      }

      // ---- offline_stockins store helpers ----
      function putOfflineStockIn(entry) {
        var doc = Object.assign({}, entry || {});
        doc.local_id = String(doc.local_id || ("sin-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8)));
        doc.queued_at = doc.queued_at || new Date().toISOString();
        doc.attempts = parseInt(doc.attempts || 0, 10) || 0;
        doc.last_error = String(doc.last_error || "");
        if (!idbSupported()) {
          try {
            var rawSi = localStorage.getItem(OFFLINE_STOCKIN_KEY);
            var listSi = rawSi ? JSON.parse(rawSi) : [];
            if (!Array.isArray(listSi)) listSi = [];
            listSi = listSi.filter(function (x) { return x && x.local_id !== doc.local_id; });
            listSi.push(doc);
            localStorage.setItem(OFFLINE_STOCKIN_KEY, JSON.stringify(listSi));
          } catch (e) {}
          return Promise.resolve(doc);
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STOCKIN_STORE, "readwrite");
            tx.oncomplete = function () { resolve(doc); };
            tx.onerror = function () { reject(tx.error || new Error("Could not store offline stock-in")); };
            tx.objectStore(OFFLINE_DB_STOCKIN_STORE).put(doc);
          }).finally(function () { try { db.close(); } catch (e) {} });
        });
      }

      function listOfflineStockIns() {
        if (!idbSupported()) {
          try {
            var rawList = localStorage.getItem(OFFLINE_STOCKIN_KEY);
            var arr = rawList ? JSON.parse(rawList) : [];
            return Promise.resolve(Array.isArray(arr) ? arr : []);
          } catch (e) { return Promise.resolve([]); }
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STOCKIN_STORE, "readonly");
            var req = tx.objectStore(OFFLINE_DB_STOCKIN_STORE).getAll();
            req.onsuccess = function () {
              var rows = Array.isArray(req.result) ? req.result : [];
              rows.sort(function (a, b) {
                return String((a && a.queued_at) || "").localeCompare(String((b && b.queued_at) || ""));
              });
              resolve(rows);
            };
            req.onerror = function () { reject(req.error || new Error("Could not read stock-in queue")); };
          }).finally(function () { try { db.close(); } catch (e) {} });
        }).catch(function () { return []; });
      }

      function deleteOfflineStockIn(localId) {
        if (!idbSupported()) {
          try {
            var rawDelList = localStorage.getItem(OFFLINE_STOCKIN_KEY);
            var listDel = rawDelList ? JSON.parse(rawDelList) : [];
            if (!Array.isArray(listDel)) listDel = [];
            listDel = listDel.filter(function (x) { return x && x.local_id !== String(localId); });
            localStorage.setItem(OFFLINE_STOCKIN_KEY, JSON.stringify(listDel));
          } catch (eDel) {}
          return Promise.resolve();
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_STOCKIN_STORE, "readwrite");
            tx.oncomplete = function () { resolve(); };
            tx.onerror = function () { reject(tx.error || new Error("Could not delete offline stock-in")); };
            tx.objectStore(OFFLINE_DB_STOCKIN_STORE).delete(String(localId));
          }).finally(function () { try { db.close(); } catch (e) {} });
        }).catch(function () {});
      }

      function getPendingOfflineStockInsCount() {
        return listOfflineStockIns().then(function (rows) { return rows.length; }).catch(function () { return 0; });
      }

      var offlineStockInSyncBusy = false;
      function syncOfflineStockInsQueue() {
        if (offlineStockInSyncBusy) return Promise.resolve({ synced: 0, failed: 0, remaining: 0 });
        offlineStockInSyncBusy = true;
        return listOfflineStockIns().then(function (rows) {
          if (!rows || !rows.length) return { synced: 0, failed: 0, remaining: 0 };
          var synced = 0;
          var failed = 0;
          var queue = Promise.resolve();
          rows.forEach(function (row) {
            queue = queue.then(function () {
              var fd = new FormData();
              fd.append("item_id", String(row.item_id || ""));
              fd.append("qty", String(row.qty || ""));
              fd.append("buying_price", String(row.buying_price || ""));
              fd.append("seller_phone", String(row.seller_phone || ""));
              fd.append("seller_name", String(row.seller_name || ""));
              fd.append("note", String(row.note || ""));
              fd.append("employee_code", String(row.employee_code || ""));
              return fetch(OFFLINE_STOCK_IN_API, {
                method: "POST",
                body: fd,
                credentials: "same-origin",
                headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
              }).then(function (r) {
                return r.json().catch(function () { return {}; });
              }).then(function (j) {
                if (!j || !j.ok) throw new Error((j && j.error) || "Server rejected queued stock-in.");
                return deleteOfflineStockIn(row.local_id).then(function () { synced++; });
              }).catch(function (err) {
                failed++;
                row.attempts = (parseInt(row.attempts || 0, 10) || 0) + 1;
                row.last_error = String((err && err.message) || err || "Unknown error");
                return putOfflineStockIn(row);
              });
            });
          });
          return queue.then(function () {
            return listOfflineStockIns().then(function (remaining) {
              return { synced: synced, failed: failed, remaining: remaining.length };
            });
          });
        }).finally(function () {
          offlineStockInSyncBusy = false;
        });
      }
      window.posSyncOfflineStockIns = syncOfflineStockInsQueue;
      window.posPendingStockInsCount = getPendingOfflineStockInsCount;

      // ---- offline_portion_refills store helpers (kitchen portion refills) ----
      function putOfflinePortionRefill(entry) {
        var doc = Object.assign({}, entry || {});
        doc.local_id = String(doc.local_id || ("ref-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8)));
        doc.queued_at = doc.queued_at || new Date().toISOString();
        doc.attempts = parseInt(doc.attempts || 0, 10) || 0;
        doc.last_error = String(doc.last_error || "");
        if (!idbSupported()) {
          try {
            var rawRf = localStorage.getItem(OFFLINE_REFILL_KEY);
            var listRf = rawRf ? JSON.parse(rawRf) : [];
            if (!Array.isArray(listRf)) listRf = [];
            listRf = listRf.filter(function (x) { return x && x.local_id !== doc.local_id; });
            listRf.push(doc);
            localStorage.setItem(OFFLINE_REFILL_KEY, JSON.stringify(listRf));
          } catch (eRfPut) {}
          return Promise.resolve(doc);
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_REFILL_STORE, "readwrite");
            tx.oncomplete = function () { resolve(doc); };
            tx.onerror = function () { reject(tx.error || new Error("Could not store offline portion refill")); };
            tx.objectStore(OFFLINE_DB_REFILL_STORE).put(doc);
          }).finally(function () { try { db.close(); } catch (e) {} });
        });
      }

      function listOfflinePortionRefills() {
        if (!idbSupported()) {
          try {
            var rawList = localStorage.getItem(OFFLINE_REFILL_KEY);
            var arr = rawList ? JSON.parse(rawList) : [];
            return Promise.resolve(Array.isArray(arr) ? arr : []);
          } catch (eRfList) { return Promise.resolve([]); }
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_REFILL_STORE, "readonly");
            var req = tx.objectStore(OFFLINE_DB_REFILL_STORE).getAll();
            req.onsuccess = function () {
              var rows = Array.isArray(req.result) ? req.result : [];
              rows.sort(function (a, b) {
                return String((a && a.queued_at) || "").localeCompare(String((b && b.queued_at) || ""));
              });
              resolve(rows);
            };
            req.onerror = function () { reject(req.error || new Error("Could not read portion refill queue")); };
          }).finally(function () { try { db.close(); } catch (e) {} });
        }).catch(function () { return []; });
      }

      function deleteOfflinePortionRefill(localId) {
        if (!idbSupported()) {
          try {
            var rawDelList = localStorage.getItem(OFFLINE_REFILL_KEY);
            var listDel = rawDelList ? JSON.parse(rawDelList) : [];
            if (!Array.isArray(listDel)) listDel = [];
            listDel = listDel.filter(function (x) { return x && x.local_id !== String(localId); });
            localStorage.setItem(OFFLINE_REFILL_KEY, JSON.stringify(listDel));
          } catch (eRfDel) {}
          return Promise.resolve();
        }
        return openOfflineDb().then(function (db) {
          return new Promise(function (resolve, reject) {
            var tx = db.transaction(OFFLINE_DB_REFILL_STORE, "readwrite");
            tx.oncomplete = function () { resolve(); };
            tx.onerror = function () { reject(tx.error || new Error("Could not delete offline portion refill")); };
            tx.objectStore(OFFLINE_DB_REFILL_STORE).delete(String(localId));
          }).finally(function () { try { db.close(); } catch (e) {} });
        }).catch(function () {});
      }

      function getPendingOfflinePortionRefillsCount() {
        return listOfflinePortionRefills().then(function (rows) { return rows.length; }).catch(function () { return 0; });
      }

      var offlinePortionRefillSyncBusy = false;
      function syncOfflinePortionRefillsQueue() {
        if (!OFFLINE_REFILL_PORTIONS_API) return Promise.resolve({ synced: 0, failed: 0, remaining: 0 });
        if (offlinePortionRefillSyncBusy) return Promise.resolve({ synced: 0, failed: 0, remaining: 0 });
        offlinePortionRefillSyncBusy = true;
        return listOfflinePortionRefills().then(function (rows) {
          if (!rows || !rows.length) return { synced: 0, failed: 0, remaining: 0 };
          var synced = 0;
          var failed = 0;
          var queue = Promise.resolve();
          rows.forEach(function (row) {
            queue = queue.then(function () {
              var fd = new FormData();
              fd.append("item_id", String(row.item_id || ""));
              fd.append("qty", String(row.qty || ""));
              fd.append("note", String(row.note || ""));
              fd.append("employee_code", String(row.employee_code || ""));
              return fetch(OFFLINE_REFILL_PORTIONS_API, {
                method: "POST",
                body: fd,
                credentials: "same-origin",
                headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
              }).then(function (r) {
                return r.json().catch(function () { return {}; });
              }).then(function (j) {
                if (!j || !j.ok) throw new Error((j && j.error) || "Server rejected queued portion refill.");
                return deleteOfflinePortionRefill(row.local_id).then(function () { synced++; });
              }).catch(function (err) {
                failed++;
                row.attempts = (parseInt(row.attempts || 0, 10) || 0) + 1;
                row.last_error = String((err && err.message) || err || "Unknown error");
                return putOfflinePortionRefill(row);
              });
            });
          });
          return queue.then(function () {
            return listOfflinePortionRefills().then(function (remaining) {
              return { synced: synced, failed: failed, remaining: remaining.length };
            });
          });
        }).finally(function () {
          offlinePortionRefillSyncBusy = false;
        });
      }
      window.posSyncOfflinePortionRefills = syncOfflinePortionRefillsQueue;
      window.posPendingPortionRefillsCount = getPendingOfflinePortionRefillsCount;

      /** Apply a +kitchen_portions delta locally so the UI reflects the refill immediately
       *  even when offline or when in shop mode (where effective field would differ).
       *  Always targets kitchen_portions regardless of inventory_mode. */
      function applyLocalKitchenPortionDelta(itemId, delta) {
        var id = Number(itemId);
        var d = Number(delta);
        if (!isFinite(id) || !isFinite(d) || d === 0) return Promise.resolve();
        return loadCatalogSnapshot().then(function (snap) {
          if (!snap || !Array.isArray(snap.items) || !snap.items.length) return null;
          var row = null;
          for (var i = 0; i < snap.items.length; i++) {
            if (snap.items[i] && Number(snap.items[i].id) === id) { row = snap.items[i]; break; }
          }
          if (!row) return null;
          var prev = parseInt(row.kitchen_portions, 10);
          if (!isFinite(prev)) prev = 0;
          var next = prev + d;
          if (next < 0) next = 0;
          row.kitchen_portions = next;
          snap.saved_at = new Date().toISOString();
          snap.source = "local-delta";
          return saveCatalogSnapshot(snap).then(function () { return [row]; });
        }).then(function (touched) {
          if (touched && typeof window.posApplyCatalogRows === "function") {
            try { window.posApplyCatalogRows(touched); } catch (e) {}
          }
        }).catch(function () {});
      }

      function randomTxnId() {
        if (window.crypto && typeof window.crypto.randomUUID === "function") {
          return window.crypto.randomUUID();
        }
        return "txn-" + Date.now() + "-" + Math.random().toString(36).slice(2, 10);
      }

      function enqueueOfflineSale(body) {
        var payload = Object.assign({}, body || {});
        payload.offline_queue_sync = true;
        payload.client_txn_id = String(payload.client_txn_id || randomTxnId());
        payload.local_receipt_number = payload.local_receipt_number || nextOfflineReceiptNumber();
        payload.queued_at = new Date().toISOString();
        payload.attempts = parseInt(payload.attempts || 0, 10) || 0;
        payload.last_error = String(payload.last_error || "");
        payload.next_retry_at = payload.next_retry_at || "";
        return putOfflineSale(payload).then(function (saved) {
          updateOfflineSyncBadge();
          // Phase 1 — locally decrement the cached catalog for real sales so the
          // cashier sees realistic stock numbers offline. Quotes do not deduct.
          var kind = String(payload.pos_offline_record_kind || "sale").toLowerCase();
          if (kind === "sale" || kind === "credit") {
            try { applyLocalCatalogDelta(buildSaleDeltaFromLines(payload.lines)); } catch (eDelta) {}
          }
          return saved;
        });
      }

      /** Strip queue metadata and branch payload shape for the correct POST endpoint. */
      function offlinePayloadForRecordSync(entry) {
        var e = entry || {};
        var kind = String(e.pos_offline_record_kind || "sale").toLowerCase();
        if (kind === "quote") {
          return {
            quote_basis: e.quote_basis,
            quote_channel: e.quote_channel || "walkin",
            total_amount: e.total_amount,
            item_count: e.item_count,
            customer_name: e.customer_name,
            customer_phone: e.customer_phone,
            lines: e.lines,
            employee: e.employee,
            client_txn_id: e.client_txn_id,
            offline_queue_sync: true,
            queued_at: e.queued_at,
          };
        }
        return {
          sale_type: e.sale_type,
          payment_method: e.payment_method,
          cash_amount: e.cash_amount,
          mpesa_amount: e.mpesa_amount,
          total_amount: e.total_amount,
          item_count: e.item_count,
          customer_name: e.customer_name,
          customer_phone: e.customer_phone,
          lines: e.lines,
          employee: e.employee,
          credit_due_date: e.credit_due_date || "",
          client_txn_id: e.client_txn_id,
          offline_queue_sync: true,
          queued_at: e.queued_at,
        };
      }

      function syncOfflineSalesQueue(opts) {
        opts = opts || {};
        var notify = !!opts.notify;
        /* Do not gate on navigator.onLine — it often stays false after reconnect or lies behind VPN/captive portals.
           We attempt POST and rely on fetch + per-entry next_retry_at backoff when there is no route to the server. */
        if (offlineSyncBusy) {
          return Promise.resolve({
            skipped: true,
            reason: "busy",
            synced: 0,
            failed: 0,
            errors: [],
            attempted: 0,
          });
        }
        offlineSyncBusy = true;
        var stats = { skipped: false, synced: 0, failed: 0, errors: [], attempted: 0 };
        var chain = listOfflineSales()
          .then(function (queue) {
            if (!queue.length) return;
            var nowMs = Date.now();
            queue = queue.filter(function (entry) {
              var nr = String((entry && entry.next_retry_at) || "");
              if (!nr) return true;
              var t = Date.parse(nr);
              return !isFinite(t) || t <= nowMs;
            });
            if (!queue.length) return;
            stats.attempted = queue.length;
            var cursor = Promise.resolve();
            queue.forEach(function (entry) {
              cursor = cursor.then(function () {
                var kind = String((entry && entry.pos_offline_record_kind) || "sale").toLowerCase();
                var api = kind === "quote" ? RECORD_QUOTE_API : RECORD_SALE_API;
                var syncBody = offlinePayloadForRecordSync(entry);
                return fetch(api, {
                  method: "POST",
                  credentials: "same-origin",
                  cache: "no-store",
                  headers: { "Content-Type": "application/json", Accept: "application/json" },
                  body: JSON.stringify(syncBody),
                }).then(function (r) {
                  return r.text().then(function (t) {
                    var j = {};
                    try {
                      j = t ? JSON.parse(t) : {};
                    } catch (parseErr) {
                      j = {};
                    }
                    if (!r.ok) {
                      var hint = (j && j.error) || t || "HTTP " + String(r.status);
                      throw new Error(String(hint).slice(0, 280));
                    }
                    return j;
                  });
                }).then(function (j) {
                  if (!j || !j.ok) throw new Error((j && j.error) || "Sync failed");
                  if (kind === "quote") {
                    stats.synced++;
                    appendOfflineSyncLogEntry({
                      receipt: (entry && entry.local_receipt_number) || "Quote",
                      ok: true,
                      detail: j.quote_id ? "Quote #" + String(j.quote_id) : "Saved to server",
                      kind: "quote",
                      client_txn_id: (entry && entry.client_txn_id) || "",
                    });
                  } else {
                    mergeReceiptSeqMaxFromReceiptString((j && j.receipt_number) || "");
                    stats.synced++;
                    appendOfflineSyncLogEntry({
                      receipt: (entry && entry.local_receipt_number) || (entry && entry.client_txn_id) || "",
                      ok: true,
                      detail: (j && j.receipt_number) ? "Saved as " + String(j.receipt_number) : "Saved to server",
                      kind: String((entry && entry.sale_type) || "").toLowerCase() === "credit" ? "credit" : "sale",
                      client_txn_id: (entry && entry.client_txn_id) || "",
                    });
                  }
                  return deleteOfflineSale(entry.client_txn_id);
                }).catch(function (err) {
                  stats.failed++;
                  var msg = String((err && err.message) || "Sync failed");
                  stats.errors.push({
                    receipt: (entry && entry.local_receipt_number) || (entry && entry.client_txn_id) || "",
                    message: msg,
                  });
                  appendOfflineSyncLogEntry({
                    receipt: (entry && entry.local_receipt_number) || (entry && entry.client_txn_id) || "",
                    ok: false,
                    detail: msg,
                    kind:
                      kind === "quote"
                        ? "quote"
                        : String((entry && entry.sale_type) || "").toLowerCase() === "credit"
                          ? "credit"
                          : "sale",
                    client_txn_id: (entry && entry.client_txn_id) || "",
                  });
                  var attempts = (parseInt(entry.attempts || 0, 10) || 0) + 1;
                  var capped = Math.min(attempts, 8);
                  var baseMs = Math.min(30 * 60 * 1000, Math.pow(2, capped) * 1000);
                  var jitterMs = Math.floor(Math.random() * 1200);
                  entry.attempts = attempts;
                  entry.last_error = msg;
                  entry.next_retry_at = new Date(Date.now() + baseMs + jitterMs).toISOString();
                  return putOfflineSale(entry);
                });
              });
            });
            return cursor;
          })
          .catch(function () {});
        return chain
          .finally(function () {
            offlineSyncBusy = false;
            updateOfflineSyncBadge();
            refreshTodayOfflineQueueModalIfOpen();
          })
          .then(function () {
            if (notify && !stats.skipped && (stats.synced > 0 || stats.failed > 0)) {
              showOfflineSyncResultToast(stats);
            }
            if ((stats.synced || 0) > 0 && typeof window.refreshPosCatalogStock === "function") {
              try {
                window.refreshPosCatalogStock();
              } catch (eRf3) {}
            }
            return stats;
          });
      }

      function fmt(n) {
        var x = parseFloat(n);
        if (isNaN(x)) x = 0;
        return x.toFixed(2);
      }

      // Quantity formatter — shows whole numbers as "1", but keeps up to 3
      // decimal places (trimming trailing zeros) for fractional sales such as
      // weighed items (kg, m, L). Used in the cart input field and anywhere
      // we display a qty back to the cashier.
      function fmtQty(n) {
        var x = parseFloat(n);
        if (isNaN(x)) x = 0;
        if (Math.abs(x - Math.round(x)) < 1e-9) return String(Math.round(x));
        var s = x.toFixed(3);
        return s.replace(/0+$/, "").replace(/\.$/, "");
      }

      function escapeHtml(s) {
        var d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
      }

      function cartLineHtml(line) {
        var qtyNum = parseFloat(line.qty);
        if (isNaN(qtyNum) || qtyNum <= 0) qtyNum = 1;
        var cap = typeof line.stock === "number" && line.stock > 0 && line.stock < 999999999 ? line.stock : 0;
        var maxAttr = cap ? ' max="' + cap + '"' : "";
        var unitPrice = parseFloat(line.price);
        if (isNaN(unitPrice)) unitPrice = 0;
        var lt = fmt(unitPrice * qtyNum);
        var allowEdit = window.POS_PRINTING && window.POS_PRINTING.allow_line_price_edit;
        var minEach = allowEdit && line.originalSellingPrice != null && !isNaN(parseFloat(line.originalSellingPrice))
          ? fmt(Math.max(0, parseFloat(line.originalSellingPrice)))
          : "0";
        var priceRow = allowEdit
          ? '<label class="mt-0 flex max-w-[11rem] flex-col gap-0 text-[10px] leading-tight text-[rgb(var(--rc-muted))]"><span>Each <span class="font-normal opacity-90">(min ' +
            minEach +
            ")</span></span><input type=\"number\" step=\"0.01\" min=\"" +
            minEach +
            "\" inputmode=\"decimal\" class=\"pos-line-price w-24 rounded-md border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface))] px-1.5 py-0.5 text-[11px] font-semibold tabular-nums text-[rgb(var(--rc-page-fg))]\" data-id=\"" +
            line.id +
            '" value="' +
            fmt(line.price) +
            '" /></label>'
          : '<p class="text-[10px] leading-tight text-[rgb(var(--rc-muted))]">' + fmt(line.price) + " each</p>";
        var amountMode =
          window.POS_PRINTING &&
          window.POS_PRINTING.pos_cart_amount_sets_qty &&
          unitPrice > 0;
        var qtyControlsHtml;
        var stockCapAttr =
          cap > 0 ? ' data-stock-cap="' + String(cap).replace(/"/g, "") + '"' : "";
        if (amountMode) {
          qtyControlsHtml =
            '<div class="pos-cart-qty-controls pos-cart-qty-controls--amount pos-cart-qty-controls--amount-flat flex flex-col items-end gap-0.5">' +
            '<span class="text-[9px] font-extrabold uppercase tracking-wide text-[rgb(var(--rc-muted))]">Amount</span>' +
            '<div class="flex items-center gap-0.5">' +
            '<button type="button" class="pos-qty-btn flex h-7 w-7 items-center justify-center rounded-md text-base leading-none text-[rgb(var(--rc-page-fg))] transition hover:bg-[rgb(var(--rc-surface-2))]" data-id="' +
            line.id +
            '" data-delta="-1" aria-label="Decrease quantity">−</button>' +
            '<input type="number" inputmode="decimal" step="0.01" min="0" class="pos-line-amount-input" data-id="' +
            line.id +
            '" data-unit-price="' +
            (isFinite(unitPrice) ? String(unitPrice) : "0") +
            '"' +
            stockCapAttr +
            ' value="' +
            lt +
            '" aria-label="Line amount" />' +
            '<button type="button" class="pos-qty-btn flex h-7 w-7 items-center justify-center rounded-md text-base leading-none text-[rgb(var(--rc-page-fg))] transition hover:bg-[rgb(var(--rc-surface-2))]" data-id="' +
            line.id +
            '" data-delta="1" aria-label="Increase quantity">+</button>' +
            "</div>" +
            '<p class="m-0 text-right text-[10px] font-semibold tabular-nums leading-tight text-[rgb(var(--rc-page-fg))]" data-pos-amount-qty-readout aria-label="Quantity for this sale">' +
            '<span class="text-[rgb(var(--rc-muted))]">Qty </span>' +
            '<span data-pos-amount-qty-value aria-live="polite">' +
            fmtQty(qtyNum) +
            "</span></p>" +
            "</div>";
        } else {
          qtyControlsHtml =
            '<div class="pos-cart-qty-controls flex items-center gap-0.5 rounded-lg p-0.5">' +
            '<button type="button" class="pos-qty-btn flex h-7 w-7 items-center justify-center rounded-md text-base leading-none text-[rgb(var(--rc-page-fg))] transition hover:bg-[rgb(var(--rc-surface-2))]" data-id="' +
            line.id +
            '" data-delta="-1" aria-label="Decrease">−</button>' +
            '<input type="number" inputmode="decimal" step="any" min="0"' +
            maxAttr +
            ' class="pos-qty-input" data-id="' +
            line.id +
            '" value="' +
            fmtQty(qtyNum) +
            '" aria-label="Quantity" />' +
            '<button type="button" class="pos-qty-btn flex h-7 w-7 items-center justify-center rounded-md text-base leading-none text-[rgb(var(--rc-page-fg))] transition hover:bg-[rgb(var(--rc-surface-2))]" data-id="' +
            line.id +
            '" data-delta="1" aria-label="Increase">+</button>' +
            "</div>";
        }
        return (
          '<div class="pos-cart-line-item group flex min-w-0 items-center gap-2">' +
          '<div class="min-w-0 flex-1">' +
          '<p class="truncate text-[13px] font-semibold leading-tight text-[rgb(var(--rc-page-fg))]">' +
          escapeHtml(line.name) +
          "</p>" +
          priceRow +
          "</div>" +
          '<div class="flex shrink-0 items-center gap-2">' +
          '<span class="pos-cart-line-total text-xs font-bold tabular-nums text-[rgb(var(--rc-page-fg))]">' +
          lt +
          "</span>" +
          qtyControlsHtml +
          "</div></div>"
        );
      }

      window.render = function () { return render(); };
      window.computePosTax = function (subtotal) { return computePosTax(subtotal); };
      window.showToast = function (msg) { return showToast(msg); };
      window.makeReceiptPayload = function (lines, mode, opts) { return makeReceiptPayload(lines, mode, opts); };
      window.runConfiguredPrinterAction = function (payload, printOpts) {
        return runConfiguredPrinterAction(payload, printOpts);
      };
      Object.defineProperty(window, "authorizedEmployee", {
        configurable: true,
        get: function () { return authorizedEmployee; },
      });
      function render() {
        var lines = load();
        var sub = 0;
        var html = "";
        lines.forEach(function (line) {
          var q = parseFloat(line.qty);
          if (isNaN(q) || q < 0) q = 0;
          sub += parseFloat(line.price) * q;
          html += cartLineHtml(line);
        });

        var empty =
          '<div class="flex flex-col items-center justify-center py-8 text-center">' +
          '<svg class="mb-2 h-7 w-7 text-[rgb(var(--rc-muted))] opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z" /></svg>' +
          '<p class="text-[11px] font-medium text-[rgb(var(--rc-muted))]">Tap items to add</p></div>';

        var inner = lines.length ? html : empty;

        var el = document.getElementById("pos-cart-lines");
        if (el) {
          el.innerHTML = inner;
          el.classList.toggle("pos-cart-lines-list", lines.length > 0);
        }

        var taxInfo = computePosTax(sub);
        var displayNum = taxInfo.includeTax ? taxInfo.grand : sub;
        var subS = fmt(displayNum);
        var st = document.getElementById("pos-cart-subtotal");
        if (st) st.textContent = subS;
        var lbl = document.getElementById("pos-cart-total-label");
        if (lbl) lbl.textContent = taxInfo.includeTax ? "Total" : "Subtotal";
        var taxNote = document.getElementById("pos-cart-tax-note");
        if (taxNote) {
          if (taxInfo.includeTax && taxInfo.taxAmt > 0.001) {
            taxNote.textContent = "Incl. tax " + taxInfo.taxPct + "%: " + fmt(taxInfo.taxAmt);
            taxNote.classList.remove("hidden");
          } else {
            taxNote.textContent = "";
            taxNote.classList.add("hidden");
          }
        }

        var lineCountEl = document.getElementById("pos-cart-line-count");
        if (lineCountEl) {
          if (lines.length) {
            var units = lines.reduce(function (a, l) {
              var q = parseFloat(l.qty);
              return a + (isNaN(q) ? 0 : q);
            }, 0);
            lineCountEl.textContent =
              lines.length + " line" + (lines.length !== 1 ? "s" : "") + " · " + fmtQty(units) + " unit" + (Math.abs(units - 1) < 1e-9 ? "" : "s");
          } else {
            lineCountEl.textContent = "";
          }
        }

        var count = lines.reduce(function (a, l) {
          var q = parseFloat(l.qty);
          return a + (isNaN(q) ? 0 : q);
        }, 0);
        document.querySelectorAll(".pos-cart-badge").forEach(function (badge) {
          badge.textContent = fmtQty(count);
          badge.classList.toggle("pos-cart-badge--empty", count <= 0);
          badge.classList.remove("opacity-50");
        });
        document.querySelectorAll(".pos-cart-open-trigger").forEach(function (cartBtn) {
          cartBtn.classList.toggle("pos-cart-btn--live", count > 0);
        });
        updateProceedState();
        try {
          if (typeof updateHoldReductionApprovalVisibility === "function") updateHoldReductionApprovalVisibility();
        } catch (eH) {}
      }

      /**
       * Chrome may synthesize a click on the element under the cursor when Bluetooth/USB pickers close.
       * Briefly ignore catalog card taps so POS lines are not changed unintentionally.
       */
      window.__posSuppressCatalogClicksUntil = 0;
      window.posSuppressCatalogGhostClicks = function (ms) {
        window.__posSuppressCatalogClicksUntil = Date.now() + Math.max(250, ms || 550);
      };
      function posCatalogClickSuppressed() {
        var t = window.__posSuppressCatalogClicksUntil;
        return typeof t === "number" && Date.now() < t;
      }

      function changeQty(id, delta) {
        var lines = load();
        var i = lines.findIndex(function (l) {
          return l.id === id;
        });
        if (i < 0) return;
        var curQty = parseFloat(lines[i].qty);
        if (isNaN(curQty)) curQty = 0;
        if (delta > 0) {
          var cap = lines[i].stock;
          if (typeof cap === "number" && cap > 0 && curQty >= cap) return;
        }
        var next = curQty + delta;
        // Snap fractional qty to 3dp so 0.5 + 1 = 1.5 stays clean.
        next = Math.round(next * 1000) / 1000;
        lines[i].qty = next;
        if (lines[i].qty <= 0) lines.splice(i, 1);
        save(lines);
        render();
      }

      function commitQtyInput(input) {
        if (!input || !input.classList || !input.classList.contains("pos-qty-input")) return;
        var id = parseInt(input.getAttribute("data-id"), 10);
        if (isNaN(id)) return;
        var raw = input.value;
        var lines = load();
        var i = lines.findIndex(function (l) {
          return l.id === id;
        });
        if (i < 0) return;
        var n = parseFloat(String(raw).trim());
        if (isNaN(n) || n <= 0) {
          // Treat empty/invalid/zero as "remove the line" — gives the cashier a
          // quick way to clear a fractional qty without an extra delete button.
          lines.splice(i, 1);
          save(lines);
          render();
          return;
        }
        var stockCap = lines[i].stock;
        if (typeof stockCap === "number" && stockCap > 0 && n > stockCap) n = stockCap;
        // Snap to 3 decimal places to avoid floating-point fuzz like 0.1+0.2=0.30000000000000004.
        n = Math.round(n * 1000) / 1000;
        lines[i].qty = n;
        save(lines);
        render();
      }

      function commitLineAmountInput(input) {
        if (!input || !input.classList || !input.classList.contains("pos-line-amount-input")) return;
        var id = parseInt(input.getAttribute("data-id"), 10);
        if (isNaN(id)) return;
        var raw = input.value;
        var lines = load();
        var i = lines.findIndex(function (l) {
          return l.id === id;
        });
        if (i < 0) return;
        var price = parseFloat(lines[i].price);
        if (isNaN(price) || price <= 0) {
          render();
          return;
        }
        var amount = parseFloat(String(raw).trim());
        if (isNaN(amount) || amount <= 0) {
          lines.splice(i, 1);
          save(lines);
          render();
          return;
        }
        var qty = amount / price;
        qty = Math.round(qty * 1000) / 1000;
        if (qty <= 0) {
          lines.splice(i, 1);
          save(lines);
          render();
          return;
        }
        var stockCap = lines[i].stock;
        if (typeof stockCap === "number" && stockCap > 0 && qty > stockCap) qty = stockCap;
        qty = Math.round(qty * 1000) / 1000;
        if (qty <= 0) {
          lines.splice(i, 1);
          save(lines);
          render();
          return;
        }
        lines[i].qty = qty;
        save(lines);
        render();
      }

      /** Live-update the quantity readout (and line total) while typing in Amount mode. */
      function updatePosLineAmountPreview(inp) {
        if (!inp || !inp.classList || !inp.classList.contains("pos-line-amount-input")) return;
        var lineItem = inp.closest(".pos-cart-line-item");
        var preview = lineItem && lineItem.querySelector("[data-pos-amount-qty-value]");
        var readout = lineItem && lineItem.querySelector("[data-pos-amount-qty-readout]");
        if (!preview) return;
        var unit = NaN;
        var priceInp = lineItem && lineItem.querySelector(".pos-line-price");
        if (priceInp) unit = parseFloat(priceInp.value);
        if (isNaN(unit) || unit <= 0) unit = parseFloat(inp.getAttribute("data-unit-price") || "0");
        var amount = parseFloat(String(inp.value || "").trim());
        if (isNaN(amount) || amount <= 0 || isNaN(unit) || unit <= 0) {
          preview.textContent = "—";
          if (readout) readout.classList.add("pos-cart-qty-readout--empty");
          var totalEl0 = lineItem && lineItem.querySelector(".pos-cart-line-total");
          if (totalEl0) {
            var lid0 = parseInt(inp.getAttribute("data-id"), 10);
            var lines0 = load();
            var j0 = lines0.findIndex(function (l) {
              return l.id === lid0;
            });
            if (j0 >= 0) {
              var pu0 = parseFloat(lines0[j0].price) || 0;
              var qq0 = parseFloat(lines0[j0].qty) || 0;
              totalEl0.textContent = fmt(pu0 * qq0);
            }
          }
          return;
        }
        var q = amount / unit;
        q = Math.round(q * 1000) / 1000;
        var capRaw = inp.getAttribute("data-stock-cap");
        if (capRaw != null && String(capRaw).trim() !== "") {
          var cap = parseFloat(capRaw);
          if (!isNaN(cap) && cap > 0 && q > cap) q = cap;
          q = Math.round(q * 1000) / 1000;
        }
        preview.textContent = fmtQty(q);
        if (readout) readout.classList.remove("pos-cart-qty-readout--empty");
        var totalEl = lineItem && lineItem.querySelector(".pos-cart-line-total");
        if (totalEl) totalEl.textContent = fmt(unit * q);
      }

      /** Selling-side cap for the active POS inventory mode (shelf vs portions — never mixed). */
      function posSellingCapFromItemCard(btn) {
        var mode = window.POS_INVENTORY_MODE || "shop";
        if (mode === "none") return 999999999;
        var cap = parseInt(btn.getAttribute("data-stock") || "0", 10);
        if (cap >= 999999998) return cap;
        if (mode === "shop") {
          var sq = parseInt(btn.getAttribute("data-shop-stock") || "", 10);
          return !isNaN(sq) ? sq : cap;
        }
        if (mode === "kitchen" || mode === "both") {
          var kp = parseInt(btn.getAttribute("data-kitchen-portions") || "", 10);
          return !isNaN(kp) ? kp : cap;
        }
        return cap;
      }

      function addFromCard(btn) {
        // Withhold POS: when the Held-orders editor is in "Pick from catalog" mode, item-card
        // clicks are diverted into the active held order's draft instead of the cart.
        if (typeof window.__posHoldPickHandler === "function") {
          var handled = false;
          try {
            handled = !!window.__posHoldPickHandler(btn);
          } catch (eHoldPick) {
            handled = false;
          }
          if (handled) return;
        }
        var id = parseInt(btn.getAttribute("data-item-id"), 10);
        var name = btn.getAttribute("data-name") || "";
        var price = parseFloat(btn.getAttribute("data-price") || "0");
        var origRaw = btn.getAttribute("data-original-selling-price");
        var originalSellingPrice = parseFloat(origRaw != null && origRaw !== "" ? origRaw : price);
        if (isNaN(originalSellingPrice)) originalSellingPrice = price;
        if (originalSellingPrice < 0) originalSellingPrice = 0;
        var stock = posSellingCapFromItemCard(btn);
        var lines = load();
        var i = lines.findIndex(function (l) {
          return l.id === id;
        });
        if (stock === 0 && i < 0) return;
        if (i >= 0) {
          var cap = lines[i].stock;
          var curQ = parseFloat(lines[i].qty);
          if (isNaN(curQ)) curQ = 0;
          if (typeof cap === "number" && cap > 0 && curQ >= cap) return;
          lines[i].qty = Math.round((curQ + 1) * 1000) / 1000;
        } else {
          lines.push({
            id: id,
            name: name,
            price: price,
            listPrice: price,
            originalSellingPrice: originalSellingPrice,
            qty: 1,
            stock: stock,
          });
        }
        save(lines);
        render();
        showToast(name);

        // After a live search pick: clear the input so the full catalog is visible again.
        var posSearchEl = document.getElementById("pos-search");
        var hadQuery = posSearchEl && String(posSearchEl.value || "").trim() !== "";
        if (hadQuery && posSearchEl) {
          posSearchEl.value = "";
          runSearch();
          requestAnimationFrame(function () {
            try {
              btn.scrollIntoView({ behavior: "smooth", block: "nearest" });
              posSearchEl.focus();
            } catch (e) {}
          });
        }
      }

      function showToast(name) {
        var t = document.getElementById("pos-toast");
        if (!t) return;
        t.textContent = "Added · " + (name.length > 42 ? name.slice(0, 40) + "…" : name);
        t.classList.remove("hidden");
        t.classList.add("pos-toast-show");
        clearTimeout(t._tid);
        t._tid = setTimeout(function () {
          t.classList.add("hidden");
          t.classList.remove("pos-toast-show");
        }, 2200);
      }

      function setAuthStatus(text, tone) {
        var el = document.getElementById("pos-auth-status");
        if (!el) return;
        el.textContent = text || "";
        el.classList.remove("pos-auth-status--ok", "pos-auth-status--error", "pos-auth-status--muted");
        el.classList.add(tone === "ok" ? "pos-auth-status--ok" : tone === "error" ? "pos-auth-status--error" : "pos-auth-status--muted");
      }

      function clearAuthorization() {
        authorizedEmployee = null;
        lastVerifiedAuthCode = "";
        var codeEl = document.getElementById("pos-auth-code");
        if (codeEl) codeEl.value = "";
        setAuthStatus("Enter employee code.", "muted");
      }

      function setCustomerLookupStatus(text, tone) {
        var el = document.getElementById("pos-customer-lookup-status");
        if (!el) return;
        el.textContent = text || "";
        el.classList.remove("pos-auth-status--ok", "pos-auth-status--error", "pos-auth-status--muted");
        el.classList.add(tone === "ok" ? "pos-auth-status--ok" : tone === "error" ? "pos-auth-status--error" : "pos-auth-status--muted");
      }

      function normalizePhone(v) {
        return (v || "").replace(/[^\d+]/g, "").trim();
      }

      function clearKnownCustomer() {
        knownCustomer = null;
        updateCustomerSectionState();
      }

      function maybeRegisterCustomerAfterAuth() {
        if (!authorizedEmployee || customerRegisterInFlight) return Promise.resolve();
        var nameEl = document.getElementById("pos-customer-name");
        var phoneEl = document.getElementById("pos-customer-phone");
        var name = ((nameEl && nameEl.value) || "").trim();
        var phone = normalizePhone((phoneEl && phoneEl.value) || "");
        if (!phone || knownCustomer) return Promise.resolve();
        if (lenDigits(phone) < 7 || name.length < 2) return Promise.resolve();

        customerRegisterInFlight = true;
        setCustomerLookupStatus("Saving customer…", "muted");
        return fetch(CUSTOMER_UPSERT_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ phone: phone, customer_name: name }),
        })
          .then(function (r) {
            return r.json().then(function (j) {
              if (!r.ok || !j.ok) throw new Error((j && j.error) || "Could not save customer");
              return j;
            });
          })
          .then(function (j) {
            knownCustomer = j.customer || null;
            if (nameEl && knownCustomer && knownCustomer.customer_name) nameEl.value = knownCustomer.customer_name;
            if (phoneEl && knownCustomer && knownCustomer.phone) phoneEl.value = knownCustomer.phone;
            setCustomerLookupStatus("Customer saved.", "ok");
            updateCustomerSectionState();
          })
          .catch(function (e) {
            setCustomerLookupStatus(e.message || String(e), "error");
          })
          .finally(function () {
            customerRegisterInFlight = false;
          });
      }

      function lenDigits(v) {
        return (v || "").replace(/\D/g, "").length;
      }

      function runCustomerLookupNow() {
        var phoneEl = document.getElementById("pos-customer-phone");
        var nameEl = document.getElementById("pos-customer-name");
        if (!phoneEl) return;
        var phone = normalizePhone(phoneEl.value || "");
        phoneEl.value = phone;

        if (lenDigits(phone) < 7) {
          clearKnownCustomer();
          setCustomerLookupStatus("Enter phone to lookup.", "muted");
          return;
        }
        if (customerLookupInFlight) return;
        customerLookupInFlight = true;
        setCustomerLookupStatus("Checking customer…", "muted");
        fetch(CUSTOMER_LOOKUP_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ phone: phone }),
        })
          .then(function (r) {
            return r.json().then(function (j) {
              if (!r.ok || !j.ok) throw new Error((j && j.error) || "Lookup failed");
              return j;
            });
          })
          .then(function (j) {
            var c = j.customer || null;
            knownCustomer = c;
            if (c) {
              if (nameEl) nameEl.value = c.customer_name || "";
              if (phoneEl) phoneEl.value = c.phone || phone;
              setCustomerLookupStatus("Registered customer found and auto-filled.", "ok");
            } else {
              setCustomerLookupStatus("Customer not found. Fill details to auto-register after employee verification.", "muted");
            }
            updateCustomerSectionState();
          })
          .catch(function (e) {
            clearKnownCustomer();
            setCustomerLookupStatus(e.message || String(e), "error");
          })
          .finally(function () {
            customerLookupInFlight = false;
          });
      }

      function scheduleCustomerLookup() {
        if (customerLookupTimer) clearTimeout(customerLookupTimer);
        customerLookupTimer = setTimeout(runCustomerLookupNow, 320);
      }

      function posPaymentFeatureFlags() {
        var p = window.POS_PRINTING || {};
        function on(flag) {
          return flag !== false && flag !== "false" && flag !== 0;
        }
        var cash = on(p.pos_payment_cash);
        var mpesa = on(p.pos_payment_mpesa);
        var both = on(p.pos_payment_both);
        if (!cash && !mpesa && !both) cash = true;
        return { cash: cash, mpesa: mpesa, both: both };
      }

      function posPaymentMethodChoiceCount(pm) {
        return (pm.cash ? 1 : 0) + (pm.mpesa ? 1 : 0) + (pm.both ? 1 : 0);
      }

      function refreshPaymentMethodUi() {
        var pm = posPaymentFeatureFlags();
        var nOpts = posPaymentMethodChoiceCount(pm);
        var paySec = document.getElementById("pos-sale-payment-section");
        var quoteEl = document.getElementById("pos-quote-only");
        var quoteOnly = !!(quoteEl && quoteEl.checked);
        var creditMode = saleType === "credit";
        var needPayUi = saleType === "sale" && !quoteOnly;
        var hidePaySection = creditMode || quoteOnly || (needPayUi && nOpts <= 1);
        if (paySec) paySec.classList.toggle("hidden", hidePaySection);

        var showPicker = needPayUi && nOpts > 1;
        var cashBtn = document.getElementById("pos-payment-method-cash");
        var mpesaBtn = document.getElementById("pos-payment-method-mpesa");
        var bothBtn = document.getElementById("pos-payment-method-both");
        if (cashBtn) cashBtn.classList.toggle("hidden", !showPicker || !pm.cash);
        if (mpesaBtn) mpesaBtn.classList.toggle("hidden", !showPicker || !pm.mpesa);
        if (bothBtn) bothBtn.classList.toggle("hidden", !showPicker || !pm.both);

        if (needPayUi && nOpts <= 1) {
          salePaymentMethod = pm.cash ? "cash" : pm.mpesa ? "mpesa" : "both";
        } else if (needPayUi && nOpts > 1) {
          var ok =
            (salePaymentMethod === "cash" && pm.cash) ||
            (salePaymentMethod === "mpesa" && pm.mpesa) ||
            (salePaymentMethod === "both" && pm.both);
          if (!ok) {
            salePaymentMethod = pm.cash ? "cash" : pm.mpesa ? "mpesa" : "both";
          }
        } else if (creditMode || quoteOnly) {
          salePaymentMethod = "";
        }

        updatePaymentMethodState();
      }

      function setSaleType(nextType) {
        saleType = nextType === "credit" ? "credit" : "sale";
        var saleBtn = document.getElementById("pos-sale-type-sale");
        var creditBtn = document.getElementById("pos-sale-type-credit");
        var quoteBtn = document.getElementById("pos-sale-type-quote");
        var quoteEl = document.getElementById("pos-quote-only");
        var isQuote = !!(quoteEl && quoteEl.checked);
        if (saleBtn) saleBtn.classList.toggle("is-active", saleType === "sale");
        if (creditBtn) creditBtn.classList.toggle("is-active", saleType === "credit");
        if (quoteBtn) quoteBtn.classList.toggle("is-active", isQuote);

        var n = document.getElementById("pos-customer-name");
        var p = document.getElementById("pos-customer-phone");
        var nl = document.getElementById("pos-customer-name-label");
        var pl = document.getElementById("pos-customer-phone-label");
        var rule = document.getElementById("pos-customer-rule");
        var must = saleType === "credit";
        if (n) n.required = must;
        if (p) p.required = must;
        if (nl) nl.textContent = "Customer name (" + (must ? "required" : "optional") + ")";
        if (pl) pl.textContent = "Customer phone (" + (must ? "required" : "optional") + ")";
        if (rule)
          rule.textContent = must ? "Credit: name + phone required." : isQuote ? "Quote: details optional unless required." : "Sale: details optional.";
        var dueSec = document.getElementById("pos-credit-due-section");
        var dueIn = document.getElementById("pos-credit-due-date");
        if (dueSec) dueSec.classList.toggle("hidden", !must);
        if (!must && dueIn) dueIn.value = "";
        refreshPaymentMethodUi();
        updatePosCompulsoryPrinterWorkspaceLock();
      }

      function printingCompulsoryOnSaleEnabled() {
        var p = window.POS_PRINTING || {};
        var f = p.print_compulsory_sale;
        if (f === false || f === 0 || f === "0") return false;
        if (f == null || f === "") return false;
        if (typeof f === "string") {
          var t = f.trim().toLowerCase();
          if (t === "false" || t === "0" || t === "no" || t === "off") return false;
          return t === "true" || t === "1" || t === "yes" || t === "on";
        }
        return !!f;
      }

      try {
        window.__posPrintingCompulsoryOnSaleEnabled = printingCompulsoryOnSaleEnabled;
      } catch (ePubComp) {}

      /** Retail sale (not quote/credit): compulsory printing needs printer ready — BT GATT; network TCP/agent; USB device present. */
      function requiresConfiguredPrinterForCurrentSale() {
        if (!printingCompulsoryOnSaleEnabled()) return false;
        var quoteEl = document.getElementById("pos-quote-only");
        var quoteOnly = !!(quoteEl && quoteEl.checked);
        return saleType === "sale" && !quoteOnly;
      }

      function posReceiptPrinterConfiguredForUi() {
        return !!window.POS_RECEIPT_PRINTER_CONFIGURED;
      }

      /** True while standard sale requires a ready printer but the UI says it is not ready yet. */
      function posCompulsoryPrinterGateBlocking() {
        return requiresConfiguredPrinterForCurrentSale() && !posReceiptPrinterConfiguredForUi();
      }

      function posPrinterCompulsoryBlockMessage() {
        return "Printing is compulsory on sale: printer must be connected — Bluetooth linked, network printer online (or print agent), or USB plugged in — then use your 6-digit code.";
      }

      function posCompulsoryPrinterModalShow() {
        var root = document.getElementById("pos-compulsory-printer-modal");
        var bd = document.getElementById("pos-compulsory-printer-modal-backdrop");
        if (!root || !bd) return;
        bd.classList.remove("hidden");
        root.classList.remove("hidden");
        bd.setAttribute("aria-hidden", "false");
        root.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
        var btn = document.getElementById("pos-compulsory-printer-modal-open-setup");
        if (btn) {
          setTimeout(function () {
            try {
              btn.focus();
            } catch (eF) {}
          }, 0);
        }
      }

      function posCompulsoryPrinterModalHide() {
        var root = document.getElementById("pos-compulsory-printer-modal");
        var bd = document.getElementById("pos-compulsory-printer-modal-backdrop");
        if (!root || !bd) return;
        bd.classList.add("hidden");
        root.classList.add("hidden");
        bd.setAttribute("aria-hidden", "true");
        root.setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
      }

      /** Lock item search / catalog and cart FAB until compulsory-sale printer readiness passes. */
      function updatePosCompulsoryPrinterWorkspaceLock() {
        var main = document.getElementById("pos-catalog-main");
        var fab = document.getElementById("pos-cart-toggle-fab");
        var locked = posCompulsoryPrinterGateBlocking();
        if (locked && typeof setCartOpen === "function") setCartOpen(false);
        if (main) {
          main.classList.toggle("pos-compulsory-printer-locked", locked);
          if (locked) main.setAttribute("inert", "");
          else main.removeAttribute("inert");
        }
        if (fab) {
          fab.disabled = !!locked;
          fab.setAttribute("aria-disabled", locked ? "true" : "false");
        }
        var dismissBtn = document.getElementById("pos-compulsory-printer-modal-dismiss");
        if (dismissBtn) dismissBtn.classList.toggle("hidden", !!locked);
        if (locked) {
          posCompulsoryPrinterModalShow();
        } else {
          posCompulsoryPrinterModalHide();
        }
      }

      try {
        window.__updatePosCompulsoryPrinterWorkspaceLock = updatePosCompulsoryPrinterWorkspaceLock;
        window.__posCompulsoryPrinterModalShow = posCompulsoryPrinterModalShow;
        window.__posCompulsoryPrinterModalHide = posCompulsoryPrinterModalHide;
        window.__posCompulsoryPrinterGateBlocking = posCompulsoryPrinterGateBlocking;
      } catch (eExpLock) {}

      (function initCompulsoryPrinterNoticeModal() {
        var root = document.getElementById("pos-compulsory-printer-modal");
        var bd = document.getElementById("pos-compulsory-printer-modal-backdrop");
        var dismiss = document.getElementById("pos-compulsory-printer-modal-dismiss");
        var openSetup = document.getElementById("pos-compulsory-printer-modal-open-setup");
        function hideIfAllowed() {
          if (posCompulsoryPrinterGateBlocking()) return;
          posCompulsoryPrinterModalHide();
        }
        if (bd) bd.addEventListener("click", hideIfAllowed);
        if (dismiss) dismiss.addEventListener("click", hideIfAllowed);
        if (openSetup)
          openSetup.addEventListener("click", function () {
            if (typeof window.__posOpenPrinterSetupFromHeader === "function") {
              window.__posOpenPrinterSetupFromHeader();
            } else {
              var po = document.getElementById("pos-printer-open");
              if (po) po.click();
            }
          });
        if (root)
          root.addEventListener("click", function (e) {
            if (e.target === root) hideIfAllowed();
          });
      })();

      function posCartFeatureFlags() {
        var p = window.POS_PRINTING || {};
        function on(flag) {
          return flag !== false && flag !== "false" && flag !== 0;
        }
        var out = {
          sale: on(p.pos_allow_cash_sale),
          credit: on(p.pos_allow_credit_sale),
          quotations: on(p.pos_allow_quotations),
          customerOnSale: on(p.pos_show_customer_details_sale),
        };
        if (!out.sale && !out.credit && !out.quotations) out.sale = true;
        return out;
      }

      function posTransactionalTypeCount(f) {
        return (f.sale ? 1 : 0) + (f.credit ? 1 : 0) + (f.quotations ? 1 : 0);
      }

      /** Withhold POS: show sale/credit/quote, payment, and customer only for direct checkout or when finalizing a linked held order — not on the empty "Hold" tab. */
      function posCartShowDirectSaleCheckoutFields() {
        var p = window.POS_PRINTING || {};
        if (p.pos_cart_mode !== "withhold") return true;
        if (window.__POS_CHECKOUT_PATH !== "hold") return true;
        var hid = window.__POS_HELD_ORDER_ID;
        if (hid != null && Number(hid) > 0) return true;
        return false;
      }
      window.posCartShowDirectSaleCheckoutFields = posCartShowDirectSaleCheckoutFields;

      function applyPosCartUiSettings() {
        var f = posCartFeatureFlags();
        var showDirectFields = posCartShowDirectSaleCheckoutFields();
        var saleCheckoutCard = document.getElementById("pos-customer-card");
        if (saleCheckoutCard) {
          saleCheckoutCard.classList.toggle("hidden", !showDirectFields);
        }
        var quoteEl = document.getElementById("pos-quote-only");
        var n = posTransactionalTypeCount(f);
        var txnBlock = document.getElementById("pos-transaction-type-block");
        if (txnBlock) txnBlock.classList.toggle("hidden", n <= 1);

        if (n <= 1) {
          if (f.credit) {
            saleType = "credit";
            if (quoteEl) quoteEl.checked = false;
          } else if (f.quotations) {
            saleType = "sale";
            if (quoteEl) quoteEl.checked = true;
          } else {
            saleType = "sale";
            if (quoteEl) quoteEl.checked = false;
          }
        } else {
          if (!f.quotations && quoteEl) quoteEl.checked = false;
          if (!f.credit && saleType === "credit") saleType = "sale";
          if (!f.sale && saleType === "sale" && !(quoteEl && quoteEl.checked)) {
            if (f.credit) saleType = "credit";
            else if (f.quotations && quoteEl) quoteEl.checked = true;
          }
        }

        var saleBtn = document.getElementById("pos-sale-type-sale");
        var creditBtn = document.getElementById("pos-sale-type-credit");
        var quoteBtn = document.getElementById("pos-sale-type-quote");
        var quoteRow = document.getElementById("pos-quotation-row");
        if (saleBtn) saleBtn.classList.toggle("hidden", !f.sale);
        if (creditBtn) creditBtn.classList.toggle("hidden", !f.credit);
        if (quoteBtn) quoteBtn.classList.toggle("hidden", !f.quotations);
        if (quoteRow) quoteRow.classList.toggle("hidden", !f.quotations);
        setSaleType(saleType === "credit" ? "credit" : "sale");
        var detailsCard = document.getElementById("pos-customer-details-card");
        var qOnly = quoteEl && quoteEl.checked;
        if (detailsCard) {
          var showCustomer =
            showDirectFields &&
            (saleType === "credit" ||
              qOnly ||
              (saleType === "sale" && !qOnly && f.customerOnSale));
          detailsCard.classList.toggle("hidden", !showCustomer);
        }
        updatePosCompulsoryPrinterWorkspaceLock();
        updateCustomerSectionState();
      }
      window.applyPosCartUiSettings = applyPosCartUiSettings;

      function round2(n) {
        return Math.round((parseFloat(n) || 0) * 100) / 100;
      }

      function cartSubtotalAmount() {
        return round2(
          load().reduce(function (sum, l) {
            var q = parseFloat((l && l.qty) || 0);
            if (isNaN(q)) q = 0;
            return sum + (parseFloat((l && l.price) || 0) || 0) * q;
          }, 0)
        );
      }

      function setPaymentMethod(next) {
        salePaymentMethod = next === "cash" || next === "mpesa" || next === "both" ? next : "";
        updatePaymentMethodState();
      }

      function updatePaymentMethodState() {
        var cashBtn = document.getElementById("pos-payment-method-cash");
        var mpesaBtn = document.getElementById("pos-payment-method-mpesa");
        var bothBtn = document.getElementById("pos-payment-method-both");
        var splitSec = document.getElementById("pos-payment-split-section");
        var splitHint = document.getElementById("pos-payment-split-hint");
        var cartDrawer = document.getElementById("pos-cart-drawer");
        var cashIn = document.getElementById("pos-payment-cash-amount");
        var mpesaIn = document.getElementById("pos-payment-mpesa-amount");
        var total = cartPosPayableTotal();
        if (cashBtn) cashBtn.classList.toggle("is-active", salePaymentMethod === "cash");
        if (mpesaBtn) mpesaBtn.classList.toggle("is-active", salePaymentMethod === "mpesa");
        if (bothBtn) bothBtn.classList.toggle("is-active", salePaymentMethod === "both");
        if (splitSec) splitSec.classList.toggle("hidden", salePaymentMethod !== "both");
        if (splitHint) splitHint.classList.toggle("hidden", salePaymentMethod !== "both");
        if (cartDrawer) cartDrawer.classList.toggle("pos-cart--split-payment", salePaymentMethod === "both");
        if (!cashIn || !mpesaIn) {
          updateCustomerSectionState();
          return;
        }
        if (salePaymentMethod === "cash") {
          cashIn.value = total.toFixed(2);
          mpesaIn.value = "";
        } else if (salePaymentMethod === "mpesa") {
          cashIn.value = "";
          mpesaIn.value = total.toFixed(2);
        } else if (salePaymentMethod === "both") {
          if (!String(cashIn.value || "").trim() && !String(mpesaIn.value || "").trim()) {
            cashIn.value = "";
            mpesaIn.value = "";
            return;
          }
          var c = round2(cashIn.value);
          var m = round2(mpesaIn.value);
          if (lastSplitEdited === "mpesa") {
            c = round2(total - m);
            if (c < 0) {
              c = 0;
              m = total;
            }
          } else {
            m = round2(total - c);
            if (m < 0) {
              m = 0;
              c = total;
            }
          }
          cashIn.value = c.toFixed(2);
          mpesaIn.value = m.toFixed(2);
        } else {
          cashIn.value = "";
          mpesaIn.value = "";
        }
        updateCustomerSectionState();
      }

      function syncSplitAmounts(changed) {
        var cashIn = document.getElementById("pos-payment-cash-amount");
        var mpesaIn = document.getElementById("pos-payment-mpesa-amount");
        if (!cashIn || !mpesaIn) return;
        var total = cartPosPayableTotal();
        var cashRaw = String(cashIn.value || "").trim();
        var mpesaRaw = String(mpesaIn.value || "").trim();
        if (!cashRaw && !mpesaRaw) {
          return;
        }
        var c = round2(cashRaw);
        var m = round2(mpesaRaw);
        if (changed === "mpesa") {
          m = Math.max(0, m);
          c = round2(total - m);
          if (c < 0) {
            c = 0;
            m = total;
          }
          lastSplitEdited = "mpesa";
          cashIn.value = c.toFixed(2);
        } else {
          c = Math.max(0, c);
          m = round2(total - c);
          if (m < 0) {
            m = 0;
            c = total;
          }
          lastSplitEdited = "cash";
          mpesaIn.value = m.toFixed(2);
        }
      }

      function normalizeSplitInputs() {
        var cashIn = document.getElementById("pos-payment-cash-amount");
        var mpesaIn = document.getElementById("pos-payment-mpesa-amount");
        if (!cashIn || !mpesaIn) return;
        if (salePaymentMethod !== "both") return;
        var cashRaw = String(cashIn.value || "").trim();
        var mpesaRaw = String(mpesaIn.value || "").trim();
        if (!cashRaw && !mpesaRaw) return;
        var c = round2(cashRaw);
        var m = round2(mpesaRaw);
        if (lastSplitEdited === "mpesa") {
          c = round2(cartPosPayableTotal() - m);
          if (c < 0) c = 0;
        } else {
          m = round2(cartPosPayableTotal() - c);
          if (m < 0) m = 0;
        }
        cashIn.value = c.toFixed(2);
        mpesaIn.value = m.toFixed(2);
      }

      function updateCustomerSectionState() {
        var card = document.getElementById("pos-customer-card");
        var nameEl = document.getElementById("pos-customer-name");
        var phoneEl = document.getElementById("pos-customer-phone");
        var authCodeEl = document.getElementById("pos-auth-code");
        var verifyBtn = document.getElementById("pos-auth-verify");
        var lockNoteEl = document.getElementById("pos-auth-lock-note");
        var lookupStatusEl = document.getElementById("pos-customer-lookup-status");
        var quoteEl = document.getElementById("pos-quote-only");
        var name = ((nameEl && nameEl.value) || "").trim();
        var phone = normalizePhone((phoneEl && phoneEl.value) || "");
        var hasBoth = !!name && lenDigits(phone) >= 7;
        var creditMode = saleType === "credit";
        var quoteOnly = !!(quoteEl && quoteEl.checked);
        var showDirectFields =
          typeof window.posCartShowDirectSaleCheckoutFields === "function"
            ? window.posCartShowDirectSaleCheckoutFields()
            : true;
        var lockCredit = showDirectFields && creditMode && !hasBoth;
        var lockForPayment = showDirectFields && saleType === "sale" && !quoteOnly && !salePaymentMethod;
        var lockPrinterMandatory =
          requiresConfiguredPrinterForCurrentSale() && !posReceiptPrinterConfiguredForUi();
        var lockAuth = lockCredit || lockForPayment || lockPrinterMandatory;
        var needProceedRefresh = false;
        if ((lockForPayment || lockPrinterMandatory) && authorizedEmployee) {
          authorizedEmployee = null;
          lastVerifiedAuthCode = "";
          needProceedRefresh = true;
        }

        if (card) {
          card.classList.toggle("pos-customer-card--required", creditMode && !hasBoth);
          card.classList.toggle("pos-customer-card--ready", hasBoth);
        }
        if (phoneEl) {
          phoneEl.classList.toggle("pos-input-required", creditMode && lenDigits(phone) < 7);
          phoneEl.classList.toggle("pos-input-ready", lenDigits(phone) >= 7);
        }
        if (nameEl) {
          nameEl.classList.toggle("pos-input-required", creditMode && !name);
          nameEl.classList.toggle("pos-input-ready", !!name);
        }
        if (authCodeEl) {
          authCodeEl.disabled = lockAuth;
          authCodeEl.setAttribute("aria-disabled", lockAuth ? "true" : "false");
          authCodeEl.classList.toggle("pos-auth-locked", lockAuth);
          if ((lockCredit || lockPrinterMandatory) && authCodeEl.value) authCodeEl.value = "";
        }
        if (verifyBtn) {
          verifyBtn.disabled = lockAuth || authVerifyInFlight;
          verifyBtn.setAttribute("aria-disabled", verifyBtn.disabled ? "true" : "false");
          verifyBtn.classList.toggle("pos-auth-locked", lockAuth);
        }
        if (lockNoteEl) {
          lockNoteEl.classList.toggle("hidden", !lockAuth);
          if (lockAuth) {
            lockNoteEl.textContent = lockCredit
              ? "Locked: add name + phone."
              : lockPrinterMandatory
                ? "Locked: Printer in the header must be set up and connected (Bluetooth: link active) before your employee code."
                : "Locked: select payment method first.";
          }
        }
        if (lookupStatusEl && lockCredit) {
          setCustomerLookupStatus("Credit needs name + phone first.", "error");
        }
        if (needProceedRefresh) updateProceedState();
      }

      function validateCustomerInput() {
        var n = (document.getElementById("pos-customer-name") || {}).value || "";
        var p = (document.getElementById("pos-customer-phone") || {}).value || "";
        n = n.trim();
        p = normalizePhone(p);
        if (!n || !p) {
          if (saleType === "credit") return { ok: false, error: "For credit, customer name and phone are required." };
          if (p && !n) return { ok: false, error: "Enter customer name to register this phone." };
          return { ok: true, name: n, phone: p };
        }
        return { ok: true, name: n, phone: p };
      }

      function updateProceedState() {
        var btn = document.getElementById("pos-cart-proceed");
        if (!btn) return;
        updatePaymentMethodState();
        var lines = load();
        var can = lines.length > 0 && !!authorizedEmployee;
        btn.disabled = !can;
      }
      window.updateProceedState = updateProceedState;

      function verifyAuthorizationCode() {
        var codeEl = document.getElementById("pos-auth-code");
        var verifyBtn = document.getElementById("pos-auth-verify");
        if (!codeEl) return;
        var showDirectFieldsVerify =
          typeof window.posCartShowDirectSaleCheckoutFields === "function"
            ? window.posCartShowDirectSaleCheckoutFields()
            : true;
        if (showDirectFieldsVerify && saleType === "credit") {
          var c = validateCustomerInput();
          if (!c.ok) {
            setCustomerLookupStatus("Complete customer details first for credit sale.", "error");
            return;
          }
        }
        var quoteElVerify = document.getElementById("pos-quote-only");
        var quoteOnlyVerify = !!(quoteElVerify && quoteElVerify.checked);
        if (showDirectFieldsVerify && saleType === "sale" && !quoteOnlyVerify && !salePaymentMethod) {
          setAuthStatus("Select payment method.", "error");
          return;
        }
        if (requiresConfiguredPrinterForCurrentSale() && !posReceiptPrinterConfiguredForUi()) {
          setAuthStatus(posPrinterCompulsoryBlockMessage(), "error");
          return;
        }
        var raw = (codeEl.value || "").replace(/\D/g, "").slice(0, 6);
        codeEl.value = raw;
        if (!/^\d{6}$/.test(raw)) {
          authorizedEmployee = null;
          lastVerifiedAuthCode = "";
          setAuthStatus("Enter a valid 6-digit employee code.", "error");
          updateProceedState();
          return;
        }
        if (authVerifyInFlight) return;
        if (authorizedEmployee && raw === lastVerifiedAuthCode) {
          updateProceedState();
          return;
        }
        authVerifyInFlight = true;
        if (verifyBtn) verifyBtn.disabled = true;
        var cachedPreview = employeeAuthCacheFindByCode(raw);
        setAuthStatus(
          cachedPreview
            ? "Checking code for " + (cachedPreview.full_name || "employee") + "…"
            : "Checking code…",
          "muted"
        );
        fetch(AUTH_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ employee_code: raw }),
        })
          .then(function (r) {
            return r.json().then(function (j) {
              if (!r.ok || !j.ok) throw new Error((j && j.error) || "Authorization failed");
              return j;
            });
          })
          .then(function (j) {
            authorizedEmployee = j.employee || null;
            employeeAuthCacheUpsert(authorizedEmployee);
            lastVerifiedAuthCode = raw;
            setAuthStatus("Authorized: " + ((authorizedEmployee && authorizedEmployee.full_name) || "Employee"), "ok");
            updateProceedState();
            maybeRegisterCustomerAfterAuth().then(function () {
              if (load().length > 0) {
                proceedSale();
              }
            });
          })
          .catch(function (e) {
            if (looksLikeOfflineNetworkError(e)) {
              var cached = employeeAuthCacheFindByCode(raw);
              if (cached) {
                authorizedEmployee = cached;
                lastVerifiedAuthCode = raw;
                setAuthStatus("Authorized offline: " + (cached.full_name || "Employee"), "ok");
                updateProceedState();
                maybeRegisterCustomerAfterAuth().then(function () {
                  if (load().length > 0) proceedSale();
                });
                return;
              }
              authorizedEmployee = null;
              lastVerifiedAuthCode = "";
              setAuthStatus("Offline: code not in local cache. Connect once to sync employees.", "error");
              updateProceedState();
              return;
            }
            authorizedEmployee = null;
            lastVerifiedAuthCode = "";
            var errText = e.message || String(e);
            setAuthStatus(errText, "error");
            if (errText.toLowerCase().indexOf("not registered") !== -1) {
              if (codeEl) {
                codeEl.value = "";
                codeEl.focus();
              }
            }
            updateProceedState();
          })
          .finally(function () {
            authVerifyInFlight = false;
            updateCustomerSectionState();
          });
      }

      function proceedSale() {
        // Withhold POS: when staff selected "Hold tab" and there is no held order linked yet,
        // authorizing the cart saves it as a new held order (and shows the order number) instead
        // of running the final-sale flow. Loading a held order from the modal sets Direct sale first,
        // then links the cart; Authorize & proceed still finalizes the sale (held_order_id when linked).
        if (
          window.__POS_CHECKOUT_PATH === "hold" &&
          !window.__POS_HELD_ORDER_ID &&
          typeof window.__posSaveCurrentCartToHold === "function"
        ) {
          window.__posSaveCurrentCartToHold();
          return;
        }
        var lines = load();
        var mode = saleType;
        var quoteEl = document.getElementById("pos-quote-only");
        var quoteOnly = !!(quoteEl && quoteEl.checked);
        var pf = posCartFeatureFlags();
        if (mode === "credit" && !pf.credit) {
          setAuthStatus("Credit sales are disabled for this shop.", "error");
          return;
        }
        if (quoteOnly && !pf.quotations) {
          setAuthStatus("Quotations are disabled for this shop.", "error");
          return;
        }
        if (!quoteOnly && mode === "sale" && !pf.sale) {
          setAuthStatus("Standard sales are disabled for this shop.", "error");
          return;
        }
        if (!lines.length) {
          setAuthStatus("Cart is empty. Add items first.", "error");
          return;
        }
        if (!quoteOnly && mode === "sale" && !salePaymentMethod) {
          setAuthStatus("Select payment method.", "error");
          return;
        }
        if (!quoteOnly && mode === "sale" && requiresConfiguredPrinterForCurrentSale() && !posReceiptPrinterConfiguredForUi()) {
          setAuthStatus(posPrinterCompulsoryBlockMessage(), "error");
          return;
        }
        if (!quoteOnly && mode === "sale" && salePaymentMethod === "both") {
          var splitCash = round2((document.getElementById("pos-payment-cash-amount") || {}).value || 0);
          var splitMpesa = round2((document.getElementById("pos-payment-mpesa-amount") || {}).value || 0);
          var splitTotal = round2(splitCash + splitMpesa);
          var saleTotal = cartPosPayableTotal();
          if (Math.abs(splitTotal - saleTotal) > 0.01) {
            setAuthStatus("Cash + M-Pesa must match total.", "error");
            return;
          }
        }
        var c = validateCustomerInput();
        if (!c.ok) {
          setAuthStatus(c.error, "error");
          return;
        }
        if (!authorizedEmployee) {
          setAuthStatus("Authorize with a valid employee code to proceed.", "error");
          return;
        }
        function doCheckoutAfterSave() {
          var lines = load();
          var quoteEl = document.getElementById("pos-quote-only");
          var quoteOnly = quoteEl && quoteEl.checked;
          if (quoteOnly) {
            return commitQuoteWithOfflineFallback(lines, mode).then(function (res) {
              var qid = res && !res.queued && res.quote_id;
              var quotePrintRef =
                res && res.queued && res.quote_print_ref ? res.quote_print_ref : qid != null ? qid : "";
              return runConfiguredPrinterAction(
                makeReceiptPayload(lines, mode, { isQuotation: true, quoteId: quotePrintRef })
              ).catch(function () {}).then(function () {
                return res;
              });
            });
          }
          return commitSaleWithOfflineFallback(lines, mode).then(function (res) {
            var persistedReceiptNo = (res && res.receipt_number) ? String(res.receipt_number) : "";
            /* Explicit variants = IT / shop “Receipts to print per sale” (customer ± company ± cashier). */
            var salePrintOpts = { receiptVariants: receiptVariantsForCheckout() };
            return runConfiguredPrinterAction(
              makeReceiptPayload(lines, mode, { persistedReceiptNo: persistedReceiptNo }),
              salePrintOpts
            ).catch(function () {}).then(function () { return res; });
          });
        }
        maybeRegisterCustomerAfterAuth()
          .then(function () {
            return doCheckoutAfterSave();
          })
          .then(function (checkoutResult) {
            try {
              if (typeof window.__posOnSaleFinalized === "function") {
                window.__posOnSaleFinalized(checkoutResult, { mode: mode });
              }
            } catch (eFinHook) {}
            save([]);
            render();
            setPaymentMethod("");
            applyPosCartUiSettings();
            clearAuthorization();
            clearKnownCustomer();
            setCustomerLookupStatus("Enter phone to lookup.", "muted");
            var nEl = document.getElementById("pos-customer-name");
            var pEl = document.getElementById("pos-customer-phone");
            if (nEl) nEl.value = "";
            if (pEl) pEl.value = "";
            var dueEl = document.getElementById("pos-credit-due-date");
            if (dueEl) dueEl.value = "";
            var qEl = document.getElementById("pos-quote-only");
            var wasQuote = qEl && qEl.checked;
            var queuedNow = !!(checkoutResult && checkoutResult.queued);
            if (queuedNow) {
              getPendingOfflineSalesCount().then(function (queueSize) {
                showToast(
                  (wasQuote ? "Quotation saved offline" : "Sale saved offline") +
                    ". Pending sync: " +
                    String(queueSize)
                );
                setAuthStatus(
                  wasQuote
                    ? "Quote queued offline. It will sync when internet returns."
                    : "Sale saved offline. It will sync automatically when internet returns.",
                  "muted"
                );
                updateOfflineSyncBadge();
              });
            } else {
              showToast(wasQuote ? "Quotation saved and printed" : (mode === "credit" ? "Credit" : "Sale") + " completed");
            }
            if (qEl) qEl.checked = false;
            var drawer = document.getElementById("pos-cart-drawer");
            if (drawer && drawer.classList.contains("is-open")) {
              var close = document.getElementById("pos-cart-close");
              if (close) close.click();
            }
          })
          .catch(function (e) {
            var msg = (e && e.message) ? e.message : "Could not save checkout to database. Try again.";
            setAuthStatus(msg, "error");
          });
      }

      function recordQuoteForLeads(body) {
        return fetch(RECORD_QUOTE_API, {
          method: "POST",
          credentials: "same-origin",
          cache: "no-store",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(body),
        })
          .then(function (r) {
            return r.text().then(function (t) {
              var j = {};
              try {
                j = t ? JSON.parse(t) : {};
              } catch (ePQ) {
                j = {};
              }
              if (!r.ok) throw new Error(String((j && j.error) || t || "HTTP " + String(r.status)).slice(0, 280));
              return j;
            });
          })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Could not save quotation.");
            return j;
          });
      }

      function commitQuoteWithOfflineFallback(lines, mode) {
        var body = buildQuoteRequestBody(lines, mode);
        body.client_txn_id = randomTxnId();
        return recordQuoteForLeads(body).then(function (res) {
          return { queued: false, quote_id: res && res.quote_id };
        }).catch(function (err) {
          var msg = String((err && err.message) || "");
          var offlineish =
            !navigator.onLine ||
            msg.toLowerCase().indexOf("failed to fetch") !== -1 ||
            msg.toLowerCase().indexOf("network") !== -1 ||
            msg.toLowerCase().indexOf("timeout") !== -1;
          if (!offlineish) throw err;
          return enqueueOfflineSale(
            Object.assign({}, body, {
              pos_offline_record_kind: "quote",
            })
          ).then(function (queued) {
            return {
              queued: true,
              quote_print_ref: String((queued && queued.local_receipt_number) || ""),
            };
          });
        });
      }

      function buildQuoteRequestBody(lines, mode) {
        var b = gatherReceiptBase(lines, mode);
        var rawLines = lines || [];
        var tx = computePosTax(b.subtotal);
        return {
          quote_basis: mode === "credit" ? "credit" : "sale",
          quote_channel: "walkin",
          total_amount: tx.grand,
          item_count: rawLines.reduce(function (sum, l) {
            var q = parseFloat((l && l.qty) || 0);
            return sum + (isNaN(q) ? 0 : q);
          }, 0),
          customer_name: b.customerName === "Walk-in customer" ? "" : b.customerName,
          customer_phone: b.customerPhone === "-" ? "" : b.customerPhone,
          lines: rawLines.map(function (l) {
            var qty = parseFloat((l && l.qty) || 0);
            if (isNaN(qty) || qty < 0) qty = 0;
            var unit = parseFloat((l && l.price) || 0) || 0;
            return {
              id: l && l.id,
              name: (l && l.name) || "Item",
              qty: qty,
              price: unit,
              total: unit * qty,
            };
          }),
          employee: {
            id: authorizedEmployee && authorizedEmployee.id,
            employee_code: authorizedEmployee && authorizedEmployee.employee_code,
            full_name: authorizedEmployee && authorizedEmployee.full_name,
          },
        };
      }

      function recordSaleForAnalytics(payload) {
        var lines = (payload && payload.lines) || [];
        return fetch(RECORD_SALE_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({
            sale_type: payload && payload.mode && String(payload.mode).toLowerCase() === "credit" ? "credit" : "sale",
            payment_method: (payload && payload.paymentMethod) || "cash",
            cash_amount: parseFloat(payload && payload.cashAmount != null ? payload.cashAmount : 0) || 0,
            mpesa_amount: parseFloat(payload && payload.mpesaAmount != null ? payload.mpesaAmount : 0) || 0,
            total_amount: computePosTax(parseFloat(payload && payload.subtotal ? payload.subtotal : 0) || 0).grand,
            item_count: lines.reduce(function (sum, l) {
              var q = parseFloat((l && l.qty) || 0);
              return sum + (isNaN(q) ? 0 : q);
            }, 0),
            customer_name: (payload && payload.customerName) || "",
            customer_phone: (payload && payload.customerPhone) || "",
            lines: lines.map(function (l) {
              var lq = parseFloat((l && l.qty) || 0);
              if (isNaN(lq) || lq < 0) lq = 0;
              return {
                id: l && l.id,
                name: (l && l.name) || "Item",
                qty: lq,
                price: parseFloat((l && l.price) || 0) || 0,
                total: parseFloat((l && l.total) || 0) || 0,
              };
            }),
            employee: {
              id: authorizedEmployee && authorizedEmployee.id,
              employee_code: authorizedEmployee && authorizedEmployee.employee_code,
              full_name: authorizedEmployee && authorizedEmployee.full_name,
            },
            client_txn_id: (payload && payload.clientTxnId) || "",
            credit_due_date:
              payload &&
              payload.mode === "credit" &&
              payload.creditDueIso &&
              /^\d{4}-\d{2}-\d{2}$/.test(payload.creditDueIso)
                ? payload.creditDueIso
                : "",
            held_order_id:
              window.__POS_HELD_ORDER_ID &&
              (payload == null || !payload.mode || payload.mode === "sale")
                ? Number(window.__POS_HELD_ORDER_ID)
                : null,
          }),
        })
          .then(function (r) { return r.json().catch(function () { return {}; }); })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Could not record sale.");
            return j;
          });
      }

      function commitSaleWithOfflineFallback(lines, mode) {
        var payload = buildAnalyticsPayload(lines, mode);
        payload.clientTxnId = randomTxnId();
        return recordSaleForAnalytics(payload).then(function (res) {
          mergeReceiptSeqMaxFromReceiptString((res && res.receipt_number) || "");
          try {
            if (typeof window.refreshPosCatalogStock === "function") {
              window.refreshPosCatalogStock();
            }
          } catch (eStockRf) {}
          return {
            queued: false,
            receipt_number: (res && res.receipt_number) ? String(res.receipt_number) : "",
          };
        }).catch(function (err) {
          var msg = String((err && err.message) || "");
          var offlineish =
            !navigator.onLine ||
            msg.toLowerCase().indexOf("failed to fetch") !== -1 ||
            msg.toLowerCase().indexOf("network") !== -1 ||
            msg.toLowerCase().indexOf("timeout") !== -1;
          if (!offlineish) throw err;
          // Phase 2 — warn the cashier before queueing a sale that would oversell
          // the local stock. Soft block: they can continue anyway.
          return assessOfflineStockShortfall(payload.lines || []).then(function (shortfalls) {
            if (shortfalls && shortfalls.length) {
              var lines = shortfalls.map(function (s) {
                return "• " + s.name + " — need " + s.needed + ", local stock " + s.available;
              }).join("\n");
              var prompt = "You are offline.\n\nThis sale will oversell local stock for:\n" + lines +
                "\n\nQueue the sale anyway? (Stock will be re-checked when the till syncs.)";
              if (!window.confirm(prompt)) {
                var cancelErr = new Error("Sale cancelled by cashier due to offline stock shortage.");
                cancelErr.code = "offline_sale_cancelled";
                throw cancelErr;
              }
            }
            return enqueueOfflineSale({
            pos_offline_record_kind: "sale",
            held_order_id:
              window.__POS_HELD_ORDER_ID &&
              (payload.mode === "sale" || !payload.mode)
                ? Number(window.__POS_HELD_ORDER_ID)
                : null,
            sale_type: payload.mode === "credit" ? "credit" : "sale",
            payment_method: payload.paymentMethod || "cash",
            cash_amount: parseFloat(payload.cashAmount != null ? payload.cashAmount : 0) || 0,
            mpesa_amount: parseFloat(payload.mpesaAmount != null ? payload.mpesaAmount : 0) || 0,
            total_amount: computePosTax(parseFloat(payload.subtotal || 0) || 0).grand,
            item_count: (payload.lines || []).reduce(function (sum, l) {
              var q = parseFloat((l && l.qty) || 0);
              return sum + (isNaN(q) ? 0 : q);
            }, 0),
            customer_name: payload.customerName || "",
            customer_phone: payload.customerPhone || "",
            lines: (payload.lines || []).map(function (l) {
              var lq = parseFloat((l && l.qty) || 0);
              if (isNaN(lq) || lq < 0) lq = 0;
              return {
                id: l && l.id,
                name: (l && l.name) || "Item",
                qty: lq,
                price: parseFloat((l && l.price) || 0) || 0,
                total: parseFloat((l && l.total) || 0) || 0,
              };
            }),
            employee: {
              id: authorizedEmployee && authorizedEmployee.id,
              employee_code: authorizedEmployee && authorizedEmployee.employee_code,
              full_name: authorizedEmployee && authorizedEmployee.full_name,
            },
            credit_due_date:
              payload.mode === "credit" &&
              payload.creditDueIso &&
              /^\d{4}-\d{2}-\d{2}$/.test(payload.creditDueIso)
                ? payload.creditDueIso
                : "",
            client_txn_id: payload.clientTxnId,
          }).then(function (queued) {
            return {
              queued: true,
              receipt_number: String(queued.local_receipt_number || ""),
            };
          });
          }); // close assessOfflineStockShortfall(...).then(shortfalls)
        });
      }

      function receiptSettings() {
        return window.POS_RECEIPT_SETTINGS || {};
      }

      function posIncludeTaxInTotals() {
        var pp = window.POS_PRINTING || {};
        return pp.pos_include_tax !== false && pp.pos_include_tax !== "false" && pp.pos_include_tax !== 0;
      }

      function computePosTax(subtotal) {
        var rs = receiptSettings();
        var taxPct = parseFloat(String(rs.tax_percent || "0").replace(",", ".")) || 0;
        /* POS tab (pos_include_tax) or Receipt format (include_tax) can enable tax; rate comes from receipt settings. */
        var includeTax =
          taxPct > 0.000001 && (!!rs.include_tax || posIncludeTaxInTotals());
        var taxAmt = includeTax ? Math.round(subtotal * (taxPct / 100) * 100) / 100 : 0;
        var grand = round2(subtotal + taxAmt);
        return { taxPct: taxPct, taxAmt: taxAmt, grand: grand, includeTax: includeTax };
      }

      function cartPosPayableTotal() {
        return computePosTax(cartSubtotalAmount()).grand;
      }

      /**
       * Canonical width from IT / shop receipt settings. Narrow rolls (often labeled 50mm/58mm) use fewer columns.
       */
      function normalizeReceiptWidthKey(raw) {
        var s = String(raw == null ? "" : raw)
          .trim()
          .toLowerCase();
        if (
          s === "58mm" ||
          s === "58" ||
          s === "50mm" ||
          s === "50" ||
          s === "narrow" ||
          s === "48mm" ||
          s === "2in" ||
          s === "2"
        ) {
          return "58mm";
        }
        return "80mm";
      }

      function receiptLineWidthChars() {
        return normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm" ? 24 : 32;
      }

      /** Physical roll width for @page / print iframe (matches receipt_width setting). */
      function receiptRollWidthMm() {
        return normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm" ? 58 : 80;
      }

      /** Local calendar date YYYY-MM-DD (aligns server receipt_scope daily keys). */
      function receiptSeqLocalYmd() {
        var d = new Date();
        var y = d.getFullYear();
        var m = String(d.getMonth() + 1).padStart(2, "0");
        var day = String(d.getDate()).padStart(2, "0");
        return y + "-" + m + "-" + day;
      }

      function receiptSeqLocalYm() {
        var d = new Date();
        var y = d.getFullYear();
        var m = String(d.getMonth() + 1).padStart(2, "0");
        return y + "-" + m;
      }

      function receiptSeqScopeKeySuffix() {
        var fmt = String(receiptSettings().receipt_number_format || "sequential").trim();
        if (fmt === "daily") return "d-" + receiptSeqLocalYmd();
        if (fmt === "per_month") return "m-" + receiptSeqLocalYm();
        return "seq";
      }

      function receiptSeqStorageKey() {
        return "richcom-pos-rseq-" + SHOP_ID + "-" + receiptSeqScopeKeySuffix();
      }

      function receiptSeqStartingNumber() {
        var startStr = String(receiptSettings().starting_number != null ? receiptSettings().starting_number : "1001").trim() || "1001";
        var startNum = parseInt(startStr, 10);
        if (isNaN(startNum) || startNum < 0) startNum = 1001;
        return startNum;
      }

      function canonicalReceiptPrefix() {
        var p = String(receiptSettings().receipt_number_prefix != null ? receiptSettings().receipt_number_prefix : "T").trim() || "T";
        return p;
      }

      /** Offline slips insert "-OF-" after the prefix (e.g. T-OF-1051 vs online T-1051). */
      function formatOfflineReceiptNumber(seqNum) {
        return canonicalReceiptPrefix() + "-OF-" + String(seqNum);
      }

      function parseTrailingReceiptSeq(receiptNo) {
        var s = String(receiptNo || "").trim();
        if (!s || s.charAt(0) === "#") return null;
        var parts = s.split("-");
        var tail = parts[parts.length - 1];
        if (!tail || !/^\d+$/.test(tail)) return null;
        var n = parseInt(tail, 10);
        return isFinite(n) ? n : null;
      }

      function mergeReceiptSeqMaxFromNumber(n) {
        var num = parseInt(n, 10);
        if (!isFinite(num) || num < 0) return;
        var key = receiptSeqStorageKey();
        var startNum = receiptSeqStartingNumber();
        var last = parseInt(localStorage.getItem(key), 10);
        if (isNaN(last)) last = startNum - 1;
        var merged = Math.max(last, num);
        localStorage.setItem(key, String(merged));
      }

      function mergeReceiptSeqMaxFromReceiptString(receiptNo) {
        var n = parseTrailingReceiptSeq(receiptNo);
        if (n != null) mergeReceiptSeqMaxFromNumber(n);
      }

      function advanceReceiptSeqAndReturnNext() {
        var key = receiptSeqStorageKey();
        var startNum = receiptSeqStartingNumber();
        var last = parseInt(localStorage.getItem(key), 10);
        if (isNaN(last)) last = startNum - 1;
        var next = last + 1;
        if (next < startNum) next = startNum;
        localStorage.setItem(key, String(next));
        return next;
      }

      /** Query receipts list using the same calendar scope as receipt numbering (approx.; capped row limit). */
      function receiptSeqSyncListQuery() {
        var fmt = String(receiptSettings().receipt_number_format || "sequential").trim();
        var y = new Date().getFullYear();
        if (fmt === "daily") return "?mode=single_day&single_day=" + encodeURIComponent(todayIso()) + "&limit=300";
        if (fmt === "per_month") return "?mode=month&month=" + encodeURIComponent(receiptSeqLocalYm()) + "&limit=300";
        return "?mode=year&year=" + encodeURIComponent(String(y)) + "&limit=300";
      }

      var lastReceiptSeqSyncAt = 0;
      function syncLocalReceiptSeqFromServerQuiet() {
        if (!navigator.onLine || !RECEIPTS_LIST_API) return Promise.resolve();
        return fetch(RECEIPTS_LIST_API + receiptSeqSyncListQuery(), {
          credentials: "same-origin",
          headers: { Accept: "application/json" },
        })
          .then(function (r) {
            return r.json().catch(function () {
              return {};
            });
          })
          .then(function (j) {
            if (!j || !j.ok || !Array.isArray(j.rows)) return;
            var mx = 0;
            var seen = false;
            j.rows.forEach(function (row) {
              var n = parseTrailingReceiptSeq(row && row.receipt_number);
              if (n != null) {
                seen = true;
                if (n > mx) mx = n;
              }
            });
            if (seen) mergeReceiptSeqMaxFromNumber(mx);
          })
          .catch(function () {})
          .finally(function () {
            lastReceiptSeqSyncAt = Date.now();
          });
      }

      /** Next display number for queued/offline sales (-OF- marker + shared numeric sequence). */
      function nextOfflineReceiptNumber() {
        var num = advanceReceiptSeqAndReturnNext();
        return formatOfflineReceiptNumber(num);
      }

      /** Fallback when no server/offline number is available yet (e.g. draft paths). */
      function nextReceiptNumber() {
        var num = advanceReceiptSeqAndReturnNext();
        return canonicalReceiptPrefix() + "-" + String(num);
      }

      function gatherReceiptBase(lines, mode, opts) {
        opts = opts || {};
        var now = new Date();
        var nameEl = document.getElementById("pos-customer-name");
        var phoneEl = document.getElementById("pos-customer-phone");
        var customerName = ((nameEl && nameEl.value) || "").trim();
        var customerPhone = normalizePhone((phoneEl && phoneEl.value) || "");
        var sub = 0;
        var discountTotal = 0;
        (lines || []).forEach(function (l) {
          var lq = parseFloat(l.qty);
          if (isNaN(lq)) lq = 0;
          sub += parseFloat(l.price || 0) * lq;
        });
        var site = window.POS_SITE || {};
        var company = (site.company_name || "").trim();
        var shopNm = (site.shop_name || "").trim();
        var branchTitle = shopNm || company || "Point of Sale";
        var companyLine = "";
        if (shopNm && company && company !== shopNm) {
          companyLine = company;
        }
        var mappedLines = (lines || []).map(function (l) {
          var qty = parseFloat(l.qty);
          if (isNaN(qty) || qty < 0) qty = 0;
          var unitNet = parseFloat(l.price || 0);
          var listUnit =
            l.listPrice != null && !isNaN(parseFloat(l.listPrice)) ? parseFloat(l.listPrice) : unitNet;
          var listTotal = listUnit * qty;
          var total = unitNet * qty;
          var lineDiscount = Math.max(0, listTotal - total);
          if (lineDiscount > 0.001) discountTotal += lineDiscount;
          var discounted = lineDiscount > 0.001;
          return {
            id: l.id,
            name: l.name || "Item",
            qty: qty,
            price: unitNet,
            listPrice: listUnit,
            total: total,
            listTotal: listTotal,
            lineDiscount: lineDiscount,
            discounted: discounted,
          };
        });
        discountTotal = Math.round(discountTotal * 100) / 100;
        var baseModeLabel = mode === "credit" ? "Credit" : "Sale";
        var modeLabel = opts.isQuote ? "Quotation · " + baseModeLabel : baseModeLabel;
        var creditDueIso = "";
        var creditDueDisplay = "";
        if (mode === "credit") {
          var ddEl = document.getElementById("pos-credit-due-date");
          var dv = ddEl && (ddEl.value || "").trim();
          if (/^\d{4}-\d{2}-\d{2}$/.test(dv)) {
            creditDueIso = dv;
            try {
              creditDueDisplay = new Date(dv + "T12:00:00").toLocaleDateString();
            } catch (e2) {
              creditDueDisplay = dv;
            }
          }
        }
        return {
          printedAt: now.toLocaleString(),
          customerName: customerName || "Walk-in customer",
          customerPhone: customerPhone || "-",
          subtotal: sub,
          discountTotal: discountTotal,
          hasDiscount: discountTotal > 0.001,
          modeLabel: modeLabel,
          shopDisplayName: branchTitle,
          companyLine: companyLine,
          lines: mappedLines,
          creditDueIso: creditDueIso,
          creditDueDisplay: creditDueDisplay,
        };
      }

      function buildAnalyticsPayload(lines, mode) {
        var b = gatherReceiptBase(lines, mode);
        var pay = computePosTax(b.subtotal).grand;
        var cashIn = document.getElementById("pos-payment-cash-amount");
        var mpesaIn = document.getElementById("pos-payment-mpesa-amount");
        var cashAmt = round2((cashIn && cashIn.value) || 0);
        var mpesaAmt = round2((mpesaIn && mpesaIn.value) || 0);
        if (mode === "credit") {
          cashAmt = 0;
          mpesaAmt = 0;
        } else if (salePaymentMethod === "cash") {
          cashAmt = round2(pay);
          mpesaAmt = 0;
        } else if (salePaymentMethod === "mpesa") {
          cashAmt = 0;
          mpesaAmt = round2(pay);
        } else {
          mpesaAmt = round2(pay - cashAmt);
        }
        return {
          mode: mode === "credit" ? "credit" : "sale",
          subtotal: b.subtotal,
          customerName: b.customerName,
          customerPhone: b.customerPhone,
          lines: b.lines,
          creditDueIso: b.creditDueIso || "",
          paymentMethod: mode === "credit" ? "credit" : salePaymentMethod,
          cashAmount: cashAmt,
          mpesaAmount: mpesaAmt,
          employeeName: (authorizedEmployee && authorizedEmployee.full_name) || "Unknown",
          employeeCode: (authorizedEmployee && authorizedEmployee.employee_code) || "",
        };
      }

      function makeReceiptPayload(lines, mode, opts) {
        opts = opts || {};
        var b = gatherReceiptBase(lines, mode, { isQuote: !!opts.isQuotation });
        var rs = receiptSettings();
        var tx = computePosTax(b.subtotal);
        var taxPct = tx.taxPct;
        var includeTax = tx.includeTax;
        var taxAmt = tx.taxAmt;
        var grand = tx.grand;
        var payLabels = { buy_goods: "Buy goods", paybill: "Pay bill", send_money: "Send money" };
        var pt = rs.payment_detail_type || "buy_goods";
        var paymentTypeLabel = payLabels[pt] || pt;
        var paymentDetailText = (rs.payment_detail_text || "").trim();
        if (mode !== "credit" && !opts.isQuotation) {
          var rcCashEl = document.getElementById("pos-payment-cash-amount");
          var rcMpesaEl = document.getElementById("pos-payment-mpesa-amount");
          var rcCash = round2((rcCashEl && rcCashEl.value) || 0);
          var rcMpesa = round2((rcMpesaEl && rcMpesaEl.value) || 0);
          if (salePaymentMethod === "cash") {
            rcCash = round2(grand);
            rcMpesa = 0;
            paymentTypeLabel = "Payment: Cash";
            paymentDetailText = "Cash: " + fmt(rcCash);
          } else if (salePaymentMethod === "mpesa") {
            rcCash = 0;
            rcMpesa = round2(grand);
            paymentTypeLabel = "Payment: M-Pesa";
            paymentDetailText = "M-Pesa: " + fmt(rcMpesa);
          } else if (salePaymentMethod === "both") {
            rcCash = Math.max(0, rcCash);
            rcMpesa = Math.max(0, round2(grand - rcCash));
            paymentTypeLabel = "Payment: Cash + M-Pesa";
            paymentDetailText = "Cash: " + fmt(rcCash) + " | M-Pesa: " + fmt(rcMpesa);
          }
        } else if (mode === "credit") {
          paymentTypeLabel = "Payment: Credit";
          paymentDetailText = paymentDetailText || "Pending payment";
        } else if (opts.isQuotation) {
          paymentTypeLabel = "Payment: Quotation";
        }
        var site = window.POS_SITE || {};
        return {
          receiptNo:
            opts.quoteId != null && opts.quoteId !== ""
              ? "Q-" + String(opts.quoteId)
              : (opts.persistedReceiptNo || "").trim()
                ? String(opts.persistedReceiptNo).trim()
              : nextReceiptNumber(),
          printedAt: b.printedAt,
          shopName: b.shopDisplayName,
          companyName: b.companyLine || "",
          shopCode: (site.shop_code || "").trim(),
          shopLocation: (site.shop_location || "").trim(),
          receiptLogoUrl: (site.receipt_logo_url || site.app_icon_url || "").trim(),
          mode: b.modeLabel,
          isQuotation: !!opts.isQuotation,
          isReprint: !!opts.isReprint,
          customerName: b.customerName,
          customerPhone: b.customerPhone,
          employeeName: (authorizedEmployee && authorizedEmployee.full_name) || "Unknown",
          employeeCode: (authorizedEmployee && authorizedEmployee.employee_code) || "",
          lines: b.lines,
          subtotal: b.subtotal,
          discountTotal: b.discountTotal || 0,
          hasDiscount: !!b.hasDiscount,
          taxAmount: taxAmt,
          taxPercent: taxPct,
          grandTotal: grand,
          includeTax: includeTax,
          paymentTypeLabel: paymentTypeLabel,
          paymentDetailText: paymentDetailText,
          receiptHeader: (rs.receipt_header || "").trim(),
          receiptFooter: (rs.receipt_footer || "").trim(),
          creditDueDate: (b.creditDueDisplay || "").trim(),
        };
      }

      function receiptEsc(s) {
        var d = document.createElement("div");
        d.textContent = String(s == null ? "" : s);
        return d.innerHTML;
      }

      /** Compact JSON for QR when mode is receipt_details (or website URL missing). */
      function buildReceiptQrPayloadDetailsJson(payload) {
        var obj = {
          r: String(payload.receiptNo || ""),
          t: fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
          d: String(payload.printedAt || ""),
          s: String(payload.shopName || ""),
          m: String(payload.mode || ""),
          c: String(payload.customerName || ""),
          p: String(payload.customerPhone || ""),
          i: (payload.lines || []).map(function (l) {
            return {
              n: String((l && l.name) || "Item"),
              q: (l && l.qty) != null ? l.qty : 0,
              x: fmt((l && l.total) || 0),
            };
          }),
        };
        if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
          obj.dc = fmt(payload.discountTotal);
        }
        var dueQr = (payload.creditDueDate || "").trim();
        if (dueQr && String(payload.mode || "").toLowerCase().indexOf("credit") !== -1) {
          obj.due = dueQr;
        }
        try {
          return JSON.stringify(obj);
        } catch (e) {
          return "";
        }
      }

      /** Payload string for QR (website URL or compact JSON for receipt_details). */
      function buildReceiptQrPayloadString(payload, S) {
        S = S || receiptSettings();
        var qm = String(S.receipt_qr_mode || "receipt_details").trim();
        if (qm === "website") {
          var u = (S.receipt_qr_website_url || "").trim();
          if (u) return u;
        }
        return buildReceiptQrPayloadDetailsJson(payload);
      }

      /** Shorter QR payload when full JSON would exceed ESC/POS QR store limits on some printers. */
      function buildReceiptQrPayloadStringEscPos(payload, S) {
        var s = buildReceiptQrPayloadString(payload, S);
        var enc = new TextEncoder();
        if (enc.encode(s).length <= 900) return s;
        try {
          return JSON.stringify({
            r: String(payload.receiptNo || ""),
            t: fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
            d: String(payload.printedAt || ""),
            s: String(payload.shopName || ""),
          });
        } catch (e) {
          return s.slice(0, 400);
        }
      }

      /**
       * ESC/POS QR Code (Model 2, GS ( k) — Epson-compatible / common thermal firmware).
       * Renders a scannable symbol; not plain text.
       */
      function buildEscPosQrCodeBytes(text) {
        var enc = new TextEncoder();
        var data = enc.encode(String(text || ""));
        var maxLen = 1000;
        if (data.length > maxLen) {
          data = enc.encode(String(text).slice(0, maxLen));
        }
        var parts = [];
        parts.push(0x1d, 0x28, 0x6b, 0x04, 0x00, 0x31, 0x41, 0x32, 0x00);
        /* GS ( k fn 167: module size 1–16; smaller = denser/smaller symbol on slip (was 8). */
        parts.push(0x1d, 0x28, 0x6b, 0x03, 0x00, 0x31, 0x43, 0x07);
        parts.push(0x1d, 0x28, 0x6b, 0x03, 0x00, 0x31, 0x45, 0x30);
        var n = data.length + 3;
        parts.push(0x1d, 0x28, 0x6b, n & 0xff, (n >> 8) & 0xff, 0x31, 0x50, 0x30);
        var i;
        for (i = 0; i < data.length; i++) parts.push(data[i]);
        parts.push(0x1d, 0x28, 0x6b, 0x03, 0x00, 0x31, 0x51, 0x30);
        return new Uint8Array(parts);
      }

      function receiptQrEnabled(S) {
        S = S || receiptSettings();
        var v = S.receipt_qr_enabled;
        return v === true || v === 1 || v === "1" || v === "true" || v === "True";
      }

      /** Wait for receipt & logo images so QR prints on first dialog (customer copy). */
      function waitForIframeImages(doc, callback, maxWaitMs) {
        maxWaitMs = maxWaitMs == null ? 5000 : maxWaitMs;
        var imgs = doc.querySelectorAll("img");
        var pending = 0;
        for (var i = 0; i < imgs.length; i++) {
          if (!imgs[i].complete) pending++;
        }
        if (pending === 0) {
          callback();
          return;
        }
        var done = false;
        function finish() {
          if (done) return;
          done = true;
          callback();
        }
        var to = setTimeout(finish, maxWaitMs);
        for (var j = 0; j < imgs.length; j++) {
          var img = imgs[j];
          if (img.complete) continue;
          (function (im) {
            function fin() {
              im.removeEventListener("load", fin);
              im.removeEventListener("error", fin);
              pending--;
              if (pending <= 0) {
                clearTimeout(to);
                finish();
              }
            }
            im.addEventListener("load", fin);
            im.addEventListener("error", fin);
          })(img);
        }
      }

      /** Monospace item rows: name (wrapped) with Qty / Amt right-aligned on the last name line. */
      function thermalItemHeaderLine(W) {
        var qtyW = 4;
        var amtW = 9;
        var right = " " + "Qty".padStart(qtyW, " ") + " " + "Amt".padStart(amtW, " ");
        var nameW = W - right.length;
        if (nameW < 6) nameW = 6;
        return ("Item" + " ".repeat(nameW)).slice(0, nameW) + right;
      }

      /** Label + value on one monospace line (receipt meta, totals) — prints reliably on thermal. */
      function thermalMetaLabelLine(label, value, W) {
        label = String(label || "").toUpperCase();
        value = String(value == null ? "" : value);
        var left = (label.length > 9 ? label.slice(0, 9) : label) + " ";
        var valMax = Math.max(8, W - left.length);
        var val = value.length > valMax ? value.slice(0, valMax - 1) + "\u2026" : value;
        return left + val.padStart(valMax, " ");
      }

      function thermalItemLines(l, W) {
        var qtyW = 4;
        var amtW = 9;
        var qty = fmtQty((l && l.qty) != null ? l.qty : 0);
        var amt = fmt((l && l.total) || 0);
        var right = " " + qty.padStart(qtyW, " ") + " " + amt.padStart(amtW, " ");
        var nameW = W - right.length;
        if (nameW < 6) nameW = 6;
        var rawName = String((l && l.name) || "Item");
        var out = [];
        var idx = 0;
        while (idx < rawName.length) {
          var take = Math.min(nameW, rawName.length - idx);
          var chunk = rawName.slice(idx, idx + take);
          idx += take;
          var isLast = idx >= rawName.length;
          if (isLast) {
            out.push((chunk + " ".repeat(nameW)).slice(0, nameW) + right);
          } else {
            out.push(chunk);
          }
        }
        if (!out.length) {
          out.push((" ".repeat(nameW) + right).slice(0, W));
        }
        if (l && l.discounted) {
          thermalLineDiscountSuffix(l, W).forEach(function (row) {
            out.push(row);
          });
        }
        return out;
      }

      /** Extra monospace line(s) under a discounted line item on thermal receipts. */
      function thermalLineDiscountSuffix(l, W) {
        if (!l || !l.discounted) return [];
        var save = fmt(l.lineDiscount || 0);
        var s = "  * DISCOUNTED  save " + save;
        W = W || receiptLineWidthChars();
        if (s.length <= W) return [s];
        s = "  * DISCOUNTED  " + save;
        if (s.length <= W) return [s];
        return [s.slice(0, W)];
      }

      function thermalPlainSep(w) {
        w = w || 32;
        var n = Math.min(Math.max(8, w), 48);
        return new Array(n + 1).join("-");
      }

      /** Stronger rule line for section breaks (thermal / monospace). */
      function thermalPlainSepHeavy(w) {
        w = w || 32;
        var n = Math.min(Math.max(8, w), 48);
        return new Array(n + 1).join("=");
      }

      function thermalPlainCenter(s, w) {
        s = String(s || "");
        if (s.length >= w) return s.slice(0, w);
        var pad = Math.floor((w - s.length) / 2);
        return new Array(pad + 1).join(" ") + s;
      }

      function receiptThermalFontCss() {
        var raw = receiptSettings().font_size || receiptSettings().receipt_font_size || "11pt";
        var s = String(raw == null ? "" : raw).trim();
        if (!s) s = "11pt";
        if (!/pt|px|mm|em|rem$/i.test(s)) s = s + "pt";
        if (normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm") {
          var n = parseFloat(s, 10);
          if (!isNaN(n) && s.indexOf("pt") !== -1 && n > 9) return "9pt";
        }
        return s;
      }

      /** Thermal browser print: structured layout (brand + meta + monospace items + HTML totals). */
      function buildThermalReceiptPlainHtml(payload, variant) {
        variant = variant || "customer";
        var isCompany = variant === "company";
        var isCashier = variant === "cashier";
        var showPrices = !isCompany;
        var showCustomerBlock = !isCompany;
        var S = receiptSettings();
        var site = window.POS_SITE || {};
        var rollWmm = receiptRollWidthMm();
        var W = receiptLineWidthChars();
        var sep = thermalPlainSep(W);
        var heavy = thermalPlainSepHeavy(W);
        var baseFont = receiptThermalFontCss();
        var lines = [];
        function L(t) {
          lines.push(String(t == null ? "" : t));
        }

        function buildThermalReceiptStyles(rollCss, rwmm, fontCss) {
          return (
            "@page{margin:0;size:" +
            rollCss +
            " auto}" +
            "*{box-sizing:border-box}" +
            "html{-webkit-print-color-adjust:exact;print-color-adjust:exact}" +
            "html,body{margin:0!important;padding:0!important;background:#fff;color:#111;" +
            "width:" +
            rollCss +
            ";min-width:" +
            rollCss +
            ";max-width:" +
            rollCss +
            "}" +
            ".receipt-thermal{padding:0 1.5mm 2mm;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:" +
            fontCss +
            ";line-height:1.22}" +
            ".receipt-thermal-accent{height:2mm;margin:0 0 1.5mm;background:linear-gradient(90deg,#1d4ed8,#2563eb,#1d4ed8)}" +
            ".receipt-logo-row{text-align:center;margin:0 auto 1mm;padding:0 1mm}" +
            ".receipt-logo-row img{display:block;margin:0 auto;max-height:10mm;max-width:68mm;height:auto;width:auto;object-fit:contain}" +
            ".receipt-thermal-brand{text-align:center;padding:0 0 0.5mm}" +
            ".receipt-thermal-badge{margin:0 0 1mm;padding:1px 6px;display:inline-block;border-radius:999px;font-size:7.5px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase}" +
            ".receipt-thermal-badge--company{background:#f3f4f6;color:#374151;border:1px solid #d1d5db}" +
            ".receipt-thermal-badge--cashier{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe}" +
            ".receipt-thermal-shopname{margin:0;font-size:clamp(11px,3.2mm,15px);font-weight:900;letter-spacing:-0.02em;line-height:1.12;color:#1e3a8a}" +
            ".receipt-thermal-company{margin:1px 0 0;font-size:9px;font-weight:600;color:#4b5563}" +
            ".receipt-thermal-rule{height:0;border:none;border-top:1px solid #d1d5db;margin:0 0 1.5mm}" +
            ".receipt-thermal-contact{margin:0 0 1mm;text-align:center}" +
            ".receipt-thermal-customhdr{margin:0 0 1mm;font-size:8.5px;line-height:1.25;color:#000;font-weight:600;white-space:pre-wrap}" +
            ".receipt-thermal-mut{margin:0;font-size:8px;line-height:1.2;color:#000;font-weight:600}" +
            ".receipt-thermal-placeholder{font-style:italic}" +
            ".receipt-thermal-quote{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #94a3b8;background:#f8fafc;text-align:center}" +
            ".receipt-thermal-quote__title{display:block;font-size:10px;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#0f172a}" +
            ".receipt-thermal-quote__sub{display:block;margin-top:2px;font-size:9.5px;color:#64748b}" +
            ".receipt-thermal-reprint{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #f59e0b;background:#fffbeb;text-align:center}" +
            ".receipt-thermal-reprint__title{display:block;font-size:10px;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#92400e}" +
            ".receipt-thermal-reprint__sub{display:block;margin-top:2px;font-size:9.5px;color:#b45309}" +
            "pre.receipt{margin:0;width:100%;max-width:100%;font-family:'Cascadia Mono','Courier New',Consolas,'Liberation Mono',monospace;font-size:9.5px;font-weight:700;line-height:1.35;white-space:pre-wrap;word-wrap:break-word;overflow:visible;color:#000}" +
            "pre.receipt--meta,pre.receipt--payment,pre.receipt--totals{margin:0;padding:0.8mm 0;border-top:1px solid #000;border-bottom:1px solid #000;color:#000;font-weight:700}" +
            "pre.receipt--items{margin:0;padding:0.8mm 0 0;border-top:1px solid #000}" +
            "pre.receipt--block{margin:1mm 0 0;padding:1mm 0 0;border-top:1px dashed #d1d5db}" +
            "pre.receipt--tail{margin:1mm 0 0;padding:1mm 0 0;border-top:1px solid #e5e7eb}" +
            ".receipt-thermal-footer{margin:0 0 1mm;padding:1mm 0 0;text-align:center;font-size:8px;line-height:1.25;color:#000;font-weight:600;border-top:1px dashed #000}" +
            ".receipt-qr{display:flex;flex-direction:column;align-items:center;width:100%;margin:0.5mm 0 0;padding:1mm 1mm 0;min-height:22mm}" +
            ".receipt-qr img{display:block;width:22mm;height:22mm;max-width:72%;object-fit:contain}" +
            ".receipt-qr-caption{margin:0.5mm 0 0;padding:0;width:100%;text-align:center;font-size:7.5px;line-height:1.25;color:#6b7280}" +
            ".receipt-browser-print-tip{margin:0 0 1.5mm;padding:1.5mm 2mm;border:1px solid #fcd34d;background:linear-gradient(180deg,#fffbeb,#fff7ed);color:#92400e;font-size:8px;line-height:1.3;border-radius:4px}" +
            "@media print{" +
            "@page{margin:0;size:" +
            rollCss +
            " auto}" +
            ".receipt-browser-print-tip{display:none!important}" +
            "html,body{width:" +
            rollCss +
            "!important;min-width:" +
            rollCss +
            "!important;max-width:" +
            rollCss +
            "!important}" +
            "pre.receipt,pre.receipt--meta,pre.receipt--items,pre.receipt--totals,pre.receipt--payment{font-size:" +
            (rwmm === 58 ? "8.5" : "9.5") +
            "pt!important;font-weight:700!important;color:#000!important}" +
            ".receipt-thermal-mut,.receipt-thermal-customhdr,.receipt-thermal-footer,.receipt-thermal-company{color:#000!important;font-weight:700!important}" +
            ".receipt-logo-row img{max-height:9mm}" +
            ".receipt-qr img{width:20mm!important;height:20mm!important}" +
            "}"
          );
        }

        function brandBlockHtml() {
          var badge = "";
          if (isCompany) {
            badge =
              '<p class="receipt-thermal-badge receipt-thermal-badge--company">Company copy · items only</p>';
          } else if (isCashier) {
            badge = '<p class="receipt-thermal-badge receipt-thermal-badge--cashier">Cashier copy</p>';
          }
          var shop = receiptEsc(String(payload.shopName || "Point of Sale"));
          var co = (payload.companyName || "").trim();
          var coHtml = co ? '<p class="receipt-thermal-company">' + receiptEsc(co) + "</p>" : "";
          return (
            '<header class="receipt-thermal-brand">' +
            badge +
            '<h1 class="receipt-thermal-shopname">' +
            shop +
            "</h1>" +
            coHtml +
            "</header>" +
            '<div class="receipt-thermal-rule" role="presentation"></div>'
          );
        }

        function metaPlainPreHtml() {
          var metaLines = [];
          metaLines.push(thermalMetaLabelLine("RECEIPT", payload.receiptNo, W));
          if (S.show_datetime || isCompany) {
            metaLines.push(thermalMetaLabelLine("DATE", payload.printedAt, W));
          }
          if (showCustomerBlock) {
            var modeStr = String(payload.mode || "");
            var isCredit = modeStr.toLowerCase().indexOf("credit") !== -1;
            if (modeStr && (isCredit || payload.isQuotation)) {
              metaLines.push(thermalMetaLabelLine("MODE", modeStr, W));
            }
            var dueT = (payload.creditDueDate || "").trim();
            if (dueT && isCredit) {
              metaLines.push(thermalMetaLabelLine("DUE", dueT, W));
            }
            var buyer =
              String(payload.customerName || "").trim() +
              (String(payload.customerPhone || "").trim() ? " · " + String(payload.customerPhone).trim() : "");
            metaLines.push(thermalMetaLabelLine("BUYER", buyer || "—", W));
          }
          if (isCompany) {
            metaLines.push(
              thermalMetaLabelLine("STAFF", String(payload.employeeName || "").trim() || "—", W)
            );
          } else if (S.show_server) {
            metaLines.push(thermalMetaLabelLine("STAFF", payload.employeeName, W));
          }
          if (!metaLines.length) return "";
          return (
            '<pre class="receipt receipt--meta">' + receiptEsc(metaLines.join("\n")) + "</pre>"
          );
        }

        function headerAndContactHtml() {
          var parts = [];
          var hdr = (payload.receiptHeader || "").trim();
          if (hdr && !isCompany) {
            var hx = hdr.length > 120 ? hdr.slice(0, 118) + "…" : hdr;
            parts.push(
              '<div class="receipt-thermal-customhdr">' + receiptEsc(hx).replace(/\n/g, " ") + "</div>"
            );
          }
          if (!isCompany && (S.show_address || S.show_contact)) {
            var bits = [];
            if (S.show_address && (payload.shopLocation || "").trim()) {
              bits.push(payload.shopLocation.trim().slice(0, W + 8));
            }
            if (S.show_contact) {
              if ((site.company_phone || "").trim()) bits.push(site.company_phone.trim());
              if ((site.company_email || "").trim()) bits.push(site.company_email.trim());
            }
            if (bits.length) {
              parts.push(
                '<p class="receipt-thermal-mut">' + receiptEsc(bits.join(" · ")) + "</p>"
              );
            }
          }
          return parts.length ? '<div class="receipt-thermal-contact">' + parts.join("") + "</div>" : "";
        }

        function quoteBannerHtml() {
          if (isCompany || !payload.isQuotation) return "";
          return (
            '<div class="receipt-thermal-quote" role="status">' +
            '<span class="receipt-thermal-quote__title">Quotation</span>' +
            '<span class="receipt-thermal-quote__sub">Not a finalized sale</span>' +
            "</div>"
          );
        }

        function reprintBannerHtml() {
          if (!payload.isReprint) return "";
          return (
            '<div class="receipt-thermal-reprint" role="status">' +
            '<span class="receipt-thermal-reprint__title">Reprinted receipt</span>' +
            '<span class="receipt-thermal-reprint__sub">This is a duplicate copy</span>' +
            "</div>"
          );
        }

        function totalsPlainPreHtml() {
          if (!showPrices || isCompany) return "";
          var totalLines = [];
          if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
            totalLines.push(
              thermalMetaLabelLine("DISCOUNT", "−" + fmt(payload.discountTotal), W)
            );
          }
          if (payload.includeTax) {
            totalLines.push(thermalMetaLabelLine("SUBTOTAL", fmt(payload.subtotal || 0), W));
            totalLines.push(
              thermalMetaLabelLine("TAX", fmt(payload.taxAmount || 0) + " (" + String(payload.taxPercent || 0) + "%)", W)
            );
            totalLines.push(
              thermalMetaLabelLine(
                "TOTAL",
                fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
                W
              )
            );
          } else {
            totalLines.push(thermalMetaLabelLine("TOTAL", fmt(payload.subtotal || 0), W));
          }
          return (
            '<pre class="receipt receipt--totals">' + receiptEsc(totalLines.join("\n")) + "</pre>"
          );
        }

        function paymentPlainPreHtml() {
          if (!showPrices) return "";
          var payType = String(payload.paymentTypeLabel || "").trim();
          var pd = (payload.paymentDetailText || "").trim();
          if (!payType && !pd) return "";
          var payLines = [];
          if (payType) payLines.push(payType);
          if (pd) {
            pd.split(/\r?\n/).forEach(function (ln) {
              if (String(ln).trim()) payLines.push(String(ln).trim());
            });
          }
          return (
            '<pre class="receipt receipt--payment">' + receiptEsc(payLines.join("\n")) + "</pre>"
          );
        }

        function footerHtmlBlock() {
          var foot = (payload.receiptFooter || "").trim();
          if (!foot || isCompany) return "";
          return (
            '<footer class="receipt-thermal-footer">' +
            receiptEsc(foot).replace(/\n/g, "<br/>") +
            "</footer>"
          );
        }

        var logoUrl = ((payload.receiptLogoUrl || site.receipt_logo_url || site.app_icon_url) || "").trim();
        var logoHtml = "";
        if (S.show_logo && logoUrl) {
          logoHtml =
            '<div class="receipt-logo-row"><img src="' +
            String(logoUrl).replace(/"/g, "") +
            '" alt="" /></div>';
        }

        var qrHtml = "";
        if (!isCompany && receiptQrEnabled(S)) {
          var qrData = buildReceiptQrPayloadString(payload, S);
          if (qrData) {
            var enc = encodeURIComponent(qrData);
            qrHtml =
              '<div class="receipt-qr"><img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&amp;data=' +
              enc +
              '" alt="" width="140" height="140" /></div>' +
              '<p class="receipt-qr-caption">Scan for receipt details</p>';
          }
        }

        var tailPlain =
          heavy +
          "\n" +
          thermalPlainCenter(isCompany ? "Company records" : "Thank you", W) +
          "\n" +
          thermalPlainCenter(isCompany ? "Richcom POS" : "Powered by Richcom POS", W);
        var escapedTail = receiptEsc(tailPlain);

        /* ── Company copy: monospace body (no prices), after brand. ── */
        if (isCompany) {
          L(thermalPlainCenter("Items sold", W));
          L(sep);
          (payload.lines || []).forEach(function (l) {
            var name = String((l && l.name) || "Item");
            if (l && l.discounted) name = name + " (DISCOUNTED)";
            L(name.slice(0, W - 4) + "  x" + fmtQty((l && l.qty) || 0));
          });
          var plainCompany = lines.join("\n");
          var rollCssC = rollWmm === 58 ? "58mm" : "80mm";
          var thermalStylesC = buildThermalReceiptStyles(rollCssC, rollWmm, baseFont);
          return (
            "<!doctype html><html><head><meta charset='utf-8'><title>Receipt</title>" +
            "<style>" +
            thermalStylesC +
            "</style></head><body>" +
            '<div class="receipt-thermal">' +
            '<div class="receipt-thermal-accent" aria-hidden="true"></div>' +
            "<div class=\"receipt-browser-print-tip\" role=\"status\"><strong>Print:</strong> Choose your physical printer under <strong>Destination</strong>, not Save as PDF.</div>" +
            logoHtml +
            brandBlockHtml() +
            metaPlainPreHtml() +
            "<pre class=\"receipt receipt--block\">" +
            receiptEsc(plainCompany) +
            "</pre>" +
            qrHtml +
            "<pre class=\"receipt receipt--tail\">" +
            escapedTail +
            "</pre>" +
            "</div></body></html>"
          );
        }

        /* ── Customer / cashier: hybrid layout. ── */
        lines.length = 0;
        L(thermalItemHeaderLine(W));
        L(sep);
        (payload.lines || []).forEach(function (l) {
          thermalItemLines(l, W).forEach(function (row) {
            L(row);
          });
        });

        var itemsPlain = lines.join("\n");
        var rollCss = rollWmm === 58 ? "58mm" : "80mm";
        var thermalStyles = buildThermalReceiptStyles(rollCss, rollWmm, baseFont);

        return (
          "<!doctype html><html><head><meta charset='utf-8'><title>Receipt</title>" +
          "<style>" +
          thermalStyles +
          "</style></head><body>" +
          '<div class="receipt-thermal">' +
          '<div class="receipt-thermal-accent" aria-hidden="true"></div>' +
          "<div class=\"receipt-browser-print-tip\" role=\"status\"><strong>Print:</strong> Choose your physical printer under <strong>Destination</strong>, not Save as PDF.</div>" +
          logoHtml +
          brandBlockHtml() +
          headerAndContactHtml() +
          quoteBannerHtml() +
          reprintBannerHtml() +
          metaPlainPreHtml() +
          '<pre class="receipt receipt--items">' +
          receiptEsc(itemsPlain) +
          "</pre>" +
          totalsPlainPreHtml() +
          paymentPlainPreHtml() +
          footerHtmlBlock() +
          qrHtml +
          "<pre class=\"receipt receipt--tail\">" +
          escapedTail +
          "</pre>" +
          "</div></body></html>"
        );
      }

      /** ESC/POS bytes for Bluetooth thermal printers (Web Bluetooth GATT write). */
      var KNOWN_BLE_PRINT_SERVICE_UUIDS = [
        "0000ffe0-0000-1000-8000-00805f9b34fb",
        "0000fff0-0000-1000-8000-00805f9b34fb",
        "0000ff00-0000-1000-8000-00805f9b34fb",
        "0000ae00-0000-1000-8000-00805f9b34fb",
        "0000ffb0-0000-1000-8000-00805f9b34fb",
        "49535343-fe7d-4ae5-8fa9-9fafd205e455",
        "6e400001-b5a3-f393-e0a9-e50e24dcca9e",
        "000018f0-0000-1000-8000-00805f9b34fb",
        /* Nordic/UART-style clones */
        "0000fdf5-0000-1000-8000-00805f9b34fb",
        "0000ffe5-0000-1000-8000-00805f9b34fb",
      ];

      /** Lower score = better candidate TX characteristic for ESC/POS raw bytes. */
      function scoreCharacteristicForEscPos(ch) {
        if (!ch || !ch.uuid) return 99;
        var u = String(ch.uuid).toLowerCase().replace(/-/g, "");
        if (u.indexOf("0000ffe1") !== -1 || u.slice(-8) === "0000ffe1") return 0;
        if (u.indexOf("6e400002") !== -1) return 1;
        if (u.indexOf("0000fff1") !== -1 || u.indexOf("0000fff2") !== -1) return 2;
        if (u.indexOf("0000ae01") !== -1) return 3;
        return 50;
      }

      /**
       * Chrome on Windows often throws DOMException "GATT Error Unknown" on the first connect after OS pairing.
       * Disconnect + delay + retry usually clears it for BLE thermal printers.
       */
      function bluetoothGattConnect(device) {
        if (!device || !device.gatt) return Promise.reject(new Error("Bluetooth GATT unavailable."));
        if (device.gatt.connected) return Promise.resolve();
        function attempt() {
          return Promise.resolve(device.gatt.connect());
        }
        return attempt().catch(function () {
          try {
            if (device.gatt.connected) device.gatt.disconnect();
          } catch (e0) {}
          return new Promise(function (resolve) {
            window.setTimeout(resolve, 450);
          }).then(function () {
            return attempt().catch(function () {
              try {
                if (device.gatt.connected) device.gatt.disconnect();
              } catch (e1) {}
              return new Promise(function (resolve) {
                window.setTimeout(resolve, 900);
              }).then(attempt);
            });
          });
        });
      }

      function bluetoothGattUserHint(err) {
        var name = (err && err.name) || "";
        var m = String((err && err.message) || err || "").toLowerCase();
        if (m.indexOf("unknown") !== -1 || name === "NetworkError") {
          return "Bluetooth GATT failed (often Windows + Chrome). Turn the printer off/on, stay within 1 m, tap Connect printer again, or use USB / browser Print.";
        }
        return "";
      }
      var BLE_PICKER_NAME_PREFIX_FILTERS = [
        "POS",
        "PRINTER",
        "THERMAL",
        "BT",
        "MTP",
        "RPP",
        "XP-",
        "TM-",
      ];

      /**
       * @param {Uint8Array} [qrRasterEscPos] — server-rendered QR bitmap (ESC/POS). When set, used instead of GS (k) native QR.
       */
      function buildEscPosReceiptBytes(payload, variant, qrRasterEscPos) {
        variant = variant || "customer";
        var isCompany = variant === "company";
        var isCashier = variant === "cashier";
        var showPrices = !isCompany;
        var S = receiptSettings();
        var site = window.POS_SITE || {};
        var W = receiptLineWidthChars();
        var enc = new TextEncoder();
        var ESC = 0x1b;
        var GS = 0x1d;
        var chunks = [];
        function pushBytes(arr) {
          chunks.push(new Uint8Array(arr));
        }
        function pushText(s) {
          chunks.push(enc.encode(String(s == null ? "" : s)));
        }
        function pushLn(s) {
          pushText(s);
          pushBytes([0x0a]);
        }
        function boldOn() {
          pushBytes([ESC, 0x45, 0x01]);
        }
        function boldOff() {
          pushBytes([ESC, 0x45, 0x00]);
        }
        function sep() {
          var n = Math.min(Math.max(8, W), 48);
          var line = new Array(n + 1).join("-");
          pushLn(line);
        }
        function charSizeNormal() {
          pushBytes([ESC, 0x21, 0x00]);
          pushBytes([GS, 0x21, 0x00]);
        }
        function charSizeTitle() {
          if (W <= 24) {
            pushBytes([ESC, 0x21, 0x10]);
          } else {
            pushBytes([ESC, 0x21, 0x30]);
          }
        }
        function charSizeSubtitle() {
          pushBytes([ESC, 0x21, 0x10]);
        }

        pushBytes([ESC, 0x40]);
        charSizeNormal();
        pushBytes([ESC, 0x61, 0x01]);
        charSizeTitle();
        boldOn();
        pushLn(String(payload.shopName || "Point of Sale"));
        boldOff();
        charSizeNormal();
        pushBytes([ESC, 0x61, 0x00]);

        if ((payload.companyName || "").trim()) {
          pushBytes([ESC, 0x61, 0x01]);
          charSizeSubtitle();
          pushLn(String(payload.companyName).slice(0, W + 8));
          charSizeNormal();
          pushBytes([ESC, 0x61, 0x00]);
        }
        if (!isCompany && S.show_address) {
          pushBytes([ESC, 0x61, 0x01]);
          if ((payload.shopLocation || "").trim()) {
            pushLn(String(payload.shopLocation).slice(0, W + 12));
          } else {
            pushLn("[Business address]");
          }
          pushBytes([ESC, 0x61, 0x00]);
        } else if (!isCompany && (payload.shopLocation || "").trim()) {
          pushLn(String(payload.shopLocation).slice(0, W + 8));
        }
        sep();

        if (isCompany || isCashier) {
          var bannerEsc = isCompany ? "COMPANY COPY (ITEMS ONLY)" : "CASHIER COPY";
          boldOn();
          pushLn(bannerEsc);
          boldOff();
          sep();
        }

        if (!isCompany) {
          var hdr = (payload.receiptHeader || "").trim();
          if (hdr) {
            hdr.split(/\r?\n/).forEach(function (ln) {
              pushBytes([ESC, 0x61, 0x01]);
              pushLn(ln.slice(0, W + 8));
              pushBytes([ESC, 0x61, 0x00]);
            });
            sep();
          }

          if (S.show_contact) {
            var bits = [];
            if ((site.company_phone || "").trim()) bits.push(site.company_phone.trim());
            if ((site.company_email || "").trim()) bits.push(site.company_email.trim());
            if (bits.length) {
              pushBytes([ESC, 0x61, 0x01]);
              pushLn(bits.join(" · ").slice(0, W + 12));
              pushBytes([ESC, 0x61, 0x00]);
              sep();
            }
          }
        }

        if (!isCompany && payload.isQuotation) {
          sep();
          pushBytes([ESC, 0x61, 0x01]);
          boldOn();
          pushLn("*** QUOTATION ***");
          boldOff();
          pushLn("NOT A FINALIZED SALE");
          pushBytes([ESC, 0x61, 0x00]);
          sep();
        }
        if (payload.isReprint) {
          sep();
          pushBytes([ESC, 0x61, 0x01]);
          boldOn();
          pushLn("*** REPRINTED RECEIPT ***");
          boldOff();
          pushLn("DUPLICATE COPY");
          pushBytes([ESC, 0x61, 0x00]);
          sep();
        }

        boldOn();
        pushLn("Receipt: " + String(payload.receiptNo || ""));
        if (S.show_datetime || isCompany) pushLn("Date: " + String(payload.printedAt || ""));
        if (!isCompany) {
          pushLn("Type: " + String(payload.mode || ""));
          var dueE = (payload.creditDueDate || "").trim();
          if (dueE && String(payload.mode || "").toLowerCase().indexOf("credit") !== -1) {
            pushLn("Pay by: " + dueE);
          }
          pushLn("Customer: " + String(payload.customerName || ""));
          pushLn("Phone: " + String(payload.customerPhone || ""));
        }
        if (isCompany) {
          pushLn("Served by: " + (String(payload.employeeName || "").trim() || "—"));
        } else if (S.show_server) {
          pushLn("Served by: " + String(payload.employeeName || ""));
        }
        boldOff();
        sep();

        var nameMax = Math.max(12, W - 4);
        if (isCompany) {
          if (S.bold_headers) boldOn();
          pushLn("Items sold");
          if (S.bold_headers) boldOff();
          (payload.lines || []).forEach(function (l) {
            var name = String((l && l.name) || "Item");
            if (l && l.discounted) name = name + " (DISCOUNTED)";
            pushLn(name.slice(0, nameMax) + "  x" + fmtQty((l && l.qty) || 0));
          });
        } else if (showPrices) {
          if (S.bold_headers) boldOn();
          pushLn(thermalItemHeaderLine(W));
          if (S.bold_headers) boldOff();
          (payload.lines || []).forEach(function (l) {
            thermalItemLines(l, W).forEach(function (row) {
              pushLn(row);
            });
          });
          sep();
          if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
            pushLn("Discounts: -" + fmt(payload.discountTotal));
          }
          if (payload.includeTax) {
            boldOn();
            pushLn("Subtotal: " + fmt(payload.subtotal || 0));
            boldOff();
            pushLn("Tax (" + String(payload.taxPercent || 0) + "%): " + fmt(payload.taxAmount || 0));
            boldOn();
            pushBytes([ESC, 0x21, 0x20]);
            pushLn("Total: " + fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0));
            charSizeNormal();
            boldOff();
          } else {
            boldOn();
            pushBytes([ESC, 0x21, 0x20]);
            pushLn("Total: " + fmt(payload.subtotal || 0));
            charSizeNormal();
            boldOff();
          }
        }

        if (showPrices) {
          var payTypeEsc = String(payload.paymentTypeLabel || "").trim();
          var pdEsc = (payload.paymentDetailText || "").trim();
          if (payTypeEsc || pdEsc) {
          sep();
          if (payTypeEsc) {
            boldOn();
            pushLn(payTypeEsc);
            boldOff();
          }
          if (pdEsc) pdEsc.split(/\r?\n/).forEach(function (ln) { pushLn(ln.slice(0, W + 8)); });
          }
        }

        var foot = (payload.receiptFooter || "").trim();
        if (foot && !isCompany) {
          sep();
          foot.split(/\r?\n/).forEach(function (ln) {
            pushBytes([ESC, 0x61, 0x01]);
            pushLn(ln.slice(0, W + 8));
            pushBytes([ESC, 0x61, 0x00]);
          });
        }

        if (!isCompany && receiptQrEnabled(S)) {
          var qrStr = buildReceiptQrPayloadStringEscPos(payload, S);
          if (qrStr) {
            sep();
            pushBytes([ESC, 0x61, 0x01]);
            charSizeSubtitle();
            pushLn("Scan QR for details");
            charSizeNormal();
            pushBytes([0x0a]);
            if (qrRasterEscPos && qrRasterEscPos.length) {
              chunks.push(qrRasterEscPos);
            } else {
              chunks.push(buildEscPosQrCodeBytes(qrStr));
            }
            pushBytes([0x0a, 0x0a]);
            pushBytes([ESC, 0x61, 0x00]);
          }
        }

        sep();
        pushBytes([ESC, 0x61, 0x01]);
        pushLn(isCompany ? "Company records" : "Thank you");
        pushBytes([ESC, 0x61, 0x00]);
        pushBytes([0x0a]);
        pushBytes([GS, 0x56, 0x00]);

        var total = 0;
        chunks.forEach(function (c) {
          total += c.length;
        });
        var out = new Uint8Array(total);
        var o = 0;
        chunks.forEach(function (c) {
          out.set(c, o);
          o += c.length;
        });
        return out;
      }

      /** IT Printing: receipt_copies 1 = customer only; 2 = + company (no prices); 3 = + cashier. */
      function receiptVariantsForCheckout() {
        var pp = window.POS_PRINTING || {};
        var rc = parseInt(String(pp.receipt_copies != null ? pp.receipt_copies : "1").trim(), 10);
        if (isNaN(rc) || rc < 1) rc = 1;
        if (rc >= 3) return ["customer", "company", "cashier"];
        if (rc === 2) return ["customer", "company"];
        return ["customer"];
      }

      function delayPrintMs(ms) {
        return new Promise(function (resolve) {
          setTimeout(resolve, ms);
        });
      }

      /**
       * Stable id for the saved printer row so local cache cannot bleed across shops/devices.
       * bluetoothId / host:port / usb vid:pid[:serial] must match server for this shop.
       */
      function printerProfileFingerprint(p) {
        if (!p || !p.printer_type) return "";
        var t = (p.printer_type || "").toLowerCase();
        var c = p.config;
        if (typeof c === "string") {
          try {
            c = JSON.parse(c) || {};
          } catch (e) {
            c = {};
          }
        }
        if (!c || typeof c !== "object") c = {};
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
          var ser = String(c.serialNumber || "").trim();
          return "usb:" + vid + ":" + pid + ":" + ser;
        }
        return t;
      }

      function offlinePrinterProfileFromCache() {
        try {
          var raw = localStorage.getItem(PRINTER_LS_KEY);
          if (!raw) return null;
          var localP = JSON.parse(raw);
          if (!localP || !localP.printer_type) return null;
          if (localP.shop_id != null && Number(localP.shop_id) !== Number(SHOP_ID)) {
            localStorage.removeItem(PRINTER_LS_KEY);
            return null;
          }
          if (
            typeof window.posPrinterTypeAllowed === "function" &&
            !window.posPrinterTypeAllowed(null, String(localP.printer_type || "").toLowerCase())
          ) {
            localStorage.removeItem(PRINTER_LS_KEY);
            return null;
          }
          return Object.assign({}, localP, { shop_id: SHOP_ID });
        } catch (e) {
          return null;
        }
      }

      /** Server is source of truth; cache in localStorage only mirrors GET /printer for this shop. */
      function loadSavedPrinterProfile() {
        return fetch(PRINTER_API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (r) {
            return r.json();
          })
          .then(function (j) {
            var sid = j && j.shop_id != null ? Number(j.shop_id) : Number(SHOP_ID);
            if (!j || Number(sid) !== Number(SHOP_ID)) {
              try {
                localStorage.removeItem(PRINTER_LS_KEY);
              } catch (e0) {}
              return null;
            }
            if (!j.ok) {
              return offlinePrinterProfileFromCache();
            }
            var serverP = j.printer || null;
            if (
              serverP &&
              typeof window.posPrinterTypeAllowed === "function" &&
              !window.posPrinterTypeAllowed(null, String(serverP.printer_type || "").toLowerCase())
            ) {
              serverP = null;
            }
            if (!serverP && j && j.stale_printer) {
              try {
                localStorage.removeItem(PRINTER_LS_KEY);
              } catch (eStale) {}
            }
            if (serverP) {
              var fp = printerProfileFingerprint(serverP);
              try {
                localStorage.setItem(
                  PRINTER_LS_KEY,
                  JSON.stringify(Object.assign({}, serverP, { shop_id: SHOP_ID, printer_fingerprint: fp }))
                );
              } catch (e1) {}
              return serverP;
            }
            try {
              localStorage.removeItem(PRINTER_LS_KEY);
            } catch (e2) {}
            return null;
          })
          .catch(function () {
            return offlinePrinterProfileFromCache();
          });
      }

      /** Simple Bluetooth session model for web apps. */
      var posBtPinnedDevice = null;
      var posBtDevicesById = Object.create(null);
      var posBtGattDisconnectHandler = null;
      var posBtBusy = false;

      function posBtPublishUiFlags() {
        window.__POS_BT = window.__POS_BT || {};
        window.__POS_BT.gattConnected = !!(posBtPinnedDevice && posBtPinnedDevice.gatt && posBtPinnedDevice.gatt.connected);
        window.__POS_BT.busyConnecting = !!posBtBusy;
        window.__POS_BT.userMuted = false;
        try {
          document.dispatchEvent(new CustomEvent("richcom-pos-bluetooth-changed"));
        } catch (eB0) {}
      }

      function posBtDetachGattHandler() {
        if (posBtPinnedDevice && posBtGattDisconnectHandler) {
          try {
            posBtPinnedDevice.removeEventListener("gattserverdisconnected", posBtGattDisconnectHandler);
          } catch (eD0) {}
        }
        posBtGattDisconnectHandler = null;
      }

      function posBtRememberDevice(device) {
        if (!device || !device.id) return;
        posBtDevicesById[String(device.id)] = device;
      }

      function posBtConnectDevice(device, timeoutMs) {
        timeoutMs = timeoutMs || 20000;
        if (!device) return Promise.reject(new Error("Bluetooth device missing."));
        if (!device.gatt) return Promise.reject(new Error("Bluetooth GATT unavailable."));
        if (device.gatt.connected) return Promise.resolve(device);
        return new Promise(function (resolve, reject) {
          var done = false;
          var tid = setTimeout(function () {
            if (done) return;
            done = true;
            reject(new Error("Bluetooth connection timed out."));
          }, timeoutMs);
          Promise.resolve(bluetoothGattConnect(device)).then(
            function () {
              if (done) return;
              done = true;
              clearTimeout(tid);
              resolve(device);
            },
            function (err) {
              if (done) return;
              done = true;
              clearTimeout(tid);
              reject(err || new Error("Bluetooth connection failed."));
            }
          );
        });
      }

      function posBtResetSession(disconnectGatt) {
        posBtDetachGattHandler();
        if (disconnectGatt && posBtPinnedDevice && posBtPinnedDevice.gatt && posBtPinnedDevice.gatt.connected) {
          try {
            posBtPinnedDevice.gatt.disconnect();
          } catch (eG0) {}
        }
        posBtPinnedDevice = null;
        posBtBusy = false;
        posBtPublishUiFlags();
      }

      function posBtWireDevice(device) {
        if (!device) return;
        posBtRememberDevice(device);
        posBtDetachGattHandler();
        posBtPinnedDevice = device;
        posBtGattDisconnectHandler = function () {
          posBtBusy = false;
          posBtPublishUiFlags();
        };
        device.addEventListener("gattserverdisconnected", posBtGattDisconnectHandler);
        posBtPublishUiFlags();
      }

      function posBtAutoConnectFromSavedRow(printerRow) {
        var cfg = normalizePrinterConfig(printerRow && printerRow.config);
        if (!cfg || !cfg.bluetoothId) return Promise.resolve(null);
        posBtBusy = true;
        posBtPublishUiFlags();
        return getBluetoothDeviceForEscPos(cfg)
          .then(function (device) {
            posBtBusy = false;
            posBtPublishUiFlags();
            return device || null;
          })
          .catch(function () {
            posBtBusy = false;
            posBtPublishUiFlags();
            return null;
          });
      }

      window.posBtOnSavedPrinterRowChanged = function (printerRow) {
        var isBt = !!(printerRow && String(printerRow.printer_type || "").toLowerCase() === "bluetooth");
        if (!isBt) {
          posBtResetSession(true);
          return;
        }
        posBtAutoConnectFromSavedRow(printerRow);
      };
      window.posBtAttachPairedThermal = function (device) {
        if (!device) return Promise.resolve(null);
        posBtBusy = true;
        posBtPublishUiFlags();
        posBtRememberDevice(device);
        if (!device.gatt) {
          posBtBusy = false;
          posBtPublishUiFlags();
          return Promise.reject(new Error("Bluetooth GATT unavailable on this device."));
        }
        return posBtConnectDevice(device, 25000)
          .then(function (d) {
            posBtBusy = false;
            posBtWireDevice(d);
            posBtPublishUiFlags();
            return d;
          })
          .catch(function (err) {
            posBtBusy = false;
            posBtPublishUiFlags();
            return Promise.reject(err || new Error("Bluetooth connection failed."));
          });
      };
      window.posBtForgetSavedThermal = function () {
        posBtResetSession(true);
      };

      /** Resolve previously paired Web Bluetooth device (getDevices — no picker). */
      function getBluetoothDeviceForEscPosBare(cfg) {
        if (!cfg || !cfg.bluetoothId) {
          return Promise.reject(new Error("No Bluetooth printer id"));
        }
        var targetId = String(cfg.bluetoothId);
        if (!navigator.bluetooth) {
          return Promise.reject(new Error("Web Bluetooth unavailable"));
        }
        if (posBtPinnedDevice && posBtPinnedDevice.id && String(posBtPinnedDevice.id) === targetId) {
          return posBtConnectDevice(posBtPinnedDevice);
        }
        var cached = posBtDevicesById[targetId];
        if (cached) {
          return posBtConnectDevice(cached);
        }
        if (typeof navigator.bluetooth.getDevices !== "function") {
          return Promise.reject(
            new Error("This browser cannot restore paired Bluetooth devices. Tap Choose printer and pair again.")
          );
        }
        return navigator.bluetooth.getDevices().then(function (devices) {
          var d = (devices || []).filter(function (x) {
            return x && x.id && String(x.id) === targetId;
          })[0];
          if (!d) {
            throw new Error("Saved printer is not available in this browser. Pair it again from Choose printer.");
          }
          posBtRememberDevice(d);
          return posBtConnectDevice(d).then(function () {
            return d;
          });
        });
      }

      /** Same as Bare, plus session wiring for disconnect / reconnect UI. */
      function getBluetoothDeviceForEscPos(cfg) {
        return getBluetoothDeviceForEscPosBare(cfg).then(function (device) {
          posBtWireDevice(device);
          return device;
        });
      }

      function pickWritableCharacteristic(svc) {
        return svc.getCharacteristics().then(function (chars) {
          var list = (chars || []).slice().sort(function (a, b) {
            return scoreCharacteristicForEscPos(a) - scoreCharacteristicForEscPos(b);
          });
          var wnr = null;
          var wr = null;
          list.forEach(function (c) {
            if (c.properties.writeWithoutResponse) wnr = wnr || c;
            else if (c.properties.write) wr = wr || c;
          });
          var chosen = wnr || wr || null;
          return chosen ? { service: svc, characteristic: chosen } : null;
        });
      }

      function findWritableEscPosCharacteristic(server, preferredCfg) {
        var preferredService = preferredCfg && preferredCfg.serviceUuid ? String(preferredCfg.serviceUuid) : "";
        var preferredChar = preferredCfg && preferredCfg.characteristicUuid ? String(preferredCfg.characteristicUuid) : "";
        function tryPreferred() {
          if (!preferredService || !preferredChar) return Promise.resolve(null);
          return server
            .getPrimaryService(preferredService)
            .then(function (svc) {
              return svc.getCharacteristic(preferredChar).then(function (ch) {
                var writable = !!(
                  (ch.properties && ch.properties.writeWithoutResponse && typeof ch.writeValueWithoutResponse === "function") ||
                  (ch.properties && ch.properties.write && typeof ch.writeValue === "function") ||
                  typeof ch.writeValueWithoutResponse === "function" ||
                  typeof ch.writeValue === "function"
                );
                return writable ? { service: svc, characteristic: ch } : null;
              });
            })
            .catch(function () {
              return null;
            });
        }
        var idx = 0;
        function tryKnown() {
          if (idx >= KNOWN_BLE_PRINT_SERVICE_UUIDS.length) {
            return server.getPrimaryServices().then(function (services) {
              function trySvc(i) {
                if (i >= services.length) return Promise.resolve(null);
                return pickWritableCharacteristic(services[i]).then(function (endpoint) {
                  if (endpoint) return endpoint;
                  return trySvc(i + 1);
                });
              }
              return trySvc(0);
            });
          }
          var uuid = KNOWN_BLE_PRINT_SERVICE_UUIDS[idx++];
          return server
            .getPrimaryService(uuid)
            .then(function (svc) {
              return pickWritableCharacteristic(svc);
            })
            .then(function (endpoint) {
              if (endpoint) return endpoint;
              return tryKnown();
            })
            .catch(function () {
              return tryKnown();
            });
        }
        return tryPreferred().then(function (preferredEndpoint) {
          if (preferredEndpoint) return preferredEndpoint;
          return tryKnown();
        });
      }

      function writeEscPosToCharacteristic(char, bytes) {
        /* BLE default ATT payload is 20 bytes; keep this unless MTU is negotiated (not exposed everywhere). */
        var chunkSize = 20;
        var offset = 0;
        var Pr = char.properties;
        var hasWo =
          typeof char.writeValueWithoutResponse === "function" &&
          (!Pr || !!Pr.writeWithoutResponse);
        var hasW = typeof char.writeValue === "function" && (!Pr || !!Pr.write);
        if (!Pr) {
          hasWo = typeof char.writeValueWithoutResponse === "function";
          hasW = typeof char.writeValue === "function";
        }
        function writeChunk(slice) {
          function viaWoResp() {
            if (!hasWo) return Promise.reject(new Error("no woResp"));
            return char.writeValueWithoutResponse(slice);
          }
          function viaWithResp() {
            if (!hasW) return Promise.reject(new Error("no write"));
            return char.writeValue(slice);
          }
          var primary = hasWo ? viaWoResp : viaWithResp;
          var fallback = hasWo && hasW ? viaWithResp : hasW && hasWo ? viaWoResp : null;
          return primary().catch(function (err) {
            var msg = String((err && err.message) || "").toLowerCase();
            var nm = (err && err.name) || "";
            var flaky =
              fallback &&
              (msg.indexOf("unknown") !== -1 ||
                msg.indexOf("gatt") !== -1 ||
                nm === "NetworkError" ||
                nm === "InvalidStateError");
            if (flaky) return fallback();
            throw err;
          });
        }
        var interChunkMs = hasW && !hasWo ? 22 : 15;
        function step() {
          if (offset >= bytes.length) return Promise.resolve();
          var end = Math.min(offset + chunkSize, bytes.length);
          var slice = bytes.subarray(offset, end);
          offset = end;
          return Promise.resolve(writeChunk(slice)).then(function () {
            if (offset < bytes.length) {
              return new Promise(function (resolve) {
                setTimeout(function () {
                  resolve(step());
                }, interChunkMs);
              });
            }
          });
        }
        return step();
      }

      function printBluetoothEscPosReceipt(device, payload, printOpts) {
        printOpts = printOpts || {};
        if (!window.isSecureContext) {
          setAuthStatus("Bluetooth printing needs HTTPS (or localhost). Open the shop POS over a secure URL.", "error");
          return Promise.reject(new Error("Bluetooth needs HTTPS"));
        }
        if (!device.gatt) {
          setAuthStatus("This Bluetooth device does not expose GATT (needed for thermal print).", "error");
          return Promise.reject(new Error("No GATT"));
        }
        var variants;
        if (Array.isArray(printOpts.receiptVariants) && printOpts.receiptVariants.length) {
          variants = printOpts.receiptVariants;
        } else if (printOpts.singleReceiptCopy) {
          variants = ["customer"];
        } else {
          variants = receiptVariantsForCheckout();
        }
        var connectP = device.gatt.connected ? Promise.resolve() : bluetoothGattConnect(device);
        return connectP
          .then(function () {
            return delayPrintMs(device.gatt.connected ? 15 : 40);
          })
          .then(function () {
            var btCfg = printOpts.btConfig || null;
            return findWritableEscPosCharacteristic(device.gatt, btCfg);
          })
          .then(function (endpoint) {
            var ch = endpoint && endpoint.characteristic ? endpoint.characteristic : null;
            if (!ch) {
              setAuthStatus(
                "No writable print channel on this printer. Open printer setup, forget, and pair again (BLE services must be allowed).",
                "error"
              );
              return Promise.reject(new Error("No BLE print channel"));
            }
            function sendVariant(i) {
              if (i >= variants.length) return Promise.resolve();
              var variant = variants[i];
              var S = receiptSettings();
              var qrTxt =
                variant !== "company" && receiptQrEnabled(S)
                  ? buildReceiptQrPayloadStringEscPos(payload, S)
                  : "";
              function writeBytes(qrRaster) {
                var bytes = buildEscPosReceiptBytes(payload, variant, qrRaster);
                return writeEscPosToCharacteristic(ch, bytes).then(function () {
                  return delayPrintMs(i + 1 < variants.length ? 380 : 0).then(function () {
                    return sendVariant(i + 1);
                  });
                });
              }
              if (!qrTxt) {
                return writeBytes();
              }
              return fetch(QR_ESC_POS_API, {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json", Accept: "application/octet-stream" },
                body: JSON.stringify({ text: qrTxt }),
              })
                .then(function (r) {
                  if (!r.ok) throw new Error("qr");
                  return r.arrayBuffer();
                })
                .then(function (ab) {
                  return writeBytes(new Uint8Array(ab));
                })
                .catch(function () {
                  return writeBytes();
                });
            }
            return sendVariant(0);
          })
          .catch(function (e) {
            var hint = bluetoothGattUserHint(e);
            var line = hint || ((e && e.message) ? e.message : "Bluetooth thermal print failed.");
            setAuthStatus(line, "error");
            return Promise.reject(e);
          });
      }

      /** Minimal receipt payload for manual “Test print” from printer setup (one slip). */
      function makePrinterTestReceiptPayload() {
        var site = window.POS_SITE || {};
        var now = new Date().toLocaleString();
        return {
          receiptNo: "TEST",
          printedAt: now,
          shopName: site.shop_name || "Shop",
          companyName: site.company_name || "",
          shopCode: (site.shop_code || "").trim(),
          shopLocation: (site.shop_location || "").trim(),
          receiptLogoUrl: (site.receipt_logo_url || site.app_icon_url || "").trim(),
          mode: "Printer test",
          isQuotation: false,
          customerName: "Test print",
          customerPhone: "-",
          employeeName: "—",
          employeeCode: "",
          lines: [
            {
              id: 0,
              name: "Test slip — OK",
              qty: 1,
              price: 0,
              total: 0,
              listPrice: 0,
              listTotal: 0,
              lineDiscount: 0,
              discounted: false,
            },
          ],
          subtotal: 0,
          discountTotal: 0,
          hasDiscount: false,
          taxAmount: 0,
          taxPercent: 0,
          grandTotal: 0,
          includeTax: false,
          paymentTypeLabel: "",
          paymentDetailText: "",
          receiptHeader: "",
          receiptFooter: "",
          creditDueDate: "",
        };
      }

      function uint8ToBase64(u8) {
        var CHUNK = 0x8000;
        var s = "";
        for (var i = 0; i < u8.length; i += CHUNK) {
          s += String.fromCharCode.apply(null, u8.subarray(i, i + CHUNK));
        }
        return btoa(s);
      }

      /** Server opens TCP to the shop’s saved network printer (browser cannot). Retries once on failure. */
      function sendEscPosToSavedNetworkPrinter(bytes, attempt) {
        attempt = attempt || 0;
        return fetch(NETWORK_ESC_POS_API, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ data_b64: uint8ToBase64(bytes) }),
        }).then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) {
              if (attempt < 1) {
                return delayPrintMs(450).then(function () {
                  return sendEscPosToSavedNetworkPrinter(bytes, attempt + 1);
                });
              }
              throw new Error((j && j.error) || r.statusText);
            }
            return j;
          });
        });
      }

      function printNetworkEscPosReceipt(payload, printOpts) {
        printOpts = printOpts || {};
        var variants =
          Array.isArray(printOpts.receiptVariants) && printOpts.receiptVariants.length
            ? printOpts.receiptVariants
            : receiptVariantsForCheckout();
        function sendVariant(i) {
          if (i >= variants.length) return Promise.resolve();
          var variant = variants[i];
          var S = receiptSettings();
          var qrTxt =
            variant !== "company" && receiptQrEnabled(S)
              ? buildReceiptQrPayloadStringEscPos(payload, S)
              : "";
          function writeBytes(qrRaster) {
            var bytes = buildEscPosReceiptBytes(payload, variant, qrRaster);
            return sendEscPosToSavedNetworkPrinter(bytes).then(function () {
              return delayPrintMs(i + 1 < variants.length ? 380 : 0).then(function () {
                return sendVariant(i + 1);
              });
            });
          }
          if (!qrTxt) {
            return writeBytes();
          }
          return fetch(QR_ESC_POS_API, {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json", Accept: "application/octet-stream" },
            body: JSON.stringify({ text: qrTxt }),
          })
            .then(function (r) {
              if (!r.ok) throw new Error("qr");
              return r.arrayBuffer();
            })
            .then(function (ab) {
              return writeBytes(new Uint8Array(ab));
            })
            .catch(function () {
              return writeBytes();
            });
        }
        return sendVariant(0).catch(function (e) {
          setAuthStatus((e && e.message) ? e.message : "Network thermal print failed.", "error");
          return Promise.reject(e);
        });
      }

      window.posIsUsbAccessDeniedError = function (e) {
        var m = (e && e.message) || String(e || "");
        return /access denied/i.test(m) || (e && e.name === "SecurityError");
      };
      window.posUsbAccessDeniedShort = function () {
        return "Receipts will print via browser (choose POS-80C). Silent USB is blocked by Windows.";
      };
      window.posUsbAccessDeniedDetail = function () {
        return (
          "Windows is using the POS-80 / thermal driver, so Chrome cannot open the printer for silent ESC/POS. " +
          "That is normal — use the print dialog and pick POS-80C. Close other apps if print fails. " +
          "For silent USB only: free the device for Chrome (advanced: WinUSB via Zadig)."
        );
      };
      window.posFormatUsbError = function (e, opts) {
        opts = opts || {};
        if (window.posIsUsbAccessDeniedError(e)) {
          return opts.detail ? window.posUsbAccessDeniedDetail() : window.posUsbAccessDeniedShort();
        }
        return (e && e.message) || String(e || "");
      };

      function openUsbDeviceOrExplain(device) {
        if (!device) return Promise.reject(new Error("No USB device."));
        if (device.opened) return Promise.resolve(device);
        return device.open().catch(function (e) {
          return Promise.reject(new Error(window.posFormatUsbError(e)));
        });
      }

      /** WebUSB: select configuration and claim first bulk OUT endpoint (thermal ESC/POS). */
      function ensureUsbConfiguration(device) {
        if (device.configuration) return Promise.resolve();
        var confs = device.configurations;
        if (!confs || !confs.length) {
          return Promise.reject(new Error("USB printer has no configuration."));
        }
        var i = 0;
        function tryNext() {
          if (i >= confs.length) {
            return Promise.reject(new Error("Could not activate USB printer (try reconnecting the cable)."));
          }
          var cv = confs[i].configurationValue;
          i++;
          return device.selectConfiguration(cv).catch(tryNext);
        }
        return tryNext();
      }

      function claimUsbBulkOut(device) {
        var cfg = device.configuration;
        if (!cfg) return Promise.reject(new Error("USB configuration not selected."));
        var ifaceList = cfg.interfaces || [];
        function tryIface(ii) {
          if (ii >= ifaceList.length) {
            return Promise.reject(
              new Error("No USB bulk OUT endpoint. Use a direct USB thermal driver or set up network printing.")
            );
          }
          var iface = ifaceList[ii];
          var alts = iface.alternates || [];
          function tryAlt(ai) {
            if (ai >= alts.length) return tryIface(ii + 1);
            var a = alts[ai];
            var eps = (a && a.endpoints) || [];
            var ep = null;
            for (var k = 0; k < eps.length; k++) {
              if (eps[k].type === "bulk" && eps[k].direction === "out") {
                ep = eps[k];
                break;
              }
            }
            if (!ep) return tryAlt(ai + 1);
            return device.claimInterface(iface.interfaceNumber).then(function () {
              return { interfaceNumber: iface.interfaceNumber, endpointNumber: ep.endpointNumber };
            });
          }
          return tryAlt(0);
        }
        return tryIface(0);
      }

      function releaseUsbPrinterInterface(device, interfaceNumber) {
        if (!device || typeof interfaceNumber !== "number") return Promise.resolve();
        return device
          .releaseInterface(interfaceNumber)
          .catch(function () {})
          .then(function () {
            if (device.opened) return device.close().catch(function () {});
          });
      }

      function writeUsbBulkOut(device, endpointNumber, bytes) {
        var chunk = 4096;
        var offset = 0;
        function step() {
          if (offset >= bytes.length) return Promise.resolve();
          var end = Math.min(offset + chunk, bytes.length);
          var slice = bytes.subarray(offset, end);
          return device.transferOut(endpointNumber, slice).then(function () {
            offset = end;
            return step();
          });
        }
        return step();
      }

      /** Raw ESC/POS over WebUSB (saved bridge/type USB printers). */
      function printUsbEscPosReceipt(usbCfg, payload, printOpts) {
        printOpts = printOpts || {};
        if (!window.isSecureContext) {
          setAuthStatus("USB printing needs HTTPS (or localhost).", "error");
          return Promise.reject(new Error("USB printing needs HTTPS (or localhost)."));
        }
        if (!navigator.usb) {
          setAuthStatus("WebUSB is not available in this browser.", "error");
          return Promise.reject(new Error("WebUSB is not available in this browser."));
        }
        var vid = parseInt(usbCfg && usbCfg.vendorId, 10);
        var pid = parseInt(usbCfg && usbCfg.productId, 10);
        if (isNaN(vid) || isNaN(pid)) {
          setAuthStatus("Invalid saved USB printer. Open printer setup and select the device again.", "error");
          return Promise.reject(new Error("Invalid saved USB printer."));
        }
        var variants =
          Array.isArray(printOpts.receiptVariants) && printOpts.receiptVariants.length
            ? printOpts.receiptVariants
            : receiptVariantsForCheckout();
        return navigator.usb
          .getDevices()
          .then(function (devices) {
            var device = devices.filter(function (x) {
              return x && x.vendorId === vid && x.productId === pid;
            })[0];
            if (!device) {
              throw new Error(
                "USB printer not found. Plug it in and open printer setup so the browser can access it again."
              );
            }
            return openUsbDeviceOrExplain(device)
              .then(function () {
                return ensureUsbConfiguration(device);
              })
              .then(function () {
                return claimUsbBulkOut(device);
              })
              .then(function (picked) {
                function cleanup() {
                  return releaseUsbPrinterInterface(device, picked.interfaceNumber);
                }
                function drainVariant(i) {
                  if (i >= variants.length) return cleanup();
                  var variant = variants[i];
                  var S = receiptSettings();
                  var qrTxt =
                    variant !== "company" && receiptQrEnabled(S)
                      ? buildReceiptQrPayloadStringEscPos(payload, S)
                      : "";
                  function writeBytes(qrRaster) {
                    var bytes = buildEscPosReceiptBytes(payload, variant, qrRaster);
                    return writeUsbBulkOut(device, picked.endpointNumber, bytes).then(function () {
                      return delayPrintMs(i + 1 < variants.length ? 380 : 0).then(function () {
                        return drainVariant(i + 1);
                      });
                    });
                  }
                  if (!qrTxt) return writeBytes();
                  return fetch(QR_ESC_POS_API, {
                    method: "POST",
                    credentials: "same-origin",
                    headers: { "Content-Type": "application/json", Accept: "application/octet-stream" },
                    body: JSON.stringify({ text: qrTxt }),
                  })
                    .then(function (r) {
                      if (!r.ok) throw new Error("qr");
                      return r.arrayBuffer();
                    })
                    .then(function (ab) {
                      return writeBytes(new Uint8Array(ab));
                    })
                    .catch(function () {
                      return writeBytes();
                    });
                }
                return drainVariant(0).catch(function (e) {
                  return cleanup().then(function () {
                    throw e;
                  });
                });
              });
          })
          .catch(function (e) {
            var denied =
              typeof window.posIsUsbAccessDeniedError === "function" && window.posIsUsbAccessDeniedError(e);
            var errMsg = window.posFormatUsbError
              ? window.posFormatUsbError(e, { detail: false })
              : (e && e.message) || "USB thermal print failed.";
            if (!denied) {
              setAuthStatus(errMsg, "error");
            }
            return Promise.reject(new Error(errMsg));
          });
      }

      /** @param {"thermal"|"normal"} printMode — thermal: narrow roll preview; normal: A4/system print dialog */
      function buildReceiptHtmlForPrint(payload, variant, printMode) {
        printMode = printMode === "normal" ? "normal" : "thermal";
        variant = variant || "customer";
        if (printMode === "thermal") {
          return buildThermalReceiptPlainHtml(payload, variant);
        }
        var isCompany = variant === "company";
        var isCashier = variant === "cashier";
        var showPrices = !isCompany;
        var S = receiptSettings();
        var site = window.POS_SITE || {};
        var label;
        if (isCompany) label = "Company copy (items only)";
        else if (isCashier) label = "Cashier copy";
        else label = "Sale receipt";
        var hb = S.bold_headers ? " b" : "";

        var rows = (payload.lines || [])
          .map(function (l) {
            var discTag = l && l.discounted ? " <span class='mut' style='font-size:10px'>(DISCOUNTED)</span>" : "";
            if (isCompany) {
              return (
                "<tr><td class='name'>" +
                receiptEsc(l.name) +
                discTag +
                '</td><td class="qty">' +
                receiptEsc(fmtQty(l.qty)) +
                "</td></tr>"
              );
            }
            return (
              '<tr><td class="name">' +
              receiptEsc(l.name) +
              discTag +
              '</td><td class="qty">' +
              receiptEsc(fmtQty(l.qty)) +
              '</td><td class="amt">' +
              receiptEsc(fmt(l.total)) +
              "</td></tr>"
            );
          })
          .join("");
        var thead = isCompany
          ? "<thead><tr><td class='name" + hb + "'>Item</td><td class='qty" + hb + "'>Qty</td></tr></thead>"
          : "<thead><tr><td class='name" + hb + "'>Item</td><td class='qty" + hb + "'>Qty</td><td class='amt" + hb + "'>Amount</td></tr></thead>";

        var logoBlock = "";
        var logoSrc = (site.receipt_logo_url || site.app_icon_url || "").replace(/"/g, "");
        if (S.show_logo && logoSrc) {
          logoBlock =
            "<div class='c' style='margin-bottom:6px'><img src=\"" +
            logoSrc +
            '" alt="" style="height:3rem;width:3rem;object-fit:contain" /></div>';
        }

        var companySubBlock = "";
        if ((payload.companyName || "").trim()) {
          companySubBlock =
            "<div class='c mut' style='font-size:12px;margin-bottom:4px'>" +
            receiptEsc(payload.companyName) +
            "</div>";
        }
        var branchBlock = "";
        var brParts = [];
        if (!isCompany && (payload.shopLocation || "").trim()) brParts.push(String(payload.shopLocation).trim());
        if (brParts.length) {
          branchBlock =
            "<div class='c mut' style='font-size:11px;line-height:1.35;margin-bottom:6px'>" +
            receiptEsc(brParts.join(" · ")) +
            "</div>";
        }

        var headerBlock = "";
        var htxt = (payload.receiptHeader || "").trim();
        if (htxt && !isCompany) {
          headerBlock =
            "<div class='c mut' style='white-space:pre-wrap;font-size:11px'>" +
            receiptEsc(htxt) +
            "</div><div class='sep'></div>";
        }

        var contactBlock = "";
        if (!isCompany && S.show_contact) {
          var bits = [];
          if ((site.company_phone || "").trim()) bits.push(site.company_phone.trim());
          if ((site.company_email || "").trim()) bits.push(site.company_email.trim());
          if (bits.length) {
            contactBlock =
              "<div class='c mut' style='font-size:11px'>" +
              receiptEsc(bits.join(" · ")) +
              "</div><div class='sep'></div>";
          }
        }

        var quoteBanner = "";
        if (!isCompany && payload.isQuotation) {
          quoteBanner =
            "<div class='c b' style='margin:6px 0'>*** QUOTATION ***</div>" +
            "<div class='c mut' style='font-size:11px'>Not a finalized sale</div><div class='sep'></div>";
        }

        var metaRows = "";
        metaRows += quoteBanner;
        metaRows +=
          "<div class='row' style='padding:8px 10px;margin:8px 0;border:2px solid #93c5fd;border-radius:8px;background:linear-gradient(180deg,#eff6ff,#f8fafc)'>" +
          "<span>Receipt</span><span class='bb'>" +
          receiptEsc(payload.receiptNo) +
          "</span></div>";
        if (S.show_datetime || isCompany) {
          metaRows +=
            "<div class='row'><span>Date</span><span class='b'>" + receiptEsc(payload.printedAt) + "</span></div>";
        }
        if (!isCompany) {
          metaRows +=
            "<div class='row'><span>Type</span><span class='b'>" + receiptEsc(payload.mode) + "</span></div>";
          var dueHtml = (payload.creditDueDate || "").trim();
          if (dueHtml && String(payload.mode || "").toLowerCase().indexOf("credit") !== -1) {
            metaRows +=
              "<div class='row'><span>Pay by</span><span class='b'>" + receiptEsc(dueHtml) + "</span></div>";
          }
          metaRows +=
            "<div class='row'><span>Customer</span><span class='b' style='color:#b45309'>" +
            receiptEsc(payload.customerName) +
            "</span></div>" +
            "<div class='row'><span>Phone</span><span class='b'>" +
            receiptEsc(payload.customerPhone) +
            "</span></div>";
          if (S.show_server) {
            metaRows +=
              "<div class='row'><span>Served by</span><span class='b' style='color:#047857'>" +
              receiptEsc(payload.employeeName) +
              "</span></div>";
          }
        } else {
          metaRows +=
            "<div class='row'><span>Served by</span><span class='b' style='color:#047857'>" +
            receiptEsc(String(payload.employeeName || "").trim() || "—") +
            "</span></div>";
        }

        var discountBlock = "";
        if (showPrices && payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
          discountBlock =
            "<div class='row'><span>Discounts</span><span>-" +
            receiptEsc(fmt(payload.discountTotal)) +
            "</span></div>";
        }

        var subRow = "";
        if (showPrices) {
          if (payload.includeTax) {
            subRow =
              "<div class='sep'></div>" +
              discountBlock +
              "<div class='row tot'><span>Subtotal</span><span>" +
              receiptEsc(fmt(payload.subtotal)) +
              "</span></div>" +
              "<div class='row'><span>Tax (" +
              receiptEsc(String(payload.taxPercent)) +
              "%)</span><span>" +
              receiptEsc(fmt(payload.taxAmount)) +
              "</span></div>" +
              "<div class='row tot'><span>Total</span><span>" +
              receiptEsc(fmt(payload.grandTotal)) +
              "</span></div>";
          } else {
            subRow =
              "<div class='sep'></div>" +
              discountBlock +
              "<div class='row tot'><span>Total</span><span>" +
              receiptEsc(fmt(payload.subtotal)) +
              "</span></div>";
          }
        }

        var payBlock = "";
        if (showPrices) {
          var payType = String(payload.paymentTypeLabel || "").trim();
          var pd = (payload.paymentDetailText || "").trim();
          if (payType || pd) {
            payBlock = "<div class='sep'></div>";
            if (payType) payBlock += "<div class='b'>" + receiptEsc(payType) + "</div>";
            if (pd) {
              payBlock +=
                "<div style='white-space:pre-wrap;font-size:11px;margin-top:4px'>" + receiptEsc(pd) + "</div>";
            }
          }
        }

        var footerBlock = "";
        var ft = (payload.receiptFooter || "").trim();
        if (ft && !isCompany) {
          footerBlock =
            "<div class='sep'></div><div class='c mut' style='white-space:pre-wrap;font-size:11px'>" +
            receiptEsc(ft) +
            "</div>";
        }

        var qrBlock = "";
        if (!isCompany && receiptQrEnabled(S)) {
          var qrDataN = buildReceiptQrPayloadString(payload, S);
          if (qrDataN) {
            var encN = encodeURIComponent(qrDataN);
            qrBlock =
              "<div class='sep'></div><div class='c' style='margin:8px 0;display:flex;flex-direction:column;align-items:center;width:100%'><img src=\"https://api.qrserver.com/v1/create-qr-code/?size=200x200&amp;data=" +
              encN +
              "\" alt=\"\" width=\"140\" height=\"140\" style=\"display:block;width:140px;height:140px;object-fit:contain;margin:0 auto\" /></div>" +
              "<div class='c mut' style='font-size:10px;text-align:center;width:100%'>Scan for receipt details</div>";
          }
        }

        var styleBlock =
          "@page{size:A4 portrait;margin:12mm;}html,body{box-sizing:border-box;background:#fff;color:#111;margin:0;padding:0;width:100%;min-width:100%;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:11pt;line-height:1.45;}" +
          "*,*::before,*::after{box-sizing:border-box}.r{width:100%;max-width:36rem;margin:0 auto;padding:0.5rem 1rem 1.5rem;}" +
          ".receipt-browser-print-tip{margin:0 auto 14px;max-width:36rem;padding:10px 14px;border:1px dashed #d97706;background:#fffbeb;color:#78350f;font-size:10.5pt;line-height:1.4;border-radius:8px}" +
          "@media print{.receipt-browser-print-tip{display:none!important;}}" +
          "html{-webkit-print-color-adjust:exact;print-color-adjust:exact}" +
          ".c{text-align:center}.b{font-weight:700;color:#0f172a}.bb{font-weight:900;color:#1e40af}.mut{color:#64748b;font-size:0.92rem}.sep{border-top:1px solid #cbd5e1;margin:10px 0}" +
          "table{width:100%;border-collapse:collapse}td{vertical-align:top;padding:4px 2px;font-weight:600;color:#0f172a}.name{width:58%}.qty{width:12%;text-align:center;color:#1d4ed8;font-weight:700}.amt{width:30%;text-align:right;font-weight:800;color:#b91c1c}" +
          ".row{display:flex;justify-content:space-between;gap:12px;margin:4px 0;padding:3px 0}.row>span:first-child{font-weight:700;color:#1d4ed8;text-transform:uppercase;font-size:9pt;letter-spacing:0.04em}.row>span:last-child{font-weight:800;color:#0f172a}" +
          ".row .b,.row .bb{color:#1e40af!important}" +
          ".tot{font-size:13pt;font-weight:900;color:#b91c1c}.tot>span:first-child{color:#1d4ed8}";
        var printTipHtml =
          "<div class=\"receipt-browser-print-tip\" role=\"status\"><strong>Print on paper:</strong> If the button says <strong>Save</strong>, open <strong>Destination</strong> and pick your physical printer (e.g. Brother), not &quot;Save as PDF&quot; — then the button becomes <strong>Print</strong>. Web pages cannot choose the printer for you.</div>";

        var headMeta =
          "<meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>Receipt</title>";
        var subhead =
          (label ? "<div class='c mut'>" + receiptEsc(label) + "</div>" : "") + "<div class='sep'></div>";
        var itemsCaption = isCompany ? "<div class='c b' style='margin-bottom:6px;font-size:12pt'>Items sold</div>" : "";
        var closingThanks = isCompany
          ? "<div class='sep'></div><div class='c mut'>Company records</div><div class='c mut'>Richcom POS</div>"
          : "<div class='sep'></div><div class='c mut'>Thank you</div><div class='c mut'>Powered by Richcom POS</div>";
        return (
          "<!doctype html><html><head>" +
          headMeta +
          "<style>" +
          styleBlock +
          "</style></head><body>" +
          printTipHtml +
          "<div class='r'>" +
          logoBlock +
          companySubBlock +
          branchBlock +
          "<div class='c bb' style='font-size:15px;letter-spacing:.03em'>" +
          receiptEsc(payload.shopName) +
          "</div>" +
          subhead +
          headerBlock +
          contactBlock +
          metaRows +
          "<div class='sep'></div>" +
          itemsCaption +
          "<table>" +
          thead +
          "<tbody>" +
          rows +
          "</tbody></table>" +
          subRow +
          payBlock +
          footerBlock +
          qrBlock +
          closingThanks +
          "</div></body></html>"
        );
      }

      /**
       * @param {"normal"|"thermal"} layout
       * @param {number} [contentWidthPx] — fallback width in px when widthMm not set (normal ≈816).
       * @param {number} [widthMm] — for thermal only: set iframe width in mm so Chrome print preview matches roll paper (avoids tiny strip).
       */
      function printReceiptInIframe(html, layout, contentWidthPx, widthMm) {
        layout = layout === "thermal" ? "thermal" : "normal";
        return new Promise(function (resolve) {
          try {
            var iframe = document.createElement("iframe");
            iframe.setAttribute("aria-hidden", "true");
            iframe.style.position = "fixed";
            iframe.style.top = "0";
            iframe.style.left = "-9999px";
            iframe.style.border = "0";
            iframe.style.margin = "0";
            iframe.style.padding = "0";
            iframe.style.opacity = "0";
            iframe.style.pointerEvents = "none";
            iframe.style.zIndex = "-1";
            var wPx =
              contentWidthPx != null && !isNaN(contentWidthPx)
                ? contentWidthPx
                : layout === "normal"
                  ? 816
                  : 302;
            if (layout === "thermal" && widthMm != null && !isNaN(Number(widthMm))) {
              var mm = Number(widthMm);
              iframe.style.width = mm + "mm";
              iframe.style.minWidth = mm + "mm";
              iframe.style.maxWidth = mm + "mm";
            } else {
              iframe.style.width = wPx + "px";
              iframe.style.minWidth = "";
              iframe.style.maxWidth = "";
            }
            iframe.style.height = "200px";
            document.body.appendChild(iframe);
            var doc = iframe.contentWindow.document;
            doc.open();
            doc.write(html);
            doc.close();
            setTimeout(function () {
              waitForIframeImages(
                doc,
                function () {
                  var h = 400;
                  try {
                    var b = doc.body;
                    var el = doc.documentElement;
                    if (layout === "thermal" && b) {
                      h = Math.ceil((b.scrollHeight || b.offsetHeight || 0) + 8);
                    } else {
                      h = Math.max(
                        b ? b.scrollHeight : 0,
                        b ? b.offsetHeight : 0,
                        el ? el.scrollHeight : 0,
                        el ? el.offsetHeight : 0
                      );
                    }
                  } catch (e) {}
                  if (!h || h < 48) h = layout === "normal" ? 600 : 320;
                  h = Math.min(h + 16, layout === "normal" ? 1600 : 2400);
                  iframe.style.height = h + "px";
                  setTimeout(function () {
                    try {
                      iframe.contentWindow.focus();
                      iframe.contentWindow.print();
                    } catch (e) {}
                    setTimeout(function () {
                      try {
                        iframe.remove();
                      } catch (e) {}
                      resolve();
                    }, 400);
                  }, layout === "normal" ? 120 : 100);
                },
                5000
              );
            }, layout === "normal" ? 200 : 150);
          } catch (e) {
            resolve();
          }
        });
      }

      function printThermalReceipt(payload, variant) {
        var rwmm = receiptRollWidthMm();
        var wpx = Math.round((rwmm * 96) / 25.4);
        return printReceiptInIframe(
          buildReceiptHtmlForPrint(payload, variant, "thermal"),
          "thermal",
          wpx,
          rwmm
        );
      }

      function thermalReceiptDocExtractStyle(fullHtml) {
        var m = String(fullHtml || "").match(/<style>([\s\S]*?)<\/style>/i);
        return m ? m[1] : "";
      }

      function thermalReceiptDocExtractBodyInner(fullHtml) {
        var m = String(fullHtml || "").match(/<body[^>]*>([\s\S]*?)<\/body>/i);
        return m ? m[1].trim() : "";
      }

      /**
       * Browser thermal: one print dialog with page breaks — sequential window.print() calls are often blocked
       * after the first sheet, so customer + company + cashier must share one document when using OS print.
       */
      function printThermalReceiptMulti(payload, variants) {
        var rwmm = receiptRollWidthMm();
        var wpx = Math.round((rwmm * 96) / 25.4);
        var vars = Array.isArray(variants) && variants.length ? variants.slice() : ["customer"];
        if (vars.length === 1) return printThermalReceipt(payload, vars[0]);
        var firstHtml = buildThermalReceiptPlainHtml(payload, vars[0]);
        var style = thermalReceiptDocExtractStyle(firstHtml);
        var chunks = vars.map(function (v) {
          return thermalReceiptDocExtractBodyInner(buildThermalReceiptPlainHtml(payload, v));
        });
        var combinedInner = chunks
          .map(function (inner, idx) {
            var pba = idx < chunks.length - 1 ? "page-break-after:always;break-after:page;" : "";
            return '<section class="rc-multi-slip" style="' + pba + '">' + inner + "</section>";
          })
          .join("");
        var rollCss = rwmm === 58 ? "58mm" : "80mm";
        var html =
          "<!doctype html><html><head><meta charset='utf-8'><title>Receipts</title>" +
          "<style>" +
          style +
          ".rc-multi-slip{display:block}" +
          "</style></head><body>" +
          combinedInner +
          "</body></html>";
        return printReceiptInIframe(html, "thermal", wpx, rwmm);
      }

      /** Print one job per variant from IT Printing (customer / company / cashier), or a caller override. */
      function printReceiptCopies(payload, printFn, variantsOverride) {
        var vars =
          Array.isArray(variantsOverride) && variantsOverride.length
            ? variantsOverride
            : receiptVariantsForCheckout();
        if (printFn === printThermalReceipt && vars.length > 1) {
          return printThermalReceiptMulti(payload, vars);
        }
        function step(i) {
          if (i >= vars.length) return Promise.resolve();
          return printFn(payload, vars[i]).then(function () {
            return delayPrintMs(520).then(function () {
              return step(i + 1);
            });
          });
        }
        return step(0);
      }

      function normalizePrinterConfig(raw) {
        if (raw == null) return null;
        if (typeof raw === "string") {
          try {
            return JSON.parse(raw) || null;
          } catch (e) {
            return null;
          }
        }
        return typeof raw === "object" ? raw : null;
      }

      /** Silent ESC/POS over Web Bluetooth using the saved shop printer only. */
      function tryBluetoothEscPosSilent(payload, pp, saved, btPrintOpts) {
        if (typeof window.posPrinterTypeAllowed === "function" && !window.posPrinterTypeAllowed(null, "bluetooth")) {
          return Promise.reject(new Error("Bluetooth printing disabled"));
        }
        if (!window.isSecureContext || !navigator.bluetooth) {
          return Promise.reject(new Error("Web Bluetooth unavailable"));
        }
        var cfg = normalizePrinterConfig(saved && saved.config);
        if (!saved || String(saved.printer_type || "").toLowerCase() !== "bluetooth" || !cfg || !cfg.bluetoothId) {
          return Promise.reject(new Error("No saved Bluetooth printer"));
        }
        return getBluetoothDeviceForEscPos(cfg).then(function (device) {
          var merged = Object.assign({ btConfig: cfg }, btPrintOpts || {});
          return printBluetoothEscPosReceipt(device, payload, merged);
        });
      }

      /** True when IT allows any POS printer path — browser thermal dialog is allowed as fallback (not only “USB printer”). */
      function posPrintingAllowsBrowserThermalFallback() {
        return (
          (typeof window.posPrinterTypeAllowed === "function" && window.posPrinterTypeAllowed(null, "usb")) ||
          window.posPrinterTypeAllowed(null, "network") ||
          window.posPrinterTypeAllowed(null, "bluetooth")
        );
      }

      /**
       * Opens the system/browser print dialog for thermal HTML receipts (OS printer / Save as PDF).
       * Skipped only when IT has turned off all printer connection types.
       */
      function printBrowserThermalDialog(payload, printOpts) {
        if (!posPrintingAllowsBrowserThermalFallback()) {
          return Promise.resolve();
        }
        var vo =
          printOpts && Array.isArray(printOpts.receiptVariants) && printOpts.receiptVariants.length
            ? printOpts.receiptVariants
            : null;
        return printReceiptCopies(payload, printThermalReceipt, vo);
      }

      /**
       * After checkout: try silent ESC/POS (network / USB / Bluetooth) using the saved profile first.
       * Preflight TCP/USB/BT probes were removed — they often false-negative while the real print path works.
       * Opens the browser print dialog when direct ESC/POS fails or no saved profile, if any printer type is allowed.
       * @param {object} [printOpts] — optional ``{ receiptVariants: [...] }`` (direct/credit checkout passes ``receiptVariantsForCheckout()``; held save uses ``["company"]`` only).
       */
      function runConfiguredPrinterAction(payload, printOpts) {
        printOpts = printOpts || {};
        var pp = window.POS_PRINTING || {};
        return loadSavedPrinterProfile().then(function (saved) {
          var pt = saved ? String(saved.printer_type || "").toLowerCase() : "";
          /* Optional printing: checkout works without a saved profile; still offer OS/browser thermal print when allowed. */
          if (!printingCompulsoryOnSaleEnabled() && (!saved || !pt)) {
            return printBrowserThermalDialog(payload, printOpts);
          }
          var cfg = saved ? normalizePrinterConfig(saved.config) : null;
          var host = cfg && String(cfg.host || "").trim();

          function fallbackBrowserOrBluetooth() {
            return tryBluetoothEscPosSilent(payload, pp, saved, printOpts).catch(function () {
              return printBrowserThermalDialog(payload, printOpts);
            });
          }

          if (
            saved &&
            pt === "network" &&
            host &&
            typeof window.posPrinterTypeAllowed === "function" &&
            window.posPrinterTypeAllowed(null, "network")
          ) {
            return printNetworkEscPosReceipt(payload, printOpts).catch(function () {
              return fallbackBrowserOrBluetooth();
            });
          }

          var usbVid = cfg != null ? parseInt(cfg.vendorId, 10) : NaN;
          var usbPid = cfg != null ? parseInt(cfg.productId, 10) : NaN;
          var isUsb =
            saved &&
            pt === "usb" &&
            cfg != null &&
            !isNaN(usbVid) &&
            !isNaN(usbPid) &&
            typeof window.posPrinterTypeAllowed === "function" &&
            window.posPrinterTypeAllowed(null, "usb");
          if (isUsb && navigator.usb) {
            return printUsbEscPosReceipt(cfg, payload, printOpts).catch(function (usbErr) {
              try {
                if (typeof toastSay === "function") {
                  var denied =
                    typeof window.posIsUsbAccessDeniedError === "function" &&
                    window.posIsUsbAccessDeniedError(usbErr);
                  if (!denied) {
                    var usbMsg = (usbErr && usbErr.message) || "USB print failed";
                    toastSay(usbMsg + " — opening browser print…");
                  }
                }
              } catch (eToastUsb) {}
              // Some USB bridges are flaky with custom variant overrides.
              // Retry once using the baseline USB path before browser fallback.
              if (printOpts && Array.isArray(printOpts.receiptVariants) && printOpts.receiptVariants.length) {
                return printUsbEscPosReceipt(cfg, payload).catch(function () {
                  return fallbackBrowserOrBluetooth();
                });
              }
              return fallbackBrowserOrBluetooth();
            });
          }

          if (saved && pt === "bluetooth" && cfg && cfg.bluetoothId) {
            return tryBluetoothEscPosSilent(payload, pp, saved, printOpts).catch(function () {
              return printBrowserThermalDialog(payload, printOpts);
            });
          }

          return fallbackBrowserOrBluetooth();
        });
      }

      document.addEventListener("click", function (e) {
        var btn = e.target.closest(".pos-qty-btn");
        if (!btn) return;
        e.preventDefault();
        var id = parseInt(btn.getAttribute("data-id"), 10);
        var delta = parseInt(btn.getAttribute("data-delta"), 10);
        changeQty(id, delta);
      });

      document.addEventListener(
        "blur",
        function (e) {
          var t = e.target;
          if (t && t.classList && t.classList.contains("pos-qty-input")) {
            commitQtyInput(t);
            return;
          }
          if (t && t.classList && t.classList.contains("pos-line-amount-input")) {
            commitLineAmountInput(t);
          }
        },
        true
      );

      document.addEventListener("input", function (e) {
        var t = e.target;
        if (t && t.classList && t.classList.contains("pos-line-amount-input")) {
          updatePosLineAmountPreview(t);
          return;
        }
        if (t && t.classList && t.classList.contains("pos-line-price")) {
          var row = t.closest && t.closest(".pos-cart-line-item");
          var amt = row && row.querySelector(".pos-line-amount-input");
          if (amt) updatePosLineAmountPreview(amt);
        }
      });

      document.addEventListener(
        "keydown",
        function (e) {
          var inp =
            e.target && e.target.closest
              ? e.target.closest(".pos-qty-input, .pos-line-amount-input")
              : null;
          if (!inp) return;
          if (e.key === "Enter") {
            e.preventDefault();
            inp.blur();
          } else if (e.key === "Escape") {
            e.preventDefault();
            try {
              var id = parseInt(inp.getAttribute("data-id"), 10);
              var lines = load();
              var ln = lines.find(function (l) {
                return l.id === id;
              });
              if (ln != null) {
                if (inp.classList.contains("pos-line-amount-input")) {
                  var pu = parseFloat(ln.price);
                  if (!isNaN(pu) && pu > 0) inp.value = fmt(pu * (parseFloat(ln.qty) || 0));
                  updatePosLineAmountPreview(inp);
                } else {
                  inp.value = fmtQty(parseFloat(ln.qty) || 1);
                }
              }
            } catch (err1) {}
            inp.blur();
          }
        },
        false
      );

      document.addEventListener(
        "wheel",
        function (e) {
          var a = document.activeElement;
          if (
            a &&
            a.classList &&
            (a.classList.contains("pos-qty-input") || a.classList.contains("pos-line-amount-input"))
          ) {
            e.preventDefault();
          }
        },
        { passive: false }
      );

      function playPosItemCardTapAnim(card) {
        if (!card || !card.classList) return;
        card.classList.remove("pos-item-card--tapped");
        void card.offsetWidth;
        card.classList.add("pos-item-card--tapped");
        window.setTimeout(function () {
          card.classList.remove("pos-item-card--tapped");
        }, 480);
      }

      function initPosItemCardMotion() {
        var catalogRoot = document.getElementById("pos-catalog");
        if (catalogRoot && !catalogRoot.dataset.posCardsDelegated) {
          catalogRoot.dataset.posCardsDelegated = "1";
          catalogRoot.addEventListener(
            "click",
            function (e) {
              var btn = e.target && e.target.closest ? e.target.closest(".pos-item-card") : null;
              if (!btn || !catalogRoot.contains(btn)) return;
              if (posCatalogClickSuppressed()) {
                e.preventDefault();
                e.stopPropagation();
                return;
              }
              playPosItemCardTapAnim(btn);
              addFromCard(btn);
            },
            true
          );
          catalogRoot.addEventListener("keydown", function (e) {
            if (e.key !== "Enter" && e.key !== " ") return;
            var btn = e.target && e.target.closest ? e.target.closest(".pos-item-card") : null;
            if (!btn || !catalogRoot.contains(btn)) return;
            if (posCatalogClickSuppressed()) {
              e.preventDefault();
              return;
            }
            e.preventDefault();
            playPosItemCardTapAnim(btn);
            addFromCard(btn);
          });
          catalogRoot.addEventListener("pointerdown", function (e) {
            var btn = e.target && e.target.closest ? e.target.closest(".pos-item-card") : null;
            if (!btn || btn.disabled) return;
            btn.classList.add("is-pressing");
          });
          catalogRoot.addEventListener("pointerup", function (e) {
            var btn = e.target && e.target.closest ? e.target.closest(".pos-item-card") : null;
            if (btn) btn.classList.remove("is-pressing");
          });
          catalogRoot.addEventListener("pointercancel", function (e) {
            var btn = e.target && e.target.closest ? e.target.closest(".pos-item-card") : null;
            if (btn) btn.classList.remove("is-pressing");
          });
        }

      }

      function bindPosStripScrollEffects() {
        document.querySelectorAll(".pos-cat-strip-outer .pos-item-scroll").forEach(function (strip) {
          if (strip.dataset.posStripFxBound) return;
          strip.dataset.posStripFxBound = "1";
          var outer = strip.closest(".pos-cat-strip-outer");
          var scrollEndTimer = null;
          strip.addEventListener(
            "scroll",
            function () {
              strip.classList.add("is-strip-scrolling");
              if (outer) outer.classList.add("is-strip-scrolling");
              if (scrollEndTimer) window.clearTimeout(scrollEndTimer);
              scrollEndTimer = window.setTimeout(function () {
                strip.classList.remove("is-strip-scrolling");
                if (outer) outer.classList.remove("is-strip-scrolling");
              }, 140);
            },
            { passive: true }
          );
        });
      }
      initPosItemCardMotion();
      bindPosStripScrollEffects();

      var searchEl = document.getElementById("pos-search");
      var noRes = document.getElementById("pos-no-results");
      /** Match item name (data-search); every whitespace-separated token must appear in the name. */
      function posItemNameMatches(card, q) {
        if (!q) return true;
        var name = (card.getAttribute("data-search") || "").toLowerCase();
        var tokens = q.split(/\s+/).filter(Boolean);
        if (!tokens.length) return true;
        return tokens.every(function (t) {
          return name.indexOf(t) !== -1;
        });
      }
      var searchStatusEl = document.getElementById("pos-search-status");
      var searchClearBtn = document.getElementById("pos-search-clear");
      var chipsScrollEl = document.getElementById("pos-cat-chips");
      var chipsRailEl = chipsScrollEl ? chipsScrollEl.closest(".pos-search-panel__chips-rail") : null;
      var catalogChipScrollLockUntil = 0;
      var catalogChipScrollTick = false;

      function posCatalogTypingTarget(el) {
        if (!el || !el.tagName) return false;
        var tag = el.tagName.toUpperCase();
        return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || el.isContentEditable;
      }

      function setActiveCatalogChip(chip, scrollChipIntoView) {
        if (!chip) return;
        document.querySelectorAll(".pos-cat-chip").forEach(function (c) {
          c.classList.remove("is-active");
          c.setAttribute("aria-selected", "false");
        });
        chip.classList.add("is-active");
        chip.setAttribute("aria-selected", "true");
        if (scrollChipIntoView !== false && chipsScrollEl && chip.scrollIntoView) {
          try {
            chip.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
          } catch (e2) {
            chip.scrollIntoView(false);
          }
        }
      }

      function syncSearchClearUi() {
        var hasQ = !!(searchEl && searchEl.value && searchEl.value.trim());
        if (searchClearBtn) searchClearBtn.classList.toggle("hidden", !hasQ);
        if (searchEl) searchEl.classList.toggle("pos-search-field__input--has-value", hasQ);
      }

      function updateCatalogChipFilters() {
        document.querySelectorAll(".pos-cat-chip:not(.pos-cat-chip--all)").forEach(function (chip) {
          var id = chip.getAttribute("data-target");
          var sec = id ? document.getElementById(id) : null;
          var hidden = !!(sec && sec.classList.contains("pos-live-search-hidden"));
          chip.classList.toggle("pos-cat-chip--hidden", hidden);
          chip.disabled = hidden;
        });
      }

      function updateSearchStatus() {
        if (!searchStatusEl) return;
        var q = (searchEl && searchEl.value ? searchEl.value : "").trim().toLowerCase();
        var visibleCards = document.querySelectorAll(".pos-item-card:not(.pos-live-search-hidden)").length;
        var visibleSections = document.querySelectorAll(".pos-cat-section:not(.pos-live-search-hidden)").length;
        if (q) {
          if (!visibleCards) {
            searchStatusEl.textContent = "No matches — try fewer words";
            searchStatusEl.classList.add("pos-search-status--warn");
          } else {
            searchStatusEl.textContent =
              visibleCards +
              " item" +
              (visibleCards === 1 ? "" : "s") +
              " · " +
              visibleSections +
              " " +
              (visibleSections === 1 ? "category" : "categories");
            searchStatusEl.classList.remove("pos-search-status--warn");
          }
        } else {
          searchStatusEl.textContent = visibleCards + " items";
          searchStatusEl.classList.remove("pos-search-status--warn");
        }
      }

      function runSearch() {
        var q = (searchEl && searchEl.value ? searchEl.value : "").toLowerCase().trim();

        var any = false;

        document.querySelectorAll(".pos-cat-section").forEach(function (sec) {
          var showSec = false;
          sec.querySelectorAll(".pos-item-card").forEach(function (card) {
            var ok = posItemNameMatches(card, q);
            card.classList.toggle("pos-live-search-hidden", !ok);
            if (ok) showSec = true;
          });
          sec.classList.toggle("pos-live-search-hidden", !showSec);
          if (showSec) any = true;
        });
        if (noRes) {
          noRes.classList.toggle("hidden", any || !q);
        }
        document.querySelectorAll(".pos-cat-strip-outer .pos-item-scroll").forEach(function (strip) {
          updatePosCatStripScrollUi(strip);
        });
        syncSearchClearUi();
        updateCatalogChipFilters();
        updateSearchStatus();
        if (q) {
          var firstVisible = document.querySelector(".pos-cat-section:not(.pos-live-search-hidden)");
          if (firstVisible) {
            catalogChipScrollLockUntil = Date.now() + 700;
            var matchChip = document.querySelector(
              '.pos-cat-chip[data-target="' + firstVisible.id + '"]'
            );
            if (matchChip) setActiveCatalogChip(matchChip);
          }
        }
      }

      function clearPosSearch(focusAfter) {
        if (!searchEl) return;
        searchEl.value = "";
        runSearch();
        syncSearchClearUi();
        var allChip = document.querySelector(".pos-cat-chip--all");
        if (allChip) setActiveCatalogChip(allChip);
        if (focusAfter) searchEl.focus();
      }

      function jumpCatalogChipTarget(chip) {
        if (!chip) return;
        catalogChipScrollLockUntil = Date.now() + 900;
        setActiveCatalogChip(chip);
        if (chip.classList.contains("pos-cat-chip--all")) {
          if (searchEl && searchEl.value.trim()) {
            clearPosSearch(false);
          }
          var panel = document.querySelector(".pos-search-panel");
          var top = panel || document.getElementById("pos-catalog");
          if (top) top.scrollIntoView({ behavior: "smooth", block: "start" });
          return;
        }
        var id = chip.getAttribute("data-target");
        var el = id ? document.getElementById(id) : null;
        if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
      }

      function updateChipsNavButtons() {
        if (!chipsScrollEl || !chipsRailEl) return;
        var max = chipsScrollEl.scrollWidth - chipsScrollEl.clientWidth;
        var left = chipsScrollEl.scrollLeft;
        var prev = chipsRailEl.querySelector(".pos-chips-nav--prev");
        var next = chipsRailEl.querySelector(".pos-chips-nav--next");
        var noScroll = max <= 4;
        if (prev) {
          prev.disabled = noScroll || left <= 2;
          prev.classList.toggle("pos-chips-nav--disabled", prev.disabled);
        }
        if (next) {
          next.disabled = noScroll || left >= max - 2;
          next.classList.toggle("pos-chips-nav--disabled", next.disabled);
        }
      }

      function initPosCatalogCommandUx() {
        if (searchEl) {
          searchEl.addEventListener("input", runSearch);
          searchEl.addEventListener("search", runSearch);
          searchEl.addEventListener("keydown", function (e) {
            if (e.key === "Escape") {
              e.preventDefault();
              clearPosSearch(false);
              searchEl.blur();
            }
          });
        }
        if (searchClearBtn) {
          searchClearBtn.addEventListener("click", function () {
            clearPosSearch(true);
          });
        }
        if (chipsScrollEl && !chipsScrollEl.dataset.posChipsDelegated) {
          chipsScrollEl.dataset.posChipsDelegated = "1";
          chipsScrollEl.addEventListener("click", function (e) {
            var chip = e.target && e.target.closest ? e.target.closest(".pos-cat-chip") : null;
            if (chip) jumpCatalogChipTarget(chip);
          });
        }
        var topBtn = document.getElementById("pos-chips-scroll-start");
        if (topBtn) {
          topBtn.addEventListener("click", function () {
            var all = document.querySelector(".pos-cat-chip--all");
            jumpCatalogChipTarget(all || null);
          });
        }
        if (chipsRailEl) {
          chipsRailEl.querySelectorAll(".pos-chips-nav").forEach(function (btn) {
            btn.addEventListener("click", function () {
              if (!chipsScrollEl || btn.disabled) return;
              var dir = btn.classList.contains("pos-chips-nav--next") ? 1 : -1;
              chipsScrollEl.scrollBy({ left: dir * Math.max(200, chipsScrollEl.clientWidth * 0.55), behavior: "smooth" });
            });
          });
        }
        if (chipsScrollEl) {
          chipsScrollEl.addEventListener("scroll", updateChipsNavButtons, { passive: true });
          updateChipsNavButtons();
        }
        document.addEventListener("keydown", function (e) {
          if (posCatalogTypingTarget(e.target) && e.key !== "Escape") return;
          if (e.key === "/" && !e.ctrlKey && !e.metaKey && !e.altKey) {
            if (!searchEl) return;
            var tag = e.target && e.target.tagName ? e.target.tagName.toUpperCase() : "";
            if (tag === "INPUT" || tag === "TEXTAREA") return;
            e.preventDefault();
            searchEl.focus();
            try {
              searchEl.select();
            } catch (e3) {}
            return;
          }
          if (e.key === "k" && (e.ctrlKey || e.metaKey)) {
            if (!searchEl) return;
            e.preventDefault();
            searchEl.focus();
            try {
              searchEl.select();
            } catch (e4) {}
          }
        });
        function syncActiveCatalogChipFromScroll() {
          if (Date.now() < catalogChipScrollLockUntil) return;
          var q = (searchEl && searchEl.value ? searchEl.value : "").trim();
          if (q) return;
          var anchor = 140 + (window.visualViewport ? 0 : 0);
          var sections = document.querySelectorAll(".pos-cat-section:not(.pos-live-search-hidden)");
          var best = null;
          var bestTop = Infinity;
          sections.forEach(function (sec) {
            var r = sec.getBoundingClientRect();
            if (r.bottom < anchor) return;
            var dist = Math.abs(r.top - anchor);
            if (dist < bestTop) {
              bestTop = dist;
              best = sec;
            }
          });
          if (!best || !best.id) return;
          var chip = document.querySelector('.pos-cat-chip[data-target="' + best.id + '"]');
          if (chip && !chip.classList.contains("is-active")) setActiveCatalogChip(chip, false);
        }
        window.addEventListener(
          "scroll",
          function () {
            if (catalogChipScrollTick) return;
            catalogChipScrollTick = true;
            requestAnimationFrame(function () {
              catalogChipScrollTick = false;
              syncActiveCatalogChipFromScroll();
            });
          },
          { passive: true }
        );
        syncSearchClearUi();
        updateSearchStatus();
        try {
          if (window.matchMedia("(min-width: 900px) and (hover: hover)").matches && !sessionStorage.getItem("pos-search-autofocused")) {
            sessionStorage.setItem("pos-search-autofocused", "1");
            setTimeout(function () {
              if (searchEl && !document.querySelector(".pos-cart-drawer.is-open")) searchEl.focus();
            }, 400);
          }
        } catch (e5) {}
      }
      initPosCatalogCommandUx();

      function updatePosCatStripScrollUi(strip) {
        var wrap = strip.closest(".pos-cat-strip-outer");
        if (!wrap) return;
        var maxScroll = strip.scrollWidth - strip.clientWidth;
        var noHop = maxScroll <= 4;
        wrap.classList.toggle("pos-cat-strip--no-horizontal-scroll", noHop);
        var prev = wrap.querySelector(".pos-cat-scroll-prev");
        var next = wrap.querySelector(".pos-cat-scroll-next");
        var left = strip.scrollLeft;
        if (prev) prev.disabled = noHop || left <= 2;
        if (next) next.disabled = noHop || left >= maxScroll - 2;
      }

      function refreshAllPosCatStrips() {
        document.querySelectorAll(".pos-cat-strip-outer .pos-item-scroll").forEach(function (strip) {
          if (typeof updatePosCatStripScrollUi === "function") updatePosCatStripScrollUi(strip);
        });
      }

      function initPosCategoryScrollStrips() {
        var catalogRoot = document.getElementById("pos-catalog");
        if (catalogRoot && !catalogRoot.dataset.posStripsDelegated) {
          catalogRoot.dataset.posStripsDelegated = "1";
          catalogRoot.addEventListener("click", function (e) {
            var btn = e.target && e.target.closest ? e.target.closest(".pos-cat-scroll-btn") : null;
            if (!btn || !catalogRoot.contains(btn)) return;
            e.preventDefault();
            e.stopPropagation();
            var wrap = btn.closest(".pos-cat-strip-outer");
            var strip = wrap && wrap.querySelector(".pos-item-scroll");
            if (!strip) return;
            var dir = parseInt(btn.getAttribute("data-pos-scroll-dir") || "0", 10) || 0;
            var step = Math.max(220, Math.floor(strip.clientWidth * 0.72));
            strip.scrollBy({ left: step * dir, behavior: "smooth" });
            window.setTimeout(function () {
              updatePosCatStripScrollUi(strip);
            }, 380);
          });
        }
        document.querySelectorAll(".pos-cat-strip-outer .pos-item-scroll").forEach(function (strip) {
          if (strip.dataset.posStripScrollBound) return;
          strip.dataset.posStripScrollBound = "1";
          strip.addEventListener("scroll", function () {
            updatePosCatStripScrollUi(strip);
          });
          if (typeof ResizeObserver !== "undefined") {
            var ro = new ResizeObserver(function () {
              updatePosCatStripScrollUi(strip);
            });
            ro.observe(strip);
          }
        });
        if (!window.__posCatStripGlobalsBound) {
          window.__posCatStripGlobalsBound = true;
          window.addEventListener("resize", refreshAllPosCatStrips);
          window.addEventListener("orientationchange", refreshAllPosCatStrips);
          window.addEventListener("load", refreshAllPosCatStrips);
        }
        requestAnimationFrame(function () {
          requestAnimationFrame(refreshAllPosCatStrips);
        });
      }

      initPosCategoryScrollStrips();

      var clearBtn = document.getElementById("pos-cart-clear");
      if (clearBtn) {
        clearBtn.addEventListener("click", function () {
          save([]);
          render();
          setPaymentMethod("");
          clearAuthorization();
          clearKnownCustomer();
          setCustomerLookupStatus("Enter phone to lookup.", "muted");
          var qOnly = document.getElementById("pos-quote-only");
          if (qOnly) qOnly.checked = false;
          applyPosCartUiSettings();
        });
      }

      var saleBtn = document.getElementById("pos-sale-type-sale");
      var creditBtn = document.getElementById("pos-sale-type-credit");
      var quoteModeBtn = document.getElementById("pos-sale-type-quote");
      var payCashBtn = document.getElementById("pos-payment-method-cash");
      var payMpesaBtn = document.getElementById("pos-payment-method-mpesa");
      var payBothBtn = document.getElementById("pos-payment-method-both");
      var payCashInput = document.getElementById("pos-payment-cash-amount");
      var payMpesaInput = document.getElementById("pos-payment-mpesa-amount");
      if (saleBtn) {
        saleBtn.addEventListener("click", function () {
          if (!posCartFeatureFlags().sale) return;
          var qOnly = document.getElementById("pos-quote-only");
          if (qOnly) qOnly.checked = false;
          setSaleType("sale");
        });
      }
      if (creditBtn) {
        creditBtn.addEventListener("click", function () {
          if (!posCartFeatureFlags().credit) return;
          var qOnly = document.getElementById("pos-quote-only");
          if (qOnly) qOnly.checked = false;
          setSaleType("credit");
        });
      }
      if (quoteModeBtn) {
        quoteModeBtn.addEventListener("click", function () {
          if (!posCartFeatureFlags().quotations) return;
          var qOnly = document.getElementById("pos-quote-only");
          if (!qOnly) return;
          qOnly.checked = !qOnly.checked;
          setSaleType("sale");
        });
      }
      if (payCashBtn)
        payCashBtn.addEventListener("click", function () {
          if (!posPaymentFeatureFlags().cash) return;
          setPaymentMethod("cash");
        });
      if (payMpesaBtn)
        payMpesaBtn.addEventListener("click", function () {
          if (!posPaymentFeatureFlags().mpesa) return;
          setPaymentMethod("mpesa");
        });
      if (payBothBtn)
        payBothBtn.addEventListener("click", function () {
          if (!posPaymentFeatureFlags().both) return;
          setPaymentMethod("both");
        });
      if (payCashInput) payCashInput.addEventListener("input", function () { syncSplitAmounts("cash"); });
      if (payMpesaInput) payMpesaInput.addEventListener("input", function () { syncSplitAmounts("mpesa"); });
      if (payCashInput) payCashInput.addEventListener("focus", function () { payCashInput.select(); });
      if (payMpesaInput) payMpesaInput.addEventListener("focus", function () { payMpesaInput.select(); });
      if (payCashInput) payCashInput.addEventListener("blur", normalizeSplitInputs);
      if (payMpesaInput) payMpesaInput.addEventListener("blur", normalizeSplitInputs);
      var quoteOnlyToggle = document.getElementById("pos-quote-only");
      if (quoteOnlyToggle) {
        quoteOnlyToggle.addEventListener("change", function () {
          var quoteBtn = document.getElementById("pos-sale-type-quote");
          if (quoteBtn) quoteBtn.classList.toggle("is-active", !!quoteOnlyToggle.checked);
          refreshPaymentMethodUi();
          updateCustomerSectionState();
          updatePosCompulsoryPrinterWorkspaceLock();
        });
      }

      var customerPhoneEl = document.getElementById("pos-customer-phone");
      var customerNameEl = document.getElementById("pos-customer-name");
      if (customerPhoneEl) {
        customerPhoneEl.addEventListener("input", function () {
          clearKnownCustomer();
          scheduleCustomerLookup();
          updateCustomerSectionState();
        });
        customerPhoneEl.addEventListener("blur", function () {
          runCustomerLookupNow();
        });
      }
      if (customerNameEl) {
        customerNameEl.addEventListener("input", function () {
          updateCustomerSectionState();
        });
        customerNameEl.addEventListener("blur", function () {
          maybeRegisterCustomerAfterAuth();
          updateCustomerSectionState();
        });
      }

      var authCodeEl = document.getElementById("pos-auth-code");
      var verifyBtn = document.getElementById("pos-auth-verify");
      if (authCodeEl) {
        authCodeEl.addEventListener("input", function () {
          var cleaned = (authCodeEl.value || "").replace(/\D/g, "").slice(0, 6);
          if (authCodeEl.value !== cleaned) authCodeEl.value = cleaned;
          if (cleaned.length < 6) lastVerifiedAuthCode = "";
          if (authorizedEmployee) {
            authorizedEmployee = null;
            setAuthStatus("Code changed. Enter 6 digits to auto-check again.", "muted");
            updateProceedState();
          }
          if (cleaned.length === 6 && cleaned !== lastVerifiedAuthCode && !authVerifyInFlight) {
            verifyAuthorizationCode();
          }
        });
        authCodeEl.addEventListener("keydown", function (e) {
          if (e.key === "Enter") {
            e.preventDefault();
            verifyAuthorizationCode();
          }
        });
      }
      if (verifyBtn) {
        verifyBtn.addEventListener("click", function () {
          verifyAuthorizationCode();
        });
      }

      var proceedBtn = document.getElementById("pos-cart-proceed");
      if (proceedBtn) {
        proceedBtn.addEventListener("click", function () {
          proceedSale();
        });
      }

      var drawer = document.getElementById("pos-cart-drawer");
      var backdrop = document.getElementById("pos-cart-backdrop");
      var cartTriggers = document.querySelectorAll(".pos-cart-open-trigger");
      var closeBtn = document.getElementById("pos-cart-close");
      var CART_MS = 360;
      var cartUiOpen = false;

      function setBodyScrollLock(on) {
        document.body.style.overflow = on ? "hidden" : "";
      }

      function setCartOpen(open) {
        if (!drawer || !backdrop) return;
        cartUiOpen = open;
        if (open) {
          drawer.hidden = false;
          backdrop.setAttribute("aria-hidden", "false");
          requestAnimationFrame(function () {
            drawer.classList.add("is-open");
            backdrop.classList.add("is-visible");
            setBodyScrollLock(true);
          });
          cartTriggers.forEach(function (t) {
            t.setAttribute("aria-expanded", "true");
          });
        } else {
          drawer.classList.remove("is-open");
          backdrop.classList.remove("is-visible");
          backdrop.setAttribute("aria-hidden", "true");
          setBodyScrollLock(false);
          cartTriggers.forEach(function (t) {
            t.setAttribute("aria-expanded", "false");
          });
          setTimeout(function () {
            drawer.hidden = true;
          }, CART_MS);
        }
      }

      function toggleCart() {
        setCartOpen(!cartUiOpen);
      }

      cartTriggers.forEach(function (btn) {
        btn.addEventListener("click", toggleCart);
      });
      if (closeBtn) closeBtn.addEventListener("click", function () {
        setCartOpen(false);
      });
      // Expose the drawer state setter so other POS subsystems (held-order save,
      // sale-finalize hook, …) can close the cart reliably without relying on
      // synthesizing a click on the close button. Synthetic clicks have been
      // flaky inside async success callbacks.
      try {
        window.__posSetCartOpen = function (open) { setCartOpen(!!open); };
      } catch (eExpose) {}
      if (backdrop) {
        backdrop.addEventListener("click", function () {
          setCartOpen(false);
        });
      }

      document.addEventListener("keydown", function (e) {
        if (e.key !== "Escape") return;
        var compDlg = document.getElementById("pos-compulsory-printer-modal");
        if (compDlg && !compDlg.classList.contains("hidden")) {
          if (typeof posCompulsoryPrinterGateBlocking === "function" && posCompulsoryPrinterGateBlocking()) {
            e.preventDefault();
            return;
          }
          posCompulsoryPrinterModalHide();
          e.preventDefault();
          return;
        }
        if (document.getElementById("pos-printer-dialog") && !document.getElementById("pos-printer-dialog").classList.contains("hidden")) return;
        if (!cartUiOpen) return;
        setCartOpen(false);
      });

      (function clock() {
        var el = document.getElementById("pos-clock");
        if (!el) return;
        function tick() {
          var d = new Date();
          el.textContent = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        }
        tick();
        setInterval(tick, 30000);
      })();

      var fy = document.getElementById("pos-footer-year");
      if (fy) fy.textContent = String(new Date().getFullYear());

      function applyLinePriceInput(inp) {
        if (!inp || !window.POS_PRINTING || !window.POS_PRINTING.allow_line_price_edit) return;
        var id = parseInt(inp.getAttribute("data-id"), 10);
        if (isNaN(id)) return;
        var lines = load();
        var i = lines.findIndex(function (l) {
          return l.id === id;
        });
        if (i < 0) return;
        var floor = lines[i].originalSellingPrice;
        if (floor == null || isNaN(parseFloat(floor))) floor = 0;
        floor = Math.max(0, parseFloat(floor));
        var v = parseFloat(inp.value);
        if (isNaN(v)) {
          inp.value = fmt(lines[i].price);
          return;
        }
        if (v < floor) {
          v = floor;
          inp.value = fmt(v);
        }
        if (v < 0) v = 0;
        if (lines[i].listPrice == null || isNaN(lines[i].listPrice)) {
          lines[i].listPrice = lines[i].price;
        }
        lines[i].price = v;
        save(lines);
        render();
      }

      document.addEventListener("change", function (e) {
        var inp = e.target.closest && e.target.closest(".pos-line-price");
        if (!inp) return;
        applyLinePriceInput(inp);
      });

      document.addEventListener("blur", function (e) {
        var inp = e.target.closest && e.target.closest(".pos-line-price");
        if (!inp) return;
        applyLinePriceInput(inp);
      }, true);

      (function applyPrinterTypeAllowListEarly() {
        if (typeof window.applyPrinterTypeAllowList === "function") {
          window.applyPrinterTypeAllowList();
          return;
        }
        var allowed = window.posPrinterTypeAllowed;
        document.querySelectorAll(".pos-printer-type-btn").forEach(function (btn) {
          var t = btn.getAttribute("data-printer-type");
          if (!t) return;
          var allow = allowed ? allowed(null, t) : false;
          btn.classList.toggle("hidden", !allow);
          btn.classList.toggle("pos-printer-type-blocked", !allow);
          btn.disabled = !allow;
        });
      })();

      (function scheduleCatalogStockPoll() {
        var stockPollTimer = null;
        function posKitchenPortionTrackedFromRow(row) {
          var n = Number(row && row.stock_update_enabled);
          if (!Number.isFinite(n)) return true;
          return n === 1;
        }
        function posEffectiveQtyFromCatalogRow(row, mode) {
          mode = mode || window.POS_INVENTORY_MODE || "shop";
          if (!row) return 0;
          if (mode === "kitchen" || mode === "both") {
            if (!posKitchenPortionTrackedFromRow(row)) return 999999999;
            var kk = parseInt(row.kitchen_portions, 10);
            return isNaN(kk) ? 0 : kk;
          }
          if (mode === "none") return 999999999;
          var ss = parseFloat(row.shop_stock_qty);
          return isFinite(ss) ? ss : 0;
        }
        function applyCatalogStockRows(list) {
          if (!list || !list.length) return;
          var mode = window.POS_INVENTORY_MODE || "shop";
          var map = {};
          list.forEach(function (row) {
            if (row && row.id != null) map[Number(row.id)] = row;
          });
          document.querySelectorAll(".pos-item-card[data-item-id]").forEach(function (btn) {
            var id = parseInt(btn.getAttribute("data-item-id"), 10);
            var row = map[id];
            if (!row) return;
            var q = posEffectiveQtyFromCatalogRow(row, mode);
            var prev = parseFloat(btn.getAttribute("data-stock"));
            if (!isFinite(prev)) prev = 0;
            btn.setAttribute("data-stock", String(q));
            var kp = parseInt(row.kitchen_portions, 10);
            var sq = parseFloat(row.shop_stock_qty);
            if (isNaN(kp)) kp = 0;
            if (!isFinite(sq)) sq = 0;
            btn.setAttribute("data-kitchen-portions", String(kp));
            btn.setAttribute("data-shop-stock", String(sq));
            if (row.price != null && !isNaN(parseFloat(row.price))) {
              btn.setAttribute("data-price", parseFloat(row.price).toFixed(2));
            }
            if (row.original_selling_price != null && !isNaN(parseFloat(row.original_selling_price))) {
              btn.setAttribute("data-original-selling-price", parseFloat(row.original_selling_price).toFixed(2));
            }
            if (prev !== q) {
              btn.classList.add("pos-stock-updated");
              setTimeout(function () {
                btn.classList.remove("pos-stock-updated");
              }, 700);
            }
            var lbl = btn.querySelector(".pos-item-card__stock-label");
            if (lbl) {
              lbl.textContent =
                mode === "kitchen" || mode === "both"
                  ? posKitchenPortionTrackedFromRow(row)
                    ? "Kitchen"
                    : "—"
                  : mode === "shop"
                    ? "Stock"
                    : "—";
            }
            var badge = btn.querySelector("[data-pos-stock]");
            if (badge) {
              var kitchenUntracked =
                (mode === "kitchen" || mode === "both") && !posKitchenPortionTrackedFromRow(row);
              badge.setAttribute("data-pos-stock-qty", mode === "none" || kitchenUntracked ? "999999999" : String(q));
              badge.classList.remove("is-in-stock", "is-out");
              if (mode === "none" || kitchenUntracked) {
                badge.classList.add("is-in-stock");
                badge.textContent = "—";
              } else {
                badge.classList.add(q > 0 ? "is-in-stock" : "is-out");
                badge.textContent = q > 0 ? String(q) : "0";
              }
            }
          });
          var lines = load();
          var changed = false;
          lines.forEach(function (l) {
            var r = map[l.id];
            if (!r) return;
            var ns = posEffectiveQtyFromCatalogRow(r, mode);
            if (l.stock !== ns) {
              l.stock = ns;
              changed = true;
            }
          });
          if (changed) {
            save(lines);
            render();
          }
        }
        function normalizeCatalogInventoryMode(m) {
          var s = String(m == null ? "" : m)
            .toLowerCase()
            .trim();
          if (s === "shop" || s === "kitchen" || s === "both" || s === "none") return s;
          return null;
        }

        function posCatalogItemIdsFromDom() {
          var ids = [];
          document.querySelectorAll(".pos-item-card[data-item-id]").forEach(function (btn) {
            var id = parseInt(btn.getAttribute("data-item-id"), 10);
            if (isFinite(id) && id > 0) ids.push(id);
          });
          ids.sort(function (a, b) {
            return a - b;
          });
          return ids;
        }

        function posCatalogItemIdsFromPayload(items) {
          var ids = [];
          (items || []).forEach(function (row) {
            var id = parseInt(row && row.id, 10);
            if (isFinite(id) && id > 0) ids.push(id);
          });
          ids.sort(function (a, b) {
            return a - b;
          });
          return ids;
        }

        function posCatalogIdsMismatch(apiItems) {
          var domIds = posCatalogItemIdsFromDom();
          var apiIds = posCatalogItemIdsFromPayload(apiItems);
          if (domIds.length !== apiIds.length) return true;
          for (var i = 0; i < domIds.length; i++) {
            if (domIds[i] !== apiIds[i]) return true;
          }
          return false;
        }

        var catalogReloadPending = false;

        function fetchCatalogStock() {
          if (!CATALOG_STOCK_API) return;
          fetch(CATALOG_STOCK_API, {
            credentials: "same-origin",
            cache: "no-store",
            headers: { Accept: "application/json" },
          })
            .then(function (r) {
              if (!r.ok) throw new Error("catalog_bad_status");
              return r.json();
            })
            .then(function (j) {
              if (!j || !j.ok) return;
              var nm = normalizeCatalogInventoryMode(j.inventory_mode);
              if (nm) window.POS_INVENTORY_MODE = nm;
              var items = Array.isArray(j.items) ? j.items : [];
              if (posCatalogIdsMismatch(items)) {
                if (!catalogReloadPending) {
                  catalogReloadPending = true;
                  location.reload();
                }
                return;
              }
              if (items.length) {
                applyCatalogStockRows(items);
                saveCatalogSnapshot({
                  inventory_mode: window.POS_INVENTORY_MODE || "shop",
                  items: items,
                  source: "network",
                  saved_at: new Date().toISOString(),
                }).catch(function () {});
                setCatalogStaleBanner("", "warn");
              }
            })
            .catch(function () {
              loadCatalogSnapshot().then(function (snap) {
                if (!snap || !Array.isArray(snap.items) || !snap.items.length) return;
                var sn = normalizeCatalogInventoryMode(snap.inventory_mode);
                if (sn) window.POS_INVENTORY_MODE = sn;
                applyCatalogStockRows(snap.items);
                var savedAtMs = Date.parse(String(snap.saved_at || ""));
                var ageMs = isFinite(savedAtMs) ? Math.max(0, Date.now() - savedAtMs) : 0;
                var tone = ageMs > OFFLINE_CATALOG_STALE_MS ? "error" : "warn";
                setCatalogStaleBanner(
                  "Using offline catalog snapshot (" + formatCatalogAge(ageMs) + "). Reconnect internet to refresh stock and prices.",
                  tone
                );
              });
            });
        }
        function startStockPoll() {
          clearInterval(stockPollTimer);
          stockPollTimer = setInterval(fetchCatalogStock, 12000);
        }
        document.addEventListener("visibilitychange", function () {
          if (document.hidden) {
            clearInterval(stockPollTimer);
          } else {
            fetchCatalogStock();
            startStockPoll();
          }
        });
        setTimeout(function () {
          loadCatalogSnapshot().then(function (snap) {
            if (!snap || !Array.isArray(snap.items) || !snap.items.length) return;
            // Only merge cached catalog when offline. If online, fetchCatalogStock() is authoritative;
            // applying a stale snapshot here races with that fetch and can overwrite correct stock with 0.
            if (!navigator.onLine) {
              var sn0 = normalizeCatalogInventoryMode(snap.inventory_mode);
              if (sn0) window.POS_INVENTORY_MODE = sn0;
              applyCatalogStockRows(snap.items);
              var savedAtMs = Date.parse(String(snap.saved_at || ""));
              var ageMs = isFinite(savedAtMs) ? Math.max(0, Date.now() - savedAtMs) : 0;
              var tone = ageMs > OFFLINE_CATALOG_STALE_MS ? "error" : "warn";
              setCatalogStaleBanner(
                "Offline mode: catalog snapshot loaded (" + formatCatalogAge(ageMs) + ").",
                tone
              );
            }
          });
          fetchCatalogStock();
          startStockPoll();
          if (Date.now() - lastReceiptSeqSyncAt > 30000) {
            syncLocalReceiptSeqFromServerQuiet();
          }
        }, 4000);
        window.refreshPosCatalogStock = fetchCatalogStock;
        window.posApplyCatalogRows = applyCatalogStockRows;
      })();

      (function initShopHeaderDropdown() {
        var wrap = document.querySelector("[data-shop-header-dd]");
        if (!wrap) return;
        var btn = document.getElementById("shop-header-dd-btn");
        var panel = document.getElementById("shop-header-dd-panel");
        var chev = wrap.querySelector("[data-shop-dd-chevron]");
        if (!btn || !panel) return;
        function setOpen(open) {
          if (open) {
            panel.classList.remove("hidden");
            panel.removeAttribute("hidden");
            btn.setAttribute("aria-expanded", "true");
            if (chev) chev.classList.add("rotate-180");
          } else {
            panel.classList.add("hidden");
            panel.setAttribute("hidden", "hidden");
            btn.setAttribute("aria-expanded", "false");
            if (chev) chev.classList.remove("rotate-180");
          }
        }
        btn.addEventListener("click", function (e) {
          e.stopPropagation();
          var on = panel.classList.contains("hidden");
          setOpen(on);
        });
        document.addEventListener("click", function (e) {
          if (!wrap.contains(e.target)) setOpen(false);
        });
        document.addEventListener("keydown", function (e) {
          if (e.key === "Escape") setOpen(false);
        });
      })();

      setSaleType("sale");
      setPaymentMethod("");
      clearAuthorization();
      setCustomerLookupStatus("Enter phone to lookup.", "muted");
      schedulePosIdleWork(function () {
        refreshEmployeeAuthCacheFromServer();
      });
      schedulePosIdleWork(function () {
        syncLocalReceiptSeqFromServerQuiet();
      }, 600);
      window.addEventListener("online", function () {
        refreshEmployeeAuthCacheFromServer();
        syncLocalReceiptSeqFromServerQuiet();
        updateNetworkStateIndicator();
        updateOfflineSyncBadge();
        setAuthStatus("Back online. Syncing offline sales…", "muted");
        // Phase 3 + kitchen refills — kick off the stock-in and portion-refill queue syncs in parallel.
        syncOfflineStockInsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
        syncOfflinePortionRefillsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
        syncOfflineSalesQueue({ notify: false })
          .then(function (s1) {
            return new Promise(function (res) {
              setTimeout(function () {
                res(s1 || {});
              }, 900);
            });
          })
          .then(function (s1) {
            return syncOfflineSalesQueue({ notify: false }).then(function (s2) {
              return mergeOfflineSyncStats(s1 || {}, s2 || {});
            });
          })
          .then(function (merged) {
            merged = merged || { synced: 0, failed: 0, errors: [] };
            if ((merged.synced || 0) > 0 || (merged.failed || 0) > 0) {
              showOfflineSyncResultToast(merged);
            }
            return getPendingOfflineSalesCount().then(function (pending) {
              var didWork = (merged.synced || 0) + (merged.failed || 0) > 0;
              if (didWork && (merged.failed || 0) === 0 && pending === 0) {
                setAuthStatus("Offline sales synced.", "ok");
              } else if ((merged.failed || 0) > 0) {
                setAuthStatus("Some offline sales failed to sync. Open Diagnostics for details.", "error");
              } else if (didWork && pending > 0) {
                setAuthStatus("Still uploading " + String(pending) + " offline sale(s)…", "muted");
              }
              updateOfflineSyncBadge();
            });
          })
          .catch(function () {
            updateOfflineSyncBadge();
          });
      });
      window.addEventListener("offline", function () {
        updateNetworkStateIndicator();
        updateOfflineSyncBadge();
      });
      document.addEventListener("visibilitychange", function () {
        if (document.hidden) return;
        syncOfflineSalesQueue({ notify: false }).then(function (stats) {
          if (stats && !stats.skipped && ((stats.synced || 0) > 0 || (stats.failed || 0) > 0)) {
            showOfflineSyncResultToast(stats);
          }
        });
        syncOfflineStockInsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
        syncOfflinePortionRefillsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
      });
      if (offlineSyncTimer) clearInterval(offlineSyncTimer);
      offlineSyncTimer = setInterval(function () {
        syncOfflineSalesQueue({ notify: false });
        syncOfflineStockInsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
        syncOfflinePortionRefillsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
      }, 20000);
      document.addEventListener(
        "click",
        function (ev) {
          var t = ev.target;
          if (!t || !t.closest) return;
          var btn = t.closest("#pos-network-state-indicator");
          if (!btn) return;
          ev.preventDefault();
          ev.stopPropagation();
          if (typeof window.__openPosTodayOfflineQueue === "function") {
            window.__openPosTodayOfflineQueue();
          }
        },
        true
      );
      document.addEventListener("keydown", function (ev) {
        if (ev.key !== "Escape") return;
        var tom = document.getElementById("pos-today-offline-modal");
        if (tom && !tom.classList.contains("hidden")) tom.classList.add("hidden");
      });
      if (offlineSyncNowBtn) {
        offlineSyncNowBtn.addEventListener("click", function () {
          if (offlineSyncBusy) return;
          setAuthStatus(
            navigator.onLine ? "Syncing offline queue…" : "Trying to sync (connection may be limited)…",
            "muted"
          );
          syncOfflineStockInsQueue().catch(function () {});
          syncOfflinePortionRefillsQueue().catch(function () {});
          syncOfflineSalesQueue({ notify: true }).then(function () {
            return getPendingOfflineSalesCount();
          }).then(function (pending) {
            if (pending === 0) setAuthStatus("Offline sales synced.", "ok");
            else setAuthStatus("Some sales are still pending sync (" + String(pending) + ").", "muted");
            updateOfflineSyncBadge();
          });
        });
      }
      if (offlineSyncDiagBtn) {
        offlineSyncDiagBtn.addEventListener("click", function () {
          var diagModal = ensureOfflineSyncDiagnosticsModal();
          diagModal.classList.remove("hidden");
          refreshOfflineDiagnostics();
          var refreshBtn = document.getElementById("pos-offline-diag-refresh");
          var syncBtn = document.getElementById("pos-offline-diag-sync");
          if (refreshBtn) {
            refreshBtn.onclick = function () {
              refreshOfflineDiagnostics();
            };
          }
          if (syncBtn) {
            syncBtn.onclick = function () {
              setAuthStatus(
                navigator.onLine ? "Syncing offline queue…" : "Trying to sync (connection may be limited)…",
                "muted"
              );
              syncOfflineStockInsQueue().catch(function () {});
              syncOfflinePortionRefillsQueue().catch(function () {});
              syncOfflineSalesQueue({ notify: true }).then(function () {
                return refreshOfflineDiagnostics();
              }).then(function () {
                return getPendingOfflineSalesCount();
              }).then(function (pending) {
                if (pending === 0) setAuthStatus("Offline sales synced.", "ok");
                else setAuthStatus("Some sales are still pending sync (" + String(pending) + ").", "muted");
                updateOfflineSyncBadge();
              });
            };
          }
        });
      }
      updateNetworkStateIndicator();
      updateOfflineSyncBadge();
      syncOfflineSalesQueue({ notify: false });
      syncOfflineStockInsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
      syncOfflinePortionRefillsQueue().then(function () { updateOfflineSyncBadge(); }).catch(function () {});
      window.__openPosTodayOfflineQueue = function () {
        try {
          var qm = ensureTodayOfflineQueueModal();
          qm.classList.remove("hidden");
          refreshTodayOfflineQueueModal();
        } catch (eOpenQ) {}
      };
      render();
      applyPosCartUiSettings();

      /** Fallback if printer script finishes after first paint — compulsory + not ready → open setup. */
      window.addEventListener("load", function () {
        setTimeout(function () {
          if (typeof window.__posEnsureCompulsoryPrinterPromptAfterLoad === "function") {
            window.__posEnsureCompulsoryPrinterPromptAfterLoad();
          } else {
            window.__posCompulsoryPrinterOnLoadPending = true;
          }
        }, 1400);
      });

      window.richcomPosPrinterRunTestPrint = function () {
        return runConfiguredPrinterAction(makePrinterTestReceiptPayload());
      };
      window.__posPrinterConnectionChanged = function () {
        updateCustomerSectionState();
        updatePosCompulsoryPrinterWorkspaceLock();
        updateProceedState();
        try {
          if (typeof window.refreshPosCatalogStock === "function") window.refreshPosCatalogStock();
        } catch (eR) {}
      };

      document.addEventListener("submit", function (ev) {
        var f = ev.target;
        if (f && f.tagName === "FORM" && String(f.method || "").toLowerCase() === "post") {
          window.__posBypassLeaveConfirm = true;
        }
      });

      window.addEventListener("beforeunload", function (e) {
        if (window.__posBypassLeaveConfirm) return;
        e.preventDefault();
        e.returnValue = "";
      });

      /* Printer setup modal is loaded from the next <script> IIFE — same symbols are out of scope there. */
      window.KNOWN_BLE_PRINT_SERVICE_UUIDS = KNOWN_BLE_PRINT_SERVICE_UUIDS;
      window.bluetoothGattConnect = bluetoothGattConnect;
      window.bluetoothGattUserHint = bluetoothGattUserHint;
      window.findWritableEscPosCharacteristic = findWritableEscPosCharacteristic;
    })();