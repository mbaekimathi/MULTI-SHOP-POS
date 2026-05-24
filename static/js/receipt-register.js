/**
 * Receipt register — bulk mark, detail modal, returns (IT Support + shop portal).
 */
(function () {
  var root = document.getElementById("receipt-register-root");
  if (!root) return;

  var CFG = {};
  try {
    var cfgEl = document.getElementById("receipt-register-cfg");
    if (cfgEl && cfgEl.textContent) CFG = JSON.parse(cfgEl.textContent);
  } catch (e) {}

  var tbody = document.getElementById("receipt-register-tbody");
  var selectAll = document.getElementById("receipt-select-all");
  var selectedCount = document.getElementById("receipt-selected-count");
  var liveSearch = document.getElementById("receipt-live-search");
  var markMsg = document.getElementById("receipt-mark-msg");
  var pageMsg = document.getElementById("receipt-page-msg");
  var rangeLabel = document.getElementById("receipt-range-label");
  var loadedCountEl = document.getElementById("receipt-loaded-count");
  if (!tbody) return;

  var selected = new Set();
  var variant = CFG.variant || "hq";

  function showPageMsg(text) {
    if (!pageMsg) return;
    if (text) {
      pageMsg.textContent = text;
      pageMsg.classList.remove("hidden");
    } else {
      pageMsg.textContent = "";
      pageMsg.classList.add("hidden");
    }
  }

  function statusBadgeClass(status) {
    var base = "rcpt-mark-badge receipt-mark-badge ";
    if (status === "confirmed") return base + "rcpt-mark-badge--confirmed";
    if (status === "cancelled") return base + "rcpt-mark-badge--cancelled";
    if (status === "partial_return") return base + "rcpt-mark-badge--partial_return";
    if (status === "returned") return base + "rcpt-mark-badge--returned";
    return base + "rcpt-mark-badge--pending";
  }

  function statusBadgeLabel(status) {
    if (status === "partial_return") return "partial return";
    return status || "pending";
  }

  function updateSelectedUi() {
    if (selectedCount) selectedCount.textContent = String(selected.size);
  }

  function clearLiveSearchForNextInput() {
    if (!liveSearch) return;
    liveSearch.value = "";
    var hiddenQ = document.getElementById("receipt-q-hidden");
    if (hiddenQ) hiddenQ.value = "";
    liveSearch.dispatchEvent(new Event("input", { bubbles: true }));
    window.setTimeout(function () {
      liveSearch.focus({ preventScroll: false });
      var searchBlock = liveSearch.closest(".rcpt-search-field") || liveSearch.closest(".rev-filters-box");
      if (searchBlock && searchBlock.scrollIntoView) {
        searchBlock.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    }, 80);
  }

  function applyRowStatus(saleId, status) {
    var row = tbody.querySelector('tr[data-sale-id="' + saleId + '"]');
    if (!row) return;
    row.setAttribute("data-mark-status", status);
    var badge = row.querySelector(".receipt-mark-badge");
    if (!badge) return;
    badge.className = statusBadgeClass(status);
    badge.setAttribute("data-mark-status", status);
    badge.textContent = statusBadgeLabel(status);
  }

  function escHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function typePill(saleType) {
    if (String(saleType || "").toLowerCase() === "credit") {
      return '<span class="rcpt-type-pill rcpt-type-pill--credit">Credit</span>';
    }
    return '<span class="rcpt-type-pill rcpt-type-pill--sale">Sale</span>';
  }

  function typePaymentCell(row) {
    var pay = String(row.payment_method || "").trim() || "—";
    return (
      '<td class="align-top rcpt-col-type-pay" data-label="Type / Payment" title="' +
      escHtml(pay) +
      '"><div class="rcpt-type-pay">' +
      typePill(row.sale_type) +
      '<div class="rcpt-type-pay__method cell-muted truncate">' +
      escHtml(pay) +
      "</div></div></td>"
    );
  }

  function headerColCount() {
    var table = document.getElementById("receipt-register-table");
    if (!table || !table.tHead || !table.tHead.rows.length) return 0;
    return table.tHead.rows[0].cells.length;
  }

  function rowColCountForData(row) {
    var table = document.createElement("table");
    var tb = document.createElement("tbody");
    table.appendChild(tb);
    tb.innerHTML = buildRowHtml(row);
    var tr = tb.querySelector("tr");
    return tr ? tr.cells.length : 0;
  }

  function buildRowHtml(row) {
    var sid = parseInt(row.sale_id || row.id || 0, 10) || 0;
    var shopId = parseInt(row.shop_id || 0, 10) || 0;
    var recNo = String(row.receipt_number || "").trim();
    var mark = String(row.receipt_mark_status || "pending").toLowerCase();
    var total = Number(row.total_amount || 0).toFixed(2);
    var showShop = !!CFG.showShopColumn;
    var canMark = !!CFG.canMark;

    var cbCell = canMark
      ? '<td class="rev-col-action align-top rcpt-col-sticky rcpt-col-check" data-label="Select"><input type="checkbox" class="receipt-select-one h-4 w-4 rounded border-[rgb(var(--rc-border))]" value="' +
        escHtml(sid) +
        '" /></td>'
      : "";

    var shopCell = showShop
      ? '<td class="align-top min-w-0 rcpt-col-shop" data-label="Shop"><div class="truncate cell-strong">' +
        escHtml(row.shop_name || "#" + shopId) +
        '</div><div class="truncate text-xs text-[rgb(var(--rc-muted))]">' +
        escHtml(row.shop_code || "") +
        "</div></td>"
      : "";

    var shopAttr = showShop ? ' data-shop-id="' + escHtml(shopId) + '"' : "";
    var viewShopAttr = showShop ? ' data-shop-id="' + escHtml(shopId) + '"' : "";

    var cust = String(row.customer_name || "Walk-in").trim() || "Walk-in";
    if (cust.length > 22) cust = cust.slice(0, 21) + "…";

    return (
      '<tr data-sale-id="' +
      escHtml(sid) +
      '"' +
      shopAttr +
      ' data-receipt-no="' +
      escHtml(recNo) +
      '" data-total-amount="' +
      escHtml(total) +
      '" data-mark-status="' +
      escHtml(mark) +
      '" class="rcpt-row">' +
      cbCell +
      '<td class="cell-strong align-top font-mono rcpt-col-sticky rcpt-col-receipt" data-label="Receipt #"><div class="rcpt-receipt-no">' +
      escHtml(recNo || "#" + sid) +
      '</div><span class="' +
      statusBadgeClass(mark) +
      '" data-mark-status="' +
      escHtml(mark) +
      '">' +
      escHtml(statusBadgeLabel(mark)) +
      "</span></td>" +
      shopCell +
      '<td class="num align-top cell-muted rcpt-col-items" data-label="Items">' +
      escHtml(parseInt(row.item_count || 0, 10) || 0) +
      "</td>" +
      '<td class="num align-top rcpt-col-total rcpt-amount" data-label="Total">' +
      escHtml(total) +
      "</td>" +
      '<td class="align-top cell-muted truncate rcpt-col-recorded" data-label="Recorded">' +
      escHtml(row.employee_name || "—") +
      "</td>" +
      typePaymentCell(row) +
      '<td class="align-top cell-muted truncate rcpt-col-customer" data-label="Customer">' +
      escHtml(cust) +
      "</td>" +
      '<td class="rev-col-action align-top rcpt-col-view" data-label="View"><button type="button" class="rev-tx-view-btn receipt-view-btn rcpt-view-btn" data-sale-id="' +
      escHtml(sid) +
      '"' +
      viewShopAttr +
      ' title="View receipt items" aria-label="View receipt items"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/><path stroke-linecap="round" d="M2.458 12C3.732 7.943 7.523 5 12 5c8.268 2.943 9.542 7-1.274 4.057-5.065 7-9.542 7S3.732 16.057 2.458 12z"/></svg></button></td></tr>'
    );
  }

  function renderRows(rows) {
    selected.clear();
    if (selectAll) selectAll.checked = false;
    updateSelectedUi();
    var colspan = CFG.emptyColspan || 7;
    if (!rows || !rows.length) {
      tbody.innerHTML =
        '<tr data-sale-id="__none__"><td colspan="' +
        colspan +
        '" class="rev-empty">No receipts in this date range.</td></tr>';
      if (loadedCountEl) loadedCountEl.textContent = "0";
      return;
    }
    var expectedCols = headerColCount();
    var builtCols = rowColCountForData(rows[0]);
    if (expectedCols > 0 && builtCols > 0 && expectedCols !== builtCols) {
      showPageMsg(
        "Receipt list could not refresh (layout mismatch). Hard-refresh this page (Ctrl+F5) to load the latest scripts."
      );
      return;
    }
    tbody.innerHTML = rows.map(buildRowHtml).join("");
    if (loadedCountEl) loadedCountEl.textContent = String(rows.length);
    if (liveSearch) liveSearch.dispatchEvent(new Event("input", { bubbles: true }));
  }

  if (CFG.canMark) {
    tbody.addEventListener("change", function (e) {
      var target = e.target;
      if (!target || !target.classList || !target.classList.contains("receipt-select-one")) return;
      var id = String(target.value || "");
      if (!id) return;
      if (target.checked) {
        selected.add(id);
        updateSelectedUi();
        clearLiveSearchForNextInput();
      } else {
        selected.delete(id);
        updateSelectedUi();
      }
    });

    if (selectAll) {
      selectAll.addEventListener("change", function () {
        tbody.querySelectorAll("tr[data-sale-id]").forEach(function (row) {
          if (row.getAttribute("data-sale-id") === "__none__" || row.classList.contains("hidden")) return;
          var cb = row.querySelector(".receipt-select-one");
          if (!cb) return;
          cb.checked = !!selectAll.checked;
          var id = String(cb.value || "");
          if (!id) return;
          if (selectAll.checked) selected.add(id);
          else selected.delete(id);
        });
        updateSelectedUi();
      });
    }

    function postBulkMark(status) {
      if (!CFG.markUrl) return;
      if (!selected.size) {
        if (markMsg) markMsg.textContent = "Select at least one receipt.";
        return;
      }
      var ids = Array.from(selected)
        .map(function (x) {
          return parseInt(x, 10);
        })
        .filter(function (n) {
          return !isNaN(n) && n > 0;
        });
      var body = { sale_ids: ids, mark_status: status };
      fetch(CFG.markUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(body),
      })
        .then(function (r) {
          return r.json().catch(function () {
            return {};
          });
        })
        .then(function (j) {
          if (!j || !j.ok) throw new Error((j && j.error) || "Could not update receipts.");
          ids.forEach(function (sid) {
            applyRowStatus(String(sid), status);
          });
          if (markMsg) markMsg.textContent = "Updated " + String(j.updated || ids.length) + " receipt(s) to " + status + ".";
        })
        .catch(function (err) {
          if (markMsg) markMsg.textContent = (err && err.message) ? err.message : "Could not update receipts.";
        });
    }

    document.querySelectorAll("[data-mark-action]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        postBulkMark(String(btn.getAttribute("data-mark-action") || "").toLowerCase());
      });
    });

    updateSelectedUi();
  }

  function ensureReceiptModal() {
    var existing = document.getElementById("receipt-items-modal");
    if (existing) return existing;
    var wrap = document.createElement("div");
    wrap.id = "receipt-items-modal";
    wrap.className = "rcpt-modal hidden";
    wrap.setAttribute("aria-hidden", "true");
    wrap.innerHTML =
      '<div class="rcpt-modal__backdrop" data-rcpt-modal-backdrop></div>' +
      '<div class="rcpt-modal__dialog rev-panel">' +
      '<div class="rev-panel__head"><div class="min-w-0"><p class="rev-panel__eyebrow">Receipt preview</p>' +
      '<h3 id="receipt-items-title" class="rev-panel__title">Receipt items</h3>' +
      '<p id="receipt-items-subtitle" class="rev-panel__desc"></p></div>' +
      '<button type="button" id="receipt-items-close" class="rev-btn rev-btn--ghost">Close</button></div>' +
      '<div id="receipt-items-body" class="rcpt-modal__body"></div>' +
      '<div id="receipt-return-footer" class="rcpt-modal__footer hidden">' +
      '<div class="flex flex-wrap items-center justify-between gap-2">' +
      '<p id="receipt-return-msg" class="text-xs text-[rgb(var(--rc-muted))]"></p>' +
      '<button type="button" id="receipt-return-submit" class="btn-rc btn-rc-primary rounded-lg px-3 py-2 text-xs font-bold uppercase tracking-wider">Return selected items</button>' +
      "</div></div></div>";
    document.body.appendChild(wrap);
    wrap.addEventListener("click", function (e) {
      if (
        e.target === wrap ||
        (e.target && e.target.classList && e.target.classList.contains("rcpt-modal__backdrop"))
      ) {
        wrap.classList.add("hidden");
      }
    });
    var closeBtn = document.getElementById("receipt-items-close");
    if (closeBtn) closeBtn.addEventListener("click", function () { wrap.classList.add("hidden"); });
    return wrap;
  }

  function statusPillHtml(status) {
    var s = String(status || "pending").toLowerCase();
    var cls = statusBadgeClass(s).replace("receipt-mark-badge", "").trim();
    return '<span class="' + escHtml(cls) + ' receipt-mark-badge">' + escHtml(statusBadgeLabel(s)) + "</span>";
  }

  function openReceiptModal(data) {
    var modal = ensureReceiptModal();
    var sale = (data && data.sale) || {};
    var items = (data && data.items) || [];
    var title = document.getElementById("receipt-items-title");
    var subtitle = document.getElementById("receipt-items-subtitle");
    var body = document.getElementById("receipt-items-body");
    var footer = document.getElementById("receipt-return-footer");
    if (title) title.textContent = "Receipt " + String(sale.receipt_number || "#" + String(sale.id || ""));
    if (subtitle) {
      var parts = [];
      if (sale.shop_name) parts.push(String(sale.shop_name));
      if (sale.created_at) parts.push(String(sale.created_at));
      parts.push(String(sale.employee_name || "Unknown"));
      subtitle.textContent = parts.join(" · ");
    }

    var theadReturn = CFG.canMark && CFG.returnUrl
      ? '<th scope="col" class="rev-col-action">Return</th>'
      : "";
    var itemRows = (items || [])
      .map(function (it) {
        var lineId = parseInt(it.line_id || 0, 10) || 0;
        var qty = parseInt(it.qty || 0, 10) || 0;
        var retCell =
          CFG.canMark && CFG.returnUrl
            ? '<td class="rev-col-action"><input type="checkbox" class="receipt-return-line h-4 w-4 rounded border-[rgb(var(--rc-border))]" value="' +
              escHtml(lineId) +
              '"' +
              (qty <= 0 ? " disabled" : "") +
              " /></td>"
            : "";
        return (
          "<tr>" +
          retCell +
          '<td class="cell-strong">' +
          escHtml(it.item_name || "Item") +
          '</td><td class="num cell-muted">' +
          escHtml(qty) +
          '</td><td class="num cell-credit">' +
          escHtml(Number(it.line_total || 0).toFixed(2)) +
          "</td></tr>"
        );
      })
      .join("");

    if (body) {
      if (!items.length) {
        body.innerHTML =
          '<div class="rev-panel"><p class="rev-empty">No items found for this receipt.</p></div>';
      } else {
        body.innerHTML =
          '<div class="mb-3 flex flex-wrap gap-2">' +
          statusPillHtml(sale.receipt_mark_status) +
          '<span class="rcpt-type-pill ' +
          (String(sale.sale_type || "").toLowerCase() === "credit" ? "rcpt-type-pill--credit" : "rcpt-type-pill--sale") +
          '">' +
          escHtml(String(sale.sale_type || "sale")) +
          "</span></div>" +
          '<div class="rev-scroll rev-scroll--hint"><table class="rev-data rev-data--wide"><thead><tr>' +
          theadReturn +
          '<th scope="col">Item</th><th scope="col" class="num">Qty</th><th scope="col" class="num">Total</th></tr></thead><tbody>' +
          itemRows +
          '</tbody></table></div><div class="mt-3 flex justify-between rounded-lg border-2 border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))] px-3 py-2"><span class="text-xs font-bold uppercase tracking-wider text-[rgb(var(--rc-muted))]">Grand total</span><span class="text-base font-black tabular-nums">' +
          escHtml(Number(sale.total_amount || 0).toFixed(2)) +
          "</span></div>";
      }
    }

    if (footer) footer.classList.toggle("hidden", !(CFG.canMark && CFG.returnUrl));

    var submitBtn = document.getElementById("receipt-return-submit");
    var returnMsg = document.getElementById("receipt-return-msg");
    if (CFG.canMark && CFG.returnUrl && submitBtn && returnMsg) {
      returnMsg.textContent = "Select item lines to return.";
      submitBtn.disabled = false;
      submitBtn.onclick = function () {
        var picks = Array.prototype.slice
          .call(document.querySelectorAll(".receipt-return-line:checked"))
          .map(function (cb) {
            return parseInt(cb.value || "0", 10) || 0;
          })
          .filter(function (n) {
            return n > 0;
          });
        if (!picks.length) {
          returnMsg.textContent = "Select at least one item.";
          return;
        }
        submitBtn.disabled = true;
        returnMsg.textContent = "Processing return…";
        var payload = { sale_id: sale.id, line_ids: picks };
        if (variant === "hq") payload.shop_id = sale.shop_id;
        fetch(CFG.returnUrl, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(payload),
        })
          .then(function (r) {
            return r.json().catch(function () {
              return {};
            });
          })
          .then(function (j) {
            if (!j || !j.ok) throw new Error((j && j.error) || "Could not process return.");
            returnMsg.textContent =
              "Returned " +
              String((j.meta && j.meta.returned_lines) || picks.length) +
              " line(s). New total: " +
              String((j.meta && Number(j.meta.new_total_amount || 0).toFixed(2)) || "0.00");
            setTimeout(function () {
              window.location.reload();
            }, 600);
          })
          .catch(function (e) {
            returnMsg.textContent = e && e.message ? e.message : "Could not process return.";
            submitBtn.disabled = false;
          });
      };
    }

    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
  }

  tbody.addEventListener("click", function (e) {
    var btn = e.target && e.target.closest ? e.target.closest(".receipt-view-btn") : null;
    if (!btn || !CFG.detailUrl) return;
    var saleId = parseInt(String(btn.getAttribute("data-sale-id") || "0"), 10) || 0;
    var shopId = parseInt(String(btn.getAttribute("data-shop-id") || "0"), 10) || 0;
    if (!saleId) return;
    var url = CFG.detailUrl + "?sale_id=" + encodeURIComponent(saleId);
    if (variant === "hq" && shopId) url += "&shop_id=" + encodeURIComponent(shopId);
    fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (r) {
        return r.json().catch(function () {
          return {};
        });
      })
      .then(function (j) {
        if (!j || !j.ok) throw new Error((j && j.error) || "Could not load receipt.");
        openReceiptModal(j);
      })
      .catch(function (err) {
        var target = markMsg || pageMsg;
        if (target) target.textContent = (err && err.message) ? err.message : "Could not load receipt.";
      });
  });

  if (CFG.listUrl && variant === "shop") {
    function fetchReceiptsForShop() {
      showPageMsg("");
      var params = new URLSearchParams(window.location.search || "");
      params.set("limit", "5000");
      var url = CFG.listUrl + (CFG.listUrl.indexOf("?") >= 0 ? "&" : "?") + params.toString();
      fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
        .then(function (r) {
          return r.json().catch(function () {
            return {};
          });
        })
        .then(function (j) {
          if (!j || !j.ok) throw new Error((j && j.error) || "Could not load receipts.");
          if (rangeLabel && j.range_label) rangeLabel.textContent = j.range_label;
          renderRows(j.rows || []);
        })
        .catch(function (err) {
          showPageMsg((err && err.message ? err.message : "Could not load receipts.") + " Showing server-rendered rows if any.");
        });
    }
    fetchReceiptsForShop();
  }
})();
