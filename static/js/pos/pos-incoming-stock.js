(function () {
  var BOOT = window.__POS_BOOT || {};
  if (!BOOT.apis || !BOOT.apis.canReviewIncomingStock) return;
  var POS_SID = BOOT.shopId;
  var API = BOOT.apis.incomingStockRequests;
  var LS_KEY = "pos_incoming_sr_sig_shop_" + POS_SID;
  var SNOOZE_KEY = "pos_incoming_sr_snooze_shop_" + POS_SID;
  var SNOOZE_MS = 5 * 60 * 1000;

  var modal = document.getElementById("pos-incoming-sr-modal");
  var detailEl = document.getElementById("pos-incoming-sr-detail");
  var queueHint = document.getElementById("pos-incoming-sr-queue-hint");
  var qtyEl = document.getElementById("pos-incoming-sr-qty");
  var qtyHint = document.getElementById("pos-incoming-sr-qty-hint");
  var codeEl = document.getElementById("pos-incoming-sr-code");
  var deliveredByEl = document.getElementById("pos-incoming-sr-delivered-by");
  var errEl = document.getElementById("pos-incoming-sr-err");
  var approveBtn = document.getElementById("pos-incoming-sr-approve");
  var declineBtn = document.getElementById("pos-incoming-sr-decline");
  var dismissBtn = document.getElementById("pos-incoming-sr-dismiss");
  var backdrop = document.getElementById("pos-incoming-sr-backdrop");
  var actionsEl = document.getElementById("pos-incoming-sr-actions");
  var stepChoice = document.getElementById("pos-incoming-sr-step-choice");
  var stepCode = document.getElementById("pos-incoming-sr-step-code");
  var approveFields = document.getElementById("pos-incoming-sr-approve-fields");
  var deliveredWrap = document.getElementById("pos-incoming-sr-delivered-wrap");

  /** null until user picks Accept/Decline; then "approve" or "reject". */
  var incomingSrIntent = null;

  var alertSoundTimer = null;
  var alertAudioCtx = null;

  function reviewUrl(requestId) {
    return "/shops/" + POS_SID + "/shop-pos/incoming-stock-requests/" + requestId + "/review";
  }

  function signatureFor(ids) {
    return ids
      .slice()
      .sort(function (a, b) {
        return a - b;
      })
      .join(",");
  }

  function isSnoozed() {
    try {
      var until = parseInt(localStorage.getItem(SNOOZE_KEY) || "0", 10) || 0;
      return until > Date.now();
    } catch (e) {
      return false;
    }
  }

  function setSnooze() {
    try {
      localStorage.setItem(SNOOZE_KEY, String(Date.now() + SNOOZE_MS));
    } catch (e) {}
  }

  function clearSnooze() {
    try {
      localStorage.removeItem(SNOOZE_KEY);
    } catch (e) {}
  }

  function playSoftChime() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      if (!alertAudioCtx) alertAudioCtx = new Ctx();
      if (alertAudioCtx.state === "suspended") {
        alertAudioCtx.resume().catch(function () {});
      }
      var ctx = alertAudioCtx;
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.type = "sine";
      o.frequency.value = 784;
      g.gain.value = 0.035;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      o.stop(ctx.currentTime + 0.1);
    } catch (e) {}
  }

  function startAlertSound() {
    stopAlertSound();
    playSoftChime();
    alertSoundTimer = setInterval(playSoftChime, 2200);
  }

  function stopAlertSound() {
    if (alertSoundTimer) {
      clearInterval(alertSoundTimer);
      alertSoundTimer = null;
    }
  }

  function setOpen(on, opts) {
    opts = opts || {};
    if (!modal) return;
    if (on) {
      modal.classList.remove("hidden");
      modal.setAttribute("aria-hidden", "false");
      if (!opts.silent) startAlertSound();
    } else {
      modal.classList.add("hidden");
      modal.setAttribute("aria-hidden", "true");
      stopAlertSound();
    }
  }

  function showErr(msg) {
    if (!errEl) return;
    errEl.textContent = msg || "";
    errEl.classList.toggle("hidden", !msg);
  }

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
  }

  function showApprovalToast(msg) {
    var toast = document.getElementById("pos-toast");
    if (!toast) return;
    toast.textContent = msg || "Approved.";
    toast.classList.remove("hidden");
    setTimeout(function () {
      toast.classList.add("hidden");
    }, 2800);
  }

  function updateBellCountAfterApproval() {
    document.querySelectorAll("[data-rc-notif-bell]").forEach(function (bell) {
      var c = parseInt(bell.getAttribute("data-rc-notif-count") || "0", 10) || 0;
      var next = Math.max(0, c - 1);
      bell.setAttribute("data-rc-notif-count", String(next));
      var badge = bell.querySelector("span.rounded-full, span.absolute");
      if (!badge) return;
      if (next > 0) badge.textContent = next < 100 ? String(next) : "99+";
      else badge.remove();
    });
  }

  function buildTransferReceiptPayload(summary) {
    if (!summary) return null;
    var site = BOOT.site || window.POS_SITE || {};
    var rs = window.POS_RECEIPT_SETTINGS || {};
    var qty = Number(summary.qty || 0);
    var itemName = summary.item_name || "Item";
    var transferTo = summary.transfer_to || "Receiving shop";
    var transferFrom = summary.transfer_from || site.shop_name || "Shop";
    return {
      receiptNo: "TR-" + String(summary.request_id || summary.tx_id || ""),
      printedAt: new Date().toLocaleString(),
      shopName: site.shop_name || "Shop",
      companyName: site.company_name || "",
      shopCode: (site.shop_code || "").trim(),
      shopLocation: (site.shop_location || "").trim(),
      receiptLogoUrl: (site.receipt_logo_url || site.app_icon_url || "").trim(),
      mode: "Stock transfer",
      isQuotation: false,
      transferFromShop: transferFrom,
      transferToShop: transferTo,
      customerName: transferTo,
      customerPhone: "-",
      employeeName: summary.served_by || "—",
      employeeCode: "",
      deliveredBy: summary.delivered_by || "",
      lines: [
        {
          id: 0,
          name: itemName,
          qty: qty,
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
      receiptHeader: (rs.receipt_header || "").trim(),
      receiptFooter: (rs.receipt_footer || "").trim(),
      creditDueDate: "",
    };
  }

  function printTransferReceiptAfterApprove(resp) {
    var payload = resp.receipt ? buildTransferReceiptPayload(resp.receipt) : null;
    if (!payload || typeof window.__posPrintReceiptLikeCheckout !== "function") {
      return Promise.resolve(false);
    }
    return window.__posPrintReceiptLikeCheckout(payload).then(function () {
      return true;
    });
  }

  function syncIncomingSrQueueBackground() {
    fetch(API, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (res) {
        return res.json();
      })
      .then(function (data) {
        if (!data || !data.ok) return;
        var reqs = data.requests || [];
        if (!reqs.length) {
          window.__posIncomingSrCurrent = null;
          window.__posIncomingSrLastSig = "";
          try {
            localStorage.removeItem(LS_KEY);
          } catch (e) {}
          return;
        }
        var sig = signatureFor(
          reqs.map(function (x) {
            return x.id;
          })
        );
        window.__posIncomingSrLastSig = sig;
        try {
          localStorage.setItem(LS_KEY, sig);
        } catch (e) {}
      })
      .catch(function () {});
  }

  function handleApproveSuccess(resp) {
    clearSnooze();
    try {
      localStorage.removeItem(LS_KEY);
    } catch (e) {}
    window.__posIncomingSrLastSig = "";
    window.__posIncomingSrCurrent = null;
    showErr("");
    setOpen(false);
    printTransferReceiptAfterApprove(resp).then(function (printed) {
      showApprovalToast(
        printed ? "Approved — printing transfer receipt." : "Request approved."
      );
      updateBellCountAfterApproval();
      if (codeEl) codeEl.value = "";
      if (deliveredByEl) deliveredByEl.value = "";
      syncIncomingSrQueueBackground();
    });
  }

  function resetReviewSteps() {
    incomingSrIntent = null;
    if (stepChoice) stepChoice.classList.remove("hidden");
    if (stepCode) stepCode.classList.add("hidden");
    if (approveFields) approveFields.classList.add("hidden");
    if (deliveredWrap) deliveredWrap.classList.add("hidden");
    if (codeEl) codeEl.value = "";
    if (deliveredByEl) deliveredByEl.value = "";
    showErr("");
  }

  function showReviewCodeStep(action) {
    incomingSrIntent = action;
    if (stepChoice) stepChoice.classList.add("hidden");
    if (stepCode) stepCode.classList.remove("hidden");
    var isApprove = action === "approve";
    if (approveFields) approveFields.classList.toggle("hidden", !isApprove);
    if (deliveredWrap) deliveredWrap.classList.toggle("hidden", !isApprove);
    showErr("");
    try {
      if (codeEl) codeEl.focus();
    } catch (e) {}
  }

  function setActionsVisible(on) {
    if (actionsEl) actionsEl.classList.toggle("hidden", !on);
  }

  function showEmptyState() {
    window.__posIncomingSrCurrent = null;
    resetReviewSteps();
    if (detailEl) {
      detailEl.innerHTML =
        '<p class="text-sm font-medium text-[rgb(var(--rc-muted))]">No requests awaiting your approval.</p>' +
        '<p class="mt-1 text-xs text-[rgb(var(--rc-muted))]">Incoming stock requests directed to this shop will appear here.</p>';
    }
    if (queueHint) queueHint.textContent = "You are up to date.";
    setActionsVisible(false);
    showErr("");
    if (codeEl) codeEl.value = "";
    if (deliveredByEl) deliveredByEl.value = "";
  }

  function applyDetail(r, total) {
    if (!detailEl) return;
    setActionsVisible(true);
    resetReviewSteps();
    window.__posIncomingSrCurrent = r;
    detailEl.innerHTML = "";
    var p1 = document.createElement("p");
    p1.className = "leading-snug";
    p1.innerHTML =
      '<span class="font-semibold">#' +
      esc(r.id) +
      "</span> · " +
      esc(r.requesting_shop_name || "Shop") +
      ' requests <span class="font-semibold">' +
      esc(r.item_name || "Item") +
      '</span> <span class="tabular-nums">× ' +
      esc(r.qty || 0) +
      "</span>";
    detailEl.appendChild(p1);
    if (r.note) {
      var pn = document.createElement("p");
      pn.className = "mt-2 text-[11px] text-[rgb(var(--rc-muted))]";
      pn.textContent = r.note;
      detailEl.appendChild(pn);
    }
    var rq = Number(r.qty);
    if (!isFinite(rq) || rq <= 0) rq = 0;
    var mx = typeof r.max_approve_qty === "number" ? r.max_approve_qty : 0;
    var src = typeof r.source_shop_stock_qty === "number" ? r.source_shop_stock_qty : 0;
    var qtyEps = 1e-9;
    if (queueHint) {
      queueHint.textContent =
        total > 1
          ? total + " pending — oldest first. Handle one at a time."
          : "Choose Accept or Decline, then enter your 6-digit code.";
    }
    if (qtyHint) {
      qtyHint.textContent =
        "Stock at this shop for this item: " +
        src +
        ". You may approve up to " +
        (mx > 0 ? Math.min(rq, mx) : 0) +
        " (requested " +
        rq +
        ").";
    }
    var cap = mx > qtyEps ? Math.min(rq, mx) : 0;
    if (!(cap > qtyEps)) {
      qtyEl.disabled = true;
      qtyEl.value = "";
      if (approveBtn) approveBtn.disabled = true;
      if (qtyHint) {
        qtyHint.textContent =
          "No stock available here to transfer. Decline the request or restock first.";
      }
    } else {
      qtyEl.disabled = false;
      qtyEl.min = "0.0001";
      qtyEl.step = "any";
      qtyEl.max = cap;
      qtyEl.value = String(cap);
      if (approveBtn) approveBtn.disabled = false;
    }
    if (deliveredByEl) deliveredByEl.value = "";
  }

  function setBusy(on) {
    window.__posIncomingSrSubmitting = !!on;
    [approveBtn, declineBtn, dismissBtn].forEach(function (b) {
      if (b) b.disabled = !!on;
    });
    if (qtyEl && !qtyEl.disabled) qtyEl.disabled = !!on;
    if (codeEl) codeEl.disabled = !!on;
    if (deliveredByEl) deliveredByEl.disabled = !!on;
  }

  function handleRequestsResponse(reqs, opts) {
    opts = opts || {};
    var force = !!opts.force;
    var allowEmpty = !!opts.allowEmpty;
    if (!reqs.length) {
      if (allowEmpty) {
        showEmptyState();
        setOpen(true, { silent: !!opts.silent });
        return;
      }
      clearSnooze();
      setOpen(false);
      showErr("");
      return;
    }
    if (!force && isSnoozed()) {
      setOpen(false);
      return;
    }
    var ids = reqs.map(function (x) {
      return x.id;
    });
    var sig = signatureFor(ids);
    var last = "";
    try {
      last = localStorage.getItem(LS_KEY) || "";
    } catch (e) {}
    if (!force && sig === last) return;
    window.__posIncomingSrLastSig = sig;
    applyDetail(reqs[0], reqs.length);
    showErr("");
    if (codeEl) codeEl.value = "";
    setOpen(true, { silent: !!opts.silent });
  }

  function poll(force) {
    if (!modal || !detailEl || !qtyEl || !codeEl) return;
    fetch(API, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (res) {
        return res.json();
      })
      .then(function (data) {
        if (!data || !data.ok) return;
        handleRequestsResponse(data.requests || [], { force: !!force, allowEmpty: false });
      })
      .catch(function () {});
  }

  function openApprovalPopupFromBell() {
    if (!modal) return;
    clearSnooze();
    fetch(API, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (res) {
        return res.json();
      })
      .then(function (data) {
        if (!data || !data.ok) return;
        handleRequestsResponse(data.requests || [], { force: true, allowEmpty: true, silent: true });
      })
      .catch(function () {});
  }

  window.__posOpenIncomingStockApprovalPopup = openApprovalPopupFromBell;

  function submitReview(action) {
    if (window.__posIncomingSrSubmitting) return;
    if (!codeEl || !qtyEl) return;
    showErr("");
    var r = window.__posIncomingSrCurrent;
    if (!r) return;
    if (!incomingSrIntent) {
      showErr("Choose Accept or Decline first.");
      return;
    }
    action = action || incomingSrIntent;
    var code = (codeEl && codeEl.value ? codeEl.value : "").trim();
    if (!/^\d{6}$/.test(code)) {
      showErr("Enter your 6-digit employee code.");
      return;
    }
    var body = { action: action, employee_code: code };
    if (action === "approve") {
      var qEps = 1e-9;
      var rawQ = String(qtyEl.value || "").replace(",", ".").trim();
      var q = parseFloat(rawQ);
      if (!isFinite(q) || !(q > qEps)) {
        showErr("Enter a valid quantity greater than zero.");
        return;
      }
      var cap = typeof r.max_approve_qty === "number" ? r.max_approve_qty : 0;
      var rq = Number(r.qty);
      if (!isFinite(rq) || rq <= 0) rq = 0;
      if (q - rq > qEps) {
        showErr("Quantity cannot exceed the requested amount (" + rq + ").");
        return;
      }
      if (cap > qEps && q - cap > qEps) {
        showErr("Quantity cannot exceed available stock here (" + cap + ").");
        return;
      }
      body.qty = q;
      body.delivered_by = deliveredByEl ? String(deliveredByEl.value || "").trim().slice(0, 120) : "";
    }
    setBusy(true);
    stopAlertSound();
    fetch(reviewUrl(r.id), {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body),
    })
      .then(function (res) {
        return res.json().then(function (j) {
          return { ok: res.ok, j: j };
        });
      })
      .then(function (x) {
        setBusy(false);
        if (!x.ok || !x.j || !x.j.ok) {
          showErr((x.j && x.j.error) || "Could not update this request.");
          return;
        }
        if (action === "approve") {
          handleApproveSuccess(x.j);
          return;
        }
        clearSnooze();
        try {
          localStorage.removeItem(LS_KEY);
        } catch (e) {}
        window.__posIncomingSrLastSig = "";
        window.__posIncomingSrCurrent = null;
        incomingSrIntent = null;
        showErr("");
        if (codeEl) codeEl.value = "";
        if (deliveredByEl) deliveredByEl.value = "";
        setOpen(false);
        showApprovalToast(action === "reject" ? "Request declined." : "Request updated.");
        syncIncomingSrQueueBackground();
      })
      .catch(function () {
        setBusy(false);
        showErr("Network error. Try again.");
      });
  }

  approveBtn &&
    approveBtn.addEventListener("click", function () {
      if (approveBtn.disabled) return;
      showReviewCodeStep("approve");
    });
  declineBtn &&
    declineBtn.addEventListener("click", function () {
      showReviewCodeStep("reject");
    });

  codeEl &&
    codeEl.addEventListener("input", function () {
      var raw = (codeEl.value || "").replace(/\D/g, "").slice(0, 6);
      if (codeEl.value !== raw) codeEl.value = raw;
      showErr("");
      if (raw.length !== 6 || !/^\d{6}$/.test(raw)) return;
      if (window.__posIncomingSrSubmitting) return;
      if (!incomingSrIntent) {
        showErr("Choose Accept or Decline first.");
        return;
      }
      var action = incomingSrIntent === "reject" ? "reject" : "approve";
      if (action === "approve") {
        if (approveBtn && approveBtn.disabled) return;
        if (qtyEl && qtyEl.disabled) return;
      }
      submitReview(action);
    });

  dismissBtn &&
    dismissBtn.addEventListener("click", function () {
      if (window.__posIncomingSrCurrent) setSnooze();
      setOpen(false);
    });

  document.querySelectorAll("[data-rc-notif-popup]").forEach(function (bell) {
    bell.addEventListener("click", function (ev) {
      if (ev && ev.preventDefault) ev.preventDefault();
      openApprovalPopupFromBell();
    });
  });

  backdrop &&
    backdrop.addEventListener("click", function () {
      if (dismissBtn) dismissBtn.click();
    });

  document.addEventListener("keydown", function (e) {
    if (e.key !== "Escape") return;
    if (!modal || modal.classList.contains("hidden")) return;
    if (dismissBtn) dismissBtn.click();
  });

  setTimeout(function () {
    poll(false);
  }, 1200);
  setInterval(function () {
    poll(false);
  }, 25000);
})();
