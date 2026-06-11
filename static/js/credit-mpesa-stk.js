/**
 * M-Pesa STK Push for shop / IT Support credit payment forms.
 * Reuses POS Daraja endpoints (/shops/:id/shop-pos/mpesa-stk-*).
 */
(function () {
  "use strict";

  var BOOT = window.CREDIT_MPESA_STK_BOOT || {};
  var POLL_MAX = 90;
  var REF_POLL_MAX = 45;
  var POLL_INTERVAL_MS = 8000;
  var REF_POLL_INTERVAL_MS = 4000;

  function fmt(n) {
    var x = Number(n);
    if (!isFinite(x)) x = 0;
    return x.toFixed(2);
  }

  function lenDigits(s) {
    return String(s || "").replace(/\D/g, "").length;
  }

  function normalizePhone(raw) {
    var d = String(raw || "").replace(/\D/g, "");
    if (!d) return "";
    if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
    if (d.charAt(0) === "0" && d.length >= 10) return "254" + d.slice(1);
    if (d.length === 9) return "254" + d;
    return d;
  }

  function apiHeaders(extra) {
    var h = extra || {};
    try {
      if (String(window.location.hostname || "").toLowerCase().indexOf("ngrok") !== -1) {
        h["ngrok-skip-browser-warning"] = "1";
      }
    } catch (e) {}
    return h;
  }

  function errorFromText(text) {
    var t = String(text || "").trim();
    if (!t) return "Request failed.";
    try {
      var j = JSON.parse(t);
      if (j && j.error) return String(j.error);
    } catch (e) {}
    if (t.indexOf("incapsula") !== -1 || t.indexOf("Incapsula") !== -1) {
      return "Safaricom blocked the request. Wait and retry.";
    }
    return t.length > 240 ? t.slice(0, 240) + "…" : t;
  }

  function darajaEnabled() {
    return !!(BOOT.daraja && BOOT.daraja.enabled);
  }

  function CreditMpesaStk(root) {
    var prefix = String(root.getAttribute("data-stk-prefix") || "credit-stk");
    var customerPhone = String(root.getAttribute("data-customer-phone") || "");
    var form = root.closest("form");
    var amountInput = form ? form.querySelector('input[name="amount"]') : null;
    var noteInput = form ? form.querySelector('input[name="note"]') : null;
    var submitBtn = form ? form.querySelector('button[type="submit"]') : null;

    var elAmount = document.getElementById(prefix + "-amount");
    var elPhone = document.getElementById(prefix + "-phone");
    var elSend = document.getElementById(prefix + "-send");
    var elRetry = document.getElementById(prefix + "-retry");
    var elRetryHint = document.getElementById(prefix + "-retry-hint");
    var elStatus = document.getElementById(prefix + "-status");
    var elSetup = document.getElementById(prefix + "-setup");
    var elPaidCard = document.getElementById(prefix + "-paid-card");
    var elPaidRefLine = document.getElementById(prefix + "-paid-ref-line");
    var elPaidRef = document.getElementById(prefix + "-paid-ref");

    var pollTimer = null;
    var checkoutId = "";
    var sendInFlight = false;
    var paidConfirmed = false;
    var receiptRef = "";
    var pollCount = 0;
    var refPollCount = 0;
    var failed = false;

    function setStatus(msg, tone) {
      if (!elStatus) return;
      elStatus.textContent = String(msg || "");
      elStatus.classList.remove("credit-mpesa-stk__status--ok", "credit-mpesa-stk__status--error");
      if (tone === "ok") elStatus.classList.add("credit-mpesa-stk__status--ok");
      else if (tone === "error") elStatus.classList.add("credit-mpesa-stk__status--error");
    }

    function stopPoll() {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    function pollIntervalMs() {
      if (paidConfirmed && !String(receiptRef || "").trim()) return REF_POLL_INTERVAL_MS;
      return POLL_INTERVAL_MS;
    }

    function restartPollTimer() {
      if (!checkoutId) return;
      stopPoll();
      pollTimer = setInterval(function () {
        pollStatus(checkoutId);
      }, pollIntervalMs());
    }

    function readAmount() {
      var v = amountInput ? parseFloat(amountInput.value) : NaN;
      return isFinite(v) ? v : 0;
    }

    function syncAmountLabel() {
      if (elAmount) elAmount.textContent = fmt(readAmount());
    }

    function readPhoneRaw() {
      if (elPhone && String(elPhone.value || "").trim()) {
        return String(elPhone.value || "").trim();
      }
      return String(customerPhone || "").trim();
    }

    function resolvePhone() {
      return normalizePhone(readPhoneRaw());
    }

    function phoneReady() {
      var p = resolvePhone();
      return !!p && lenDigits(p) >= 9;
    }

    function syncSubmitButton() {
      if (submitBtn) submitBtn.disabled = !!sendInFlight;
    }

    function setButtonsBusy(busy) {
      if (elSend) elSend.disabled = !!busy;
      if (elRetry) elRetry.disabled = !!busy;
      if (elPhone) elPhone.readOnly = !!busy;
      syncSubmitButton();
    }

    function setRetryUi(show) {
      failed = !!show;
      if (elRetryHint) elRetryHint.classList.toggle("hidden", !failed);
      if (elSend) elSend.classList.toggle("hidden", failed);
      if (elRetry) {
        elRetry.classList.toggle("hidden", !failed);
        if (failed) elRetry.disabled = false;
      }
    }

    function hidePaidCard() {
      receiptRef = "";
      if (elPaidRef) elPaidRef.textContent = "";
      if (elPaidRefLine) elPaidRefLine.classList.add("hidden");
      if (elPaidCard) elPaidCard.classList.add("hidden");
    }

    function showPaidCard(ref) {
      receiptRef = String(ref || "").trim();
      if (elPaidCard) elPaidCard.classList.remove("hidden");
      if (elPaidRefLine && elPaidRef) {
        if (receiptRef) {
          elPaidRef.textContent = receiptRef;
          elPaidRefLine.classList.remove("hidden");
        } else {
          elPaidRef.textContent = "";
          elPaidRefLine.classList.add("hidden");
        }
      }
    }

    function extractReceipt(j) {
      if (!j) return "";
      var direct = String(j.mpesa_receipt_number || "").trim();
      if (direct) return direct;
      var meta = (j.status && j.status.metadata) || {};
      return String(meta.MpesaReceiptNumber || meta.mpesa_receipt_number || meta.ReceiptNumber || "").trim();
    }

    function noteFromStk(ref) {
      var r = String(ref || "").trim();
      return r ? "M-Pesa Ref: " + r : "M-Pesa STK";
    }

    function applyNote(ref) {
      if (!noteInput) return;
      noteInput.value = noteFromStk(ref);
    }

    function promptRecordPayment(ref) {
      if (!paidConfirmed) return;
      applyNote(ref);
      var r = String(ref || "").trim();
      setStatus(
        r
          ? "Payment confirmed on M-Pesa. Ref: " + r + " — click Update payment to record."
          : "Payment confirmed on M-Pesa — click Update payment to record.",
        "ok"
      );
      syncSubmitButton();
      if (submitBtn) {
        try {
          submitBtn.focus();
        } catch (eFocus) {}
      }
    }

    function statusStillProcessing(j) {
      if (!j) return false;
      if (j.pending) return true;
      var desc = String((j.status && j.status.result_desc) || "").toLowerCase();
      return (
        desc.indexOf("under processing") !== -1 ||
        desc.indexOf("still being processed") !== -1 ||
        desc.indexOf("still under processing") !== -1 ||
        (desc.indexOf("still") !== -1 && desc.indexOf("process") !== -1) ||
        (desc.indexOf("waiting") !== -1 && desc.indexOf("phone") !== -1)
      );
    }

    function paymentFailed(j) {
      if (!j || !j.status) return false;
      if (j.failed) return true;
      if (!j.status.completed) return false;
      if (statusStillProcessing(j)) return false;
      return String(j.status.result_code) !== "0";
    }

    function prepareRetry() {
      stopPoll();
      checkoutId = "";
      pollCount = 0;
      refPollCount = 0;
      paidConfirmed = false;
      hidePaidCard();
    }

    function pollStatus(cid) {
      var base = (BOOT.apis && BOOT.apis.mpesaStkStatusBase) || "";
      if (!base || !cid) return;

      if (paidConfirmed) {
        refPollCount += 1;
        if (refPollCount > REF_POLL_MAX) {
          stopPoll();
          promptRecordPayment(receiptRef);
          return;
        }
      } else {
        pollCount += 1;
        if (pollCount > POLL_MAX) {
          stopPoll();
          setStatus("Payment check timed out. Try STK Push again or record manually.", "error");
          sendInFlight = false;
          setButtonsBusy(false);
          setRetryUi(true);
          return;
        }
      }

      fetch(base + encodeURIComponent(cid), {
        credentials: "same-origin",
        cache: "no-store",
        headers: apiHeaders({ Accept: "application/json" }),
      })
        .then(function (r) {
          return r.json().catch(function () {
            return {};
          });
        })
        .then(function (j) {
          if (!j || !j.ok || !j.status) return;
          var paid = !!(j.paid || String(j.status.result_code) === "0");
          if (paid) {
            paidConfirmed = true;
            var ref = extractReceipt(j);
            showPaidCard(ref);
            setRetryUi(false);
            sendInFlight = false;
            setButtonsBusy(false);
            if (ref) {
              stopPoll();
              promptRecordPayment(ref);
              return;
            }
            setStatus("Payment confirmed on M-Pesa. Waiting for Safaricom reference…", "ok");
            restartPollTimer();
            return;
          }
          if (paymentFailed(j)) {
            stopPoll();
            setStatus(
              j.status_message || j.status.result_desc || "M-Pesa payment failed or was cancelled.",
              "error"
            );
            sendInFlight = false;
            setButtonsBusy(false);
            setRetryUi(true);
            return;
          }
          if (statusStillProcessing(j)) {
            var waitMsg =
              String((j.status && j.status.result_desc) || "").trim() ||
              "Waiting for customer to complete M-Pesa on phone.";
            setStatus("Live check: " + waitMsg, "muted");
            return;
          }
          setStatus("Checking M-Pesa payment…", "muted");
        })
        .catch(function () {
          if (pollCount >= 3 && !paidConfirmed) {
            stopPoll();
            sendInFlight = false;
            setButtonsBusy(false);
            setStatus("Could not check payment status. Try STK Push again.", "error");
            setRetryUi(true);
          }
        });
    }

    function sendStkPush() {
      if (!darajaEnabled()) return;
      var api = BOOT.apis && BOOT.apis.mpesaStkPush;
      if (!api) {
        setStatus("STK Push API is not configured.", "error");
        return;
      }
      var amount = readAmount();
      var phone = resolvePhone();
      if (amount < 1) {
        setStatus("Enter the payment amount first (at least 1).", "error");
        if (amountInput) amountInput.focus();
        return;
      }
      if (!phoneReady()) {
        setStatus("Enter a valid M-Pesa phone number (e.g. 0712345678).", "error");
        if (elPhone) elPhone.focus();
        return;
      }
      if (sendInFlight) return;
      sendInFlight = true;
      prepareRetry();
      setButtonsBusy(true);
      setStatus("Sending STK Push…", "muted");

      fetch(api, {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
        headers: apiHeaders({
          "Content-Type": "application/json",
          Accept: "application/json",
        }),
        body: JSON.stringify({ phone: phone, amount: amount }),
      })
        .then(function (r) {
          return r.text().then(function (t) {
            var j = {};
            try {
              j = t ? JSON.parse(t) : {};
            } catch (e) {
              j = {};
            }
            if (!r.ok || !j.ok) {
              throw new Error(String((j && j.error) || errorFromText(t) || "STK Push failed"));
            }
            return j;
          });
        })
        .then(function (j) {
          checkoutId = String(j.checkout_request_id || "");
          var msg = String(
            j.customer_message ||
              "STK Push sent. Customer enters M-Pesa PIN on their phone."
          ).trim();
          setStatus(msg, "ok");
          setRetryUi(false);
          if (checkoutId) {
            pollCount = 0;
            refPollCount = 0;
            paidConfirmed = false;
            setStatus("Live check: waiting for M-Pesa payment on phone…", "muted");
            pollStatus(checkoutId);
            restartPollTimer();
          }
          sendInFlight = false;
          setButtonsBusy(false);
        })
        .catch(function (e) {
          setStatus((e && e.message) || "Could not send STK Push.", "error");
          sendInFlight = false;
          setButtonsBusy(false);
          setRetryUi(true);
        });
    }

    function updateUi() {
      syncAmountLabel();
      var ready = darajaEnabled();
      var canSend = ready && readAmount() >= 1 && phoneReady();
      if (elSetup) elSetup.classList.toggle("hidden", ready);
      if (elSend && !failed) {
        elSend.disabled = sendInFlight || !canSend;
        elSend.classList.toggle("opacity-60", !ready);
      }
      if (elRetry && failed) {
        elRetry.disabled = sendInFlight || !ready || !phoneReady();
      }
    }

    if (amountInput) {
      amountInput.addEventListener("input", updateUi);
      amountInput.addEventListener("change", updateUi);
    }
    if (elPhone) {
      elPhone.addEventListener("input", updateUi);
      elPhone.addEventListener("change", updateUi);
    }
    if (elSend) elSend.addEventListener("click", sendStkPush);
    if (elRetry) elRetry.addEventListener("click", sendStkPush);

    updateUi();
  }

  function init() {
    if (!BOOT.apis || !BOOT.apis.mpesaStkPush) return;
    var roots = document.querySelectorAll("[data-credit-mpesa-stk]");
    for (var i = 0; i < roots.length; i++) {
      try {
        CreditMpesaStk(roots[i]);
      } catch (e) {
        console.warn("Credit M-Pesa STK init failed", e);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
