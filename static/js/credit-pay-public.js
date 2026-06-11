/**
 * Public credit payment page — M-Pesa STK Push via signed link.
 */
(function () {
  "use strict";

  var POLL_MS = 5000;
  var POLL_MAX = 90;

  function readBoot() {
    var el = document.getElementById("credit-pay-boot");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "{}");
    } catch (e) {
      return null;
    }
  }

  function setStatus(msg, tone) {
    var el = document.getElementById("credit-pay-status");
    if (!el) return;
    el.textContent = String(msg || "");
    el.classList.remove("credit-pay-status--ok", "credit-pay-status--error");
    if (tone === "ok") el.classList.add("credit-pay-status--ok");
    else if (tone === "error") el.classList.add("credit-pay-status--error");
  }

  function showSuccess(msg, ref) {
    var box = document.getElementById("credit-pay-success");
    var msgEl = document.getElementById("credit-pay-success-msg");
    var refEl = document.getElementById("credit-pay-success-ref");
    var form = document.getElementById("credit-pay-form");
    if (form) form.classList.add("hidden");
    if (box) box.classList.remove("hidden");
    if (msgEl) msgEl.textContent = msg || "Thank you — your payment was recorded.";
    if (refEl) {
      if (ref) {
        refEl.textContent = "M-Pesa ref: " + ref;
        refEl.classList.remove("hidden");
      } else {
        refEl.classList.add("hidden");
      }
    }
    setStatus("", "muted");
  }

  function lenDigits(s) {
    return String(s || "").replace(/\D/g, "").length;
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

  function init() {
    var boot = readBoot();
    var form = document.getElementById("credit-pay-form");
    var submitBtn = document.getElementById("credit-pay-submit");
    var amountEl = document.getElementById("credit-pay-amount");
    var phoneEl = document.getElementById("credit-pay-phone");
    if (!boot || !form || !submitBtn) return;

    var pollTimer = null;
    var pollCount = 0;
    var checkoutId = "";
    var recording = false;

    function stopPoll() {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    function recordPayment(cid, ref) {
      if (recording || !boot.apis || !boot.apis.record) return;
      recording = true;
      setStatus("Recording your payment…", "muted");
      fetch(boot.apis.record, {
        method: "POST",
        headers: apiHeaders({ "Content-Type": "application/json", Accept: "application/json" }),
        body: JSON.stringify({ checkout_request_id: cid }),
      })
        .then(function (r) {
          return r.json().catch(function () {
            return {};
          });
        })
        .then(function (j) {
          recording = false;
          submitBtn.disabled = false;
          if (!j || !j.ok) {
            setStatus((j && j.error) || "Payment confirmed but could not record. Contact the shop.", "error");
            return;
          }
          showSuccess(j.message || "Payment recorded. Thank you!", ref || j.mpesa_receipt_number);
        })
        .catch(function () {
          recording = false;
          submitBtn.disabled = false;
          setStatus("Payment confirmed but recording failed. Contact the shop with your M-Pesa message.", "error");
        });
    }

    function pollStatus(cid) {
      var base = boot.apis && boot.apis.stkStatusBase;
      if (!base || !cid) return;
      pollCount += 1;
      if (pollCount > POLL_MAX) {
        stopPoll();
        setStatus("Payment check timed out. If you paid, contact the shop.", "error");
        submitBtn.disabled = false;
        return;
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
          if (!j || !j.ok) return;
          if (j.recorded) {
            stopPoll();
            showSuccess("Payment already recorded. Thank you!", j.mpesa_receipt_number);
            return;
          }
          if (j.paid) {
            stopPoll();
            recordPayment(cid, j.mpesa_receipt_number);
            return;
          }
          if (j.failed) {
            stopPoll();
            setStatus(
              (j.status && j.status.result_desc) || "Payment failed or was cancelled.",
              "error"
            );
            submitBtn.disabled = false;
            return;
          }
          setStatus("Waiting for M-Pesa on your phone…", "muted");
        })
        .catch(function () {
          if (pollCount >= 3) {
            setStatus("Could not check payment status. Try again.", "error");
          }
        });
    }

    form.addEventListener("submit", function (e) {
      e.preventDefault();
      if (!boot.apis || !boot.apis.stkPush) return;

      var phone = String((phoneEl && phoneEl.value) || "").trim();
      var amount = parseFloat((amountEl && amountEl.value) || "0");
      var maxAmt = Number(boot.maxAmount) || 0;

      if (lenDigits(phone) < 9) {
        setStatus("Enter a valid M-Pesa phone number.", "error");
        if (phoneEl) phoneEl.focus();
        return;
      }
      if (!isFinite(amount) || amount < 1) {
        setStatus("Enter an amount of at least 1.", "error");
        if (amountEl) amountEl.focus();
        return;
      }
      if (amount > maxAmt + 0.02) {
        setStatus("Amount cannot exceed KES " + maxAmt.toFixed(2) + " on this link.", "error");
        return;
      }

      stopPoll();
      pollCount = 0;
      checkoutId = "";
      submitBtn.disabled = true;
      setStatus("Sending M-Pesa prompt to your phone…", "muted");

      fetch(boot.apis.stkPush, {
        method: "POST",
        headers: apiHeaders({ "Content-Type": "application/json", Accept: "application/json" }),
        body: JSON.stringify({ phone: phone, amount: amount }),
      })
        .then(function (r) {
          return r.text().then(function (t) {
            var j = {};
            try {
              j = t ? JSON.parse(t) : {};
            } catch (eParse) {
              j = {};
            }
            if (!r.ok || !j.ok) {
              throw new Error((j && j.error) || "Could not send M-Pesa prompt.");
            }
            return j;
          });
        })
        .then(function (j) {
          checkoutId = String(j.checkout_request_id || "");
          setStatus(
            String(j.customer_message || "Check your phone and enter your M-Pesa PIN."),
            "ok"
          );
          if (checkoutId) {
            pollStatus(checkoutId);
            pollTimer = setInterval(function () {
              pollStatus(checkoutId);
            }, POLL_MS);
          } else {
            submitBtn.disabled = false;
          }
        })
        .catch(function (err) {
          setStatus((err && err.message) || "Could not send M-Pesa prompt.", "error");
          submitBtn.disabled = false;
        });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
