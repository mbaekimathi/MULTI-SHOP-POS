/**
 * Build and open M-Pesa credit pay links (button UI — no long URL on page).
 */
(function () {
  "use strict";

  function readBoot() {
    var el = document.getElementById("credit-note-whatsapp-data");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "{}");
    } catch (e) {
      return null;
    }
  }

  function normalizeShareUrl(raw) {
    var u = String(raw || "").trim();
    if (!u) return "";
    if (!/^https?:\/\//i.test(u)) {
      u = "https://" + u.replace(/^\/+/, "");
    }
    return u;
  }

  function payAmountFromBoot(data) {
    if (!data) return 0;
    var saleBal = Number(data.focusSaleBalance);
    if (parseInt(data.focusSaleId, 10) > 0 && isFinite(saleBal) && saleBal > 0.009) {
      return saleBal;
    }
    var suggested = Number(data.suggestedAmount);
    if (isFinite(suggested) && suggested > 0.009) return suggested;
    return Number(data.balanceDue) || 0;
  }

  function readSharePhone() {
    var input = document.querySelector("[data-credit-note-wa-phone]");
    if (input && String(input.value || "").trim()) {
      return String(input.value || "").trim();
    }
    var data = readBoot();
    return data ? String(data.customerPhone || "").trim() : "";
  }

  function buildPayLink(base, amount, phone) {
    var url = normalizeShareUrl(base);
    if (!url) return "";
    var params = [];
    if (isFinite(amount) && amount > 0.009) {
      params.push("amount=" + encodeURIComponent(Number(amount).toFixed(2)));
    }
    var p = String(phone || "").trim();
    if (p && p !== "-") {
      params.push("phone=" + encodeURIComponent(p));
    }
    return params.length ? url + "?" + params.join("&") : url;
  }

  function resolvePayLink(card) {
    var actions = card ? card.querySelector("[data-pay-link-base]") : null;
    var base = actions ? actions.getAttribute("data-pay-link-base") : "";
    if (!base) {
      var data = readBoot();
      base = data ? data.payLinkBase : "";
    }
    var amount = 0;
    if (actions && actions.getAttribute("data-pay-balance")) {
      amount = Number(actions.getAttribute("data-pay-balance"));
    }
    if (!isFinite(amount) || amount <= 0.009) {
      amount = payAmountFromBoot(readBoot());
    }
    return buildPayLink(base, amount, readSharePhone());
  }

  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      try {
        var ta = document.createElement("textarea");
        ta.value = text;
        ta.setAttribute("readonly", "");
        ta.style.position = "fixed";
        ta.style.left = "-9999px";
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        ta.remove();
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

  function bindCard(card) {
    var openBtn = card.querySelector("[data-credit-mpesa-pay-open]");
    var copyBtn = card.querySelector("[data-credit-mpesa-pay-copy]");

    if (openBtn) {
      openBtn.addEventListener("click", function (e) {
        e.preventDefault();
        var url = resolvePayLink(card);
        if (!url) {
          window.alert("M-Pesa payment link is not available.");
          return;
        }
        if (/localhost|127\.0\.0\.1/i.test(url)) {
          var ok = window.confirm(
            "This link uses localhost and will not work on the customer's phone.\n\nOpen anyway?"
          );
          if (!ok) return;
        }
        window.open(url, "_blank", "noopener,noreferrer");
      });
    }

    if (copyBtn) {
      copyBtn.addEventListener("click", function () {
        var url = resolvePayLink(card);
        if (!url) {
          window.alert("M-Pesa payment link is not available.");
          return;
        }
        var label = "Copy link";
        copyText(url)
          .then(function () {
            copyBtn.textContent = "Copied";
            setTimeout(function () {
              copyBtn.textContent = label;
            }, 2000);
          })
          .catch(function () {
            window.prompt("Copy this payment link:", url);
          });
      });
    }
  }

  function bind() {
    document.querySelectorAll("[data-credit-mpesa-pay-card]").forEach(bindCard);
  }

  window.CreditMpesaPayLink = {
    readBoot: readBoot,
    buildPayLink: buildPayLink,
    payAmountFromBoot: payAmountFromBoot,
    readSharePhone: readSharePhone,
    resolvePayLink: function () {
      var card = document.querySelector("[data-credit-mpesa-pay-card]");
      return resolvePayLink(card);
    },
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
