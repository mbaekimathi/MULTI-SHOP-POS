(function () {
  function qs(sel, root) {
    return (root || document).querySelector(sel);
  }
  function qsa(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  function bindCompanyExpensePayments() {
    var modal = qs("#company-expense-pay-modal");
    var form = qs("#company-expense-pay-form");
    var apiTemplate = window.__COMPANY_EXPENSE_PAYMENT_API || "";
    if (!modal || !form || !apiTemplate) return;

    var shopIdEl = qs("#company-expense-pay-shop-id");
    var idEl = qs("#company-expense-pay-id");
    var nameEl = qs("#company-expense-pay-name");
    var totalEl = qs("#company-expense-pay-total");
    var paidEl = qs("#company-expense-pay-paid");
    var balanceEl = qs("#company-expense-pay-balance");
    var amountEl = qs("#company-expense-pay-amount");
    var msgEl = qs("#company-expense-pay-msg");
    var closeBtn = qs("#company-expense-pay-close");
    var cancelBtn = qs("#company-expense-pay-cancel");
    var fullBtn = qs("#company-expense-pay-full");
    var backdrop = modal.querySelector(".exp-pay-modal-backdrop");
    var currentBalance = 0;

    function fmt(n) {
      return Number(n || 0).toFixed(2);
    }
    function paymentApi(shopId, expenseId) {
      return String(apiTemplate).replace("/0/0/payment", "/" + String(shopId) + "/" + String(expenseId) + "/payment");
    }
    function setMsg(text, tone) {
      if (!msgEl) return;
      if (!text) {
        msgEl.classList.add("hidden");
        msgEl.textContent = "";
        return;
      }
      msgEl.textContent = text;
      msgEl.classList.remove("hidden");
      msgEl.className =
        "mt-2 text-xs " +
        (tone === "error" ? "text-rose-600 dark:text-rose-300" : "text-[rgb(var(--rc-muted))]");
    }
    function openModal(btn) {
      var shopId = btn.getAttribute("data-shop-id") || "";
      var id = btn.getAttribute("data-expense-id") || "";
      currentBalance = parseFloat(btn.getAttribute("data-balance") || "0") || 0;
      if (shopIdEl) shopIdEl.value = shopId;
      if (idEl) idEl.value = id;
      if (nameEl) nameEl.textContent = btn.getAttribute("data-expense-name") || "—";
      if (totalEl) totalEl.textContent = fmt(btn.getAttribute("data-total"));
      if (paidEl) paidEl.textContent = fmt(btn.getAttribute("data-paid"));
      if (balanceEl) balanceEl.textContent = fmt(currentBalance);
      if (amountEl) {
        amountEl.value = "";
        amountEl.max = String(currentBalance > 0 ? currentBalance : "");
      }
      setMsg("", "");
      modal.classList.remove("hidden");
      modal.setAttribute("aria-hidden", "false");
      if (amountEl) amountEl.focus();
    }
    function closeModal() {
      modal.classList.add("hidden");
      modal.setAttribute("aria-hidden", "true");
      setMsg("", "");
    }
    function submitPayment(payload) {
      var shopId = shopIdEl ? String(shopIdEl.value || "").trim() : "";
      var id = idEl ? String(idEl.value || "").trim() : "";
      if (!shopId || !id) {
        setMsg("Invalid expense.", "error");
        return;
      }
      setMsg("Saving…", "");
      fetch(paymentApi(shopId, id), {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: JSON.stringify(payload),
      })
        .then(function (r) {
          return r.json().then(function (j) {
            return { ok: r.ok, j: j };
          });
        })
        .then(function (x) {
          if (!x.ok || !x.j || !x.j.ok) {
            setMsg((x.j && x.j.error) || "Could not save payment.", "error");
            return;
          }
          window.location.reload();
        })
        .catch(function () {
          setMsg("Could not save payment.", "error");
        });
    }

    qsa(".js-company-expense-pay").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openModal(btn);
      });
    });
    if (closeBtn) closeBtn.addEventListener("click", closeModal);
    if (cancelBtn) cancelBtn.addEventListener("click", closeModal);
    if (backdrop) backdrop.addEventListener("click", closeModal);
    if (fullBtn) {
      fullBtn.addEventListener("click", function () {
        if (currentBalance <= 0) {
          setMsg("Nothing left to pay.", "error");
          return;
        }
        submitPayment({ pay_mode: "full" });
      });
    }
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      var raw = amountEl ? String(amountEl.value || "").trim() : "";
      var amt = parseFloat(raw);
      if (!isFinite(amt) || amt <= 0) {
        setMsg("Enter a valid payment amount.", "error");
        return;
      }
      if (currentBalance > 0 && amt > currentBalance + 0.001) {
        setMsg("Amount exceeds balance (" + fmt(currentBalance) + ").", "error");
        return;
      }
      submitPayment({ additional_payment: amt });
    });
  }

  document.addEventListener("DOMContentLoaded", bindCompanyExpensePayments);
})();
