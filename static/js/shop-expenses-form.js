(function () {
  function qs(sel, root) {
    return (root || document).querySelector(sel);
  }
  function qsa(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }
  function toUpperInput(el) {
    if (!el) return;
    el.addEventListener("input", function () {
      var p = el.selectionStart;
      var q = el.selectionEnd;
      el.value = String(el.value || "").toUpperCase();
      try {
        el.setSelectionRange(p, q);
      } catch (e) {}
    });
  }
  function updateTotal() {
    var qtyEl = qs("#shop-expense-qty");
    var unitEl = qs("#shop-expense-unit-price");
    var totalEl = qs("#shop-expense-total");
    if (!qtyEl || !unitEl || !totalEl) return;
    var qty = parseFloat(qtyEl.value || "0");
    var unit = parseFloat(unitEl.value || "0");
    var total = (isFinite(qty) && isFinite(unit) ? qty * unit : 0);
    totalEl.textContent = total.toFixed(2);
  }
  function bindExpenseNamePicker() {
    var api = window.__SHOP_EXPENSE_SEARCH_API || "";
    var catEl = qs("#shop-expense-category");
    var nameEl = qs("#shop-expense-name");
    var toggleEl = qs("#shop-expense-list-toggle");
    var box = qs("#shop-expense-name-suggest");
    if (!api || !nameEl) return;
    var timer = null;
    var catalogRows = [];

    function filterCatalogRows(query, showAll) {
      var q = showAll ? "" : String(query || "").trim().toLowerCase();
      return catalogRows.filter(function (row) {
        if (!q) return true;
        var name = String(row.name || "");
        var cat = String(row.category_name || "");
        return (name + " " + cat).toLowerCase().indexOf(q) !== -1;
      });
    }
    function hide() {
      if (!box) return;
      box.classList.add("hidden");
      box.innerHTML = "";
    }
    function loadCatalog() {
      var cat = catEl ? String(catEl.value || "").trim() : "";
      var url = api + "?kind=items&limit=100";
      if (cat) url += "&category=" + encodeURIComponent(cat);
      return fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
        .then(function (r) {
          return r.json();
        })
        .then(function (j) {
          catalogRows = (j && j.results) || [];
          return catalogRows;
        })
        .catch(function () {
          catalogRows = [];
          return [];
        });
    }
    function show(rows, emptyMessage) {
      if (!box) return;
      if (!rows.length) {
        box.innerHTML =
          '<div class="px-3 py-2 text-xs text-[rgb(var(--rc-muted))]">' +
          (emptyMessage || "No matching expenses. Type a new name.") +
          "</div>";
        box.classList.remove("hidden");
        return;
      }
      box.innerHTML = rows
        .map(function (r) {
          var name = String(r.name || "");
          var cat = String(r.category_name || "");
          var label = name + (cat ? " — " + cat : "");
          return (
            '<button type="button" class="block w-full px-3 py-2 text-left text-sm uppercase hover:bg-[rgb(var(--rc-surface-2))]" data-name="' +
            name.replace(/"/g, "&quot;") +
            '" data-category="' +
            cat.replace(/"/g, "&quot;") +
            '">' +
            label +
            "</button>"
          );
        })
        .join("");
      box.classList.remove("hidden");
      qsa("button[data-name]", box).forEach(function (btn) {
        btn.addEventListener("click", function () {
          var picked = String(btn.getAttribute("data-name") || "").toUpperCase();
          nameEl.value = picked;
          var pickedCat = String(btn.getAttribute("data-category") || "").trim();
          if (pickedCat && catEl && !String(catEl.value || "").trim()) {
            catEl.value = pickedCat;
          }
          hide();
        });
      });
    }
    function openSuggest(showAll) {
      var rows = filterCatalogRows(nameEl.value, showAll);
      if (!rows.length) {
        show([], showAll ? "No registered expenses yet." : "No matching expenses. Type a new name.");
        return;
      }
      show(rows);
    }
    function lookup(showAll) {
      if (catalogRows.length) {
        openSuggest(!!showAll);
        return Promise.resolve();
      }
      return loadCatalog().then(function () {
        openSuggest(!!showAll);
      });
    }
    nameEl.addEventListener("input", function () {
      clearTimeout(timer);
      timer = setTimeout(function () {
        lookup(false);
      }, 180);
    });
    nameEl.addEventListener("focus", function () {
      lookup(false);
    });
    if (toggleEl) {
      toggleEl.addEventListener("click", function (e) {
        e.preventDefault();
        lookup(true);
        nameEl.focus();
      });
    }
    if (catEl) {
      catEl.addEventListener("change", function () {
        loadCatalog().then(function () {
          lookup(false);
        });
      });
      catEl.addEventListener("input", function () {
        clearTimeout(timer);
        timer = setTimeout(function () {
          loadCatalog().then(function () {
            lookup(false);
          });
        }, 280);
      });
    }
    document.addEventListener("click", function (e) {
      if (!box || box.classList.contains("hidden")) return;
      if (box.contains(e.target)) return;
      if (e.target === nameEl) return;
      if (e.target === toggleEl) return;
      hide();
    });
    loadCatalog();
  }

  function bindExpensePayments() {
    var modal = qs("#shop-expense-pay-modal");
    var form = qs("#shop-expense-pay-form");
    var apiTemplate = window.__SHOP_EXPENSE_PAYMENT_API || "";
    if (!modal || !form || !apiTemplate) return;

    var idEl = qs("#shop-expense-pay-id");
    var nameEl = qs("#shop-expense-pay-name");
    var totalEl = qs("#shop-expense-pay-total");
    var paidEl = qs("#shop-expense-pay-paid");
    var balanceEl = qs("#shop-expense-pay-balance");
    var amountEl = qs("#shop-expense-pay-amount");
    var msgEl = qs("#shop-expense-pay-msg");
    var closeBtn = qs("#shop-expense-pay-close");
    var cancelBtn = qs("#shop-expense-pay-cancel");
    var fullBtn = qs("#shop-expense-pay-full");
    var backdrop = modal.querySelector(".exp-pay-modal-backdrop");
    var currentBalance = 0;

    function fmt(n) {
      return Number(n || 0).toFixed(2);
    }
    function paymentApi(id) {
      return String(apiTemplate).replace("/0/payment", "/" + String(id) + "/payment");
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
      var id = btn.getAttribute("data-expense-id") || "";
      currentBalance = parseFloat(btn.getAttribute("data-balance") || "0") || 0;
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
      var id = idEl ? String(idEl.value || "").trim() : "";
      if (!id) {
        setMsg("Invalid expense.", "error");
        return;
      }
      setMsg("Saving…", "");
      fetch(paymentApi(id), {
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

    qsa(".js-shop-expense-pay").forEach(function (btn) {
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

  function bindSupplierLookup() {
    var api = window.__SELLER_LOOKUP_API || "";
    var phoneEl = qs("#shop-expense-seller-phone");
    var nameEl = qs("#shop-expense-seller-name");
    var hintEl = qs("#shop-expense-seller-hint");
    if (!api || !phoneEl || !nameEl) return;
    var timer = null;
    function lookup() {
      var phone = String(phoneEl.value || "").trim();
      if (window.KenyaPhone && typeof window.KenyaPhone.normalize === "function") {
        phone = window.KenyaPhone.normalize(phone) || phone;
      } else if (phone.indexOf("+254") === 0) {
        phone = "254" + phone.slice(4);
      }
      phone = phone.replace(/\s+/g, "");
      if (phone && phoneEl.value !== phone) phoneEl.value = phone;
      var digits = phone.replace(/\D/g, "");
      if (!phone) {
        if (hintEl) hintEl.textContent = "";
        return;
      }
      if (digits.length < 10) {
        if (hintEl) hintEl.textContent = "Continue typing supplier phone (10 digits)...";
        return;
      }
      var valid10 = /^((07|01)\d{8})$/.test(digits);
      var valid12 = /^(254\d{9})$/.test(digits);
      if (!valid10 && !valid12) {
        if (hintEl) hintEl.textContent = "Use 07… or 01… (10 digits) or 254… (12 digits).";
        return;
      }
      var fd = new FormData();
      fd.append("seller_phone", phone);
      fetch(api, { method: "POST", body: fd, credentials: "same-origin" })
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          if (!data || !data.ok || !data.registered || !data.seller) {
            if (hintEl) hintEl.textContent = "New supplier phone. Enter supplier name to register.";
            return;
          }
          nameEl.value = String(data.seller.seller_name || "").toUpperCase();
          if (hintEl) hintEl.textContent = "Registered supplier found. Name auto-filled.";
        })
        .catch(function () {
          if (hintEl) hintEl.textContent = "";
        });
    }
    phoneEl.addEventListener("blur", function () {
      if (window.KenyaPhone) window.KenyaPhone.applyToInput(phoneEl);
      lookup();
    });
    phoneEl.addEventListener("input", function () {
      clearTimeout(timer);
      timer = setTimeout(lookup, 220);
    });
  }

  function bindRegisterFormToggle() {
    var toggleBtn = qs("#shop-expense-form-toggle");
    var cancelBtn = qs("#shop-expense-form-cancel");
    var wrap = qs("#shop-expense-form-wrap");
    var form = qs("#shop-expense-form");
    var firstField = qs("#shop-expense-category");
    if (!toggleBtn || !wrap) return;

    function isOpen() {
      return !wrap.classList.contains("hidden");
    }
    function setOpen(open) {
      wrap.classList.toggle("hidden", !open);
      toggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
      toggleBtn.textContent = open ? "Hide form" : "New expense";
      toggleBtn.classList.toggle("btn-rc-secondary", open);
      toggleBtn.classList.toggle("btn-rc-primary", !open);
      if (open && firstField) {
        setTimeout(function () {
          firstField.focus();
        }, 30);
      }
    }
    function closeForm() {
      setOpen(false);
    }
    toggleBtn.addEventListener("click", function () {
      setOpen(!isOpen());
    });
    if (cancelBtn) {
      cancelBtn.addEventListener("click", function () {
        if (form) form.reset();
        updateTotal();
        closeForm();
      });
    }
    var errorFlash = document.querySelector('[role="alert"] li.border-red-200');
    if (errorFlash) setOpen(true);
  }

  document.addEventListener("DOMContentLoaded", function () {
    qsa(".shop-exp-upper").forEach(toUpperInput);
    qsa("#shop-expense-qty, #shop-expense-unit-price").forEach(function (el) {
      el.addEventListener("input", updateTotal);
    });
    updateTotal();
    bindRegisterFormToggle();
    bindExpenseNamePicker();
    bindSupplierLookup();
    bindExpensePayments();
  });
})();
