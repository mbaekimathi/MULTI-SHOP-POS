(function () {
  function fmtMoney(n) {
    try {
      return Number(n || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } catch (e) {
      return String(n || "0.00");
    }
  }

  function hasPendingClosing(boot) {
    return !!(boot && boot.pending_closing && boot.pending_closing.required !== false);
  }

  function needsGateModal(boot) {
    if (!boot || !boot.can_submit) return false;
    if (boot.today_closed) return false;
    return hasPendingClosing(boot) || !boot.completed;
  }

  window.initShopDayOpeningModal = function (opts) {
    opts = opts || {};
    var boot = opts.boot || {};
    var openingApi = opts.submitApi || opts.openingApi || "";
    var closingApi = opts.closingApi || "";

    function syncReadyState() {
      if (typeof window.syncPosTillReadyState === "function") {
        window.syncPosTillReadyState();
        return;
      }
      boot.ready_for_sales = !!boot.completed && !hasPendingClosing(boot) && !boot.today_closed;
    }

    function applyTillOpenState(j) {
      j = j || {};
      if (j.pending_closing !== undefined) boot.pending_closing = j.pending_closing;
      if (j.record) boot.record = j.record;
      if (j.closing_reminder) boot.closing_reminder = j.closing_reminder;
      if (j.reopened === true || j.ready_for_sales === true || j.completed === true) {
        boot.completed = true;
        boot.today_closed = false;
      } else {
        if (j.completed === true || (j.ok && j.completed !== false)) boot.completed = true;
        if (typeof j.today_closed === "boolean") boot.today_closed = j.today_closed;
        else boot.today_closed = j.today_closed === true;
      }
      if (typeof j.sales_allowed === "boolean") boot.sales_allowed = j.sales_allowed;
      if (j.sales_blocked_message) boot.sales_blocked_message = j.sales_blocked_message;
      syncReadyState();
    }

    var onComplete = typeof opts.onComplete === "function" ? opts.onComplete : function () {};

    function finishAll() {
      syncReadyState();
      hideModal();
      if (typeof onComplete === "function") onComplete(boot);
    }

    function setOpeningMsg(text, kind) {
      var el = document.getElementById("pos-day-opening-msg");
      if (!el) return;
      if (!text) {
        el.textContent = "";
        el.className = "hidden text-sm font-semibold";
        return;
      }
      el.textContent = text;
      el.className =
        "text-sm font-semibold " +
        (kind === "ok"
          ? "text-emerald-600 dark:text-emerald-400"
          : kind === "muted"
            ? "text-[rgb(var(--rc-muted))]"
            : "text-rose-600 dark:text-rose-400");
    }

    function setClosingMsg(text, kind) {
      var el = document.getElementById("pos-day-closing-msg");
      if (!el) return;
      if (!text) {
        el.textContent = "";
        el.className = "hidden text-sm font-semibold";
        return;
      }
      el.textContent = text;
      el.className =
        "text-sm font-semibold " +
        (kind === "ok"
          ? "text-emerald-600 dark:text-emerald-400"
          : kind === "muted"
            ? "text-[rgb(var(--rc-muted))]"
            : "text-rose-600 dark:text-rose-400");
    }

    function currentStep() {
      return hasPendingClosing(boot) ? "closing" : "opening";
    }

    function updatePanels() {
      var closingPanel = document.getElementById("pos-day-closing-panel");
      var openingPanel = document.getElementById("pos-day-opening-panel");
      var step = currentStep();
      if (closingPanel) closingPanel.classList.toggle("hidden", step !== "closing");
      if (openingPanel) openingPanel.classList.toggle("hidden", step !== "opening");
      var stepLabel = document.getElementById("pos-day-opening-step-label");
      if (stepLabel) {
        stepLabel.textContent = hasPendingClosing(boot)
          ? "Step 2 of 2 · Required"
          : "Start of day · Required";
      }
      if (step === "closing") populateClosingHint();
    }

    function populateClosingHint() {
      var hint = document.getElementById("pos-day-closing-hint");
      var pc = boot.pending_closing || {};
      if (!hint) return;
      var label = pc.label || pc.business_date || "the previous day";
      var title = document.getElementById("pos-day-closing-modal-title");
      if (title) title.textContent = "Closing balances for " + label;
      hint.textContent =
        "Opening: KES " +
        fmtMoney(pc.opening_cash) +
        " cash + KES " +
        fmtMoney(pc.opening_mpesa) +
        " M-Pesa. Sales recorded: KES " +
        fmtMoney(pc.cash_revenue) +
        " cash + KES " +
        fmtMoney(pc.mpesa_revenue) +
        " M-Pesa. Suggested closing: KES " +
        fmtMoney(pc.suggested_closing_cash) +
        " cash + KES " +
        fmtMoney(pc.suggested_closing_mpesa) +
        " M-Pesa.";
      var cashEl = document.getElementById("pos-day-closing-cash");
      var mpesaEl = document.getElementById("pos-day-closing-mpesa");
      if (cashEl && cashEl.value === "0" && pc.suggested_closing_cash != null) {
        cashEl.value = String(pc.suggested_closing_cash);
      }
      if (mpesaEl && mpesaEl.value === "0" && pc.suggested_closing_mpesa != null) {
        mpesaEl.value = String(pc.suggested_closing_mpesa);
      }
    }

    function populateOpeningForReopen() {
      var rec = boot.record || {};
      var cashEl = document.getElementById("pos-day-opening-cash");
      var mpesaEl = document.getElementById("pos-day-opening-mpesa");
      var title = document.getElementById("pos-day-opening-modal-title");
      var desc = document.getElementById("pos-day-opening-modal-desc");
      var submitBtn = document.getElementById("pos-day-opening-submit");
      if (cashEl && rec.opening_cash != null) cashEl.value = String(rec.opening_cash);
      if (mpesaEl && rec.opening_mpesa != null) mpesaEl.value = String(rec.opening_mpesa);
      if (boot.today_closed) {
        if (title) title.textContent = "Reopen shop for today";
        if (desc) {
          desc.textContent =
            "The shop was closed for today. Confirm or update opening cash and M-Pesa to resume sales.";
        }
        if (submitBtn) submitBtn.textContent = "Reopen shop & save";
      }
    }

    function showModal(opts) {
      opts = opts || {};
      if (!opts.force && !needsGateModal(boot)) return;
      updatePanels();
      if (opts.force && (opts.step === "opening" || opts.reopen) && !hasPendingClosing(boot)) {
        var closingPanel = document.getElementById("pos-day-closing-panel");
        var openingPanel = document.getElementById("pos-day-opening-panel");
        if (closingPanel) closingPanel.classList.add("hidden");
        if (openingPanel) openingPanel.classList.remove("hidden");
        if (opts.reopen || boot.today_closed) populateOpeningForReopen();
      }
      var modal = document.getElementById("pos-day-opening-modal");
      var backdrop = document.getElementById("pos-day-opening-modal-backdrop");
      if (!modal || !backdrop) return;
      modal.classList.remove("hidden");
      backdrop.classList.remove("hidden");
      modal.setAttribute("aria-hidden", "false");
      backdrop.setAttribute("aria-hidden", "false");
      document.body.classList.add("pos-day-opening-modal-open");
      var focusEl =
        currentStep() === "closing"
          ? document.getElementById("pos-day-closing-cash")
          : document.getElementById("pos-day-opening-cash");
      if (focusEl) {
        try {
          focusEl.focus();
        } catch (eFocus) {}
      }
    }

    function hideModal() {
      var modal = document.getElementById("pos-day-opening-modal");
      var backdrop = document.getElementById("pos-day-opening-modal-backdrop");
      if (modal) {
        modal.classList.add("hidden");
        modal.setAttribute("aria-hidden", "true");
      }
      if (backdrop) {
        backdrop.classList.add("hidden");
        backdrop.setAttribute("aria-hidden", "true");
      }
      document.body.classList.remove("pos-day-opening-modal-open");
    }

    var closingSubmitInFlight = false;
    var closingLastAutoCode = "";
    var openingSubmitInFlight = false;
    var openingLastAutoCode = "";

    function readClosingEmployeeCode(codeEl) {
      var code = String((codeEl && codeEl.value) || "").replace(/\D/g, "").slice(0, 6);
      if (codeEl) codeEl.value = code;
      return code;
    }

    function submitClosingForm(ev, autoFromCode) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (!closingApi) return;
      if (closingSubmitInFlight) return;
      var pc = boot.pending_closing || {};
      var cashEl = document.getElementById("pos-day-closing-cash");
      var mpesaEl = document.getElementById("pos-day-closing-mpesa");
      var codeEl = document.getElementById("pos-day-closing-employee-code");
      var btn = document.getElementById("pos-day-closing-submit");
      var cash = parseFloat((cashEl && cashEl.value) || "0");
      var mpesa = parseFloat((mpesaEl && mpesaEl.value) || "0");
      if (isNaN(cash) || cash < 0 || isNaN(mpesa) || mpesa < 0) {
        setClosingMsg("Enter valid closing cash and M-Pesa amounts (0 is allowed).", "error");
        if (autoFromCode) closingLastAutoCode = "";
        return;
      }
      var code = readClosingEmployeeCode(codeEl);
      if (!/^\d{6}$/.test(code)) {
        setClosingMsg(
          "Enter a manager, admin, company manager, IT support, or super admin 6-digit code.",
          "error"
        );
        return;
      }
      var closeLabel = pc.label || pc.business_date || "the previous day";
      if (
        !autoFromCode &&
        !window.confirm(
          "Submit closing balances for " +
            closeLabel +
            "?\n\nThis is required before today's opening can continue."
        )
      ) {
        return;
      }
      var payload = {
        business_date: pc.business_date || "",
        closing_cash: cash,
        closing_mpesa: mpesa,
        employee_code: code,
      };
      closingSubmitInFlight = true;
      if (btn) btn.disabled = true;
      setClosingMsg(autoFromCode ? "Verifying code and saving closing…" : "", autoFromCode ? "muted" : "");
      fetch(closingApi, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest" },
        body: JSON.stringify(payload),
      })
        .then(function (r) {
          return r
            .json()
            .catch(function () {
              return {};
            })
            .then(function (j) {
              return { status: r.status, body: j };
            });
        })
        .then(function (res) {
          var j = res.body || {};
          if (!j.ok) throw new Error(j.error || "Could not save closing balances.");
          boot.pending_closing = j.pending_closing || null;
          boot.today_closed = j.today_closed === true;
          syncReadyState();
          setClosingMsg("Closing balances saved.", "ok");
          closingLastAutoCode = "";
          if (boot.ready_for_sales) {
            finishAll();
            return;
          }
          updatePanels();
          setOpeningMsg("", "");
          if (!boot.completed) {
            var cashOpen = document.getElementById("pos-day-opening-cash");
            if (cashOpen) {
              try {
                cashOpen.focus();
              } catch (eFocusOpen) {}
            }
          } else {
            finishAll();
          }
        })
        .catch(function (err) {
          closingLastAutoCode = "";
          setClosingMsg(String((err && err.message) || "Could not save closing balances."), "error");
        })
        .finally(function () {
          closingSubmitInFlight = false;
          if (btn) btn.disabled = false;
        });
    }

    function readOpeningEmployeeCode(codeEl) {
      var code = String((codeEl && codeEl.value) || "").replace(/\D/g, "").slice(0, 6);
      if (codeEl) codeEl.value = code;
      return code;
    }

    function submitOpeningForm(ev, autoFromCode) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (openingSubmitInFlight) return;
      if (hasPendingClosing(boot)) {
        setOpeningMsg("Submit the previous day's closing balances first.", "error");
        updatePanels();
        return;
      }
      if (!openingApi) return;
      var cashEl = document.getElementById("pos-day-opening-cash");
      var mpesaEl = document.getElementById("pos-day-opening-mpesa");
      var stockEl = document.getElementById("pos-day-opening-stock-confirmed");
      var codeEl = document.getElementById("pos-day-opening-employee-code");
      var btn = document.getElementById("pos-day-opening-submit");
      var cash = parseFloat((cashEl && cashEl.value) || "0");
      var mpesa = parseFloat((mpesaEl && mpesaEl.value) || "0");
      if (isNaN(cash) || cash < 0 || isNaN(mpesa) || mpesa < 0) {
        setOpeningMsg("Enter valid opening cash and M-Pesa amounts (0 is allowed).", "error");
        if (autoFromCode) openingLastAutoCode = "";
        return;
      }
      var needStock = boot.requires_stock_confirmation !== false;
      if (needStock && (!stockEl || !stockEl.checked)) {
        setOpeningMsg("Confirm that shop stock is up to date.", "error");
        if (autoFromCode) openingLastAutoCode = "";
        return;
      }
      var code = readOpeningEmployeeCode(codeEl);
      if (!/^\d{6}$/.test(code)) {
        setOpeningMsg("Enter a manager or admin 6-digit code to open the shop.", "error");
        return;
      }
      var payload = {
        opening_cash: cash,
        opening_mpesa: mpesa,
        stock_confirmed: needStock ? true : false,
        employee_code: code,
      };
      openingSubmitInFlight = true;
      if (btn) btn.disabled = true;
      setOpeningMsg(autoFromCode ? "Verifying code and saving opening…" : "", autoFromCode ? "muted" : "");
      fetch(openingApi, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest" },
        body: JSON.stringify(payload),
      })
        .then(function (r) {
          return r
            .json()
            .catch(function () {
              return {};
            })
            .then(function (j) {
              return { status: r.status, body: j };
            });
        })
        .then(function (res) {
          var j = res.body || {};
          if (!j.ok) throw new Error(j.error || "Could not save opening balances.");
          applyTillOpenState(j);
          openingLastAutoCode = "";
          setOpeningMsg(j.reopened ? "Shop reopened — sales can continue." : "Shop opening saved for today.", "ok");
          finishAll();
        })
        .catch(function (err) {
          openingLastAutoCode = "";
          setOpeningMsg(String((err && err.message) || "Could not save opening balances."), "error");
        })
        .finally(function () {
          openingSubmitInFlight = false;
          if (btn) btn.disabled = false;
        });
    }

    var closingForm = document.getElementById("pos-day-closing-form");
    if (closingForm) {
      closingForm.addEventListener("submit", function (ev) {
        submitClosingForm(ev, false);
      });
    }
    var closingCodeInput = document.getElementById("pos-day-closing-employee-code");
    if (closingCodeInput) {
      closingCodeInput.addEventListener("input", function () {
        var raw = readClosingEmployeeCode(closingCodeInput);
        if (!/^\d{6}$/.test(raw)) return;
        if (raw === closingLastAutoCode) return;
        closingLastAutoCode = raw;
        submitClosingForm(null, true);
      });
    }
    var openingForm = document.getElementById("pos-day-opening-form");
    if (openingForm) {
      openingForm.addEventListener("submit", function (ev) {
        submitOpeningForm(ev, false);
      });
    }
    var openingCodeInput = document.getElementById("pos-day-opening-employee-code");
    if (openingCodeInput) {
      openingCodeInput.addEventListener("input", function () {
        var raw = readOpeningEmployeeCode(openingCodeInput);
        if (!/^\d{6}$/.test(raw)) return;
        if (raw === openingLastAutoCode) return;
        openingLastAutoCode = raw;
        submitOpeningForm(null, true);
      });
    }

    document.addEventListener("keydown", function (ev) {
      if (!needsGateModal(boot)) return;
      if (ev.key === "Escape") {
        ev.preventDefault();
        ev.stopPropagation();
      }
    });

    if (opts.autoInit !== false && boot.auto_prompt === true && boot.can_submit && boot.today_closed) {
      setTimeout(function () {
        showModal({ force: true, step: "opening", reopen: true });
      }, opts.delayMs != null ? opts.delayMs : 350);
    } else if (opts.autoInit !== false && boot.auto_prompt === true && needsGateModal(boot)) {
      setTimeout(function () {
        showModal();
      }, opts.delayMs != null ? opts.delayMs : 350);
    }

    return {
      show: function (opts) {
        showModal(opts);
      },
      showOpening: function () {
        showModal({ force: true, step: "opening" });
      },
      showReopen: function () {
        showModal({ force: true, step: "opening", reopen: true });
      },
      hide: hideModal,
      isComplete: function () {
        syncReadyState();
        return !!boot.ready_for_sales;
      },
      boot: boot,
      fmtMoney: fmtMoney,
    };
  };
})();
