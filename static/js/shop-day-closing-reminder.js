(function () {
  function fmtMoney(n) {
    try {
      return Number(n || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } catch (e) {
      return String(n || "0.00");
    }
  }

  function dismissStorageKey(shopId, businessDate) {
    return "posClosingReminderDismissed:" + String(shopId || 0) + ":" + String(businessDate || "");
  }

  function isDismissed(shopId, businessDate) {
    try {
      return sessionStorage.getItem(dismissStorageKey(shopId, businessDate)) === "1";
    } catch (e) {
      return false;
    }
  }

  function setDismissed(shopId, businessDate) {
    try {
      sessionStorage.setItem(dismissStorageKey(shopId, businessDate), "1");
    } catch (e) {}
  }

  function clearDismissed(shopId, businessDate) {
    try {
      sessionStorage.removeItem(dismissStorageKey(shopId, businessDate));
    } catch (e) {}
  }

  window.initShopDayClosingReminder = function (opts) {
    opts = opts || {};
    var boot = opts.boot || {};
    var closingApi = opts.closingApi || "";
    var statusApi = opts.statusApi || "";
    var shopId = opts.shopId || 0;
    var onSubmitted = typeof opts.onSubmitted === "function" ? opts.onSubmitted : function () {};
    var pollMs = opts.pollMs != null ? opts.pollMs : 60000;

    function pendingContext() {
      return boot.pending || null;
    }

    var submitInFlight = false;
    var lastAutoCode = "";

    function resetCodeField() {
      lastAutoCode = "";
      var codeEl = document.getElementById("shop-day-closing-reminder-employee-code");
      if (codeEl) codeEl.value = "";
    }

    function readEmployeeCode(codeEl) {
      var code = String((codeEl && codeEl.value) || "").replace(/\D/g, "").slice(0, 6);
      if (codeEl) codeEl.value = code;
      return code;
    }

    function canUseReminder() {
      if (boot.today_closed) return false;
      if (!boot.can_submit) return false;
      var ctx = pendingContext();
      return !!(ctx && !ctx.submitted);
    }

    function setMsg(text, kind) {
      var el = document.getElementById("shop-day-closing-reminder-msg");
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

    function populateFields(ctx, forceValues) {
      ctx = ctx || {};
      var hint = document.getElementById("shop-day-closing-reminder-hint");
      var title = document.getElementById("shop-day-closing-reminder-title");
      var timing = document.getElementById("shop-day-closing-reminder-timing");
      var label = ctx.label || ctx.business_date || "this day";
      if (title) title.textContent = "Closing balances for " + label;
      if (hint) {
        hint.textContent =
          "Opening: KES " +
          fmtMoney(ctx.opening_cash) +
          " cash + KES " +
          fmtMoney(ctx.opening_mpesa) +
          " M-Pesa. Sales recorded: KES " +
          fmtMoney(ctx.cash_revenue) +
          " cash + KES " +
          fmtMoney(ctx.mpesa_revenue) +
          " M-Pesa. Suggested closing: KES " +
          fmtMoney(ctx.suggested_closing_cash) +
          " cash + KES " +
          fmtMoney(ctx.suggested_closing_mpesa) +
          " M-Pesa.";
      }
      if (timing) {
        if (boot.close_time) {
          var mins = boot.minutes_until_close;
          if (typeof mins === "number" && mins > 0) {
            timing.textContent = "Shop closes at " + boot.close_time + " · about " + mins + " min remaining";
          } else if (boot.close_time) {
            timing.textContent = "Shop closes at " + boot.close_time;
          } else {
            timing.textContent = "";
          }
        } else {
          timing.textContent = "";
        }
      }
      var cashEl = document.getElementById("shop-day-closing-reminder-cash");
      var mpesaEl = document.getElementById("shop-day-closing-reminder-mpesa");
      if (forceValues || (cashEl && (cashEl.value === "" || cashEl.value === "0"))) {
        if (cashEl && ctx.suggested_closing_cash != null) cashEl.value = String(ctx.suggested_closing_cash);
      }
      if (forceValues || (mpesaEl && (mpesaEl.value === "" || mpesaEl.value === "0"))) {
        if (mpesaEl && ctx.suggested_closing_mpesa != null) mpesaEl.value = String(ctx.suggested_closing_mpesa);
      }
      var codeWrap = document.getElementById("shop-day-closing-reminder-code-wrap");
      if (codeWrap) codeWrap.classList.remove("hidden");
    }

    function showModal(manual) {
      if (!manual && !canUseReminder()) return;
      if (manual) {
        if (!boot.can_submit) {
          setMsg(
            "Only a manager, admin, company manager, IT support, or super admin can close the shop.",
            "error"
          );
          return;
        }
        var ctxCheck = pendingContext();
        if (!ctxCheck || ctxCheck.submitted) {
          setMsg("Set today's opening balances first, or closing is already recorded for today.", "error");
          return;
        }
      }
      resetCodeField();
      submitInFlight = false;
      var ctx = pendingContext();
      populateFields(ctx, !!manual);
      var modal = document.getElementById("shop-day-closing-reminder-modal");
      if (!modal) return;
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("shop-day-closing-reminder-open");
      requestAnimationFrame(function () {
        modal.classList.add("is-open");
      });
      var cashEl = document.getElementById("shop-day-closing-reminder-cash");
      if (cashEl) {
        window.setTimeout(function () {
          try {
            cashEl.focus();
          } catch (eFocus) {}
        }, 120);
      }
    }

    function hideModal() {
      var modal = document.getElementById("shop-day-closing-reminder-modal");
      if (!modal) return;
      modal.classList.remove("is-open");
      modal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("shop-day-closing-reminder-open");
    }

    function dismissReminder() {
      var ctx = pendingContext();
      if (ctx && ctx.business_date) setDismissed(shopId, ctx.business_date);
      hideModal();
    }

    function maybeAutoShow() {
      /* Closing is manual only — never auto-popup. */
    }

    function refreshFromServer() {
      if (!statusApi) return;
      fetch(statusApi, { headers: { "X-Requested-With": "XMLHttpRequest" } })
        .then(function (r) {
          return r
            .json()
            .catch(function () {
              return {};
            });
        })
        .then(function (j) {
          if (!j || !j.ok) return;
          if (j.today_closed) boot.today_closed = true;
          var cr = j.closing_reminder;
          if (!cr) return;
          boot.close_time = cr.close_time || boot.close_time;
          boot.minutes_until_close = cr.minutes_until_close;
          boot.can_submit = cr.can_submit === true;
          boot.can_close_today = cr.can_close_today === true;
          boot.requires_employee_code = cr.requires_employee_code === true;
          if (cr.pending && !cr.pending.submitted) boot.pending = cr.pending;
          else if (!cr.pending) boot.pending = null;
        })
        .catch(function () {});
    }

    function submitForm(ev, autoFromCode) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (!closingApi || !canUseReminder()) return;
      if (submitInFlight) return;
      var ctx = pendingContext() || {};
      var cashEl = document.getElementById("shop-day-closing-reminder-cash");
      var mpesaEl = document.getElementById("shop-day-closing-reminder-mpesa");
      var codeEl = document.getElementById("shop-day-closing-reminder-employee-code");
      var btn = document.getElementById("shop-day-closing-reminder-submit");
      var cash = parseFloat((cashEl && cashEl.value) || "0");
      var mpesa = parseFloat((mpesaEl && mpesaEl.value) || "0");
      if (isNaN(cash) || cash < 0 || isNaN(mpesa) || mpesa < 0) {
        setMsg("Enter valid closing cash and M-Pesa amounts (0 is allowed).", "error");
        if (autoFromCode) lastAutoCode = "";
        return;
      }
      var code = readEmployeeCode(codeEl);
      if (!/^\d{6}$/.test(code)) {
        setMsg(
          "Enter a manager, admin, company manager, IT support, or super admin 6-digit code.",
          "error"
        );
        return;
      }
      if (
        !autoFromCode &&
        !window.confirm(
          "Close the shop for " +
            (ctx.label || ctx.business_date || "today") +
            "?\n\nClosing balances will be saved and sales will be disabled. Staff can browse the catalog but cannot checkout until the next opening."
        )
      ) {
        return;
      }
      var payload = {
        business_date: ctx.business_date || boot.business_date || "",
        closing_cash: cash,
        closing_mpesa: mpesa,
        employee_code: code,
      };
      submitInFlight = true;
      if (btn) btn.disabled = true;
      setMsg(autoFromCode ? "Verifying code and closing shop…" : "", autoFromCode ? "muted" : "");
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
          boot.pending = null;
          boot.auto_show = false;
          boot.today_closed = true;
          if (ctx.business_date) clearDismissed(shopId, ctx.business_date);
          setMsg("Closing balances saved. Shop is closed for today.", "ok");
          hideModal();
          onSubmitted(j, boot);
        })
        .catch(function (err) {
          lastAutoCode = "";
          setMsg(String((err && err.message) || "Could not save closing balances."), "error");
        })
        .finally(function () {
          submitInFlight = false;
          if (btn) btn.disabled = false;
        });
    }

    var form = document.getElementById("shop-day-closing-reminder-form");
    if (form) form.addEventListener("submit", function (ev) {
      submitForm(ev, false);
    });
    var codeInput = document.getElementById("shop-day-closing-reminder-employee-code");
    if (codeInput) {
      codeInput.addEventListener("input", function () {
        var raw = readEmployeeCode(codeInput);
        if (!/^\d{6}$/.test(raw)) return;
        if (raw === lastAutoCode) return;
        lastAutoCode = raw;
        submitForm(null, true);
      });
    }
    var dismissBtn = document.getElementById("shop-day-closing-reminder-dismiss");
    if (dismissBtn) dismissBtn.addEventListener("click", dismissReminder);
    var laterBtn = document.getElementById("shop-day-closing-reminder-later");
    if (laterBtn) laterBtn.addEventListener("click", dismissReminder);
    var backdropEl = document.getElementById("shop-day-closing-reminder-backdrop");
    if (backdropEl) backdropEl.addEventListener("click", dismissReminder);
    document.addEventListener("keydown", function (ev) {
      var modalEl = document.getElementById("shop-day-closing-reminder-modal");
      if (ev.key === "Escape" && modalEl && modalEl.classList.contains("is-open")) {
        ev.preventDefault();
        dismissReminder();
      }
    });

    if (opts.autoInit !== false) {
      setTimeout(function () {
        refreshFromServer();
      }, opts.delayMs != null ? opts.delayMs : 800);
      if (statusApi) {
        setInterval(function () {
          refreshFromServer();
        }, pollMs);
      }
    }

    return {
      show: function () {
        if (!boot.can_submit) {
          window.alert(
            "Only a manager, admin, company manager, IT support, or super admin can close the shop."
          );
          return;
        }
        var ctx = pendingContext();
        if (ctx && ctx.business_date) clearDismissed(shopId, ctx.business_date);
        setMsg("", "");
        showModal(true);
      },
      hide: hideModal,
      dismiss: dismissReminder,
      refreshBoot: function (nextBoot) {
        boot = nextBoot || boot;
      },
      boot: boot,
    };
  };
})();
