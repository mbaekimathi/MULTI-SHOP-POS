(function () {
  function fmtMoney(n) {
    try {
      return Number(n || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } catch (e) {
      return String(n || "0.00");
    }
  }

  function apiUrl(template, shopId) {
    return String(template || "").replace("__SHOP_ID__", String(shopId));
  }

  function setPanelMsg(el, text, kind) {
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

  function badgeHtml(state, label) {
    var styles = {
      open: "border-emerald-500/35 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
      closed: "border-violet-500/35 bg-violet-500/15 text-violet-700 dark:text-violet-300",
      not_opened: "border-amber-500/35 bg-amber-500/15 text-amber-800 dark:text-amber-200",
      pending_closing: "border-orange-500/35 bg-orange-500/15 text-orange-800 dark:text-orange-200",
    };
    var cls = styles[state] || styles.not_opened;
    return (
      '<span class="js-till-badge inline-flex min-w-[7.5rem] items-center justify-center rounded-xl border px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider ' +
      cls +
      '">' +
      (label || state) +
      "</span>"
    );
  }

  function toggleButtonHtml(summary) {
    if (summary.can_close_till) {
      return (
        '<button type="button" class="js-till-toggle btn-rc btn-rc-warning px-3 py-1.5 text-xs" data-action="close">Close till</button>'
      );
    }
    if (summary.can_open_till) {
      var openLabel = summary.till_state === "closed" ? "Reopen till" : "Open till";
      return (
        '<button type="button" class="js-till-toggle btn-rc btn-rc-success px-3 py-1.5 text-xs" data-action="open">' +
        openLabel +
        "</button>"
      );
    }
    return '<span class="text-[10px] text-[rgb(var(--rc-muted))]">—</span>';
  }

  window.initItSupportShopDayToggle = function (opts) {
    opts = opts || {};
    var statusTpl = opts.statusUrlTemplate || "";
    var openingTpl = opts.openingUrlTemplate || "";
    var closingTpl = opts.closingUrlTemplate || "";

    var modal = document.getElementById("it-shop-day-modal");
    if (!modal) return;

    var backdrop = document.getElementById("it-shop-day-modal-backdrop");
    var titleEl = document.getElementById("it-shop-day-modal-title");
    var descEl = document.getElementById("it-shop-day-modal-desc");
    var hintEl = document.getElementById("it-shop-day-modal-hint");
    var kickerEl = document.getElementById("it-shop-day-modal-kicker");
    var accentEl = document.getElementById("it-shop-day-modal-accent");
    var closingForm = document.getElementById("it-shop-day-closing-form");
    var openingForm = document.getElementById("it-shop-day-opening-form");
    var closingCash = document.getElementById("it-shop-day-closing-cash");
    var closingMpesa = document.getElementById("it-shop-day-closing-mpesa");
    var closingMsg = document.getElementById("it-shop-day-closing-msg");
    var openingCash = document.getElementById("it-shop-day-opening-cash");
    var openingMpesa = document.getElementById("it-shop-day-opening-mpesa");
    var openingMsg = document.getElementById("it-shop-day-opening-msg");
    var stockWrap = document.getElementById("it-shop-day-stock-wrap");
    var stockEl = document.getElementById("it-shop-day-stock-confirmed");

    var activeShopId = 0;
    var activeShopName = "";
    var activeSummary = null;
    var activeStep = "";
    var afterClosingStep = "";
    var submitInFlight = false;

    function hideModal() {
      modal.classList.remove("is-open");
      modal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("it-shop-day-modal-open");
      activeShopId = 0;
      activeStep = "";
      afterClosingStep = "";
    }

    function showModal() {
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("it-shop-day-modal-open");
      requestAnimationFrame(function () {
        modal.classList.add("is-open");
      });
    }

    function closingContextFromSummary(summary) {
      if (summary.pending_closing) return summary.pending_closing;
      if (summary.closing_context && !summary.closing_context.submitted) return summary.closing_context;
      var boot = summary.boot || {};
      var cr = boot.closing_reminder || {};
      if (cr.pending && !cr.pending.submitted) return cr.pending;
      return null;
    }

    function populateClosing(ctx) {
      ctx = ctx || {};
      if (hintEl) {
        hintEl.classList.remove("hidden");
        hintEl.textContent =
          "Opening: KES " +
          fmtMoney(ctx.opening_cash) +
          " cash + KES " +
          fmtMoney(ctx.opening_mpesa) +
          " M-Pesa. Sales: KES " +
          fmtMoney(ctx.cash_revenue) +
          " cash + KES " +
          fmtMoney(ctx.mpesa_revenue) +
          " M-Pesa. Suggested closing: KES " +
          fmtMoney(ctx.suggested_closing_cash) +
          " cash + KES " +
          fmtMoney(ctx.suggested_closing_mpesa) +
          " M-Pesa.";
      }
      if (closingCash && ctx.suggested_closing_cash != null) closingCash.value = String(ctx.suggested_closing_cash);
      if (closingMpesa && ctx.suggested_closing_mpesa != null) closingMpesa.value = String(ctx.suggested_closing_mpesa);
    }

    function showStep(step, summary) {
      activeStep = step;
      summary = summary || activeSummary || {};
      if (closingForm) closingForm.classList.toggle("hidden", step !== "closing");
      if (openingForm) openingForm.classList.toggle("hidden", step !== "opening");
      setPanelMsg(closingMsg, "", "");
      setPanelMsg(openingMsg, "", "");

      if (step === "closing") {
        var ctx = closingContextFromSummary(summary);
        var label = (ctx && (ctx.label || ctx.business_date)) || "today";
        if (titleEl) titleEl.textContent = "Closing balances — " + (activeShopName || "Shop");
        if (descEl) {
          descEl.textContent =
            summary.till_state === "pending_closing"
              ? "Submit closing balances for " + label + " before today's till can open."
              : "Enter closing cash and M-Pesa to close the till for " + label + ".";
        }
        if (kickerEl) kickerEl.textContent = "Close till";
        if (accentEl) accentEl.className = "h-1 shrink-0 bg-gradient-to-r from-violet-500 via-indigo-500 to-sky-500";
        populateClosing(ctx);
        if (closingCash) closingCash.focus();
      } else if (step === "opening") {
        var isReopen = summary.till_state === "closed";
        if (titleEl) titleEl.textContent = (isReopen ? "Reopen till — " : "Opening balances — ") + (activeShopName || "Shop");
        if (descEl) {
          descEl.textContent = isReopen
            ? "The till was closed for today. Confirm or update opening balances to resume sales."
            : "Enter opening cash and M-Pesa to open the till for today.";
        }
        if (kickerEl) kickerEl.textContent = isReopen ? "Reopen till" : "Open till";
        if (accentEl) accentEl.className = "h-1 shrink-0 bg-gradient-to-r from-emerald-500 via-sky-500 to-teal-500";
        if (hintEl) hintEl.classList.add("hidden");
        var needStock = summary.requires_stock_confirmation === true;
        if (stockWrap) stockWrap.classList.toggle("hidden", !needStock);
        if (stockEl) stockEl.checked = false;
        var rec = summary.opening_record || {};
        if (openingCash) openingCash.value = rec.opening_cash != null ? String(rec.opening_cash) : "0";
        if (openingMpesa) openingMpesa.value = rec.opening_mpesa != null ? String(rec.opening_mpesa) : "0";
        var openSubmit = document.getElementById("it-shop-day-opening-submit");
        if (openSubmit) openSubmit.textContent = isReopen ? "Reopen till & save" : "Open till & save";
        if (openingCash) openingCash.focus();
      }
    }

    function updateRow(shopId, summary) {
      var row = document.querySelector('.js-shop-row[data-shop-id="' + shopId + '"]');
      if (!row) return;
      row.setAttribute("data-till-state", summary.till_state || "");
      var badgeCell = row.querySelector(".js-till-badge-cell");
      if (badgeCell) badgeCell.innerHTML = badgeHtml(summary.till_state, summary.till_label);
      var toggleCell = row.querySelector(".js-till-toggle-cell");
      if (toggleCell) toggleCell.innerHTML = toggleButtonHtml(summary);
      bindToggleButtons(toggleCell);
    }

    function fetchStatus(shopId) {
      return fetch(apiUrl(statusTpl, shopId), {
        headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
      })
        .then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) throw new Error((j && j.error) || "Could not load till status.");
            return j;
          });
        });
    }

    function openFlow(action, shopId, shopName) {
      activeShopId = shopId;
      activeShopName = shopName || "";
      fetchStatus(shopId)
        .then(function (summary) {
          activeSummary = summary;
          if (action === "close") {
            if (!summary.can_close_till) {
              window.alert("This till is not open — nothing to close.");
              return;
            }
            afterClosingStep = "";
            showStep("closing", summary);
            showModal();
            return;
          }
          if (!summary.can_open_till) {
            window.alert(summary.till_label || "This till cannot be opened right now.");
            return;
          }
          if (summary.till_state === "pending_closing" || summary.pending_closing) {
            afterClosingStep = "opening";
            showStep("closing", summary);
            showModal();
            return;
          }
          if (summary.till_state === "closed") {
            showStep("opening", summary);
            showModal();
            return;
          }
          if (summary.till_state === "not_opened" || !summary.opening_record) {
            showStep("opening", summary);
            showModal();
            return;
          }
          window.alert("Till is already open for today.");
        })
        .catch(function (err) {
          window.alert(String((err && err.message) || err || "Could not load till status."));
        });
    }

    function submitClosing(ev) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (!activeShopId || submitInFlight) return;
      var ctx = closingContextFromSummary(activeSummary || {}) || {};
      var cash = parseFloat((closingCash && closingCash.value) || "0");
      var mpesa = parseFloat((closingMpesa && closingMpesa.value) || "0");
      if (isNaN(cash) || cash < 0 || isNaN(mpesa) || mpesa < 0) {
        setPanelMsg(closingMsg, "Enter valid closing amounts (0 is allowed).", "error");
        return;
      }
      submitInFlight = true;
      setPanelMsg(closingMsg, "Saving closing balances…", "muted");
      fetch(apiUrl(closingTpl, activeShopId), {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest" },
        body: JSON.stringify({
          business_date: ctx.business_date || (activeSummary && activeSummary.business_date) || "",
          closing_cash: cash,
          closing_mpesa: mpesa,
        }),
      })
        .then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) throw new Error((j && j.error) || "Could not save closing balances.");
            return j;
          });
        })
        .then(function (summary) {
          activeSummary = summary;
          updateRow(activeShopId, summary);
          if (afterClosingStep === "opening") {
            afterClosingStep = "";
            return fetchStatus(activeShopId).then(function (fresh) {
              activeSummary = fresh;
              showStep("opening", fresh);
            });
          }
          setPanelMsg(closingMsg, "Closing saved.", "ok");
          window.setTimeout(hideModal, 500);
        })
        .catch(function (err) {
          setPanelMsg(closingMsg, String((err && err.message) || "Could not save closing."), "error");
        })
        .finally(function () {
          submitInFlight = false;
        });
    }

    function submitOpening(ev) {
      if (ev && ev.preventDefault) ev.preventDefault();
      if (!activeShopId || submitInFlight) return;
      var cash = parseFloat((openingCash && openingCash.value) || "0");
      var mpesa = parseFloat((openingMpesa && openingMpesa.value) || "0");
      if (isNaN(cash) || cash < 0 || isNaN(mpesa) || mpesa < 0) {
        setPanelMsg(openingMsg, "Enter valid opening amounts (0 is allowed).", "error");
        return;
      }
      var needStock = activeSummary && activeSummary.requires_stock_confirmation === true;
      if (needStock && (!stockEl || !stockEl.checked)) {
        setPanelMsg(openingMsg, "Confirm shop stock is up to date.", "error");
        return;
      }
      submitInFlight = true;
      setPanelMsg(openingMsg, "Saving opening balances…", "muted");
      fetch(apiUrl(openingTpl, activeShopId), {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest" },
        body: JSON.stringify({
          opening_cash: cash,
          opening_mpesa: mpesa,
          stock_confirmed: needStock ? true : false,
        }),
      })
        .then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok || !j.ok) throw new Error((j && j.error) || "Could not save opening balances.");
            return j;
          });
        })
        .then(function (summary) {
          activeSummary = summary;
          updateRow(activeShopId, summary);
          setPanelMsg(openingMsg, summary.reopened ? "Till reopened — sales can continue." : "Till opened for today.", "ok");
          window.setTimeout(hideModal, 500);
        })
        .catch(function (err) {
          setPanelMsg(openingMsg, String((err && err.message) || "Could not save opening."), "error");
        })
        .finally(function () {
          submitInFlight = false;
        });
    }

    function bindToggleButtons(root) {
      (root || document).querySelectorAll(".js-till-toggle").forEach(function (btn) {
        if (btn.dataset.bound === "1") return;
        btn.dataset.bound = "1";
        btn.addEventListener("click", function () {
          var row = btn.closest(".js-shop-row");
          var shopId = row && row.getAttribute("data-shop-id");
          var shopName = row && row.getAttribute("data-shop-name");
          if (!shopId) return;
          openFlow(btn.getAttribute("data-action") || "open", parseInt(shopId, 10), shopName || "");
        });
      });
    }

    if (closingForm) closingForm.addEventListener("submit", submitClosing);
    if (openingForm) openingForm.addEventListener("submit", submitOpening);
    document.querySelectorAll(".it-shop-day-cancel, #it-shop-day-modal-dismiss").forEach(function (btn) {
      btn.addEventListener("click", hideModal);
    });
    if (backdrop) backdrop.addEventListener("click", hideModal);
    document.addEventListener("keydown", function (ev) {
      if (ev.key === "Escape" && modal.classList.contains("is-open")) hideModal();
    });

    bindToggleButtons(document);
  };
})();
