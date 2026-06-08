(function () {
  var BOOT = window.__POS_BOOT || {};
  var POS_SID = BOOT.shopId;
  var API = BOOT.apis.incomingStockRequests;
  var LS_KEY = "pos_incoming_sr_sig_shop_" + POS_SID;
var modal = document.getElementById("pos-incoming-sr-modal");
      var detailEl = document.getElementById("pos-incoming-sr-detail");
      var queueHint = document.getElementById("pos-incoming-sr-queue-hint");
      var qtyEl = document.getElementById("pos-incoming-sr-qty");
      var qtyHint = document.getElementById("pos-incoming-sr-qty-hint");
      var codeEl = document.getElementById("pos-incoming-sr-code");
      var errEl = document.getElementById("pos-incoming-sr-err");
      var approveBtn = document.getElementById("pos-incoming-sr-approve");
      var declineBtn = document.getElementById("pos-incoming-sr-decline");
      var dismissBtn = document.getElementById("pos-incoming-sr-dismiss");
      var backdrop = document.getElementById("pos-incoming-sr-backdrop");

      /** null = auto-submit approve on 6 digits; "reject" = auto-submit decline on 6 digits (after Decline click). */
      var incomingSrIntent = null;

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

      function setOpen(on) {
        if (!modal) return;
        if (on) {
          modal.classList.remove("hidden");
          modal.setAttribute("aria-hidden", "false");
        } else {
          modal.classList.add("hidden");
          modal.setAttribute("aria-hidden", "true");
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

      function applyDetail(r, total) {
        if (!detailEl) return;
        incomingSrIntent = null;
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
              : "Enter your employee code to approve or decline.";
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
      }

      function setBusy(on) {
        window.__posIncomingSrSubmitting = !!on;
        [approveBtn, declineBtn, dismissBtn].forEach(function (b) {
          if (b) b.disabled = !!on;
        });
        if (qtyEl && !qtyEl.disabled) qtyEl.disabled = !!on;
        if (codeEl) codeEl.disabled = !!on;
      }

      function poll(force) {
        if (!modal || !detailEl || !qtyEl || !codeEl) return;
        fetch(API, { credentials: "same-origin", headers: { Accept: "application/json" } })
          .then(function (res) {
            return res.json();
          })
          .then(function (data) {
            if (!data || !data.ok) return;
            var reqs = data.requests || [];
            if (!reqs.length) {
              setOpen(false);
              showErr("");
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
            setOpen(true);
          })
          .catch(function () {});
      }

      function submitReview(action) {
        if (window.__posIncomingSrSubmitting) return;
        if (!codeEl || !qtyEl) return;
        showErr("");
        var r = window.__posIncomingSrCurrent;
        if (!r) return;
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
        }
        setBusy(true);
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
            try {
              localStorage.removeItem(LS_KEY);
            } catch (e) {}
            window.__posIncomingSrLastSig = "";
            showErr("");
            poll(true);
          })
          .catch(function () {
            setBusy(false);
            showErr("Network error. Try again.");
          });
      }

      approveBtn &&
        approveBtn.addEventListener("click", function () {
          incomingSrIntent = "approve";
          submitReview("approve");
        });
      declineBtn &&
        declineBtn.addEventListener("click", function () {
          incomingSrIntent = "reject";
          submitReview("reject");
        });

      codeEl &&
        codeEl.addEventListener("input", function () {
          var raw = (codeEl.value || "").replace(/\D/g, "").slice(0, 6);
          if (codeEl.value !== raw) codeEl.value = raw;
          showErr("");
          if (raw.length !== 6 || !/^\d{6}$/.test(raw)) return;
          if (window.__posIncomingSrSubmitting) return;
          var useReject = incomingSrIntent === "reject";
          if (!useReject) {
            if (approveBtn && approveBtn.disabled) return;
            if (qtyEl && qtyEl.disabled) return;
          }
          var action = useReject ? "reject" : "approve";
          incomingSrIntent = null;
          /* One POST to /review — it validates the 6-digit code (no separate auth round trip). */
          submitReview(action);
        });

      dismissBtn &&
        dismissBtn.addEventListener("click", function () {
          try {
            if (window.__posIncomingSrLastSig) localStorage.setItem(LS_KEY, window.__posIncomingSrLastSig);
          } catch (e) {}
          setOpen(false);
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

      setTimeout(function () { poll(false); }, 1200);
      setInterval(function () { poll(false); }, 25000);
    })();