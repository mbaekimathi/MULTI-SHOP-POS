/**
 * WhatsApp share — credit note PDF and M-Pesa pay link as separate actions.
 */
(function () {
  "use strict";

  var pdfCache = { key: "", blob: null, promise: null };

  function readBoot() {
    var el = document.getElementById("credit-note-whatsapp-data");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "{}");
    } catch (e) {
      return null;
    }
  }

  function fmtMoney(n) {
    var x = Number(n);
    if (!isFinite(x)) x = 0;
    return "KES " + x.toLocaleString("en-KE", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function fmtDate() {
    try {
      return new Date().toLocaleDateString("en-KE", {
        weekday: "short",
        day: "numeric",
        month: "short",
        year: "numeric",
      });
    } catch (e) {
      return "";
    }
  }

  function firstName(full) {
    var s = String(full || "").trim();
    if (!s || s.toUpperCase() === "WALK IN") return "";
    return s.split(/\s+/)[0];
  }

  function normalizeWaPhone(raw) {
    var d = String(raw || "").replace(/\D/g, "");
    if (!d || d === "-") return "";
    if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
    if (d.charAt(0) === "0" && d.length >= 10) return "254" + d.slice(1);
    if (d.length === 9) return "254" + d;
    return d;
  }

  function normalizeShareUrl(raw) {
    var u = String(raw || "").trim();
    if (!u) return "";
    if (!/^https?:\/\//i.test(u)) {
      u = "https://" + u.replace(/^\/+/, "");
    }
    return u;
  }

  function slugify(value) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^\w\s-]+/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
  }

  function pdfFileName(data) {
    var customer = slugify(data.customerName || "customer") || "customer";
    var ref = slugify(data.ref || "");
    var saleId = parseInt(data.focusSaleId, 10);
    if (saleId > 0) return "credit-sale-" + saleId + "-" + customer + ".pdf";
    return "credit-note-" + customer + (ref ? "-" + ref : "") + ".pdf";
  }

  function getPdfFetchUrl(data) {
    if (data && data.pdfUrl) return String(data.pdfUrl);
    var url = new URL(window.location.href);
    var path = url.pathname.replace(/\/$/, "");
    if (!path.endsWith(".pdf")) path += ".pdf";
    url.pathname = path;
    return url.toString();
  }

  function payAmount(data) {
    var saleBal = Number(data.focusSaleBalance);
    if (parseInt(data.focusSaleId, 10) > 0 && isFinite(saleBal) && saleBal > 0.009) {
      return saleBal;
    }
    var suggested = Number(data.suggestedAmount);
    if (isFinite(suggested) && suggested > 0.009) return suggested;
    return Number(data.balanceDue) || 0;
  }

  function applyPhoneToPayUrl(fullUrl, sharePhone) {
    var url = normalizeShareUrl(fullUrl);
    if (!url) return "";
    var phone = String(sharePhone || "").trim();
    if (!phone || phone === "-") return url;
    try {
      var u = new URL(url);
      u.searchParams.set("phone", phone);
      return u.toString();
    } catch (e) {
      return url;
    }
  }

  function buildPayLink(data, sharePhone) {
    if (!data) return "";
    var full = normalizeShareUrl(data.payLinkFull || "");
    if (full) return applyPhoneToPayUrl(full, sharePhone);

    if (window.CreditMpesaPayLink && typeof window.CreditMpesaPayLink.resolvePayLink === "function") {
      var resolved = window.CreditMpesaPayLink.resolvePayLink();
      if (resolved) return resolved;
    }

    var base = normalizeShareUrl(data.payLinkBase || "");
    if (!base) return "";
    var params = [];
    var amt = payAmount(data);
    if (isFinite(amt) && amt > 0.009) {
      params.push("amount=" + encodeURIComponent(amt.toFixed(2)));
    }
    var phone = String(sharePhone || "").trim();
    if (phone && phone !== "-") {
      params.push("phone=" + encodeURIComponent(phone));
    }
    return params.length ? base + "?" + params.join("&") : base;
  }

  function publicPdfLink(data) {
    return normalizeShareUrl((data && data.publicPdfUrl) || "");
  }

  function greetingLines(data) {
    var lines = [];
    var name = firstName(data.customerName || "");
    if (name) {
      lines.push("Hello " + name + ",");
    } else {
      lines.push("Hello,");
    }
    lines.push("");
    return lines;
  }

  /** WhatsApp message for credit note PDF only (no payment link). */
  function buildPdfMessage(data, sharePhone, usePublicLink) {
    var lines = greetingLines(data);
    var company = String(data.company || "").trim();
    var shop = String(data.shopName || "").trim();
    var balanceDue = Number(data.balanceDue) || 0;
    var today = fmtDate();
    var pdfLink = publicPdfLink(data);

    lines.push("*Credit note*");
    if (shop) lines.push("Shop: " + shop);
    if (company && company !== shop) lines.push(company);
    if (today) lines.push("Date: " + today);
    if (data.ref) lines.push("Ref: " + data.ref);
    lines.push("");
    lines.push("*Balance due:* " + fmtMoney(balanceDue));
    lines.push("");

    if (usePublicLink && pdfLink) {
      lines.push("📎 Open your credit note PDF:");
      lines.push(pdfLink);
    } else {
      lines.push("📎 Your credit note PDF is attached to this message.");
    }

    lines.push("");
    lines.push("Thank you.");
    return lines.join("\n");
  }

  /** WhatsApp message for M-Pesa payment link only (no PDF). */
  function buildPayLinkMessage(data, sharePhone, payLink) {
    var lines = greetingLines(data);
    var link = payLink || buildPayLink(data, sharePhone);
    var payAmt = payAmount(data);
    var balanceDue = Number(data.balanceDue) || 0;

    if (!link) {
      lines.push("Please contact us to arrange payment.");
      lines.push("");
      lines.push("Thank you.");
      return lines.join("\n");
    }

    lines.push("*Pay your credit balance*");
    lines.push("Amount due: " + fmtMoney(balanceDue > 0.009 ? payAmt : balanceDue));
    lines.push("");
    lines.push("💳 *Pay with M-Pesa (STK Push)*");
    lines.push("Tap this link on your phone to pay:");
    lines.push(link);
    lines.push("");
    lines.push("Thank you.");
    return lines.join("\n");
  }

  function readSharePhone(block) {
    var input = block
      ? block.querySelector("[data-credit-note-wa-phone]")
      : document.querySelector("[data-credit-note-wa-phone]");
    if (input && String(input.value || "").trim()) {
      return String(input.value || "").trim();
    }
    var data = readBoot();
    return data ? String(data.customerPhone || "").trim() : "";
  }

  function whatsAppUrl(sharePhone, text) {
    var phone = normalizeWaPhone(sharePhone);
    var encoded = encodeURIComponent(text);
    if (phone && phone.length >= 12) {
      return "https://api.whatsapp.com/send?phone=" + phone + "&text=" + encoded;
    }
    return "https://api.whatsapp.com/send?text=" + encoded;
  }

  function setShareStatus(block, message) {
    if (!block) return;
    var el = block.querySelector("[data-credit-note-wa-status]");
    if (!el) return;
    if (!message) {
      el.textContent = "";
      el.classList.add("hidden");
      return;
    }
    el.textContent = message;
    el.classList.remove("hidden");
  }

  function setPdfButtonReady(ready) {
    document.querySelectorAll("[data-credit-note-wa-pdf]").forEach(function (btn) {
      btn.disabled = !ready;
      btn.setAttribute("aria-busy", ready ? "false" : "true");
      if (!btn.dataset.cnPdfLabel) {
        btn.dataset.cnPdfLabel = btn.innerHTML;
      }
      btn.innerHTML = ready
        ? btn.dataset.cnPdfLabel
        : '<span class="credit-note-wa-share__icon" aria-hidden="true">…</span> Preparing PDF…';
    });
  }

  function syncPayButton(data) {
    var payLink = buildPayLink(data, readSharePhone(null));
    var hasPay = !!payLink;
    document.querySelectorAll("[data-credit-note-wa-pay]").forEach(function (btn) {
      btn.disabled = !hasPay;
      btn.title = hasPay ? "" : "M-Pesa payment link is not configured";
    });
  }

  function generatePdfBlob(data) {
    return fetch(getPdfFetchUrl(data), {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/pdf" },
    })
      .then(function (res) {
        if (!res.ok) {
          return res.text().then(function (body) {
            var msg = (body || "").replace(/<[^>]+>/g, " ").trim();
            throw new Error(msg || ("Could not load PDF (HTTP " + res.status + ")."));
          });
        }
        return res.blob();
      })
      .then(function (blob) {
        if (!blob || blob.size < 32) throw new Error("PDF file was empty.");
        return blob;
      });
  }

  function prefetchPdf(data) {
    if (!data) return Promise.reject(new Error("No credit note data."));
    var key = getPdfFetchUrl(data);
    if (pdfCache.key === key && pdfCache.blob) {
      return Promise.resolve(pdfCache.blob);
    }
    if (pdfCache.key === key && pdfCache.promise) {
      return pdfCache.promise;
    }
    pdfCache.key = key;
    pdfCache.blob = null;
    pdfCache.promise = generatePdfBlob(data)
      .then(function (blob) {
        pdfCache.blob = blob;
        return blob;
      })
      .catch(function (err) {
        pdfCache.promise = null;
        throw err;
      });
    return pdfCache.promise;
  }

  function getCachedPdfBlob(data) {
    if (!data) return null;
    if (pdfCache.key === getPdfFetchUrl(data) && pdfCache.blob) {
      return pdfCache.blob;
    }
    return null;
  }

  function openWhatsApp(text, sharePhone) {
    var url = whatsAppUrl(sharePhone, text);
    try {
      window.open(url, "_blank", "noopener,noreferrer");
    } catch (e) {
      window.location.href = url;
    }
  }

  function makePdfFile(blob, filename) {
    try {
      return new File([blob], filename, { type: "application/pdf" });
    } catch (e) {
      return null;
    }
  }

  function canNativeSharePdf(file, message) {
    if (!file || !navigator.share || !navigator.canShare) return false;
    try {
      return navigator.canShare({ title: "Credit note", text: message, files: [file] });
    } catch (e) {
      return false;
    }
  }

  function nativeSharePdfOnly(data, blob, block) {
    var filename = pdfFileName(data);
    var sharePhone = readSharePhone(block);
    var message = buildPdfMessage(data, sharePhone, false);
    var file = makePdfFile(blob, filename);

    if (!canNativeSharePdf(file, message)) {
      return Promise.resolve(false);
    }

    return navigator
      .share({ title: "Credit note", text: message, files: [file] })
      .then(function () {
        setShareStatus(block, "Credit note PDF sent. Use “Send payment link” for M-Pesa.");
        return true;
      })
      .catch(function (err) {
        if (err && err.name === "AbortError") return true;
        return false;
      });
  }

  function whatsAppPdfLink(data, block) {
    var sharePhone = readSharePhone(block);
    var message = buildPdfMessage(data, sharePhone, true);
    openWhatsApp(message, sharePhone);
    setShareStatus(
      block,
      "WhatsApp opened with credit note PDF link. Send it, then use “Send payment link”."
    );
  }

  function sharePdfOnly(block, btn) {
    var data = readBoot();
    if (!data) {
      window.alert("Credit note data is not available on this page.");
      return;
    }

    var blob = getCachedPdfBlob(data);
    var wasReady = !!blob;

    function deliver(b) {
      if (wasReady) {
        nativeSharePdfOnly(data, b, block).then(function (shared) {
          if (!shared) {
            whatsAppPdfLink(data, block);
          }
        });
        return;
      }
      whatsAppPdfLink(data, block);
    }

    if (blob) {
      deliver(blob);
      return;
    }

    if (btn) {
      btn.disabled = true;
      btn.textContent = "Preparing…";
    }

    prefetchPdf(data)
      .then(deliver)
      .catch(function (err) {
        window.alert(
          (err && err.message ? err.message : "Could not prepare the credit note PDF.") +
            "\n\nCheck you are logged in and try again."
        );
      })
      .finally(function () {
        setPdfButtonReady(!!getCachedPdfBlob(readBoot()));
      });
  }

  function sharePayLinkOnly(block) {
    var data = readBoot();
    if (!data) {
      window.alert("Credit note data is not available on this page.");
      return;
    }

    var sharePhone = readSharePhone(block);
    var payLink = buildPayLink(data, sharePhone);

    if (!payLink) {
      window.alert(
        "M-Pesa payment link is not available. Check Daraja settings and PUBLIC_APP_URL."
      );
      return;
    }

    if (/localhost|127\.0\.0\.1/i.test(payLink)) {
      if (
        !window.confirm(
          "The M-Pesa link uses localhost and will not work on the customer's phone.\n\n" +
            "Set PUBLIC_APP_URL in .env or use ngrok, then refresh.\n\nSend anyway?"
        )
      ) {
        return;
      }
    }

    openWhatsApp(buildPayLinkMessage(data, sharePhone, payLink), sharePhone);
    setShareStatus(block, "WhatsApp opened with M-Pesa payment link only.");
  }

  function warmPdfCache() {
    var data = readBoot();
    syncPayButton(data);
    if (!data) {
      setPdfButtonReady(false);
      return;
    }
    prefetchPdf(data)
      .then(function () {
        setPdfButtonReady(true);
      })
      .catch(function () {
        setPdfButtonReady(false);
        document.querySelectorAll("[data-credit-note-wa-block]").forEach(function (block) {
          setShareStatus(block, "Could not prepare PDF. Refresh the page and try again.");
        });
      });
  }

  function bind() {
    warmPdfCache();

    document.querySelectorAll("[data-credit-note-wa-phone]").forEach(function (input) {
      input.addEventListener("input", function () {
        syncPayButton(readBoot());
      });
    });

    document.querySelectorAll("[data-credit-note-wa-pdf]").forEach(function (btn) {
      btn.addEventListener("mouseenter", warmPdfCache);
      btn.addEventListener("focus", warmPdfCache);
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        sharePdfOnly(this.closest("[data-credit-note-wa-block]"), btn);
      });
    });

    document.querySelectorAll("[data-credit-note-wa-pay]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        sharePayLinkOnly(this.closest("[data-credit-note-wa-block]"));
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
