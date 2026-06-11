/**
 * Share customer credit note as PDF on WhatsApp with pay link in the message text.
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

  function buildPayLink(data, sharePhone) {
    var base = normalizeShareUrl((data && data.payLinkBase) || "");
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

  function buildShareMessage(data, sharePhone) {
    var lines = [];
    var company = String(data.company || "").trim();
    var shop = String(data.shopName || "").trim();
    var customer = String(data.customerName || "").trim() || "Customer";
    var greetingName = firstName(customer);
    var balanceDue = Number(data.balanceDue) || 0;
    var saleId = parseInt(data.focusSaleId, 10);
    var saleBalance = Number(data.focusSaleBalance) || 0;
    var today = fmtDate();
    var payLink = buildPayLink(data, sharePhone);

    if (greetingName) {
      lines.push("Hello " + greetingName + ",");
    } else {
      lines.push("Hello,");
    }
    lines.push("");

    if (!data.hasUnpaid && balanceDue <= 0.009 && saleBalance <= 0.009) {
      lines.push("Good news — your account is fully paid.");
      if (shop) lines.push("Shop: " + shop);
      if (company && company !== shop) lines.push(company);
      lines.push("");
      lines.push("Thank you for your business.");
      if (data.ref) lines.push("Ref: " + data.ref);
      lines.push("");
      lines.push("Your credit note is attached as a PDF.");
      return lines.join("\n");
    }

    lines.push("*Credit note*");
    if (shop) lines.push("Shop: " + shop);
    if (company && company !== shop) lines.push(company);
    if (today) lines.push("Date: " + today);
    if (data.ref) lines.push("Ref: " + data.ref);
    lines.push("");

    if (saleId > 0) {
      lines.push("*Sale #" + saleId + "*");
      lines.push("Balance on this sale: " + fmtMoney(saleBalance));
      lines.push("");
    }

    lines.push("*Account balance due*");
    lines.push(fmtMoney(balanceDue));
    lines.push("");

    var paidToDate = Number(data.paidToDate) || 0;
    if (paidToDate > 0.009) {
      lines.push("Already paid: " + fmtMoney(paidToDate));
    }

    lines.push("");
    lines.push("Please find the full credit note attached as a PDF.");
    lines.push("");

    if (payLink) {
      lines.push("*Pay with M-Pesa:*");
      lines.push(payLink);
      lines.push("");
      lines.push("Tap the link above to pay, then send this message with the PDF attached.");
    } else {
      lines.push("Please pay at the shop or contact us to clear your balance.");
    }

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

  function setButtonBusy(btn, busy) {
    if (!btn) return;
    if (busy) {
      if (!btn.dataset.cnWaHtml) btn.dataset.cnWaHtml = btn.innerHTML;
      btn.disabled = true;
      btn.setAttribute("aria-busy", "true");
      btn.innerHTML = "Preparing PDF…";
    } else {
      btn.disabled = false;
      btn.removeAttribute("aria-busy");
      if (btn.dataset.cnWaHtml) btn.innerHTML = btn.dataset.cnWaHtml;
    }
  }

  function downloadBlob(blob, filename) {
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.style.display = "none";
    document.body.appendChild(a);
    a.click();
    setTimeout(function () {
      URL.revokeObjectURL(url);
      a.remove();
    }, 1500);
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
            throw new Error(msg || ("Could not download PDF (HTTP " + res.status + ")."));
          });
        }
        return res.blob();
      })
      .then(function (blob) {
        if (!blob || blob.size < 32) throw new Error("PDF file was empty.");
        return blob;
      });
  }

  function openWhatsApp(text, sharePhone) {
    var url = whatsAppUrl(sharePhone, text);
    try {
      window.open(url, "_blank", "noopener,noreferrer");
    } catch (e) {
      window.location.href = url;
    }
  }

  function sharePdfBlob(data, blob, block) {
    var filename = pdfFileName(data);
    var sharePhone = readSharePhone(block);
    var message = buildShareMessage(data, sharePhone);
    var file;

    try {
      file = new File([blob], filename, { type: "application/pdf" });
    } catch (e) {
      file = null;
    }

    if (file && navigator.share && navigator.canShare) {
      var payload = { title: "Credit note", text: message, files: [file] };
      if (navigator.canShare(payload)) {
        return navigator.share(payload).catch(function (err) {
          if (err && err.name === "AbortError") return;
          throw err;
        });
      }
    }

    downloadBlob(blob, filename);
    openWhatsApp(
      message +
        "\n\n(PDF saved as \"" + filename + "\" — please attach it in WhatsApp before sending.)",
      sharePhone
    );
  }

  function shareCreditNote(block, btn) {
    var data = readBoot();
    if (!data) {
      window.alert("Credit note data is not available on this page.");
      return Promise.resolve();
    }

    var sharePhone = readSharePhone(block);
    var payLink = buildPayLink(data, sharePhone);
    if (payLink && /localhost|127\.0\.0\.1/i.test(payLink)) {
      var proceed = window.confirm(
        "The M-Pesa link uses localhost and will not work on the customer's phone.\n\n" +
          "Set PUBLIC_APP_URL in .env or use ngrok, then refresh.\n\nShare anyway?"
      );
      if (!proceed) return Promise.resolve();
    }

    setButtonBusy(btn, true);
    return generatePdfBlob(data)
      .then(function (blob) {
        return sharePdfBlob(data, blob, block);
      })
      .catch(function (err) {
        if (
          window.confirm(
            (err && err.message
              ? err.message + "\n\n"
              : "Could not create a PDF for this credit note.\n\n") +
              "Send the WhatsApp text message only (without PDF)?"
          )
        ) {
          openWhatsApp(buildShareMessage(data, sharePhone), sharePhone);
        }
      })
      .finally(function () {
        setButtonBusy(btn, false);
      });
  }

  function bind() {
    document.querySelectorAll("[data-credit-note-whatsapp-share]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        var block = this.closest("[data-credit-note-wa-block]");
        shareCreditNote(block, btn);
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
