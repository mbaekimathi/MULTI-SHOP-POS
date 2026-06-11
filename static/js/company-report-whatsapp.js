/**
 * Share company / shop period report as PDF on WhatsApp (server PDF + Web Share API).
 */
(function () {
  "use strict";

  function readBoot() {
    var el =
      document.getElementById("company-report-whatsapp-data") ||
      document.getElementById("shop-report-whatsapp-data");
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

  function normalizeWaPhone(raw) {
    var d = String(raw || "").replace(/\D/g, "");
    if (!d || d === "-") return "";
    if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
    if (d.charAt(0) === "0" && d.length >= 10) return "254" + d.slice(1);
    if (d.length === 9) return "254" + d;
    return d;
  }

  function whatsAppUrl(sharePhone, text) {
    var phone = normalizeWaPhone(sharePhone);
    var encoded = encodeURIComponent(text);
    if (phone && phone.length >= 12) {
      return "https://api.whatsapp.com/send?phone=" + phone + "&text=" + encoded;
    }
    return "https://api.whatsapp.com/send?text=" + encoded;
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
    var base = slugify(data.shopName || data.companyName || "report") || "report";
    var period = slugify(data.periodLabel || "");
    return base + "-report" + (period ? "-" + period : "") + ".pdf";
  }

  function getPdfFetchUrl(data) {
    if (data && data.pdfUrl) return String(data.pdfUrl);
    var url = new URL(window.location.href);
    var path = url.pathname.replace(/\/$/, "");
    if (!path.endsWith(".pdf")) path += ".pdf";
    url.pathname = path;
    return url.toString();
  }

  function buildShortCaption(data) {
    var title = String(data.reportTitle || "REPORT").trim() || "REPORT";
    var name = String(data.shopName || data.companyName || "Report").trim() || "Report";
    var period = String(data.periodLabel || "Selected period").trim();
    var lines = [
      "*" + title + "*",
      name,
      "Period: " + period,
    ];
    if (data.scopeLabel) lines.push("Scope: " + data.scopeLabel);
    lines.push("");
    lines.push(
      "Revenue " + fmtMoney(data.totalRevenue) +
      " · Expenditure " + fmtMoney(data.totalExpenditure) +
      " · Net " + fmtMoney(data.netProfit)
    );
    lines.push("");
    lines.push("Full report attached as PDF.");
    if (data.generatedAt) lines.push("Generated " + data.generatedAt);
    return lines.join("\n");
  }

  function buildMessage(data, visibleRows) {
    var lines = [];
    var title = String(data.reportTitle || "COMPANY REPORT").trim() || "COMPANY REPORT";
    var name =
      String(data.shopName || data.companyName || "Report").trim() || "Report";
    var period = String(data.periodLabel || "Selected period").trim();
    lines.push("*" + title + "*");
    lines.push(name);
    lines.push("Period: " + period);
    if (data.scopeLabel) {
      lines.push("Scope: " + data.scopeLabel);
    }
    lines.push("");
    lines.push("*Summary*");
    lines.push("Total revenue: " + fmtMoney(data.totalRevenue));
    lines.push("  Cash sales: " + fmtMoney(data.saleRevenue));
    lines.push("  Credit sales: " + fmtMoney(data.creditRevenue));
    lines.push("  Cash paid: " + fmtMoney(data.cashRevenue));
    lines.push("  M-Pesa paid: " + fmtMoney(data.mpesaRevenue));
    lines.push("Expenditure: " + fmtMoney(data.totalExpenditure));
    lines.push("Net profit: " + fmtMoney(data.netProfit));
    lines.push("");

    var items = visibleRows && visibleRows.length ? visibleRows : (data.items || []);
    var maxItems = 20;
    if (items.length) {
      lines.push("*Items (" + Math.min(items.length, maxItems) + (items.length > maxItems ? "+" : "") + ")*");
      for (var i = 0; i < items.length && i < maxItems; i++) {
        var it = items[i];
        var itemName = String(it.name || "Item").trim();
        var sold = parseInt(it.stockSold, 10);
        if (isNaN(sold)) sold = 0;
        var rev = Number(it.revenue);
        if (!isFinite(rev)) rev = 0;
        lines.push(
          (i + 1) + ". " + itemName +
          " | Start: " + (it.startingStock || 0) +
          " End: " + (it.endingStock || 0) +
          " | In: " + (it.stockIn || 0) +
          " Out: " + (it.stockOut || 0) +
          " Sold: " + sold +
          " | " + fmtMoney(rev)
        );
      }
      if (items.length > maxItems) {
        lines.push("… +" + (items.length - maxItems) + " more item(s) on full report");
      }
      lines.push("");
    }

    if (data.shareUrl) {
      lines.push("Full report:");
      lines.push(String(data.shareUrl));
      lines.push("");
    }

    if (data.generatedAt) {
      lines.push("Generated " + data.generatedAt);
    }

    return lines.join("\n");
  }

  function scrapeVisibleRows() {
    var out = [];
    var table =
      document.getElementById("company-report-table") ||
      document.getElementById("shop-report-table");
    if (!table) return out;
    var trs = table.querySelectorAll("tbody tr.cr-row");
    for (var i = 0; i < trs.length; i++) {
      var tr = trs[i];
      if (tr.classList.contains("hidden")) continue;
      var cells = tr.querySelectorAll("td");
      if (cells.length < 7) continue;
      var nameEl = cells[0].querySelector(".font-medium");
      out.push({
        name: nameEl ? String(nameEl.textContent || "").trim() : String(cells[0].textContent || "").trim(),
        startingStock: String(cells[1].textContent || "0").trim(),
        endingStock: String(cells[2].textContent || "0").trim(),
        stockIn: String(cells[3].textContent || "0").trim(),
        stockOut: String(cells[4].textContent || "0").trim(),
        stockSold: String(cells[5].textContent || "0").trim(),
        revenue: String(cells[6].textContent || "0").replace(/,/g, "").trim(),
      });
    }
    return out;
  }

  function readSharePhone() {
    var input = document.querySelector(
      "[data-company-report-wa-phone], [data-shop-report-wa-phone]"
    );
    return input ? String(input.value || "").trim() : "";
  }

  function setButtonBusy(btn, busy) {
    if (!btn) return;
    if (busy) {
      if (!btn.dataset.crWaHtml) {
        btn.dataset.crWaHtml = btn.innerHTML;
      }
      btn.disabled = true;
      btn.setAttribute("aria-busy", "true");
      btn.innerHTML = "Preparing PDF…";
    } else {
      btn.disabled = false;
      btn.removeAttribute("aria-busy");
      if (btn.dataset.crWaHtml) {
        btn.innerHTML = btn.dataset.crWaHtml;
      }
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
    var pdfUrl = getPdfFetchUrl(data);
    return fetch(pdfUrl, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/pdf" },
    }).then(function (res) {
      if (!res.ok) {
        return res.text().then(function (body) {
          var msg = (body || "").replace(/<[^>]+>/g, " ").trim();
          throw new Error(msg || ("Could not download PDF (HTTP " + res.status + ")."));
        });
      }
      return res.blob();
    }).then(function (blob) {
      if (!blob || blob.size < 32) {
        throw new Error("PDF file was empty.");
      }
      return blob;
    });
  }

  function openWhatsApp(text) {
    var url = whatsAppUrl(readSharePhone(), text);
    try {
      window.open(url, "_blank", "noopener,noreferrer");
    } catch (e) {
      window.location.href = url;
    }
  }

  function shareTextReport(data) {
    var visible = scrapeVisibleRows();
    openWhatsApp(buildMessage(data, visible));
  }

  function sharePdfBlob(data, blob) {
    var filename = pdfFileName(data);
    var caption = buildShortCaption(data);
    var file;

    try {
      file = new File([blob], filename, { type: "application/pdf" });
    } catch (e) {
      file = null;
    }

    if (file && navigator.share && navigator.canShare) {
      var payload = { title: caption, text: caption, files: [file] };
      if (navigator.canShare(payload)) {
        return navigator.share(payload).catch(function (err) {
          if (err && err.name === "AbortError") return;
          throw err;
        });
      }
    }

    downloadBlob(blob, filename);
    openWhatsApp(
      caption +
        "\n\nPDF saved as \"" + filename + "\". Attach it in WhatsApp before sending."
    );
  }

  function shareReport(btn) {
    var data = readBoot();
    if (!data) {
      window.alert("Report data is not available on this page.");
      return Promise.resolve();
    }

    setButtonBusy(btn, true);
    return generatePdfBlob(data)
      .then(function (blob) {
        return sharePdfBlob(data, blob);
      })
      .catch(function (err) {
        if (
          window.confirm(
            (err && err.message
              ? err.message + "\n\n"
              : "Could not create a PDF for this report.\n\n") +
              "Share a text summary on WhatsApp instead?"
          )
        ) {
          shareTextReport(data);
        }
      })
      .finally(function () {
        setButtonBusy(btn, false);
      });
  }

  function bind() {
    document.querySelectorAll(
      "[data-company-report-whatsapp-share], [data-shop-report-whatsapp-share]"
    ).forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        shareReport(btn);
      });
    });
    document.querySelectorAll(
      "[data-company-report-print], [data-shop-report-print]"
    ).forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        window.print();
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
