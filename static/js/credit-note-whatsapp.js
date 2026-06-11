/**
 * Share customer credit note summary on WhatsApp (wa.me).
 * Plain ASCII formatting so WhatsApp linkifies payment URLs.
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

  function isLocalShareUrl(url) {
    return /localhost|127\.0\.0\.1|192\.168\.|10\.\d+\.|172\.(1[6-9]|2\d|3[0-1])\./i.test(
      String(url || "")
    );
  }

  function scrapeLineItems(maxRows) {
    var root = document.getElementById("shop-customer-credit-note-print");
    if (!root) return { items: [], total: 0 };
    var rows = root.querySelectorAll(".shop-cn-items-table tbody tr");
    var items = [];
    var max = maxRows == null ? 20 : maxRows;
    var hasShopCol =
      root.querySelector(".shop-cn-items-table thead th") &&
      String(root.querySelector(".shop-cn-items-table thead th").textContent || "")
        .trim()
        .toLowerCase() === "shop";
    var offset = hasShopCol ? 1 : 0;
    for (var i = 0; i < rows.length && items.length < max; i++) {
      var cells = rows[i].querySelectorAll("td");
      if (!cells.length) continue;
      var shopName = hasShopCol && cells[0] ? String(cells[0].textContent || "").trim() : "";
      var name = cells[offset] ? String(cells[offset].textContent || "").trim() : "";
      var qty = cells[offset + 1] ? String(cells[offset + 1].textContent || "").trim() : "";
      var amount = cells[offset + 2] ? String(cells[offset + 2].textContent || "").trim() : "";
      if (!name) continue;
      items.push({ shopName: shopName, name: name, qty: qty, amount: amount });
    }
    return { items: items, total: rows.length };
  }

  function formatItemLine(index, it) {
    var line = index + ". " + it.name;
    if (it.qty && it.qty !== "-") {
      var q = parseFloat(it.qty);
      var unit = !isNaN(q) && q === 1 ? "pc" : "pcs";
      line += " (" + it.qty + " " + unit + ")";
    }
    if (it.amount) {
      var amt = String(it.amount).replace(/[^\d.,-]/g, "").trim();
      line += " - " + (amt.indexOf("KES") === 0 ? amt : "KES " + amt);
    }
    if (it.shopName) line += " [" + it.shopName + "]";
    return line;
  }

  function buildPayLink(data, sharePhone) {
    var base = normalizeShareUrl((data && data.payLinkBase) || "");
    if (!base) return "";
    var params = [];
    var amt = Number(data.suggestedAmount);
    if (isFinite(amt) && amt > 0.009) {
      params.push("amount=" + encodeURIComponent(amt.toFixed(2)));
    }
    var phone = String(sharePhone || "").trim();
    if (phone && phone !== "-") {
      params.push("phone=" + encodeURIComponent(phone));
    }
    return params.length ? base + "?" + params.join("&") : base;
  }

  function buildMessage(data, sharePhone) {
    var lines = [];
    var company = String(data.company || "").trim();
    var shop = String(data.shopName || "").trim();
    var ref = String(data.ref || "").trim();
    var customer = String(data.customerName || "").trim() || "Customer";
    var greetingName = firstName(customer);
    var balanceDue = Number(data.balanceDue) || 0;
    var paidToDate = Number(data.paidToDate) || 0;
    var unpaidSales = Number(data.unpaidSales) || 0;
    var today = fmtDate();
    var scraped = scrapeLineItems(20);
    var items = scraped.items || [];

    if (greetingName) {
      lines.push("Hello " + greetingName + ",");
    } else {
      lines.push("Hello,");
    }
    lines.push("");

    if (!data.hasUnpaid && balanceDue <= 0.009) {
      lines.push("Good news - your account is fully paid.");
      if (shop) lines.push("Shop: " + shop);
      if (company && company !== shop) lines.push(company);
      lines.push("");
      lines.push("Thank you for your business.");
      if (ref) lines.push("Ref: " + ref);
      return lines.join("\n");
    }

    lines.push("Here is your credit account summary");
    if (shop) lines.push("Shop: " + shop);
    if (company && company !== shop) lines.push(company);
    if (today) lines.push("Date: " + today);
    lines.push("");
    lines.push("---");
    lines.push("AMOUNT TO PAY");
    lines.push(fmtMoney(balanceDue));
    lines.push("---");
    lines.push("");

    if (paidToDate > 0.009) {
      lines.push("Already paid: " + fmtMoney(paidToDate));
    }
    if (unpaidSales > 0) {
      lines.push(
        "Unpaid bill" + (unpaidSales === 1 ? "" : "s") + ": " + String(unpaidSales)
      );
    }
    lines.push("");

    if (items.length) {
      lines.push("What you owe for:");
      for (var j = 0; j < items.length; j++) {
        lines.push(formatItemLine(j + 1, items[j]));
      }
      if (scraped.total > items.length) {
        lines.push("...plus " + (scraped.total - items.length) + " more item(s)");
      }
      lines.push("");
    }

    var payLink = buildPayLink(data, sharePhone);
    if (payLink) {
      lines.push("Pay with M-Pesa:");
      lines.push("");
      lines.push(payLink);
      lines.push("");
      lines.push(
        "Tap the link above, check amount and phone, then press Pay with M-Pesa."
      );
    } else {
      lines.push("Please pay when you can, or visit us to clear your balance.");
    }
    lines.push("");
    lines.push("Thank you.");
    if (ref) {
      lines.push("Ref: " + ref);
    }

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

  function shareCreditNote(block) {
    var data = readBoot();
    if (!data) {
      window.alert("Credit note data is not available on this page.");
      return;
    }
    var sharePhone = readSharePhone(block);
    var payLink = buildPayLink(data, sharePhone);
    if (payLink && isLocalShareUrl(payLink)) {
      var proceed = window.confirm(
        "This payment link uses localhost (127.0.0.1). It will NOT be clickable for customers on their phones.\n\n" +
          "Start ngrok (ngrok http 5000) or set PUBLIC_APP_URL in .env to your live HTTPS site, then refresh this page.\n\n" +
          "Share anyway?"
      );
      if (!proceed) return;
    }
    var text = buildMessage(data, sharePhone);
    var url = whatsAppUrl(sharePhone, text);
    try {
      window.open(url, "_blank", "noopener,noreferrer");
    } catch (e) {
      window.location.href = url;
    }
  }

  function bind() {
    var buttons = document.querySelectorAll("[data-credit-note-whatsapp-share]");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function (e) {
        e.preventDefault();
        var block = this.closest("[data-credit-note-wa-block]");
        shareCreditNote(block);
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
