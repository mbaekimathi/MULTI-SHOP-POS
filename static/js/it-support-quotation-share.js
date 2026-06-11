(function () {
  var root = document.querySelector("[data-quotation-share-root]");
  if (!root) return;

  var catalog = window.__QUOTATION_SHARE_ITEMS__ || [];
  var apiUrl = window.__QUOTATION_SHARE_API__ || "";
  var byId = {};
  catalog.forEach(function (item) {
    byId[String(item.id)] = item;
  });

  var searchEl = root.querySelector("[data-qs-search]");
  var emptyEl = root.querySelector("[data-qs-empty]");
  var itemEls = Array.prototype.slice.call(root.querySelectorAll("[data-qs-item]"));
  var checkboxes = Array.prototype.slice.call(root.querySelectorAll("[data-qs-checkbox]"));
  var countEl = root.querySelector("[data-qs-selected-count]");
  var totalEl = root.querySelector("[data-qs-selected-total]");
  var previewEl = root.querySelector("[data-qs-preview-list]");
  var phoneEl = root.querySelector("[data-qs-phone]");
  var generateBtn = root.querySelector("[data-qs-generate]");
  var resultEl = root.querySelector("[data-qs-share-result]");
  var urlInput = root.querySelector("[data-qs-share-url]");
  var copyBtn = root.querySelector("[data-qs-copy]");
  var copiedEl = root.querySelector("[data-qs-copied]");
  var waLink = root.querySelector("[data-qs-wa-link]");
  var waHint = root.querySelector("[data-qs-wa-hint]");
  var openLink = root.querySelector("[data-qs-open-link]");
  var errorEl = root.querySelector("[data-qs-error]");
  var selectAllBtn = root.querySelector("[data-qs-select-all]");
  var clearBtn = root.querySelector("[data-qs-clear]");
  var lastWaText = "";

  function esc(s) {
    return String(s || "").replace(/</g, "&lt;").replace(/"/g, "&quot;");
  }

  function fmt(n) {
    var x = parseFloat(n);
    if (isNaN(x)) x = 0;
    return x.toFixed(2);
  }

  function normalizeWaPhone(raw) {
    var d = String(raw || "").replace(/\D+/g, "");
    if (!d || d === "-") return "";
    if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
    if (d.indexOf("0") === 0 && d.length >= 10) return "254" + d.slice(1, 11);
    if (d.length === 9) return "254" + d;
    return d;
  }

  function isValidWaPhone(raw) {
    return normalizeWaPhone(raw).length >= 12;
  }

  function buildWaUrl(phoneRaw, text) {
    var encoded = encodeURIComponent(String(text || "").trim());
    var phone = normalizeWaPhone(phoneRaw);
    if (phone.length >= 12) {
      return "https://api.whatsapp.com/send?phone=" + phone + "&text=" + encoded;
    }
    return "https://api.whatsapp.com/send?text=" + encoded;
  }

  function phoneValue() {
    return (phoneEl && phoneEl.value || "").trim();
  }

  function syncWaLink() {
    if (!waLink) return;
    var phone = phoneValue();
    var valid = isValidWaPhone(phone);
    if (lastWaText) {
      waLink.href = buildWaUrl(phone, lastWaText);
    }
    waLink.classList.toggle("qs-btn-wa--disabled", !valid || !lastWaText);
    waLink.setAttribute("aria-disabled", (!valid || !lastWaText) ? "true" : "false");
    if (waHint) waHint.classList.toggle("hidden", valid || !lastWaText);
  }

  function selectedIds() {
    return checkboxes
      .filter(function (cb) {
        return cb.checked;
      })
      .map(function (cb) {
        return parseInt(cb.value, 10);
      })
      .filter(function (id) {
        return id > 0;
      });
  }

  function filterItems() {
    var q = (searchEl && searchEl.value || "").trim().toLowerCase();
    var visible = 0;
    itemEls.forEach(function (el) {
      var name = (el.getAttribute("data-qs-name") || "").toLowerCase();
      var cat = (el.getAttribute("data-qs-category") || "").toLowerCase();
      var show = !q || name.indexOf(q) >= 0 || cat.indexOf(q) >= 0;
      el.classList.toggle("hidden", !show);
      if (show) visible += 1;
    });
    if (emptyEl) emptyEl.classList.toggle("hidden", visible > 0);
  }

  function previewThumb(item) {
    var img = item && item.image_url ? String(item.image_url) : "";
    if (img) {
      return '<img src="' + esc(img) + '" alt="" class="qs-preview-item__thumb" loading="lazy" />';
    }
    var letter = esc((item && item.name ? String(item.name).charAt(0) : "?").toUpperCase());
    return '<span class="qs-preview-item__ph">' + letter + "</span>";
  }

  function syncSelection() {
    var ids = selectedIds();
    var total = 0;
    ids.forEach(function (id) {
      var item = byId[String(id)];
      if (item) total += parseFloat(item.price) || 0;
    });
    if (countEl) countEl.textContent = String(ids.length);
    if (totalEl) totalEl.textContent = "KES " + fmt(total);
    if (generateBtn) generateBtn.disabled = ids.length === 0;
    if (resultEl) resultEl.classList.add("hidden");
    if (errorEl) {
      errorEl.textContent = "";
      errorEl.classList.add("hidden");
    }
    lastWaText = "";
    syncWaLink();

    if (!previewEl) return;
    if (!ids.length) {
      previewEl.innerHTML = '<li class="qs-preview-empty">Select items from the catalog to preview your quotation.</li>';
      return;
    }
    previewEl.innerHTML = ids
      .map(function (id) {
        var item = byId[String(id)];
        if (!item) return "";
        return (
          '<li class="qs-preview-item">' +
          previewThumb(item) +
          '<span class="qs-preview-item__name">' + esc(item.name || "Item") + "</span>" +
          '<span class="qs-preview-item__price">KES ' + fmt(item.price) + "</span>" +
          "</li>"
        );
      })
      .join("");
  }

  function setCardSelected(el, on) {
    el.classList.toggle("qs-item-card--selected", !!on);
  }

  function toggleCard(card) {
    var cb = card && card.querySelector("[data-qs-checkbox]");
    if (!cb) return;
    cb.checked = !cb.checked;
    setCardSelected(card, cb.checked);
    syncSelection();
  }

  checkboxes.forEach(function (cb) {
    cb.addEventListener("change", function (e) {
      e.stopPropagation();
      var card = cb.closest("[data-qs-item]");
      if (card) setCardSelected(card, cb.checked);
      syncSelection();
    });
  });

  itemEls.forEach(function (card) {
    card.addEventListener("click", function (e) {
      if (e.target.closest("[data-qs-checkbox]")) return;
      toggleCard(card);
    });
    card.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggleCard(card);
      }
    });
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "button");
  });

  if (searchEl) searchEl.addEventListener("input", filterItems);

  if (phoneEl) {
    phoneEl.addEventListener("input", syncWaLink);
    phoneEl.addEventListener("change", syncWaLink);
  }

  if (selectAllBtn) {
    selectAllBtn.addEventListener("click", function () {
      checkboxes.forEach(function (cb) {
        var card = cb.closest("[data-qs-item]");
        if (card && !card.classList.contains("hidden")) {
          cb.checked = true;
          setCardSelected(card, true);
        }
      });
      syncSelection();
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", function () {
      checkboxes.forEach(function (cb) {
        cb.checked = false;
        var card = cb.closest("[data-qs-item]");
        if (card) setCardSelected(card, false);
      });
      syncSelection();
    });
  }

  if (generateBtn) {
    generateBtn.addEventListener("click", function () {
      var ids = selectedIds();
      if (!ids.length || !apiUrl) return;
      var phone = phoneValue();
      if (phone && !isValidWaPhone(phone)) {
        if (errorEl) {
          errorEl.textContent = "Enter a valid WhatsApp number (e.g. 0712345678) or leave it blank.";
          errorEl.classList.remove("hidden");
        }
        if (phoneEl) phoneEl.focus();
        return;
      }
      generateBtn.disabled = true;
      generateBtn.textContent = "Generating…";
      if (errorEl) {
        errorEl.textContent = "";
        errorEl.classList.add("hidden");
      }
      fetch(apiUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ item_ids: ids, customer_phone: phone }),
      })
        .then(function (res) {
          return res.json().then(function (data) {
            return { ok: res.ok, data: data };
          });
        })
        .then(function (out) {
          if (!out.ok || !out.data || !out.data.ok) {
            throw new Error((out.data && out.data.error) || "Could not create share link.");
          }
          if (urlInput) urlInput.value = out.data.url || "";
          lastWaText = out.data.whatsapp_text || "";
          if (openLink) openLink.href = out.data.url || "#";
          if (resultEl) resultEl.classList.remove("hidden");
          if (copiedEl) copiedEl.classList.add("hidden");
          syncWaLink();
        })
        .catch(function (err) {
          if (errorEl) {
            errorEl.textContent = err.message || "Could not create share link.";
            errorEl.classList.remove("hidden");
          }
        })
        .finally(function () {
          generateBtn.disabled = selectedIds().length === 0;
          generateBtn.textContent = "Generate share link";
        });
    });
  }

  if (copyBtn && urlInput) {
    copyBtn.addEventListener("click", function () {
      var url = urlInput.value || "";
      if (!url) return;
      function done() {
        if (copiedEl) copiedEl.classList.remove("hidden");
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(done).catch(function () {
          urlInput.select();
          document.execCommand("copy");
          done();
        });
      } else {
        urlInput.select();
        document.execCommand("copy");
        done();
      }
    });
  }

  syncSelection();
  filterItems();
  syncWaLink();
})();
