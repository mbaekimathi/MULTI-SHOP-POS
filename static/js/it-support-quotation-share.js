(function () {
  var root = document.querySelector("[data-quotation-share-root]");
  if (!root) return;

  var ROW_SELECTED = "bg-brand-500/10";

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
  var sendWaBtn = root.querySelector("[data-qs-send-wa]");
  var resultEl = root.querySelector("[data-qs-share-result]");
  var urlInput = root.querySelector("[data-qs-share-url]");
  var copyBtn = root.querySelector("[data-qs-copy]");
  var copiedEl = root.querySelector("[data-qs-copied]");
  var waHint = root.querySelector("[data-qs-wa-hint]");
  var openLink = root.querySelector("[data-qs-open-link]");
  var errorEl = root.querySelector("[data-qs-error]");
  var selectAllBtn = root.querySelector("[data-qs-select-all]");
  var clearBtn = root.querySelector("[data-qs-clear]");
  var lastWaText = "";
  var lastShareUrl = "";
  var generating = false;

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

  function canSendWhatsApp() {
    return selectedIds().length > 0 && isValidWaPhone(phoneValue()) && !generating;
  }

  function syncShareControls() {
    var ids = selectedIds();
    var phone = phoneValue();
    var validPhone = isValidWaPhone(phone);
    if (sendWaBtn) sendWaBtn.disabled = !canSendWhatsApp();
    if (waHint) {
      if (!ids.length) {
        waHint.textContent = "Select catalog items to include in the quotation.";
      } else if (!phone) {
        waHint.textContent = "Enter the client's WhatsApp number above.";
      } else if (!validPhone) {
        waHint.textContent = "Enter a valid number (e.g. 0712345678).";
      } else {
        waHint.textContent = "Ready — opens WhatsApp with the quotation message and link.";
      }
    }
  }

  function applyShareResult(data) {
    lastWaText = data.whatsapp_text || "";
    lastShareUrl = data.url || "";
    if (urlInput) urlInput.value = lastShareUrl;
    if (openLink) openLink.href = lastShareUrl || "#";
    if (resultEl) resultEl.classList.remove("hidden");
    if (copiedEl) copiedEl.classList.add("hidden");
  }

  function clearShareResult() {
    lastWaText = "";
    lastShareUrl = "";
    if (resultEl) resultEl.classList.add("hidden");
    if (urlInput) urlInput.value = "";
    if (copiedEl) copiedEl.classList.add("hidden");
  }

  function generateShareLink() {
    var ids = selectedIds();
    if (!ids.length || !apiUrl) {
      return Promise.reject(new Error("Select at least one item."));
    }
    var phone = phoneValue();
    if (!isValidWaPhone(phone)) {
      return Promise.reject(new Error("Enter a valid WhatsApp number (e.g. 0712345678)."));
    }
    generating = true;
    syncShareControls();
    if (sendWaBtn) sendWaBtn.textContent = "Preparing quotation…";
    if (errorEl) {
      errorEl.textContent = "";
      errorEl.classList.add("hidden");
    }
    return fetch(apiUrl, {
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
        applyShareResult(out.data);
        return out.data;
      })
      .finally(function () {
        generating = false;
        if (sendWaBtn) {
          sendWaBtn.innerHTML =
            '<svg class="h-5 w-5 shrink-0" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/><path d="M12 0C5.373 0 0 5.373 0 12c0 2.625.846 5.059 2.284 7.034L.789 23.492a.75.75 0 00.918.918l4.458-1.495A11.945 11.945 0 0012 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 21.75a9.714 9.714 0 01-4.915-1.332l-.352-.209-2.642.886.886-2.578-.23-.375A9.736 9.736 0 012.25 12C2.25 6.615 6.615 2.25 12 2.25S21.75 6.615 21.75 12 17.385 21.75 12 21.75z"/></svg> Send quotation via WhatsApp';
        }
        syncShareControls();
      });
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

  function previewItemCell(item) {
    var thumb = "";
    var img = item && item.image_url ? String(item.image_url) : "";
    if (img) {
      thumb =
        '<img src="' +
        esc(img) +
        '" alt="" class="h-6 w-6 shrink-0 rounded object-cover border border-[rgb(var(--rc-border))]" loading="lazy" />';
    } else {
      var letter = esc((item && item.name ? String(item.name).charAt(0) : "?").toUpperCase());
      thumb =
        '<span class="flex h-6 w-6 shrink-0 items-center justify-center rounded border border-[rgb(var(--rc-border))] bg-[rgb(var(--rc-surface-2))] text-[10px] font-bold text-[rgb(var(--rc-muted))]">' +
        letter +
        "</span>";
    }
    return (
      '<div class="flex min-w-0 items-center gap-2">' +
      thumb +
      '<span class="truncate font-medium text-[rgb(var(--rc-page-fg))]">' +
      esc(item.name || "Item") +
      "</span></div>"
    );
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
    clearShareResult();
    if (errorEl) {
      errorEl.textContent = "";
      errorEl.classList.add("hidden");
    }
    syncShareControls();

    if (!previewEl) return;
    if (!ids.length) {
      previewEl.innerHTML =
        '<tr><td colspan="2" class="px-3 py-4 text-center text-xs text-[rgb(var(--rc-muted))]">Select items from the catalog.</td></tr>';
      return;
    }
    previewEl.innerHTML = ids
      .map(function (id) {
        var item = byId[String(id)];
        if (!item) return "";
        return (
          "<tr>" +
          '<td class="px-3 py-2">' +
          previewItemCell(item) +
          "</td>" +
          '<td class="whitespace-nowrap px-3 py-2 text-right font-semibold tabular-nums text-[rgb(var(--rc-muted))]">KES ' +
          fmt(item.price) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function setRowSelected(el, on) {
    el.classList.toggle(ROW_SELECTED, !!on);
  }

  function toggleRow(row) {
    var cb = row && row.querySelector("[data-qs-checkbox]");
    if (!cb) return;
    cb.checked = !cb.checked;
    setRowSelected(row, cb.checked);
    syncSelection();
  }

  checkboxes.forEach(function (cb) {
    cb.addEventListener("click", function (e) {
      e.stopPropagation();
    });
    cb.addEventListener("change", function (e) {
      e.stopPropagation();
      var row = cb.closest("[data-qs-item]");
      if (row) setRowSelected(row, cb.checked);
      syncSelection();
    });
  });

  itemEls.forEach(function (row) {
    row.addEventListener("click", function (e) {
      if (e.target.closest("[data-qs-checkbox]")) return;
      toggleRow(row);
    });
    row.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggleRow(row);
      }
    });
  });

  if (searchEl) searchEl.addEventListener("input", filterItems);

  if (phoneEl) {
    phoneEl.addEventListener("input", syncShareControls);
    phoneEl.addEventListener("change", syncShareControls);
  }

  if (selectAllBtn) {
    selectAllBtn.addEventListener("click", function () {
      checkboxes.forEach(function (cb) {
        var row = cb.closest("[data-qs-item]");
        if (row && !row.classList.contains("hidden")) {
          cb.checked = true;
          setRowSelected(row, true);
        }
      });
      syncSelection();
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", function () {
      checkboxes.forEach(function (cb) {
        cb.checked = false;
        var row = cb.closest("[data-qs-item]");
        if (row) setRowSelected(row, false);
      });
      syncSelection();
    });
  }

  if (sendWaBtn) {
    sendWaBtn.addEventListener("click", function () {
      if (!canSendWhatsApp()) return;
      generateShareLink()
        .then(function (data) {
          var waUrl = buildWaUrl(phoneValue(), data.whatsapp_text || lastWaText);
          window.open(waUrl, "_blank", "noopener,noreferrer");
        })
        .catch(function (err) {
          if (errorEl) {
            errorEl.textContent = err.message || "Could not send quotation.";
            errorEl.classList.remove("hidden");
          }
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
})();
