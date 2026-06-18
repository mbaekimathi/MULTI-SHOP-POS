(function () {
  var root = document.getElementById("website-designs-page");
  if (!root) return;

  var bootEl = document.getElementById("wsd-editor-boot");
  var boot = {};
  if (bootEl) {
    try {
      boot = JSON.parse(bootEl.textContent || "{}");
    } catch (e) {
      boot = {};
    }
  }

  var catalog = boot.catalog || [];
  var catalogById = {};
  catalog.forEach(function (p) {
    catalogById[String(p.id)] = p;
  });

  var selectedList = document.getElementById("wsd-selected-list");
  var catalogList = document.getElementById("wsd-catalog-list");
  var hiddenInput = document.getElementById("wsd-featured-ids");
  var searchInput = document.getElementById("wsd-catalog-search");
  var countEl = document.getElementById("wsd-selected-count");
  var modeEl = document.getElementById("wsd-mode-label");
  var form = document.getElementById("wsd-form");
  var maxItems = boot.maxFeatured || 6;

  function fmtMoney(n) {
    return "KES " + Math.round(Number(n) || 0).toLocaleString("en-KE");
  }

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  function selectedIds() {
    if (!selectedList) return [];
    return Array.prototype.map
      .call(selectedList.querySelectorAll(".wsd-row--selected[data-item-id]"), function (el) {
        return parseInt(el.getAttribute("data-item-id"), 10);
      })
      .filter(function (id) {
        return id > 0;
      });
  }

  function clearEmptyPlaceholder() {
    if (!selectedList) return;
    selectedList.querySelectorAll(".wsd-empty").forEach(function (el) {
      el.remove();
    });
  }

  function syncHidden() {
    var ids = selectedIds();
    if (hiddenInput) hiddenInput.value = JSON.stringify(ids);
    if (countEl) countEl.textContent = String(ids.length);
    if (modeEl) {
      modeEl.textContent = ids.length ? "Custom selection" : "Automatic (best-sellers)";
      modeEl.classList.toggle("is-custom", ids.length > 0);
    }
    refreshCatalogVisibility();
  }

  function refreshCatalogVisibility() {
    if (!catalogList) return;
    var set = {};
    selectedIds().forEach(function (id) {
      set[String(id)] = true;
    });
    catalogList.querySelectorAll("[data-catalog-id]").forEach(function (row) {
      var id = row.getAttribute("data-catalog-id");
      row.classList.toggle("is-added", !!set[id]);
    });
  }

  function selectedRowHtml(p) {
    var img = p.image_url
      ? '<img src="' + esc(p.image_url) + '" alt="" class="wsd-row__img" />'
      : '<span class="wsd-row__img wsd-row__img--ph">' + esc((p.name || "?").charAt(0)) + "</span>";
    return (
      '<li class="wsd-row wsd-row--selected" draggable="true" data-item-id="' +
      p.id +
      '">' +
      '<span class="wsd-row__drag" aria-hidden="true">⋮⋮</span>' +
      img +
      '<div class="wsd-row__body">' +
      '<p class="wsd-row__name">' +
      esc(p.name) +
      "</p>" +
      '<p class="wsd-row__meta">' +
      esc(p.category) +
      " · " +
      fmtMoney(p.price) +
      "</p>" +
      "</div>" +
      '<div class="wsd-row__actions">' +
      '<button type="button" class="wsd-row__btn" data-move-up aria-label="Move up">↑</button>' +
      '<button type="button" class="wsd-row__btn" data-move-down aria-label="Move down">↓</button>' +
      '<button type="button" class="wsd-row__btn wsd-row__btn--remove" data-remove aria-label="Remove">×</button>' +
      "</div>" +
      "</li>"
    );
  }

  function catalogRowHtml(p) {
    var img = p.image_url
      ? '<img src="' + esc(p.image_url) + '" alt="" class="wsd-row__img" />'
      : '<span class="wsd-row__img wsd-row__img--ph">' + esc((p.name || "?").charAt(0)) + "</span>";
    return (
      '<li class="wsd-row wsd-row--catalog" data-catalog-id="' +
      p.id +
      '" data-search="' +
      esc(((p.name || "") + " " + (p.category || "")).toLowerCase()) +
      '">' +
      img +
      '<div class="wsd-row__body">' +
      '<p class="wsd-row__name">' +
      esc(p.name) +
      "</p>" +
      '<p class="wsd-row__meta">' +
      esc(p.category) +
      " · " +
      fmtMoney(p.price) +
      "</p>" +
      "</div>" +
      '<button type="button" class="wsd-row__add btn-rc btn-rc-primary px-2.5 py-1.5 text-[10px] font-bold" data-add-id="' +
      p.id +
      '">Add</button>' +
      "</li>"
    );
  }

  function renderCatalog(filter) {
    if (!catalogList) return;
    var q = (filter || "").trim().toLowerCase();
    catalogList.innerHTML = catalog
      .filter(function (p) {
        if (!q) return true;
        var hay = ((p.name || "") + " " + (p.category || "")).toLowerCase();
        return hay.indexOf(q) >= 0;
      })
      .map(catalogRowHtml)
      .join("");
    refreshCatalogVisibility();
  }

  function addProduct(id) {
    var sid = String(id);
    var p = catalogById[sid];
    if (!p || !selectedList) return;
    if (selectedIds().length >= maxItems) {
      window.alert("Maximum " + maxItems + " featured products on the homepage.");
      return;
    }
    if (selectedIds().indexOf(parseInt(sid, 10)) >= 0) return;
    clearEmptyPlaceholder();
    selectedList.insertAdjacentHTML("beforeend", selectedRowHtml(p));
    syncHidden();
  }

  function removeProduct(id) {
    if (!selectedList) return;
    var row = selectedList.querySelector('[data-item-id="' + id + '"]');
    if (row) row.remove();
    syncHidden();
  }

  function moveProduct(id, dir) {
    if (!selectedList) return;
    var row = selectedList.querySelector('[data-item-id="' + id + '"]');
    if (!row) return;
    if (dir < 0 && row.previousElementSibling) {
      selectedList.insertBefore(row, row.previousElementSibling);
    } else if (dir > 0 && row.nextElementSibling) {
      selectedList.insertBefore(row.nextElementSibling, row);
    }
    syncHidden();
  }

  var dragEl = null;

  if (selectedList) {
    selectedList.addEventListener("dragstart", function (e) {
      var row = e.target.closest(".wsd-row--selected");
      if (!row) return;
      dragEl = row;
      row.classList.add("is-dragging");
      e.dataTransfer.effectAllowed = "move";
    });
    selectedList.addEventListener("dragend", function () {
      if (dragEl) dragEl.classList.remove("is-dragging");
      dragEl = null;
      syncHidden();
    });
    selectedList.addEventListener("dragover", function (e) {
      e.preventDefault();
      var row = e.target.closest(".wsd-row--selected");
      if (!row || row === dragEl) return;
      var rect = row.getBoundingClientRect();
      var after = e.clientY > rect.top + rect.height / 2;
      if (after) row.after(dragEl);
      else row.before(dragEl);
    });
    selectedList.addEventListener("click", function (e) {
      var rem = e.target.closest("[data-remove]");
      if (rem) {
        var li = rem.closest("[data-item-id]");
        if (li) removeProduct(li.getAttribute("data-item-id"));
        return;
      }
      var up = e.target.closest("[data-move-up]");
      if (up) {
        var liU = up.closest("[data-item-id]");
        if (liU) moveProduct(liU.getAttribute("data-item-id"), -1);
        return;
      }
      var down = e.target.closest("[data-move-down]");
      if (down) {
        var liD = down.closest("[data-item-id]");
        if (liD) moveProduct(liD.getAttribute("data-item-id"), 1);
      }
    });
  }

  if (catalogList) {
    catalogList.addEventListener("click", function (e) {
      var addBtn = e.target.closest("[data-add-id]");
      if (addBtn) addProduct(addBtn.getAttribute("data-add-id"));
    });
  }

  if (searchInput) {
    searchInput.addEventListener("input", function () {
      renderCatalog(searchInput.value);
    });
  }

  if (form) {
    form.addEventListener("submit", function () {
      syncHidden();
    });
  }

  renderCatalog("");
  syncHidden();

  var previewFrame = document.getElementById("wsd-preview-frame");
  var previewPlaceholder = document.getElementById("wsd-preview-placeholder");
  var previewHideBtn = document.getElementById("wsd-preview-hide-btn");

  function showPreview() {
    if (!previewFrame || !previewPlaceholder) return;
    if (!previewFrame.getAttribute("data-loaded")) {
      previewFrame.src = previewFrame.getAttribute("data-preview-src") || "/site";
      previewFrame.setAttribute("data-loaded", "1");
    }
    previewPlaceholder.hidden = true;
    previewFrame.hidden = false;
    if (previewHideBtn) previewHideBtn.hidden = false;
    previewFrame.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  function hidePreview() {
    if (!previewFrame || !previewPlaceholder) return;
    previewFrame.hidden = true;
    previewPlaceholder.hidden = false;
    if (previewHideBtn) previewHideBtn.hidden = true;
  }

  root.querySelectorAll(".wsd-preview-trigger, #wsd-preview-btn").forEach(function (btn) {
    btn.addEventListener("click", showPreview);
  });
  if (previewHideBtn) {
    previewHideBtn.addEventListener("click", hidePreview);
  }
})();
