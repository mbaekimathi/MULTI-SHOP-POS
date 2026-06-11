(function () {
  var root = document.getElementById("wsf-root");
  if (!root) return;

  var header = root.querySelector(".wsf-header");
  var toast = document.getElementById("wsf-toast");
  var toastTimer = null;
  var fab = document.getElementById("wsf-fab-cart");

  function showToast(message) {
    if (!toast) return;
    toast.textContent = message;
    toast.classList.add("is-show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () {
      toast.classList.remove("is-show");
    }, 2600);
  }

  window.wsfShowToast = showToast;

  if (header) {
    var onScroll = function () {
      header.classList.toggle("is-scrolled", window.scrollY > 8);
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  if ("IntersectionObserver" in window) {
    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            io.unobserve(entry.target);
          }
        });
      },
      { root: null, rootMargin: "0px 0px -4% 0px", threshold: 0.06 }
    );
    root.querySelectorAll(".wsf-product-card").forEach(function (card) {
      io.observe(card);
    });
  } else {
    root.querySelectorAll(".wsf-product-card").forEach(function (card) {
      card.classList.add("is-visible");
    });
  }

  function updateRowCounts() {
    root.querySelectorAll(".wsf-cat-row").forEach(function (row) {
      var countEl = row.querySelector(".wsf-cat-row__count");
      if (!countEl) return;
      var n = row.querySelectorAll(".wsf-product-card:not(.is-hidden-filter)").length;
      countEl.textContent = n ? n + " item" + (n === 1 ? "" : "s") : "";
    });
  }

  function applyFilters() {
    var activeCat =
      (root.querySelector(".wsf-cat-pill.is-active") &&
        root.querySelector(".wsf-cat-pill.is-active").getAttribute("data-wsf-cat")) ||
      "all";
    var searchInput = root.querySelector('.wsf-search input[type="search"]');
    var q = searchInput ? (searchInput.value || "").trim().toLowerCase() : "";
    var visibleTotal = 0;

    root.querySelectorAll(".wsf-cat-row").forEach(function (row) {
      var rowCat = row.getAttribute("data-wsf-category-row") || "";
      var rowCatOk = activeCat === "all" || rowCat === activeCat;
      var rowVisible = false;

      row.querySelectorAll(".wsf-product-card").forEach(function (card) {
        var textOk = !q || (card.textContent || "").toLowerCase().indexOf(q) >= 0;
        var show = rowCatOk && textOk;
        card.classList.toggle("is-hidden-filter", !show);
        if (show) {
          rowVisible = true;
          visibleTotal += 1;
        }
      });

      row.classList.toggle("is-hidden-filter", !rowVisible);
    });

    var countEl = document.getElementById("wsf-visible-count");
    if (countEl) countEl.textContent = String(visibleTotal);
    updateRowCounts();
  }

  root.querySelectorAll(".wsf-cat-pill").forEach(function (btn) {
    btn.addEventListener("click", function () {
      root.querySelectorAll(".wsf-cat-pill").forEach(function (b) {
        b.classList.toggle("is-active", b === btn);
      });
      applyFilters();
      var cat = btn.getAttribute("data-wsf-cat");
      if (cat && cat !== "all") {
        var row = root.querySelector('.wsf-cat-row[data-wsf-category-row="' + cat + '"]');
        if (row && !row.classList.contains("is-hidden-filter")) {
          row.scrollIntoView({ behavior: "smooth", block: "nearest" });
        }
      }
    });
  });

  var searchInput = root.querySelector('.wsf-search input[type="search"]');
  if (searchInput) {
    searchInput.addEventListener("input", applyFilters);
  }

  function syncFab(count) {
    if (!fab) return;
    fab.classList.toggle("is-hidden", count <= 0);
    var fabCount = fab.querySelector("[data-wsf-fab-count]");
    if (fabCount) fabCount.textContent = String(count);
  }

  window.wsfSyncFab = syncFab;

  document.addEventListener("wsf-cart-updated", function (e) {
    var count = (e.detail && e.detail.count) || 0;
    syncFab(count);
  });

  applyFilters();
})();
