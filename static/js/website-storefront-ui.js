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
      { root: null, rootMargin: "0px 0px -6% 0px", threshold: 0.08 }
    );
    root.querySelectorAll(".wsf-product-card").forEach(function (card) {
      io.observe(card);
    });
  } else {
    root.querySelectorAll(".wsf-product-card").forEach(function (card) {
      card.classList.add("is-visible");
    });
  }

  function applyFilters() {
    var activeCat =
      (root.querySelector(".wsf-cat-pill.is-active") &&
        root.querySelector(".wsf-cat-pill.is-active").getAttribute("data-wsf-cat")) ||
      "all";
    var searchInput = root.querySelector('.wsf-search input[type="search"]');
    var q = searchInput ? (searchInput.value || "").trim().toLowerCase() : "";

    root.querySelectorAll("#wsf-product-grid .wsf-product-card").forEach(function (card) {
      var cat = card.getAttribute("data-wsf-product-cat") || "";
      var catOk = activeCat === "all" || cat === activeCat;
      var textOk = !q || (card.textContent || "").toLowerCase().indexOf(q) >= 0;
      card.classList.toggle("is-hidden-filter", !(catOk && textOk));
    });

    var visible = root.querySelectorAll("#wsf-product-grid .wsf-product-card:not(.is-hidden-filter)").length;
    var countEl = document.getElementById("wsf-visible-count");
    if (countEl) countEl.textContent = String(visible);
  }

  root.querySelectorAll(".wsf-cat-pill").forEach(function (btn) {
    btn.addEventListener("click", function () {
      root.querySelectorAll(".wsf-cat-pill").forEach(function (b) {
        b.classList.toggle("is-active", b === btn);
      });
      applyFilters();
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
})();
