(function () {
  var root = document.getElementById("wsf-root");
  if (!root) return;

  var prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (!prefersReducedMotion) {
    root.classList.add("wsf-motion-on");
  }

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

  /* Dark / light theme */
  (function initStorefrontTheme() {
    var storageKey = "marketing-theme";
    var docEl = document.documentElement;
    var isPreview = root.getAttribute("data-wsf-design-preview") === "1";

    function currentTheme() {
      if (isPreview) {
        return root.classList.contains("wsf-theme-dark") ? "dark" : "light";
      }
      return docEl.getAttribute("data-marketing-theme") === "dark" ? "dark" : "light";
    }

    function applyTheme(theme, persist) {
      var dark = theme === "dark";
      if (isPreview) {
        root.classList.toggle("wsf-theme-dark", dark);
        root.classList.toggle("wsf-theme-light", !dark);
        root.setAttribute("data-wsf-theme", theme);
      } else {
        docEl.setAttribute("data-marketing-theme", theme);
        docEl.style.colorScheme = dark ? "dark" : "light";
      }
      root.querySelectorAll("[data-wsf-theme-toggle]").forEach(function (btn) {
        btn.setAttribute("aria-pressed", dark ? "true" : "false");
        btn.setAttribute("aria-label", dark ? "Switch to light mode" : "Switch to dark mode");
      });
      if (persist === false) return;
      try {
        localStorage.setItem(storageKey, theme);
        var cfg = docEl.getAttribute("data-marketing-theme-config") || "";
        if (cfg) localStorage.setItem("marketing-theme-config", cfg);
      } catch (e) {}
    }

    if (isPreview) {
      var stored = null;
      try {
        stored = localStorage.getItem(storageKey);
      } catch (e) {}
      if (stored === "dark" || stored === "light") {
        applyTheme(stored);
      }
    } else {
      root.querySelectorAll("[data-wsf-theme-toggle]").forEach(function (btn) {
        btn.setAttribute("aria-pressed", currentTheme() === "dark" ? "true" : "false");
      });
    }

    root.querySelectorAll("[data-wsf-theme-toggle]").forEach(function (btn) {
      if (btn.getAttribute("data-wsf-theme-bound") === "1") return;
      btn.setAttribute("data-wsf-theme-bound", "1");
      btn.addEventListener("click", function () {
        applyTheme(currentTheme() === "dark" ? "light" : "dark");
      });
    });

    if (!isPreview) {
      var themeDefault = docEl.getAttribute("data-marketing-theme-default") || "system";
      if (themeDefault === "system") {
        try {
          var cfg = docEl.getAttribute("data-marketing-theme-config") || "";
          var mq = window.matchMedia("(prefers-color-scheme: dark)");
          mq.addEventListener("change", function () {
            var storedCfg = null;
            var stored = null;
            try {
              storedCfg = localStorage.getItem("marketing-theme-config");
              stored = localStorage.getItem(storageKey);
            } catch (e) {}
            if (storedCfg && cfg && storedCfg === cfg && (stored === "dark" || stored === "light")) return;
            applyTheme(mq.matches ? "dark" : "light", false);
          });
        } catch (e) {}
      }
    }
  })();

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
    root.querySelectorAll(".wsf-product-card, .wsf-reveal").forEach(function (el) {
      io.observe(el);
    });
    root.querySelectorAll(".wsf-reveal").forEach(function (el) {
      if (el.classList.contains("wsf-hero-static")) {
        el.classList.add("is-visible");
      }
    });
  } else {
    root.querySelectorAll(".wsf-product-card, .wsf-reveal").forEach(function (el) {
      el.classList.add("is-visible");
    });
  }

  requestAnimationFrame(function () {
    root.classList.add("wsf-is-ready");
  });

  root.querySelectorAll(".wsf-stat-pill__num[data-wsf-count]").forEach(function (el) {
    var target = parseInt(el.getAttribute("data-wsf-count") || "0", 10);
    if (!target || target < 2) return;
    var start = 0;
    var duration = 900;
    var t0 = null;
    function step(ts) {
      if (!t0) t0 = ts;
      var p = Math.min((ts - t0) / duration, 1);
      var eased = 1 - Math.pow(1 - p, 3);
      el.textContent = String(Math.round(start + (target - start) * eased));
      if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  });

  var parallax = root.querySelector("[data-wsf-hero-parallax]");
  if (parallax && window.matchMedia("(min-width: 1024px) and (prefers-reduced-motion: no-preference)").matches) {
    root.addEventListener("mousemove", function (e) {
      var rect = parallax.getBoundingClientRect();
      var cx = rect.left + rect.width / 2;
      var cy = rect.top + rect.height / 2;
      var dx = (e.clientX - cx) / rect.width;
      var dy = (e.clientY - cy) / rect.height;
      parallax.style.setProperty("--wsf-px", dx.toFixed(3));
      parallax.style.setProperty("--wsf-py", dy.toFixed(3));
    });
  }

  root.querySelectorAll('a[href^="#"]').forEach(function (link) {
    if (link.hasAttribute("data-wsf-cat-nav")) return;
    link.addEventListener("click", function (e) {
      var id = link.getAttribute("href");
      if (!isInPageNavHref(id)) return;
      var target = document.querySelector(id);
      if (!target) {
        var catalogPath = root.getAttribute("data-wsf-catalog-url") || "";
        if (catalogPath && id.indexOf("wsf-product-") >= 0) {
          window.location.href = catalogPath + id;
        }
        return;
      }
      e.preventDefault();
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  function updateRowCounts() {
    root.querySelectorAll(".wsf-cat-row").forEach(function (row) {
      var countEl = row.querySelector(".wsf-cat-row__count");
      if (!countEl) return;
      var n = row.querySelectorAll(".wsf-product-card:not(.is-hidden-filter)").length;
      countEl.textContent = n ? String(n) : "";
    });
  }

  function findCategoryRow(catUpper) {
    if (!catUpper || catUpper === "ALL") return null;
    var row = root.querySelector('[data-wsf-category-row="' + catUpper + '"]');
    if (row) return row;
    var match = null;
    root.querySelectorAll("[data-wsf-cat-nav]").forEach(function (link) {
      if (match) return;
      var key = (link.getAttribute("data-wsf-cat-nav") || "").toUpperCase();
      if (key !== catUpper) return;
      var href = link.getAttribute("href") || "";
      if (href.charAt(0) === "#" && href.length > 1) {
        match = document.querySelector(href);
      }
    });
    return match;
  }

  function scrollToCategory(catUpper) {
    var target = findCategoryRow(catUpper);
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    var products = document.getElementById("wsf-products");
    if (products) products.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function setActiveCategoryNav(catUpper) {
    var normalized = (catUpper || "ALL").toUpperCase();
    root.querySelectorAll("[data-wsf-cat-nav]").forEach(function (link) {
      var key = (link.getAttribute("data-wsf-cat-nav") || "ALL").toUpperCase();
      link.classList.toggle("is-active", key === normalized);
    });
  }

  function applyFilters() {
    var searchInput = root.querySelector('.wsf-search input[type="search"]');
    var q = searchInput ? (searchInput.value || "").trim().toLowerCase() : "";
    var visibleTotal = 0;
    var grid = root.querySelector(".wsf-product-grid");

    if (grid) {
      grid.querySelectorAll(".wsf-product-card").forEach(function (card) {
        var textOk = !q || (card.textContent || "").toLowerCase().indexOf(q) >= 0;
        card.classList.toggle("is-hidden-filter", !textOk);
        if (textOk) visibleTotal += 1;
      });
      var countEl = document.getElementById("wsf-visible-count");
      if (countEl) countEl.textContent = String(visibleTotal);
      return;
    }

    root.querySelectorAll(".wsf-cat-row").forEach(function (row) {
      var rowVisible = false;

      row.querySelectorAll(".wsf-product-card").forEach(function (card) {
        var textOk = !q || (card.textContent || "").toLowerCase().indexOf(q) >= 0;
        card.classList.toggle("is-hidden-filter", !textOk);
        if (textOk) {
          rowVisible = true;
          visibleTotal += 1;
        }
      });

      row.classList.toggle("is-hidden-filter", !rowVisible && !!q);
    });

    var countEl = document.getElementById("wsf-visible-count");
    if (countEl) countEl.textContent = String(visibleTotal);
    updateRowCounts();
  }

  function setCategoryInUrl(cat, label) {
    if (!window.history || !window.history.replaceState) return;
    try {
      var url = new URL(window.location.href);
      if (!cat || cat === "all") {
        url.searchParams.delete("cat");
      } else if (label) {
        url.searchParams.set("cat", decodeURIComponent(label));
      } else {
        url.searchParams.set("cat", cat);
      }
      window.history.replaceState(null, "", url.toString());
    } catch (e) {}
  }

  function applyCategoryFromUrl() {
    var params = new URLSearchParams(window.location.search);
    var raw = params.get("cat");
    if (!raw) return;
    var catUpper = raw.trim().toUpperCase();
    setActiveCategoryNav(catUpper);
    window.requestAnimationFrame(function () {
      scrollToCategory(catUpper);
    });
  }

  function isInPageNavHref(href) {
    return !!href && href.charAt(0) === "#" && href.length > 1;
  }

  function bindCategoryNav(link) {
    link.addEventListener("click", function (e) {
      var href = link.getAttribute("href") || "";
      if (!isInPageNavHref(href)) return;
      e.preventDefault();
      var cat = (link.getAttribute("data-wsf-cat-nav") || "all").toUpperCase();
      setCategoryInUrl(cat === "ALL" ? "all" : cat, link.getAttribute("data-wsf-cat-label") || "");
      setActiveCategoryNav(cat);
      scrollToCategory(cat);
    });
  }

  root.querySelectorAll("[data-wsf-cat-nav]").forEach(bindCategoryNav);

  var searchInput = root.querySelector('.wsf-search input[type="search"]');
  if (searchInput) {
    searchInput.addEventListener("input", applyFilters);
  }

  function syncFab(count) {
    if (!fab) return;
    root.querySelectorAll("[data-wsf-cart-count]").forEach(function (fabCount) {
      fabCount.textContent = String(count);
      fabCount.classList.toggle("hidden", count <= 0);
    });
  }

  window.wsfSyncFab = syncFab;

  document.addEventListener("wsf-cart-updated", function (e) {
    var count = (e.detail && e.detail.count) || 0;
    syncFab(count);
  });

  syncFab(0);

  applyCategoryFromUrl();
  applyFilters();
})();
