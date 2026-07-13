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
      applyTheme("dark", false);
    }

    root.querySelectorAll("[data-wsf-theme-toggle]").forEach(function (btn) {
      if (btn.getAttribute("data-wsf-theme-bound") === "1") return;
      btn.setAttribute("data-wsf-theme-bound", "1");
      btn.addEventListener("click", function () {
        if (isPreview) {
          applyTheme(currentTheme() === "dark" ? "light" : "dark");
        }
      });
    });
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

  function markCardImageLoaded(img) {
    var media = img.closest(".wsf-card__media") || img.closest(".wsf-home-hero__tile");
    if (media) media.classList.add("is-img-loaded");
  }

  root.querySelectorAll(".wsf-card__img, .wsf-home-hero__tile-media img").forEach(function (img) {
    if (img.complete && img.naturalWidth > 0) {
      markCardImageLoaded(img);
    } else {
      img.addEventListener("load", function () {
        markCardImageLoaded(img);
      });
      img.addEventListener("error", function () {
        markCardImageLoaded(img);
      });
    }
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

  var parallaxPanels = root.querySelectorAll("[data-wsf-hero-parallax]");
  if (parallaxPanels.length && window.matchMedia("(min-width: 900px) and (prefers-reduced-motion: no-preference)").matches) {
    var parallaxHero = parallaxPanels[0].closest(".wsf-hero--slideshow") || root;
    root.addEventListener("mousemove", function (e) {
      var rect = parallaxHero.getBoundingClientRect();
      var cx = rect.left + rect.width / 2;
      var cy = rect.top + rect.height / 2;
      var dx = (e.clientX - cx) / rect.width;
      var dy = (e.clientY - cy) / rect.height;
      parallaxPanels.forEach(function (panel) {
        var i = parseInt(panel.style.getPropertyValue("--wsf-float-i") || "0", 10);
        var depth = 6 + i * 4;
        panel.style.setProperty("--wsf-px", dx.toFixed(3));
        panel.style.setProperty("--wsf-py", dy.toFixed(3));
        panel.style.setProperty("--wsf-float-depth", String(depth));
      });
    });
  }

  var orbitRing = root.querySelector("[data-wsf-orbit-ring]");
  var orbitArena = root.querySelector(".wsf-hero-orbit__arena");
  if (orbitRing && orbitArena && window.matchMedia("(prefers-reduced-motion: no-preference)").matches) {
    orbitArena.addEventListener("mouseenter", function () {
      orbitRing.style.animationPlayState = "paused";
    });
    orbitArena.addEventListener("mouseleave", function () {
      orbitRing.style.animationPlayState = "running";
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
    var catRows = root.querySelectorAll(".wsf-cat-row");

    if (catRows.length) {
      catRows.forEach(function (row) {
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
      return;
    }

    var grid = root.querySelector(".wsf-product-grid");
    if (grid) {
      grid.querySelectorAll(".wsf-product-card").forEach(function (card) {
        var textOk = !q || (card.textContent || "").toLowerCase().indexOf(q) >= 0;
        card.classList.toggle("is-hidden-filter", !textOk);
        if (textOk) visibleTotal += 1;
      });
      var countEl = document.getElementById("wsf-visible-count");
      if (countEl) countEl.textContent = String(visibleTotal);
    }
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
    if (!raw) {
      setActiveCategoryNav("ALL");
      return;
    }
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

  root.querySelectorAll('.wsf-search input[type="search"]').forEach(function (searchInput) {
    searchInput.addEventListener("input", function () {
      var val = searchInput.value;
      root.querySelectorAll('.wsf-search input[type="search"]').forEach(function (other) {
        if (other !== searchInput) other.value = val;
      });
      applyFilters();
    });
  });

  function syncFab(count) {
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

  (function initPageheadSlideshow() {
    var slideshow = root.querySelector("[data-wsf-pagehead-slideshow]");
    if (!slideshow) return;
    var slides = slideshow.querySelectorAll(".wsf-pagehead__slide");
    if (!slides.length) return;
    var dots = root.querySelectorAll("[data-wsf-slide-dot]");
    var caption = root.querySelector("[data-wsf-slide-caption]");
    var idx = 0;
    var timer = null;
    var delay = 5500;

    function labelFor(i) {
      var slide = slides[i];
      return slide ? slide.getAttribute("data-wsf-slide-label") || "" : "";
    }

    function setCaption(i) {
      if (caption) caption.textContent = labelFor(i);
    }

    function go(n) {
      if (slides.length <= 1) {
        setCaption(0);
        return;
      }
      slides[idx].classList.remove("is-active");
      if (dots[idx]) {
        dots[idx].classList.remove("is-active");
        dots[idx].setAttribute("aria-selected", "false");
      }
      idx = ((n % slides.length) + slides.length) % slides.length;
      slides[idx].classList.add("is-active");
      if (dots[idx]) {
        dots[idx].classList.add("is-active");
        dots[idx].setAttribute("aria-selected", "true");
      }
      setCaption(idx);
    }

    function schedule() {
      if (timer) clearInterval(timer);
      if (slides.length <= 1) return;
      timer = setInterval(function () {
        go(idx + 1);
      }, delay);
    }

    dots.forEach(function (dot) {
      dot.addEventListener("click", function () {
        var n = parseInt(dot.getAttribute("data-wsf-slide-dot") || "0", 10);
        go(n);
        schedule();
      });
    });

    setCaption(0);
    if (dots[0]) dots[0].setAttribute("aria-selected", "true");
    schedule();
  })();

  (function initContactDrawer() {
    var contactDrawer = document.getElementById("wsf-contact-drawer");
    var contactBackdrop = document.getElementById("wsf-contact-backdrop");
    if (!contactDrawer) return;

    function closeCartDrawer() {
      var cartDrawer = document.getElementById("wsf-cart-drawer");
      var cartBackdrop = document.getElementById("wsf-cart-backdrop");
      if (cartDrawer) cartDrawer.classList.remove("is-open");
      if (cartBackdrop) cartBackdrop.classList.remove("is-open");
      document.body.classList.remove("wsf-cart-open");
    }

    function closeContactDrawer() {
      contactDrawer.classList.remove("is-open");
      if (contactBackdrop) contactBackdrop.classList.remove("is-open");
      document.body.classList.remove("wsf-contact-open");
      contactDrawer.setAttribute("aria-hidden", "true");
      if (contactBackdrop) contactBackdrop.setAttribute("aria-hidden", "true");
    }

    function openContactDrawer() {
      closeCartDrawer();
      contactDrawer.classList.add("is-open");
      if (contactBackdrop) contactBackdrop.classList.add("is-open");
      document.body.classList.add("wsf-contact-open");
      contactDrawer.setAttribute("aria-hidden", "false");
      if (contactBackdrop) contactBackdrop.setAttribute("aria-hidden", "false");
    }

    window.wsfCloseContactDrawer = closeContactDrawer;
    window.wsfOpenContactDrawer = openContactDrawer;

    root.addEventListener("click", function (e) {
      if (e.target.closest("[data-wsf-open-contact]")) {
        e.preventDefault();
        openContactDrawer();
        return;
      }
      if (e.target.closest("[data-wsf-close-contact]")) {
        e.preventDefault();
        closeContactDrawer();
      }
    });

    if (contactBackdrop) {
      contactBackdrop.addEventListener("click", closeContactDrawer);
    }

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && contactDrawer.classList.contains("is-open")) {
        closeContactDrawer();
      }
    });
  })();

  /* Mobile navigation drawer */
  (function initMobileMenu() {
    var menuDrawer = document.getElementById("wsf-menu-drawer");
    var menuBackdrop = document.getElementById("wsf-menu-backdrop");
    var menuBtn = root.querySelector("[data-wsf-open-menu]");
    if (!menuDrawer || !menuBackdrop) return;

    function closeMenu() {
      menuDrawer.classList.remove("is-open");
      menuBackdrop.classList.remove("is-open");
      document.body.classList.remove("wsf-menu-open");
      menuDrawer.setAttribute("aria-hidden", "true");
      menuBackdrop.setAttribute("aria-hidden", "true");
      if (menuBtn) menuBtn.setAttribute("aria-expanded", "false");
    }

    function openMenu() {
      var cartDrawer = document.getElementById("wsf-cart-drawer");
      var cartBackdrop = document.getElementById("wsf-cart-backdrop");
      if (cartDrawer) cartDrawer.classList.remove("is-open");
      if (cartBackdrop) cartBackdrop.classList.remove("is-open");
      document.body.classList.remove("wsf-cart-open");
      if (window.wsfCloseContactDrawer) window.wsfCloseContactDrawer();
      menuDrawer.classList.add("is-open");
      menuBackdrop.classList.add("is-open");
      document.body.classList.add("wsf-menu-open");
      menuDrawer.setAttribute("aria-hidden", "false");
      menuBackdrop.setAttribute("aria-hidden", "false");
      if (menuBtn) menuBtn.setAttribute("aria-expanded", "true");
    }

    root.addEventListener("click", function (e) {
      if (e.target.closest("[data-wsf-open-menu]")) {
        e.preventDefault();
        if (menuDrawer.classList.contains("is-open")) closeMenu();
        else openMenu();
        return;
      }
      if (e.target.closest("[data-wsf-close-menu]")) {
        e.preventDefault();
        closeMenu();
        return;
      }
      if (e.target.closest("[data-wsf-close-menu-on-click]")) {
        closeMenu();
      }
    });

    menuBackdrop.addEventListener("click", closeMenu);

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && menuDrawer.classList.contains("is-open")) {
        closeMenu();
      }
    });
  })();

  /* Premium: ambient scroll parallax */
  (function initAmbientParallax() {
    if (prefersReducedMotion) return;
    var ambient = root.querySelector(".wsf-ambient");
    if (!ambient) return;
    var ticking = false;
    window.addEventListener(
      "scroll",
      function () {
        if (ticking) return;
        ticking = true;
        requestAnimationFrame(function () {
          ambient.style.transform = "translate3d(0, " + window.scrollY * 0.12 + "px, 0)";
          ticking = false;
        });
      },
      { passive: true }
    );
  })();

  /* Premium: subtle 3D tilt on cards (pointer devices, large screens) */
  (function initCardTilt() {
    if (prefersReducedMotion) return;
    if (!window.matchMedia("(min-width: 768px) and (hover: hover) and (pointer: fine)").matches) return;

    var tiltSelector = ".wsf-deal-card, .wsf-shop-card";
    var maxTilt = 6;

    root.querySelectorAll(tiltSelector).forEach(function (card) {
      card.setAttribute("data-wsf-tilt", "1");

      card.addEventListener("mousemove", function (e) {
        var rect = card.getBoundingClientRect();
        var px = (e.clientX - rect.left) / rect.width - 0.5;
        var py = (e.clientY - rect.top) / rect.height - 0.5;
        card.style.setProperty("--wsf-tilt-x", (-py * maxTilt).toFixed(2) + "deg");
        card.style.setProperty("--wsf-tilt-y", (px * maxTilt).toFixed(2) + "deg");
      });

      card.addEventListener("mouseleave", function () {
        card.style.removeProperty("--wsf-tilt-x");
        card.style.removeProperty("--wsf-tilt-y");
      });
    });
  })();
})();
