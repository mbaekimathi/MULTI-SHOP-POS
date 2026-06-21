/**
 * Login auth toasts — wrong credentials, welcome name, etc. Auto-dismiss after 2s.
 */
(function () {
  var AUTO_MS = 2000;
  var TOAST_CATEGORIES = { error: 1, warning: 1, login_welcome: 1 };

  var ICONS = {
    success:
      '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>',
    error:
      '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/></svg>',
    warning:
      '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/></svg>',
  };

  function formatName(name) {
    var raw = (name || "").trim();
    if (!raw) return "there";
    return raw
      .toLowerCase()
      .replace(/\b\w/g, function (ch) {
        return ch.toUpperCase();
      });
  }

  function mapToast(category, message) {
    var msg = (message || "").trim();
    var cat = (category || "").trim().toLowerCase();

    if (cat === "login_welcome") {
      var name = formatName(msg);
      return {
        type: "success",
        title: "Welcome back, " + name + "!",
        message: "You are signed in successfully.",
      };
    }

    if (cat === "error") {
      if (/invalid employee code or password/i.test(msg)) {
        return {
          type: "error",
          title: "Wrong credentials",
          message: "The employee code or password is incorrect. Please try again.",
        };
      }
      return {
        type: "error",
        title: "Sign-in failed",
        message: msg || "Something went wrong. Please try again.",
      };
    }

    if (cat === "warning") {
      return {
        type: "warning",
        title: "Account notice",
        message: msg || "Please review your account status.",
      };
    }

    return null;
  }

  function initIdleSignoutModal() {
    var modal = document.getElementById("auth-idle-signout-modal");
    if (!modal) return;

    function closeModal() {
      modal.classList.remove("is-visible");
      modal.setAttribute("aria-hidden", "true");
      window.setTimeout(function () {
        if (modal.parentNode) modal.parentNode.removeChild(modal);
      }, 220);
    }

    modal.querySelectorAll("[data-idle-signout-dismiss]").forEach(function (el) {
      el.addEventListener("click", closeModal);
    });

    document.addEventListener("keydown", function onKey(e) {
      if (e.key === "Escape" && modal.classList.contains("is-visible")) {
        e.preventDefault();
        closeModal();
        document.removeEventListener("keydown", onKey);
      }
    });

    var dismissBtn = modal.querySelector(".auth-login-toast__dismiss");
    if (dismissBtn) {
      window.setTimeout(function () {
        try {
          dismissBtn.focus();
        } catch (e) {}
      }, 50);
    }
  }

  function init() {
    var dataEl = document.getElementById("auth-login-toast-data");
    var root = document.getElementById("auth-login-toast");
    if (!dataEl || !root) return;

    var items = [];
    try {
      items = JSON.parse(dataEl.textContent || "[]");
    } catch (e) {
      return;
    }
    if (!items.length) return;

    var card = root.querySelector(".auth-login-toast__card");
    var iconEl = root.querySelector(".auth-login-toast__icon");
    var titleEl = root.querySelector(".auth-login-toast__title");
    var messageEl = root.querySelector(".auth-login-toast__message");
    var progressBar = root.querySelector(".auth-login-toast__progress-bar");
    if (!card || !iconEl || !titleEl || !messageEl || !progressBar) return;

    var queue = [];
    items.forEach(function (entry) {
      if (!entry || entry.length < 2) return;
      if (!TOAST_CATEGORIES[entry[0]]) return;
      var mapped = mapToast(entry[0], entry[1]);
      if (mapped) queue.push(mapped);
    });
    if (!queue.length) return;

    var hideTimer = null;
    var animTimer = null;
    var idx = 0;
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function hideToast() {
      root.classList.remove("is-visible");
      window.setTimeout(function () {
        showNext();
      }, reduced ? 0 : 220);
    }

    function showNext() {
      if (idx >= queue.length) return;
      var toast = queue[idx++];
      root.className = "auth-login-toast auth-login-toast--" + toast.type;
      iconEl.innerHTML = ICONS[toast.type] || ICONS.warning;
      titleEl.textContent = toast.title;
      messageEl.textContent = toast.message;

      progressBar.style.transition = "none";
      progressBar.style.transform = "scaleX(1)";

      root.classList.add("is-visible");
      root.setAttribute("aria-hidden", "false");

      clearTimeout(hideTimer);
      clearTimeout(animTimer);

      if (!reduced) {
        window.requestAnimationFrame(function () {
          progressBar.style.transition = "transform " + AUTO_MS + "ms linear";
          progressBar.style.transform = "scaleX(0)";
        });
      }

      hideTimer = window.setTimeout(hideToast, AUTO_MS);
    }

    showNext();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      initIdleSignoutModal();
      init();
    });
  } else {
    initIdleSignoutModal();
    init();
  }
})();
