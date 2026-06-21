/**
 * Warn before employee session idle timeout; sign out if the user does not respond.
 */
(function () {
  "use strict";

  var root = document.getElementById("employee-session-idle-root");
  var modal = document.getElementById("employee-session-idle-modal");
  if (!root || !modal) return;

  var idleMs = parseInt(root.getAttribute("data-idle-ms") || "300000", 10);
  var warnMs = parseInt(root.getAttribute("data-warn-ms") || "60000", 10);
  var pingUrl = root.getAttribute("data-ping-url") || "";
  var logoutUrl = root.getAttribute("data-logout-url") || "";
  var loginUrl = root.getAttribute("data-login-url") || "/login";

  if (!idleMs || idleMs < 60000) idleMs = 300000;
  if (!warnMs || warnMs >= idleMs) warnMs = Math.min(60000, idleMs - 15000);

  var countEl = document.getElementById("employee-session-idle-count");
  var stayBtn = modal.querySelector("[data-idle-stay]");
  var logoutBtn = modal.querySelector("[data-idle-logout]");

  var lastActivity = Date.now();
  var warnVisible = false;
  var countdownTimer = null;
  var signingOut = false;

  function hideWarn() {
    warnVisible = false;
    modal.hidden = true;
    document.documentElement.classList.remove("employee-session-idle-open");
    if (countdownTimer) {
      clearInterval(countdownTimer);
      countdownTimer = null;
    }
  }

  function currentReturnUrl() {
    var path = window.location.pathname || "";
    var search = window.location.search || "";
    var url = path + search;
    if (!url || !url.startsWith("/") || url.startsWith("//")) return "";
    if (path === "/login" || path === "/logout") return "";
    return url;
  }

  function appendQuery(url, key, value) {
    if (!url || !key || value === undefined || value === null || value === "") return url;
    var sep = url.indexOf("?") >= 0 ? "&" : "?";
    return url + sep + encodeURIComponent(key) + "=" + encodeURIComponent(value);
  }

  function loginUrlWithContinuation(baseUrl, data) {
    var url = (data && data.login_url) || baseUrl || "/login";
    if (url.indexOf("next=") >= 0) return url;
    var returnUrl = currentReturnUrl();
    if (!returnUrl) return appendQuery(url, "reason", "idle");
    url = appendQuery(url, "next", returnUrl);
    return appendQuery(url, "reason", "idle");
  }

  function pingServer() {
    if (!pingUrl) return Promise.resolve({ ok: true });
    return fetch(pingUrl, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        Accept: "application/json",
      },
      credentials: "same-origin",
    }).then(function (res) {
      return res.json().then(function (data) {
        return { ok: res.ok, data: data };
      });
    });
  }

  function redirectExpired(data) {
    window.location.href = loginUrlWithContinuation(loginUrl, data);
  }

  function doLogout() {
    if (signingOut) return;
    signingOut = true;
    var url = logoutUrl || loginUrl;
    var returnUrl = currentReturnUrl();
    if (returnUrl) url = appendQuery(url, "next", returnUrl);
    window.location.href = url;
  }

  function markActive(fromUser) {
    lastActivity = Date.now();
    if (!warnVisible) return;
    hideWarn();
    if (fromUser) {
      pingServer().then(function (res) {
        if (!res.ok) redirectExpired(res.data);
      }).catch(function () {});
    }
  }

  function showWarn(secondsLeft) {
    if (warnVisible || signingOut) return;
    warnVisible = true;
    modal.hidden = false;
    document.documentElement.classList.add("employee-session-idle-open");

    var left = Math.max(1, secondsLeft);
    if (countEl) countEl.textContent = String(left);

    if (countdownTimer) clearInterval(countdownTimer);
    countdownTimer = setInterval(function () {
      left -= 1;
      if (countEl) countEl.textContent = String(Math.max(0, left));
      if (left <= 0) {
        clearInterval(countdownTimer);
        countdownTimer = null;
        doLogout();
      }
    }, 1000);
  }

  function checkIdle() {
    if (warnVisible || signingOut) return;
    var idle = Date.now() - lastActivity;
    var warnAt = idleMs - warnMs;
    if (idle >= warnAt) {
      var secondsLeft = Math.max(1, Math.ceil((idleMs - idle) / 1000));
      showWarn(secondsLeft);
    }
  }

  ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"].forEach(function (ev) {
    document.addEventListener(
      ev,
      function () {
        if (warnVisible) {
          markActive(true);
        } else {
          lastActivity = Date.now();
        }
      },
      { passive: true }
    );
  });

  if (stayBtn) {
    stayBtn.addEventListener("click", function () {
      stayBtn.disabled = true;
      pingServer()
        .then(function (res) {
          if (res.ok && res.data && res.data.ok) {
            markActive(false);
            lastActivity = Date.now();
          } else {
            redirectExpired(res.data);
          }
        })
        .catch(function () {
          lastActivity = Date.now();
          hideWarn();
        })
        .finally(function () {
          stayBtn.disabled = false;
        });
    });
  }

  if (logoutBtn) {
    logoutBtn.addEventListener("click", doLogout);
  }

  setInterval(checkIdle, 5000);
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") checkIdle();
  });
})();
