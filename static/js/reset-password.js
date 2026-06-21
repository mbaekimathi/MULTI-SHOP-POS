(function () {
  "use strict";

  var cfg = window.__RESET_PASSWORD_CONFIG || {};
  var codeInput = document.getElementById("rp_employee_code");
  var emailInput = document.getElementById("rp_email");
  var emailStatus = document.getElementById("rp_email_status");
  var verifyBtn = document.getElementById("rp-verify-btn");
  var sendBtn = document.getElementById("rp-send-btn");
  var resetStep = document.getElementById("rp-reset-step");
  var form = document.getElementById("reset-password-form");
  var passwordStatus = document.getElementById("rp_password_status");

  var verified = false;
  var codeSent = false;
  var lookupTimer = null;

  function setStatus(el, text, tone) {
    if (!el) return;
    el.textContent = text || "";
    el.className = "rp-status" + (tone ? " rp-status--" + tone : "");
  }

  function payload() {
    var code = (codeInput && codeInput.value ? codeInput.value : "").replace(/\D/g, "").slice(0, 6);
    return {
      employee_code: code.length === 6 ? code : "",
      email: (emailInput && emailInput.value ? emailInput.value : "").trim().toLowerCase(),
    };
  }

  function postJson(url, body) {
    return fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "X-Requested-With": "XMLHttpRequest",
      },
      credentials: "same-origin",
      body: JSON.stringify(body),
    }).then(function (res) {
      return res.json().then(function (data) {
        return { ok: res.ok, status: res.status, data: data || {} };
      });
    });
  }

  function resetFlowState() {
    verified = false;
    codeSent = false;
    if (sendBtn) {
      sendBtn.hidden = true;
      sendBtn.disabled = true;
    }
    if (resetStep) resetStep.hidden = true;
  }

  function lookupEmailForCode(code) {
    if (!cfg.lookupUrl || code.length !== 6) return;
    var url = cfg.lookupUrl + (cfg.lookupUrl.indexOf("?") >= 0 ? "&" : "?") + "code=" + encodeURIComponent(code);
    fetch(url, { headers: { Accept: "application/json" }, credentials: "same-origin" })
      .then(function (res) {
        return res.json();
      })
      .then(function (data) {
        if (data && data.ok && data.email && emailInput) {
          emailInput.value = data.email;
          setStatus(emailStatus, "Registered email loaded from your employee code.", "ok");
          resetFlowState();
        }
      })
      .catch(function () {});
  }

  if (codeInput) {
    codeInput.addEventListener("input", function () {
      codeInput.value = (codeInput.value || "").replace(/\D/g, "").slice(0, 6);
      resetFlowState();
      setStatus(emailStatus, "", "");
      clearTimeout(lookupTimer);
      if (codeInput.value.length === 6) {
        lookupTimer = setTimeout(function () {
          lookupEmailForCode(codeInput.value);
        }, 250);
      }
    });
  }

  if (emailInput) {
    emailInput.addEventListener("input", function () {
      resetFlowState();
      setStatus(emailStatus, "", "");
    });
  }

  if (cfg.prefillCode && codeInput && !codeInput.value) {
    codeInput.value = String(cfg.prefillCode).replace(/\D/g, "").slice(0, 6);
  }
  if (codeInput && codeInput.value.length === 6) {
    lookupEmailForCode(codeInput.value);
  }

  if (verifyBtn) {
    verifyBtn.addEventListener("click", function () {
      var body = payload();
      if (!body.email || body.email.indexOf("@") < 1) {
        setStatus(emailStatus, "Enter your registered email address.", "err");
        if (emailInput) emailInput.focus();
        return;
      }
      verifyBtn.disabled = true;
      setStatus(emailStatus, "Checking email…", "muted");
      postJson(cfg.verifyUrl, body)
        .then(function (res) {
          var data = res.data || {};
          if (!res.ok) {
            setStatus(emailStatus, data.error || "Could not verify email.", "err");
            resetFlowState();
            return;
          }
          if (!data.registered) {
            setStatus(emailStatus, data.message || "This email is not registered.", "err");
            resetFlowState();
            return;
          }
          if (data.matched === false) {
            setStatus(emailStatus, data.message || "Email and employee code do not match.", "warn");
            resetFlowState();
            return;
          }
          verified = true;
          setStatus(emailStatus, data.message || "Email verified.", "ok");
          if (sendBtn) {
            sendBtn.hidden = false;
            sendBtn.disabled = false;
          }
        })
        .catch(function () {
          setStatus(emailStatus, "Could not reach the server.", "err");
          resetFlowState();
        })
        .finally(function () {
          verifyBtn.disabled = false;
        });
    });
  }

  if (sendBtn) {
    sendBtn.addEventListener("click", function () {
      if (!verified) return;
      var body = payload();
      sendBtn.disabled = true;
      setStatus(emailStatus, "Sending verification code…", "muted");
      postJson(cfg.sendUrl, body)
        .then(function (res) {
          var data = res.data || {};
          if (!res.ok) {
            setStatus(emailStatus, data.error || "Could not send code.", "err");
            return;
          }
          codeSent = true;
          setStatus(emailStatus, data.message || "Verification code sent.", "ok");
          if (resetStep) {
            resetStep.hidden = false;
            var codeField = document.getElementById("rp_verification_code");
            if (codeField) codeField.focus();
          }
        })
        .catch(function () {
          setStatus(emailStatus, "Could not reach the server.", "err");
        })
        .finally(function () {
          sendBtn.disabled = false;
        });
    });
  }

  if (form) {
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      if (!verified || !codeSent) {
        setStatus(emailStatus, "Verify your email and send a verification code first.", "err");
        return;
      }
      var body = payload();
      body.verification_code = (document.getElementById("rp_verification_code") || {}).value || "";
      body.password = (document.getElementById("rp_password") || {}).value || "";
      body.confirm_password = (document.getElementById("rp_confirm_password") || {}).value || "";
      body.verification_code = body.verification_code.replace(/\D/g, "").slice(0, 6);

      if (body.verification_code.length !== 6) {
        setStatus(passwordStatus, "Enter the 6-digit verification code.", "err");
        return;
      }
      if (body.password.length < 6) {
        setStatus(passwordStatus, "Password must be at least 6 characters.", "err");
        return;
      }
      if (body.password !== body.confirm_password) {
        setStatus(passwordStatus, "Passwords do not match.", "err");
        return;
      }

      var submitBtn = document.getElementById("rp-submit-btn");
      if (submitBtn) submitBtn.disabled = true;
      setStatus(passwordStatus, "Updating password…", "muted");

      postJson(cfg.completeUrl, body)
        .then(function (res) {
          var data = res.data || {};
          if (!res.ok) {
            setStatus(passwordStatus, data.error || "Could not reset password.", "err");
            return;
          }
          setStatus(passwordStatus, data.message || "Password updated.", "ok");
          window.setTimeout(function () {
            window.location.href = data.login_url || cfg.loginUrl || "/login";
          }, 900);
        })
        .catch(function () {
          setStatus(passwordStatus, "Could not reach the server.", "err");
        })
        .finally(function () {
          if (submitBtn) submitBtn.disabled = false;
        });
    });
  }
})();
