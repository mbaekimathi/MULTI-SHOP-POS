(function () {
  function setStatus(el, text, tone) {
    if (!el) return;
    el.textContent = text || "";
    el.className = "su-status" + (tone ? " su-status--" + tone : " su-status--muted");
  }

  var name = document.getElementById("full_name");
  if (name) {
    name.addEventListener("input", function () {
      this.value = this.value.toUpperCase();
    });
    name.addEventListener("blur", function () {
      this.value = this.value.trim().toUpperCase();
    });
  }

  var emailLocal = document.getElementById("email_local");
  var emailDomain = document.getElementById("email_domain");
  var emailCustom = document.getElementById("email_domain_custom");
  var emailCustomWrap = document.getElementById("email-custom-wrap");
  var emailHidden = document.getElementById("email");
  var emailStatus = document.getElementById("email-status");

  function emailDomainMeta() {
    if (!emailDomain) return { value: "gmail.com", hint: "" };
    var opt = emailDomain.options[emailDomain.selectedIndex];
    return { value: opt ? opt.value : "gmail.com", hint: (opt && opt.getAttribute("data-hint")) || "" };
  }

  function sanitizeEmailLocal(raw) {
    return String(raw || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "")
      .replace(/@+/g, "")
      .replace(/[^a-z0-9._-]/g, "");
  }

  function providerLabel() {
    var meta = emailDomainMeta();
    if (meta.value === "__custom__") return "your domain";
    return meta.value || "gmail.com";
  }

  function resolveEmailDomain() {
    var meta = emailDomainMeta();
    if (meta.value === "__custom__") {
      return (emailCustom && emailCustom.value || "").trim().toLowerCase().replace(/^@+/, "");
    }
    return meta.value;
  }

  function composeEmailAddress() {
    var local = sanitizeEmailLocal(emailLocal ? emailLocal.value : "");
    var domain = resolveEmailDomain();
    return local && domain ? local + "@" + domain : "";
  }

  function emailLooksValid(address) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(address || "");
  }

  function splitPastedEmail(value) {
    var v = String(value || "").trim().toLowerCase();
    var at = v.indexOf("@");
    if (at <= 0 || !emailDomain) return;
    var local = v.slice(0, at);
    var domain = v.slice(at + 1);
    emailLocal.value = local;
    var matched = false;
    for (var i = 0; i < emailDomain.options.length; i++) {
      if (emailDomain.options[i].value === domain) {
        emailDomain.selectedIndex = i;
        matched = true;
        break;
      }
    }
    if (!matched) {
      for (var j = 0; j < emailDomain.options.length; j++) {
        if (emailDomain.options[j].value === "__custom__") {
          emailDomain.selectedIndex = j;
          break;
        }
      }
      if (emailCustom) emailCustom.value = domain;
    }
    toggleEmailCustom();
  }

  function toggleEmailCustom() {
    var isCustom = emailDomainMeta().value === "__custom__";
    if (emailCustomWrap) emailCustomWrap.classList.toggle("hidden", !isCustom);
    if (emailCustom) {
      emailCustom.required = isCustom;
      if (!isCustom) emailCustom.value = "";
    }
  }

  function syncEmailField() {
    toggleEmailCustom();
    var full = composeEmailAddress();
    if (emailHidden) emailHidden.value = full;
    if (!emailStatus) return;
    if (!emailLocal || !sanitizeEmailLocal(emailLocal.value)) {
      setStatus(
        emailStatus,
        "Username only — pick " + (emailDomainMeta().value === "gmail.com" ? "Gmail" : "provider") + " on the right (no @)",
        "muted"
      );
      return;
    }
    if (emailDomainMeta().value === "__custom__" && !resolveEmailDomain()) {
      setStatus(emailStatus, "Enter custom domain without @", "warn");
      return;
    }
    if (emailLooksValid(full)) {
      setStatus(emailStatus, "Your email: " + full, "ok");
    } else {
      setStatus(emailStatus, "Check username — do not type @", "warn");
    }
  }

  if (emailDomain) emailDomain.addEventListener("change", syncEmailField);
  if (emailCustom) emailCustom.addEventListener("input", syncEmailField);
  if (emailLocal) {
    emailLocal.addEventListener("input", function () {
      var raw = this.value;
      if (raw.indexOf("@") >= 0) {
        splitPastedEmail(raw);
        setStatus(emailStatus, "We split that for you — just type the username, pick provider on the right", "warn");
      } else {
        this.value = sanitizeEmailLocal(raw);
      }
      syncEmailField();
    });
    emailLocal.addEventListener("paste", function (e) {
      var text = (e.clipboardData && e.clipboardData.getData("text")) || "";
      if (text.indexOf("@") >= 0) {
        e.preventDefault();
        splitPastedEmail(text);
        setStatus(emailStatus, "Full email pasted — username and provider set automatically", "ok");
        syncEmailField();
      }
    });
    emailLocal.addEventListener("keydown", function (e) {
      if (e.key === "@") {
        e.preventDefault();
        setStatus(emailStatus, "No @ needed — select " + providerLabel() + " on the right", "warn");
      }
    });
    emailLocal.addEventListener("blur", syncEmailField);
  }
  syncEmailField();

  var countrySelect = document.getElementById("phone_country");
  var phoneLocal = document.getElementById("phone_local");
  var phoneHidden = document.getElementById("phone");
  var phoneStatus = document.getElementById("phone-status");

  function phoneCountryMeta() {
    if (!countrySelect) return { code: "254", placeholder: "712345678", hint: "" };
    var opt = countrySelect.options[countrySelect.selectedIndex];
    return {
      code: opt ? opt.value : "254",
      placeholder: (opt && opt.getAttribute("data-placeholder")) || "712345678",
      hint: (opt && opt.getAttribute("data-hint")) || "",
    };
  }

  function normalizeSignupPhone(countryCode, raw) {
    var d = String(raw || "").replace(/\D/g, "");
    if (!d) return "";
    if (countryCode === "254") {
      if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
      if (d.indexOf("0") === 0 && d.length >= 10) return "254" + d.slice(1, 11);
      if (d.length === 9) return "254" + d;
      return "254" + d.replace(/^0+/, "");
    }
    return countryCode + (d.indexOf("0") === 0 ? d.slice(1) : d);
  }

  function phoneLooksValid(countryCode, normalized) {
    if (!normalized) return false;
    if (countryCode === "254") return normalized.indexOf("254") === 0 && normalized.length === 12;
    return normalized.length >= 10;
  }

  function syncPhoneField() {
    var meta = phoneCountryMeta();
    if (phoneLocal) phoneLocal.placeholder = meta.placeholder;
    var normalized = normalizeSignupPhone(meta.code, phoneLocal ? phoneLocal.value : "");
    if (phoneHidden) phoneHidden.value = normalized;
    if (!phoneLocal || !phoneLocal.value.trim()) {
      setStatus(phoneStatus, meta.code === "254" ? "Kenya +254" : "Enter mobile number", "muted");
      return;
    }
    setStatus(
      phoneStatus,
      phoneLooksValid(meta.code, normalized) ? "+" + normalized : meta.code === "254" ? "Use 07… or 712…" : "Check number",
      phoneLooksValid(meta.code, normalized) ? "ok" : "warn"
    );
  }

  if (countrySelect) countrySelect.addEventListener("change", syncPhoneField);
  if (phoneLocal) {
    phoneLocal.addEventListener("input", function () {
      this.value = this.value.replace(/\D/g, "").slice(0, 15);
      syncPhoneField();
    });
    phoneLocal.addEventListener("blur", syncPhoneField);
  }
  syncPhoneField();

  var codeInput = document.getElementById("employee_code");
  var codeStatus = document.getElementById("code-status");
  var checkUrl = window.__SIGNUP_CHECK_CODE_URL__ || "";

  function checkCode() {
    var code = codeInput && codeInput.value ? codeInput.value.trim() : "";
    if (!codeStatus || !codeInput) return;
    if (code.length !== 6 || !/^\d{6}$/.test(code)) {
      setStatus(codeStatus, "", "muted");
      return;
    }
    setStatus(codeStatus, "Checking…", "muted");
    fetch(checkUrl + "?code=" + encodeURIComponent(code))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (data.ok && data.available) setStatus(codeStatus, "Available", "ok");
        else if (data.ok && !data.available) setStatus(codeStatus, "Already taken", "err");
        else setStatus(codeStatus, data.message || "Could not verify", "warn");
      })
      .catch(function () {
        setStatus(codeStatus, "Could not verify", "warn");
      });
  }

  if (codeInput) {
    codeInput.addEventListener("blur", checkCode);
    codeInput.addEventListener("input", function () {
      this.value = this.value.replace(/\D/g, "").slice(0, 6);
      setStatus(codeStatus, "", "muted");
    });
  }

  var passwordInput = document.getElementById("password");
  var confirmInput = document.getElementById("confirm_password");
  var passwordHint = document.getElementById("password-hint");
  var matchStatus = document.getElementById("password-match-status");
  var signupForm = document.getElementById("signup-form");

  function digitsOnly(el) {
    if (!el) return;
    el.value = el.value.replace(/\D/g, "").slice(0, 32);
  }

  function syncPasswordFields() {
    var pwd = passwordInput ? passwordInput.value : "";
    var confirm = confirmInput ? confirmInput.value : "";
    if (!pwd) setStatus(passwordHint, "Numbers only", "muted");
    else if (pwd.length < 6) setStatus(passwordHint, pwd.length + "/6 digits", "warn");
    else setStatus(passwordHint, pwd.length + " digits", "ok");

    if (!confirm) {
      setStatus(matchStatus, pwd ? "Confirm password" : "", "muted");
      return;
    }
    if (pwd.length < 6) {
      setStatus(matchStatus, "Need 6+ digits", "warn");
      return;
    }
    setStatus(matchStatus, pwd === confirm ? "Match" : "No match", pwd === confirm ? "ok" : "err");
  }

  if (passwordInput) {
    passwordInput.addEventListener("input", function () {
      digitsOnly(passwordInput);
      syncPasswordFields();
    });
  }
  if (confirmInput) {
    confirmInput.addEventListener("input", function () {
      digitsOnly(confirmInput);
      syncPasswordFields();
    });
    confirmInput.addEventListener("blur", syncPasswordFields);
  }
  syncPasswordFields();

  if (signupForm) {
    signupForm.addEventListener("submit", function (e) {
      syncEmailField();
      var fullEmail = emailHidden ? emailHidden.value : "";
      if (!emailLooksValid(fullEmail)) {
        e.preventDefault();
        setStatus(emailStatus, "Enter username only, then pick provider (no @)", "err");
        if (emailLocal) emailLocal.focus();
        return;
      }
      syncPhoneField();
      var pMeta = phoneCountryMeta();
      var normalized = phoneHidden ? phoneHidden.value : "";
      if (!phoneLooksValid(pMeta.code, normalized)) {
        e.preventDefault();
        setStatus(phoneStatus, "Enter a valid phone", "err");
        if (phoneLocal) phoneLocal.focus();
        return;
      }
      digitsOnly(passwordInput);
      digitsOnly(confirmInput);
      syncPasswordFields();
      var pwd = passwordInput ? passwordInput.value : "";
      var confirm = confirmInput ? confirmInput.value : "";
      if (!/^\d{6,32}$/.test(pwd)) {
        e.preventDefault();
        setStatus(passwordHint, "Min 6 digits", "err");
        if (passwordInput) passwordInput.focus();
        return;
      }
      if (pwd !== confirm) {
        e.preventDefault();
        setStatus(matchStatus, "Passwords must match", "err");
        if (confirmInput) confirmInput.focus();
      }
    });
  }
})();
