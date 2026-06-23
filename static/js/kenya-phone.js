/**
 * Kenya mobile numbers — canonical 254… storage (matches server normalize_*_phone).
 */
(function (global) {
  function digitsOnly(raw) {
    return String(raw || "").replace(/\D/g, "");
  }

  function normalizeKenyaPhone(raw) {
    var d = digitsOnly(raw);
    if (!d || d === "-") return "";
    if (d.indexOf("254") === 0 && d.length >= 12) return d.slice(0, 12);
    if (d.charAt(0) === "0" && d.length >= 10) return "254" + d.slice(1, 11);
    if (d.length === 9 && (d.charAt(0) === "7" || d.charAt(0) === "1")) return "254" + d;
    return d;
  }

  function applyKenyaPhoneInput(el) {
    if (!el) return "";
    var n = normalizeKenyaPhone(el.value);
    if (n) el.value = n;
    return n;
  }

  function bindKenyaPhoneInputs(selector, root) {
    var scope = root || document;
    scope.querySelectorAll(selector).forEach(function (el) {
      if (el._kenyaPhoneBound) return;
      el._kenyaPhoneBound = true;
      el.addEventListener("blur", function () {
        applyKenyaPhoneInput(el);
      });
    });
  }

  global.KenyaPhone = {
    normalize: normalizeKenyaPhone,
    applyToInput: applyKenyaPhoneInput,
    bind: bindKenyaPhoneInputs,
  };
})(window);
