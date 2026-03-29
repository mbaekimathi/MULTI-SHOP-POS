/**
 * Show/hide password fields via .js-toggle-password buttons (aria-controls = input id).
 */
(function () {
  function init() {
    document.querySelectorAll(".js-toggle-password").forEach(function (btn) {
      var targetId = btn.getAttribute("aria-controls");
      if (!targetId) return;
      var input = document.getElementById(targetId);
      if (!input) return;
      var iconShow = btn.querySelector(".js-pw-eye-open");
      var iconHide = btn.querySelector(".js-pw-eye-off");

      btn.addEventListener("click", function () {
        var masked = input.type === "password";
        if (masked) {
          input.type = "text";
          btn.setAttribute("aria-pressed", "true");
          btn.setAttribute("aria-label", "Hide password");
          if (iconShow) iconShow.classList.add("hidden");
          if (iconHide) iconHide.classList.remove("hidden");
        } else {
          input.type = "password";
          btn.setAttribute("aria-pressed", "false");
          btn.setAttribute("aria-label", "Show password");
          if (iconShow) iconShow.classList.remove("hidden");
          if (iconHide) iconHide.classList.add("hidden");
        }
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
