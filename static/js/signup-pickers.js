(function () {
  function closeAllPickers(except) {
    document.querySelectorAll("[data-su-picker]").forEach(function (picker) {
      if (except && picker === except) return;
      var menu = picker.querySelector(".su-picker__menu");
      var btn = picker.querySelector(".su-picker__toggle");
      if (menu) menu.hidden = true;
      if (btn) btn.setAttribute("aria-expanded", "false");
    });
  }

  function bindFlagFallbacks(root) {
    (root || document).querySelectorAll(".su-flag__img").forEach(function (img) {
      if (img.dataset.fallbackBound) return;
      img.dataset.fallbackBound = "1";
      img.addEventListener("error", function () {
        var wrap = img.closest(".su-flag");
        if (wrap) wrap.classList.add("su-flag--fallback");
      });
    });
  }

  function syncPickerDisplay(picker, optionEl) {
    var select = picker.querySelector(".su-picker-native");
    var iconSlot = picker.querySelector("[data-picker-icon]");
    var labelSlot = picker.querySelector("[data-picker-label]");
    if (!select || !optionEl) return;

    var value = optionEl.getAttribute("data-value") || "";
    select.value = value;

    picker.querySelectorAll(".su-picker__option").forEach(function (li) {
      var on = li === optionEl;
      li.classList.toggle("is-selected", on);
      li.setAttribute("aria-selected", on ? "true" : "false");
    });

    if (iconSlot) {
      var iconSource = optionEl.querySelector(".su-picker__option-icon");
      iconSlot.innerHTML = iconSource ? iconSource.innerHTML : "";
      bindFlagFallbacks(iconSlot);
    }

    if (labelSlot) {
      var kind = picker.getAttribute("data-su-picker");
      if (kind === "phone") {
        labelSlot.textContent = "+" + value;
      } else {
        labelSlot.textContent = optionEl.getAttribute("data-label") || value;
      }
    }

    select.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function initPicker(picker) {
    var select = picker.querySelector(".su-picker-native");
    var btn = picker.querySelector(".su-picker__toggle");
    var menu = picker.querySelector(".su-picker__menu");
    if (!select || !btn || !menu) return;

    bindFlagFallbacks(picker);

    btn.addEventListener("click", function (e) {
      e.stopPropagation();
      var open = btn.getAttribute("aria-expanded") === "true";
      closeAllPickers(picker);
      btn.setAttribute("aria-expanded", open ? "false" : "true");
      menu.hidden = open;
    });

    menu.querySelectorAll(".su-picker__option").forEach(function (optionEl) {
      optionEl.addEventListener("click", function () {
        syncPickerDisplay(picker, optionEl);
        btn.setAttribute("aria-expanded", "false");
        menu.hidden = true;
      });
    });

    var selected =
      menu.querySelector(".su-picker__option.is-selected") ||
      menu.querySelector('.su-picker__option[data-value="' + select.value + '"]') ||
      menu.querySelector(".su-picker__option");
    if (selected) syncPickerDisplay(picker, selected);
  }

  document.querySelectorAll("[data-su-picker]").forEach(initPicker);

  document.addEventListener("click", function () {
    closeAllPickers(null);
  });

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") closeAllPickers(null);
  });
})();
