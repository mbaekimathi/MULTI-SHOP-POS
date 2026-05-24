/**
 * Shop credit analytics — customer line-item modal (reads live JSON after filter refresh).
 */
(function () {
  var modal = document.getElementById("it-credit-customer-modal");
  if (!modal) return;

  var body = document.getElementById("it-credit-customer-modal-body");
  var title = document.getElementById("it-credit-customer-modal-title");
  var subtitle = document.getElementById("it-credit-customer-modal-subtitle");
  var closeBtn = document.getElementById("it-credit-customer-modal-close");
  var backdrop = modal.querySelector("[data-it-credit-modal-backdrop]");

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmt(n) {
    var x = parseFloat(n);
    if (isNaN(x)) x = 0;
    return x.toFixed(2);
  }

  function getDetailsMap() {
    var el = document.getElementById("it-support-analytics-json");
    if (!el || !el.textContent) return {};
    try {
      var p = JSON.parse(el.textContent);
      return (p.shop_data && p.shop_data.customer_details) || {};
    } catch (e) {
      return {};
    }
  }

  function openModal(customerKey) {
    var detailsMap = getDetailsMap();
    var sales = detailsMap[customerKey] || [];
    var parts = String(customerKey || "").split("|||");
    var customerName = parts[0] || "Customer";
    var customerPhone = parts[1] || "-";
    if (title) title.textContent = customerName;
    if (subtitle) subtitle.textContent = customerPhone;

    if (!body) return;
    if (!sales.length) {
      body.innerHTML =
        '<p class="rev-empty">No purchase details found for this customer in the selected range.</p>';
    } else {
      body.innerHTML = sales
        .map(function (s) {
          var items = (s.items || [])
            .map(function (it) {
              return (
                "<tr><td class=\"cell-strong\">" +
                esc(it.item_name) +
                '</td><td class="num cell-muted">' +
                esc(String(it.qty || 0)) +
                '</td><td class="num cell-credit">' +
                esc(fmt(it.line_total || 0)) +
                "</td></tr>"
              );
            })
            .join("");
          return (
            '<section class="rev-credit-sale-block">' +
            '<div class="rev-credit-sale-block__head">' +
            '<span>When: <strong>' +
            esc(s.created_at || "-") +
            "</strong></span>" +
            "<span>Served by: " +
            esc(s.employee_name || "Unknown") +
            (s.employee_code ? " (" + esc(s.employee_code) + ")" : "") +
            "</span>" +
            '<span class="rev-credit-sale-block__total">Total: ' +
            esc(fmt(s.total_amount || 0)) +
            "</span>" +
            "</div>" +
            '<div class="rev-scroll rev-scroll--hint"><table class="rev-data rev-data--compact"><thead><tr>' +
            "<th scope=\"col\">Item</th><th scope=\"col\" class=\"num\">Qty</th><th scope=\"col\" class=\"num\">Amount</th>" +
            "</tr></thead><tbody>" +
            items +
            "</tbody></table></div></section>"
          );
        })
        .join("");
    }

    modal.classList.remove("hidden");
    requestAnimationFrame(function () {
      modal.classList.remove("it-credit-modal--closed");
      modal.setAttribute("aria-hidden", "false");
    });
  }

  function closeModal() {
    modal.classList.add("it-credit-modal--closed");
    modal.setAttribute("aria-hidden", "true");
    setTimeout(function () {
      modal.classList.add("hidden");
    }, 180);
  }

  document.addEventListener("click", function (e) {
    var btn = e.target && e.target.closest ? e.target.closest(".it-credit-customer-view") : null;
    if (btn) {
      e.preventDefault();
      openModal(btn.getAttribute("data-customer-key") || "");
      return;
    }
    if (
      !modal.classList.contains("hidden") &&
      (e.target === backdrop || e.target === modal)
    ) {
      closeModal();
    }
  });

  if (closeBtn) closeBtn.addEventListener("click", closeModal);
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && !modal.classList.contains("hidden")) closeModal();
  });
})();
