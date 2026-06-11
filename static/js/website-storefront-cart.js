(function () {
  var root = document.getElementById("wsf-root");
  if (!root || root.getAttribute("data-wsf-cart-disabled") === "1") return;

  var quoteUrl = root.getAttribute("data-quote-url") || "";
  var storageKey = "wsf-cart-v1";
  var cart = [];
  var productsById = {};

  try {
    var catalogEl = document.getElementById("wsf-products-json");
    if (catalogEl && catalogEl.textContent) {
      JSON.parse(catalogEl.textContent).forEach(function (p) {
        productsById[String(p.id)] = p;
      });
    }
  } catch (e) {}

  function loadCart() {
    try {
      var raw = localStorage.getItem(storageKey);
      cart = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(cart)) cart = [];
    } catch (e) {
      cart = [];
    }
    cart = cart.filter(function (row) {
      return row && productsById[String(row.id)] && Number(row.qty) > 0;
    });
    saveCart();
  }

  function saveCart() {
    try {
      localStorage.setItem(storageKey, JSON.stringify(cart));
    } catch (e) {}
  }

  function cartCount() {
    return cart.reduce(function (n, row) {
      return n + Number(row.qty || 0);
    }, 0);
  }

  function cartTotal() {
    return cart.reduce(function (sum, row) {
      var p = productsById[String(row.id)];
      if (!p) return sum;
      return sum + Number(p.price || 0) * Number(row.qty || 0);
    }, 0);
  }

  function fmtMoney(n) {
    return "KES " + Number(n || 0).toFixed(2);
  }

  function findRow(id) {
    var sid = String(id);
    for (var i = 0; i < cart.length; i++) {
      if (String(cart[i].id) === sid) return cart[i];
    }
    return null;
  }

  function notifyCartUpdate() {
    var count = cartCount();
    document.dispatchEvent(new CustomEvent("wsf-cart-updated", { detail: { count: count } }));
  }

  function addToCart(id, qty, addBtn) {
    var p = productsById[String(id)];
    if (!p) return;
    var q = Math.max(1, Math.min(999, parseInt(qty, 10) || 1));
    var row = findRow(id);
    if (row) row.qty = Math.min(999, Number(row.qty || 0) + q);
    else cart.push({ id: p.id, qty: q });
    saveCart();
    render();
    notifyCartUpdate();

    if (addBtn) {
      addBtn.classList.add("is-added");
      setTimeout(function () {
        addBtn.classList.remove("is-added");
      }, 500);
    }

    var cartBtn = root.querySelector(".wsf-cart-btn");
    if (cartBtn) {
      cartBtn.classList.remove("is-bump");
      void cartBtn.offsetWidth;
      cartBtn.classList.add("is-bump");
    }

    if (typeof window.wsfShowToast === "function") {
      window.wsfShowToast("Added · " + (p.name || "Product"));
    }
  }

  function setQty(id, qty) {
    var row = findRow(id);
    if (!row) return;
    var q = parseInt(qty, 10) || 0;
    if (q <= 0) removeFromCart(id);
    else {
      row.qty = Math.min(999, q);
      saveCart();
      render();
    }
  }

  function removeFromCart(id) {
    var sid = String(id);
    cart = cart.filter(function (row) {
      return String(row.id) !== sid;
    });
    saveCart();
    render();
  }

  var drawer = document.getElementById("wsf-cart-drawer");
  var backdrop = document.getElementById("wsf-cart-backdrop");
  var linesEl = document.getElementById("wsf-cart-lines");
  var totalEl = document.getElementById("wsf-cart-total");
  var countEls = root.querySelectorAll("[data-wsf-cart-count]");
  var form = document.getElementById("wsf-quote-form");
  var formWrap = document.getElementById("wsf-quote-form-wrap");
  var successEl = document.getElementById("wsf-quote-success");
  var errorEl = document.getElementById("wsf-quote-error");
  var submitBtn = document.getElementById("wsf-quote-submit");

  function openDrawer() {
    if (!drawer) return;
    drawer.classList.add("is-open");
    if (backdrop) backdrop.classList.add("is-open");
    document.body.classList.add("wsf-cart-open");
  }

  function closeDrawer() {
    if (!drawer) return;
    drawer.classList.remove("is-open");
    if (backdrop) backdrop.classList.remove("is-open");
    document.body.classList.remove("wsf-cart-open");
  }

  function render() {
    var count = cartCount();
    countEls.forEach(function (el) {
      el.textContent = String(count);
      el.classList.toggle("hidden", count <= 0);
    });

    if (!linesEl) return;

    if (!cart.length) {
      linesEl.innerHTML =
        '<p class="wsf-cart-empty">Your cart is empty. Add products to request a quotation.</p>';
      if (totalEl) totalEl.textContent = fmtMoney(0);
      if (formWrap) formWrap.classList.add("hidden");
      notifyCartUpdate();
      return;
    }

    if (formWrap) formWrap.classList.remove("hidden");
    linesEl.innerHTML = cart
      .map(function (row) {
        var p = productsById[String(row.id)];
        if (!p) return "";
        var img = p.image_url
          ? '<img src="' + p.image_url + '" alt="" class="wsf-cart-line__img" />'
          : '<span class="wsf-cart-line__img wsf-cart-line__img--ph">' + (p.name || "?").charAt(0) + "</span>";
        return (
          '<div class="wsf-cart-line" data-cart-id="' +
          p.id +
          '">' +
          img +
          '<div class="wsf-cart-line__body">' +
          '<p class="wsf-cart-line__name">' +
          (p.name || "Product") +
          "</p>" +
          '<p class="wsf-cart-line__price">' +
          fmtMoney(Number(p.price || 0) * Number(row.qty || 0)) +
          "</p>" +
          '<div class="wsf-cart-line__qty">' +
          '<button type="button" class="wsf-qty-btn" data-qty-delta="-1" data-id="' +
          p.id +
          '" aria-label="Decrease quantity">−</button>' +
          '<span class="wsf-qty-val">' +
          row.qty +
          "</span>" +
          '<button type="button" class="wsf-qty-btn" data-qty-delta="1" data-id="' +
          p.id +
          '" aria-label="Increase quantity">+</button>' +
          "</div>" +
          "</div>" +
          '<button type="button" class="wsf-cart-line__remove" data-remove-id="' +
          p.id +
          '" aria-label="Remove item">&times;</button>' +
          "</div>"
        );
      })
      .join("");

    if (totalEl) totalEl.textContent = fmtMoney(cartTotal());
    notifyCartUpdate();
  }

  root.addEventListener("click", function (e) {
    var addBtn = e.target.closest("[data-wsf-add-id]");
    if (addBtn) {
      e.preventDefault();
      addToCart(addBtn.getAttribute("data-wsf-add-id"), 1, addBtn);
      return;
    }
    var openBtn = e.target.closest("[data-wsf-open-cart]");
    if (openBtn) {
      e.preventDefault();
      openDrawer();
      return;
    }
    var closeBtn = e.target.closest("[data-wsf-close-cart]");
    if (closeBtn) {
      e.preventDefault();
      closeDrawer();
    }
  });

  if (linesEl) {
    linesEl.addEventListener("click", function (e) {
      var rem = e.target.closest("[data-remove-id]");
      if (rem) {
        removeFromCart(rem.getAttribute("data-remove-id"));
        return;
      }
      var deltaBtn = e.target.closest("[data-qty-delta]");
      if (deltaBtn) {
        var id = deltaBtn.getAttribute("data-id");
        var row = findRow(id);
        if (!row) return;
        var delta = parseInt(deltaBtn.getAttribute("data-qty-delta"), 10) || 0;
        setQty(id, Number(row.qty || 0) + delta);
      }
    });
  }

  if (backdrop) {
    backdrop.addEventListener("click", closeDrawer);
  }

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") closeDrawer();
  });

  if (form && quoteUrl) {
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      if (!cart.length) {
        if (errorEl) {
          errorEl.textContent = "Add at least one product to your cart.";
          errorEl.classList.remove("hidden");
        }
        return;
      }
      if (errorEl) errorEl.classList.add("hidden");
      if (successEl) successEl.classList.add("hidden");
      if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "Sending…";
      }

      var payload = {
        customer_name: (form.customer_name && form.customer_name.value) || "",
        customer_phone: (form.customer_phone && form.customer_phone.value) || "",
        customer_notes: (form.customer_notes && form.customer_notes.value) || "",
        lines: cart.map(function (row) {
          return { id: row.id, qty: row.qty };
        }),
      };

      fetch(quoteUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(payload),
      })
        .then(function (r) {
          return r.json().then(function (j) {
            return { ok: r.ok, body: j };
          });
        })
        .then(function (res) {
          if (!res.ok || !res.body || !res.body.ok) {
            throw new Error((res.body && res.body.error) || "Could not submit request.");
          }
          cart = [];
          saveCart();
          render();
          form.reset();
          if (successEl) {
            successEl.textContent = res.body.message || "Quotation request received.";
            successEl.classList.remove("hidden");
          }
          if (typeof window.wsfShowToast === "function") {
            window.wsfShowToast("Quotation sent — we'll be in touch!");
          }
        })
        .catch(function (err) {
          if (errorEl) {
            errorEl.textContent = err.message || "Could not submit request.";
            errorEl.classList.remove("hidden");
          }
        })
        .finally(function () {
          if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = "Request quotation";
          }
        });
    });
  }

  loadCart();
  render();
})();
