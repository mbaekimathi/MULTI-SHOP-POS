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

    var cartBtn = document.getElementById("wsf-fab-cart");
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
  var headCountEl = document.getElementById("wsf-cart-head-count");
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
    if (headCountEl) headCountEl.textContent = String(count);

    if (!linesEl) return;

    if (!cart.length) {
      linesEl.innerHTML =
        '<div class="wsf-cart-empty">' +
        '<div class="wsf-cart-empty__icon" aria-hidden="true">' +
        '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 11-4 0 2 2 0 014 0z"/></svg>' +
        "</div>" +
        '<p class="wsf-cart-empty__title">Your bag is empty</p>' +
        '<p class="wsf-cart-empty__sub">Browse products and tap add to build your quote.</p>' +
        "</div>";
      if (totalEl) totalEl.textContent = fmtMoney(0);
      if (formWrap) formWrap.classList.add("hidden");
      if (submitBtn) submitBtn.disabled = true;
      notifyCartUpdate();
      return;
    }

    if (formWrap) formWrap.classList.remove("hidden");
    if (submitBtn) submitBtn.disabled = false;
    linesEl.innerHTML = cart
      .map(function (row) {
        var p = productsById[String(row.id)];
        if (!p) return "";
        var unit = Number(p.price || 0);
        var lineTotal = unit * Number(row.qty || 0);
        var img = p.image_url
          ? '<img src="' + p.image_url + '" alt="" class="wsf-cart-line__img" />'
          : '<span class="wsf-cart-line__img wsf-cart-line__img--ph">' + (p.name || "?").charAt(0) + "</span>";
        return (
          '<article class="wsf-cart-line" data-cart-id="' +
          p.id +
          '">' +
          '<div class="wsf-cart-line__media">' +
          img +
          "</div>" +
          '<div class="wsf-cart-line__main">' +
          '<div class="wsf-cart-line__top">' +
          '<p class="wsf-cart-line__name">' +
          (p.name || "Product") +
          "</p>" +
          '<button type="button" class="wsf-cart-line__remove" data-remove-id="' +
          p.id +
          '" aria-label="Remove item">' +
          '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>' +
          "</button>" +
          "</div>" +
          '<p class="wsf-cart-line__unit">' +
          fmtMoney(unit) +
          " each</p>" +
          '<div class="wsf-cart-line__foot">' +
          '<div class="wsf-cart-line__qty">' +
          '<button type="button" class="wsf-qty-btn" data-qty-delta="-1" data-id="' +
          p.id +
          '" aria-label="Decrease quantity">' +
          '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M20 12H4"/></svg>' +
          "</button>" +
          '<span class="wsf-qty-val">' +
          row.qty +
          "</span>" +
          '<button type="button" class="wsf-qty-btn" data-qty-delta="1" data-id="' +
          p.id +
          '" aria-label="Increase quantity">' +
          '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4"/></svg>' +
          "</button>" +
          "</div>" +
          '<p class="wsf-cart-line__price">' +
          fmtMoney(lineTotal) +
          "</p>" +
          "</div>" +
          "</div>" +
          "</article>"
        );
      })
      .join("");

    if (totalEl) totalEl.textContent = fmtMoney(cartTotal());
    notifyCartUpdate();
  }

  function resolveAddAnimEl(el) {
    if (!el) return null;
    if (el.classList && el.classList.contains("wsf-product-card")) {
      return el.querySelector(".wsf-product-card__cart") || el;
    }
    return el;
  }

  function handleAddClick(e, el) {
    if (!el) return;
    e.preventDefault();
    addToCart(el.getAttribute("data-wsf-add-id"), 1, resolveAddAnimEl(el));
  }

  root.addEventListener("click", function (e) {
    var addTarget = e.target.closest(".wsf-product-card[data-wsf-add-id], [data-wsf-add-id]");
    if (addTarget) {
      handleAddClick(e, addTarget);
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
    if (e.key !== "Enter" && e.key !== " ") return;
    var card = e.target.closest(".wsf-product-card[data-wsf-add-id]");
    if (!card || !root.contains(card)) return;
    if (e.target.closest("a, input, textarea, select")) return;
    e.preventDefault();
    addToCart(card.getAttribute("data-wsf-add-id"), 1, resolveAddAnimEl(card));
  });

  function setLocationFieldOpen(open) {
    var wrap = document.getElementById("wsf-location-field-wrap");
    var checkbox = document.getElementById("wsf-add-location");
    var input = document.getElementById("wsf-customer-location");
    if (checkbox) checkbox.checked = !!open;
    if (wrap) wrap.classList.toggle("is-open", !!open);
    if (open) {
      document.dispatchEvent(new CustomEvent("wsf-location-opened"));
      window.requestAnimationFrame(function () {
        if (typeof window.wsfInitLocationAutocomplete === "function") {
          window.wsfInitLocationAutocomplete();
        }
        if (input) input.focus();
      });
    } else if (input) {
      input.value = "";
      if (typeof window.wsfClearLocationDistance === "function") window.wsfClearLocationDistance();
      var hint = document.getElementById("wsf-location-hint");
      if (hint) hint.textContent = "";
    }
  }

  window.wsfResetLocationField = function () {
    setLocationFieldOpen(false);
  };

  window.wsfLocationEnabled = function () {
    var checkbox = document.getElementById("wsf-add-location");
    return !!(checkbox && checkbox.checked);
  };

  var locationCheckbox = document.getElementById("wsf-add-location");
  if (locationCheckbox) {
    locationCheckbox.addEventListener("change", function () {
      setLocationFieldOpen(locationCheckbox.checked);
    });
  }

  function openWhatsappUrl(url) {
    if (!url) return false;
    var win = window.open(url, "_blank", "noopener,noreferrer");
    if (!win) {
      window.location.href = url;
    }
    return true;
  }

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
        submitBtn.querySelector("span").textContent = "Sending…";
      }

      var locationEnabled =
        typeof window.wsfLocationEnabled === "function" && window.wsfLocationEnabled();
      var locationValue =
        locationEnabled && form.customer_location ? (form.customer_location.value || "").trim() : "";
      if (locationEnabled && locationValue.length < 2) {
        if (errorEl) {
          errorEl.textContent = "Select a location from the list or uncheck Add location.";
          errorEl.classList.remove("hidden");
        }
        if (submitBtn) {
          submitBtn.disabled = false;
          submitBtn.querySelector("span").textContent = "Request quote";
        }
        return;
      }

      var distanceKm = null;
      var distanceEl = document.getElementById("wsf-customer-location-km");
      if (locationEnabled && distanceEl && distanceEl.value) {
        var parsed = parseFloat(distanceEl.value);
        if (isFinite(parsed) && parsed >= 0) distanceKm = parsed;
      }

      var payload = {
        customer_phone: (form.customer_phone && form.customer_phone.value) || "",
        customer_name: (form.customer_name && form.customer_name.value) || "",
        customer_location: locationValue,
        customer_location_distance_km: distanceKm,
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
          setLocationFieldOpen(false);
          if (nameEl) nameEl.removeAttribute("data-wsf-autofill");
          lastLookupPhone = "";
          setPhoneHint("", "");
          if (successEl) {
            successEl.textContent = res.body.message || "Quotation request received.";
            successEl.classList.remove("hidden");
          }
          var companyWa =
            (res.body && (res.body.company_whatsapp_url || res.body.whatsapp_url)) || "";
          if (companyWa) {
            openWhatsappUrl(companyWa);
          }
          if (typeof window.wsfShowToast === "function") {
            window.wsfShowToast(
              res.body.system_saved && companyWa
                ? "Saved to system — tap Send in WhatsApp for the company phone"
                : res.body.system_saved
                  ? "Quotation saved to our system"
                  : "Quotation request received"
            );
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
            submitBtn.querySelector("span").textContent = "Request quote";
          }
        });
    });
  }

  var lookupUrl = root.getAttribute("data-customer-lookup-url") || "";
  var phoneEl = document.getElementById("wsf-customer-phone");
  var nameEl = document.getElementById("wsf-customer-name");
  var phoneHintEl = document.getElementById("wsf-customer-phone-hint");
  var lookupTimer = null;
  var lookupInFlight = false;
  var lastLookupPhone = "";

  function normalizePhone(v) {
    return String(v || "").replace(/[^\d+]/g, "").trim();
  }

  function phoneDigits(v) {
    return normalizePhone(v).replace(/\D/g, "");
  }

  function setPhoneHint(message, tone) {
    if (!phoneHintEl) return;
    phoneHintEl.textContent = message || "";
    phoneHintEl.classList.remove("is-found", "is-error");
    if (tone === "found") phoneHintEl.classList.add("is-found");
    if (tone === "error") phoneHintEl.classList.add("is-error");
  }

  function runCustomerLookup() {
    if (!lookupUrl || !phoneEl) return;
    var phone = normalizePhone(phoneEl.value || "");
    phoneEl.value = phone;
    var digits = phoneDigits(phone);
    if (digits.length < 7) {
      lastLookupPhone = "";
      setPhoneHint(digits.length ? "Enter phone to lookup." : "", "");
      if (nameEl && nameEl.getAttribute("data-wsf-autofill") === "1") {
        nameEl.value = "";
        nameEl.removeAttribute("data-wsf-autofill");
      }
      return;
    }
    if (lookupInFlight || phone === lastLookupPhone) return;
    lookupInFlight = true;
    setPhoneHint("Checking customer…", "");
    fetch(lookupUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ phone: phone }),
    })
      .then(function (r) {
        return r.json().then(function (j) {
          if (!r.ok || !j.ok) throw new Error((j && j.error) || "Lookup failed");
          return j;
        });
      })
      .then(function (j) {
        lastLookupPhone = phone;
        var customer = j.customer || null;
        if (customer && customer.customer_name) {
          nameEl.value = customer.customer_name;
          nameEl.setAttribute("data-wsf-autofill", "1");
          if (customer.phone) phoneEl.value = customer.phone;
          setPhoneHint("Registered customer found and auto-filled.", "found");
        } else {
          if (nameEl && nameEl.getAttribute("data-wsf-autofill") === "1") {
            nameEl.value = "";
            nameEl.removeAttribute("data-wsf-autofill");
          }
          setPhoneHint("Customer not found. Enter your details to continue.", "");
        }
      })
      .catch(function (e) {
        setPhoneHint((e && e.message) || "Lookup failed.", "error");
      })
      .finally(function () {
        lookupInFlight = false;
      });
  }

  function scheduleCustomerLookup() {
    if (lookupTimer) clearTimeout(lookupTimer);
    lookupTimer = setTimeout(runCustomerLookup, 420);
  }

  if (phoneEl) {
    phoneEl.addEventListener("input", function () {
      if (phoneDigits(phoneEl.value).length < 7) lastLookupPhone = "";
      scheduleCustomerLookup();
    });
    phoneEl.addEventListener("blur", runCustomerLookup);
  }

  if (nameEl) {
    nameEl.addEventListener("input", function () {
      if (nameEl.getAttribute("data-wsf-autofill") === "1") {
        nameEl.removeAttribute("data-wsf-autofill");
      }
    });
  }

  loadCart();
  render();
})();
