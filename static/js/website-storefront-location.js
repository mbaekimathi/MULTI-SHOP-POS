(function () {
  "use strict";

  var KENYA_CENTER = { lat: -1.2921, lng: 36.8219 };
  var mapsLoadPromise = null;
  var selectedDistanceKm = null;

  function loadGoogleMaps(apiKey) {
    if (!apiKey) {
      return Promise.reject(new Error("Missing Google Maps API key"));
    }
    if (window.google && window.google.maps && window.google.maps.places) {
      return Promise.resolve(window.google.maps);
    }
    if (mapsLoadPromise) return mapsLoadPromise;

    mapsLoadPromise = new Promise(function (resolve, reject) {
      var existing = document.querySelector('script[data-wsf-google-maps="1"]');
      if (existing) {
        existing.addEventListener("load", function () {
          if (window.google && window.google.maps) resolve(window.google.maps);
          else reject(new Error("Google Maps failed to load"));
        });
        existing.addEventListener("error", function () {
          reject(new Error("Google Maps failed to load"));
        });
        return;
      }

      window.__wsfGoogleMapsReady = function () {
        if (window.google && window.google.maps) resolve(window.google.maps);
        else reject(new Error("Google Maps failed to load"));
      };

      var script = document.createElement("script");
      script.src =
        "https://maps.googleapis.com/maps/api/js?key=" +
        encodeURIComponent(apiKey) +
        "&libraries=places&callback=__wsfGoogleMapsReady";
      script.async = true;
      script.defer = true;
      script.dataset.wsfGoogleMaps = "1";
      script.onerror = function () {
        mapsLoadPromise = null;
        reject(new Error("Google Maps failed to load"));
      };
      document.head.appendChild(script);
    });

    return mapsLoadPromise;
  }

  function setHint(el, message) {
    if (!el) return;
    el.textContent = message || "";
  }

  function shopCoords(root) {
    if (!root) return null;
    var lat = parseFloat(root.getAttribute("data-shop-lat") || "");
    var lng = parseFloat(root.getAttribute("data-shop-lng") || "");
    if (!isFinite(lat) || !isFinite(lng)) return null;
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return null;
    return { lat: lat, lng: lng };
  }

  function haversineKm(from, to) {
    var R = 6371;
    var dLat = ((to.lat - from.lat) * Math.PI) / 180;
    var dLng = ((to.lng - from.lng) * Math.PI) / 180;
    var a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos((from.lat * Math.PI) / 180) *
        Math.cos((to.lat * Math.PI) / 180) *
        Math.sin(dLng / 2) *
        Math.sin(dLng / 2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }

  function formatKm(km) {
    if (!isFinite(km) || km < 0) return "";
    if (km < 10) return km.toFixed(1);
    return String(Math.round(km));
  }

  function setDistanceKm(km) {
    selectedDistanceKm = isFinite(km) && km >= 0 ? km : null;
    var hidden = document.getElementById("wsf-customer-location-km");
    var badge = document.getElementById("wsf-location-distance");
    if (hidden) hidden.value = selectedDistanceKm != null ? String(selectedDistanceKm) : "";
    if (!badge) return;
    if (selectedDistanceKm == null) {
      badge.textContent = "";
      badge.classList.add("hidden");
      return;
    }
    badge.textContent = "Approx. " + formatKm(selectedDistanceKm) + " km from shop";
    badge.classList.remove("hidden");
  }

  window.wsfClearLocationDistance = function () {
    setDistanceKm(null);
  };

  window.wsfGetLocationDistanceKm = function () {
    return selectedDistanceKm;
  };

  function kenyaBounds(maps) {
    return new maps.LatLngBounds(
      new maps.LatLng(KENYA_CENTER.lat - 4.5, KENYA_CENTER.lng - 4.5),
      new maps.LatLng(KENYA_CENTER.lat + 4.5, KENYA_CENTER.lng + 4.5)
    );
  }

  function updateDistanceForPlace(root, place, hint) {
    var shop = shopCoords(root);
    if (!shop || !place || !place.geometry || !place.geometry.location) {
      setDistanceKm(null);
      setHint(hint, "Location selected.");
      return;
    }
    var dest = place.geometry.location;
    var km = haversineKm(shop, { lat: dest.lat(), lng: dest.lng() });
    setDistanceKm(km);
    setHint(hint, "Location selected — distance calculated from shop.");
  }

  function bindAutocomplete(root, input, hint, maps) {
    if (input.getAttribute("data-wsf-places-ready") === "1") return;

    var autocomplete = new maps.places.Autocomplete(input, {
      fields: ["formatted_address", "geometry", "name"],
      componentRestrictions: { country: "ke" },
      types: ["geocode", "establishment"],
    });
    autocomplete.setBounds(kenyaBounds(maps));

    autocomplete.addListener("place_changed", function () {
      var place = autocomplete.getPlace();
      if (!place) return;
      var label = (place.formatted_address || place.name || "").trim();
      if (label) input.value = label;
      updateDistanceForPlace(root, place, hint);
    });

    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter") e.preventDefault();
    });

    input.addEventListener("input", function () {
      if (!input.value.trim()) {
        setDistanceKm(null);
        setHint(hint, "Start typing to search on Google Maps.");
      }
    });

    input.setAttribute("data-wsf-places-ready", "1");
    var shop = shopCoords(root);
    if (shop) {
      setHint(hint, "Start typing to search on Google Maps.");
    } else {
      setHint(hint, "Start typing to search. Set shop location in settings to show distance.");
    }
  }

  window.wsfInitLocationAutocomplete = function () {
    var root = document.getElementById("wsf-root");
    var input = document.getElementById("wsf-customer-location");
    var hint = document.getElementById("wsf-location-hint");
    var wrap = document.getElementById("wsf-location-field-wrap");
    if (!root || !input || !wrap || !wrap.classList.contains("is-open")) return;

    var apiKey = (root.getAttribute("data-google-maps-key") || "").trim();
    if (!apiKey) {
      setHint(hint, "Enter your area manually.");
      return;
    }

    if (input.getAttribute("data-wsf-places-ready") === "1") {
      var shop = shopCoords(root);
      setHint(
        hint,
        shop
          ? "Start typing to search on Google Maps."
          : "Start typing to search. Set shop location in settings to show distance."
      );
      return;
    }

    setHint(hint, "Loading maps…");
    loadGoogleMaps(apiKey)
      .then(function (maps) {
        window.requestAnimationFrame(function () {
          bindAutocomplete(root, input, hint, maps);
        });
      })
      .catch(function () {
        setHint(hint, "Maps unavailable — enter your area manually.");
      });
  };

  function initStorefrontLocation() {
    var root = document.getElementById("wsf-root");
    if (!root) return;
    var apiKey = (root.getAttribute("data-google-maps-key") || "").trim();
    if (apiKey) loadGoogleMaps(apiKey).catch(function () {});
    document.addEventListener("wsf-location-opened", function () {
      window.requestAnimationFrame(function () {
        window.wsfInitLocationAutocomplete();
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initStorefrontLocation);
  } else {
    initStorefrontLocation();
  }
})();
