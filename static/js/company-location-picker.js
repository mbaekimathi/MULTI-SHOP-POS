(function () {
  "use strict";

  var DEFAULT_CENTER = { lat: -1.2921, lng: 36.8219 };
  var mapsLoadPromise = null;
  var MAPS_CALLBACK = "__companyLocationMapsReady";

  function parseCoord(raw, min, max) {
    var n = parseFloat(String(raw || "").trim());
    if (!isFinite(n) || n < min || n > max) return null;
    return n;
  }

  function mapsReady() {
    return !!(window.google && window.google.maps && window.google.maps.places);
  }

  function loadGoogleMaps(apiKey) {
    if (mapsReady()) {
      return Promise.resolve(window.google.maps);
    }
    if (mapsLoadPromise) {
      return mapsLoadPromise;
    }

    mapsLoadPromise = new Promise(function (resolve, reject) {
      var settled = false;
      function finish(err, maps) {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (err) {
          mapsLoadPromise = null;
          reject(err);
          return;
        }
        resolve(maps);
      }

      var timer = setTimeout(function () {
        finish(
          new Error(
            "Google Maps timed out. If the key works in Cloud Console, add http://127.0.0.1:5000/* and http://localhost:5000/* to key HTTP referrer restrictions."
          )
        );
      }, 20000);

      window[MAPS_CALLBACK] = function () {
        try {
          delete window[MAPS_CALLBACK];
        } catch (e1) {
          window[MAPS_CALLBACK] = undefined;
        }
        if (mapsReady()) {
          finish(null, window.google.maps);
        } else {
          finish(new Error("Google Maps loaded but Places library is unavailable. Enable Places API in Google Cloud."));
        }
      };

      var script = document.createElement("script");
      script.src =
        "https://maps.googleapis.com/maps/api/js?key=" +
        encodeURIComponent(apiKey) +
        "&libraries=places&callback=" +
        MAPS_CALLBACK;
      script.async = true;
      script.defer = true;
      script.onerror = function () {
        finish(new Error("Could not download Google Maps. Check network and API key."));
      };
      document.head.appendChild(script);
    });

    return mapsLoadPromise;
  }

  function showMapError(mapEl, message) {
    mapEl.innerHTML =
      '<div class="flex h-full flex-col items-center justify-center gap-2 px-4 text-center text-sm text-[rgb(var(--rc-muted))]">' +
      "<p>" +
      String(message || "Could not load Google Maps.") +
      "</p>" +
      '<p class="text-xs">Enable <strong>Maps JavaScript API</strong> and <strong>Places API</strong>. For local dev, allow referrers <code class="rounded bg-black/10 px-1">http://127.0.0.1:5000/*</code> and <code class="rounded bg-black/10 px-1">http://localhost:5000/*</code>.</p>' +
      "</div>";
  }

  function initPicker(root, apiKey) {
    var searchInput = root.querySelector(".company-location-search");
    var mapEl = root.querySelector(".company-location-map");
    var nameInput = root.querySelector(".company-location-name");
    var latInput = root.querySelector(".company-location-lat");
    var lngInput = root.querySelector(".company-location-lng");
    var clearBtn = root.querySelector(".company-location-clear");
    if (!searchInput || !mapEl || !nameInput || !latInput || !lngInput) return;

    loadGoogleMaps(apiKey)
      .then(function (maps) {
        var lat = parseCoord(latInput.value, -90, 90);
        var lng = parseCoord(lngInput.value, -180, 180);
        var center = lat != null && lng != null ? { lat: lat, lng: lng } : DEFAULT_CENTER;
        var hasPoint = lat != null && lng != null;

        var map = new maps.Map(mapEl, {
          center: center,
          zoom: hasPoint ? 16 : 6,
          mapTypeControl: false,
          streetViewControl: false,
          fullscreenControl: false,
        });

        var marker = new maps.Marker({
          map: hasPoint ? map : null,
          position: hasPoint ? center : null,
          draggable: true,
        });

        function setLocation(nextLat, nextLng, label) {
          latInput.value = String(nextLat);
          lngInput.value = String(nextLng);
          nameInput.value = label || "";
          searchInput.value = label || "";
          var pos = { lat: nextLat, lng: nextLng };
          marker.setPosition(pos);
          marker.setMap(map);
          map.panTo(pos);
          map.setZoom(16);
          root.dispatchEvent(new Event("input", { bubbles: true }));
          root.dispatchEvent(new Event("change", { bubbles: true }));
        }

        function clearLocation() {
          latInput.value = "";
          lngInput.value = "";
          nameInput.value = "";
          searchInput.value = "";
          marker.setMap(null);
          map.setCenter(DEFAULT_CENTER);
          map.setZoom(6);
          root.dispatchEvent(new Event("input", { bubbles: true }));
          root.dispatchEvent(new Event("change", { bubbles: true }));
        }

        var autocomplete = new maps.places.Autocomplete(searchInput, {
          fields: ["formatted_address", "geometry", "name"],
        });
        autocomplete.bindTo("bounds", map);

        autocomplete.addListener("place_changed", function () {
          var place = autocomplete.getPlace();
          if (!place || !place.geometry || !place.geometry.location) return;
          var label = (place.formatted_address || place.name || "").trim();
          setLocation(place.geometry.location.lat(), place.geometry.location.lng(), label);
        });

        marker.addListener("dragend", function () {
          var pos = marker.getPosition();
          if (!pos) return;
          setLocation(pos.lat(), pos.lng(), nameInput.value.trim());
        });

        if (clearBtn) {
          clearBtn.addEventListener("click", clearLocation);
        }
      })
      .catch(function (err) {
        showMapError(mapEl, err && err.message ? err.message : "Could not load Google Maps.");
      });
  }

  window.initCompanyLocationPickers = function (apiKey) {
    if (!apiKey) return;
    document.querySelectorAll("[data-company-location-picker]").forEach(function (root) {
      initPicker(root, apiKey);
    });
  };
})();
