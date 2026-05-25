/**
 * Global PWA + POS service worker.
 *
 * Scope: "/" (served from /pos-sw.js with Service-Worker-Allowed: /).
 *
 * Strategy summary:
 *  - POS pages (/shops/{id}/shop-pos)         : network-first navigation, cache fallback
 *  - POS JSON  (/shops/{id}/shop-pos/*.json)  : network-first, cache fallback
 *  - Static assets (/static/*)                : stale-while-revalidate
 *  - App shell roots (/, /home, /manifest)    : network-first, cache fallback
 *  - Other navigations                        : network-first, fall back to /offline
 *  - All other GETs                           : pass-through
 *
 * IMPORTANT: HTML navigations should NEVER be served from a stale cache when
 * online (auth, flash messages, csrf). We only fall back to cache when the
 * network attempt itself fails (true offline).
 */

const SW_VERSION = "pwa-sw-v4";
const APP_SHELL_CACHE = `app-shell-${SW_VERSION}`;
const RUNTIME_CACHE = `runtime-${SW_VERSION}`;
const STATIC_CACHE = `static-${SW_VERSION}`;

const APP_SHELL_URLS = [
  "/",
  "/offline",
  "/manifest.webmanifest",
  "/static/app-icon.svg",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      try {
        const cache = await caches.open(APP_SHELL_CACHE);
        await Promise.all(
          APP_SHELL_URLS.map((u) =>
            cache.add(new Request(u, { cache: "reload" })).catch(() => null)
          )
        );
      } catch (e) {
        /* ignore install precache errors */
      }
      self.skipWaiting();
    })()
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      try {
        const keys = await caches.keys();
        const allowed = new Set([APP_SHELL_CACHE, RUNTIME_CACHE, STATIC_CACHE]);
        await Promise.all(
          keys.map((k) => (allowed.has(k) ? Promise.resolve() : caches.delete(k)))
        );
        if ("navigationPreload" in self.registration) {
          try {
            await self.registration.navigationPreload.enable();
          } catch (e) {
            /* not supported */
          }
        }
      } catch (e) {
        /* ignore activate cleanup errors */
      }
      await self.clients.claim();
    })()
  );
});

self.addEventListener("message", (event) => {
  if (!event || !event.data) return;
  if (event.data === "SKIP_WAITING" || event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

function isShopPosPage(url, req) {
  return req.mode === "navigate" && /^\/shops\/\d+\/shop-pos\b/.test(url.pathname);
}
function isShopPosDynamicJson(url) {
  return /^\/shops\/\d+\/shop-pos\/[^?]+\.json$/.test(url.pathname);
}
function isStaticAsset(url) {
  return url.pathname.startsWith("/static/");
}
function isManifest(url) {
  return url.pathname === "/manifest.webmanifest";
}
function isAppShellRoot(url) {
  return url.pathname === "/" || url.pathname === "/offline";
}

async function networkFirstWithCache(req, cacheName, fallbackReq) {
  const cache = await caches.open(cacheName);
  try {
    const fresh = await fetch(req);
    if (fresh && (fresh.ok || fresh.type === "opaqueredirect")) {
      try {
        await cache.put(req, fresh.clone());
      } catch (e) {
        /* ignore cache write errors (opaque, etc.) */
      }
    }
    return fresh;
  } catch (err) {
    const cached = await cache.match(req);
    if (cached) return cached;
    if (fallbackReq) {
      const fb = await caches.match(fallbackReq);
      if (fb) return fb;
    }
    throw err;
  }
}

async function staleWhileRevalidate(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  const network = fetch(req)
    .then((res) => {
      if (res && res.ok) {
        try {
          cache.put(req, res.clone());
        } catch (e) {
          /* ignore */
        }
      }
      return res;
    })
    .catch(() => null);
  return cached || (await network) || Promise.reject(new Error("offline"));
}

async function handleNavigation(event, url) {
  try {
    const preload = "preloadResponse" in event ? await event.preloadResponse : null;
    if (preload) {
      const cache = await caches.open(RUNTIME_CACHE);
      try {
        cache.put(event.request, preload.clone());
      } catch (e) {
        /* ignore */
      }
      return preload;
    }
    const fresh = await fetch(event.request);
    if (fresh && fresh.ok) {
      const cache = await caches.open(RUNTIME_CACHE);
      try {
        cache.put(event.request, fresh.clone());
      } catch (e) {
        /* ignore */
      }
    }
    return fresh;
  } catch (err) {
    const cached = await caches.match(event.request);
    if (cached) return cached;
    if (isShopPosPage(url, event.request)) {
      const root = await caches.match("/");
      if (root) return root;
    }
    const offline = await caches.match("/offline");
    if (offline) return offline;
    return new Response(
      "<h1>You're offline</h1><p>Please reconnect and try again.</p>",
      { status: 503, headers: { "Content-Type": "text/html; charset=utf-8" } }
    );
  }
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  if (req.mode === "navigate") {
    event.respondWith(handleNavigation(event, url));
    return;
  }

  if (isShopPosDynamicJson(url)) {
    event.respondWith(networkFirstWithCache(req, RUNTIME_CACHE));
    return;
  }

  if (isManifest(url)) {
    event.respondWith(networkFirstWithCache(req, APP_SHELL_CACHE));
    return;
  }

  if (isStaticAsset(url)) {
    event.respondWith(staleWhileRevalidate(req, STATIC_CACHE));
    return;
  }

  if (isAppShellRoot(url)) {
    event.respondWith(networkFirstWithCache(req, APP_SHELL_CACHE));
  }
});
