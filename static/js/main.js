/**
 * Scroll reveal + smooth anchor + contact form AJAX.
 */
(function () {
  const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  document.querySelectorAll('a[href^="#"]').forEach((a) => {
    a.addEventListener("click", (e) => {
      const id = a.getAttribute("href");
      if (id.length > 1) {
        const el = document.querySelector(id);
        if (el) {
          e.preventDefault();
          el.scrollIntoView({ behavior: prefersReduced ? "auto" : "smooth", block: "start" });
        }
      }
    });
  });

  if (!prefersReduced) {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("reveal-in");
            observer.unobserve(entry.target);
          }
        });
      },
      { rootMargin: "0px 0px -8% 0px", threshold: 0.08 }
    );

    document.querySelectorAll(".reveal").forEach((el) => observer.observe(el));
  } else {
    document.querySelectorAll(".reveal").forEach((el) => el.classList.add("reveal-in"));
  }

  const form = document.getElementById("contact-form");
  const statusEl = document.getElementById("contact-status");
  if (form && statusEl) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      statusEl.textContent = "Sending…";
      statusEl.className = "mt-3 text-sm text-slate-600";

      const fd = new FormData(form);
      try {
        const res = await fetch(form.action, {
          method: "POST",
          body: fd,
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data.ok) {
          statusEl.textContent = data.message || "Sent.";
          statusEl.className = "mt-3 text-sm font-medium text-emerald-700";
          form.reset();
        } else {
          statusEl.textContent = data.error || "Something went wrong.";
          statusEl.className = "mt-3 text-sm font-medium text-red-700";
        }
      } catch {
        statusEl.textContent = "Network error. Try again.";
        statusEl.className = "mt-3 text-sm font-medium text-red-700";
      }
    });
  }

  const y = document.getElementById("year");
  if (y) y.textContent = new Date().getFullYear();
})();
