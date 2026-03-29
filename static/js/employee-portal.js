/**
 * Employee portal: mobile sidebar + footer year.
 */
(function () {
  const sidebar = document.getElementById("employee-sidebar");
  const openBtn = document.getElementById("employee-sidebar-open");
  const closeBtn = document.getElementById("employee-sidebar-close");
  const backdrop = document.getElementById("employee-sidebar-backdrop");

  function closeSidebar() {
    sidebar?.classList.add("-translate-x-full");
    backdrop?.classList.add("opacity-0", "pointer-events-none");
    backdrop?.classList.remove("opacity-100");
    openBtn?.setAttribute("aria-expanded", "false");
  }

  function openSidebar() {
    sidebar?.classList.remove("-translate-x-full");
    backdrop?.classList.remove("opacity-0", "pointer-events-none");
    backdrop?.classList.add("opacity-100");
    openBtn?.setAttribute("aria-expanded", "true");
  }

  openBtn?.addEventListener("click", () => {
    if (sidebar?.classList.contains("-translate-x-full")) openSidebar();
    else closeSidebar();
  });

  closeBtn?.addEventListener("click", closeSidebar);
  backdrop?.addEventListener("click", closeSidebar);

  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    if (!window.matchMedia("(max-width: 767px)").matches) return;
    if (sidebar?.classList.contains("-translate-x-full")) return;
    closeSidebar();
  });

  sidebar?.querySelectorAll("a").forEach((a) => {
    a.addEventListener("click", () => {
      if (window.matchMedia("(max-width: 767px)").matches) closeSidebar();
    });
  });

  const y = document.getElementById("portal-footer-year");
  if (y) y.textContent = String(new Date().getFullYear());
})();
