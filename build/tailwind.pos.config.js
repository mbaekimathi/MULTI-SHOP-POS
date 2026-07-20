/** Tailwind build for Shop POS only (replaces cdn.tailwindcss.com on base_pos.html). */
module.exports = {
  content: [
    "../templates/base_pos.html",
    "../templates/shop_pos.html",
    "../templates/partials/theme_toggle.html",
    "../templates/partials/notification_bell.html",
    "../templates/partials/shop_session_header_menu.html",
    "../templates/partials/employee_portal_header_user.html",
    "../templates/partials/flashes.html",
    "../static/js/pos/**/*.js",
  ],
  darkMode: ["selector", '[data-theme="dark"]'],
  theme: {
    extend: {
      fontFamily: {
        sans: ["var(--rc-font-family)"],
      },
      colors: {
        brand: {
          50: "#fff7ed",
          100: "#ffedd5",
          200: "#fed7aa",
          300: "#fdba74",
          400: "#fb923c",
          500: "#f97316",
          600: "#ea580c",
          700: "#c2410c",
          800: "#9a3412",
          900: "#7c2d12",
        },
      },
    },
  },
  plugins: [],
};
