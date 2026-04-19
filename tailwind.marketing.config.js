/**
 * Reference theme (CLI builds). Runtime config lives in templates/marketing/base_marketing.html
 */
module.exports = {
  content: ["./templates/marketing/**/*.html"],
  darkMode: ["selector", '[data-marketing-theme="dark"]'],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        display: ["Lexend", "system-ui", "sans-serif"],
      },
      fontSize: {
        display: ["2.75rem", { lineHeight: "1.1", letterSpacing: "-0.03em", fontWeight: "700" }],
        "display-sm": ["2rem", { lineHeight: "1.15", letterSpacing: "-0.02em", fontWeight: "700" }],
      },
      colors: {
        pos: {
          50: "#faf5ff",
          100: "#f3e8ff",
          200: "#e9d5ff",
          300: "#d8b4fe",
          400: "#c084fc",
          500: "#a855f7",
          600: "#9333ea",
          700: "#7e22ce",
          800: "#6b21a8",
          900: "#581c87",
          950: "#3b0764",
        },
      },
      boxShadow: {
        soft: "0 1px 3px rgba(15, 23, 42, 0.06), 0 1px 2px rgba(15, 23, 42, 0.04)",
        card: "0 4px 24px rgba(15, 23, 42, 0.06)",
      },
    },
  },
  plugins: [],
};
