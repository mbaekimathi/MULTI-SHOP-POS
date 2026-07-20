"""Build base-pos-shell.css and patch shop_pos.html to use external assets."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "templates" / "base.html"
POS = ROOT / "templates" / "shop_pos.html"
SHELL_CSS = ROOT / "static" / "css" / "base-pos-shell.css"

POS_ASSET_VER = "1"


def build_shell_css() -> None:
    base = BASE.read_text(encoding="utf-8")
    start = base.index("      .btn-rc {")
    end = base.index("      /* Public nav (partials/nav.html) */")
    btn_chunk = base[start:end]
    prefix = """/* POS shell: minimal portal base styles (buttons, typography). */
html {
  font-family: var(--rc-font-family, "Plus Jakarta Sans", system-ui, sans-serif);
}
body {
  min-height: 100vh;
  background-color: rgb(var(--rc-page-bg));
  color: rgb(var(--rc-page-fg));
  overflow-x: hidden;
  font-family: inherit;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
  line-height: 1.55;
}
button,
input,
select,
textarea,
optgroup {
  font-family: inherit;
}
html[data-theme="light"] body {
  background-color: rgb(var(--rc-page-bg));
  background-image: radial-gradient(ellipse 90% 60% at 10% -8%, rgba(var(--rc-primary) / 0.07), transparent 52%),
    radial-gradient(ellipse 70% 50% at 100% 0%, rgba(var(--rc-accent) / 0.05), transparent 48%);
  background-attachment: fixed;
}
html[data-theme="dark"] body {
  background-color: rgb(var(--rc-page-bg));
  background-image: radial-gradient(ellipse 100% 55% at 50% -15%, rgba(var(--rc-primary) / 0.08), transparent 58%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.03) 0%, transparent 32%);
  background-attachment: fixed;
}
::selection {
  background: rgba(var(--rc-primary) / 0.22);
  color: rgb(var(--rc-page-fg));
}
:focus-visible {
  outline: 2px solid rgba(var(--rc-primary) / 0.45);
  outline-offset: 2px;
}
input:not([type="checkbox"]):not([type="radio"]):not([type="color"]):not([type="range"]),
select,
textarea {
  box-shadow: none;
}
html[data-theme="light"] input:not([type="checkbox"]):not([type="radio"]):not([type="color"]),
html[data-theme="light"] select,
html[data-theme="light"] textarea {
  background-color: rgb(var(--rc-surface));
}
img,
video {
  max-width: 100%;
  height: auto;
}
p,
a,
li,
span {
  overflow-wrap: anywhere;
}

"""
    SHELL_CSS.write_text(prefix + btn_chunk, encoding="utf-8")
    print(f"Wrote {SHELL_CSS} ({len((prefix + btn_chunk).encode()) / 1024:.1f} KB)")


def patch_shop_pos() -> None:
    text = POS.read_text(encoding="utf-8")
    body_split = text.split("{% block body %}", 1)
    if len(body_split) != 2:
        raise SystemExit("Could not split shop_pos block body")
    body_rest = body_split[1]

    # Strip inline scripts at end of body (before {% endblock %})
    body_rest = re.sub(
        r"\n  <script>.*?\{% endif %\}\n\{% endblock %\}\s*$",
        "\n  {% include \"partials/shop_pos_scripts.html\" %}\n{% endblock %}\n",
        body_rest,
        count=1,
        flags=re.S,
    )
    if "{% include \"partials/shop_pos_scripts.html\" %}" not in body_rest:
        # Fallback: withhold block absent
        body_rest = re.sub(
            r"\n  <script>.*?\n\{% endblock %\}\s*$",
            "\n  {% include \"partials/shop_pos_scripts.html\" %}\n{% endblock %}\n",
            body_rest,
            count=1,
            flags=re.S,
        )

    new_head = f"""{{% extends "base_pos.html" %}}
{{% block title %}}Shop POS — {{{{ shop.shop_name }}}}{{% endblock %}}

{{% block head %}}
  <link rel="stylesheet" href="{{{{ url_for('static', filename='css/shop-pos-page.css') }}}}?v={POS_ASSET_VER}" />
  <link rel="stylesheet" href="{{{{ url_for('static', filename='css/shop-pos-pro.css') }}}}?v=7" />
{{% endblock %}}

{{% block body %}}"""

    POS.write_text(new_head + body_rest, encoding="utf-8")
    print(f"Patched {POS}")


if __name__ == "__main__":
    build_shell_css()
    patch_shop_pos()
