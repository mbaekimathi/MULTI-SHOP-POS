"""One-off POS page load profiler (server-side HTML generation)."""
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import app  # noqa: E402

SHOP_ID = 2


def analyze_html(label: str, html: str) -> None:
    scripts = re.findall(r"<script[^>]*>.*?</script>", html, re.S)
    styles = re.findall(r"<style[^>]*>.*?</style>", html, re.S)
    script_kb = sum(len(s.encode("utf-8")) for s in scripts) / 1024
    style_kb = sum(len(s.encode("utf-8")) for s in styles) / 1024
    head_m = re.search(r"<head[^>]*>(.*)</head>", html, re.S)
    body_m = re.search(r"<body[^>]*>(.*)</body>", html, re.S)
    head_kb = len(head_m.group(1).encode("utf-8")) / 1024 if head_m else 0
    body_kb = len(body_m.group(1).encode("utf-8")) / 1024 if body_m else 0
    item_cards = len(re.findall(r"pos-item-card pos-item-card--media", html))
    data_item_ids = len(re.findall(r'data-item-id="', html))
    catalog_m = re.search(r'id="pos-catalog"(.*?)(?=<div id="pos-cart-drawer")', html, re.S)
    catalog_kb = len(catalog_m.group(0).encode("utf-8")) / 1024 if catalog_m else 0

    print(f"\n=== {label} ===")
    print(f"  total_kb: {len(html.encode('utf-8')) / 1024:.1f}")
    print(f"  head_kb: {head_kb:.1f}")
    print(f"  body_kb: {body_kb:.1f}")
    print(f"  catalog_kb: {catalog_kb:.1f}")
    print(f"  inline_script_kb: {script_kb:.1f} ({len(scripts)} blocks)")
    print(f"  inline_style_kb: {style_kb:.1f} ({len(styles)} blocks)")
    print(f"  pos item cards: {item_cards}")
    print(f"  data-item-id attrs: {data_item_ids}")
    print(f"  tailwind_cdn: {'cdn.tailwindcss.com' in html}")
    print(f"  google_fonts_links: {len(re.findall(r'fonts.googleapis.com', html))}")
    if scripts:
        sizes = sorted(((len(s.encode("utf-8")), i) for i, s in enumerate(scripts)), reverse=True)
        print(f"  largest_script_kb: {sizes[0][0] / 1024:.1f} (block #{sizes[0][1]})")


def main() -> None:
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["shop_id"] = SHOP_ID
            sess["shop_name"] = "Test Shop"

        runs = []
        for _ in range(3):
            t0 = time.perf_counter()
            resp = client.get(f"/shops/{SHOP_ID}/shop-pos")
            runs.append((time.perf_counter() - t0) * 1000)
        html = resp.get_data(as_text=True)
        analyze_html("full_request", html)
        print(f"\n=== timings (3 runs) ===")
        print(f"  http_ms: {[round(x, 1) for x in runs]}")
        print(f"  avg_ms: {sum(runs) / len(runs):.1f}")
        print(f"  status: {resp.status_code}")


if __name__ == "__main__":
    main()
