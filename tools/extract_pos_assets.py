"""Extract inline POS CSS/JS from shop_pos.html into static files (one-time maintainer script)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "templates" / "shop_pos.html"
CSS_OUT = ROOT / "static" / "css" / "shop-pos-page.css"
JS_DIR = ROOT / "static" / "js" / "pos"

SCRIPT_MARKERS = [
    ("pos-core.js", 0),
    ("pos-printer.js", 1),
    ("pos-incoming-stock.js", 2),
    ("pos-hold.js", 3),
]

BOOT_PREFIX_MAIN = """\
(function () {
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var KEY = "richcom-shop-pos-cart-" + SHOP_ID;
  if (typeof window.__POS_CHECKOUT_PATH === "undefined") {
    window.__POS_CHECKOUT_PATH = "direct";
  }
  var OFFLINE_SALES_KEY = "richcom-shop-pos-offline-sales-" + SHOP_ID;
  var OFFLINE_CATALOG_KEY = "richcom-shop-pos-catalog-snapshot-" + SHOP_ID;
  var OFFLINE_STOCKIN_KEY = "richcom-shop-pos-offline-stockins-" + SHOP_ID;
  var OFFLINE_REFILL_KEY = "richcom-shop-pos-offline-refills-" + SHOP_ID;
  var OFFLINE_DB_NAME = "richcom-pos-offline-db-v2";
  var OFFLINE_DB_VERSION = 4;
  var OFFLINE_DB_STORE = "offline_sales";
  var OFFLINE_DB_CATALOG_STORE = "catalog_snapshot";
  var OFFLINE_DB_STOCKIN_STORE = "offline_stockins";
  var OFFLINE_DB_REFILL_STORE = "offline_portion_refills";
  var OFFLINE_CATALOG_DOC_ID = "shop-" + SHOP_ID;
  var OFFLINE_STOCK_IN_API = BOOT.apis.stockIn;
  var OFFLINE_REFILL_PORTIONS_API = BOOT.apis.refillPortions || "";
  var OFFLINE_CATALOG_STALE_MS = 6 * 60 * 60 * 1000;
  var AUTH_API = BOOT.apis.authorizeEmployee;
  var EMPLOYEE_CACHE_API = BOOT.apis.employeeAuthCache;
  var EMPLOYEE_AUTH_CACHE_KEY = "richcom-shop-pos-employee-auth-cache-" + SHOP_ID;
  var CUSTOMER_LOOKUP_API = BOOT.apis.customerLookup;
  var CUSTOMER_UPSERT_API = BOOT.apis.customerUpsert;
  var RECORD_SALE_API = BOOT.apis.recordSale;
  var RECORD_QUOTE_API = BOOT.apis.recordQuote;
  var SELLER_LOOKUP_API = BOOT.apis.sellerLookup;
  var CATALOG_STOCK_API = BOOT.apis.catalogStock;
  var RECEIPTS_LIST_API = BOOT.apis.receiptsList;
  var RECEIPTS_MARK_API = BOOT.apis.receiptsMark;
  var RECEIPTS_DETAIL_API = BOOT.apis.receiptsDetail;
  var RECEIPTS_RETURN_LINES_API = BOOT.apis.receiptsReturnLines;
  var POS_INVENTORY_MODE = BOOT.inventoryMode;
  window.POS_INVENTORY_MODE = POS_INVENTORY_MODE;
  var PRINTER_API = BOOT.apis.printer;
  var QR_ESC_POS_API = BOOT.apis.printerQrEscpos;
  var NETWORK_ESC_POS_API = BOOT.apis.printerSendEscpos;
  var PRINTER_LS_KEY = "richcom-shop-pos-printer-local-" + SHOP_ID;
  window.POS_PRINTING = BOOT.printing;
  (function () {
    var p = window.POS_PRINTING;
    if (!p || typeof p !== "object") return;
    function triPrinterAllow(v) {
      if (v === true || v === 1 || v === "1") return true;
      if (typeof v === "string") {
        var s = v.trim().toLowerCase();
        if (s === "true" || s === "1" || s === "yes" || s === "on") return true;
      }
      return false;
    }
    p.printer_allow_bluetooth = triPrinterAllow(p.printer_allow_bluetooth);
    p.printer_allow_network = triPrinterAllow(p.printer_allow_network);
    p.printer_allow_usb = triPrinterAllow(p.printer_allow_usb);
    window.posPrinterTypeAllowed = function (pp, type) {
      pp = pp || window.POS_PRINTING || {};
      type = String(type || "").toLowerCase();
      if (type === "bluetooth") return pp.printer_allow_bluetooth === true;
      if (type === "network") return pp.printer_allow_network === true;
      if (type === "usb") return pp.printer_allow_usb === true;
      return false;
    };
    function triBoolSetting(v) {
      if (v === true || v === 1 || v === "1") return true;
      if (typeof v === "string") {
        var s = v.trim().toLowerCase();
        if (s === "true" || s === "1" || s === "yes" || s === "on") return true;
      }
      return false;
    }
    p.print_compulsory_sale = triBoolSetting(p.print_compulsory_sale);
  })();
  window.POS_RECEIPT_SETTINGS = BOOT.receiptSettings;
  window.POS_SITE = BOOT.site;
"""

BOOT_PREFIX_PRINTER = """\
(function () {
  window.POS_RECEIPT_PRINTER_CONFIGURED = false;
  try {
    if (window.__updatePosCompulsoryPrinterWorkspaceLock) window.__updatePosCompulsoryPrinterWorkspaceLock();
  } catch (eLock0) {}
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var PRINTER_API = BOOT.apis.printer;
  var SCAN_API = BOOT.apis.printerScanNetwork;
  var SCAN_STREAM_API = BOOT.apis.printerScanNetworkStream;
  var PRINT_AGENT_STATUS_API = BOOT.apis.printAgentStatus;
  var PRINT_AGENT_CONFIGURE_API = BOOT.apis.printAgentConfigure;
  var TCP_REACHABLE_API = BOOT.apis.printerTcpReachable;
  var LS_KEY = "richcom-shop-pos-printer-local-" + SHOP_ID;
"""

BOOT_PREFIX_INCOMING = """\
(function () {
  var BOOT = window.__POS_BOOT || {};
  var POS_SID = BOOT.shopId;
  var API = BOOT.apis.incomingStockRequests;
  var LS_KEY = "pos_incoming_sr_sig_shop_" + POS_SID;
"""

BOOT_PREFIX_HOLD = """\
(function () {
  var BOOT = window.__POS_BOOT || {};
  var SHOP_ID = BOOT.shopId;
  var HOLD_SAVE_API = BOOT.apis.holdSave;
  var HOLD_LIST_API = BOOT.apis.holdList;
  var HOLD_GET_BASE = BOOT.apis.holdGetBase;
  var CART_LS_KEY = "richcom-shop-pos-cart-" + SHOP_ID;
  var HOLD_STATE_KEY = "richcom-shop-pos-hold-" + SHOP_ID;
  var CHECKOUT_PATH_KEY = "richcom-shop-pos-checkout-path-" + SHOP_ID;
"""


def extract_css(text: str) -> str:
    m = re.search(r"{% block head %}\s*<style>(.*?)</style>", text, re.S)
    if not m:
        raise SystemExit("Could not find POS head <style> block")
    return m.group(1).strip() + "\n"


def extract_script_blocks(text: str) -> list[str]:
    body = text.split("{% block body %}", 1)[1]
    blocks = re.findall(r"<script>\s*(.*?)\s*</script>", body, re.S)
    # Last block may be inside {% if withhold %} — still captured by regex
    hold_blocks = [b for b in blocks if "HOLD_SAVE_API" in b or "pos-hold-modal" in b]
    core_blocks = [b for b in blocks if b not in hold_blocks]
    if len(core_blocks) < 3:
        raise SystemExit(f"Expected at least 3 core script blocks, found {len(core_blocks)}")
    ordered = core_blocks[:3]
    if hold_blocks:
        ordered.append(hold_blocks[0])
    return ordered


def strip_main_preamble(src: str) -> str:
    """Remove Jinja-backed variable declarations from start of main POS script."""
    marker = "var saleType = "
    idx = src.find(marker)
    if idx < 0:
        raise SystemExit("Could not find saleType marker in main script")
    return src[idx:]


def strip_printer_preamble(src: str) -> str:
    marker = "var dlg = document.getElementById"
    idx = src.find(marker)
    if idx < 0:
        raise SystemExit("Could not find printer dlg marker")
    return src[idx:]


def strip_incoming_preamble(src: str) -> str:
    marker = "var modal = document.getElementById"
    idx = src.find(marker)
    if idx < 0:
        raise SystemExit("Could not find incoming modal marker")
    return src[idx:]


def strip_hold_preamble(src: str) -> str:
    marker = "function loadCheckoutPathPreference"
    idx = src.find(marker)
    if idx < 0:
        raise SystemExit("Could not find hold checkout marker")
    return src[idx:]


def main() -> None:
    text = TEMPLATE.read_text(encoding="utf-8")
    css = extract_css(text)
    CSS_OUT.parent.mkdir(parents=True, exist_ok=True)
    CSS_OUT.write_text(css, encoding="utf-8")
    print(f"Wrote {CSS_OUT} ({len(css.encode()) / 1024:.1f} KB)")

    blocks = extract_script_blocks(text)
    JS_DIR.mkdir(parents=True, exist_ok=True)

    processors = [
        (BOOT_PREFIX_MAIN, strip_main_preamble),
        (BOOT_PREFIX_PRINTER, strip_printer_preamble),
        (BOOT_PREFIX_INCOMING, strip_incoming_preamble),
        (BOOT_PREFIX_HOLD, strip_hold_preamble),
    ]

    for (filename, _), (prefix, strip_fn), block in zip(SCRIPT_MARKERS, processors, blocks):
        body = strip_fn(block)
        out = prefix + body
        if not out.rstrip().endswith("})();"):
            if not out.rstrip().endswith("})();"):
                pass
        path = JS_DIR / filename
        path.write_text(out, encoding="utf-8")
        print(f"Wrote {path} ({len(out.encode()) / 1024:.1f} KB)")

    print("Done.")


if __name__ == "__main__":
    main()
