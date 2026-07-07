import re
from pathlib import Path

src = Path("static/js/pos/pos-core.js").read_text(encoding="utf-8")


def extract_func(name: str, end_marker: str | None = None) -> str:
    pat = rf"function {re.escape(name)}\([^)]*\) \{{"
    m = re.search(pat, src)
    if not m:
        raise SystemExit(f"missing {name}")
    start = m.start()
    if end_marker:
        em = src.find(end_marker, m.end())
        if em < 0:
            raise SystemExit(f"end marker for {name}")
        return src[start:em].rstrip()
    i = m.end() - 1
    depth = 0
    while i < len(src):
        c = src[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return src[start : i + 1]
        i += 1
    raise SystemExit(f"no end for {name}")


funcs = [
    "fmt",
    "fmtQty",
    "round2",
    "receiptEsc",
    "normalizeReceiptWidthKey",
    "receiptLineWidthChars",
    "receiptRollWidthMm",
    "receiptThermalFontCss",
    "computePosTax",
    "buildPersistedReceiptPayload",
    "buildReceiptQrPayloadDetailsJson",
    "buildReceiptQrPayloadString",
    "receiptQrEnabled",
    "waitForIframeImages",
    "thermalTransferItemHeaderLine",
    "thermalTransferItemLines",
    "thermalItemHeaderLine",
    "thermalMetaLabelLine",
    "thermalDeliveredByLine",
    "thermalItemLines",
    "thermalLineDiscountSuffix",
    "thermalPlainSep",
    "thermalPlainSepHeavy",
    "thermalPlainCenter",
    "receiptAttributionTailPlain",
    "isStockTransferReceipt",
    "buildThermalReceiptPlainHtml",
    "thermalReceiptDocExtractStyle",
    "thermalReceiptDocExtractBodyInner",
    "printReceiptInIframe",
    "printThermalReceipt",
    "printThermalReceiptMulti",
    "receiptVariantsForCheckout",
]

out: list[str] = []
out.append("/** Standalone thermal receipt reprint (IT Support + shop receipt register). */")
out.append("(function () {")
out.append("  var ACTIVE_BOOT = null;")
out.append("  function receiptSettings() { return (ACTIVE_BOOT && ACTIVE_BOOT.receiptSettings) || {}; }")
out.append("  function getSite() { return (ACTIVE_BOOT && ACTIVE_BOOT.site) || {}; }")
out.append("  function getPrinting() { return (ACTIVE_BOOT && ACTIVE_BOOT.printing) || {}; }")
out.append("  function posIncludeTaxInTotals() {")
out.append("    var pp = getPrinting();")
out.append(
    "    return pp.pos_include_tax !== false && pp.pos_include_tax !== \"false\" && pp.pos_include_tax !== 0;"
)
out.append("  }")
out.append('  var RECEIPT_ATTRIBUTION_BY = "BUILT & MAINTAINED BY";')
out.append('  var RECEIPT_ATTRIBUTION_NAME = "FINAGRITECH SOLUTIONS";')

for fn in funcs:
    if fn == "buildThermalReceiptPlainHtml":
        body = extract_func(fn, end_marker="      /** ESC/POS bytes for Bluetooth thermal printers")
    else:
        body = extract_func(fn)
    body = body.replace("window.POS_SITE", "getSite()")
    body = body.replace("window.POS_PRINTING", "getPrinting()")
    lines = body.splitlines()
    indented = "\n".join("  " + ln if ln.strip() else ln for ln in lines)
    out.append(indented)

out.append("")
out.append("  function buildPersistedReprintPayload(sale, items) {")
out.append("    var payload = buildPersistedReceiptPayload(sale, items);")
out.append("    payload.isReprint = true;")
out.append("    return payload;")
out.append("  }")
out.append("")
out.append("  window.receiptThermalReprint = function (sale, items, boot) {")
out.append("    ACTIVE_BOOT = boot || {};")
out.append("    var payload = buildPersistedReprintPayload(sale, items);")
out.append("    var variants = receiptVariantsForCheckout();")
out.append("    return printThermalReceiptMulti(payload, variants);")
out.append("  };")
out.append("})();")

Path("static/js/receipt-thermal-reprint.js").write_text("\n".join(out) + "\n", encoding="utf-8")
print("written receipt-thermal-reprint.js")
