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
    "receiptThermalMonoFontCss",
    "computePosTax",
    "buildPersistedReceiptPayload",
    "buildReceiptQrPayloadDetailsJson",
    "buildReceiptQrPayloadString",
    "receiptQrEnabled",
    "waitForIframeImages",
    "thermalTransferItemHeaderLine",
    "thermalTransferItemLines",
    "thermalItemLayout",
    "thermalItemRightWidth",
    "thermalItemValues",
    "thermalItemRightPart",
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
out.append("  function buildReceiptThermalHtmlMulti(payload, variants) {")
out.append("    var vars = Array.isArray(variants) && variants.length ? variants.slice() : [\"customer\"];")
out.append("    if (vars.length === 1) return buildThermalReceiptPlainHtml(payload, vars[0]);")
out.append("    var firstHtml = buildThermalReceiptPlainHtml(payload, vars[0]);")
out.append("    var style = thermalReceiptDocExtractStyle(firstHtml);")
out.append("    var chunks = vars.map(function (v) {")
out.append("      return thermalReceiptDocExtractBodyInner(buildThermalReceiptPlainHtml(payload, v));")
out.append("    });")
out.append("    var combinedInner = chunks")
out.append("      .map(function (inner, idx) {")
out.append("        var pba = idx < chunks.length - 1 ? \"page-break-after:always;break-after:page;\" : \"\";")
out.append("        return '<section class=\"rc-multi-slip\" style=\"' + pba + '\">' + inner + \"</section>\";")
out.append("      })")
out.append("      .join(\"\");")
out.append("    return (")
out.append("      \"<!doctype html><html><head><meta charset='utf-8'><title>Receipts</title>\" +")
out.append("      \"<style>\" +")
out.append("      style +")
out.append("      \".rc-multi-slip{display:block}\" +")
out.append("      \"</style></head><body>\" +")
out.append("      combinedInner +")
out.append("      \"</body></html>\"")
out.append("    );")
out.append("  }")
out.append("")
out.append("  function buildDemoReceiptPreviewPayload(opts) {")
out.append("    opts = opts || {};")
out.append("    var rs = opts.receiptSettings || {};")
out.append("    var site = opts.site || {};")
out.append("    var demoLines = [")
out.append("      { name: \"Coffee · Regular Size Large Cup\", qty: 2, price: 4.5, total: 9.0 },")
out.append("      { name: \"Bread · Whole Wheat Loaf Fresh\", qty: 1, price: 2.0, total: 2.0 },")
out.append("    ];")
out.append("    var subtotal = 11.0;")
out.append("    var taxPct = parseFloat(String(rs.tax_percent || \"0\").replace(\",\", \".\")) || 0;")
out.append("    var includeTax = taxPct > 0.000001 && !!rs.include_tax;")
out.append("    var taxAmt = includeTax ? round2(subtotal * (taxPct / 100)) : 0;")
out.append("    var grand = round2(subtotal + taxAmt);")
out.append("    var prefix = String(rs.receipt_number_prefix || \"T\").trim() || \"T\";")
out.append("    var startNum = String(rs.starting_number || \"1001\").trim() || \"1001\";")
out.append("    var receiptNo = prefix + \"-\" + startNum;")
out.append("    var company = String(site.company_name || \"\").trim();")
out.append("    return {")
out.append("      receiptNo: receiptNo,")
out.append("      printedAt: new Date().toLocaleString(),")
out.append("      shopName: String(site.shop_name || company || \"Demo Shop\"),")
out.append("      companyName: company,")
out.append("      shopCode: String(site.shop_code || \"\").trim(),")
out.append("      shopLocation: String(site.shop_location || site.company_location_name || \"\").trim(),")
out.append("      receiptLogoUrl: String(site.receipt_logo_url || site.app_icon_url || \"\").trim(),")
out.append("      mode: \"Sale\",")
out.append("      isQuotation: false,")
out.append("      isReprint: false,")
out.append("      customerName: \"Walk-in customer\",")
out.append("      customerPhone: \"-\",")
out.append("      employeeName: \"Jane Doe\",")
out.append("      employeeCode: \"123456\",")
out.append("      lines: demoLines,")
out.append("      subtotal: subtotal,")
out.append("      discountTotal: 0,")
out.append("      hasDiscount: false,")
out.append("      taxAmount: taxAmt,")
out.append("      taxPercent: taxPct,")
out.append("      grandTotal: grand,")
out.append("      includeTax: includeTax,")
out.append("      paymentTypeLabel: \"Payment: Cash\",")
out.append("      paymentDetailText: \"Cash: \" + fmt(grand),")
out.append("      receiptHeader: String(rs.receipt_header || \"\").trim(),")
out.append("      receiptFooter: String(rs.receipt_footer || \"\").trim(),")
out.append("      creditDueDate: \"\",")
out.append("    };")
out.append("  }")
out.append("")
out.append("  window.receiptThermalRenderSettingsPreview = function (boot, variant) {")
out.append("    ACTIVE_BOOT = boot || {};")
out.append("    var payload = buildDemoReceiptPreviewPayload(boot);")
out.append("    var fullHtml = buildThermalReceiptPlainHtml(payload, variant || \"customer\");")
out.append("    return {")
out.append("      widthMm: receiptRollWidthMm(),")
out.append("      fullHtml: fullHtml,")
out.append("      style: thermalReceiptDocExtractStyle(fullHtml),")
out.append("      body: thermalReceiptDocExtractBodyInner(fullHtml),")
out.append("    };")
out.append("  };")
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
