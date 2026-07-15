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
    "receiptFontSizeBucket",
    "receiptThermalFontCss",
    "receiptThermalMonoFontCss",
    "receiptThermalLineHeightCss",
    "receiptThermalDensityFactor",
    "receiptThermalPaperScale",
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
    "thermalKvRowHtml",
    "thermalKvBlockHtml",
    "thermalSaleItemsTableHtml",
    "thermalTransferItemsTableHtml",
    "receiptTailHtml",
    "thermalPlainSep",
    "thermalPlainSepHeavy",
    "thermalPlainCenter",
    "receiptAttributionTailPlain",
    "isReceiptTransactionPaymentLabel",
    "receiptTxnPaymentMethodTitle",
    "receiptExpandPaymentDetailLines",
    "receiptPaymentDetailKvRows",
    "receiptNormalizePaymentDetailType",
    "receiptPaymentInstructionLabel",
    "receiptPaymentInstructionLines",
    "receiptPaybillInstructionFields",
    "receiptPaymentInstructionHasContent",
    "receiptPaybillInstructionHtml",
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
out.append('  var RECEIPT_ATTRIBUTION_NAME_DEFAULT = "FINAGRITECH SOLUTIONS";')
out.append("  function receiptShowAttribution() {")
out.append("          var S = receiptSettings();")
out.append(
    '          if (S.show_attribution === false || S.show_attribution === "false" || S.show_attribution === 0 || S.show_attribution === "0") {'
)
out.append("            return false;")
out.append("          }")
out.append(
    '          if (S.show_attribution === true || S.show_attribution === "true" || S.show_attribution === 1 || S.show_attribution === "1") {'
)
out.append("            return true;")
out.append("          }")
out.append("          return true;")
out.append("        }")
out.append("  function receiptAttributionName() {")
out.append("          var S = receiptSettings();")
out.append('          var n = String(S.attribution_name != null ? S.attribution_name : "").trim();')
out.append("          if (n) return n;")
out.append('          if (S.attribution_name === "" || S.attribution_name === null) return "";')
out.append("          return RECEIPT_ATTRIBUTION_NAME_DEFAULT;")
out.append("        }")
out.append("  function syncReceiptThermalEngineBoot() {}")

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
out.append("  var RECEIPT_PREVIEW_PRINT_CSS = (")
out.append('    ".receipt-browser-print-tip{display:none!important}" +')
out.append(
    '    "html,body{margin:0!important;padding:0!important;width:100%!important;max-width:100%!important;overflow-x:hidden!important}" +'
)
out.append(
    '    ".receipt-thermal{width:100%!important;max-width:100%!important;padding-left:1mm!important;padding-right:1mm!important}" +'
)
out.append(
    '    ".receipt-items,.receipt-kv,.receipt-totals,.receipt-payment,.receipt-tail,.receipt-items-wrap{width:100%!important;max-width:100%!important}"'
)
out.append("  );")
out.append("")
out.append("  function receiptThermalPrepareHtmlForPrint(fullHtml) {")
out.append('    return String(fullHtml || "").replace("</style>", RECEIPT_PREVIEW_PRINT_CSS + "</style>");')
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
out.append("  /** Shared thermal receipt engine — POS checkout uses the same HTML layout as IT preview. */")
out.append("  window.receiptThermalEngine = {")
out.append("    setBoot: function (boot) { ACTIVE_BOOT = boot || {}; },")
out.append("    buildPlainHtml: buildThermalReceiptPlainHtml,")
out.append("    buildHtmlMulti: buildReceiptThermalHtmlMulti,")
out.append("    variantsForCheckout: receiptVariantsForCheckout,")
out.append("    docExtractStyle: thermalReceiptDocExtractStyle,")
out.append("    docExtractBody: thermalReceiptDocExtractBodyInner,")
out.append("    prepareHtmlForPrint: receiptThermalPrepareHtmlForPrint,")
out.append("  };")
out.append("")
out.append("  window.receiptThermalReprint = function (sale, items, boot) {")
out.append("    ACTIVE_BOOT = boot || {};")
out.append("    var payload = buildPersistedReprintPayload(sale, items);")
out.append("    var variants = receiptVariantsForCheckout();")
out.append("    return printThermalReceiptMulti(payload, variants);")
out.append("  };")
out.append("")
out.append("  /**")
out.append("   * Hidden embed iframe entry (shop/IT receipt register auto_print=1).")
out.append("   * Uses the same thermal HTML layout as settings preview + POS checkout.")
out.append("   */")
out.append("  window.receiptThermalEmbedAutoPrint = function (data) {")
out.append("    data = data || {};")
out.append("    var sale = data.sale || {};")
out.append("    var items = data.items || [];")
out.append("    var boot = data.boot || {};")
out.append("    if (!sale || !items || !items.length) return Promise.resolve();")
out.append("    var printed = window.receiptThermalReprint(sale, items, boot);")
out.append('    var recordUrl = String(data.reprintRecordUrl || "").trim();')
out.append("    if (recordUrl) {")
out.append("      try {")
out.append("        var body = {")
out.append("          sale_id: parseInt(data.saleId || sale.id || 0, 10) || 0,")
out.append("        };")
out.append("        var shopId = parseInt(data.shopId || sale.shop_id || 0, 10) || 0;")
out.append("        if (shopId) body.shop_id = shopId;")
out.append("        fetch(recordUrl, {")
out.append('          method: "POST",')
out.append('          credentials: "same-origin",')
out.append('          headers: { "Content-Type": "application/json", Accept: "application/json" },')
out.append("          body: JSON.stringify(body),")
out.append("        }).catch(function () {});")
out.append("      } catch (e) {}")
out.append("    }")
out.append("    return printed;")
out.append("  };")
out.append("})();")

Path("static/js/receipt-thermal-reprint.js").write_text("\n".join(out) + "\n", encoding="utf-8")
print("written receipt-thermal-reprint.js")
