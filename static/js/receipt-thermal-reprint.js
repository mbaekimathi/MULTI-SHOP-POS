/** Standalone thermal receipt reprint (IT Support + shop receipt register). */
(function () {
  var ACTIVE_BOOT = null;
  function receiptSettings() { return (ACTIVE_BOOT && ACTIVE_BOOT.receiptSettings) || {}; }
  function getSite() { return (ACTIVE_BOOT && ACTIVE_BOOT.site) || {}; }
  function getPrinting() { return (ACTIVE_BOOT && ACTIVE_BOOT.printing) || {}; }
  function posIncludeTaxInTotals() {
    var pp = getPrinting();
    return pp.pos_include_tax !== false && pp.pos_include_tax !== "false" && pp.pos_include_tax !== 0;
  }
  var RECEIPT_ATTRIBUTION_BY = "BUILT & MAINTAINED BY";
  var RECEIPT_ATTRIBUTION_NAME_DEFAULT = "FINAGRITECH SOLUTIONS";
  function receiptShowAttribution() {
          var S = receiptSettings();
          if (S.show_attribution === false || S.show_attribution === "false" || S.show_attribution === 0 || S.show_attribution === "0") {
            return false;
          }
          if (S.show_attribution === true || S.show_attribution === "true" || S.show_attribution === 1 || S.show_attribution === "1") {
            return true;
          }
          return true;
        }
  function receiptAttributionName() {
          var S = receiptSettings();
          var n = String(S.attribution_name != null ? S.attribution_name : "").trim();
          if (n) return n;
          if (S.attribution_name === "" || S.attribution_name === null) return "";
          return RECEIPT_ATTRIBUTION_NAME_DEFAULT;
        }
  function syncReceiptThermalEngineBoot() {}
  function fmt(n) {
          var x = parseFloat(n);
          if (isNaN(x)) x = 0;
          return x.toFixed(2);
        }
  function fmtQty(n) {
          var x = parseFloat(n);
          if (isNaN(x)) x = 0;
          if (Math.abs(x - Math.round(x)) < 1e-9) return String(Math.round(x));
          var s = x.toFixed(3);
          return s.replace(/0+$/, "").replace(/\.$/, "");
        }
  function round2(n) {
          return Math.round((parseFloat(n) || 0) * 100) / 100;
        }
  function receiptEsc(s) {
          var d = document.createElement("div");
          d.textContent = String(s == null ? "" : s);
          return d.innerHTML;
        }
  function normalizeReceiptWidthKey(raw) {
          var s = String(raw == null ? "" : raw)
            .trim()
            .toLowerCase();
          if (
            s === "58mm" ||
            s === "58" ||
            s === "50mm" ||
            s === "50" ||
            s === "narrow" ||
            s === "48mm" ||
            s === "2in" ||
            s === "2"
          ) {
            return "58mm";
          }
          return "80mm";
        }
  function receiptLineWidthChars() {
          return normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm" ? 32 : 48;
        }
  function receiptRollWidthMm() {
          return normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm" ? 58 : 80;
        }
  function receiptThermalFontCss() {
          var raw = receiptSettings().font_size || receiptSettings().receipt_font_size || "11pt";
          var s = String(raw == null ? "" : raw).trim();
          if (!s) s = "11pt";
          if (!/pt|px|mm|em|rem$/i.test(s)) s = s + "pt";
          if (normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm") {
            var n = parseFloat(s, 10);
            if (!isNaN(n) && s.indexOf("pt") !== -1 && n > 9) return "9pt";
          }
          return s;
        }
  function receiptThermalMonoFontCss() {
          return receiptThermalFontCss();
        }
  function computePosTax(subtotal) {
          var rs = receiptSettings();
          var taxPct = parseFloat(String(rs.tax_percent || "0").replace(",", ".")) || 0;
          /* POS tab (pos_include_tax) or Receipt format (include_tax) can enable tax; rate comes from receipt settings. */
          var includeTax =
            taxPct > 0.000001 && (!!rs.include_tax || posIncludeTaxInTotals());
          var taxAmt = includeTax ? Math.round(subtotal * (taxPct / 100) * 100) / 100 : 0;
          var grand = round2(subtotal + taxAmt);
          return { taxPct: taxPct, taxAmt: taxAmt, grand: grand, includeTax: includeTax };
        }
  function buildPersistedReceiptPayload(sale, items) {
          var saleObj = sale || {};
          var rows = (items || []).map(function (it) {
            var qty = parseFloat(it.qty || 0);
            if (isNaN(qty) || qty < 0) qty = 0;
            var total = Number(it.line_total || 0);
            var unit = Number(it.unit_price || 0);
            return {
              id: it.item_id,
              name: it.item_name || "Item",
              qty: qty,
              price: unit,
              listPrice: unit,
              total: total,
              listTotal: total,
              lineDiscount: 0,
              discounted: false,
            };
          });
          var subtotal = rows.reduce(function (sum, it) { return sum + Number(it.total || 0); }, 0);
          var tx = computePosTax(subtotal);
          var pm = String(saleObj.payment_method || "").toLowerCase();
          var paymentTypeLabel = "Payment";
          var paymentDetailText = "";
          var cashAmt = Number(saleObj.cash_amount || 0);
          var mpesaAmt = Number(saleObj.mpesa_amount || 0);
          var totalAmt = Number(saleObj.total_amount || 0);
          var detailLines = [];
          if (pm === "cash") {
            paymentTypeLabel = "Payment: Cash";
            detailLines.push("Cash: " + fmt(cashAmt || totalAmt));
          } else if (pm === "mpesa") {
            paymentTypeLabel = "Payment: M-Pesa";
            detailLines.push("M-Pesa: " + fmt(mpesaAmt || totalAmt));
          } else if (pm === "both") {
            paymentTypeLabel = "Payment: Cash + M-Pesa";
            detailLines.push("Cash: " + fmt(cashAmt));
            detailLines.push("M-Pesa: " + fmt(mpesaAmt));
          } else if (String(saleObj.sale_type || "") === "credit" || pm === "credit") {
            paymentTypeLabel = "Payment: Credit";
            detailLines.push("Pending payment");
          }
          var mpesaRef = String(saleObj.mpesa_receipt_number || "").trim();
          if (mpesaRef && (pm === "mpesa" || pm === "both")) {
            detailLines.push("M-Pesa Ref: " + mpesaRef);
          }
          paymentDetailText = detailLines.join("\n");
          var createdAt = saleObj.created_at ? String(saleObj.created_at) : new Date().toLocaleString();
          var site = getSite() || {};
          var rs = receiptSettings();
          return {
            receiptNo: String(saleObj.receipt_number || ("#" + String(saleObj.id || ""))),
            printedAt: createdAt,
            shopName: String(saleObj.shop_name || site.shop_name || "Point of Sale"),
            companyName: String(site.company_name || ""),
            shopCode: String(saleObj.shop_code || site.shop_code || ""),
            shopLocation: String(site.shop_location || ""),
            receiptLogoUrl: String(site.receipt_logo_url || site.app_icon_url || ""),
            mode: String(saleObj.sale_type || "sale") === "credit" ? "Credit" : "Sale",
            isQuotation: false,
            isReprint: true,
            customerName: String(saleObj.customer_name || "Walk-in customer"),
            customerPhone: String(saleObj.customer_phone || "-"),
            employeeName: String(saleObj.employee_name || "Unknown"),
            employeeCode: String(saleObj.employee_code || "-"),
            lines: rows,
            subtotal: subtotal,
            discountTotal: 0,
            hasDiscount: false,
            taxAmount: tx.taxAmt,
            taxPercent: tx.taxPct,
            grandTotal: Number(saleObj.total_amount || tx.grand || subtotal),
            includeTax: tx.includeTax,
            paymentTypeLabel: paymentTypeLabel,
            paymentDetailText: paymentDetailText,
            receiptHeader: (rs.receipt_header || "").trim(),
            receiptFooter: (rs.receipt_footer || "").trim(),
            creditDueDate: "",
          };
        }
  function buildReceiptQrPayloadDetailsJson(payload) {
          var obj = {
            r: String(payload.receiptNo || ""),
            t: fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
            d: String(payload.printedAt || ""),
            s: String(payload.shopName || ""),
            m: String(payload.mode || ""),
            c: String(payload.customerName || ""),
            p: String(payload.customerPhone || ""),
            i: (payload.lines || []).map(function (l) {
              return {
                n: String((l && l.name) || "Item"),
                q: (l && l.qty) != null ? l.qty : 0,
                x: fmt((l && l.total) || 0),
              };
            }),
          };
          if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
            obj.dc = fmt(payload.discountTotal);
          }
          var dueQr = (payload.creditDueDate || "").trim();
          if (dueQr && String(payload.mode || "").toLowerCase().indexOf("credit") !== -1) {
            obj.due = dueQr;
          }
          try {
            return JSON.stringify(obj);
          } catch (e) {
            return "";
          }
        }
  function buildReceiptQrPayloadString(payload, S) {
          S = S || receiptSettings();
          var qm = String(S.receipt_qr_mode || "receipt_details").trim();
          if (qm === "website") {
            var u = (S.receipt_qr_website_url || "").trim();
            if (u) return u;
          }
          return buildReceiptQrPayloadDetailsJson(payload);
        }
  function receiptQrEnabled(S) {
          S = S || receiptSettings();
          var v = S.receipt_qr_enabled;
          return v === true || v === 1 || v === "1" || v === "true" || v === "True";
        }
  function waitForIframeImages(doc, callback, maxWaitMs) {
          maxWaitMs = maxWaitMs == null ? 5000 : maxWaitMs;
          var imgs = doc.querySelectorAll("img");
          var pending = 0;
          for (var i = 0; i < imgs.length; i++) {
            if (!imgs[i].complete) pending++;
          }
          if (pending === 0) {
            callback();
            return;
          }
          var done = false;
          function finish() {
            if (done) return;
            done = true;
            callback();
          }
          var to = setTimeout(finish, maxWaitMs);
          for (var j = 0; j < imgs.length; j++) {
            var img = imgs[j];
            if (img.complete) continue;
            (function (im) {
              function fin() {
                im.removeEventListener("load", fin);
                im.removeEventListener("error", fin);
                pending--;
                if (pending <= 0) {
                  clearTimeout(to);
                  finish();
                }
              }
              im.addEventListener("load", fin);
              im.addEventListener("error", fin);
            })(img);
          }
        }
  function thermalTransferItemHeaderLine(W) {
          var qtyW = 4;
          var right = " " + "Qty".padStart(qtyW, " ");
          var nameW = W - right.length;
          if (nameW < 6) nameW = 6;
          return ("Item" + " ".repeat(nameW)).slice(0, nameW) + right;
        }
  function thermalTransferItemLines(l, W) {
          var qtyW = 4;
          var qty = fmtQty((l && l.qty) != null ? l.qty : 0);
          var right = " " + qty.padStart(qtyW, " ");
          var nameW = W - right.length;
          if (nameW < 6) nameW = 6;
          var rawName = String((l && l.name) || "Item");
          var out = [];
          var idx = 0;
          while (idx < rawName.length) {
            var take = Math.min(nameW, rawName.length - idx);
            var chunk = rawName.slice(idx, idx + take);
            idx += take;
            var isLast = idx >= rawName.length;
            if (isLast) {
              out.push((chunk + " ".repeat(nameW)).slice(0, nameW) + right);
            } else {
              out.push(chunk);
            }
          }
          if (!out.length) {
            out.push((" ".repeat(nameW) + right).slice(0, W));
          }
          return out;
        }
  function thermalItemLayout(W) {
          if (W <= 32) {
            return { nameMin: 8, qtyW: 3, amtW: 8 };
          }
          return { nameMin: 12, qtyW: 4, amtW: 10 };
        }
  function thermalItemRightWidth(W) {
          var L = thermalItemLayout(W);
          return 1 + L.qtyW + 1 + L.amtW;
        }
  function thermalItemValues(l) {
          var qtyN = parseFloat((l && l.qty) != null ? l.qty : 0);
          if (isNaN(qtyN) || qtyN < 0) qtyN = 0;
          return {
            qty: fmtQty(qtyN),
            amt: fmt((l && l.total) || 0),
          };
        }
  function thermalItemRightPart(W, cols) {
          var L = thermalItemLayout(W);
          function col(v, w) {
            var s = String(v == null ? "" : v);
            if (s.length > w) s = s.slice(0, w);
            return s.padStart(w, " ");
          }
          return " " + col(cols.qty, L.qtyW) + " " + col(cols.amt, L.amtW);
        }
  function thermalItemHeaderLine(W) {
          var L = thermalItemLayout(W);
          var right = thermalItemRightPart(W, {
            qty: "Qty",
            amt: "Amt",
          });
          var nameW = Math.max(L.nameMin, W - right.length);
          return ("Item" + " ".repeat(nameW)).slice(0, nameW) + right;
        }
  function thermalMetaLabelLine(label, value, W) {
          label = String(label || "").toUpperCase();
          value = String(value == null ? "" : value);
          var left = (label.length > 9 ? label.slice(0, 9) : label) + " ";
          var valMax = Math.max(8, W - left.length);
          var val = value.length > valMax ? value.slice(0, valMax - 1) + "\u2026" : value;
          return left + val.padStart(valMax, " ");
        }
  function thermalDeliveredByLine(name, W) {
          var label = "DELIVERED BY";
          var left = label + " ";
          var val = String(name || "").trim();
          var valMax = Math.max(8, W - left.length);
          if (val) {
            var shown = val.length > valMax ? val.slice(0, valMax - 1) + "\u2026" : val;
            return left + shown.padStart(valMax, " ");
          }
          return left + "_".repeat(valMax);
        }
  function thermalItemLines(l, W) {
          var vals = thermalItemValues(l);
          var right = thermalItemRightPart(W, vals);
          var L = thermalItemLayout(W);
          var nameW = Math.max(L.nameMin, W - right.length);
          var rawName = String((l && l.name) || "Item");
          if (rawName.length > nameW) {
            rawName = rawName.slice(0, Math.max(1, nameW - 1)) + "\u2026";
          }
          var out = [(rawName + " ".repeat(nameW)).slice(0, nameW) + right];
          if (l && l.discounted) {
            thermalLineDiscountSuffix(l, W).forEach(function (row) {
              out.push(row);
            });
          }
          return out;
        }
  function thermalLineDiscountSuffix(l, W) {
          if (!l || !l.discounted) return [];
          var save = fmt(l.lineDiscount || 0);
          var s = "  * DISCOUNTED  save " + save;
          W = W || receiptLineWidthChars();
          if (s.length <= W) return [s];
          s = "  * DISCOUNTED  " + save;
          if (s.length <= W) return [s];
          return [s.slice(0, W)];
        }
  function thermalKvRowHtml(label, value, extraClass) {
          return (
            '<div class="receipt-kv__row' +
            (extraClass ? " " + extraClass : "") +
            '"><span class="receipt-kv__label">' +
            receiptEsc(label) +
            '</span><span class="receipt-kv__value">' +
            receiptEsc(value) +
            "</span></div>"
          );
        }
  function thermalKvBlockHtml(rows, blockClass) {
          if (!rows || !rows.length) return "";
          var inner = rows
            .map(function (r) {
              return thermalKvRowHtml(r[0], r[1], r[2]);
            })
            .join("");
          return (
            '<div class="receipt-kv receipt-body' +
            (blockClass ? " " + blockClass : "") +
            '">' +
            inner +
            "</div>"
          );
        }
  function thermalSaleItemsTableHtml(lines) {
          if (!lines || !lines.length) return "";
          var rows = "";
          lines.forEach(function (l) {
            var vals = thermalItemValues(l);
            var name = String((l && l.name) || "Item");
            if (l && l.discounted) name += " *";
            rows +=
              "<tr>" +
              '<td class="receipt-items__name">' +
              receiptEsc(name) +
              "</td>" +
              '<td class="receipt-items__qty">' +
              receiptEsc(vals.qty) +
              "</td>" +
              '<td class="receipt-items__amt">' +
              receiptEsc(vals.amt) +
              "</td>" +
              "</tr>";
            if (l && l.discounted) {
              rows +=
                '<tr class="receipt-items__disc-row"><td colspan="3" class="receipt-items__disc">* Discount save ' +
                receiptEsc(fmt(l.lineDiscount || 0)) +
                "</td></tr>";
            }
          });
          return (
            '<div class="receipt-items-wrap receipt-body">' +
            '<table class="receipt-items" role="table">' +
            '<colgroup><col class="receipt-items__name"><col class="receipt-items__qty"><col class="receipt-items__amt"></colgroup>' +
            "<thead><tr>" +
            '<th class="receipt-items__name">Item</th>' +
            '<th class="receipt-items__qty">Qty</th>' +
            '<th class="receipt-items__amt">Amt</th>' +
            "</tr></thead><tbody>" +
            rows +
            "</tbody></table></div>"
          );
        }
  function thermalTransferItemsTableHtml(lines) {
          if (!lines || !lines.length) return "";
          var rows = "";
          lines.forEach(function (l) {
            var qty = fmtQty((l && l.qty) != null ? l.qty : 0);
            rows +=
              "<tr>" +
              '<td class="receipt-items__name">' +
              receiptEsc(String((l && l.name) || "Item")) +
              "</td>" +
              '<td class="receipt-items__qty">' +
              receiptEsc(qty) +
              "</td>" +
              "</tr>";
          });
          return (
            '<div class="receipt-items-wrap receipt-body">' +
            '<table class="receipt-items receipt-items--transfer" role="table">' +
            "<thead><tr>" +
            '<th class="receipt-items__name">Item</th>' +
            '<th class="receipt-items__qty">Qty</th>' +
            "</tr></thead><tbody>" +
            rows +
            "</tbody></table></div>"
          );
        }
  function receiptTailHtml(isCompany) {
          var parts = [];
          if (!isCompany) {
            parts.push('<p class="receipt-tail__thanks">Thank you</p>');
          }
          var name = receiptAttributionName();
          if (receiptShowAttribution() && name) {
            parts.push('<p class="receipt-tail__line">' + receiptEsc(RECEIPT_ATTRIBUTION_BY) + "</p>");
            parts.push(
              '<p class="receipt-tail__line receipt-tail__line--brand">' +
                receiptEsc(name) +
                "</p>"
            );
          }
          return '<footer class="receipt-tail receipt-body">' + parts.join("") + "</footer>";
        }
  function thermalPlainSep(w) {
          w = w || 32;
          var n = Math.min(Math.max(8, w), 48);
          return new Array(n + 1).join("-");
        }
  function thermalPlainSepHeavy(w) {
          w = w || 32;
          var n = Math.min(Math.max(8, w), 48);
          return new Array(n + 1).join("=");
        }
  function thermalPlainCenter(s, w) {
          s = String(s || "");
          if (s.length >= w) return s.slice(0, w);
          var pad = Math.floor((w - s.length) / 2);
          return new Array(pad + 1).join(" ") + s;
        }
  function receiptAttributionTailPlain(isCompany, W) {
          var name = receiptAttributionName();
          var showAttr = receiptShowAttribution() && !!name;
          if (isCompany) {
            var co = thermalPlainCenter("Company records", W);
            if (showAttr) co += "\n" + thermalPlainCenter(name, W);
            return co;
          }
          var out = thermalPlainCenter("Thank you", W);
          if (showAttr) {
            out +=
              "\n" +
              thermalPlainCenter(RECEIPT_ATTRIBUTION_BY, W) +
              "\n" +
              thermalPlainCenter(name, W);
          }
          return out;
        }
  function isReceiptTransactionPaymentLabel(label) {
          return /^Payment:/i.test(String(label || "").trim());
        }
  function receiptTxnPaymentMethodTitle(label) {
          var s = String(label || "").trim();
          var m = s.match(/^Payment:\s*(.+)$/i);
          return (m ? m[1] : s).trim() || "Payment";
        }
  function receiptExpandPaymentDetailLines(raw) {
          var out = [];
          String(raw || "")
            .split(/\r?\n/)
            .forEach(function (line) {
              String(line || "")
                .split(/\s*\|\s*/)
                .forEach(function (part) {
                  var t = String(part || "").trim();
                  if (t) out.push(t);
                });
            });
          return out;
        }
  function receiptPaymentDetailKvRows(raw) {
          var rows = [];
          var plain = [];
          receiptExpandPaymentDetailLines(raw).forEach(function (ln) {
            var m = String(ln).match(/^([^:]+):\s*(.+)$/);
            if (m) rows.push([m[1].trim(), m[2].trim()]);
            else plain.push(ln);
          });
          return { rows: rows, plain: plain };
        }
  function receiptNormalizePaymentDetailType(rs) {
          var pt = String((rs && rs.payment_detail_type) || "buy_goods")
            .trim()
            .toLowerCase();
          if (pt === "paybill" || pt === "send_money" || pt === "buy_goods") return pt;
          return "buy_goods";
        }
  function receiptPaymentInstructionLabel(rs) {
          var payLabels = { buy_goods: "Buy goods", paybill: "Pay bill", send_money: "Send money" };
          var pt = receiptNormalizePaymentDetailType(rs);
          return payLabels[pt] || "Buy goods";
        }
  function receiptPaymentInstructionLines(rs) {
          var pt = receiptNormalizePaymentDetailType(rs);
          if (pt === "paybill") return [];
          var text = String((rs && rs.payment_detail_text) || "").trim();
          if (!text) return [];
          return text
            .split(/\r?\n/)
            .map(function (ln) {
              return String(ln).trim();
            })
            .filter(Boolean);
        }
  function receiptPaybillInstructionFields(rs) {
          if (receiptNormalizePaymentDetailType(rs) !== "paybill") {
            return { business: "", account: "" };
          }
          return {
            business: String((rs && rs.payment_paybill_business) || "").trim(),
            account: String((rs && rs.payment_paybill_account) || "").trim(),
          };
        }
  function receiptPaymentInstructionHasContent(rs) {
          if (!(rs && rs.show_payment_details)) return false;
          var pt = receiptNormalizePaymentDetailType(rs);
          if (pt === "paybill") {
            var fields = receiptPaybillInstructionFields(rs);
            return !!(fields.business || fields.account);
          }
          return receiptPaymentInstructionLines(rs).length > 0;
        }
  function receiptPaybillInstructionHtml(rs) {
          var fields = receiptPaybillInstructionFields(rs);
          if (!fields.business && !fields.account) return "";
          var label = receiptPaymentInstructionLabel(rs);
          return (
            '<div class="receipt-payment receipt-payment--instructions receipt-payment--paybill">' +
            '<p class="receipt-payment__heading">PAY VIA</p>' +
            '<p class="receipt-payment__method">' +
            receiptEsc(label) +
            "</p>" +
            '<div class="receipt-paybill-stack receipt-body">' +
            '<div class="receipt-paybill-row">' +
            '<span class="receipt-paybill-label">Business</span>' +
            '<span class="receipt-paybill-value">' +
            receiptEsc(fields.business || "—") +
            "</span>" +
            "</div>" +
            '<div class="receipt-paybill-row">' +
            '<span class="receipt-paybill-label">Account</span>' +
            '<span class="receipt-paybill-value">' +
            receiptEsc(fields.account || "—") +
            "</span>" +
            "</div>" +
            "</div></div>"
          );
        }
  function isStockTransferReceipt(payload) {
          return String((payload && payload.mode) || "")
            .trim()
            .toLowerCase() === "stock transfer";
        }
  function buildThermalReceiptPlainHtml(payload, variant) {
          variant = variant || "customer";
          var isCompany = variant === "company";
          var isCashier = variant === "cashier";
          var showPrices = !isCompany;
          var showCustomerBlock = !isCompany;
          var S = receiptSettings();
          var site = getSite() || {};
          var rollWmm = receiptRollWidthMm();
          var W = receiptLineWidthChars();
          var sep = thermalPlainSep(W);
          var heavy = thermalPlainSepHeavy(W);
          var baseFont = receiptThermalFontCss();
          var monoFont = receiptThermalMonoFontCss();
          var boldHeaders = !!S.bold_headers;
          var receiptWeight = boldHeaders ? "700" : "600";
          var titleWeight = boldHeaders ? "900" : "700";
          var lines = [];
          function L(t) {
            lines.push(String(t == null ? "" : t));
          }

          function buildThermalReceiptStyles(rollCss, rwmm, fontCss, monoCss, preWeight, headingWeight) {
            monoCss = monoCss || monoFont;
            preWeight = preWeight || receiptWeight;
            headingWeight = headingWeight || titleWeight;
            return (
              "@page{margin:0;size:" +
              rollCss +
              " auto}" +
              "*{box-sizing:border-box}" +
              "html{-webkit-print-color-adjust:exact;print-color-adjust:exact}" +
              "html,body{margin:0!important;padding:0!important;background:#fff;color:#111;width:100%;max-width:100%}" +
              ".receipt-thermal{padding:0 1mm 2mm;width:100%;max-width:100%;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:" +
              fontCss +
              ";line-height:1.22}" +
              ".receipt-thermal-accent{height:2mm;margin:0 0 1.5mm;background:linear-gradient(90deg,#1d4ed8,#2563eb,#1d4ed8)}" +
              ".receipt-logo-row{text-align:center;margin:0 auto 1mm;padding:0 1mm}" +
              ".receipt-logo-row img{display:block;margin:0 auto;max-height:10mm;max-width:68mm;height:auto;width:auto;object-fit:contain}" +
              ".receipt-thermal-brand{text-align:center;padding:0 0 0.5mm}" +
              ".receipt-thermal-badge{margin:0 0 1mm;padding:1px 6px;display:inline-block;border-radius:999px;font-size:0.625em;font-weight:800;letter-spacing:0.08em;text-transform:uppercase}" +
              ".receipt-thermal-badge--company{background:#f3f4f6;color:#374151;border:1px solid #d1d5db}" +
              ".receipt-thermal-badge--cashier{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe}" +
              ".receipt-thermal-shopname{margin:0;font-size:1.05em;font-weight:" +
              headingWeight +
              ";letter-spacing:-0.02em;line-height:1.12;color:#1e3a8a}" +
              ".receipt-thermal-company{margin:1px 0 0;font-size:0.75em;font-weight:600;color:#4b5563}" +
              ".receipt-thermal-rule{height:0;border:none;border-top:1px solid #d1d5db;margin:0 0 1.5mm}" +
              ".receipt-thermal-contact{margin:0 0 1mm;text-align:center}" +
              ".receipt-thermal-customhdr{margin:0 0 1mm;font-size:0.71em;line-height:1.25;color:#000;font-weight:600;white-space:pre-wrap}" +
              ".receipt-thermal-mut{margin:0;font-size:0.67em;line-height:1.2;color:#000;font-weight:600}" +
              ".receipt-thermal-placeholder{font-style:italic}" +
              ".receipt-thermal-quote{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #94a3b8;background:#f8fafc;text-align:center}" +
              ".receipt-thermal-quote__title{display:block;font-size:0.83em;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#0f172a}" +
              ".receipt-thermal-quote__sub{display:block;margin-top:2px;font-size:0.79em;color:#64748b}" +
              ".receipt-thermal-reprint{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #f59e0b;background:#fffbeb;text-align:center}" +
              ".receipt-thermal-reprint__title{display:block;font-size:0.83em;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#92400e}" +
              ".receipt-thermal-reprint__sub{display:block;margin-top:2px;font-size:0.79em;color:#b45309}" +
              ".receipt-thermal-stock-transfer__title{margin:0 0 2px;font-size:0.92em;font-weight:900;letter-spacing:0.1em;text-transform:uppercase;color:#0f172a}" +
              ".receipt-thermal-stock-transfer__sub{margin:0;font-size:0.79em;font-weight:700;line-height:1.25;color:#1e3a8a;text-transform:none}" +
              "pre.receipt{margin:0;width:100%;max-width:100%;display:block;font-family:'Courier New',Consolas,'Liberation Mono',monospace;font-size:1em;font-weight:" +
              preWeight +
              ";line-height:1.3;white-space:pre-wrap;word-wrap:break-word;color:#000;letter-spacing:0}" +
              ".receipt-body{font-family:'Courier New',Consolas,'Liberation Mono',monospace;font-size:0.94em;font-weight:" +
              preWeight +
              ";color:#000;width:100%;max-width:100%}" +
              ".receipt-kv{border-top:1.5px solid #000;border-bottom:1.5px solid #000;padding:1mm 0;margin:0;width:100%}" +
              ".receipt-kv__row{display:grid;grid-template-columns:minmax(0,36%) minmax(0,64%);column-gap:1.5mm;align-items:baseline;padding:0.3mm 0;line-height:1.28;width:100%}" +
              ".receipt-kv__label{font-size:0.94em;font-weight:" +
              (boldHeaders ? "800" : "700") +
              ";letter-spacing:0.03em;text-transform:uppercase;color:#111;overflow-wrap:break-word}" +
              ".receipt-kv__value{text-align:right;font-variant-numeric:tabular-nums;overflow-wrap:break-word;word-break:break-word}" +
              ".receipt-items-wrap{border-top:1.5px solid #000;padding:0.8mm 0 0;margin:0;width:100%}" +
              ".receipt-items{width:100%;border-collapse:collapse;table-layout:fixed}" +
              ".receipt-items thead th{font-size:0.9em;font-weight:" +
              (boldHeaders ? "900" : "800") +
              ";letter-spacing:0.02em;text-transform:uppercase;padding:0 0 0.6mm;border-bottom:1px dashed #444;color:#111}" +
              ".receipt-items th,.receipt-items td{padding:0.4mm 0;vertical-align:top;line-height:1.28;font-variant-numeric:tabular-nums}" +
                ".receipt-items__name{text-align:left;width:68%;padding-right:0.6mm;white-space:normal;word-break:break-word;overflow-wrap:break-word;font-size:0.94em}" +
                ".receipt-items__qty{width:12%;text-align:center;white-space:nowrap;font-size:0.88em;padding:0}" +
                ".receipt-items__amt{width:20%;text-align:right;white-space:nowrap;font-weight:" +
                (boldHeaders ? "800" : "700") +
                ";font-size:0.88em;padding:0}" +
                ".receipt-items thead th.receipt-items__qty,.receipt-items thead th.receipt-items__amt{font-size:0.82em;letter-spacing:0.01em;padding:0}" +
              ".receipt-items--transfer .receipt-items__name{width:72%}" +
              ".receipt-items--transfer .receipt-items__qty{width:28%}" +
              ".receipt-items tbody tr+tr td{border-top:1px dotted #ccc}" +
              ".receipt-items__disc-row td{border-top:none!important;padding-top:0}" +
              ".receipt-items__disc{font-size:0.86em;font-style:italic;color:#333;padding:0 0 0.5mm!important}" +
              ".receipt-totals{border-top:1.5px solid #000;border-bottom:1.5px solid #000;padding:0.8mm 0;margin:0}" +
              ".receipt-totals .receipt-kv__row--grand{margin-top:0.6mm;padding-top:0.8mm;border-top:1px solid #000}" +
              ".receipt-totals .receipt-kv__row--grand .receipt-kv__label,.receipt-totals .receipt-kv__row--grand .receipt-kv__value{font-size:1.04em;font-weight:" +
              (boldHeaders ? "900" : "800") +
              "}" +
              ".receipt-payment{border-top:1px dashed #666;padding:1.2mm 0 0.8mm;margin:0;text-align:center}" +
              ".receipt-payment--txn{border-top-style:solid;border-top-color:#000}" +
              ".receipt-payment--instructions{border-top-style:dashed;border-top-color:#666;margin-top:0.4mm}" +
              ".receipt-payment__heading{margin:0 0 0.4mm;font-size:0.72em;font-weight:" +
              (boldHeaders ? "800" : "700") +
              ";letter-spacing:0.08em;text-transform:uppercase;color:#444}" +
              ".receipt-payment__method{margin:0;font-weight:" +
              (boldHeaders ? "900" : "800") +
              ";font-size:1em;letter-spacing:0.02em}" +
              ".receipt-payment__type{margin:0;font-weight:" +
              (boldHeaders ? "800" : "700") +
              ";font-size:0.95em}" +
              ".receipt-payment__detail{margin:0.4mm 0 0;font-variant-numeric:tabular-nums;font-size:0.92em}" +
              ".receipt-pay-lines{margin:0.8mm 0 0;width:100%;text-align:left}" +
              ".receipt-pay-line{display:grid;grid-template-columns:minmax(0,42%) minmax(0,58%);column-gap:1.5mm;align-items:baseline;padding:0.25mm 0;font-variant-numeric:tabular-nums;font-size:0.9em}" +
              ".receipt-pay-line__label{font-weight:" +
              (boldHeaders ? "700" : "600") +
              ";text-transform:uppercase;letter-spacing:0.02em;font-size:0.88em}" +
              ".receipt-pay-line__value{text-align:right;overflow-wrap:break-word;word-break:break-word}" +
              ".receipt-paybill-stack{display:flex;flex-direction:column;gap:1.2mm;margin-top:0.8mm;width:100%}" +
              ".receipt-paybill-row{min-width:0;text-align:center}" +
              ".receipt-paybill-label{display:block;font-size:0.82em;font-weight:" +
              (boldHeaders ? "800" : "700") +
              ";letter-spacing:0.03em;text-transform:uppercase}" +
              ".receipt-paybill-value{display:block;margin-top:0.3mm;font-variant-numeric:tabular-nums;word-break:break-word}" +
              ".receipt-tail{margin:1.2mm 0 0;padding:1mm 0 0;border-top:1px dashed #888;text-align:center;line-height:1.38}" +
              ".receipt-tail__thanks{margin:0 0 0.6mm;font-size:1em;font-weight:" +
              (boldHeaders ? "800" : "700") +
              "}" +
              ".receipt-tail__line{margin:0;font-size:0.82em;letter-spacing:0.03em;text-transform:uppercase;color:#333}" +
              ".receipt-tail__line--brand{margin-top:0.3mm;font-weight:" +
              (boldHeaders ? "800" : "700") +
              ";color:#111}" +
              "pre.receipt--meta,pre.receipt--payment,pre.receipt--totals{margin:0;padding:0.8mm 0;border-top:1px solid #000;border-bottom:1px solid #000;color:#000;font-weight:" +
              preWeight +
              "}" +
              "pre.receipt--items{margin:0;padding:0.8mm 0 0;border-top:1px solid #000}" +
              "pre.receipt--block{margin:1mm 0 0;padding:1mm 0 0;border-top:1px dashed #d1d5db}" +
              "pre.receipt--tail{margin:1mm 0 0;padding:1mm 0 0;border-top:1px solid #e5e7eb}" +
              ".receipt-thermal-footer{margin:0 0 1mm;padding:1mm 0 0;text-align:center;font-size:0.67em;line-height:1.25;color:#000;font-weight:600;border-top:1px dashed #000}" +
              ".receipt-qr{display:flex;flex-direction:column;align-items:center;width:100%;margin:0.5mm 0 0;padding:1mm 1mm 0;min-height:22mm}" +
              ".receipt-qr img{display:block;width:22mm;height:22mm;max-width:72%;object-fit:contain}" +
              ".receipt-qr-caption{margin:0.5mm 0 0;padding:0;width:100%;text-align:center;font-size:0.625em;line-height:1.25;color:#6b7280}" +
              ".receipt-browser-print-tip{margin:0 0 1.5mm;padding:1.5mm 2mm;border:1px solid #fcd34d;background:linear-gradient(180deg,#fffbeb,#fff7ed);color:#92400e;font-size:0.67em;line-height:1.3;border-radius:4px}" +
              "@media print{" +
              "@page{margin:0;size:" +
              rollCss +
              " auto}" +
              ".receipt-browser-print-tip{display:none!important}" +
              "html,body{width:" +
              rollCss +
              "!important;max-width:" +
              rollCss +
              "!important}" +
              "pre.receipt,pre.receipt--meta,pre.receipt--items,pre.receipt--totals,pre.receipt--payment{font-size:1em!important;font-weight:" +
              preWeight +
              "!important;color:#000!important}" +
              ".receipt-body,.receipt-kv,.receipt-items-wrap,.receipt-totals,.receipt-payment,.receipt-tail{font-size:1em!important;font-weight:" +
              preWeight +
              "!important;color:#000!important}" +
              ".receipt-thermal-mut,.receipt-thermal-customhdr,.receipt-thermal-footer,.receipt-thermal-company{color:#000!important;font-weight:" +
              preWeight +
              "!important}" +
              ".receipt-logo-row img{max-height:9mm}" +
              ".receipt-qr img{width:20mm!important;height:20mm!important}" +
              "}"
            );
          }

          function brandBlockHtml() {
            var badge = "";
            if (isCompany) {
              badge =
                '<p class="receipt-thermal-badge receipt-thermal-badge--company">Company copy · items only</p>';
            } else if (isCashier) {
              badge = '<p class="receipt-thermal-badge receipt-thermal-badge--cashier">Cashier copy</p>';
            }
            if (isStockTransferReceipt(payload)) {
              return (
                '<header class="receipt-thermal-brand">' +
                badge +
                '<p class="receipt-thermal-stock-transfer__title"><strong>DELIVERY NOTE</strong></p>' +
                "</header>" +
                '<div class="receipt-thermal-rule" role="presentation"></div>'
              );
            }
            var shop = receiptEsc(String(payload.shopName || "Point of Sale"));
            var co = (payload.companyName || "").trim();
            var coHtml = co ? '<p class="receipt-thermal-company">' + receiptEsc(co) + "</p>" : "";
            return (
              '<header class="receipt-thermal-brand">' +
              badge +
              '<h1 class="receipt-thermal-shopname">' +
              shop +
              "</h1>" +
              coHtml +
              "</header>" +
              '<div class="receipt-thermal-rule" role="presentation"></div>'
            );
          }

          function metaBlockHtml() {
            var rows = [];
            rows.push(["Receipt", payload.receiptNo]);
            if (S.show_datetime || isCompany) {
              rows.push(["Date", payload.printedAt]);
            }
            if (showCustomerBlock) {
              var modeStr = String(payload.mode || "");
              var isCredit = modeStr.toLowerCase().indexOf("credit") !== -1;
              if (modeStr && (isCredit || payload.isQuotation) && !isStockTransferReceipt(payload)) {
                rows.push(["Mode", modeStr]);
              }
              var dueT = (payload.creditDueDate || "").trim();
              if (dueT && isCredit) {
                rows.push(["Due", dueT]);
              }
              if (isStockTransferReceipt(payload)) {
                rows.push([
                  "To",
                  String(payload.transferToShop || payload.customerName || "—").trim(),
                ]);
              } else {
                var buyer =
                  String(payload.customerName || "").trim() +
                  (String(payload.customerPhone || "").trim()
                    ? " · " + String(payload.customerPhone).trim()
                    : "");
                rows.push(["Buyer", buyer || "—"]);
              }
            }
            if (isCompany) {
              rows.push(["Staff", String(payload.employeeName || "").trim() || "—"]);
              if (isStockTransferReceipt(payload)) {
                rows.push([
                  "To",
                  String(payload.transferToShop || payload.customerName || "—").trim(),
                ]);
              }
            } else if (S.show_server) {
              rows.push(["Staff", payload.employeeName]);
            }
            if (isStockTransferReceipt(payload)) {
              var delivered = String(payload.deliveredBy || "").trim();
              rows.push(["Delivered by", delivered || "____________"]);
            }
            return thermalKvBlockHtml(rows);
          }

          function headerAndContactHtml() {
            var parts = [];
            var hdr = (payload.receiptHeader || "").trim();
            if (hdr && !isCompany) {
              var hx = hdr.length > 120 ? hdr.slice(0, 118) + "…" : hdr;
              parts.push(
                '<div class="receipt-thermal-customhdr">' + receiptEsc(hx).replace(/\n/g, " ") + "</div>"
              );
            }
            if (!isCompany && (S.show_address || S.show_contact)) {
              var bits = [];
              if (S.show_address && (payload.shopLocation || "").trim()) {
                bits.push(payload.shopLocation.trim().slice(0, W + 8));
              }
              if (S.show_contact) {
                if ((site.company_phone || "").trim()) bits.push(site.company_phone.trim());
                if ((site.company_email || "").trim()) bits.push(site.company_email.trim());
              }
              if (bits.length) {
                parts.push(
                  '<p class="receipt-thermal-mut">' + receiptEsc(bits.join(" · ")) + "</p>"
                );
              }
            }
            return parts.length ? '<div class="receipt-thermal-contact">' + parts.join("") + "</div>" : "";
          }

          function quoteBannerHtml() {
            if (isCompany || !payload.isQuotation) return "";
            return (
              '<div class="receipt-thermal-quote" role="status">' +
              '<span class="receipt-thermal-quote__title">Quotation</span>' +
              '<span class="receipt-thermal-quote__sub">Not a finalized sale</span>' +
              "</div>"
            );
          }

          function reprintBannerHtml() {
            if (!payload.isReprint) return "";
            return (
              '<div class="receipt-thermal-reprint" role="status">' +
              '<span class="receipt-thermal-reprint__title">Reprinted receipt</span>' +
              '<span class="receipt-thermal-reprint__sub">This is a duplicate copy</span>' +
              "</div>"
            );
          }

          function totalsBlockHtml() {
            if (!showPrices || isCompany) return "";
            var rows = [];
            if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
              rows.push(["Discount", "−" + fmt(payload.discountTotal)]);
            }
            if (payload.includeTax) {
              rows.push(["Subtotal", fmt(payload.subtotal || 0)]);
              rows.push([
                "Tax",
                fmt(payload.taxAmount || 0) + " (" + String(payload.taxPercent || 0) + "%)",
              ]);
              rows.push([
                "Total",
                fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
                "receipt-kv__row--grand",
              ]);
            } else {
              rows.push(["Total", fmt(payload.subtotal || 0), "receipt-kv__row--grand"]);
            }
            return thermalKvBlockHtml(rows, "receipt-totals");
          }

          function paymentBlockHtml() {
            if (!showPrices) return "";
            var payType = String(payload.paymentTypeLabel || "").trim();
            if (!isReceiptTransactionPaymentLabel(payType)) return "";
            var method = receiptTxnPaymentMethodTitle(payType);
            var parsed = receiptPaymentDetailKvRows(payload.paymentDetailText || "");
            var parts = [];
            parts.push('<p class="receipt-payment__heading">PAID BY</p>');
            parts.push('<p class="receipt-payment__method">' + receiptEsc(method) + "</p>");
            if (parsed.rows.length) {
              parts.push('<div class="receipt-pay-lines receipt-body">');
              parsed.rows.forEach(function (row) {
                parts.push(
                  '<div class="receipt-pay-line"><span class="receipt-pay-line__label">' +
                    receiptEsc(row[0]) +
                    '</span><span class="receipt-pay-line__value">' +
                    receiptEsc(row[1]) +
                    "</span></div>"
                );
              });
              parts.push("</div>");
            }
            parsed.plain.forEach(function (ln) {
              parts.push(
                '<p class="receipt-payment__detail receipt-body">' + receiptEsc(ln) + "</p>"
              );
            });
            return '<div class="receipt-payment receipt-payment--txn">' + parts.join("") + "</div>";
          }

          function paymentInstructionBlockHtml() {
            if (!showPrices || isCompany || !receiptPaymentInstructionHasContent(S)) return "";
            var pt = receiptNormalizePaymentDetailType(S);
            if (pt === "paybill") {
              return receiptPaybillInstructionHtml(S);
            }
            var label = receiptPaymentInstructionLabel(S);
            var lines = receiptPaymentInstructionLines(S);
            if (!label || !lines.length) return "";
            var parts = [];
            parts.push('<p class="receipt-payment__heading">PAY VIA</p>');
            parts.push('<p class="receipt-payment__method">' + receiptEsc(label) + "</p>");
            lines.forEach(function (ln) {
              parts.push(
                '<p class="receipt-payment__detail receipt-body">' + receiptEsc(ln) + "</p>"
              );
            });
            return (
              '<div class="receipt-payment receipt-payment--instructions">' + parts.join("") + "</div>"
            );
          }

          function footerHtmlBlock() {
            var foot = (payload.receiptFooter || "").trim();
            if (!foot || isCompany) return "";
            return (
              '<footer class="receipt-thermal-footer">' +
              receiptEsc(foot).replace(/\n/g, "<br/>") +
              "</footer>"
            );
          }

          var logoUrl = ((payload.receiptLogoUrl || site.receipt_logo_url || site.app_icon_url) || "").trim();
          var logoHtml = "";
          if (S.show_logo && logoUrl) {
            logoHtml =
              '<div class="receipt-logo-row"><img src="' +
              String(logoUrl).replace(/"/g, "") +
              '" alt="" /></div>';
          }

          var qrHtml = "";
          if (!isCompany && receiptQrEnabled(S)) {
            var qrData = buildReceiptQrPayloadString(payload, S);
            if (qrData) {
              var enc = encodeURIComponent(qrData);
              qrHtml =
                '<div class="receipt-qr"><img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&amp;data=' +
                enc +
                '" alt="" width="140" height="140" /></div>' +
                '<p class="receipt-qr-caption">Scan for receipt details</p>';
            }
          }

          var rollCss = rollWmm === 58 ? "58mm" : "80mm";
          var thermalStyles = buildThermalReceiptStyles(rollCss, rollWmm, baseFont, monoFont, receiptWeight, titleWeight);

          /* ── Company copy: monospace body (no prices), after brand. ── */
          if (isCompany) {
            L(thermalPlainCenter(isStockTransferReceipt(payload) ? "Items transferred" : "Items sold", W));
            L(sep);
            (payload.lines || []).forEach(function (l) {
              var name = String((l && l.name) || "Item");
              if (l && l.discounted) name = name + " (DISCOUNTED)";
              L(name.slice(0, W - 4) + "  x" + fmtQty((l && l.qty) || 0));
            });
            var plainCompany = lines.join("\n");
            var rollCssC = rollWmm === 58 ? "58mm" : "80mm";
            var thermalStylesC = buildThermalReceiptStyles(rollCssC, rollWmm, baseFont, monoFont, receiptWeight, titleWeight);
            return (
              "<!doctype html><html><head><meta charset='utf-8'><title>Receipt</title>" +
              "<style>" +
              thermalStylesC +
              "</style></head><body>" +
              '<div class="receipt-thermal">' +
              '<div class="receipt-thermal-accent" aria-hidden="true"></div>' +
              "<div class=\"receipt-browser-print-tip\" role=\"status\"><strong>Print:</strong> Choose your physical printer under <strong>Destination</strong>, not Save as PDF.</div>" +
              logoHtml +
              brandBlockHtml() +
              metaBlockHtml() +
              "<pre class=\"receipt receipt--block\">" +
              receiptEsc(plainCompany) +
              "</pre>" +
              qrHtml +
              receiptTailHtml(true) +
              "</div></body></html>"
            );
          }

          var itemsHtml = isStockTransferReceipt(payload)
            ? thermalTransferItemsTableHtml(payload.lines || [])
            : thermalSaleItemsTableHtml(payload.lines || []);
          var transferReceipt = isStockTransferReceipt(payload);

          return (
            "<!doctype html><html><head><meta charset='utf-8'><title>Receipt</title>" +
            "<style>" +
            thermalStyles +
            "</style></head><body>" +
            '<div class="receipt-thermal">' +
            '<div class="receipt-thermal-accent" aria-hidden="true"></div>' +
            "<div class=\"receipt-browser-print-tip\" role=\"status\"><strong>Print:</strong> Choose your physical printer under <strong>Destination</strong>, not Save as PDF.</div>" +
            logoHtml +
            brandBlockHtml() +
            headerAndContactHtml() +
            quoteBannerHtml() +
            reprintBannerHtml() +
            metaBlockHtml() +
            itemsHtml +
            (transferReceipt ? "" : totalsBlockHtml()) +
            (transferReceipt ? "" : paymentBlockHtml()) +
            (transferReceipt ? "" : paymentInstructionBlockHtml()) +
            footerHtmlBlock() +
            qrHtml +
            receiptTailHtml(isCompany) +
            "</div></body></html>"
          );
        }
  function thermalReceiptDocExtractStyle(fullHtml) {
          var m = String(fullHtml || "").match(/<style>([\s\S]*?)<\/style>/i);
          return m ? m[1] : "";
        }
  function thermalReceiptDocExtractBodyInner(fullHtml) {
          var m = String(fullHtml || "").match(/<body[^>]*>([\s\S]*?)<\/body>/i);
          return m ? m[1].trim() : "";
        }
  function printReceiptInIframe(html, layout, contentWidthPx, widthMm) {
          layout = layout === "thermal" ? "thermal" : "normal";
          return new Promise(function (resolve) {
            try {
              var iframe = document.createElement("iframe");
              iframe.setAttribute("aria-hidden", "true");
              iframe.style.position = "fixed";
              iframe.style.top = "0";
              iframe.style.left = "-9999px";
              iframe.style.border = "0";
              iframe.style.margin = "0";
              iframe.style.padding = "0";
              iframe.style.opacity = "0";
              iframe.style.pointerEvents = "none";
              iframe.style.zIndex = "-1";
              var wPx =
                contentWidthPx != null && !isNaN(contentWidthPx)
                  ? contentWidthPx
                  : layout === "normal"
                    ? 816
                    : 302;
              if (layout === "thermal" && widthMm != null && !isNaN(Number(widthMm))) {
                var mm = Number(widthMm);
                iframe.style.width = mm + "mm";
                iframe.style.minWidth = mm + "mm";
                iframe.style.maxWidth = mm + "mm";
              } else {
                iframe.style.width = wPx + "px";
                iframe.style.minWidth = "";
                iframe.style.maxWidth = "";
              }
              iframe.style.height = "200px";
              document.body.appendChild(iframe);
              var doc = iframe.contentWindow.document;
              if (layout === "thermal") {
                var engPrep = window.receiptThermalEngine;
                if (engPrep && typeof engPrep.prepareHtmlForPrint === "function") {
                  html = engPrep.prepareHtmlForPrint(html);
                }
              }
              doc.open();
              doc.write(html);
              doc.close();
              setTimeout(function () {
                waitForIframeImages(
                  doc,
                  function () {
                    var h = 400;
                    try {
                      var b = doc.body;
                      var el = doc.documentElement;
                      if (layout === "thermal" && b) {
                        h = Math.ceil((b.scrollHeight || b.offsetHeight || 0) + 8);
                      } else {
                        h = Math.max(
                          b ? b.scrollHeight : 0,
                          b ? b.offsetHeight : 0,
                          el ? el.scrollHeight : 0,
                          el ? el.offsetHeight : 0
                        );
                      }
                    } catch (e) {}
                    if (!h || h < 48) h = layout === "normal" ? 600 : 320;
                    h = Math.min(h + 16, layout === "normal" ? 1600 : 2400);
                    iframe.style.height = h + "px";
                    setTimeout(function () {
                      try {
                        iframe.contentWindow.focus();
                        iframe.contentWindow.print();
                      } catch (e) {}
                      setTimeout(function () {
                        try {
                          iframe.remove();
                        } catch (e) {}
                        resolve();
                      }, 400);
                    }, layout === "normal" ? 120 : 100);
                  },
                  5000
                );
              }, layout === "normal" ? 200 : 150);
            } catch (e) {
              resolve();
            }
          });
        }
  function printThermalReceipt(payload, variant) {
          syncReceiptThermalEngineBoot();
          var rwmm = receiptRollWidthMm();
          var wpx = Math.round((rwmm * 96) / 25.4);
          var buildFn =
            window.receiptThermalEngine && typeof window.receiptThermalEngine.buildPlainHtml === "function"
              ? window.receiptThermalEngine.buildPlainHtml
              : buildThermalReceiptPlainHtml;
          return printReceiptInIframe(buildFn(payload, variant), "thermal", wpx, rwmm);
        }
  function printThermalReceiptMulti(payload, variants) {
          syncReceiptThermalEngineBoot();
          var buildFn =
            window.receiptThermalEngine && typeof window.receiptThermalEngine.buildPlainHtml === "function"
              ? window.receiptThermalEngine.buildPlainHtml
              : buildThermalReceiptPlainHtml;
          var rwmm = receiptRollWidthMm();
          var wpx = Math.round((rwmm * 96) / 25.4);
          var vars = Array.isArray(variants) && variants.length ? variants.slice() : ["customer"];
          if (vars.length === 1) return printThermalReceipt(payload, vars[0]);
          var firstHtml = buildFn(payload, vars[0]);
          var style = thermalReceiptDocExtractStyle(firstHtml);
          var chunks = vars.map(function (v) {
            return thermalReceiptDocExtractBodyInner(buildFn(payload, v));
          });
          var combinedInner = chunks
            .map(function (inner, idx) {
              var pba = idx < chunks.length - 1 ? "page-break-after:always;break-after:page;" : "";
              return '<section class="rc-multi-slip" style="' + pba + '">' + inner + "</section>";
            })
            .join("");
          var rollCss = rwmm === 58 ? "58mm" : "80mm";
          var html =
            "<!doctype html><html><head><meta charset='utf-8'><title>Receipts</title>" +
            "<style>" +
            style +
            ".rc-multi-slip{display:block}" +
            "</style></head><body>" +
            combinedInner +
            "</body></html>";
          return printReceiptInIframe(html, "thermal", wpx, rwmm);
        }
  function receiptVariantsForCheckout() {
          var pp = getPrinting() || {};
          var rc = parseInt(String(pp.receipt_copies != null ? pp.receipt_copies : "1").trim(), 10);
          if (isNaN(rc) || rc < 1) rc = 1;
          if (rc >= 3) return ["customer", "company", "cashier"];
          if (rc === 2) return ["customer", "company"];
          return ["customer"];
        }

  function buildPersistedReprintPayload(sale, items) {
    var payload = buildPersistedReceiptPayload(sale, items);
    payload.isReprint = true;
    return payload;
  }

  function buildReceiptThermalHtmlMulti(payload, variants) {
    var vars = Array.isArray(variants) && variants.length ? variants.slice() : ["customer"];
    if (vars.length === 1) return buildThermalReceiptPlainHtml(payload, vars[0]);
    var firstHtml = buildThermalReceiptPlainHtml(payload, vars[0]);
    var style = thermalReceiptDocExtractStyle(firstHtml);
    var chunks = vars.map(function (v) {
      return thermalReceiptDocExtractBodyInner(buildThermalReceiptPlainHtml(payload, v));
    });
    var combinedInner = chunks
      .map(function (inner, idx) {
        var pba = idx < chunks.length - 1 ? "page-break-after:always;break-after:page;" : "";
        return '<section class="rc-multi-slip" style="' + pba + '">' + inner + "</section>";
      })
      .join("");
    return (
      "<!doctype html><html><head><meta charset='utf-8'><title>Receipts</title>" +
      "<style>" +
      style +
      ".rc-multi-slip{display:block}" +
      "</style></head><body>" +
      combinedInner +
      "</body></html>"
    );
  }

  var RECEIPT_PREVIEW_PRINT_CSS = (
    ".receipt-browser-print-tip{display:none!important}" +
    "html,body{margin:0!important;padding:0!important;width:100%!important;max-width:100%!important;overflow-x:hidden!important}" +
    ".receipt-thermal{width:100%!important;max-width:100%!important;padding-left:1mm!important;padding-right:1mm!important}" +
    ".receipt-items,.receipt-kv,.receipt-totals,.receipt-payment,.receipt-tail,.receipt-items-wrap{width:100%!important;max-width:100%!important}"
  );

  function receiptThermalPrepareHtmlForPrint(fullHtml) {
    return String(fullHtml || "").replace("</style>", RECEIPT_PREVIEW_PRINT_CSS + "</style>");
  }

  function buildDemoReceiptPreviewPayload(opts) {
    opts = opts || {};
    var rs = opts.receiptSettings || {};
    var site = opts.site || {};
    var demoLines = [
      { name: "Coffee · Regular Size Large Cup", qty: 2, price: 4.5, total: 9.0 },
      { name: "Bread · Whole Wheat Loaf Fresh", qty: 1, price: 2.0, total: 2.0 },
    ];
    var subtotal = 11.0;
    var taxPct = parseFloat(String(rs.tax_percent || "0").replace(",", ".")) || 0;
    var includeTax = taxPct > 0.000001 && !!rs.include_tax;
    var taxAmt = includeTax ? round2(subtotal * (taxPct / 100)) : 0;
    var grand = round2(subtotal + taxAmt);
    var prefix = String(rs.receipt_number_prefix || "T").trim() || "T";
    var startNum = String(rs.starting_number || "1001").trim() || "1001";
    var receiptNo = prefix + "-" + startNum;
    var company = String(site.company_name || "").trim();
    return {
      receiptNo: receiptNo,
      printedAt: new Date().toLocaleString(),
      shopName: String(site.shop_name || company || "Demo Shop"),
      companyName: company,
      shopCode: String(site.shop_code || "").trim(),
      shopLocation: String(site.shop_location || site.company_location_name || "").trim(),
      receiptLogoUrl: String(site.receipt_logo_url || site.app_icon_url || "").trim(),
      mode: "Sale",
      isQuotation: false,
      isReprint: false,
      customerName: "Walk-in customer",
      customerPhone: "-",
      employeeName: "Jane Doe",
      employeeCode: "123456",
      lines: demoLines,
      subtotal: subtotal,
      discountTotal: 0,
      hasDiscount: false,
      taxAmount: taxAmt,
      taxPercent: taxPct,
      grandTotal: grand,
      includeTax: includeTax,
      paymentTypeLabel: "Payment: Cash",
      paymentDetailText: "Cash: " + fmt(grand),
      receiptHeader: String(rs.receipt_header || "").trim(),
      receiptFooter: String(rs.receipt_footer || "").trim(),
      creditDueDate: "",
    };
  }

  window.receiptThermalRenderSettingsPreview = function (boot, variant) {
    ACTIVE_BOOT = boot || {};
    var payload = buildDemoReceiptPreviewPayload(boot);
    var fullHtml = buildThermalReceiptPlainHtml(payload, variant || "customer");
    return {
      widthMm: receiptRollWidthMm(),
      fullHtml: fullHtml,
      style: thermalReceiptDocExtractStyle(fullHtml),
      body: thermalReceiptDocExtractBodyInner(fullHtml),
    };
  };

  /** Shared thermal receipt engine — POS checkout uses the same HTML layout as IT preview. */
  window.receiptThermalEngine = {
    setBoot: function (boot) { ACTIVE_BOOT = boot || {}; },
    buildPlainHtml: buildThermalReceiptPlainHtml,
    buildHtmlMulti: buildReceiptThermalHtmlMulti,
    variantsForCheckout: receiptVariantsForCheckout,
    docExtractStyle: thermalReceiptDocExtractStyle,
    docExtractBody: thermalReceiptDocExtractBodyInner,
    prepareHtmlForPrint: receiptThermalPrepareHtmlForPrint,
  };

  window.receiptThermalReprint = function (sale, items, boot) {
    ACTIVE_BOOT = boot || {};
    var payload = buildPersistedReprintPayload(sale, items);
    var variants = receiptVariantsForCheckout();
    return printThermalReceiptMulti(payload, variants);
  };

  /**
   * Hidden embed iframe entry (shop/IT receipt register auto_print=1).
   * Uses the same thermal HTML layout as settings preview + POS checkout.
   */
  window.receiptThermalEmbedAutoPrint = function (data) {
    data = data || {};
    var sale = data.sale || {};
    var items = data.items || [];
    var boot = data.boot || {};
    if (!sale || !items || !items.length) return Promise.resolve();
    var printed = window.receiptThermalReprint(sale, items, boot);
    var recordUrl = String(data.reprintRecordUrl || "").trim();
    if (recordUrl) {
      try {
        var body = {
          sale_id: parseInt(data.saleId || sale.id || 0, 10) || 0,
        };
        var shopId = parseInt(data.shopId || sale.shop_id || 0, 10) || 0;
        if (shopId) body.shop_id = shopId;
        fetch(recordUrl, {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(body),
        }).catch(function () {});
      } catch (e) {}
    }
    return printed;
  };
})();
