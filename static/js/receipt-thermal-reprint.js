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
  var RECEIPT_ATTRIBUTION_NAME = "FINAGRITECH SOLUTIONS";
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
          return normalizeReceiptWidthKey(receiptSettings().receipt_width) === "58mm" ? 24 : 32;
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
          if (pm === "cash") {
            paymentTypeLabel = "Payment: Cash";
            paymentDetailText = "Cash: " + fmt(saleObj.cash_amount || saleObj.total_amount || 0);
          } else if (pm === "mpesa") {
            paymentTypeLabel = "Payment: M-Pesa";
            paymentDetailText = "M-Pesa: " + fmt(saleObj.mpesa_amount || saleObj.total_amount || 0);
          } else if (pm === "both") {
            paymentTypeLabel = "Payment: Cash + M-Pesa";
            paymentDetailText = "Cash: " + fmt(saleObj.cash_amount || 0) + " | M-Pesa: " + fmt(saleObj.mpesa_amount || 0);
          } else if (String(saleObj.sale_type || "") === "credit") {
            paymentTypeLabel = "Payment: Credit";
            paymentDetailText = "Pending payment";
          }
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
  function thermalItemHeaderLine(W) {
          var qtyW = 4;
          var amtW = 9;
          var right = " " + "Qty".padStart(qtyW, " ") + " " + "Amt".padStart(amtW, " ");
          var nameW = W - right.length;
          if (nameW < 6) nameW = 6;
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
          var qtyW = 4;
          var amtW = 9;
          var qty = fmtQty((l && l.qty) != null ? l.qty : 0);
          var amt = fmt((l && l.total) || 0);
          var right = " " + qty.padStart(qtyW, " ") + " " + amt.padStart(amtW, " ");
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
          if (isCompany) {
            return (
              thermalPlainCenter("Company records", W) +
              "\n" +
              thermalPlainCenter(RECEIPT_ATTRIBUTION_NAME, W)
            );
          }
          return (
            thermalPlainCenter("Thank you", W) +
            "\n" +
            thermalPlainCenter(RECEIPT_ATTRIBUTION_BY, W) +
            "\n" +
            thermalPlainCenter(RECEIPT_ATTRIBUTION_NAME, W)
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
          var lines = [];
          function L(t) {
            lines.push(String(t == null ? "" : t));
          }

          function buildThermalReceiptStyles(rollCss, rwmm, fontCss) {
            return (
              "@page{margin:0;size:" +
              rollCss +
              " auto}" +
              "*{box-sizing:border-box}" +
              "html{-webkit-print-color-adjust:exact;print-color-adjust:exact}" +
              "html,body{margin:0!important;padding:0!important;background:#fff;color:#111;" +
              "width:" +
              rollCss +
              ";min-width:" +
              rollCss +
              ";max-width:" +
              rollCss +
              "}" +
              ".receipt-thermal{padding:0 1.5mm 2mm;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:" +
              fontCss +
              ";line-height:1.22}" +
              ".receipt-thermal-accent{height:2mm;margin:0 0 1.5mm;background:linear-gradient(90deg,#1d4ed8,#2563eb,#1d4ed8)}" +
              ".receipt-logo-row{text-align:center;margin:0 auto 1mm;padding:0 1mm}" +
              ".receipt-logo-row img{display:block;margin:0 auto;max-height:10mm;max-width:68mm;height:auto;width:auto;object-fit:contain}" +
              ".receipt-thermal-brand{text-align:center;padding:0 0 0.5mm}" +
              ".receipt-thermal-badge{margin:0 0 1mm;padding:1px 6px;display:inline-block;border-radius:999px;font-size:7.5px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase}" +
              ".receipt-thermal-badge--company{background:#f3f4f6;color:#374151;border:1px solid #d1d5db}" +
              ".receipt-thermal-badge--cashier{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe}" +
              ".receipt-thermal-shopname{margin:0;font-size:clamp(11px,3.2mm,15px);font-weight:900;letter-spacing:-0.02em;line-height:1.12;color:#1e3a8a}" +
              ".receipt-thermal-company{margin:1px 0 0;font-size:9px;font-weight:600;color:#4b5563}" +
              ".receipt-thermal-rule{height:0;border:none;border-top:1px solid #d1d5db;margin:0 0 1.5mm}" +
              ".receipt-thermal-contact{margin:0 0 1mm;text-align:center}" +
              ".receipt-thermal-customhdr{margin:0 0 1mm;font-size:8.5px;line-height:1.25;color:#000;font-weight:600;white-space:pre-wrap}" +
              ".receipt-thermal-mut{margin:0;font-size:8px;line-height:1.2;color:#000;font-weight:600}" +
              ".receipt-thermal-placeholder{font-style:italic}" +
              ".receipt-thermal-quote{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #94a3b8;background:#f8fafc;text-align:center}" +
              ".receipt-thermal-quote__title{display:block;font-size:10px;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#0f172a}" +
              ".receipt-thermal-quote__sub{display:block;margin-top:2px;font-size:9.5px;color:#64748b}" +
              ".receipt-thermal-reprint{margin:0 0 2.5mm;padding:2.5mm 2mm;border-radius:6px;border:1px dashed #f59e0b;background:#fffbeb;text-align:center}" +
              ".receipt-thermal-reprint__title{display:block;font-size:10px;font-weight:900;letter-spacing:0.14em;text-transform:uppercase;color:#92400e}" +
              ".receipt-thermal-reprint__sub{display:block;margin-top:2px;font-size:9.5px;color:#b45309}" +
              ".receipt-thermal-stock-transfer__title{margin:0 0 2px;font-size:11px;font-weight:900;letter-spacing:0.1em;text-transform:uppercase;color:#0f172a}" +
              ".receipt-thermal-stock-transfer__sub{margin:0;font-size:9.5px;font-weight:700;line-height:1.25;color:#1e3a8a;text-transform:none}" +
              "pre.receipt{margin:0;width:100%;max-width:100%;font-family:'Cascadia Mono','Courier New',Consolas,'Liberation Mono',monospace;font-size:9.5px;font-weight:700;line-height:1.35;white-space:pre-wrap;word-wrap:break-word;overflow:visible;color:#000}" +
              "pre.receipt--meta,pre.receipt--payment,pre.receipt--totals{margin:0;padding:0.8mm 0;border-top:1px solid #000;border-bottom:1px solid #000;color:#000;font-weight:700}" +
              "pre.receipt--items{margin:0;padding:0.8mm 0 0;border-top:1px solid #000}" +
              "pre.receipt--block{margin:1mm 0 0;padding:1mm 0 0;border-top:1px dashed #d1d5db}" +
              "pre.receipt--tail{margin:1mm 0 0;padding:1mm 0 0;border-top:1px solid #e5e7eb}" +
              ".receipt-thermal-footer{margin:0 0 1mm;padding:1mm 0 0;text-align:center;font-size:8px;line-height:1.25;color:#000;font-weight:600;border-top:1px dashed #000}" +
              ".receipt-qr{display:flex;flex-direction:column;align-items:center;width:100%;margin:0.5mm 0 0;padding:1mm 1mm 0;min-height:22mm}" +
              ".receipt-qr img{display:block;width:22mm;height:22mm;max-width:72%;object-fit:contain}" +
              ".receipt-qr-caption{margin:0.5mm 0 0;padding:0;width:100%;text-align:center;font-size:7.5px;line-height:1.25;color:#6b7280}" +
              ".receipt-browser-print-tip{margin:0 0 1.5mm;padding:1.5mm 2mm;border:1px solid #fcd34d;background:linear-gradient(180deg,#fffbeb,#fff7ed);color:#92400e;font-size:8px;line-height:1.3;border-radius:4px}" +
              "@media print{" +
              "@page{margin:0;size:" +
              rollCss +
              " auto}" +
              ".receipt-browser-print-tip{display:none!important}" +
              "html,body{width:" +
              rollCss +
              "!important;min-width:" +
              rollCss +
              "!important;max-width:" +
              rollCss +
              "!important}" +
              "pre.receipt,pre.receipt--meta,pre.receipt--items,pre.receipt--totals,pre.receipt--payment{font-size:" +
              (rwmm === 58 ? "8.5" : "9.5") +
              "pt!important;font-weight:700!important;color:#000!important}" +
              ".receipt-thermal-mut,.receipt-thermal-customhdr,.receipt-thermal-footer,.receipt-thermal-company{color:#000!important;font-weight:700!important}" +
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

          function metaPlainPreHtml() {
            var metaLines = [];
            metaLines.push(thermalMetaLabelLine("RECEIPT", payload.receiptNo, W));
            if (S.show_datetime || isCompany) {
              metaLines.push(thermalMetaLabelLine("DATE", payload.printedAt, W));
            }
            if (showCustomerBlock) {
              var modeStr = String(payload.mode || "");
              var isCredit = modeStr.toLowerCase().indexOf("credit") !== -1;
              if (modeStr && (isCredit || payload.isQuotation) && !isStockTransferReceipt(payload)) {
                metaLines.push(thermalMetaLabelLine("MODE", modeStr, W));
              }
              var dueT = (payload.creditDueDate || "").trim();
              if (dueT && isCredit) {
                metaLines.push(thermalMetaLabelLine("DUE", dueT, W));
              }
              if (isStockTransferReceipt(payload)) {
                metaLines.push(
                  thermalMetaLabelLine(
                    "TO",
                    String(payload.transferToShop || payload.customerName || "—").trim(),
                    W
                  )
                );
              } else {
                var buyer =
                  String(payload.customerName || "").trim() +
                  (String(payload.customerPhone || "").trim() ? " · " + String(payload.customerPhone).trim() : "");
                metaLines.push(thermalMetaLabelLine("BUYER", buyer || "—", W));
              }
            }
            if (isCompany) {
              metaLines.push(
                thermalMetaLabelLine("STAFF", String(payload.employeeName || "").trim() || "—", W)
              );
              if (isStockTransferReceipt(payload)) {
                metaLines.push(
                  thermalMetaLabelLine(
                    "TO",
                    String(payload.transferToShop || payload.customerName || "—").trim(),
                    W
                  )
                );
              }
            } else if (S.show_server) {
              metaLines.push(thermalMetaLabelLine("STAFF", payload.employeeName, W));
            }
            if (isStockTransferReceipt(payload)) {
              metaLines.push(thermalDeliveredByLine(payload.deliveredBy, W));
            }
            if (!metaLines.length) return "";
            return (
              '<pre class="receipt receipt--meta">' + receiptEsc(metaLines.join("\n")) + "</pre>"
            );
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

          function totalsPlainPreHtml() {
            if (!showPrices || isCompany) return "";
            var totalLines = [];
            if (payload.hasDiscount && (payload.discountTotal || 0) > 0.001) {
              totalLines.push(
                thermalMetaLabelLine("DISCOUNT", "−" + fmt(payload.discountTotal), W)
              );
            }
            if (payload.includeTax) {
              totalLines.push(thermalMetaLabelLine("SUBTOTAL", fmt(payload.subtotal || 0), W));
              totalLines.push(
                thermalMetaLabelLine("TAX", fmt(payload.taxAmount || 0) + " (" + String(payload.taxPercent || 0) + "%)", W)
              );
              totalLines.push(
                thermalMetaLabelLine(
                  "TOTAL",
                  fmt(payload.grandTotal != null ? payload.grandTotal : payload.subtotal || 0),
                  W
                )
              );
            } else {
              totalLines.push(thermalMetaLabelLine("TOTAL", fmt(payload.subtotal || 0), W));
            }
            return (
              '<pre class="receipt receipt--totals">' + receiptEsc(totalLines.join("\n")) + "</pre>"
            );
          }

          function paymentPlainPreHtml() {
            if (!showPrices) return "";
            var payType = String(payload.paymentTypeLabel || "").trim();
            var pd = (payload.paymentDetailText || "").trim();
            if (!payType && !pd) return "";
            var payLines = [];
            if (payType) payLines.push(payType);
            if (pd) {
              pd.split(/\r?\n/).forEach(function (ln) {
                if (String(ln).trim()) payLines.push(String(ln).trim());
              });
            }
            return (
              '<pre class="receipt receipt--payment">' + receiptEsc(payLines.join("\n")) + "</pre>"
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

          var tailPlain = heavy + "\n" + receiptAttributionTailPlain(isCompany, W);
          var escapedTail = receiptEsc(tailPlain);

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
            var thermalStylesC = buildThermalReceiptStyles(rollCssC, rollWmm, baseFont);
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
              metaPlainPreHtml() +
              "<pre class=\"receipt receipt--block\">" +
              receiptEsc(plainCompany) +
              "</pre>" +
              qrHtml +
              "<pre class=\"receipt receipt--tail\">" +
              escapedTail +
              "</pre>" +
              "</div></body></html>"
            );
          }

          /* ── Customer / cashier: hybrid layout. ── */
          lines.length = 0;
          if (isStockTransferReceipt(payload)) {
            L(thermalTransferItemHeaderLine(W));
            L(sep);
            (payload.lines || []).forEach(function (l) {
              thermalTransferItemLines(l, W).forEach(function (row) {
                L(row);
              });
            });
          } else {
            L(thermalItemHeaderLine(W));
            L(sep);
            (payload.lines || []).forEach(function (l) {
              thermalItemLines(l, W).forEach(function (row) {
                L(row);
              });
            });
          }

          var itemsPlain = lines.join("\n");
          var rollCss = rollWmm === 58 ? "58mm" : "80mm";
          var thermalStyles = buildThermalReceiptStyles(rollCss, rollWmm, baseFont);
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
            metaPlainPreHtml() +
            '<pre class="receipt receipt--items">' +
            receiptEsc(itemsPlain) +
            "</pre>" +
            (transferReceipt ? "" : totalsPlainPreHtml()) +
            (transferReceipt ? "" : paymentPlainPreHtml()) +
            footerHtmlBlock() +
            qrHtml +
            "<pre class=\"receipt receipt--tail\">" +
            escapedTail +
            "</pre>" +
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
          var rwmm = receiptRollWidthMm();
          var wpx = Math.round((rwmm * 96) / 25.4);
          return printReceiptInIframe(buildThermalReceiptPlainHtml(payload, variant), "thermal", wpx, rwmm);
        }
  function printThermalReceiptMulti(payload, variants) {
          var rwmm = receiptRollWidthMm();
          var wpx = Math.round((rwmm * 96) / 25.4);
          var vars = Array.isArray(variants) && variants.length ? variants.slice() : ["customer"];
          if (vars.length === 1) return printThermalReceipt(payload, vars[0]);
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

  function recordEmbedReprint(d) {
    var recordUrl = d && d.reprintRecordUrl;
    if (!recordUrl) return;
    var body = { sale_id: parseInt((d.sale && d.sale.id) || d.saleId || 0, 10) || 0 };
    var shopId = parseInt(d.shopId || (d.sale && d.sale.shop_id) || 0, 10) || 0;
    if (shopId > 0) body.shop_id = shopId;
    fetch(recordUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body),
    }).catch(function () {});
  }

  window.receiptThermalEmbedAutoPrint = function (d) {
    ACTIVE_BOOT = (d && d.boot) || {};
    var payload = buildPersistedReprintPayload((d && d.sale) || {}, (d && d.items) || []);
    var html = buildReceiptThermalHtmlMulti(payload, receiptVariantsForCheckout());
    document.open();
    document.write(html);
    document.close();
    waitForIframeImages(
      document,
      function () {
        setTimeout(function () {
          try {
            window.focus();
            window.print();
          } catch (e) {}
          recordEmbedReprint(d);
        }, 120);
      },
      5000
    );
  };

  window.receiptThermalReprint = function (sale, items, boot) {
    ACTIVE_BOOT = boot || {};
    var payload = buildPersistedReprintPayload(sale, items);
    var variants = receiptVariantsForCheckout();
    return printThermalReceiptMulti(payload, variants);
  };
})();
