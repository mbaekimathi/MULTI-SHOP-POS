"""PDF builders for customer credit notes."""

from __future__ import annotations

import io
from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos

from report_pdf import (
    C_AMBER,
    C_BORDER,
    C_BRAND,
    C_EMERALD,
    C_MUTED,
    C_ROSE,
    C_ROW_ALT,
    C_SKY,
    C_SLATE,
    C_SURFACE,
    C_WHITE,
    _hex_to_rgb,
    _pdf_text,
)

C_TEAL = (15, 118, 110)
C_TEAL_SOFT = (240, 253, 250)


def _fmt_money(value: object) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"KES {amount:,.2f}"


def _qr_png_bytes(payload: str, *, box_size: int = 4) -> bytes | None:
    url = (payload or "").strip()
    if not url:
        return None
    try:
        import qrcode
        from qrcode.constants import ERROR_CORRECT_M

        qr = qrcode.QRCode(
            version=None,
            error_correction=ERROR_CORRECT_M,
            box_size=box_size,
            border=1,
        )
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        return None


class CreditNotePDF(FPDF):
    def __init__(self, *, brand_rgb: tuple[int, int, int] = C_BRAND) -> None:
        super().__init__(orientation="P", unit="mm", format="A4")
        self.brand_rgb = brand_rgb
        self.brand_dark = (
            max(0, brand_rgb[0] - 18),
            max(0, brand_rgb[1] - 20),
            max(0, brand_rgb[2] - 4),
        )
        self.content_w = 186.0
        self.set_auto_page_break(auto=True, margin=16)
        self.set_margins(12, 12, 12)

    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*C_MUTED)
        self.cell(0, 5, _pdf_text(f"Page {self.page_no()}"), align="C")

    def _accent_bar(self, x: float, y: float, w: float, h: float) -> None:
        colors = [C_EMERALD, (16, 185, 129), C_SKY]
        steps = 24
        step_w = w / steps
        for i in range(steps):
            t = i / max(steps - 1, 1)
            if t <= 0.5:
                blend = t / 0.5
                c0, c1 = colors[0], colors[1]
            else:
                blend = (t - 0.5) / 0.5
                c0, c1 = colors[1], colors[2]
            r = int(c0[0] + (c1[0] - c0[0]) * blend)
            g = int(c0[1] + (c1[1] - c0[1]) * blend)
            b = int(c0[2] + (c1[2] - c0[2]) * blend)
            self.set_fill_color(r, g, b)
            self.rect(x + i * step_w, y, step_w + 0.2, h, style="F")

    def _draw_logo(self, path: str, x: float, y: float, size: float = 20.0) -> bool:
        raw = (path or "").strip()
        if not raw:
            return False
        p = Path(raw)
        if not p.is_file():
            return False
        ext = p.suffix.lower()
        if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"):
            return False
        try:
            self.set_fill_color(*C_WHITE)
            self.set_draw_color(*C_BORDER)
            self.rect(x, y, size, size, style="DF", round_corners=True, corner_radius=2.2)
            pad = 1.0 if ext == ".svg" else 1.4
            self.image(str(p), x=x + pad, y=y + pad, w=size - pad * 2, h=size - pad * 2)
            return True
        except Exception:
            return False

    def _draw_qr(self, png_bytes: bytes | None, x: float, y: float, size: float = 24.0) -> bool:
        if not png_bytes:
            return False
        try:
            self.set_fill_color(*C_WHITE)
            self.set_draw_color(*C_BORDER)
            self.rect(x, y, size, size + 5.5, style="DF", round_corners=True, corner_radius=2.2)
            self.image(io.BytesIO(png_bytes), x=x + 1.6, y=y + 1.6, w=size - 3.2, h=size - 3.2)
            self.set_xy(x, y + size - 0.1)
            self.set_font("Helvetica", "B", 5.5)
            self.set_text_color(*C_MUTED)
            self.cell(size, 4.2, _pdf_text("SCAN WEBSITE"), align="C")
            return True
        except Exception:
            return False

    def _pay_button(self, *, label: str, url: str, y: float | None = None) -> float:
        """Draw a simple clickable button; returns the Y after the button."""
        link = (url or "").strip()
        if not link:
            return self.get_y()
        y0 = self.get_y() if y is None else y
        btn_w = 58.0
        btn_h = 10.0
        btn_x = 12 + (self.content_w - btn_w) / 2.0
        self.set_fill_color(*C_EMERALD)
        self.set_draw_color(*C_EMERALD)
        self.rect(btn_x, y0, btn_w, btn_h, style="DF", round_corners=True, corner_radius=2.5)
        self.set_xy(btn_x, y0 + 2.4)
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*C_WHITE)
        self.cell(btn_w, 5.2, _pdf_text(label), align="C", link=link)
        # Invisible link hit-area covering the whole button
        self.link(btn_x, y0, btn_w, btn_h, link)
        return y0 + btn_h

    def _letterhead(
        self,
        *,
        company_name: str,
        shop_name: str,
        credit_note_ref: str,
        subtitle: str,
        company_phone: str = "",
        company_email: str = "",
        company_location: str = "",
        logo_path: str = "",
        qr_png: bytes | None = None,
    ) -> None:
        y0 = self.get_y()
        head_h = 36.0
        self.set_fill_color(*C_TEAL_SOFT)
        self.set_draw_color(*C_BORDER)
        self.rect(12, y0, self.content_w, head_h, style="DF", round_corners=True, corner_radius=2.5)
        self._accent_bar(12, y0, self.content_w, 2.4)

        logo_size = 22.0
        text_x = 16.0
        if self._draw_logo(logo_path, 15.0, y0 + 7.0, logo_size):
            text_x = 15.0 + logo_size + 3.8
        else:
            # Fallback monogram tile so the header never looks empty
            self.set_fill_color(*C_TEAL)
            self.rect(15.0, y0 + 7.0, logo_size, logo_size, style="F", round_corners=True, corner_radius=2.2)
            self.set_xy(15.0, y0 + 13.5)
            self.set_font("Helvetica", "B", 14)
            self.set_text_color(*C_WHITE)
            initial = (company_name or shop_name or "C")[:1].upper()
            self.cell(logo_size, 8, _pdf_text(initial), align="C")
            text_x = 15.0 + logo_size + 3.8

        qr_size = 24.0
        qr_x = 12 + self.content_w - qr_size - 3.5
        has_qr = self._draw_qr(qr_png, qr_x, y0 + 5.0, qr_size)
        title_right = qr_x - 5 if has_qr else (12 + self.content_w - 4)

        self.set_xy(text_x, y0 + 6.5)
        self.set_font("Helvetica", "B", 7)
        self.set_text_color(*C_MUTED)
        self.cell(max(20, title_right - text_x - 44), 3.5, _pdf_text((company_name or "Company").upper()))

        self.set_xy(text_x, y0 + 10.8)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*C_SLATE)
        self.cell(max(20, title_right - text_x - 44), 6, _pdf_text(shop_name or "Shop"))

        detail_bits = [b for b in (company_location, company_phone, company_email) if (b or "").strip()]
        detail_y = y0 + 18.0
        self.set_font("Helvetica", "", 7.5)
        self.set_text_color(*C_MUTED)
        for line in detail_bits[:3]:
            self.set_xy(text_x, detail_y)
            self.cell(max(20, title_right - text_x - 44), 3.5, _pdf_text(line))
            detail_y += 3.5

        type_w = 40.0
        type_x = title_right - type_w
        if type_x < text_x + 36:
            type_x = text_x + 36
        self.set_xy(type_x, y0 + 6.5)
        self.set_font("Helvetica", "B", 6)
        self.set_text_color(*C_MUTED)
        self.cell(type_w, 3, _pdf_text("STATEMENT"), align="R")
        self.set_xy(type_x, y0 + 10.2)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*C_TEAL)
        self.cell(type_w, 5.5, _pdf_text("CREDIT NOTE"), align="R")
        self.set_xy(type_x, y0 + 16.5)
        self.set_font("Helvetica", "", 7.5)
        self.set_text_color(*C_MUTED)
        self.cell(type_w, 3.5, _pdf_text(f"Ref {credit_note_ref}"), align="R")
        if subtitle:
            self.set_xy(type_x, y0 + 20.5)
            self.set_font("Helvetica", "", 7)
            self.cell(type_w, 3.5, _pdf_text(subtitle), align="R")

        self.set_y(y0 + head_h + 4)

    def _kpi_card(
        self,
        x: float,
        y: float,
        w: float,
        h: float,
        label: str,
        value: str,
        accent: tuple[int, int, int],
        value_color: tuple[int, int, int],
    ) -> None:
        self.set_fill_color(*C_WHITE)
        self.set_draw_color(*C_BORDER)
        self.rect(x, y, w, h, style="DF", round_corners=True, corner_radius=2.5)
        self.set_fill_color(*accent)
        self.rect(x, y, w, 2.8, style="F", round_corners=True, corner_radius=2.5)
        self.set_xy(x + 3, y + 5)
        self.set_font("Helvetica", "B", 6.5)
        self.set_text_color(*C_MUTED)
        self.cell(w - 6, 3.5, _pdf_text(label.upper()))
        self.set_xy(x + 3, y + 9)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*value_color)
        self.cell(w - 6, 6, _pdf_text(value))

    def _meta_row(self, label: str, value: str) -> None:
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*C_MUTED)
        self.cell(28, 5, _pdf_text(label))
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*C_SLATE)
        self.cell(0, 5, _pdf_text(value), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def _section_title(self, title: str, subtitle: str = "") -> None:
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*C_SLATE)
        self.cell(0, 6, _pdf_text(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if subtitle:
            self.set_font("Helvetica", "", 8)
            self.set_text_color(*C_MUTED)
            self.cell(0, 4, _pdf_text(subtitle), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def _table_header(self, headers: list[str], col_w: list[float]) -> None:
        self.set_fill_color(*C_TEAL)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 8)
        self.set_draw_color(*C_TEAL)
        for idx, label in enumerate(headers):
            align = "R" if idx and idx >= len(headers) - 2 else "L"
            if label.lower() in ("qty", "amount", "unit", "line total", "revenue"):
                align = "R"
            self.cell(col_w[idx], 7.5, _pdf_text(label), border=1, align=align, fill=True)
        self.ln()

    def _table_row(
        self,
        values: list[str],
        col_w: list[float],
        *,
        zebra: bool,
        align_right_from: int = 1,
    ) -> None:
        self.set_fill_color(*(C_ROW_ALT if zebra else C_WHITE))
        self.set_draw_color(*C_BORDER)
        self.set_font("Helvetica", "", 7.5)
        for idx, value in enumerate(values):
            if idx == 0:
                self.set_font("Helvetica", "B", 7.5)
                self.set_text_color(*C_SLATE)
            elif idx >= len(values) - 1:
                self.set_font("Helvetica", "B", 7.5)
                self.set_text_color(*C_SLATE)
            else:
                self.set_font("Helvetica", "", 7.5)
                self.set_text_color(71, 85, 105)
            align = "R" if idx >= align_right_from else "L"
            self.cell(col_w[idx], 6.5, _pdf_text(value), border=1, align=align, fill=True)
        self.ln()


def build_credit_note_pdf(
    *,
    company_name: str,
    shop_name: str,
    credit_note_ref: str,
    period_label: str,
    all_time: bool = False,
    scope_label: str = "",
    customer_name: str,
    customer_phone: str,
    balance_due: float,
    outstanding: float,
    paid_to_date: float,
    unpaid_sales_count: int,
    unpaid_items: list[dict],
    company_scope: bool = False,
    pay_link: str = "",
    focus_sale: dict | None = None,
    generated_at: str = "",
    primary_color: str | None = None,
    company_phone: str = "",
    company_email: str = "",
    company_location: str = "",
    logo_path: str = "",
    website_url: str = "",
) -> bytes:
    """Render a modern customer credit note as PDF bytes."""
    brand_rgb = _hex_to_rgb(primary_color, C_TEAL)
    pdf = CreditNotePDF(brand_rgb=brand_rgb)
    pdf.add_page()

    subtitle = (
        "Outstanding — full account"
        if all_time
        else f"Partial account — {period_label or 'selected period'}"
    )
    qr_png = _qr_png_bytes(website_url) or _qr_png_bytes(pay_link)
    pdf._letterhead(
        company_name=company_name or "Company",
        shop_name=shop_name or "Shop",
        credit_note_ref=credit_note_ref,
        subtitle=subtitle,
        company_phone=company_phone,
        company_email=company_email,
        company_location=company_location,
        logo_path=logo_path,
        qr_png=qr_png,
    )

    pdf.set_fill_color(*C_SURFACE)
    pdf.set_draw_color(*C_BORDER)
    box_y = pdf.get_y()
    meta_h = 26 if (scope_label and not all_time) else 22
    pdf.rect(12, box_y, pdf.content_w, meta_h, style="DF", round_corners=True, corner_radius=2.5)
    pdf.set_xy(15, box_y + 3)
    pdf._meta_row("Customer", customer_name or "Customer")
    pdf.set_x(15)
    pdf._meta_row("Phone", customer_phone or "-")
    pdf.set_x(15)
    pdf._meta_row(
        "Statement",
        "Full account" if all_time else (period_label or "Partial account"),
    )
    if scope_label and not all_time:
        pdf.set_x(15)
        pdf._meta_row("Scope", scope_label)
    pdf.set_y(box_y + meta_h + 2)

    x0 = pdf.get_x()
    y0 = pdf.get_y()
    card_w = (pdf.content_w - 9) / 4
    card_h = 20.0
    pdf._kpi_card(x0, y0, card_w, card_h, "Balance due", _fmt_money(balance_due), C_ROSE, C_ROSE)
    pdf._kpi_card(
        x0 + card_w + 3, y0, card_w, card_h,
        "Outstanding", _fmt_money(outstanding), C_AMBER, C_AMBER,
    )
    pdf._kpi_card(
        x0 + (card_w + 3) * 2, y0, card_w, card_h,
        "Unpaid sales", str(int(unpaid_sales_count or 0)), C_SKY, C_SLATE,
    )
    pdf._kpi_card(
        x0 + (card_w + 3) * 3, y0, card_w, card_h,
        "Paid to date", _fmt_money(paid_to_date), C_EMERALD, C_EMERALD,
    )
    pdf.set_y(y0 + card_h + 6)

    if focus_sale:
        sale = focus_sale.get("sale") or focus_sale
        sale_items = focus_sale.get("items") or []
        sale_id = int(sale.get("id") or 0)
        pdf._section_title(
            f"Credit sale #{sale_id}",
            str(sale.get("created_at") or "").strip(),
        )
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*C_MUTED)
        pdf.cell(
            0,
            4,
            _pdf_text(
                f"Amount {_fmt_money(sale.get('total_amount'))}  |  "
                f"Paid {_fmt_money(sale.get('paid_amount'))}  |  "
                f"Balance {_fmt_money(sale.get('remaining_amount'))}"
            ),
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        pdf.ln(2)
        if sale_items:
            headers = ["Item", "Qty", "Unit", "Line total"]
            col_w = [88.0, 22.0, 32.0, 44.0]
            pdf._table_header(headers, col_w)
            for idx, it in enumerate(sale_items):
                if pdf.get_y() > 268:
                    pdf.add_page()
                    pdf._table_header(headers, col_w)
                pdf._table_row(
                    [
                        str(it.get("item_name") or "Item"),
                        str(int(it.get("qty") or 0)),
                        f"{float(it.get('unit_price') or 0):,.2f}",
                        f"{float(it.get('line_total') or 0):,.2f}",
                    ],
                    col_w,
                    zebra=idx % 2 == 1,
                    align_right_from=1,
                )
        pdf.ln(4)

    pdf._section_title("Unpaid items", "All items on outstanding credit sales")
    pdf.ln(1)

    if not unpaid_items:
        pdf.set_fill_color(*C_SURFACE)
        pdf.set_draw_color(*C_BORDER)
        pdf.rect(pdf.get_x(), pdf.get_y(), pdf.content_w, 14, style="DF", round_corners=True, corner_radius=2.5)
        pdf.set_xy(pdf.get_x() + 4, pdf.get_y() + 4.5)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*C_EMERALD)
        pdf.cell(pdf.content_w - 8, 5, _pdf_text("All settled — no outstanding credit."))
        pdf.ln(16)
    else:
        if company_scope:
            headers = ["Shop", "Date picked", "Item", "Qty", "Amount"]
            col_w = [28.0, 26.0, 62.0, 20.0, 42.0]
        else:
            headers = ["Date picked", "Item", "Qty", "Amount"]
            col_w = [30.0, 88.0, 24.0, 44.0]
        pdf._table_header(headers, col_w)
        for idx, row in enumerate(unpaid_items):
            if pdf.get_y() > 268:
                pdf.add_page()
                pdf._table_header(headers, col_w)
            qty = row.get("qty")
            qty_text = "—" if qty is None else str(int(qty))
            picked = str(row.get("picked_at") or "—")
            if company_scope:
                values = [
                    str(row.get("shop_name") or "—"),
                    picked,
                    str(row.get("item_name") or "Item"),
                    qty_text,
                    f"{float(row.get('amount') or 0):,.2f}",
                ]
                align_from = 3
            else:
                values = [
                    picked,
                    str(row.get("item_name") or "Item"),
                    qty_text,
                    f"{float(row.get('amount') or 0):,.2f}",
                ]
                align_from = 2
            pdf._table_row(values, col_w, zebra=idx % 2 == 1, align_right_from=align_from)

    pdf.ln(4)
    if balance_due > 0.009:
        note_y = pdf.get_y()
        box_h = 28 if pay_link else 12
        pdf.set_fill_color(240, 253, 250)
        pdf.set_draw_color(167, 243, 208)
        pdf.rect(12, note_y, pdf.content_w, box_h, style="DF", round_corners=True, corner_radius=2.5)
        pdf.set_xy(15, note_y + 3.2)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*C_MUTED)
        pdf.cell(0, 4, _pdf_text("AMOUNT TO PAY"), align="C")
        pdf.set_xy(15, note_y + 7.5)
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(*C_ROSE)
        pdf.cell(0, 6, _pdf_text(_fmt_money(balance_due)), align="C")
        if pay_link:
            btn_y = note_y + 15.0
            pdf._pay_button(label="Pay with M-Pesa", url=pay_link, y=btn_y)
        pdf.set_y(note_y + box_h + 2)
    else:
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*C_EMERALD)
        pdf.cell(0, 5, _pdf_text("This account is fully paid. Thank you for your business."))

    if generated_at:
        pdf.ln(3)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(*C_MUTED)
        pdf.cell(0, 5, _pdf_text(f"Generated {generated_at}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    return bytes(pdf.output())
