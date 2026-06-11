"""PDF builders for customer credit notes."""

from __future__ import annotations

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


def _fmt_money(value: object) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"KES {amount:,.2f}"


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
        colors = [C_EMERALD, (37, 211, 102), C_SKY]
        steps = 20
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

    def _hero(
        self,
        *,
        company_name: str,
        shop_name: str,
        credit_note_ref: str,
        subtitle: str,
    ) -> None:
        y0 = self.get_y()
        self.set_fill_color(*self.brand_rgb)
        self.rect(12, y0, self.content_w, 26, style="F")
        self._accent_bar(12, y0 + 26, self.content_w, 2)

        self.set_xy(16, y0 + 5)
        self.set_font("Helvetica", "B", 15)
        self.set_text_color(*C_WHITE)
        self.cell(0, 7, _pdf_text(company_name), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(16)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(255, 237, 213)
        self.cell(0, 5, _pdf_text(shop_name), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(16)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*C_WHITE)
        self.cell(0, 6, _pdf_text("Credit note"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(16)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(255, 237, 213)
        meta = f"Ref {credit_note_ref}"
        if subtitle:
            meta += f"  |  {subtitle}"
        self.cell(0, 4, _pdf_text(meta), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_y(y0 + 31)

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
        self.set_fill_color(*self.brand_dark)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 8)
        self.set_draw_color(*self.brand_dark)
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
) -> bytes:
    """Render a customer credit note as PDF bytes."""
    brand_rgb = _hex_to_rgb(primary_color, C_BRAND)
    pdf = CreditNotePDF(brand_rgb=brand_rgb)
    pdf.add_page()

    subtitle = (
        "Outstanding balance — full account"
        if all_time
        else f"Outstanding — {period_label or 'period'}"
    )
    pdf._hero(
        company_name=company_name or "Company",
        shop_name=shop_name or "Shop",
        credit_note_ref=credit_note_ref,
        subtitle=subtitle,
    )
    pdf.ln(2)

    pdf.set_fill_color(*C_SURFACE)
    pdf.set_draw_color(*C_BORDER)
    box_y = pdf.get_y()
    pdf.rect(12, box_y, pdf.content_w, 22, style="DF", round_corners=True, corner_radius=2.5)
    pdf.set_xy(15, box_y + 3)
    pdf._meta_row("Customer", customer_name or "Customer")
    pdf.set_x(15)
    pdf._meta_row("Phone", customer_phone or "-")
    pdf.set_x(15)
    pdf._meta_row("Period", period_label or ("Full account" if all_time else "Selected period"))
    if scope_label and not all_time:
        pdf.set_x(15)
        pdf._meta_row("Scope", scope_label)
    pdf.set_y(box_y + 24)

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
            headers = ["Shop", "Item", "Qty", "Amount"]
            col_w = [34.0, 78.0, 24.0, 50.0]
        else:
            headers = ["Item", "Qty", "Amount"]
            col_w = [108.0, 28.0, 50.0]
        pdf._table_header(headers, col_w)
        for idx, row in enumerate(unpaid_items):
            if pdf.get_y() > 268:
                pdf.add_page()
                pdf._table_header(headers, col_w)
            qty = row.get("qty")
            qty_text = "—" if qty is None else str(int(qty))
            if company_scope:
                values = [
                    str(row.get("shop_name") or "—"),
                    str(row.get("item_name") or "Item"),
                    qty_text,
                    f"{float(row.get('amount') or 0):,.2f}",
                ]
                align_from = 2
            else:
                values = [
                    str(row.get("item_name") or "Item"),
                    qty_text,
                    f"{float(row.get('amount') or 0):,.2f}",
                ]
                align_from = 1
            pdf._table_row(values, col_w, zebra=idx % 2 == 1, align_right_from=align_from)

    pdf.ln(4)
    if balance_due > 0.009:
        pdf.set_fill_color(255, 247, 237)
        pdf.set_draw_color(251, 191, 36)
        note_y = pdf.get_y()
        pdf.rect(12, note_y, pdf.content_w, 16 if pay_link else 10, style="DF", round_corners=True, corner_radius=2.5)
        pdf.set_xy(15, note_y + 3)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*C_ROSE)
        pdf.cell(0, 5, _pdf_text(f"Amount to pay: {_fmt_money(balance_due)}"))
        if pay_link:
            pdf.set_xy(15, note_y + 8.5)
            pdf.set_font("Helvetica", "", 7.5)
            pdf.set_text_color(*C_MUTED)
            pdf.cell(0, 4, _pdf_text("M-Pesa pay link:"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.set_x(15)
            pdf.set_text_color(*C_SKY)
            pdf.set_font("Helvetica", "", 7)
            pdf.multi_cell(pdf.content_w - 6, 3.5, _pdf_text(pay_link))
        pdf.set_y(note_y + (18 if pay_link else 12))
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
