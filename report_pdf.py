"""PDF builders for company / shop period reports."""

from __future__ import annotations

from fpdf import FPDF
from fpdf.enums import XPos, YPos


# Report palette aligned with the web company/shop report UI
C_BRAND = (249, 115, 22)       # #f97316
C_BRAND_DARK = (234, 88, 12)    # #ea580c
C_EMERALD = (5, 150, 105)       # revenue
C_AMBER = (217, 119, 6)         # expenditure
C_SKY = (2, 132, 199)           # credit / M-Pesa accent
C_SLATE = (15, 23, 42)          # headings
C_MUTED = (100, 116, 139)       # secondary text
C_SURFACE = (248, 250, 252)     # card fill
C_BORDER = (226, 232, 240)      # borders
C_WHITE = (255, 255, 255)
C_ROSE = (244, 63, 94)          # stock out
C_ROW_ALT = (241, 245, 249)     # zebra stripe


def _hex_to_rgb(hex_color: str | None, fallback: tuple[int, int, int]) -> tuple[int, int, int]:
    raw = str(hex_color or "").strip().lstrip("#")
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    if len(raw) != 6:
        return fallback
    try:
        return (int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16))
    except ValueError:
        return fallback


def _pdf_text(value: object) -> str:
    return str(value or "").encode("latin-1", "replace").decode("latin-1")


def _fmt_money(value: object) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"KES {amount:,.2f}"


class PeriodReportPDF(FPDF):
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

    def _gradient_bar(self, x: float, y: float, w: float, h: float) -> None:
        colors = [C_EMERALD, C_SKY, C_AMBER]
        steps = 24
        step_w = w / steps
        for i in range(steps):
            t = i / max(steps - 1, 1)
            if t <= 0.45:
                blend = t / 0.45
                c0, c1 = colors[0], colors[1]
            else:
                blend = (t - 0.45) / 0.55
                c0, c1 = colors[1], colors[2]
            r = int(c0[0] + (c1[0] - c0[0]) * blend)
            g = int(c0[1] + (c1[1] - c0[1]) * blend)
            b = int(c0[2] + (c1[2] - c0[2]) * blend)
            self.set_fill_color(r, g, b)
            self.rect(x + i * step_w, y, step_w + 0.2, h, style="F")

    def _hero_header(
        self,
        *,
        entity_name: str,
        title: str,
        period_label: str,
        scope_label: str,
    ) -> None:
        y0 = self.get_y()
        self.set_fill_color(*self.brand_rgb)
        self.rect(12, y0, self.content_w, 28, style="F")
        self._gradient_bar(12, y0 + 28, self.content_w, 2.2)

        self.set_xy(16, y0 + 6)
        self.set_font("Helvetica", "B", 17)
        self.set_text_color(*C_WHITE)
        self.cell(0, 8, _pdf_text(entity_name), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_x(16)
        self.set_font("Helvetica", "", 11)
        self.set_text_color(255, 237, 213)
        self.cell(0, 6, _pdf_text(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_x(16)
        self.set_font("Helvetica", "", 9)
        meta = f"Period: {period_label or 'Selected period'}"
        if scope_label:
            meta += f"  |  {scope_label}"
        self.cell(0, 5, _pdf_text(meta), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_y(y0 + 34)

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
        self.set_font("Helvetica", "B", 7)
        self.set_text_color(*C_MUTED)
        self.cell(w - 6, 4, _pdf_text(label.upper()))
        self.set_xy(x + 3, y + 10)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(*value_color)
        self.cell(w - 6, 7, _pdf_text(value))

    def _detail_box(
        self,
        x: float,
        y: float,
        w: float,
        h: float,
        heading: str,
        rows: list[tuple[str, str, tuple[int, int, int] | None]],
    ) -> None:
        self.set_fill_color(*C_SURFACE)
        self.set_draw_color(*C_BORDER)
        self.rect(x, y, w, h, style="DF", round_corners=True, corner_radius=2.5)
        self.set_xy(x + 3.5, y + 3.5)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*C_SLATE)
        self.cell(w - 7, 4, _pdf_text(heading.upper()))
        line_y = y + 9
        for label, value, color in rows:
            self.set_xy(x + 3.5, line_y)
            self.set_font("Helvetica", "", 8)
            self.set_text_color(*C_MUTED)
            self.cell((w - 7) * 0.55, 4, _pdf_text(label))
            self.set_font("Helvetica", "B", 9)
            self.set_text_color(*(color or C_SLATE))
            self.cell((w - 7) * 0.45, 4, _pdf_text(value), align="R")
            line_y += 5.5

    def _draw_items_header(self, col_w: list[float]) -> None:
        headers = ["Item", "Start", "End", "In", "Out", "Sold", "Revenue"]
        self.set_fill_color(*self.brand_dark)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 8)
        self.set_draw_color(*self.brand_dark)
        for idx, label in enumerate(headers):
            align = "R" if idx else "L"
            self.cell(col_w[idx], 8, _pdf_text(label), border=1, align=align, fill=True)
        self.ln()

    def _draw_item_row(
        self,
        col_w: list[float],
        values: list[str],
        *,
        zebra: bool,
        stock_in: int,
        stock_out: int,
    ) -> None:
        if zebra:
            self.set_fill_color(*C_ROW_ALT)
        else:
            self.set_fill_color(*C_WHITE)
        self.set_draw_color(*C_BORDER)
        self.set_font("Helvetica", "", 7.5)
        for idx, value in enumerate(values):
            align = "R" if idx else "L"
            if idx == 0:
                self.set_text_color(*C_SLATE)
                self.set_font("Helvetica", "B", 7.5)
            elif idx == 3 and stock_in > 0:
                self.set_text_color(*C_EMERALD)
                self.set_font("Helvetica", "B", 7.5)
            elif idx == 4 and stock_out > 0:
                self.set_text_color(*C_ROSE)
                self.set_font("Helvetica", "B", 7.5)
            elif idx == 6:
                self.set_text_color(*C_SLATE)
                self.set_font("Helvetica", "B", 7.5)
            else:
                self.set_text_color(71, 85, 105)
                self.set_font("Helvetica", "", 7.5)
            self.cell(col_w[idx], 6.5, _pdf_text(value), border=1, align=align, fill=True)
        self.ln()


def build_period_report_pdf(
    *,
    title: str,
    entity_name: str,
    period_label: str,
    scope_label: str,
    report_data: dict,
    generated_at: str = "",
    primary_color: str | None = None,
) -> bytes:
    """Render a period report summary + item table as PDF bytes."""
    brand_rgb = _hex_to_rgb(primary_color, C_BRAND)
    pdf = PeriodReportPDF(brand_rgb=brand_rgb)
    pdf.add_page()
    pdf.set_text_color(*C_SLATE)

    pdf._hero_header(
        entity_name=entity_name,
        title=title,
        period_label=period_label,
        scope_label=scope_label,
    )
    pdf.ln(2)

    rd = report_data or {}
    net_profit = float(rd.get("net_profit") or 0)
    net_color = C_EMERALD if net_profit >= 0 else C_ROSE

    x0 = pdf.get_x()
    y0 = pdf.get_y()
    card_w = (pdf.content_w - 8) / 3
    card_h = 22.0
    pdf._kpi_card(
        x0, y0, card_w, card_h,
        "Total revenue", _fmt_money(rd.get("total_revenue")),
        C_EMERALD, C_EMERALD,
    )
    pdf._kpi_card(
        x0 + card_w + 4, y0, card_w, card_h,
        "Total expenditure", _fmt_money(rd.get("total_expenditure")),
        C_AMBER, C_AMBER,
    )
    pdf._kpi_card(
        x0 + (card_w + 4) * 2, y0, card_w, card_h,
        "Net profit", _fmt_money(net_profit),
        net_color, net_color,
    )
    pdf.set_y(y0 + card_h + 5)

    box_w = (pdf.content_w - 4) / 2
    box_h = 24.0
    y1 = pdf.get_y()
    pdf._detail_box(
        x0,
        y1,
        box_w,
        box_h,
        "Revenue by sale type",
        [
            ("Cash sales", _fmt_money(rd.get("sale_revenue")), C_SLATE),
            ("Credit sales", _fmt_money(rd.get("credit_revenue")), C_SKY),
        ],
    )
    pdf._detail_box(
        x0 + box_w + 4,
        y1,
        box_w,
        box_h,
        "Revenue by payment",
        [
            ("Cash", _fmt_money(rd.get("cash_revenue")), C_SLATE),
            ("M-Pesa", _fmt_money(rd.get("mpesa_revenue")), C_EMERALD),
        ],
    )
    pdf.set_y(y1 + box_h + 7)

    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*C_SLATE)
    pdf.cell(0, 7, _pdf_text("Items in period"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*C_MUTED)
    pdf.cell(
        0,
        5,
        _pdf_text("Starting/ending stock, movements, sales, and revenue for the filtered period."),
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.ln(2)

    col_w = [62.0, 20.0, 20.0, 18.0, 18.0, 18.0, 24.0]
    items = list(rd.get("items") or [])
    if not items:
        pdf.set_fill_color(*C_SURFACE)
        pdf.set_draw_color(*C_BORDER)
        pdf.rect(pdf.get_x(), pdf.get_y(), pdf.content_w, 12, style="DF", round_corners=True, corner_radius=2.5)
        pdf.set_xy(pdf.get_x() + 4, pdf.get_y() + 4)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*C_MUTED)
        pdf.cell(pdf.content_w - 8, 5, _pdf_text("No item activity found for this period."))
        pdf.ln(14)
    else:
        pdf._draw_items_header(col_w)
        for row_idx, row in enumerate(items):
            if pdf.get_y() > 268:
                pdf.add_page()
                pdf._draw_items_header(col_w)

            name = (row.get("name") or "").strip() or "Item"
            category = (row.get("category") or "").strip()
            item_label = f"{name} ({category})" if category else name
            if len(item_label) > 46:
                item_label = item_label[:43] + "..."

            stock_in = int(row.get("stock_in") or 0)
            stock_out = int(row.get("stock_out") or 0)
            values = [
                item_label,
                str(int(row.get("starting_stock") or 0)),
                str(int(row.get("ending_stock") or 0)),
                str(stock_in),
                str(stock_out),
                str(int(row.get("stock_sold") or 0)),
                f"{float(row.get('revenue') or 0):,.2f}",
            ]
            pdf._draw_item_row(
                col_w,
                values,
                zebra=row_idx % 2 == 1,
                stock_in=stock_in,
                stock_out=stock_out,
            )

    if generated_at:
        pdf.ln(3)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(*C_MUTED)
        pdf.cell(
            0,
            5,
            _pdf_text(f"Generated {generated_at}"),
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )

    out = pdf.output()
    return bytes(out)
