#!/usr/bin/env python3
"""
Generate a realistic-looking IRS Schedule K-1 (Form 1065) PDF.

This script creates a sample K-1 document for demo/testing purposes.
All data is fictitious. Uses reportlab for PDF generation.

Usage:
    cd /home/daniel/pp/tyler_demo/k1_pipeline && uv run python scripts/generate_sample_k1.py

The output PDF is written to data/input/sample_k1.pdf by default.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import black, white, HexColor
from reportlab.pdfgen.canvas import Canvas


# ---------------------------------------------------------------------------
# Colour constants
# ---------------------------------------------------------------------------
LIGHT_GRAY = HexColor("#E8E8E8")
MED_GRAY = HexColor("#D0D0D0")
DARK_GRAY = HexColor("#404040")
HEADER_BG = HexColor("#000000")
SECTION_BG = HexColor("#D8D8D8")
BOX_BG = HexColor("#F5F5F5")

# ---------------------------------------------------------------------------
# Default form data -- override via function arguments for reuse
# ---------------------------------------------------------------------------
DEFAULT_DATA = dict(
    tax_year="2024",
    # Partnership (Part I)
    partnership_name="Meridian Capital Growth Fund, LP",
    partnership_ein="82-4571903",
    partnership_addr1="450 Park Avenue, Suite 2100",
    partnership_addr2="New York, NY 10022",
    irs_center="Ogden, UT",
    publicly_traded="No",
    # Partner (Part II)
    partner_name="Jonathan A. Blackwell",
    partner_ssn="478-93-6215",
    partner_addr1="1847 Oakridge Drive",
    partner_addr2="Greenwich, CT 06831",
    partner_type_individual=True,
    partner_type_general=True,
    partner_domestic=True,
    partner_share_profit_beg="3.75",
    partner_share_profit_end="3.75",
    partner_share_loss_beg="3.75",
    partner_share_loss_end="3.75",
    partner_share_capital_beg="3.75",
    partner_share_capital_end="3.75",
    partner_share_liab_recourse_beg="12,500",
    partner_share_liab_recourse_end="11,200",
    partner_share_liab_qual_beg="0",
    partner_share_liab_qual_end="0",
    partner_share_liab_nonrecourse_beg="38,750",
    partner_share_liab_nonrecourse_end="36,100",
    capital_account_beg="542,100",
    capital_contributed="50,000",
    capital_current_yr_increase="244,145",
    capital_withdrawals="(95,000)",
    capital_ending="741,245",
    capital_method="Tax basis",
    # Part III -- Income / Loss / Deductions
    box1_ordinary_income="127,450",
    box2_rental_real_estate="(18,200)",
    box3_other_rental="0",
    box4_guaranteed_payments="45,000",
    box5_interest_income="8,325",
    box6a_ordinary_dividends="12,780",
    box6b_qualified_dividends="9,150",
    box7_royalties="",
    box8_net_st_cap_gain="(3,400)",
    box9a_net_lt_cap_gain="67,890",
    box9b_collectibles="",
    box9c_unrecaptured_1250="",
    box10_net_1231_gain="0",
    box11_other_income="4,200",
    box12_section_179="15,000",
    box13_other_deductions="",
    box14_self_employment="172,450",
    box15_credits="",
    box16_foreign_transactions="1,890",
    box17_amt_items="(2,300)",
    box18_tax_exempt="3,100",
    box19_distributions="95,000",
    box20_other_info="",
    # Page 2 supplemental
    se_earnings="172,450",
    amt_adjustment="(2,300)",
    tax_exempt_income="3,100",
    foreign_taxes_paid="1,890",
    investment_interest_expense="4,500",
    section_199a_qbi="127,450",
)


# ---------------------------------------------------------------------------
# Helper drawing functions
# ---------------------------------------------------------------------------

def _fmt(val: str | None) -> str:
    """Return value string or empty."""
    if val is None:
        return ""
    return str(val)


def draw_header(c: Canvas, w: float, h: float, data: dict) -> float:
    """Draw the top header band. Returns the y position below the header."""
    top = h - 0.4 * inch
    band_h = 1.15 * inch

    # Black top band
    c.setFillColor(HEADER_BG)
    c.rect(0.5 * inch, top - band_h, w - 1.0 * inch, band_h, fill=1, stroke=0)

    # White text in header
    c.setFillColor(white)
    c.setFont("Courier-Bold", 7)
    c.drawString(0.6 * inch, top - 0.18 * inch, "Schedule K-1")
    c.setFont("Courier-Bold", 9)
    c.drawString(0.6 * inch, top - 0.36 * inch, "(Form 1065)")
    c.setFont("Courier", 6.5)
    c.drawString(0.6 * inch, top - 0.52 * inch, "Department of the Treasury")
    c.drawString(0.6 * inch, top - 0.64 * inch, "Internal Revenue Service")

    # Center title
    c.setFont("Courier-Bold", 11)
    cx = w / 2
    c.drawCentredString(cx, top - 0.22 * inch, f"{data['tax_year']}")
    c.setFont("Courier-Bold", 8.5)
    c.drawCentredString(cx, top - 0.40 * inch, "Partner's Share of Income, Deductions,")
    c.drawCentredString(cx, top - 0.54 * inch, "Credits, etc.")
    c.setFont("Courier", 6.5)
    c.drawCentredString(cx, top - 0.70 * inch, "See separate instructions.")

    # Right side -- OMB / sequence
    c.setFont("Courier", 6)
    rx = w - 0.6 * inch
    c.drawRightString(rx, top - 0.18 * inch, "OMB No. 1545-0123")
    c.setFont("Courier-Bold", 7.5)
    c.drawRightString(rx, top - 0.38 * inch, f"For calendar year {data['tax_year']},")
    c.setFont("Courier", 6.5)
    c.drawRightString(rx, top - 0.52 * inch, f"or tax year beginning _________ {data['tax_year']}")
    c.drawRightString(rx, top - 0.64 * inch, f"ending _________ 20__")

    # Final / Amended checkboxes row
    c.setFont("Courier", 6)
    c.drawCentredString(cx, top - 0.88 * inch, "Final K-1  [ ]        Amended K-1  [ ]")

    c.setFillColor(black)
    return top - band_h


def draw_section_header(c: Canvas, x: float, y: float, w: float,
                        title: str, subtitle: str = "") -> float:
    """Draw a gray section header band. Returns y below it."""
    band_h = 0.22 * inch
    c.setFillColor(SECTION_BG)
    c.rect(x, y - band_h, w, band_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier-Bold", 8)
    c.drawString(x + 0.08 * inch, y - 0.16 * inch, title)
    if subtitle:
        c.setFont("Courier", 6.5)
        c.drawString(x + 0.08 * inch + c.stringWidth(title, "Courier-Bold", 8) + 6,
                     y - 0.16 * inch, subtitle)
    return y - band_h


def draw_labeled_box(c: Canvas, x: float, y: float, w: float, h: float,
                     label: str, value: str, label_size: float = 6.5,
                     value_size: float = 8, bold_value: bool = False):
    """Draw a labeled box with a value."""
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(x, y - h, w, h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", label_size)
    c.drawString(x + 3, y - 10, label)
    font = "Courier-Bold" if bold_value else "Courier"
    c.setFont(font, value_size)
    c.drawString(x + 3, y - h + 4, value)
    c.setStrokeColor(black)


def draw_line_item(c: Canvas, x: float, y: float, box_w: float,
                   total_w: float, box_num: str, description: str,
                   value: str, line_h: float = 0.22 * inch) -> float:
    """Draw a single numbered line item row. Returns y below."""
    # Box number cell
    c.setStrokeColor(MED_GRAY)
    c.rect(x, y - line_h, box_w, line_h, stroke=1)
    c.setFont("Courier-Bold", 7.5)
    c.drawCentredString(x + box_w / 2, y - line_h + 4, box_num)

    # Description cell
    desc_w = total_w - box_w - 1.2 * inch
    c.rect(x + box_w, y - line_h, desc_w, line_h, stroke=1)
    c.setFont("Courier", 7)
    c.drawString(x + box_w + 3, y - line_h + 4, description)

    # Value cell
    val_w = 1.2 * inch
    c.rect(x + box_w + desc_w, y - line_h, val_w, line_h, stroke=1)
    c.setFont("Courier-Bold", 8.5)
    if value and value != "0":
        display_val = _fmt(value)
        if display_val and not display_val.startswith("(") and not display_val.startswith("$"):
            display_val = "$" + display_val
        elif display_val.startswith("("):
            display_val = "($" + display_val[1:]
        c.drawString(x + box_w + desc_w + 4, y - line_h + 4, display_val)

    c.setStrokeColor(black)
    return y - line_h


# ---------------------------------------------------------------------------
# Page 1 -- main K-1 form
# ---------------------------------------------------------------------------

def draw_page1(c: Canvas, w: float, h: float, data: dict):
    """Render the first page of the K-1."""
    y = draw_header(c, w, h, data)
    margin_l = 0.5 * inch
    content_w = w - 1.0 * inch
    col_left_w = 3.4 * inch
    col_right_w = content_w - col_left_w - 0.08 * inch

    # -----------------------------------------------------------------------
    # PART I -- Information About the Partnership
    # -----------------------------------------------------------------------
    y = draw_section_header(c, margin_l, y, content_w,
                            "Part I   ", "Information About the Partnership")

    box_h = 0.32 * inch
    # A  Partnership EIN
    draw_labeled_box(c, margin_l, y, col_left_w / 2, box_h,
                     "A  Partnership's employer identification number",
                     data["partnership_ein"])
    # B  Partnership name / address
    name_box_h = 0.82 * inch
    draw_labeled_box(c, margin_l, y - box_h, col_left_w, name_box_h,
                     "B  Partnership's name, address, city, state, and ZIP code", "")
    # Fill in name/address -- offset below the label line
    c.setFont("Courier-Bold", 8)
    inner_y = y - box_h - 0.24 * inch
    c.drawString(margin_l + 10, inner_y, data["partnership_name"])
    c.setFont("Courier", 7.5)
    c.drawString(margin_l + 10, inner_y - 14, data["partnership_addr1"])
    c.drawString(margin_l + 10, inner_y - 26, data["partnership_addr2"])

    y_after_part1_left = y - box_h - name_box_h

    # C  IRS Center
    irs_box_h = 0.30 * inch
    draw_labeled_box(c, margin_l, y_after_part1_left, col_left_w, irs_box_h,
                     "C  IRS Center where partnership filed return",
                     data.get("irs_center", ""))

    # D  Check if publicly traded
    pt_box_h = 0.24 * inch
    draw_labeled_box(c, margin_l, y_after_part1_left - irs_box_h,
                     col_left_w, pt_box_h,
                     f"D  Check if this is a publicly traded partnership  [ ]", "")
    y_after_part1 = y_after_part1_left - irs_box_h - pt_box_h

    # -----------------------------------------------------------------------
    # PART II -- Information About the Partner  (left column continues)
    # -----------------------------------------------------------------------
    y2 = draw_section_header(c, margin_l, y_after_part1, col_left_w,
                             "Part II  ", "Information About the Partner")

    # E  Partner SSN
    draw_labeled_box(c, margin_l, y2, col_left_w / 2, box_h,
                     "E  Partner's SSN or TIN",
                     data["partner_ssn"])
    # remaining half -- blank / entity type
    draw_labeled_box(c, margin_l + col_left_w / 2, y2, col_left_w / 2, box_h,
                     "  (Do not use TIN of disregarded entity)", "")

    # F  Partner name / address
    partner_name_h = 0.78 * inch
    draw_labeled_box(c, margin_l, y2 - box_h, col_left_w, partner_name_h,
                     "F  Partner's name, address, city, state, and ZIP code", "")
    inner_y2 = y2 - box_h - 0.24 * inch
    c.setFont("Courier-Bold", 8)
    c.drawString(margin_l + 10, inner_y2, data["partner_name"])
    c.setFont("Courier", 7.5)
    c.drawString(margin_l + 10, inner_y2 - 14, data["partner_addr1"])
    c.drawString(margin_l + 10, inner_y2 - 26, data["partner_addr2"])

    y_below_f = y2 - box_h - partner_name_h

    # G  Checkboxes -- partner type & H1 entity type
    g_h = 0.62 * inch
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(margin_l, y_below_f - g_h, col_left_w, g_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", 5.8)
    gx = margin_l + 4
    gy = y_below_f - 10
    lh = 9  # line height in points
    c.drawString(gx, gy, "G  General partner or LLC")
    c.drawString(gx, gy - lh, "   member-manager            [X]")
    c.drawString(gx, gy - lh * 2, "   Limited partner or other")
    c.drawString(gx, gy - lh * 3, "   LLC member                [ ]")
    right_col_x = margin_l + 1.9 * inch
    c.drawString(right_col_x, gy, "Domestic partner  [X]")
    c.drawString(right_col_x, gy - lh, "Foreign partner   [ ]")
    c.setFont("Courier", 6)
    c.drawString(gx, gy - lh * 5,
                 "H1  What type of entity is this partner?  Individual")
    c.setStrokeColor(black)

    y_below_g = y_below_f - g_h

    # I / J / K  -- partner shares, liabilities, capital account
    # Partner's share of profit, loss, capital
    share_h = 0.76 * inch
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(margin_l, y_below_g - share_h, col_left_w, share_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", 6.5)
    sx = margin_l + 4
    sy = y_below_g - 10
    beg_x = margin_l + 2.05 * inch
    end_x = margin_l + 2.75 * inch
    c.drawString(sx, sy, "I  Partner's share of profit, loss, and capital:")
    c.setFont("Courier", 6)
    # Column headers on a separate row below the label
    c.drawRightString(beg_x + 0.35 * inch, sy - 11, "Beginning")
    c.drawRightString(end_x + 0.35 * inch, sy - 11, "Ending")
    items_share = [
        ("   Profit", "partner_share_profit_beg", "partner_share_profit_end"),
        ("   Loss", "partner_share_loss_beg", "partner_share_loss_end"),
        ("   Capital", "partner_share_capital_beg", "partner_share_capital_end"),
    ]
    for i, (lbl, bk, ek) in enumerate(items_share):
        row_y = sy - 11 * (i + 2)
        c.drawString(sx, row_y, lbl)
        c.drawRightString(beg_x + 0.35 * inch, row_y, f"{data[bk]}%")
        c.drawRightString(end_x + 0.35 * inch, row_y, f"{data[ek]}%")

    c.setFont("Courier", 6)
    j_y = sy - 11 * 6
    c.drawString(sx, j_y, "J  Partner's share of liabilities:")
    c.drawRightString(beg_x + 0.35 * inch, j_y, "Beginning")
    c.drawRightString(end_x + 0.35 * inch, j_y, "Ending")
    c.setStrokeColor(black)

    y_below_share = y_below_g - share_h

    # Liabilities
    liab_h = 0.48 * inch
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(margin_l, y_below_share - liab_h, col_left_w, liab_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", 6)
    lx = margin_l + 4
    ly = y_below_share - 10
    liab_items = [
        ("   Nonrecourse", "partner_share_liab_nonrecourse_beg", "partner_share_liab_nonrecourse_end"),
        ("   Qualified nonrecourse", "partner_share_liab_qual_beg", "partner_share_liab_qual_end"),
        ("   Recourse", "partner_share_liab_recourse_beg", "partner_share_liab_recourse_end"),
    ]
    for i, (lbl, bk, ek) in enumerate(liab_items):
        row_y = ly - 11 * i
        c.drawString(lx, row_y, lbl)
        c.drawRightString(beg_x + 0.35 * inch, row_y, f"${data[bk]}")
        c.drawRightString(end_x + 0.35 * inch, row_y, f"${data[ek]}")
    c.setStrokeColor(black)

    y_below_liab = y_below_share - liab_h

    # Capital account analysis (K)
    cap_h = 0.82 * inch
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(margin_l, y_below_liab - cap_h, col_left_w, cap_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", 6.5)
    cx_l = margin_l + 4
    cy = y_below_liab - 10
    c.drawString(cx_l, cy, "K  Partner's capital account analysis:")
    cap_lines = [
        ("   Beginning capital account", data["capital_account_beg"]),
        ("   Capital contributed during the year", data["capital_contributed"]),
        ("   Current year increase (decrease)", data["capital_current_yr_increase"]),
        ("   Withdrawals & distributions", data["capital_withdrawals"]),
        ("   Ending capital account", data["capital_ending"]),
    ]
    for i, (lbl, val) in enumerate(cap_lines):
        row_y = cy - 11 * (i + 1)
        c.drawString(cx_l, row_y, lbl)
        c.drawRightString(margin_l + col_left_w - 8, row_y, f"${val}")
    # Method
    c.drawString(cx_l, cy - 11 * 6, f"   Method: {data['capital_method']}")
    c.setStrokeColor(black)

    # -----------------------------------------------------------------------
    # PART III -- Partner's Share of Current Year Income, Deductions, etc.
    # (right column, spanning full height)
    # -----------------------------------------------------------------------
    right_x = margin_l + col_left_w + 0.08 * inch
    y3 = draw_section_header(c, right_x, y, col_right_w,
                             "Part III ", "Partner's Share of Current Year Income,")
    # Subtitle line
    c.setFont("Courier", 6.5)
    sub_h = 0.16 * inch
    c.setFillColor(SECTION_BG)
    c.rect(right_x, y3 - sub_h, col_right_w, sub_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.drawString(right_x + 0.08 * inch, y3 - sub_h + 3,
                 "Deductions, Credits, and Other Items")
    y3 = y3 - sub_h

    box_num_w = 0.32 * inch

    lines_part3 = [
        ("1", "Ordinary business income (loss)", data["box1_ordinary_income"]),
        ("2", "Net rental real estate income (loss)", data["box2_rental_real_estate"]),
        ("3", "Other net rental income (loss)", data["box3_other_rental"]),
        ("4", "Guaranteed payments", data["box4_guaranteed_payments"]),
        ("5", "Interest income", data["box5_interest_income"]),
        ("6a", "Ordinary dividends", data["box6a_ordinary_dividends"]),
        ("6b", "Qualified dividends", data["box6b_qualified_dividends"]),
        ("7", "Royalties", data.get("box7_royalties", "")),
        ("8", "Net short-term capital gain (loss)", data["box8_net_st_cap_gain"]),
        ("9a", "Net long-term capital gain (loss)", data["box9a_net_lt_cap_gain"]),
        ("9b", "Collectibles (28%) gain (loss)", data.get("box9b_collectibles", "")),
        ("9c", "Unrecaptured section 1250 gain", data.get("box9c_unrecaptured_1250", "")),
        ("10", "Net section 1231 gain (loss)", data["box10_net_1231_gain"]),
        ("11", "Other income (loss)", data["box11_other_income"]),
        ("12", "Section 179 deduction", data["box12_section_179"]),
        ("13", "Other deductions", data.get("box13_other_deductions", "")),
        ("14", "Self-employment earnings (loss)", data["box14_self_employment"]),
        ("15", "Credits", data.get("box15_credits", "")),
        ("16", "Foreign transactions", data["box16_foreign_transactions"]),
        ("17", "Alternative minimum tax (AMT) items", data["box17_amt_items"]),
        ("18", "Tax-exempt income and nondeductible expenses", data["box18_tax_exempt"]),
        ("19", "Distributions", data["box19_distributions"]),
        ("20", "Other information", data.get("box20_other_info", "")),
    ]

    for box_num, desc, val in lines_part3:
        y3 = draw_line_item(c, right_x, y3, box_num_w, col_right_w,
                            box_num, desc, val, line_h=0.21 * inch)

    # Footer
    c.setFont("Courier", 5.5)
    c.drawString(margin_l, 0.42 * inch,
                 "For Paperwork Reduction Act Notice, see Instructions for Form 1065.")
    c.drawString(margin_l + 3.0 * inch, 0.42 * inch,
                 f"Cat. No. 11394R")
    c.drawRightString(w - margin_l, 0.42 * inch,
                      f"Schedule K-1 (Form 1065) {data['tax_year']}")

    # Outer border
    c.setStrokeColor(black)
    c.setLineWidth(1.2)
    c.rect(margin_l, 0.35 * inch, content_w, h - 0.75 * inch, stroke=1, fill=0)
    c.setLineWidth(0.5)


# ---------------------------------------------------------------------------
# Page 2 -- Supplemental Information
# ---------------------------------------------------------------------------

def draw_page2(c: Canvas, w: float, h: float, data: dict):
    """Render the second page with supplemental details."""
    margin_l = 0.5 * inch
    content_w = w - 1.0 * inch
    top = h - 0.4 * inch

    # Header band
    band_h = 0.55 * inch
    c.setFillColor(HEADER_BG)
    c.rect(margin_l, top - band_h, content_w, band_h, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Courier-Bold", 9)
    c.drawString(0.6 * inch, top - 0.20 * inch,
                 f"Schedule K-1 (Form 1065) {data['tax_year']}    Page 2")
    c.setFont("Courier", 7)
    c.drawString(0.6 * inch, top - 0.38 * inch,
                 "Supplemental Information  --  Partner's Share of Income, Deductions, Credits, and Other Items (continued)")
    c.setFillColor(black)

    y = top - band_h

    # Partnership / Partner identification
    id_h = 0.50 * inch
    c.setStrokeColor(MED_GRAY)
    c.setFillColor(BOX_BG)
    c.rect(margin_l, y - id_h, content_w, id_h, fill=1, stroke=1)
    c.setFillColor(black)
    c.setFont("Courier", 7)
    ix = margin_l + 6
    iy = y - 14
    c.drawString(ix, iy, f"Partnership:  {data['partnership_name']}")
    c.drawString(ix, iy - 12, f"EIN:  {data['partnership_ein']}")
    c.drawString(ix + 3.5 * inch, iy, f"Partner:  {data['partner_name']}")
    c.drawString(ix + 3.5 * inch, iy - 12, f"SSN:  {data['partner_ssn']}")
    c.setStrokeColor(black)

    y = y - id_h - 0.15 * inch

    # Section: Self-Employment Earnings
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 14  ", "Self-Employment Earnings (Loss)")
    se_lines = [
        ("14A", "Net earnings (loss) from self-employment", data["se_earnings"]),
        ("14B", "Gross farming or fishing income", ""),
        ("14C", "Gross non-farm income", data["se_earnings"]),
    ]
    box_w = 0.36 * inch
    for bn, desc, val in se_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    y -= 0.12 * inch

    # Section: Foreign Transactions
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 16  ", "Foreign Transactions")
    foreign_lines = [
        ("16A", "Name of country or U.S. possession", "Various"),
        ("16B", "Gross income from all sources", ""),
        ("16C", "Gross income sourced at partner level", ""),
        ("16D", "Foreign gross income -- passive category", ""),
        ("16E", "Foreign taxes paid", data["foreign_taxes_paid"]),
        ("16F", "Foreign taxes accrued", ""),
        ("16G", "Reduction in taxes available for credit", ""),
    ]
    for bn, desc, val in foreign_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    y -= 0.12 * inch

    # Section: AMT Items
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 17  ", "Alternative Minimum Tax (AMT) Items")
    amt_lines = [
        ("17A", "Post-1986 depreciation adjustment", data["amt_adjustment"]),
        ("17B", "Adjusted gain or loss", ""),
        ("17C", "Depletion (other than oil & gas)", ""),
        ("17D", "Oil, gas, & geothermal -- gross income", ""),
        ("17E", "Oil, gas, & geothermal -- deductions", ""),
        ("17F", "Other AMT items", ""),
    ]
    for bn, desc, val in amt_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    y -= 0.12 * inch

    # Section: Tax-Exempt Income
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 18  ", "Tax-Exempt Income and Nondeductible Expenses")
    te_lines = [
        ("18A", "Tax-exempt interest income", data["tax_exempt_income"]),
        ("18B", "Other tax-exempt income", ""),
        ("18C", "Nondeductible expenses", ""),
    ]
    for bn, desc, val in te_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    y -= 0.12 * inch

    # Section: Distributions
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 19  ", "Distributions")
    dist_lines = [
        ("19A", "Cash and marketable securities distributed", data["box19_distributions"]),
        ("19B", "Distribution subject to section 737", ""),
    ]
    for bn, desc, val in dist_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    y -= 0.12 * inch

    # Section: Other Information (20)
    y = draw_section_header(c, margin_l, y, content_w,
                            "Box 20  ", "Other Information")
    other_lines = [
        ("20A", "Investment income", data["box5_interest_income"]),
        ("20B", "Investment expenses", data["investment_interest_expense"]),
        ("20C", "Fuel tax credit information", ""),
        ("20N", "Investment interest expense -- Form 4952", data["investment_interest_expense"]),
        ("20Z", "Section 199A qualified business income", data["section_199a_qbi"]),
    ]
    for bn, desc, val in other_lines:
        y = draw_line_item(c, margin_l, y, box_w, content_w, bn, desc, val)

    # Footer
    c.setFont("Courier", 5.5)
    c.drawString(margin_l, 0.42 * inch,
                 "See attached statements for additional information.")
    c.drawRightString(w - margin_l, 0.42 * inch,
                      f"Schedule K-1 (Form 1065) {data['tax_year']}  Page 2")

    # Outer border
    c.setStrokeColor(black)
    c.setLineWidth(1.2)
    c.rect(margin_l, 0.35 * inch, content_w, h - 0.75 * inch, stroke=1, fill=0)
    c.setLineWidth(0.5)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_k1_pdf(output_path: str | Path, data: dict | None = None) -> Path:
    """Generate a Schedule K-1 PDF and return the output path.

    Parameters
    ----------
    output_path:
        Where to write the PDF file.
    data:
        Dictionary of form values.  Missing keys fall back to DEFAULT_DATA.
    """
    merged = dict(DEFAULT_DATA)
    if data:
        merged.update(data)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    w, h = letter
    c = Canvas(str(output_path), pagesize=letter)
    c.setTitle(f"Schedule K-1 (Form 1065) - {merged['tax_year']}")
    c.setAuthor("IRS (Sample)")
    c.setSubject("Partner's Share of Income, Deductions, Credits, etc.")

    # Page 1
    draw_page1(c, w, h, merged)
    c.showPage()

    # Page 2
    draw_page2(c, w, h, merged)
    c.showPage()

    c.save()
    return output_path


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate a sample IRS Schedule K-1 (Form 1065) PDF."
    )
    parser.add_argument(
        "-o", "--output",
        default="data/input/sample_k1.pdf",
        help="Output PDF path (default: data/input/sample_k1.pdf)",
    )
    args = parser.parse_args()

    path = generate_k1_pdf(args.output)
    print(f"K-1 PDF generated: {path.resolve()}")


if __name__ == "__main__":
    main()
