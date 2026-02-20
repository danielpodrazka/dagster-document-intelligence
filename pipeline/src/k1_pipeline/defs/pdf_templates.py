"""
PDF Report Templates for K-1 Pipeline

Generates professional PDF reports using WeasyPrint (HTML -> PDF).
Theme: navy/white/black matching the frontend dashboard.
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Color palette (matches frontend index.css)
# ---------------------------------------------------------------------------
NAVY = "#0c1e3a"
NAVY_600 = "#152d50"
NAVY_50 = "#eef2f7"
POSITIVE = "#1a6b42"
NEGATIVE = "#a12029"
BORDER = "#dfe3ea"
TEXT_SECONDARY = "#4a556b"
TEXT_MUTED = "#8893a7"


def _fmt_currency(value) -> str:
    """Format a number as currency, handling None and negatives."""
    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if v < 0:
        return f'<span style="color:{NEGATIVE}">(${ abs(v):,.0f})</span>'
    return f"${v:,.0f}"


def _fmt_pct(value) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _fmt_ratio(value) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.2f}x"
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Base CSS
# ---------------------------------------------------------------------------

def _base_css() -> str:
    return f"""
    @page {{
        size: letter;
        margin: 0.75in;
        @bottom-center {{
            content: "Confidential";
            font-size: 8pt;
            color: {TEXT_MUTED};
        }}
        @bottom-right {{
            content: "Page " counter(page) " of " counter(pages);
            font-size: 8pt;
            color: {TEXT_MUTED};
        }}
    }}
    @page landscape {{
        size: letter landscape;
        margin: 0.5in 0.6in;
    }}

    body {{
        font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-size: 10pt;
        color: {NAVY};
        line-height: 1.5;
    }}

    .page-break {{
        page-break-before: always;
    }}

    /* Header */
    .report-header {{
        background: {NAVY};
        color: white;
        padding: 24px 32px;
        margin: -0.75in -0.75in 24px -0.75in;
        /* stretch to page edges */
    }}
    .report-header h1 {{
        font-size: 20pt;
        font-weight: 700;
        margin: 0 0 4px 0;
    }}
    .report-header .subtitle {{
        font-size: 11pt;
        opacity: 0.85;
    }}

    /* Section */
    .section {{
        margin-bottom: 20px;
    }}
    .section-title {{
        font-size: 12pt;
        font-weight: 600;
        color: {NAVY};
        border-bottom: 2px solid {NAVY};
        padding-bottom: 4px;
        margin-bottom: 10px;
    }}

    /* Tables */
    table {{
        width: 100%;
        border-collapse: collapse;
        margin-bottom: 14px;
        font-size: 9.5pt;
    }}
    th {{
        background: {NAVY_50};
        color: {NAVY};
        font-weight: 600;
        text-align: left;
        padding: 6px 10px;
        border-bottom: 2px solid {BORDER};
    }}
    td {{
        padding: 5px 10px;
        border-bottom: 1px solid {BORDER};
    }}
    tr:last-child td {{
        border-bottom: none;
    }}
    .amount {{
        text-align: right;
        font-variant-numeric: tabular-nums;
    }}

    /* Summary boxes */
    .summary-row {{
        display: flex;
        gap: 16px;
        margin-bottom: 16px;
    }}
    .summary-box {{
        flex: 1;
        background: {NAVY_50};
        border: 1px solid {BORDER};
        border-radius: 6px;
        padding: 12px 16px;
        text-align: center;
    }}
    .summary-box .label {{
        font-size: 8pt;
        color: {TEXT_SECONDARY};
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    .summary-box .value {{
        font-size: 16pt;
        font-weight: 700;
        color: {NAVY};
        margin-top: 2px;
    }}

    /* Lists */
    .obs-list {{
        padding-left: 18px;
        margin-bottom: 12px;
    }}
    .obs-list li {{
        margin-bottom: 4px;
    }}

    /* Footer note */
    .footer-note {{
        font-size: 8pt;
        color: {TEXT_MUTED};
        border-top: 1px solid {BORDER};
        padding-top: 8px;
        margin-top: 24px;
    }}

    /* Badge */
    .badge {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 8pt;
        font-weight: 600;
    }}
    .badge-gp {{
        background: {NAVY};
        color: white;
    }}
    .badge-lp {{
        background: {NAVY_50};
        color: {NAVY};
    }}
    .badge-success {{
        background: #e6f4ed;
        color: {POSITIVE};
    }}
    .badge-error {{
        background: #fde8e8;
        color: {NEGATIVE};
    }}
    """


# ---------------------------------------------------------------------------
# K-1 fields table rows (reused in single + batch)
# ---------------------------------------------------------------------------

_K1_FIELDS = [
    ("Tax Year", "tax_year", None),
    ("Partnership Name", "partnership_name", None),
    ("Partner Type", "partner_type", None),
    ("Share Percentage", "partner_share_percentage", "pct"),
    ("Box 1 — Ordinary Business Income", "ordinary_business_income", "currency"),
    ("Box 2 — Rental Real Estate Income", "rental_real_estate_income", "currency"),
    ("Box 4 — Guaranteed Payments", "guaranteed_payments", "currency"),
    ("Box 5 — Interest Income", "interest_income", "currency"),
    ("Box 6a — Ordinary Dividends", "ordinary_dividends", "currency"),
    ("Box 6b — Qualified Dividends", "qualified_dividends", "currency"),
    ("Box 8 — Short-Term Capital Gains", "short_term_capital_gains", "currency"),
    ("Box 9a — Long-Term Capital Gains", "long_term_capital_gains", "currency"),
    ("Box 12 — Section 179 Deduction", "section_179_deduction", "currency"),
    ("Box 14 — Self-Employment Earnings", "self_employment_earnings", "currency"),
    ("Box 16 — Foreign Taxes Paid", "foreign_taxes_paid", "currency"),
    ("Box 19 — Distributions", "distributions", "currency"),
    ("Box 20z — QBI Deduction", "qbi_deduction", "currency"),
    ("Capital Account — Beginning", "capital_account_beginning", "currency"),
    ("Capital Account — Ending", "capital_account_ending", "currency"),
]


def _k1_table_html(k1_data: dict) -> str:
    rows = []
    for label, key, fmt in _K1_FIELDS:
        val = k1_data.get(key)
        if fmt == "currency":
            display = _fmt_currency(val)
        elif fmt == "pct":
            display = _fmt_pct(val)
        elif val is None:
            display = "—"
        else:
            display = str(val)
        css_class = ' class="amount"' if fmt in ("currency", "pct") else ""
        rows.append(f"<tr><td>{label}</td><td{css_class}>{display}</td></tr>")
    return "\n".join(rows)


def _analysis_section_html(analysis: dict) -> str:
    """Render financial analysis as HTML."""
    parts = []

    # Summary figures
    parts.append('<div class="summary-row">')
    for label, key in [
        ("Total Income", "total_income"),
        ("Total Deductions", "total_deductions"),
        ("Net Taxable Income", "net_taxable_income"),
    ]:
        parts.append(f"""
        <div class="summary-box">
            <div class="label">{label}</div>
            <div class="value">{_fmt_currency(analysis.get(key))}</div>
        </div>""")
    parts.append("</div>")

    # Distribution ratio
    ratio = analysis.get("distribution_vs_income_ratio")
    if ratio is not None:
        parts.append(f'<p><strong>Distribution / Income Ratio:</strong> {_fmt_ratio(ratio)}</p>')

    # Capital gains summary
    cg = analysis.get("capital_gains_summary")
    if cg:
        parts.append(f'<p><strong>Capital Gains:</strong> {cg}</p>')

    # Tax considerations
    tc = analysis.get("effective_tax_considerations")
    if tc:
        parts.append(f'<p><strong>Tax Considerations:</strong> {tc}</p>')

    # Key observations
    obs = analysis.get("key_observations", [])
    if obs:
        parts.append('<div class="section-title">Key Observations</div>')
        parts.append('<ul class="obs-list">')
        for o in obs:
            parts.append(f"<li>{o}</li>")
        parts.append("</ul>")

    # Recommendations
    recs = analysis.get("tax_planning_recommendations", [])
    if recs:
        parts.append('<div class="section-title">Tax Planning Recommendations</div>')
        parts.append('<ul class="obs-list">')
        for r in recs:
            parts.append(f"<li>{r}</li>")
        parts.append("</ul>")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Single-file report
# ---------------------------------------------------------------------------

def render_single_report_html(
    k1_data: dict,
    analysis: dict,
    pii_stats: dict,
    metadata: dict,
) -> str:
    """Render a single K-1 analysis report as HTML."""

    partnership = k1_data.get("partnership_name", "K-1 Partnership")
    tax_year = k1_data.get("tax_year", "")
    generated = metadata.get("report_generated_at", "")

    pii_total = pii_stats.get("total_entities_detected", 0)
    pii_redacted = pii_stats.get("entities_redacted", 0)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><style>{_base_css()}</style></head>
<body>

<div class="report-header">
    <h1>K-1 Analysis Report</h1>
    <div class="subtitle">{partnership} {("— " + tax_year) if tax_year else ""}</div>
</div>

<div class="section">
    <div class="section-title">Extracted K-1 Data</div>
    <table>
        <tr><th>Field</th><th class="amount">Value</th></tr>
        {_k1_table_html(k1_data)}
    </table>
</div>

<div class="section">
    <div class="section-title">PII Detection Summary</div>
    <p><strong>{pii_total}</strong> PII entities detected and <strong>{pii_redacted}</strong> redacted before AI processing.</p>
</div>

<div class="page-break"></div>

<div class="section">
    <div class="section-title">Financial Analysis</div>
    {_analysis_section_html(analysis)}
</div>

<div class="footer-note">
    Generated {generated} | K-1 Document Intelligence Pipeline v1.0
</div>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Processing overview report
# ---------------------------------------------------------------------------

def render_overview_html(reports: list[dict], generated_at: str) -> str:
    """Render an overview PDF summarizing all processed K-1 reports."""

    total = len(reports)

    # -- Summary table rows --
    table_rows = []
    total_income_sum = 0.0
    total_net_sum = 0.0

    for i, r in enumerate(reports, 1):
        k1 = r.get("k1_data", {})
        fa = r.get("financial_analysis", {})
        name = k1.get("partnership_name", "—")
        tax_year = k1.get("tax_year", "—")
        partner_type_raw = k1.get("partner_type", "—") or "—"
        pt = partner_type_raw.lower()
        if "general" in pt and "limited" in pt:
            partner_type = "GP/LP"
        elif "general" in pt:
            partner_type = "GP"
        elif "limited" in pt:
            partner_type = "LP"
        else:
            partner_type = partner_type_raw
        income = fa.get("total_income")
        net = fa.get("net_taxable_income")
        distributions = k1.get("distributions")
        processed_raw = r.get("processed_at", "—")
        # Shorten ISO timestamp to "YYYY-MM-DD HH:MM"
        if processed_raw and processed_raw != "—":
            processed = processed_raw[:16].replace("T", " ")
        else:
            processed = "—"

        if income is not None:
            total_income_sum += float(income)
        if net is not None:
            total_net_sum += float(net)

        table_rows.append(f"""<tr>
            <td>{i}</td>
            <td class="wrap">{name}</td>
            <td>{tax_year}</td>
            <td>{partner_type}</td>
            <td class="amount">{_fmt_currency(income)}</td>
            <td class="amount">{_fmt_currency(net)}</td>
            <td class="amount">{_fmt_currency(distributions)}</td>
            <td>{processed}</td>
        </tr>""")

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><style>
{_base_css()}
body {{ page: overview; }}
@page overview {{
    size: letter landscape;
    margin: 0.5in 0.6in;
}}
.overview-table {{
    font-size: 8.5pt;
}}
.overview-table th, .overview-table td {{
    padding: 5px 8px;
    white-space: nowrap;
}}
.overview-table td.wrap {{
    white-space: normal;
}}
</style></head>
<body>

<div class="report-header">
    <h1>K-1 Processing Overview</h1>
    <div class="subtitle">{total} Document{"s" if total != 1 else ""} Processed</div>
</div>

<div class="summary-row">
    <div class="summary-box">
        <div class="label">Documents Processed</div>
        <div class="value">{total}</div>
    </div>
    <div class="summary-box">
        <div class="label">Aggregate Income</div>
        <div class="value">{_fmt_currency(total_income_sum)}</div>
    </div>
    <div class="summary-box">
        <div class="label">Aggregate Net Taxable</div>
        <div class="value">{_fmt_currency(total_net_sum)}</div>
    </div>
</div>

<div class="section">
    <div class="section-title">Processed Documents</div>
    <table class="overview-table">
        <tr>
            <th>#</th>
            <th>Partnership</th>
            <th>Year</th>
            <th>Type</th>
            <th class="amount">Total Income</th>
            <th class="amount">Net Taxable</th>
            <th class="amount">Distributions</th>
            <th>Processed</th>
        </tr>
        {"".join(table_rows)}
    </table>
</div>

<div class="footer-note">
    Generated {generated_at[:16].replace("T", " ") if generated_at else ""} | K-1 Document Intelligence Pipeline v1.0
</div>

</body>
</html>"""


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

def generate_pdf(html: str, output_path: Path) -> Path:
    """Convert HTML string to PDF using WeasyPrint."""
    from weasyprint import HTML

    output_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html).write_pdf(str(output_path))
    return output_path
