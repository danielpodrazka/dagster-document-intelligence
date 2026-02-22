"""Fill an official IRS Schedule K-1 (Form 1065) 2024 PDF with sample data.

Downloads the blank form from irs.gov (if not cached) and fills it
programmatically using PyPDFForm. This produces a realistic filled K-1
that can be run through the OCR/PII/AI pipeline to test real-world performance.
"""

from pathlib import Path
from PyPDFForm import PdfWrapper

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_INPUT = PROJECT_ROOT / "data" / "input"

BLANK_FORM = DATA_INPUT / "irs_k1_2024.pdf"
FILLED_OUTPUT = DATA_INPUT / "irs_k1_filled.pdf"

# Same fake PII data as our generated K-1 for comparison
FILL_DATA = {
    # --- Top date section ---
    # "beginning" and "ending" only needed for fiscal year; blank = calendar year 2024

    # --- Part I: Information About the Partnership ---
    "f1_6[0]": "82-4571903",                          # A: EIN
    "f1_7[0]": (
        "Meridian Capital Growth Fund, LP\n"
        "450 Park Avenue, Suite 2100\n"
        "New York, NY 10022"
    ),                                                  # B: Name + address
    "f1_8[0]": "Ogden, UT",                           # C: IRS center

    # --- Part II: Information About the Partner ---
    "f1_9[0]": "478-93-6215",                         # E: Partner SSN
    "f1_10[0]": (
        "Jonathan A. Blackwell\n"
        "1847 Oakridge Drive\n"
        "Greenwich, CT 06831"
    ),                                                  # F: Name + address
    "c1_4[0]": True,                                   # G: General partner (checked)
    "c1_5[0]": True,                                   # H1: Domestic partner (checked)
    "f1_13[0]": "Individual",                          # I1: Entity type

    # J: Partner share of profit, loss, capital
    "f1_14[0]": "3.75",   "f1_15[0]": "3.75",         # Profit: beginning/ending %
    "f1_16[0]": "3.75",   "f1_17[0]": "3.75",         # Loss: beginning/ending %
    "f1_18[0]": "3.75",   "f1_19[0]": "3.75",         # Capital: beginning/ending %

    # K1: Partner share of liabilities
    "f1_20[0]": "38,750",  "f1_21[0]": "38,750",      # Nonrecourse
    "f1_24[0]": "12,500",  "f1_25[0]": "12,500",      # Recourse

    # L: Capital Account Analysis
    "f1_26[0]": "542,100",                             # Beginning capital account
    "f1_27[0]": "50,000",                              # Capital contributed
    "f1_28[0]": "244,145",                             # Current year net income
    "f1_30[0]": "95,000",                              # Withdrawals & distributions
    "f1_31[0]": "741,245",                             # Ending capital account

    # c1_8: Capital account method (Tax basis)
    "c1_8[0]": True,                                   # Tax basis

    # --- Part III: Partner's Share of Current Year Income ---
    "f1_34[0]": "127,450",                             # Box 1: Ordinary business income
    "f1_35[0]": "(18,200)",                            # Box 2: Net rental real estate
    "f1_37[0]": "45,000",                              # Box 4a: Guaranteed payments for services
    "f1_39[0]": "45,000",                              # Box 4c: Total guaranteed payments
    "f1_40[0]": "8,325",                               # Box 5: Interest income
    "f1_41[0]": "12,780",                              # Box 6a: Ordinary dividends
    "f1_42[0]": "9,150",                               # Box 6b: Qualified dividends
    "f1_45[0]": "(3,400)",                             # Box 8: Net short-term capital gain
    "f1_46[0]": "67,890",                              # Box 9a: Net long-term capital gain
    "f1_54[0]": "4,200",                               # Box 12: Section 179 deduction

    # Box 13: Other deductions
    "f1_55[0]": "15,000",                              # Other deductions amount

    # Box 14: Self-employment earnings
    "f1_60[0]": "A  172,450",                          # 14A: Net SE earnings
    "f1_61[0]": "C  172,450",                          # 14C: Gross non-farm

    # Box 17: AMT items
    "f1_79[0]": "A  (2,300)",                          # 17A: Post-1986 depreciation

    # Box 18: Tax-exempt income
    "f1_84[0]": "C  3,100",                            # 18C: Nondeductible expenses

    # Box 19: Distributions
    "f1_89[0]": "A  95,000",                           # 19A: Cash distributions

    # Box 20: Other information
    "f1_92[0]": "A  8,325",                            # 20A: Investment income
    "f1_93[0]": "B  4,500",                            # 20B: Investment expenses
    "f1_94[0]": "Z  127,450",                          # 20Z: QBI
}


def fill_k1(output_path: Path | None = None) -> Path:
    """Fill the IRS K-1 form and return the output path."""
    if not BLANK_FORM.exists():
        raise FileNotFoundError(f"Blank IRS K-1 form not found at {BLANK_FORM}")

    out = output_path or FILLED_OUTPUT
    filled = PdfWrapper(str(BLANK_FORM)).fill(FILL_DATA)
    out.write_bytes(filled.read())

    print(f"Filled K-1 written to {out}")
    return out


if __name__ == "__main__":
    fill_k1()
