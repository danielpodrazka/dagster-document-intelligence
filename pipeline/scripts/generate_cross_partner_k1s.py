#!/usr/bin/env python3
"""Generate cross-partner test K-1 PDFs for multi-partner and multi-year validation.

Downloads both the 2023 and 2024 IRS Schedule K-1 (Form 1065) blank forms,
then fills each profile using PyPDFForm. Outputs go to data/input/batch/cross_partner/.

Profiles 11-14: 2024 form (same-partnership partner tests)
Profiles 15-18: 2023 form (multi-year continuity tests)

Usage:
    cd pipeline && uv run python scripts/generate_cross_partner_k1s.py
"""

from __future__ import annotations

import json
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from PyPDFForm import PdfWrapper

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_INPUT = PROJECT_ROOT / "data" / "input"
CROSS_PARTNER_DIR = DATA_INPUT / "batch" / "cross_partner"
ARCHIVE_DIR = DATA_INPUT / "archive"

IRS_K1_FORM_URLS = {
    "2024": "https://www.irs.gov/pub/irs-prior/f1065sk1--2024.pdf",
    "2023": "https://www.irs.gov/pub/irs-prior/f1065sk1--2023.pdf",
}


def _ensure_blank_form(year: str) -> Path:
    """Download the blank IRS K-1 form for the given year if not already cached."""
    form_path = ARCHIVE_DIR / f"irs_k1_{year}.pdf"
    if form_path.exists():
        return form_path
    form_path.parent.mkdir(parents=True, exist_ok=True)
    url = IRS_K1_FORM_URLS[year]
    print(f"Downloading blank IRS K-1 {year} form from {url}...")
    urllib.request.urlretrieve(url, str(form_path))
    print(f"Saved to {form_path}")
    return form_path


def _profile_to_form_fields(profile: dict) -> dict:
    """Map a profile data dict to IRS K-1 PDF form field names."""
    fields: dict = {}

    # --- Part I: Partnership ---
    fields["f1_6[0]"] = profile["ein"]
    fields["f1_7[0]"] = f"{profile['partnership_name']}\n{profile['partnership_address']}"
    fields["f1_8[0]"] = profile["irs_center"]

    # --- Part II: Partner ---
    fields["f1_9[0]"] = profile["ssn"]
    fields["f1_10[0]"] = f"{profile['partner_name']}\n{profile['partner_address']}"

    # G: General vs Limited partner checkbox
    if profile["is_general_partner"]:
        fields["c1_4[0]"] = True   # General partner
    else:
        fields["c1_4[1]"] = True   # Limited partner

    # H1: Domestic partner
    fields["c1_5[0]"] = True

    # I1: Entity type
    fields["f1_13[0]"] = profile["entity_type"]

    # J: Share percentages (same for beginning and ending)
    fields["f1_14[0]"] = profile["profit_pct"]
    fields["f1_15[0]"] = profile["profit_pct"]
    fields["f1_16[0]"] = profile["loss_pct"]
    fields["f1_17[0]"] = profile["loss_pct"]
    fields["f1_18[0]"] = profile["capital_pct"]
    fields["f1_19[0]"] = profile["capital_pct"]

    # K: Liabilities
    fields["f1_20[0]"] = profile["nonrecourse_beginning"]
    fields["f1_21[0]"] = profile["nonrecourse_ending"]
    fields["f1_24[0]"] = profile["recourse_beginning"]
    fields["f1_25[0]"] = profile["recourse_ending"]

    # L: Capital Account
    fields["f1_26[0]"] = profile["capital_beginning"]
    fields["f1_27[0]"] = profile["capital_contributed"]
    fields["f1_28[0]"] = profile["capital_net_income"]
    withdrawals = profile["capital_withdrawals"]
    if withdrawals and withdrawals != "0":
        fields["f1_30[0]"] = withdrawals.strip("()")
    fields["f1_31[0]"] = profile["capital_ending"]
    fields["c1_8[0]"] = True  # Tax basis

    # --- Part III: Income / Loss / Deductions ---
    _set_if(fields, "f1_34[0]", profile["box1_ordinary_income"])
    _set_if(fields, "f1_35[0]", profile["box2_rental_real_estate"])
    _set_if(fields, "f1_37[0]", profile["box4a_guaranteed_services"])
    _set_if(fields, "f1_39[0]", profile["box4c_total_guaranteed"])
    _set_if(fields, "f1_40[0]", profile["box5_interest"])
    _set_if(fields, "f1_41[0]", profile["box6a_ordinary_dividends"])
    _set_if(fields, "f1_42[0]", profile["box6b_qualified_dividends"])
    _set_if(fields, "f1_45[0]", profile["box8_st_capital_gain"])
    _set_if(fields, "f1_46[0]", profile["box9a_lt_capital_gain"])
    _set_if(fields, "f1_54[0]", profile["box12_section_179"])
    _set_if(fields, "f1_55[0]", profile["box13_other_deductions"])

    # Box 14: Self-employment
    _set_if(fields, "f1_60[0]", profile["box14a_se_earnings"])
    _set_if(fields, "f1_61[0]", profile["box14c_gross_nonfarm"])

    # Box 17: AMT
    _set_if(fields, "f1_79[0]", profile["box17a_amt"])

    # Box 18: Tax-exempt / nondeductible
    _set_if(fields, "f1_84[0]", profile["box18c_nondeductible"])

    # Box 19: Distributions
    _set_if(fields, "f1_89[0]", profile["box19a_distributions"])

    # Box 20: Other information
    _set_if(fields, "f1_92[0]", profile["box20a_investment_income"])
    _set_if(fields, "f1_93[0]", profile["box20b_investment_expenses"])
    _set_if(fields, "f1_94[0]", profile["box20z_qbi"])

    # Box 21: Foreign taxes
    if profile.get("box21_foreign_taxes"):
        fields["f1_95[0]"] = profile["box21_foreign_taxes"]

    return fields


def _set_if(fields: dict, key: str, value: str) -> None:
    """Set a form field only if the value is non-empty."""
    if value:
        fields[key] = value


def _slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


def generate_cross_partner_k1s() -> list[dict]:
    """Generate cross-partner test K-1 PDFs. Returns manifest entries."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from k1_cross_partner_profiles import CROSS_PARTNER_PROFILES

    # Pre-download both form versions
    blank_forms = {
        year: _ensure_blank_form(year)
        for year in IRS_K1_FORM_URLS
    }

    CROSS_PARTNER_DIR.mkdir(parents=True, exist_ok=True)

    manifest_entries = []
    for profile in CROSS_PARTNER_PROFILES:
        profile_num = profile["profile_number"]
        form_version = profile["form_version"]
        blank = blank_forms[form_version]

        partner_slug = _slugify(profile["partner_name"])
        partnership_slug = _slugify(profile["partnership_name"])
        year = profile["tax_year"]
        filename = f"profile_{profile_num:02d}_{partnership_slug}_{year}.pdf"
        output_path = CROSS_PARTNER_DIR / filename

        form_fields = _profile_to_form_fields(profile)
        filled = PdfWrapper(str(blank)).fill(form_fields)
        with open(output_path, "wb") as f:
            f.write(filled.read())

        entry = {
            "profile_number": profile_num,
            "filename": filename,
            "tax_year": year,
            "form_version": form_version,
            "partnership_name": profile["partnership_name"],
            "partnership_ein": profile["ein"],
            "partner_name": profile["partner_name"],
            "entity_type": profile["entity_type"],
            "is_general_partner": profile["is_general_partner"],
            "profit_pct": profile["profit_pct"],
            "fields_filled": len(form_fields),
        }
        manifest_entries.append(entry)
        print(f"  [{profile_num:2d}] {filename}")

    # Write manifest
    manifest = {
        "description": "Cross-partner validation test K-1s",
        "total_profiles": len(manifest_entries),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": manifest_entries,
    }
    manifest_path = CROSS_PARTNER_DIR / "cross_partner_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {manifest_path}")

    return manifest_entries


if __name__ == "__main__":
    print("Generating cross-partner test K-1 PDFs...\n")
    entries = generate_cross_partner_k1s()
    print(f"\nDone. {len(entries)} PDFs written to {CROSS_PARTNER_DIR}")
