#!/usr/bin/env python3
"""Generate all 10 test K-1 PDFs by filling the official IRS form with profile data.

Downloads the blank IRS Schedule K-1 (Form 1065) 2024 if not cached, then fills
it once per profile using PyPDFForm. Outputs go to data/input/batch/.

Usage:
    cd pipeline && uv run python scripts/generate_batch_k1s.py
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
BATCH_DIR = DATA_INPUT / "batch"
BLANK_FORM = DATA_INPUT / "archive" / "irs_k1_2024.pdf"
IRS_K1_FORM_URL = "https://www.irs.gov/pub/irs-prior/f1065sk1--2024.pdf"


def _ensure_blank_form() -> Path:
    """Download the blank IRS K-1 form if not already cached."""
    if BLANK_FORM.exists():
        return BLANK_FORM
    BLANK_FORM.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading blank IRS K-1 form from {IRS_K1_FORM_URL}...")
    urllib.request.urlretrieve(IRS_K1_FORM_URL, str(BLANK_FORM))
    print(f"Saved to {BLANK_FORM}")
    return BLANK_FORM


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
    # Strip parentheses for the withdrawal field (it's always a withdrawal)
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

    # Box 14: Self-employment (multi-line codes)
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

    # Box 21: Foreign taxes (mapped to f1_95 if present)
    if profile.get("box21_foreign_taxes"):
        fields["f1_95[0]"] = profile["box21_foreign_taxes"]

    return fields


def _set_if(fields: dict, key: str, value: str) -> None:
    """Set a form field only if the value is non-empty."""
    if value:
        fields[key] = value


def _slugify(name: str) -> str:
    """Convert a partnership name to a filesystem-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


def generate_all() -> list[dict]:
    """Generate all 10 profile K-1 PDFs. Returns manifest entries."""
    # Import profile data (co-located in scripts/)
    import sys
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from k1_profiles_1_5 import PROFILES_1_5
    from k1_profiles_6_10 import PROFILES_6_10

    all_profiles = PROFILES_1_5 + PROFILES_6_10
    blank = _ensure_blank_form()
    BATCH_DIR.mkdir(parents=True, exist_ok=True)

    manifest_entries = []
    for i, profile in enumerate(all_profiles, 1):
        slug = _slugify(profile["partnership_name"])
        filename = f"profile_{i:02d}_{slug}.pdf"
        output_path = BATCH_DIR / filename

        form_fields = _profile_to_form_fields(profile)
        filled = PdfWrapper(str(blank)).fill(form_fields)
        output_path.write_bytes(filled.read())

        entry = {
            "profile_number": i,
            "filename": filename,
            "partnership_name": profile["partnership_name"],
            "partner_name": profile["partner_name"],
            "entity_type": profile["entity_type"],
            "is_general_partner": profile["is_general_partner"],
            "fields_filled": len(form_fields),
        }
        manifest_entries.append(entry)
        print(f"  [{i:2d}/10] {filename}")

    # Write manifest
    manifest = {
        "total_profiles": len(manifest_entries),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": manifest_entries,
    }
    manifest_path = BATCH_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {manifest_path}")

    return manifest_entries


if __name__ == "__main__":
    print("Generating 10 K-1 PDFs from official IRS form...\n")
    entries = generate_all()
    print(f"\nDone. {len(entries)} PDFs written to {BATCH_DIR}")
