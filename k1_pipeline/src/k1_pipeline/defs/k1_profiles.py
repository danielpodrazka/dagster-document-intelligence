"""
Consolidated K-1 partner profiles (10 total) and PDF form field mapping.

Profiles 1-5: Investment funds, hedge funds, PE, oil & gas
Profiles 6-10: Family LLC, medical practice, CRE, clean energy, restaurant group

All names, EINs, SSNs, and addresses are entirely fictitious.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Profiles 1-5
# ---------------------------------------------------------------------------

PROFILES_1_5 = [
    # Profile 1 -- Sunbelt Retail Real Estate Fund, LP
    {
        "partnership_name": "Sunbelt Retail Real Estate Fund II, LP",
        "partnership_address": "3200 Southwest Freeway, Suite 1800\nHouston, TX 77027",
        "irs_center": "Ogden, UT",
        "ein": "46-3819204",
        "partner_name": "Margaret L. Okonkwo",
        "partner_address": "5412 Inwood Road\nDallas, TX 75209",
        "ssn": "621-47-8830",
        "is_general_partner": False,
        "entity_type": "Individual",
        "profit_pct": "2.50",
        "loss_pct": "2.50",
        "capital_pct": "2.50",
        "nonrecourse_beginning": "287,450",
        "nonrecourse_ending": "271,880",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "318,500",
        "capital_contributed": "0",
        "capital_net_income": "(42,315)",
        "capital_withdrawals": "(18,000)",
        "capital_ending": "258,185",
        "box1_ordinary_income": "",
        "box2_rental_real_estate": "(42,315)",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "1,840",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "",
        "box12_section_179": "6,200",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "",
        "box18c_nondeductible": "C  875",
        "box19a_distributions": "A  18,000",
        "box20a_investment_income": "1,840",
        "box20b_investment_expenses": "",
        "box20z_qbi": "",
        "box21_foreign_taxes": "",
    },
    # Profile 2 -- Sequoia Ridge Venture Partners III, LP
    {
        "partnership_name": "Sequoia Ridge Venture Partners III, LP",
        "partnership_address": "2882 Sand Hill Road, Suite 240\nMenlo Park, CA 94025",
        "irs_center": "Ogden, UT",
        "ein": "81-2047563",
        "partner_name": "Priya R. Nambiar",
        "partner_address": "725 Forest Avenue\nPalo Alto, CA 94301",
        "ssn": "549-82-1173",
        "is_general_partner": True,
        "entity_type": "Individual",
        "profit_pct": "20.00",
        "loss_pct": "20.00",
        "capital_pct": "1.00",
        "nonrecourse_beginning": "0",
        "nonrecourse_ending": "0",
        "recourse_beginning": "145,000",
        "recourse_ending": "145,000",
        "capital_beginning": "1,284,700",
        "capital_contributed": "0",
        "capital_net_income": "892,340",
        "capital_withdrawals": "(450,000)",
        "capital_ending": "1,727,040",
        "box1_ordinary_income": "127,615",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "360,000",
        "box4c_total_guaranteed": "360,000",
        "box5_interest": "14,220",
        "box6a_ordinary_dividends": "8,440",
        "box6b_qualified_dividends": "6,780",
        "box8_st_capital_gain": "23,190",
        "box9a_lt_capital_gain": "740,885",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "A  487,615",
        "box14c_gross_nonfarm": "C  487,615",
        "box17a_amt": "A  (18,440)",
        "box18c_nondeductible": "C  2,350",
        "box19a_distributions": "A  450,000",
        "box20a_investment_income": "14,220",
        "box20b_investment_expenses": "6,800",
        "box20z_qbi": "Z  127,615",
        "box21_foreign_taxes": "",
    },
    # Profile 3 -- Blackwater Offshore Macro Fund, LP
    {
        "partnership_name": "Blackwater Offshore Macro Fund, LP",
        "partnership_address": "601 Brickell Key Drive, Suite 700\nMiami, FL 33131",
        "irs_center": "Kansas City, MO",
        "ein": "27-6534891",
        "partner_name": "Theodore J. Vanhanen",
        "partner_address": "14 Harbour Court\nGreenwich, CT 06830",
        "ssn": "071-56-4422",
        "is_general_partner": False,
        "entity_type": "Individual",
        "profit_pct": "0.75",
        "loss_pct": "0.75",
        "capital_pct": "0.75",
        "nonrecourse_beginning": "0",
        "nonrecourse_ending": "0",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "148,200",
        "capital_contributed": "100,000",
        "capital_net_income": "(31,740)",
        "capital_withdrawals": "0",
        "capital_ending": "216,460",
        "box1_ordinary_income": "(31,740)",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "18,905",
        "box6a_ordinary_dividends": "4,115",
        "box6b_qualified_dividends": "1,870",
        "box8_st_capital_gain": "(87,330)",
        "box9a_lt_capital_gain": "12,440",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "",
        "box18c_nondeductible": "C  540",
        "box19a_distributions": "",
        "box20a_investment_income": "23,020",
        "box20b_investment_expenses": "11,350",
        "box20z_qbi": "",
        "box21_foreign_taxes": "1,628",
    },
    # Profile 4 -- Ironclad Industrial Buyout Fund IV, LP
    {
        "partnership_name": "Ironclad Industrial Buyout Fund IV, LP",
        "partnership_address": "200 West Street, 21st Floor\nNew York, NY 10282",
        "irs_center": "Ogden, UT",
        "ein": "83-1726450",
        "partner_name": "Robert F. Callahan",
        "partner_address": "88 Overlook Trail\nSummit, NJ 07901",
        "ssn": "138-60-7714",
        "is_general_partner": False,
        "entity_type": "Individual",
        "profit_pct": "5.25",
        "loss_pct": "5.25",
        "capital_pct": "5.25",
        "nonrecourse_beginning": "524,000",
        "nonrecourse_ending": "0",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "3,841,200",
        "capital_contributed": "0",
        "capital_net_income": "2,187,640",
        "capital_withdrawals": "(1,500,000)",
        "capital_ending": "4,528,840",
        "box1_ordinary_income": "87,640",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "32,180",
        "box6a_ordinary_dividends": "21,350",
        "box6b_qualified_dividends": "21,350",
        "box8_st_capital_gain": "(14,220)",
        "box9a_lt_capital_gain": "2,100,000",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (37,800)",
        "box18c_nondeductible": "C  4,200",
        "box19a_distributions": "A  1,500,000",
        "box20a_investment_income": "32,180",
        "box20b_investment_expenses": "18,750",
        "box20z_qbi": "",
        "box21_foreign_taxes": "",
    },
    # Profile 5 -- Permian Basin Royalties & Exploration Partners, LP
    {
        "partnership_name": "Permian Basin Royalties & Exploration Partners, LP",
        "partnership_address": "500 W. Texas Avenue, Suite 1200\nMidland, TX 79701",
        "irs_center": "Ogden, UT",
        "ein": "75-2893041",
        "partner_name": "James D. Whitacre",
        "partner_address": "3901 Mockingbird Lane\nMidland, TX 79703",
        "ssn": "457-31-9962",
        "is_general_partner": True,
        "entity_type": "Individual",
        "profit_pct": "15.00",
        "loss_pct": "15.00",
        "capital_pct": "10.00",
        "nonrecourse_beginning": "0",
        "nonrecourse_ending": "0",
        "recourse_beginning": "412,500",
        "recourse_ending": "387,000",
        "capital_beginning": "892,300",
        "capital_contributed": "0",
        "capital_net_income": "314,780",
        "capital_withdrawals": "(120,000)",
        "capital_ending": "1,087,080",
        "box1_ordinary_income": "194,780",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "180,000",
        "box4c_total_guaranteed": "180,000",
        "box5_interest": "5,620",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "38,400",
        "box12_section_179": "22,500",
        "box13_other_deductions": "67,340",
        "box14a_se_earnings": "A  374,780",
        "box14c_gross_nonfarm": "C  374,780",
        "box17a_amt": "A  (28,650)",
        "box18c_nondeductible": "C  1,890",
        "box19a_distributions": "A  120,000",
        "box20a_investment_income": "5,620",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  194,780",
        "box21_foreign_taxes": "3,215",
    },
]

# ---------------------------------------------------------------------------
# Profiles 6-10
# ---------------------------------------------------------------------------

PROFILES_6_10 = [
    # Profile 6 -- Nakamura Family Investment LLC (Trust, Oregon, NET LOSS)
    {
        "partnership_name": "Nakamura Family Investment LLC",
        "partnership_address": "7821 SW Barbur Boulevard, Suite 310\nPortland, OR 97219",
        "irs_center": "Ogden, UT",
        "ein": "93-2847561",
        "partner_name": "Nakamura 2018 Irrevocable Trust\nc/o Kenji Nakamura, Trustee",
        "partner_address": "4455 NE Fremont Street\nPortland, OR 97213",
        "ssn": "93-7142608",
        "is_general_partner": False,
        "entity_type": "Trust",
        "profit_pct": "22.50",
        "loss_pct": "22.50",
        "capital_pct": "22.50",
        "nonrecourse_beginning": "14,200",
        "nonrecourse_ending": "11,800",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "87,400",
        "capital_contributed": "0",
        "capital_net_income": "(19,315)",
        "capital_withdrawals": "0",
        "capital_ending": "68,085",
        "box1_ordinary_income": "(19,315)",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "1,240",
        "box6a_ordinary_dividends": "3,180",
        "box6b_qualified_dividends": "2,750",
        "box8_st_capital_gain": "(4,620)",
        "box9a_lt_capital_gain": "6,890",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (1,840)",
        "box18c_nondeductible": "",
        "box19a_distributions": "",
        "box20a_investment_income": "4,420",
        "box20b_investment_expenses": "1,875",
        "box20z_qbi": "",
        "box21_foreign_taxes": "",
    },
    # Profile 7 -- Pacific Coast Orthopedic Partners (GP Individual, Arizona)
    {
        "partnership_name": "Pacific Coast Orthopedic Partners, LLP",
        "partnership_address": "2250 East Camelback Road, Suite 450\nPhoenix, AZ 85016",
        "irs_center": "Ogden, UT",
        "ein": "86-3091745",
        "partner_name": "Dr. Priya R. Venkataraman",
        "partner_address": "14820 North Scottsdale Road, Unit 203\nScottsdale, AZ 85254",
        "ssn": "612-74-3891",
        "is_general_partner": True,
        "entity_type": "Individual",
        "profit_pct": "16.67",
        "loss_pct": "16.67",
        "capital_pct": "16.67",
        "nonrecourse_beginning": "0",
        "nonrecourse_ending": "0",
        "recourse_beginning": "48,500",
        "recourse_ending": "41,200",
        "capital_beginning": "312,750",
        "capital_contributed": "25,000",
        "capital_net_income": "198,440",
        "capital_withdrawals": "(175,000)",
        "capital_ending": "361,190",
        "box1_ordinary_income": "148,440",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "120,000",
        "box4c_total_guaranteed": "120,000",
        "box5_interest": "2,810",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "",
        "box12_section_179": "8,340",
        "box13_other_deductions": "",
        "box14a_se_earnings": "A  268,440",
        "box14c_gross_nonfarm": "C  268,440",
        "box17a_amt": "",
        "box18c_nondeductible": "C  4,200",
        "box19a_distributions": "A  175,000",
        "box20a_investment_income": "",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  148,440",
        "box21_foreign_taxes": "",
    },
    # Profile 8 -- Sunbelt CRE Opportunity Fund III (LP Individual, Michigan)
    {
        "partnership_name": "Sunbelt CRE Opportunity Fund III, LP",
        "partnership_address": "One Campus Martius, Suite 1800\nDetroit, MI 48226",
        "irs_center": "Ogden, UT",
        "ein": "38-4702193",
        "partner_name": "Marcus T. Oduya",
        "partner_address": "6340 Orchard Lake Road, Suite 105\nWest Bloomfield, MI 48322",
        "ssn": "384-51-7029",
        "is_general_partner": False,
        "entity_type": "Individual",
        "profit_pct": "4.80",
        "loss_pct": "4.80",
        "capital_pct": "4.80",
        "nonrecourse_beginning": "412,000",
        "nonrecourse_ending": "398,500",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "1,840,000",
        "capital_contributed": "240,000",
        "capital_net_income": "(62,180)",
        "capital_withdrawals": "(48,000)",
        "capital_ending": "1,969,820",
        "box1_ordinary_income": "",
        "box2_rental_real_estate": "(62,180)",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "3,920",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "11,450",
        "box12_section_179": "89,600",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (58,340)",
        "box18c_nondeductible": "",
        "box19a_distributions": "A  48,000",
        "box20a_investment_income": "3,920",
        "box20b_investment_expenses": "2,160",
        "box20z_qbi": "Z  (62,180)",
        "box21_foreign_taxes": "",
    },
    # Profile 9 -- Cascadia Clean Energy Fund LP (Corporation, Virginia)
    {
        "partnership_name": "Cascadia Clean Energy Fund LP",
        "partnership_address": "1760 Reston Parkway, Suite 600\nReston, VA 20190",
        "irs_center": "Ogden, UT",
        "ein": "54-8031297",
        "partner_name": "Dominion Sustainable Capital Corp.",
        "partner_address": "700 East Main Street, 12th Floor\nRichmond, VA 23219",
        "ssn": "54-1967834",
        "is_general_partner": False,
        "entity_type": "Corporation",
        "profit_pct": "12.50",
        "loss_pct": "12.50",
        "capital_pct": "12.50",
        "nonrecourse_beginning": "1,820,000",
        "nonrecourse_ending": "1,745,000",
        "recourse_beginning": "0",
        "recourse_ending": "0",
        "capital_beginning": "2,150,000",
        "capital_contributed": "500,000",
        "capital_net_income": "(387,620)",
        "capital_withdrawals": "0",
        "capital_ending": "2,262,380",
        "box1_ordinary_income": "(387,620)",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "8,740",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (312,400)",
        "box18c_nondeductible": "C  14,500",
        "box19a_distributions": "",
        "box20a_investment_income": "8,740",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  (387,620)",
        "box21_foreign_taxes": "47,380",
    },
    # Profile 10 -- Southern Hospitality Restaurant Group (S-Corp GP, Georgia)
    {
        "partnership_name": "Southern Hospitality Restaurant Group, LLC",
        "partnership_address": "3080 Peachtree Road NW, Suite 900\nAtlanta, GA 30305",
        "irs_center": "Ogden, UT",
        "ein": "58-2614039",
        "partner_name": "Peach State Holdings, Inc.",
        "partner_address": "1200 Abernathy Road NE, Suite 1700\nAtlanta, GA 30328",
        "ssn": "58-3801562",
        "is_general_partner": True,
        "entity_type": "S Corporation",
        "profit_pct": "35.00",
        "loss_pct": "35.00",
        "capital_pct": "35.00",
        "nonrecourse_beginning": "0",
        "nonrecourse_ending": "0",
        "recourse_beginning": "62,400",
        "recourse_ending": "58,100",
        "capital_beginning": "15,000",
        "capital_contributed": "0",
        "capital_net_income": "28,945",
        "capital_withdrawals": "(18,000)",
        "capital_ending": "25,945",
        "box1_ordinary_income": "28,945",
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "",
        "box12_section_179": "",
        "box13_other_deductions": "",
        "box14a_se_earnings": "",
        "box14c_gross_nonfarm": "",
        "box17a_amt": "",
        "box18c_nondeductible": "",
        "box19a_distributions": "A  18,000",
        "box20a_investment_income": "",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  28,945",
        "box21_foreign_taxes": "",
    },
]

# ---------------------------------------------------------------------------
# All 10 profiles
# ---------------------------------------------------------------------------

ALL_PROFILES = PROFILES_1_5 + PROFILES_6_10


def profile_to_fill_data(profile: dict) -> dict:
    """Map a K-1 profile dict to IRS Schedule K-1 (Form 1065) PDF field names.

    Returns a dict suitable for passing to PyPDFForm's PdfWrapper.fill().
    """
    fill: dict = {}

    # --- Part I: Information About the Partnership ---
    fill["f1_6[0]"] = profile["ein"]
    fill["f1_7[0]"] = f"{profile['partnership_name']}\n{profile['partnership_address']}"
    fill["f1_8[0]"] = profile["irs_center"]

    # --- Part II: Information About the Partner ---
    fill["f1_9[0]"] = profile["ssn"]
    fill["f1_10[0]"] = f"{profile['partner_name']}\n{profile['partner_address']}"

    # G: General / Limited partner checkbox
    if profile["is_general_partner"]:
        fill["c1_4[0]"] = True   # General partner or LLC member-manager

    # H1: Domestic partner (all profiles are domestic)
    fill["c1_5[0]"] = True

    # I: Entity type
    fill["f1_13[0]"] = profile["entity_type"]

    # J: Share percentages (beginning = ending for these profiles)
    fill["f1_14[0]"] = profile["profit_pct"]
    fill["f1_15[0]"] = profile["profit_pct"]
    fill["f1_16[0]"] = profile["loss_pct"]
    fill["f1_17[0]"] = profile["loss_pct"]
    fill["f1_18[0]"] = profile["capital_pct"]
    fill["f1_19[0]"] = profile["capital_pct"]

    # K1: Partner share of liabilities
    fill["f1_20[0]"] = profile["nonrecourse_beginning"]
    fill["f1_21[0]"] = profile["nonrecourse_ending"]
    fill["f1_24[0]"] = profile["recourse_beginning"]
    fill["f1_25[0]"] = profile["recourse_ending"]

    # L: Capital Account Analysis
    fill["f1_26[0]"] = profile["capital_beginning"]
    fill["f1_27[0]"] = profile["capital_contributed"]
    fill["f1_28[0]"] = profile["capital_net_income"]
    # Withdrawals: strip parentheses since the form field implies deduction
    raw_wd = profile["capital_withdrawals"]
    fill["f1_30[0]"] = raw_wd.strip("()") if raw_wd else "0"
    fill["f1_31[0]"] = profile["capital_ending"]
    fill["c1_8[0]"] = True  # Tax basis method

    # --- Part III: Income/Deduction boxes (only fill non-empty) ---
    _BOX_TO_FIELD = {
        "box1_ordinary_income": "f1_34[0]",
        "box2_rental_real_estate": "f1_35[0]",
        "box4a_guaranteed_services": "f1_37[0]",
        "box4c_total_guaranteed": "f1_39[0]",
        "box5_interest": "f1_40[0]",
        "box6a_ordinary_dividends": "f1_41[0]",
        "box6b_qualified_dividends": "f1_42[0]",
        "box8_st_capital_gain": "f1_45[0]",
        "box9a_lt_capital_gain": "f1_46[0]",
        "box12_section_179": "f1_54[0]",
        "box13_other_deductions": "f1_55[0]",
        "box14a_se_earnings": "f1_60[0]",
        "box14c_gross_nonfarm": "f1_61[0]",
        "box17a_amt": "f1_79[0]",
        "box18c_nondeductible": "f1_84[0]",
        "box19a_distributions": "f1_89[0]",
        "box20a_investment_income": "f1_92[0]",
        "box20b_investment_expenses": "f1_93[0]",
        "box20z_qbi": "f1_94[0]",
        "box21_foreign_taxes": "f1_95[0]",
    }

    for profile_key, form_field in _BOX_TO_FIELD.items():
        value = profile.get(profile_key, "")
        if value:
            fill[form_field] = value

    return fill
