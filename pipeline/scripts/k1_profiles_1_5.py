"""
Realistic Schedule K-1 (Form 1065) partner profiles for testing.

Five distinct real-world scenarios:
  1. Sunbelt Retail Real Estate Fund, LP  -- Limited partner, heavy rental losses, passive activity
  2. Sequoia Ridge Venture Partners III, LP -- General partner (fund manager), large carried interest
  3. Blackwater Offshore Macro Fund, LP -- LP in hedge fund, short-term losses, foreign taxes
  4. Ironclad Industrial Buyout Fund IV, LP -- LP in private equity, massive LT capital gain year
  5. Permian Basin Royalties & Exploration, LP -- GP in oil & gas, depletion, AMT adjustments

All EINs, SSNs, addresses, and financial figures are entirely fictitious.
Dollar amounts follow IRS K-1 convention: negatives as "(amount)", positives plain.
Multi-code box entries (14, 17, 18, 19, 20) use "CODE  amount" format per form instructions.
"""

PROFILES_1_5 = [
    # =========================================================================
    # PROFILE 1 -- Sunbelt Retail Real Estate Fund, LP
    # Real estate private equity fund; LP interest acquired at inception.
    # Year characterized by heavy rental real estate losses (passive), modest
    # interest income, and a small guaranteed management fee waiver reallocation.
    # No foreign taxes. Significant nonrecourse mortgage liabilities.
    # =========================================================================
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

    # =========================================================================
    # PROFILE 2 -- Sequoia Ridge Venture Partners III, LP
    # Early-stage VC fund. This partner is the GENERAL PARTNER (fund manager
    # entity). Receives a substantial management fee (guaranteed payments) plus
    # carried interest (LT cap gains). Ordinary income from portfolio company
    # operations flows through. High capital account from years of retained gains.
    # No rental real estate. No foreign taxes on this K-1.
    # =========================================================================
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

    # =========================================================================
    # PROFILE 3 -- Blackwater Offshore Macro Fund, LP
    # Global macro hedge fund structured as a domestic LP with offshore exposure.
    # This is a small LP interest (<1%). Short-term trading losses dominate;
    # interest income from T-bills is meaningful. Foreign taxes paid on
    # European equity income. No guaranteed payments, no rental real estate,
    # no Section 179. Capital account is modest -- recent investor.
    # =========================================================================
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

    # =========================================================================
    # PROFILE 4 -- Ironclad Industrial Buyout Fund IV, LP
    # Large-cap private equity fund; institutional LP with a 5.25% interest.
    # This is a liquidity event year -- a major portfolio company was sold,
    # generating a large long-term capital gain. Some ordinary income from
    # management fee offsets and deal-level operations. Large capital account.
    # No rental real estate, no foreign taxes (all domestic portfolio).
    # =========================================================================
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

    # =========================================================================
    # PROFILE 5 -- Permian Basin Royalties & Exploration Partners, LP
    # Oil & gas working interest partnership. This partner is the GENERAL
    # PARTNER and operator. Receives guaranteed payments for operating services.
    # Substantial depletion, intangible drilling costs (other deductions),
    # and AMT adjustments typical of oil & gas. Some foreign taxes from
    # Canadian royalty income routed through the fund. Modest LT capital gain
    # from sale of surface acreage. Recourse liabilities from credit facility.
    # =========================================================================
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
