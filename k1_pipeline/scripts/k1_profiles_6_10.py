"""
Schedule K-1 (Form 1065) test data -- Profiles 6-10.

Five distinct real-world partner scenarios:
  6  -- Nakamura Family Investment LLC  (family investment LLC, Trust, Oregon, NET LOSS)
  7  -- Pacific Coast Orthopedic Partners (medical practice, GP Individual, Arizona, profitable)
  8  -- Sunbelt CRE Opportunity Fund III  (commercial real estate syndication, LP Individual,
         Michigan, large Section 179, profitable)
  9  -- Cascadia Clean Energy Fund LP     (renewable energy, LP Corporation, Virginia,
         significant foreign taxes, net loss)
 10  -- Southern Hospitality Restaurant Group (restaurant group, GP S-Corp, Georgia,
         very simple K-1, modestly profitable)

All names, EINs, SSNs, and addresses are entirely fictitious.
"""

PROFILES_6_10 = [
    # ------------------------------------------------------------------
    # Profile 6 -- Family Investment LLC
    # Partnership type : Family investment LLC
    # Partner entity   : Trust (Irrevocable)
    # Partner role     : Limited partner
    # State            : Oregon
    # Theme            : NET LOSS year; modest capital account
    # ------------------------------------------------------------------
    {
        "partnership_name": "Nakamura Family Investment LLC",
        "partnership_address": "7821 SW Barbur Boulevard, Suite 310\nPortland, OR 97219",
        "irs_center": "Ogden, UT",
        "ein": "93-2847561",
        "partner_name": "Nakamura 2018 Irrevocable Trust\nc/o Kenji Nakamura, Trustee",
        "partner_address": "4455 NE Fremont Street\nPortland, OR 97213",
        "ssn": "93-7142608",            # trust TIN
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
        "capital_net_income": "(19,315)",     # net loss share
        "capital_withdrawals": "0",
        "capital_ending": "68,085",
        "box1_ordinary_income": "(19,315)",   # loss from failed startup investments
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
        "box14a_se_earnings": "",             # limited partner -- no SE
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (1,840)",
        "box18c_nondeductible": "",
        "box19a_distributions": "",           # no distributions in loss year
        "box20a_investment_income": "4,420",
        "box20b_investment_expenses": "1,875",
        "box20z_qbi": "",                     # loss -- no QBI deduction benefit
        "box21_foreign_taxes": "",
    },

    # ------------------------------------------------------------------
    # Profile 7 -- Medical Practice Partnership
    # Partnership type : Medical practice partnership (physician group)
    # Partner entity   : Individual
    # Partner role     : General partner
    # State            : Arizona
    # Theme            : Very profitable; large guaranteed payments; high SE income
    # ------------------------------------------------------------------
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
        "box1_ordinary_income": "148,440",    # professional fees income share
        "box2_rental_real_estate": "",
        "box4a_guaranteed_services": "120,000",  # physician base draw
        "box4c_total_guaranteed": "120,000",
        "box5_interest": "2,810",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "",
        "box12_section_179": "8,340",         # share of equipment purchase (exam tables, etc.)
        "box13_other_deductions": "",
        "box14a_se_earnings": "A  268,440",   # GP: ordinary income + guaranteed payments
        "box14c_gross_nonfarm": "C  268,440",
        "box17a_amt": "",
        "box18c_nondeductible": "C  4,200",   # meals/entertainment 50% disallowed
        "box19a_distributions": "A  175,000",
        "box20a_investment_income": "",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  148,440",
        "box21_foreign_taxes": "",
    },

    # ------------------------------------------------------------------
    # Profile 8 -- Commercial Real Estate Syndication
    # Partnership type : Commercial real estate syndication (Class B office/industrial)
    # Partner entity   : Individual
    # Partner role     : Limited partner
    # State            : Michigan
    # Theme            : Large Section 179; rental real estate losses; large capital account
    # ------------------------------------------------------------------
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
        "capital_net_income": "(62,180)",      # net loss from depreciation / vacancy
        "capital_withdrawals": "(48,000)",
        "capital_ending": "1,969,820",
        "box1_ordinary_income": "",
        "box2_rental_real_estate": "(62,180)",  # passive rental loss
        "box4a_guaranteed_services": "",
        "box4c_total_guaranteed": "",
        "box5_interest": "3,920",
        "box6a_ordinary_dividends": "",
        "box6b_qualified_dividends": "",
        "box8_st_capital_gain": "",
        "box9a_lt_capital_gain": "11,450",      # disposition of one property in portfolio
        "box12_section_179": "89,600",          # large Sec 179 on HVAC/roof improvements
        "box13_other_deductions": "",
        "box14a_se_earnings": "",               # limited partner -- no SE
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (58,340)",            # AMT depreciation adjustment
        "box18c_nondeductible": "",
        "box19a_distributions": "A  48,000",
        "box20a_investment_income": "3,920",
        "box20b_investment_expenses": "2,160",
        "box20z_qbi": "Z  (62,180)",            # rental real estate QBI loss
        "box21_foreign_taxes": "",
    },

    # ------------------------------------------------------------------
    # Profile 9 -- Renewable Energy Fund
    # Partnership type : Renewable energy fund (solar + wind PTC projects)
    # Partner entity   : Corporation
    # Partner role     : Limited partner
    # State            : Virginia
    # Theme            : Significant foreign taxes; net loss from startup costs + depreciation
    # ------------------------------------------------------------------
    {
        "partnership_name": "Cascadia Clean Energy Fund LP",
        "partnership_address": "1760 Reston Parkway, Suite 600\nReston, VA 20190",
        "irs_center": "Ogden, UT",
        "ein": "54-8031297",
        "partner_name": "Dominion Sustainable Capital Corp.",
        "partner_address": "700 East Main Street, 12th Floor\nRichmond, VA 23219",
        "ssn": "54-1967834",            # corporate EIN used in SSN field
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
        "box1_ordinary_income": "(387,620)",    # large startup / MACRS bonus depreciation loss
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
        "box14a_se_earnings": "",               # limited partner / corporate -- no SE
        "box14c_gross_nonfarm": "",
        "box17a_amt": "A  (312,400)",           # large AMT depreciation adjustment (bonus dep.)
        "box18c_nondeductible": "C  14,500",    # lobbying / non-deductible regulatory costs
        "box19a_distributions": "",             # no distributions in startup phase
        "box20a_investment_income": "8,740",
        "box20b_investment_expenses": "",
        "box20z_qbi": "Z  (387,620)",
        "box21_foreign_taxes": "47,380",        # foreign taxes on offshore wind project (Ireland)
    },

    # ------------------------------------------------------------------
    # Profile 10 -- Restaurant Group
    # Partnership type : Restaurant group (casual dining chain, 4 locations)
    # Partner entity   : S Corporation
    # Partner role     : General partner (managing member)
    # State            : Georgia
    # Theme            : Very simple K-1; modestly profitable; few boxes filled
    # ------------------------------------------------------------------
    {
        "partnership_name": "Southern Hospitality Restaurant Group, LLC",
        "partnership_address": "3080 Peachtree Road NW, Suite 900\nAtlanta, GA 30305",
        "irs_center": "Ogden, UT",
        "ein": "58-2614039",
        "partner_name": "Peach State Holdings, Inc.",
        "partner_address": "1200 Abernathy Road NE, Suite 1700\nAtlanta, GA 30328",
        "ssn": "58-3801562",            # S-corp EIN in SSN field
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
        "box1_ordinary_income": "28,945",       # modest restaurant operating profit
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
        "box14a_se_earnings": "",               # S-corp partner -- SE handled at S-corp level
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
