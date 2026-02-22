# 04 - Cross-Partner and Multi-Year Validation Rules

## Overview

This document defines validation rules that apply **across multiple K-1 documents** from the same partnership, **across multiple tax years** for the same partner-partnership pair, and **duplicate detection** rules for identifying redundant or conflicting ingested data.

Unlike per-K-1 validations (arithmetic checks, field constraints, capital account reconciliation), these rules require data from **other pipeline runs** -- either from different partners in the same partnership or from the same partner in different years. They are designed to catch inconsistencies that are invisible when examining a single K-1 in isolation.

These validations are organized into four categories:
- **Category A**: Cross-partner validations (multiple K-1s from same partnership, same year)
- **Category B**: Multi-year continuity validations (K-1s from consecutive years for same partner-partnership pair)
- **Category C**: Duplicate detection (identifying redundant or conflicting K-1s)
- **Category D**: Cross-K-1 reasonableness checks (aggregate sanity checks)

### Current Pipeline Architecture

The pipeline processes each K-1 independently with per-run isolation (each PDF receives a unique `run_id`). Cross-partner and multi-year validations therefore require access to a **data store** of previously extracted K-1 data, keyed by `(partnership_ein, partner_ssn, tax_year)`. These validations should run as a separate downstream asset or sensor that queries the accumulated data after each new K-1 is ingested.

---

## IRS References

| Reference | Description |
|---|---|
| [IRS Instructions for Form 1065 (2025)](https://www.irs.gov/instructions/i1065) | Partnership return instructions; establishes that all K-1s must reconcile to Schedule K totals |
| [IRS Partner's Instructions for Schedule K-1 (Form 1065) (2025)](https://www.irs.gov/instructions/i1065sk1) | Partner reporting requirements; defines capital account reporting, percentage fields, and consistency obligations |
| [IRS IRM 3.0.101: Schedule K-1 Processing](https://www.irs.gov/irm/part3/irm_03-000-101r) | Internal Revenue Manual section on how IRS validates and processes K-1 forms; error categories, TIN checks, field validation |
| [GAO Report GAO-04-1040](https://www.govinfo.gov/content/pkg/GAOREPORTS-GAO-04-1040/html/GAOREPORTS-GAO-04-1040.htm) | "IRS Should Take Steps to Improve the Accuracy of Schedule K-1 Data" -- detailed error rates and data quality analysis |
| [IRS Publication 541: Partnerships](https://www.irs.gov/publications/p541) | General partnership rules including distribution limitations and basis calculations |
| [IRS Publication 925: Passive Activity and At-Risk Rules](https://www.irs.gov/publications/p925) | Loss limitation rules applied in sequence: basis, at-risk, passive activity |
| IRC Section 704(d) | Partner's distributive share of loss limited to adjusted basis |
| IRC Section 465 | At-risk limitation on deductions |
| IRC Section 469 | Passive activity loss limitation |
| IRC Section 731(a)(1) | Gain recognition on distributions exceeding basis |
| IRC Section 707(c) | Definition of guaranteed payments |
| [KPMG: Tax Basis Capital Account Reporting](https://assets.kpmg.com/content/dam/kpmg/us/pdf/2021/03/ai-tax-matters-tax-basis-capital-account-reporting.pdf) | Professional guidance on tax basis capital reporting requirements effective 2020+ |
| [AICPA BBA Partnership Audit Framework](https://www.aicpa-cima.com/resources/article/partnership-audit-and-adjustment-rules) | AICPA guidance on centralized partnership audit regime |
| [TIGTA Report 201930078](https://www.tigta.gov/sites/default/files/reports/2022-02/201930078fr.pdf) | "The Use of Schedule K-1 Data to Address Taxpayer Noncompliance" |
| [RSM Partnership Tax Filing Checklist 2024](https://rsmus.com/insights/services/business-tax/partnership-tax-filing-checklist-2024.html) | Professional practice checklist for partnership compliance |
| IRS Form 8082 | Notice of Inconsistent Treatment or Administrative Adjustment Request |

---

## Rules

### Category A: Cross-Partner Validations

These rules require two or more K-1s from the **same partnership** (identified by EIN) for the **same tax year**.

---

#### A1: Partner Profit Percentages Must Not Exceed 100%

**Rule ID**: `CROSS_A1_PROFIT_PCT_SUM`

**Description**: The sum of all partners' profit-sharing percentages (Box J, "Profit" column) for the same partnership EIN and tax year must not exceed 100%. A sum greater than 100% indicates data error, OCR misread of percentages, or duplicate K-1 ingestion.

**IRS Basis**: IRS Form 1065 Instructions require that the total of all partners' profit interests equals 100%. Schedule K allocations are based on these percentages; if they exceed 100%, the partnership return itself is erroneous.

**Fields Involved**:
- `K1ExtractedData.partner_share_percentage` (maps to Box J profit percentage)
- Partnership EIN (not currently in `K1ExtractedData` -- needs addition or lookup from extracted text)
- Tax year: `K1ExtractedData.tax_year`

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group:
  total_profit_pct = SUM(partner_share_percentage for all K-1s in group)
  IF total_profit_pct > 100.0:
    RAISE ERROR "Partner profit percentages sum to {total_profit_pct}%, exceeding 100%"
```

**Severity**: Critical

**Example**:
- Valid: Three partners with 50%, 30%, 20% = 100%
- Valid (partial data): Two partners with 50%, 30% = 80% (other partners not yet ingested)
- Invalid: Two partners with 60%, 55% = 115%

**Note**: The current `K1ExtractedData` model has a single `partner_share_percentage` field. On the actual K-1 form, Box J reports separate beginning/ending percentages for profit, loss, and capital. For full validation, the model should be extended to capture all six values. Profit, loss, and capital percentages can legitimately differ for the same partner (e.g., a GP with carried interest may have 20% profit but only 1% capital -- see test Profile 2, Granite Peak).

---

#### A2: Partner Percentages Sum Below 100% Warning

**Rule ID**: `CROSS_A2_PCT_SUM_INCOMPLETE`

**Description**: When the sum of all ingested partners' percentages for a partnership is less than 100%, this indicates that not all K-1s have been ingested. This is an informational warning, not an error.

**IRS Basis**: Same as A1. The sum must ultimately equal 100% for a complete set of K-1s.

**Fields Involved**: Same as A1.

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group:
  total_profit_pct = SUM(partner_share_percentage for all K-1s in group)
  IF total_profit_pct < 100.0:
    RAISE WARNING "Partner percentages sum to {total_profit_pct}%. {100 - total_profit_pct}% of partners may not yet be ingested."
```

**Severity**: Advisory

**Example**:
- Advisory: One partner at 2.50% out of a 40-partner fund = 2.50% total (37.50 partners missing)
- Not triggered: Exactly 100% total

---

#### A3: Income Allocation Proportionality Check

**Rule ID**: `CROSS_A3_INCOME_PROPORTIONALITY`

**Description**: When two or more K-1s from the same partnership are available, each partner's share of a given income line should be approximately proportional to their stated profit/loss percentage. Significant deviations indicate potential OCR errors, special allocations, or data quality issues.

**IRS Basis**: IRS Form 1065 Instructions, Schedule K. By default, partnership items are allocated according to the partnership agreement percentages. Special allocations under IRC Section 704(b) are permitted but must have "substantial economic effect." Disproportionate allocations are valid but unusual and warrant scrutiny.

**Fields Involved**:
- `K1ExtractedData.partner_share_percentage`
- `K1ExtractedData.ordinary_business_income` (Box 1)
- `K1ExtractedData.rental_real_estate_income` (Box 2)
- `K1ExtractedData.interest_income` (Box 5)
- `K1ExtractedData.ordinary_dividends` (Box 6a)
- `K1ExtractedData.short_term_capital_gains` (Box 8)
- `K1ExtractedData.long_term_capital_gains` (Box 9a)

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  FOR each income field (box1, box2, box5, box6a, box8, box9a):
    total_field = SUM(field value for all K-1s in group where field is not null)
    IF total_field != 0:
      FOR each K-1 in group where field is not null:
        expected_share = partner_share_percentage / SUM(all partner_share_percentages in group)
        actual_share = field_value / total_field
        deviation = ABS(actual_share - expected_share) / expected_share
        IF deviation > 0.10:  # 10% tolerance
          RAISE WARNING "Partner's {field} allocation deviates {deviation*100}% from expected pro-rata share"
```

**Severity**: Warning

**Example**:
- Valid: Partner A (50%) gets $50,000 ordinary income, Partner B (50%) gets $50,000
- Valid (special allocation): Partner A (50%) gets $80,000 carried interest gain, Partner B (50%) gets $20,000 (GP carry arrangement)
- Suspect: Partner A (5%) gets $500,000 ordinary income, Partner B (5%) gets $5,000

**Edge Case**: Guaranteed payments (Box 4) are inherently non-proportional since they are partner-specific compensation. Do NOT apply proportionality checks to guaranteed payments.

---

#### A4: Cross-Partner Capital Account Consistency

**Rule ID**: `CROSS_A4_CAPITAL_ACCOUNT_CONSISTENCY`

**Description**: Each partner's capital account should be approximately proportional to their capital percentage. When multiple K-1s from the same partnership are available, verify that capital account balances align with stated capital ownership percentages.

**IRS Basis**: IRS Form 1065 Schedule M-2 (Analysis of Partners' Capital Accounts) requires that the sum of all partners' capital accounts reconcile to the partnership's balance sheet. Each partner's share should reflect their capital percentage unless special allocations apply.

**Fields Involved**:
- `K1ExtractedData.partner_share_percentage` (ideally the capital percentage specifically)
- `K1ExtractedData.capital_account_beginning`
- `K1ExtractedData.capital_account_ending`

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  total_beginning = SUM(capital_account_beginning for all K-1s)
  total_ending = SUM(capital_account_ending for all K-1s)

  FOR each K-1 in group:
    IF total_ending != 0:
      expected_capital_share = capital_pct / SUM(all capital_pcts)
      actual_capital_share = capital_account_ending / total_ending
      deviation = ABS(actual_capital_share - expected_capital_share)
      IF deviation > 0.15:  # 15% tolerance (wider due to contributions/distributions)
        RAISE WARNING "Partner's capital account share ({actual_capital_share*100}%) deviates from capital percentage ({capital_pct}%)"
```

**Severity**: Warning

**Example**:
- Valid: Partner with 10% capital owns $100,000 of $1,000,000 total capital
- Valid (deviation due to recent contribution): Partner with 5% capital owns $200,000 of $1,000,000 (contributed mid-year)
- Suspect: Partner with 1% capital owns $500,000 of $600,000 total capital

---

#### A5: Partnership Identity Consistency Across K-1s

**Rule ID**: `CROSS_A5_PARTNERSHIP_IDENTITY`

**Description**: All K-1s from the same partnership EIN in the same tax year should have consistent partnership identifying information: partnership name, partnership address, and IRS Center.

**IRS Basis**: IRS IRM 3.0.101 validates that K-1 payer information matches the parent return. Inconsistent identifying information across K-1s from the same EIN suggests OCR extraction errors.

**Fields Involved**:
- `K1ExtractedData.partnership_name`
- Partnership EIN (identifier for grouping)
- Partnership address and IRS center (not currently in `K1ExtractedData` -- extracted from raw text)

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  partnership_names = SET(partnership_name for all K-1s in group)
  IF LEN(partnership_names) > 1:
    # Apply fuzzy matching to account for OCR variations
    IF NOT all_fuzzy_match(partnership_names, threshold=0.85):
      RAISE WARNING "Inconsistent partnership names across K-1s with same EIN: {partnership_names}"
```

**Severity**: Warning

**Example**:
- Valid: "Sunbelt Retail Real Estate Fund II, LP" across all K-1s for EIN 46-3819204
- Valid (minor OCR variation): "Sunbelt Retail Real Estate Fund II, LP" vs "Sunbelt Retail Real Estate Fund Il, LP" (OCR confuses II/Il)
- Suspect: "Sunbelt Retail Real Estate Fund II, LP" vs "Granite Peak Venture Partners III, LP" for the same EIN

---

### Category B: Multi-Year Continuity Validations

These rules require K-1s from **consecutive tax years** for the **same partner-partnership pair** (identified by partner SSN/TIN + partnership EIN).

---

#### B1: Ending Capital Must Equal Next Year's Beginning Capital

**Rule ID**: `CROSS_B1_CAPITAL_CONTINUITY`

**Description**: A partner's ending capital account balance for Year N must equal the beginning capital account balance for Year N+1 at the same partnership. This is the single most important multi-year validation. A mismatch indicates data quality errors, basis method changes, or potential OCR misreads.

**IRS Basis**: IRS Partner's Instructions for Schedule K-1 (2025), Section L (Partner's Capital Account Analysis). The IRS requires tax basis capital account reporting (mandatory since 2020). If the prior year's ending capital does not match the current year's beginning capital, the partnership must include a reconciliation statement explaining the difference. Per KPMG guidance, permissible reasons for mismatch include: transition from GAAP/704(b)/other basis to tax basis method, prior-year amended returns, or IRS audit adjustments.

**Fields Involved**:
- `K1ExtractedData.capital_account_ending` (Year N)
- `K1ExtractedData.capital_account_beginning` (Year N+1)
- `K1ExtractedData.tax_year`
- Partnership EIN and Partner SSN/TIN (composite key)

**Validation Logic**:
```
FOR each unique (partnership_ein, partner_ssn) pair with K-1s in consecutive years (Y, Y+1):
  ending_Y = K1[year=Y].capital_account_ending
  beginning_Y1 = K1[year=Y+1].capital_account_beginning

  IF ending_Y is not null AND beginning_Y1 is not null:
    IF ending_Y != beginning_Y1:
      difference = ABS(ending_Y - beginning_Y1)
      pct_difference = difference / MAX(ABS(ending_Y), ABS(beginning_Y1), 1) * 100
      RAISE ERROR "Capital account discontinuity: Year {Y} ending ({ending_Y}) != Year {Y+1} beginning ({beginning_Y1}). Difference: {difference} ({pct_difference}%)"
```

**Severity**: Critical

**Example**:
- Valid: 2023 ending capital = $258,185, 2024 beginning capital = $258,185
- Invalid: 2023 ending capital = $258,185, 2024 beginning capital = $318,500 (unexplained $60,315 gap)
- Edge case: Mismatch due to basis method transition (GAAP to tax basis) -- valid but should be documented

---

#### B2: Liability Balance Continuity

**Rule ID**: `CROSS_B2_LIABILITY_CONTINUITY`

**Description**: A partner's ending nonrecourse and recourse liability balances for Year N should equal the beginning balances for Year N+1 at the same partnership.

**IRS Basis**: IRS Partner's Instructions for Schedule K-1, Section K (Partner's Share of Liabilities). Liability allocations affect the partner's outside basis under IRC Section 752 and directly impact loss limitation calculations.

**Fields Involved**:
- Nonrecourse liabilities beginning/ending (not currently in `K1ExtractedData` -- needs model extension)
- Recourse liabilities beginning/ending (not currently in `K1ExtractedData` -- needs model extension)
- `K1ExtractedData.tax_year`
- Partnership EIN and Partner SSN/TIN

**Validation Logic**:
```
FOR each unique (partnership_ein, partner_ssn) pair with K-1s in consecutive years (Y, Y+1):
  FOR each liability type in [nonrecourse, recourse]:
    ending_Y = K1[year=Y].{liability_type}_ending
    beginning_Y1 = K1[year=Y+1].{liability_type}_beginning

    IF ending_Y is not null AND beginning_Y1 is not null:
      IF ending_Y != beginning_Y1:
        RAISE WARNING "Liability discontinuity ({liability_type}): Year {Y} ending ({ending_Y}) != Year {Y+1} beginning ({beginning_Y1})"
```

**Severity**: Warning

**Example**:
- Valid: 2023 nonrecourse ending = $271,880, 2024 nonrecourse beginning = $271,880
- Suspect: 2023 nonrecourse ending = $271,880, 2024 nonrecourse beginning = $0 (may be valid if loan was paid off, but should flag)

---

#### B3: Partnership Identity Continuity Across Years

**Rule ID**: `CROSS_B3_PARTNERSHIP_IDENTITY_MULTIYEAR`

**Description**: Across tax years, the same partnership EIN should have a consistent partnership name and IRS Center. Name changes or IRS Center changes for the same EIN are unusual and may indicate OCR errors.

**IRS Basis**: IRS IRM 3.0.101 requires that K-1 payer TINs match the parent return. While partnerships can legally change names, this is uncommon and should be flagged for review.

**Fields Involved**:
- `K1ExtractedData.partnership_name`
- `K1ExtractedData.tax_year`
- Partnership EIN

**Validation Logic**:
```
FOR each unique partnership_ein with K-1s in multiple years:
  names_by_year = {year: partnership_name for each K-1}
  unique_names = SET(names_by_year.values())
  IF LEN(unique_names) > 1:
    IF NOT all_fuzzy_match(unique_names, threshold=0.85):
      RAISE WARNING "Partnership name changed across years for EIN {ein}: {names_by_year}"
```

**Severity**: Warning

**Example**:
- Valid: "Sunbelt Retail Real Estate Fund II, LP" in both 2023 and 2024
- Warning: "Sunbelt Retail Real Estate Fund II, LP" (2023) vs "Sunbelt CRE Opportunity Fund III, LP" (2024) for the same EIN

---

#### B4: Partner Type Consistency Across Years

**Rule ID**: `CROSS_B4_PARTNER_TYPE_CONTINUITY`

**Description**: A partner's type (General Partner vs Limited Partner) should remain consistent across years for the same partnership unless there is a documented change. A GP becoming an LP or vice versa without a corresponding capital transaction is unusual.

**IRS Basis**: IRS Schedule K-1 Part II, Box G (General partner or LLC member-manager / Limited partner or other LLC member). Changes in partner type affect self-employment tax treatment (IRC Section 1402(a)(13)) and loss limitation ordering.

**Fields Involved**:
- `K1ExtractedData.partner_type`
- `K1ExtractedData.tax_year`
- Partnership EIN and Partner SSN/TIN

**Validation Logic**:
```
FOR each unique (partnership_ein, partner_ssn) pair with K-1s in multiple years:
  types_by_year = {year: partner_type for each K-1}
  unique_types = SET(types_by_year.values())
  IF LEN(unique_types) > 1:
    RAISE WARNING "Partner type changed from {types_by_year[earlier]} to {types_by_year[later]} for same partner-partnership pair"
```

**Severity**: Warning

**Example**:
- Valid: "General Partner" in both 2023 and 2024
- Warning: "Limited Partner" (2023) -> "General Partner" (2024) (may indicate promotion to GP, but should verify)

---

### Category C: Duplicate Detection

These rules identify K-1s that may be duplicates, amended versions, or OCR-related re-submissions.

---

#### C1: Exact Duplicate Detection

**Rule ID**: `CROSS_C1_EXACT_DUPLICATE`

**Description**: Two K-1s with the same partnership EIN, partner SSN/TIN, tax year, AND identical dollar amounts across all fields are exact duplicates. This most commonly occurs when the same PDF is processed twice by the pipeline.

**IRS Basis**: The IRS uses the (EIN, TIN, tax year) tuple as its primary matching key for K-1 processing (per IRM 3.0.101). Each partner should receive exactly one K-1 per partnership per tax year.

**Fields Involved**:
- Partnership EIN (primary key component)
- Partner SSN/TIN (primary key component)
- `K1ExtractedData.tax_year` (primary key component)
- All dollar amount fields (for confirming exact match)

**Validation Logic**:
```
composite_key = (partnership_ein, partner_ssn, tax_year)

FOR each new K-1 ingested:
  existing = LOOKUP(data_store, composite_key)
  IF existing is not null:
    IF all_amounts_match(existing, new_k1):
      RAISE ERROR "Exact duplicate K-1 detected: same EIN ({ein}), SSN ({ssn}), year ({year}) with identical amounts. Likely re-processed PDF."
```

**Severity**: Critical

**Example**:
- Duplicate: Two K-1s for EIN 46-3819204, SSN 621-47-8830, tax year 2024, both showing identical $42,315 rental loss
- Not duplicate: Same EIN and SSN but different tax years (2023 vs 2024)

---

#### C2: Possible Amended K-1 Detection

**Rule ID**: `CROSS_C2_AMENDED_K1`

**Description**: Two K-1s with the same (EIN, SSN, tax year) but different dollar amounts may represent an original and an amended K-1. The pipeline should flag this and prompt the user to determine which version is authoritative.

**IRS Basis**: Amended K-1s are issued when the partnership files an amended Form 1065 or when an error is discovered. The K-1 form has checkboxes for "Amended K-1" and "Final K-1" in Box A. Per IRS Form 8082 instructions, partners must report inconsistencies and file Form 8082 if they disagree with the partnership's treatment.

**Fields Involved**:
- Partnership EIN, Partner SSN/TIN, `K1ExtractedData.tax_year` (matching key)
- All dollar amount fields (for confirming they differ)
- Amended/Final checkboxes (if extracted from the form)

**Validation Logic**:
```
composite_key = (partnership_ein, partner_ssn, tax_year)

FOR each new K-1 ingested:
  existing = LOOKUP(data_store, composite_key)
  IF existing is not null:
    IF NOT all_amounts_match(existing, new_k1):
      differing_fields = identify_differences(existing, new_k1)
      RAISE WARNING "Possible amended K-1 detected for ({ein}, {ssn}, {year}). Differing fields: {differing_fields}. Check for 'Amended K-1' checkbox."
```

**Severity**: Warning

**Example**:
- Amended: Two K-1s for same key, first shows $42,315 rental loss, second shows $38,200 rental loss (corrected amount)
- Not amended: Two K-1s with same EIN but different SSNs (different partners -- expected)

---

#### C3: Near-Duplicate PDF Detection

**Rule ID**: `CROSS_C3_NEAR_DUPLICATE_PDF`

**Description**: Detect when the same physical PDF file (or a nearly identical file) is processed multiple times. This uses file-level metadata rather than extracted field values.

**IRS Basis**: Not IRS-specific; this is a data pipeline quality control measure to prevent double-counting from re-ingestion of the same source document.

**Fields Involved**:
- PDF file hash (MD5 or SHA-256 of source file)
- PDF file name
- PDF file size
- Ingestion timestamp

**Validation Logic**:
```
FOR each new PDF ingested:
  file_hash = SHA256(pdf_bytes)
  existing = LOOKUP(processed_files, file_hash)
  IF existing is not null:
    RAISE ERROR "Near-duplicate PDF detected: file hash {file_hash} was previously processed as run_id={existing.run_id} on {existing.ingested_at}"
  ALSO check:
    IF file_name matches pattern of existing file (e.g., "K1_2024.pdf" vs "K1_2024 (1).pdf"):
      RAISE WARNING "File name suggests possible duplicate download: {file_name}"
```

**Severity**: Critical (exact hash match) / Warning (name similarity)

---

#### C4: Software-Split K-1 Detection

**Rule ID**: `CROSS_C4_SOFTWARE_SPLIT`

**Description**: Some tax preparation software (notably TurboTax) requires splitting a single K-1 into multiple entries when a partner has both ordinary business income (Box 1) and rental real estate income (Box 2). These splits share the same (EIN, SSN, tax year) but populate different box sets and should NOT be flagged as duplicates.

**IRS Basis**: TurboTax and similar software handle Boxes 1 and 2 as separate activities (trade/business vs. rental) because they have different tax treatment (passive vs. non-passive, SE tax implications). The underlying K-1 is a single document, but the software representation is split.

**Fields Involved**:
- Partnership EIN, Partner SSN/TIN, `K1ExtractedData.tax_year`
- `K1ExtractedData.ordinary_business_income` (Box 1)
- `K1ExtractedData.rental_real_estate_income` (Box 2)

**Validation Logic**:
```
composite_key = (partnership_ein, partner_ssn, tax_year)

FOR each pair of K-1s sharing the same composite_key:
  k1_a, k1_b = pair
  # Check if one has Box 1 and the other has Box 2 (split pattern)
  IF (k1_a.ordinary_business_income is not null AND k1_b.rental_real_estate_income is not null
      AND k1_a.rental_real_estate_income is null AND k1_b.ordinary_business_income is null):
    MARK as "software split" -- do not flag as duplicate
    RAISE ADVISORY "Two K-1 entries for same key appear to be a software-mandated split (Box 1 vs Box 2)"
  ELSE:
    # Apply C1 or C2 rules
```

**Severity**: Advisory

---

### Category D: Cross-K-1 Reasonableness Checks

These rules apply aggregate sanity checks across all ingested K-1s for a given partnership.

---

#### D1: Total Distributions Reasonableness

**Rule ID**: `CROSS_D1_DISTRIBUTION_REASONABLENESS`

**Description**: The total distributions across all partners in a partnership should be reasonable relative to the partnership's total income. If total distributions vastly exceed total income, this may indicate data quality issues or an unusual liquidation event.

**IRS Basis**: IRC Section 731 governs partnership distributions. Distributions do not generate income unless they exceed the partner's basis, but total distributions substantially exceeding total partnership income is unusual for an ongoing concern. IRS Publication 541 establishes that distributions reduce basis and must be tracked.

**Fields Involved**:
- `K1ExtractedData.distributions` (Box 19)
- `K1ExtractedData.ordinary_business_income` (Box 1)
- `K1ExtractedData.rental_real_estate_income` (Box 2)
- All income fields

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  total_distributions = SUM(distributions for all K-1s in group)
  total_income = SUM(ordinary_business_income + rental_real_estate_income + interest_income +
                     ordinary_dividends + long_term_capital_gains + short_term_capital_gains
                     for all K-1s in group)
  IF total_income > 0 AND total_distributions > total_income * 3.0:
    RAISE WARNING "Total distributions ({total_distributions}) exceed 3x total income ({total_income}) for partnership {ein}. May indicate liquidation or data error."
```

**Severity**: Warning

**Example**:
- Valid: Partnership earns $1M total, distributes $800K (common in profitable year)
- Valid: Partnership earns $500K total, distributes $700K (drawing down prior-year earnings)
- Suspect: Partnership earns $100K total, distributes $5M (unusual unless liquidating)

---

#### D2: Loss Proportionality Check

**Rule ID**: `CROSS_D2_LOSS_PROPORTIONALITY`

**Description**: Partners with identical loss percentages should receive identical loss allocations (for the same income line). Significant deviations indicate special allocations, OCR errors, or data extraction problems.

**IRS Basis**: IRS Form 1065 Instructions. Default allocations follow the partnership agreement percentages. IRC Section 704(b) allows special allocations only if they have "substantial economic effect." Disproportionate loss allocations are a common IRS audit trigger.

**Fields Involved**:
- `K1ExtractedData.partner_share_percentage` (loss percentage specifically)
- All loss-capable fields: `ordinary_business_income`, `rental_real_estate_income`, `short_term_capital_gains`

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  # Group partners by loss percentage
  pct_groups = GROUP_BY(K-1s, partner_share_percentage)
  FOR each group with 2+ partners having identical loss percentage:
    FOR each loss field:
      values = [field_value for each K-1 in group where field is not null]
      IF LEN(SET(values)) > 1:
        RAISE WARNING "Partners with same loss percentage ({pct}%) have different loss allocations for {field}: {values}"
```

**Severity**: Warning

---

#### D3: Outlier Detection Across Partners

**Rule ID**: `CROSS_D3_OUTLIER_DETECTION`

**Description**: When three or more K-1s from the same partnership are available, identify any partner whose per-percentage-point allocation is dramatically different from the median. This catches OCR errors that produce implausible amounts.

**IRS Basis**: Not based on a specific IRS rule; this is a data quality heuristic. However, IRS examination procedures specifically target returns with anomalous allocations, and the IRS uses AI-based selection criteria to identify partnership returns for audit (per IRS Commissioner statements, 2024).

**Fields Involved**:
- `K1ExtractedData.partner_share_percentage`
- All dollar amount fields

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 3+ K-1s:
  FOR each dollar amount field:
    # Normalize amounts by partner percentage
    normalized = [field_value / partner_share_percentage for each K-1 where both are not null and pct > 0]
    IF LEN(normalized) >= 3:
      median = MEDIAN(normalized)
      FOR each K-1:
        normalized_value = field_value / partner_share_percentage
        IF ABS(normalized_value - median) > 3 * MAD(normalized):  # MAD = Median Absolute Deviation
          RAISE WARNING "Partner's {field} allocation per percentage point ({normalized_value}) is a statistical outlier vs peers (median: {median})"
```

**Severity**: Warning

---

#### D4: Cross-Partner Self-Employment Consistency

**Rule ID**: `CROSS_D4_SE_CONSISTENCY`

**Description**: Within the same partnership, self-employment earnings reporting should be consistent with partner type. All general partners should have SE earnings; all limited partners should not (unless they receive guaranteed payments for services).

**IRS Basis**: IRC Section 1402(a)(13) exempts limited partners from SE tax on their distributive share (with exceptions for guaranteed payments for services). The IRS has active compliance campaigns targeting partnerships where the limited partner exception is improperly claimed.

**Fields Involved**:
- `K1ExtractedData.partner_type` (General / Limited)
- `K1ExtractedData.self_employment_earnings` (Box 14a)
- `K1ExtractedData.guaranteed_payments` (Box 4)
- `K1ExtractedData.ordinary_business_income` (Box 1)

**Validation Logic**:
```
FOR each unique (partnership_ein, tax_year) group with 2+ K-1s:
  gp_k1s = [k1 for k1 in group if k1.partner_type == "General"]
  lp_k1s = [k1 for k1 in group if k1.partner_type == "Limited"]

  FOR each gp in gp_k1s:
    IF gp.ordinary_business_income > 0 AND gp.self_employment_earnings is null:
      RAISE WARNING "General partner has ordinary income but no SE earnings reported"

  FOR each lp in lp_k1s:
    IF lp.self_employment_earnings is not null AND lp.self_employment_earnings > 0:
      IF lp.guaranteed_payments is null OR lp.guaranteed_payments == 0:
        RAISE WARNING "Limited partner has SE earnings but no guaranteed payments -- LP SE exemption may be improperly bypassed"
```

**Severity**: Warning

**Example (from test profiles)**:
- Valid: Profile 2 (Granite Peak GP) has SE earnings = $487,615 (ordinary $127,615 + guaranteed $360,000)
- Valid: Profile 1 (Sunbelt LP) has no SE earnings
- Valid: Profile 10 (Southern Hospitality GP, S-Corp entity) has no SE earnings on K-1 (SE handled at S-Corp level)
- Suspect: An LP with $100,000 SE earnings and $0 guaranteed payments

---

## Edge Cases

### 1. Profit, Loss, and Capital Percentages Can Differ

It is entirely valid for a partner to have different profit, loss, and capital percentages. This is common in:
- **Carried interest arrangements**: GPs have high profit percentage (e.g., 20%) but low capital percentage (e.g., 1%). See test Profile 2 (Granite Peak Venture Partners: 20% profit, 1% capital).
- **Waterfall structures**: Profit percentages may change once certain return hurdles are met.
- **Special allocations under IRC 704(b)**: Partnerships can allocate specific items differently if the allocations have substantial economic effect.

**Implication**: Validators must handle separate profit, loss, and capital percentage fields. The current `K1ExtractedData.partner_share_percentage` is insufficient for complete validation.

### 2. Same Name, Different Partnerships (Sunbelt Test Case)

Test Profiles 1 and 8 both contain "Sunbelt" in the partnership name:
- Profile 1: "Sunbelt Retail Real Estate Fund II, LP" (EIN 46-3819204)
- Profile 8: "Sunbelt CRE Opportunity Fund III, LP" (EIN 38-4702193)

These are different partnerships with different EINs. Duplicate detection must key on EIN, not name. Fuzzy name matching without EIN verification would produce false positives.

### 3. S-Corp and Corporate Partners Do Not Have SE Earnings

Test Profile 10 (Southern Hospitality Restaurant Group) has an S-Corporation as the general partner. Even though it is a GP, the K-1 correctly shows no self-employment earnings because SE tax is handled at the S-Corporation level (on W-2s to shareholder-employees). Similarly, corporate partners (Profile 9, Cascadia Clean Energy) do not have SE earnings.

**Implication**: Cross-partner SE consistency checks (Rule D4) must account for entity type. The check "GP must have SE earnings" should only apply to individual GPs, not entity GPs.

### 4. Negative Capital Accounts Are Valid

IRS guidance permits negative capital accounts. A partner with a negative capital account can still have positive outside basis if their share of partnership liabilities exceeds the capital deficit. This is common in:
- Highly leveraged real estate partnerships
- Partnerships with significant nonrecourse debt
- Partners who received large distributions funded by debt

**Implication**: Do not flag negative capital accounts as errors. Instead, flag the combination of `capital_account_ending < 0 AND (nonrecourse_ending + recourse_ending) < ABS(capital_account_ending)` as a warning about potential basis issues.

### 5. Partial Partner Data Is Normal

For large partnerships (hundreds of LPs), the pipeline may only ever ingest a small subset of K-1s. Cross-partner validations like percentage sum checks will routinely show sums far below 100%. The system must clearly distinguish between "incomplete data" (sum < 100%) and "bad data" (sum > 100%).

### 6. Mid-Year Partner Admissions and Withdrawals

When a partner enters or exits a partnership mid-year, their K-1 will show beginning or ending percentages of 0%. The partner may receive a "Final K-1" with distributions and income only for their period of participation. These K-1s are valid but may appear unusual in cross-partner checks.

### 7. Multi-Tier Partnership Structures

Some K-1s are issued to partnerships that are themselves partners in other partnerships (tiered structures). The partner's SSN/TIN field will contain an EIN rather than an SSN. This is valid and the duplicate detection key should handle both SSN and EIN formats in the partner TIN field.

### 8. Tax Year Mismatches

Most partnerships use a calendar tax year (January-December), but some have fiscal year ends. Cross-year continuity checks (Rule B1) should match on the partnership's specific tax year end date, not just the calendar year.

---

## Sources

1. [IRS Partner's Instructions for Schedule K-1 (Form 1065) 2025](https://www.irs.gov/instructions/i1065sk1) -- Primary reference for K-1 field definitions, capital account reporting, and partner percentage requirements.
2. [IRS Instructions for Form 1065 (2025)](https://www.irs.gov/instructions/i1065) -- Partnership return instructions establishing Schedule K to K-1 reconciliation requirement.
3. [IRS IRM 3.0.101: Schedule K-1 Processing](https://www.irs.gov/irm/part3/irm_03-000-101r) -- Internal Revenue Manual section detailing IRS K-1 processing, validation checks, error categories, TIN validation, and unprocessable form criteria.
4. [GAO Report GAO-04-1040: Tax Administration: IRS Should Take Steps to Improve the Accuracy of Schedule K-1 Data](https://www.govinfo.gov/content/pkg/GAOREPORTS-GAO-04-1040/html/GAOREPORTS-GAO-04-1040.htm) -- Found 5-9.5% transcription error rates, 6% invalid TIN rates ($57.3B in unmatched income), and recommended expanded data capture and e-filing.
5. [IRS Publication 541: Partnerships](https://www.irs.gov/publications/p541) -- General partnership rules covering distributions, basis calculations, and partner transactions.
6. [IRS Publication 925: Passive Activity and At-Risk Rules](https://www.irs.gov/publications/p925) -- Loss limitation rules applied in sequence: basis (704(d)), at-risk (465), passive activity (469).
7. [KPMG: Insights into Schedule K-1 Reporting, Tax Basis Capital Account Reporting (2021)](https://assets.kpmg.com/content/dam/kpmg/us/pdf/2021/03/ai-tax-matters-tax-basis-capital-account-reporting.pdf) -- Professional guidance on mandatory tax basis capital reporting, reconciliation requirements, and negative capital account treatment.
8. [TIGTA Report 201930078: The Use of Schedule K-1 Data to Address Taxpayer Noncompliance](https://www.tigta.gov/sites/default/files/reports/2022-02/201930078fr.pdf) -- Treasury Inspector General findings on K-1 data quality limitations and compliance gaps.
9. [AICPA BBA Partnership Audit and Adjustment Rules Framework](https://www.aicpa-cima.com/resources/article/partnership-audit-and-adjustment-rules) -- Professional guidance on centralized partnership audit regime under Bipartisan Budget Act of 2015.
10. [RSM Partnership Tax Filing Checklist 2024](https://rsmus.com/insights/services/business-tax/partnership-tax-filing-checklist-2024.html) -- Professional practice checklist covering partnership return preparation and K-1 issuance.
11. [Mahoney CPA: Reporting Partnership Schedule K-1 Capital Accounts on the Tax Basis](https://mahoneycpa.com/reporting-partnership-schedule-k-1-capital-accounts-on-the-tax-basis/) -- Practical guidance on tax basis capital account reconciliation and year-over-year continuity.
12. [IRS Form 8082 Instructions](https://www.irs.gov/instructions/i8082) -- Procedures for reporting inconsistent treatment between a partner's return and the partnership's K-1.
13. [TaxSlayer Pro: Form 1065 Schedule M-2 Analysis of Partners' Capital Accounts](https://support.taxslayerpro.com/hc/en-us/articles/360009163494) -- Reference for Schedule M-2 capital account reconciliation components.
