# K-1 Field Constraints and Validation Rules

## 1. Overview

This document defines field-level validation constraints for every field in the `K1ExtractedData` Pydantic model. It covers:

- **Required vs optional fields** -- which fields must always be present on a valid K-1
- **Data type and format constraints** -- string patterns, numeric ranges, field sizes
- **Sign conventions** -- which monetary fields allow negative values (losses) and which must be non-negative
- **Cross-field validation** -- relationships between fields that must hold (e.g., qualified dividends cannot exceed ordinary dividends)
- **Magnitude sanity checks** -- realistic value ranges to detect OCR/extraction errors
- **Partner type constraints** -- how partner classification affects valid values in other fields
- **Common extraction errors** -- known failure modes of OCR/AI document processing on K-1 forms

These rules are grounded in IRS form instructions, the official K-1 2D barcode specification, IRS Modernized e-File (MeF) schemas, and tax preparation software validation behavior.

---

## 2. IRS References

| Reference | Description | Relevance |
|---|---|---|
| **Form 1065 Instructions (2025)** | Official IRS instructions for U.S. Return of Partnership Income | Defines K-1 field requirements, Box J percentages, Section L capital accounts |
| **Partner's Instructions for Schedule K-1 (Form 1065) (2025)** | Partner-facing instructions for reading/using K-1 data | Defines how each box is used, which are income (loss), and reporting rules |
| **Schedule K-1 (Form 1065) 2D Barcode Specification** | IRS technical spec for barcode encoding of K-1 data | Defines exact field sizes, data types, numeric formats, partner type codes |
| **IRC Section 179(b)(1)** | Internal Revenue Code -- Section 179 deduction limits | Sets annual statutory limit on Section 179 deductions |
| **IRS Publication 541** | Partnerships | General partnership taxation rules |
| **IRS Form 8582 Instructions** | Passive Activity Loss Limitations | Rules for passive losses from Box 2 rental activities |
| **IRC Section 199A** | Qualified Business Income Deduction | Rules for QBI (Box 20, Code Z) |
| **IRS MeF Business Rules for Form 1065** | XML schema validation rules for e-filed returns | E-file acceptance/rejection criteria |
| **IRS Schedule K-1 Form 1065 (Final) Barcode Spec** | Field-level technical specification | Field 25 partner type codes, Field 32-37 percentage formats, monetary field sizes |

---

## 3. Rules

### 3.1 Required Fields

#### RULE FC-001: tax_year Must Be Present

- **Description**: Every valid K-1 has a tax year printed on the form header. This field must always be populated.
- **IRS Basis**: Form 1065 Schedule K-1 header field; required on every K-1 per Form 1065 Instructions.
- **Fields Involved**: `tax_year`
- **Validation Logic**:
  ```python
  assert tax_year is not None
  assert tax_year != ""
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `"2024"`, `"2023"`
  - Invalid: `None`, `""`

#### RULE FC-002: tax_year Format and Range

- **Description**: Tax year must be a 4-digit year string within a reasonable range. The IRS only processes returns for recent tax years (current year and approximately 3 prior years for original filing; amended returns up to 3 years back). Fiscal year partnerships may show a year that differs from the calendar year, but the 4-digit format is always used.
- **IRS Basis**: Form 1065 header; IRS e-file schema requires 4-digit year.
- **Fields Involved**: `tax_year`
- **Validation Logic**:
  ```python
  assert re.match(r'^\d{4}$', tax_year)
  year = int(tax_year)
  assert 1950 <= year <= current_year
  # Warning if year is more than 5 years old
  if year < current_year - 5:
      warn("Tax year is unusually old")
  ```
- **Severity**: Critical (format), Advisory (age check)
- **Example**:
  - Valid: `"2024"`, `"2019"`
  - Invalid: `"24"`, `"20244"`, `"abcd"`, `"2030"` (future year)

#### RULE FC-003: partnership_name Must Be Present

- **Description**: Part I of every K-1 requires the partnership's name. This is a required identification field.
- **IRS Basis**: Form 1065 Schedule K-1, Part I (Information About the Partnership). The IRS Instructions state: "Do not enter 'See attached' instead of completing entry spaces."
- **Fields Involved**: `partnership_name`
- **Validation Logic**:
  ```python
  assert partnership_name is not None
  assert len(partnership_name.strip()) > 0
  assert len(partnership_name) <= 70  # Two 35-char lines per barcode spec
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `"ACME PARTNERS LP"`, `"SMITH & JONES LLC"`
  - Invalid: `None`, `""`, `"   "`

#### RULE FC-004: partner_type Must Be Present

- **Description**: Item H2 on the K-1 requires a checkbox indicating whether the partner is a general partner/LLC member-manager or a limited partner/other LLC member. Exactly one must be selected.
- **IRS Basis**: Schedule K-1 Item H2; Barcode Specification Field 25 (1-digit code: 1 = General Partner or LLC Member-Manager, 2 = Limited Partner or Other LLC Member).
- **Fields Involved**: `partner_type`
- **Validation Logic**:
  ```python
  VALID_PARTNER_TYPES = {
      "general partner",
      "limited partner",
      "LLC member-manager",
      "LLC member",
      "general partner or LLC member-manager",
      "limited partner or other LLC member",
  }
  assert partner_type is not None
  assert partner_type.lower().strip() in VALID_PARTNER_TYPES
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `"General Partner"`, `"Limited Partner"`, `"LLC member-manager"`
  - Invalid: `None`, `"partner"`, `"shareholder"`, `"owner"`

---

### 3.2 Partner Share Percentage

#### RULE FC-010: partner_share_percentage Range

- **Description**: Box J reports the partner's share of profit, loss, and capital as percentages. Each individual partner's share must be between 0% and 100% inclusive. The IRS barcode spec defines this as a 6-character numeric field with an implied decimal after the 3rd digit (e.g., 25.32% = "025320").
- **IRS Basis**: Schedule K-1 Item J; Barcode Specification Fields 32-37 (6-character percentage format). Form 1065 Instructions: "If the partnership agreement does not express the partners' shares... the partnership may use a reasonable method."
- **Fields Involved**: `partner_share_percentage`
- **Validation Logic**:
  ```python
  if partner_share_percentage is not None:
      assert partner_share_percentage >= 0.0
      assert partner_share_percentage <= 100.0
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `25.5`, `0.001`, `100.0`, `50.0`
  - Invalid: `-5.0`, `105.0`, `200.0`

#### RULE FC-011: partner_share_percentage Zero Warning

- **Description**: A share percentage of exactly 0% is technically valid (e.g., a partner who sold their entire interest mid-year may have a 0% ending percentage), but it is unusual for the "current" percentage and warrants review.
- **IRS Basis**: Schedule K-1 Item J; IRS Instructions note that ending capital percentage is zero when capital account is negative or zero.
- **Fields Involved**: `partner_share_percentage`
- **Validation Logic**:
  ```python
  if partner_share_percentage == 0.0:
      warn("Partner share percentage is 0% -- verify this is a final/transfer K-1")
  ```
- **Severity**: Advisory
- **Example**:
  - Valid but suspicious: `0.0` (should only appear on final K-1 or mid-year transfer)

---

### 3.3 Monetary Fields -- Sign Conventions

#### RULE FC-020: Fields That Allow Negative Values (Losses)

- **Description**: Certain K-1 boxes are explicitly labeled "Income (Loss)" or "Gain (Loss)" on the IRS form, indicating they can be negative when the partnership incurred a loss. The K-1 form uses parentheses to denote negative amounts.
- **IRS Basis**: Partner's Instructions for Schedule K-1: "Any loss should be reported as a negative amount in any field stating income (loss)." Barcode Specification: monetary fields are 12 characters with minus sign for negatives.
- **Fields Involved**: `ordinary_business_income`, `rental_real_estate_income`, `short_term_capital_gains`, `long_term_capital_gains`, `self_employment_earnings`, `qbi_deduction`, `capital_account_beginning`, `capital_account_ending`
- **Validation Logic**:
  ```python
  # These fields accept any real number (positive, negative, or zero)
  ALLOWS_NEGATIVE = {
      "ordinary_business_income",      # Box 1: "Ordinary Business Income (Loss)"
      "rental_real_estate_income",     # Box 2: "Net Rental Real Estate Income (Loss)"
      "short_term_capital_gains",      # Box 8: "Net Short-Term Capital Gain (Loss)"
      "long_term_capital_gains",       # Box 9a: "Net Long-Term Capital Gain (Loss)"
      "self_employment_earnings",      # Box 14: "Self-Employment Earnings (Loss)"
      "qbi_deduction",                 # Box 20z: QBI can reflect net loss
      "capital_account_beginning",     # Section L: can be negative per IRS guidance
      "capital_account_ending",        # Section L: can be negative per IRS guidance
  }
  # No sign constraint needed for these fields
  ```
- **Severity**: N/A (informational -- defines which fields have no sign constraint)

#### RULE FC-021: Fields That Must Be Non-Negative

- **Description**: Certain K-1 boxes represent amounts that are inherently non-negative: payments received, income types that cannot be losses, deductions (reported as positive amounts), taxes paid, and distributions. A negative value in any of these fields indicates an extraction error.
- **IRS Basis**: Partner's Instructions for Schedule K-1: guaranteed payments are compensation amounts; interest and dividends are income items; Section 179 is a deduction amount; foreign taxes are taxes paid; distributions are cash/property received. Barcode Specification: these fields are numeric but conceptually non-negative.
- **Fields Involved**: `guaranteed_payments`, `interest_income`, `ordinary_dividends`, `qualified_dividends`, `section_179_deduction`, `foreign_taxes_paid`, `distributions`
- **Validation Logic**:
  ```python
  MUST_BE_NON_NEGATIVE = {
      "guaranteed_payments",     # Box 4: payments for services/capital
      "interest_income",         # Box 5: interest earned
      "ordinary_dividends",      # Box 6a: dividends received
      "qualified_dividends",     # Box 6b: subset of ordinary dividends
      "section_179_deduction",   # Box 12: deduction amount
      "foreign_taxes_paid",      # Box 16: taxes paid
      "distributions",           # Box 19: cash/property distributed
  }
  for field in MUST_BE_NON_NEGATIVE:
      value = getattr(data, field)
      if value is not None:
          assert value >= 0, f"{field} must be non-negative, got {value}"
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `guaranteed_payments = 50000.0`, `distributions = 0.0`
  - Invalid: `guaranteed_payments = -5000.0`, `interest_income = -100.0`

---

### 3.4 Cross-Field Validation

#### RULE FC-030: Qualified Dividends Cannot Exceed Ordinary Dividends

- **Description**: Qualified dividends (Box 6b) are a subset of ordinary dividends (Box 6a). By definition, the qualified amount can never be larger than the total ordinary dividend amount. This rule is enforced by all major tax preparation software (TurboTax, TaxAct, TaxSlayer, etc.).
- **IRS Basis**: IRS Form 1099-DIV Instructions: "Qualified dividends are also reported in box 1a as part of ordinary dividends." Partner's Instructions for Schedule K-1: Box 6b is defined as "Qualified dividends" which are included in the Box 6a total.
- **Fields Involved**: `qualified_dividends`, `ordinary_dividends`
- **Validation Logic**:
  ```python
  if qualified_dividends is not None and ordinary_dividends is not None:
      assert qualified_dividends <= ordinary_dividends, (
          f"Qualified dividends ({qualified_dividends}) cannot exceed "
          f"ordinary dividends ({ordinary_dividends})"
      )
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `ordinary_dividends = 10000, qualified_dividends = 7500`
  - Valid: `ordinary_dividends = 5000, qualified_dividends = 5000` (all dividends are qualified)
  - Valid: `ordinary_dividends = 5000, qualified_dividends = 0`
  - Invalid: `ordinary_dividends = 5000, qualified_dividends = 6000`

#### RULE FC-031: Section 179 Deduction Statutory Limit

- **Description**: IRC Section 179(b)(1) sets an annual statutory limit on the Section 179 expense deduction. For 2024, this limit is $1,220,000 (indexed for inflation annually). A K-1 reporting a Section 179 amount exceeding this limit is invalid. Note: this is a per-taxpayer limit, not per-K-1, but a single K-1 cannot allocate more than the full statutory limit.
- **IRS Basis**: IRC Section 179(b)(1); IRS Form 4562 Instructions; the limit is adjusted annually for inflation.
- **Fields Involved**: `section_179_deduction`, `tax_year`
- **Validation Logic**:
  ```python
  SECTION_179_LIMITS = {
      "2020": 1_040_000,
      "2021": 1_050_000,
      "2022": 1_080_000,
      "2023": 1_160_000,
      "2024": 1_220_000,
      "2025": 1_250_000,  # estimated, confirm when IRS publishes
  }
  if section_179_deduction is not None and section_179_deduction > 0:
      limit = SECTION_179_LIMITS.get(tax_year, 1_250_000)  # default to recent limit
      if section_179_deduction > limit:
          error(f"Section 179 deduction ({section_179_deduction}) exceeds "
                f"statutory limit ({limit}) for tax year {tax_year}")
  ```
- **Severity**: Critical
- **Example**:
  - Valid: `section_179_deduction = 500000` (2024)
  - Valid: `section_179_deduction = 1220000` (2024, at the limit)
  - Invalid: `section_179_deduction = 2000000` (2024, exceeds $1.22M limit)

#### RULE FC-032: Self-Employment Earnings vs Partner Type Consistency

- **Description**: The relationship between self-employment earnings (Box 14) and partner type has specific IRS rules. Limited partners should generally only report self-employment earnings equal to their guaranteed payments. General partners report their full distributive share plus guaranteed payments. If a limited partner shows large self-employment earnings with zero guaranteed payments, the data is likely incorrect.
- **IRS Basis**: IRC Section 1402(a)(13); Partner's Instructions for Schedule K-1, Box 14: "If you are a limited partner, amounts in box 14, code A (net earnings from self-employment) are generally only guaranteed payments for services." Drake Tax KB Article 10149.
- **Fields Involved**: `partner_type`, `self_employment_earnings`, `guaranteed_payments`
- **Validation Logic**:
  ```python
  if partner_type and "limited" in partner_type.lower():
      if (self_employment_earnings is not None
          and guaranteed_payments is not None
          and abs(self_employment_earnings) > abs(guaranteed_payments) * 1.1):
          warn("Limited partner self-employment earnings significantly exceed "
               "guaranteed payments -- verify partner classification")
  ```
- **Severity**: Warning
- **Example**:
  - Valid: `partner_type="limited partner", self_employment_earnings=50000, guaranteed_payments=50000`
  - Suspicious: `partner_type="limited partner", self_employment_earnings=500000, guaranteed_payments=0`

#### RULE FC-033: General Partner Self-Employment Earnings Presence

- **Description**: A general partner of a partnership that has ordinary business income should typically have non-zero self-employment earnings. Zero self-employment earnings for a general partner when the partnership has positive ordinary income is unusual and warrants review.
- **IRS Basis**: IRC Section 1402(a); general partners' distributive share of ordinary business income is subject to self-employment tax. Partner's Instructions for Schedule K-1, Box 14.
- **Fields Involved**: `partner_type`, `self_employment_earnings`, `ordinary_business_income`, `guaranteed_payments`
- **Validation Logic**:
  ```python
  if partner_type and "general" in partner_type.lower():
      has_income = (
          (ordinary_business_income is not None and ordinary_business_income > 0) or
          (guaranteed_payments is not None and guaranteed_payments > 0)
      )
      if has_income and (self_employment_earnings is None or self_employment_earnings == 0):
          warn("General partner has income but zero self-employment earnings")
  ```
- **Severity**: Warning
- **Example**:
  - Valid: `partner_type="general partner", ordinary_business_income=200000, self_employment_earnings=200000`
  - Suspicious: `partner_type="general partner", ordinary_business_income=200000, self_employment_earnings=0`

---

### 3.5 Magnitude Sanity Checks

#### RULE FC-040: Monetary Amount Magnitude Checks

- **Description**: While the IRS does not impose maximum dollar amounts on K-1 boxes, extremely large or small values often indicate OCR/extraction errors (e.g., decimal point misplacement, extra digits). These thresholds serve as warning-level checks to flag amounts that warrant human review. The thresholds below are conservative and designed to catch errors while allowing legitimately large partnership allocations.
- **IRS Basis**: IRS Barcode Specification defines monetary fields as 12 characters maximum (up to 999,999,999,999 before sign). Practical limits are based on IRS Statistics of Income data for partnership returns.
- **Fields Involved**: All monetary fields
- **Validation Logic**:
  ```python
  MAGNITUDE_THRESHOLDS = {
      # field_name: (min_warning, max_warning)
      "ordinary_business_income":     (-50_000_000, 100_000_000),
      "rental_real_estate_income":    (-20_000_000,  20_000_000),
      "guaranteed_payments":          (          0,   5_000_000),
      "interest_income":              (          0,  10_000_000),
      "ordinary_dividends":           (          0,  50_000_000),
      "qualified_dividends":          (          0,  50_000_000),
      "short_term_capital_gains":     (-50_000_000, 100_000_000),
      "long_term_capital_gains":      (-50_000_000, 500_000_000),
      "section_179_deduction":        (          0,   1_500_000),
      "self_employment_earnings":     (-10_000_000,  10_000_000),
      "foreign_taxes_paid":           (          0,   1_000_000),
      "distributions":                (          0, 100_000_000),
      "qbi_deduction":                (-50_000_000, 100_000_000),
      "capital_account_beginning":    (-50_000_000, 500_000_000),
      "capital_account_ending":       (-50_000_000, 500_000_000),
  }
  for field, (lo, hi) in MAGNITUDE_THRESHOLDS.items():
      value = getattr(data, field)
      if value is not None and (value < lo or value > hi):
          warn(f"{field} value {value} is outside typical range [{lo}, {hi}]")
  ```
- **Severity**: Advisory
- **Example**:
  - Flagged: `ordinary_business_income = 999_999_999` (likely decimal point error)
  - Flagged: `guaranteed_payments = 50_000_000` (unusually large)
  - Not flagged: `long_term_capital_gains = 100_000_000` (large but plausible for investment partnerships)

---

### 3.6 Capital Account Constraints

#### RULE FC-050: Capital Accounts Can Be Negative

- **Description**: Unlike a partner's outside basis (which can never go below zero), a partner's tax-basis capital account CAN be negative. This commonly occurs when the partnership allocates losses or makes distributions funded by partnership debt. Negative capital accounts are explicitly addressed in IRS guidance and are valid.
- **IRS Basis**: IRS FAQ on negative capital accounts (2019); Drake Tax KB Article 16232; Partnership Tax Complications guidance from IRS. "A partner's basis in the partnership interest can never be negative, however, a partner's capital account can be negative."
- **Fields Involved**: `capital_account_beginning`, `capital_account_ending`
- **Validation Logic**:
  ```python
  # No sign constraint -- negative values are valid
  # But flag extremely negative values as potentially erroneous
  if capital_account_beginning is not None and capital_account_beginning < -50_000_000:
      warn("Beginning capital account is extremely negative -- verify")
  if capital_account_ending is not None and capital_account_ending < -50_000_000:
      warn("Ending capital account is extremely negative -- verify")
  ```
- **Severity**: Advisory (for extreme values only)
- **Example**:
  - Valid: `capital_account_beginning = -150000` (partnership allocated losses exceeding contributions)
  - Valid: `capital_account_ending = -500000` (large loss year)
  - Flagged: `capital_account_ending = -999_999_999` (likely extraction error)

#### RULE FC-051: Capital Account Directional Consistency

- **Description**: The ending capital account should differ from the beginning capital account in a direction that is broadly consistent with the partner's share of income (Box 1) and distributions (Box 19). If the partner received large positive income and no distributions, yet the ending capital is lower than beginning, or vice versa, this warrants review. This is a soft check because other factors (contributions, other income/loss items, special allocations) also affect the capital account.
- **IRS Basis**: Schedule K-1 Section L: `ending = beginning + contributions + net_income - withdrawals +/- other_adjustments`. Partner's Instructions for Schedule K-1.
- **Fields Involved**: `capital_account_beginning`, `capital_account_ending`, `ordinary_business_income`, `distributions`
- **Validation Logic**:
  ```python
  if all(v is not None for v in [capital_account_beginning, capital_account_ending,
                                   ordinary_business_income, distributions]):
      expected_direction = ordinary_business_income - (distributions or 0)
      actual_change = capital_account_ending - capital_account_beginning
      # Only warn if the direction is very strongly contradicted
      if expected_direction > 100_000 and actual_change < -100_000:
          warn("Capital account decreased despite large positive income and modest distributions")
      if expected_direction < -100_000 and actual_change > 100_000:
          warn("Capital account increased despite large losses")
  ```
- **Severity**: Advisory
- **Example**:
  - Normal: `beginning=100000, ending=150000, income=80000, distributions=30000`
  - Suspicious: `beginning=100000, ending=50000, income=200000, distributions=0`

---

## 4. Edge Cases

### 4.1 Final K-1 (Partnership Termination or Partner Exit)

When a partnership terminates or a partner exits, the K-1 may show unusual values:
- `partner_share_percentage` may be `0.0` for the ending percentage
- `capital_account_ending` may be `0.0` or very close to zero (fully liquidated)
- `distributions` may equal or exceed `capital_account_beginning` (final distribution)
- The K-1 will have the "Final K-1" checkbox marked

**Implication**: Do not flag 0% ending percentage or zero ending capital as errors on a final K-1.

### 4.2 Mid-Year Partner Transfer

When a partnership interest is transferred mid-year:
- Both the transferor and transferee receive K-1s for the same partnership
- Their combined share percentages should sum to what the original partner held
- One partner may have beginning percentage > 0 and ending percentage = 0
- The other may have beginning percentage = 0 and ending percentage > 0

**Implication**: A K-1 with 0% beginning OR 0% ending (but not both) is valid for mid-year transfers.

### 4.3 Fiscal Year Partnerships

Most partnerships use a calendar tax year, but some use a fiscal year (e.g., ending June 30). In this case:
- `tax_year` still shows the year the fiscal year ends in (e.g., "2024" for a fiscal year ending June 30, 2024)
- All boxes report fiscal year amounts, not calendar year

**Implication**: Do not assume the tax year aligns with the calendar year.

### 4.4 Tiered Partnerships (Partnership as a Partner)

When one partnership is a partner in another:
- `partner_type` may be unusual (the "partner" is itself a partnership entity)
- Item I1 entity type would be "partnership" rather than "individual"
- Income amounts may be very large (aggregated from the upper-tier partnership)
- Self-employment earnings rules differ for partnerships as partners

**Implication**: Magnitude checks should be more lenient when the partner entity type is not an individual.

### 4.5 Negative Capital Account with Positive Basis

A partner can have a negative capital account but still have a positive outside basis because the outside basis includes the partner's share of partnership liabilities. This is common in leveraged real estate partnerships.

**Implication**: A negative `capital_account_ending` is NOT an error and does not mean the partner's basis is negative.

### 4.6 Zero Income K-1

A K-1 where all income/loss boxes are zero or null is technically valid. This occurs when:
- The partnership had no activity during the year but still exists
- The partner's share of all items rounds to zero
- The K-1 is issued solely to report capital account information or distributions

**Implication**: Do not require at least one income box to be non-zero.

### 4.7 Special Allocations -- Percentages May Not Match Income

Partnership agreements can specify "special allocations" where income items are allocated differently from the default profit/loss percentages. For example, a partner with a 25% profit share might receive 50% of the rental income due to a special allocation.

**Implication**: Do not validate that `ordinary_business_income` equals `partner_share_percentage * total_partnership_income`. The percentage is informational and actual allocations may differ.

### 4.8 QBI with Loss

Box 20, Code Z can report a negative QBI amount when the qualified business had a net loss for the year. This loss carries forward and reduces QBI in future years.

**Implication**: `qbi_deduction` can legitimately be negative.

---

## 5. Common OCR/AI Extraction Errors

These are known failure modes when using OCR or AI document processing to extract K-1 data. Validation rules should be designed to catch these patterns:

### 5.1 Negative Sign Misread

The K-1 form uses parentheses `(1,234)` to denote negative/loss amounts. OCR systems may:
- Drop the parentheses and extract `1234` as positive
- Extract only the opening parenthesis, producing garbled output
- Misinterpret parentheses as other characters

**Detection**: If a field that allows losses (e.g., Box 1, Box 2) has a positive value, and related fields suggest a loss (e.g., capital account decreased significantly), flag for review.

### 5.2 Decimal Point Displacement

OCR may misplace the decimal point, producing values off by a factor of 10, 100, or 1000:
- `$12,345` extracted as `$1,234.5` or `$123,450`
- `$1,234.00` extracted as `$123,400`

**Detection**: Magnitude checks (Rule FC-040) will catch most of these.

### 5.3 Comma/Period Confusion

In some scanned documents or international contexts:
- `1,234` misread as `1.234` (value reduced by ~1000x)
- `12.50` misread as `1,250` (value increased by 100x)

**Detection**: Cross-reference with partner share percentage and total partnership amounts if available.

### 5.4 Character Substitution

Common character confusions in OCR:
- `0` (zero) vs `O` (letter O)
- `1` (one) vs `l` (lowercase L) vs `I` (uppercase i)
- `5` vs `S`
- `8` vs `B`

**Detection**: Type validation will catch non-numeric characters in monetary fields.

### 5.5 Field Misalignment

OCR may extract the value from one box and assign it to an adjacent box:
- Box 1 value placed in Box 2
- Box 6a and 6b values swapped
- Section L beginning and ending values swapped

**Detection**: Cross-field rules (e.g., FC-030 qualified <= ordinary) help detect swaps. Capital account beginning/ending swaps may be harder to detect.

### 5.6 Blank vs Zero Confusion

An empty box on the K-1 means "not applicable" (should be `None`/`null`), while `0` means "applicable but the amount is zero." OCR systems may:
- Extract empty boxes as `0` instead of `None`
- Extract `0` values as empty/null

**Detection**: If many fields are exactly `0.0` (rather than `None`), the extraction may be confusing blanks with zeros. A K-1 with all boxes set to 0 is more suspicious than one with all boxes set to None.

### 5.7 Percentage Format Errors

Box J percentages may be extracted incorrectly:
- `25.5%` extracted as `255` (missing decimal)
- `25.5%` extracted as `0.255` (decimal form instead of percentage)
- `25.500%` extracted as `25500` (treating implied decimal literally)

**Detection**: Any percentage > 100 is clearly an extraction error (Rule FC-010). Values like `0.255` when the expected range is 1-100 suggest decimal form conversion needed.

### 5.8 Multi-Line Amount Truncation

Large dollar amounts that wrap across two lines on the form may be:
- Truncated to only the first line's digits
- Doubled by concatenating both lines

**Detection**: Magnitude checks will catch most cases. Unusually round numbers (e.g., exactly `100,000` when other amounts are precise) may indicate truncation.

### 5.9 Prior-Year Value Extraction

Box J has both beginning and ending percentages. Section L has both beginning and ending capital accounts. OCR may extract the wrong column's value:
- Beginning percentage extracted as the ending percentage
- Prior year's ending capital extracted as current year's ending

**Detection**: If `capital_account_beginning` equals `capital_account_ending` exactly and there is significant income or distributions, one value may be a copy of the other.

---

## 6. Complete Field Reference Table

| Field | K-1 Location | Type | Required | Can Be Negative | Non-Negative | Typical Range | Hard Constraint |
|---|---|---|---|---|---|---|---|
| `tax_year` | Header | `str` | Yes | N/A | N/A | 2015-2026 | 4-digit year, <= current year |
| `partnership_name` | Part I | `str` | Yes | N/A | N/A | 1-70 chars | Non-empty |
| `partner_type` | Item H2 | `str` | Yes | N/A | N/A | Enum | Must be valid partner type |
| `partner_share_percentage` | Box J | `float` | Conditional | No | Yes | 0.001-100 | >= 0, <= 100 |
| `ordinary_business_income` | Box 1 | `float` | No | Yes | -- | -$10M to +$50M | None |
| `rental_real_estate_income` | Box 2 | `float` | No | Yes | -- | -$500K to +$5M | None |
| `guaranteed_payments` | Box 4 | `float` | No | No | Yes | $0 to $2M | >= 0 |
| `interest_income` | Box 5 | `float` | No | No | Yes | $0 to $1M | >= 0 |
| `ordinary_dividends` | Box 6a | `float` | No | No | Yes | $0 to $5M | >= 0 |
| `qualified_dividends` | Box 6b | `float` | No | No | Yes | $0 to $5M | >= 0, <= ordinary_dividends |
| `short_term_capital_gains` | Box 8 | `float` | No | Yes | -- | -$10M to +$50M | None |
| `long_term_capital_gains` | Box 9a | `float` | No | Yes | -- | -$10M to +$100M | None |
| `section_179_deduction` | Box 12 | `float` | No | No | Yes | $0 to ~$1.22M | >= 0, <= IRC 179 limit |
| `self_employment_earnings` | Box 14 | `float` | No | Yes | -- | -$500K to +$5M | None |
| `foreign_taxes_paid` | Box 16 | `float` | No | No | Yes | $0 to $500K | >= 0 |
| `distributions` | Box 19 | `float` | No | No | Yes | $0 to $50M | >= 0 |
| `qbi_deduction` | Box 20z | `float` | No | Yes | -- | -$10M to +$50M | None |
| `capital_account_beginning` | Section L | `float` | Conditional | Yes | -- | -$5M to +$100M | None |
| `capital_account_ending` | Section L | `float` | Conditional | Yes | -- | -$5M to +$100M | None |

---

## 7. Sources

1. **IRS Instructions for Form 1065 (2025)** -- https://www.irs.gov/instructions/i1065
2. **Partner's Instructions for Schedule K-1 (Form 1065) (2025)** -- https://www.irs.gov/instructions/i1065sk1
3. **Schedule K-1 (Form 1065) PDF (2025)** -- https://www.irs.gov/pub/irs-pdf/f1065sk1.pdf
4. **IRS Schedule K-1 2D Barcode Specifications** -- https://www.irs.gov/e-file-providers/schedules-k-1-two-dimensional-bar-code-specifications
5. **IRS Schedule K-1 Form 1065 (Final) Barcode Field Spec** -- https://www.irs.gov/e-file-providers/schedule-k-1-form-1065-final
6. **IRS MeF Schemas and Business Rules for Form 1065** -- https://www.irs.gov/e-file-providers/valid-xml-schemas-and-business-rules-for-1065-modernized-e-file-mef
7. **TY 2024 MeF Business Rules for Form 1065** -- https://www.irs.gov/tax-professionals/ty-2024-valid-xml-schemas-and-business-rules-for-form-1065-modernized-e-file-mef
8. **IRS FAQ on Negative Capital Accounts** -- https://assets.kpmg.com/content/dam/kpmg/us/pdf/2019/04/19175.pdf
9. **Drake Tax: Negative Tax Basis Capital Account Information** -- https://kb.drakesoftware.com/kb/Drake-Tax/16232.htm
10. **Drake Tax: 1065 K-1 Line 14 Self-Employment** -- https://kb.drakesoftware.com/kb/Drake-Tax/10149.htm
11. **TurboTax Community: K-1 Box 6b Qualified Dividends Validation** -- https://ttlc.intuit.com/community/taxes/discussion/k-1-box-6b-should-not-be-greater-than-the-total-dividend-amount/00/3221240
12. **TaxAct: Schedule K-1 Partner Classification** -- https://www.taxact.com/support/22429/schedule-k-1-form-1065-partner-classification
13. **TaxSlayer Pro: Schedule K-1 Income (Loss) Items** -- https://support.taxslayerpro.com/hc/en-us/articles/360009304253-Schedule-K-1-Form-1065-Income-Loss-Items
14. **TaxSlayer Pro: Schedule K-1 Self-Employment Earnings (Loss)** -- https://support.taxslayerpro.com/hc/en-us/articles/360009304293-Schedule-K-1-Form-1065-Self-Employment-Earnings-Loss
15. **IRS Publication 541 -- Partnerships** -- https://www.irs.gov/publications/p541
16. **IRC Section 179(b)(1)** -- Annual deduction limit
17. **IRC Section 1402(a)(13)** -- Limited partner SE tax exclusion
18. **IRC Section 199A** -- Qualified Business Income Deduction
19. **IRS Form 4562 Instructions** -- https://www.irs.gov/instructions/i4562
20. **Gennai Blog: Common OCR Errors** -- https://www.gennai.io/blog/common-ocr-errors-fix-them
21. **Crowe LLP: Correcting Partnership Return Errors** -- https://www.crowe.com/insights/tax-news-highlights/correcting-partnership-return-errors
22. **KPMG: Tax Basis Capital Account Reporting** -- https://assets.kpmg.com/content/dam/kpmg/us/pdf/2021/03/ai-tax-matters-tax-basis-capital-account-reporting.pdf
