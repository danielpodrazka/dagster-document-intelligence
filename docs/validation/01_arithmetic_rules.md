# K-1 Arithmetic Rules and Cross-Field Relationships

## Overview

This document defines the arithmetic relationships, subset constraints, and cross-field
validation rules for IRS Schedule K-1 (Form 1065) data extracted into the `K1ExtractedData`
Pydantic model. These rules are derived from official IRS instructions, form definitions,
and established tax preparation practices.

Arithmetic rules verify that values reported across multiple K-1 boxes are internally
consistent. They range from hard constraints (e.g., qualified dividends cannot exceed
ordinary dividends) to soft plausibility checks (e.g., capital account reconciliation
with missing intermediary fields).

Each rule is classified by severity:

- **Critical** -- a violation indicates definitively incorrect data.
- **Warning** -- a violation is strongly suggestive of an error but edge cases exist.
- **Advisory** -- a violation is unusual and worth flagging but may be legitimate.

---

## IRS References

| Reference | Description |
|-----------|-------------|
| Partner's Instructions for Schedule K-1 (Form 1065), 2024-2025 | Primary instructions for partners receiving K-1s; defines box contents and relationships |
| Instructions for Form 1065, 2025 | Partnership-level instructions; defines how Schedule K and K-1 items are computed |
| IRS Form 1065 Schedule K-1 (2025) | The actual form layout showing all boxes and Section L |
| IRC Section 1(h)(11) | Defines qualified dividends as a subset of ordinary dividends |
| IRC Section 1402(a)(13) | Limited partner exclusion from self-employment tax |
| IRC Section 199A | Qualified Business Income deduction rules |
| IRC Section 179 | Election to expense certain depreciable business assets |
| IRC Section 731 | Treatment of distributions from partnerships |
| IRS MeF Business Rules for Form 1065 | Electronic filing validation rules (available via IRS Registered User Portal) |

---

## Rules

### ARITH-001: Qualified Dividends Cannot Exceed Ordinary Dividends

**Description**: Box 6b (qualified dividends) is a subset of Box 6a (ordinary dividends).
Qualified dividends are those ordinary dividends that meet holding period and other
requirements to qualify for preferential capital gains tax rates. The IRS instructions
explicitly state: "Line 6a includes any qualified dividends reported on line 6b."

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Box 6a/6b instructions;
IRC Section 1(h)(11).

**Fields Involved**:
- `ordinary_dividends` (Box 6a)
- `qualified_dividends` (Box 6b)

**Validation Logic**:
```python
if (
    data.qualified_dividends is not None
    and data.ordinary_dividends is not None
    and data.qualified_dividends > data.ordinary_dividends
):
    raise ValidationError("qualified_dividends (Box 6b) cannot exceed ordinary_dividends (Box 6a)")
```

**Severity**: Critical

**Examples**:

| ordinary_dividends (6a) | qualified_dividends (6b) | Valid? | Reason |
|--------------------------|--------------------------|--------|--------|
| 10000.00 | 8000.00 | Yes | 6b < 6a |
| 5000.00 | 5000.00 | Yes | 6b == 6a (all dividends are qualified) |
| 5000.00 | 0.00 | Yes | No qualified dividends |
| 5000.00 | 7000.00 | No | 6b > 6a -- impossible by definition |
| None | 3000.00 | No | 6b present without 6a is suspicious |
| 0.00 | 0.00 | Yes | No dividends |

**Notes**: Tax software (TurboTax, TaxSlayer) explicitly validates this constraint and
will reject input where Box 6b exceeds Box 6a.

---

### ARITH-002: Partner Share Percentage Range

**Description**: Box J reports the partner's share of profit, loss, and capital as
percentages. Each percentage must be between 0 (exclusive) and 100 (inclusive). A partner
with a 0% share would not receive a K-1. Values above 100% are impossible for a single
partner's share.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Item J; partnership
agreement allocation rules under IRC Sections 704(a) and 704(b).

**Fields Involved**:
- `partner_share_percentage` (Box J)

**Validation Logic**:
```python
if data.partner_share_percentage is not None:
    if data.partner_share_percentage <= 0 or data.partner_share_percentage > 100:
        raise ValidationError("partner_share_percentage must be > 0 and <= 100")
```

**Severity**: Critical

**Examples**:

| partner_share_percentage | Valid? | Reason |
|--------------------------|--------|--------|
| 50.0 | Yes | Standard 50% partner |
| 100.0 | Yes | Sole partner |
| 0.5 | Yes | Small fractional interest |
| 33.33333 | Yes | One-third interest |
| 0.0 | No | A 0% partner would not receive a K-1 |
| -5.0 | No | Negative percentage is impossible |
| 150.0 | No | Exceeds 100% |

**Notes**: In reality, Box J contains six separate values: beginning and ending
percentages for profit, loss, and capital. Our model extracts a single
`partner_share_percentage`. The profit, loss, and capital percentages can differ from
each other per the partnership agreement, so this field likely corresponds to one of
the three (most commonly the profit-sharing percentage).

---

### ARITH-003: Non-Negative Field Constraints

**Description**: Certain K-1 fields represent amounts that by definition cannot be
negative. These include dividends (which are distributions from corporations, always
non-negative), guaranteed payments (compensation to a partner, always non-negative),
deductions (reported as positive amounts), taxes paid, and cash/property distributions.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), definitions of
each box; fundamental accounting principles.

**Fields Involved**:
- `ordinary_dividends` (Box 6a) -- dividends received are non-negative
- `qualified_dividends` (Box 6b) -- subset of 6a, non-negative
- `guaranteed_payments` (Box 4) -- payments for services/capital, non-negative
- `section_179_deduction` (Box 12) -- deduction amount, non-negative
- `foreign_taxes_paid` (Box 16) -- taxes paid, non-negative
- `distributions` (Box 19) -- cash/property distributed, non-negative

**Validation Logic**:
```python
non_negative_fields = [
    ("ordinary_dividends", "Box 6a"),
    ("qualified_dividends", "Box 6b"),
    ("guaranteed_payments", "Box 4"),
    ("section_179_deduction", "Box 12"),
    ("foreign_taxes_paid", "Box 16"),
    ("distributions", "Box 19"),
]

for field_name, box_label in non_negative_fields:
    value = getattr(data, field_name)
    if value is not None and value < 0:
        raise ValidationError(f"{field_name} ({box_label}) must be >= 0")
```

**Severity**: Critical

**Notes**: The following fields CAN be negative (representing a loss):
- `ordinary_business_income` (Box 1) -- can be a loss
- `rental_real_estate_income` (Box 2) -- can be a loss
- `interest_income` (Box 5) -- rarely negative but theoretically possible
- `short_term_capital_gains` (Box 8) -- can be a loss
- `long_term_capital_gains` (Box 9a) -- can be a loss
- `self_employment_earnings` (Box 14) -- can be a net loss
- `capital_account_beginning` (Section L) -- can be negative (deficit)
- `capital_account_ending` (Section L) -- can be negative (deficit)
- `qbi_deduction` (Box 20z) -- can be a loss if QBI is negative

---

### ARITH-004: Capital Account Year-Over-Year Continuity

**Description**: The beginning capital account for a given tax year must exactly equal
the ending capital account from the prior tax year. The IRS instructions state:
"Beginning capital account must match the prior year's ending capital account exactly."
This is a fundamental accounting continuity requirement.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Item L instructions;
IRS requirement for tax-basis capital account reporting.

**Fields Involved**:
- `capital_account_beginning` (Section L, current year)
- `capital_account_ending` (Section L, prior year)
- `tax_year` (to identify sequential years)

**Validation Logic**:
```python
# Requires access to prior year data for the same partner/partnership
if (
    current_year_data.capital_account_beginning is not None
    and prior_year_data is not None
    and prior_year_data.capital_account_ending is not None
):
    if current_year_data.capital_account_beginning != prior_year_data.capital_account_ending:
        raise ValidationError(
            f"capital_account_beginning ({current_year_data.capital_account_beginning}) "
            f"does not match prior year capital_account_ending "
            f"({prior_year_data.capital_account_ending})"
        )
```

**Severity**: Critical (when prior year data is available)

**Examples**:

| Prior Year Ending | Current Year Beginning | Valid? | Reason |
|-------------------|------------------------|--------|--------|
| 50000.00 | 50000.00 | Yes | Exact match |
| 50000.00 | 50001.00 | No | Mismatch -- OCR error or restatement |
| -10000.00 | -10000.00 | Yes | Deficit carried forward correctly |
| None | 25000.00 | N/A | Cannot validate without prior year |

**Notes**: Small discrepancies may indicate OCR extraction errors. Consider a tolerance
of +/- $1.00 for rounding, but flag any larger differences. A mismatch could also
indicate a mid-year partnership interest transfer or capital account restatement, which
would require manual review.

---

### ARITH-005: Capital Account Directional Plausibility

**Description**: Section L of the K-1 reconciles the partner's capital account:

```
Ending = Beginning + Contributions + Net Income + Other Increases
         - Withdrawals/Distributions - Other Decreases
```

Since our model does not extract "Capital Contributed," "Other Increases," or "Other
Decreases," we cannot validate the exact formula. However, we can perform a directional
plausibility check using the fields we do have.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Item L; Form 1065
Schedule M-2 (Analysis of Partners' Capital Accounts).

**Fields Involved**:
- `capital_account_beginning` (Section L)
- `capital_account_ending` (Section L)
- `ordinary_business_income` (Box 1)
- `rental_real_estate_income` (Box 2)
- `guaranteed_payments` (Box 4)
- `interest_income` (Box 5)
- `ordinary_dividends` (Box 6a)
- `short_term_capital_gains` (Box 8)
- `long_term_capital_gains` (Box 9a)
- `distributions` (Box 19)

**Validation Logic**:
```python
if (
    data.capital_account_beginning is not None
    and data.capital_account_ending is not None
):
    # Sum all known income items (those that flow into current year net income)
    known_income = sum(
        v for v in [
            data.ordinary_business_income,
            data.rental_real_estate_income,
            data.guaranteed_payments,
            data.interest_income,
            data.ordinary_dividends,
            data.short_term_capital_gains,
            data.long_term_capital_gains,
        ]
        if v is not None
    )

    known_distributions = data.distributions or 0.0

    # Estimated ending (missing: contributions, other increases/decreases)
    estimated_ending = (
        data.capital_account_beginning + known_income - known_distributions
    )

    actual_change = data.capital_account_ending - data.capital_account_beginning
    estimated_change = known_income - known_distributions
    unexplained_change = actual_change - estimated_change

    # Flag if unexplained change is very large relative to the account size
    account_magnitude = max(
        abs(data.capital_account_beginning), abs(data.capital_account_ending), 1.0
    )

    if abs(unexplained_change) > account_magnitude * 2.0:
        flag_warning(
            f"Capital account change has large unexplained component: "
            f"${unexplained_change:,.2f} (may indicate contributions or "
            f"other adjustments not captured in extracted fields)"
        )
```

**Severity**: Advisory

**Notes**: The "unexplained change" represents the net of contributions, other increases,
and other decreases that we do not extract. A large unexplained change is not necessarily
wrong -- it may reflect a significant capital contribution or distribution of property.
This check is most useful for catching OCR errors where a digit is misread, causing the
ending balance to be wildly inconsistent.

---

### ARITH-006: Self-Employment Earnings for General Partners

**Description**: For general partners, self-employment earnings (Box 14, Code A)
generally include the partner's distributive share of ordinary trade or business income
(Box 1) plus guaranteed payments for services (Box 4a). Since our model captures only
total guaranteed payments (Box 4c = 4a + 4b), this is an approximate check.

**IRS Basis**: IRC Section 1402(a); Partner's Instructions for Schedule K-1 (Form 1065),
Box 14 instructions; Schedule SE instructions.

**Fields Involved**:
- `self_employment_earnings` (Box 14, Code A)
- `ordinary_business_income` (Box 1)
- `guaranteed_payments` (Box 4)
- `partner_type` (Part II, Item G -- "general partner" or "limited partner")

**Validation Logic**:
```python
if (
    data.partner_type is not None
    and "general" in data.partner_type.lower()
    and data.self_employment_earnings is not None
    and data.ordinary_business_income is not None
):
    expected_se = (data.ordinary_business_income or 0.0) + (data.guaranteed_payments or 0.0)

    # Allow tolerance for adjustments (Section 179, unreimbursed expenses, etc.)
    tolerance = max(abs(expected_se) * 0.15, 1000.0)

    if abs(data.self_employment_earnings - expected_se) > tolerance:
        flag_warning(
            f"General partner self_employment_earnings ({data.self_employment_earnings}) "
            f"differs significantly from Box 1 + Box 4 ({expected_se})"
        )
```

**Severity**: Warning

**Examples**:

| partner_type | Box 1 | Box 4 | Box 14 (SE) | Valid? | Reason |
|--------------|-------|-------|-------------|--------|--------|
| General | 80000 | 20000 | 100000 | Yes | SE = Box 1 + Box 4 |
| General | 80000 | 20000 | 95000 | Yes | Close -- adjustments |
| General | 80000 | 20000 | 20000 | No | Missing Box 1 portion |
| General | 80000 | 0 | 80000 | Yes | No guaranteed payments |

**Notes**: The tolerance accounts for adjustments such as Section 179 deductions,
unreimbursed partner expenses, and other separately stated items that modify
self-employment income. Guaranteed payments for capital (Box 4b) are generally NOT
included in self-employment earnings, so if Box 4 is entirely capital payments,
the expected SE would be closer to just Box 1.

---

### ARITH-007: Self-Employment Earnings for Limited Partners

**Description**: For limited partners, self-employment earnings (Box 14, Code A)
should generally include only guaranteed payments for services rendered to the
partnership. Limited partners' distributive share of partnership ordinary income
(Box 1) is excluded from self-employment income under IRC Section 1402(a)(13).

**IRS Basis**: IRC Section 1402(a)(13); Partner's Instructions for Schedule K-1
(Form 1065), Box 14 instructions.

**Fields Involved**:
- `self_employment_earnings` (Box 14, Code A)
- `ordinary_business_income` (Box 1)
- `guaranteed_payments` (Box 4)
- `partner_type` (Part II, Item G)

**Validation Logic**:
```python
if (
    data.partner_type is not None
    and "limited" in data.partner_type.lower()
    and data.self_employment_earnings is not None
    and data.guaranteed_payments is not None
):
    if data.self_employment_earnings > data.guaranteed_payments + 100.0:
        flag_warning(
            f"Limited partner self_employment_earnings ({data.self_employment_earnings}) "
            f"exceeds guaranteed_payments ({data.guaranteed_payments}). "
            f"Limited partners should generally only have SE income from "
            f"guaranteed payments for services."
        )
```

**Severity**: Warning

**Examples**:

| partner_type | Box 1 | Box 4 | Box 14 (SE) | Valid? | Reason |
|--------------|-------|-------|-------------|--------|--------|
| Limited | 80000 | 10000 | 10000 | Yes | SE = guaranteed payments only |
| Limited | 80000 | 10000 | 0 | Yes | GP for capital only (not services) |
| Limited | 80000 | 10000 | 90000 | No | Includes Box 1 -- unusual for LP |
| Limited | 80000 | 0 | 0 | Yes | No guaranteed payments, no SE |

**Notes**: The definition of "limited partner" for SE tax purposes is complex. LLC
members classified as limited partners may still have SE income in some jurisdictions.
The IRS has proposed (but not finalized) regulations clarifying this. Flag violations
as warnings, not errors, due to this ambiguity.

---

### ARITH-008: QBI Deduction Plausibility

**Description**: The Qualified Business Income (QBI) amount reported in Box 20, Code Z
generally starts from the partner's share of ordinary business income (Box 1). However,
guaranteed payments for services are excluded from QBI, and other adjustments may apply.
The QBI deduction itself is up to 20% of the QBI amount, subject to W-2 wage and
property basis limitations.

**IRS Basis**: IRC Section 199A; Partner's Instructions for Schedule K-1 (Form 1065),
Box 20 Code Z instructions; Form 8995/8995-A instructions.

**Fields Involved**:
- `qbi_deduction` (Box 20z -- this is the QBI amount, not the 20% deduction)
- `ordinary_business_income` (Box 1)
- `guaranteed_payments` (Box 4)

**Validation Logic**:
```python
if (
    data.qbi_deduction is not None
    and data.ordinary_business_income is not None
):
    # QBI generally approximates Box 1 minus guaranteed payments for services
    # Since we cannot distinguish Box 4a from 4b, use total guaranteed payments
    estimated_qbi = data.ordinary_business_income - (data.guaranteed_payments or 0.0)

    tolerance = max(abs(data.ordinary_business_income) * 0.25, 5000.0)

    if abs(data.qbi_deduction - estimated_qbi) > tolerance:
        flag_advisory(
            f"qbi_deduction ({data.qbi_deduction}) differs significantly from "
            f"estimated QBI (Box 1 - Box 4 = {estimated_qbi}). "
            f"This may indicate other adjustments or a data extraction error."
        )
```

**Severity**: Advisory

**Examples**:

| Box 1 | Box 4 | Box 20z (QBI) | Valid? | Reason |
|-------|-------|---------------|--------|--------|
| 100000 | 20000 | 80000 | Yes | QBI = Box 1 - guaranteed payments |
| 100000 | 0 | 100000 | Yes | No GP exclusion needed |
| 100000 | 20000 | 100000 | Maybe | Box 4 may be for capital (not excluded) |
| 100000 | 0 | 5000 | Flag | Very different -- investigate |

**Notes**: Box 20 Code Z actually contains a statement with multiple sub-items including
QBI amount, W-2 wages, and UBIA of qualified property. Our `qbi_deduction` field
captures only the QBI amount. The wide tolerance accounts for the many possible
adjustments under Section 199A.

---

### ARITH-009: Ordinary Dividends Present When Qualified Dividends Reported

**Description**: If qualified dividends (Box 6b) are reported, ordinary dividends
(Box 6a) must also be reported and be at least as large. Additionally, if Box 6a is
null/missing but Box 6b has a value, this indicates a data extraction error.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Box 6a/6b
instructions. Box 6b is always a subset of Box 6a.

**Fields Involved**:
- `ordinary_dividends` (Box 6a)
- `qualified_dividends` (Box 6b)

**Validation Logic**:
```python
if data.qualified_dividends is not None and data.qualified_dividends > 0:
    if data.ordinary_dividends is None:
        raise ValidationError(
            "qualified_dividends (Box 6b) is reported but "
            "ordinary_dividends (Box 6a) is missing"
        )
```

**Severity**: Critical

**Notes**: This is a companion rule to ARITH-001. While ARITH-001 checks the magnitude
relationship, this rule catches the case where 6b is present but 6a was not extracted
at all (likely an OCR or extraction failure).

---

### ARITH-010: Foreign Taxes Require Income Context

**Description**: If foreign taxes paid (Box 16) is reported with a non-zero value, the
K-1 should generally have some income reported in other boxes. A K-1 with foreign taxes
but zero income across all boxes is suspicious and may indicate a data extraction error.

**IRS Basis**: IRC Section 901 (foreign tax credit); Form 1116 instructions. Foreign
taxes are paid on foreign-source income, implying income must exist.

**Fields Involved**:
- `foreign_taxes_paid` (Box 16)
- All income fields: Boxes 1, 2, 4, 5, 6a, 8, 9a

**Validation Logic**:
```python
if data.foreign_taxes_paid is not None and data.foreign_taxes_paid > 0:
    income_fields = [
        data.ordinary_business_income,
        data.rental_real_estate_income,
        data.guaranteed_payments,
        data.interest_income,
        data.ordinary_dividends,
        data.short_term_capital_gains,
        data.long_term_capital_gains,
    ]
    has_any_income = any(v is not None and v != 0 for v in income_fields)

    if not has_any_income:
        flag_warning(
            f"foreign_taxes_paid ({data.foreign_taxes_paid}) is reported "
            f"but no income is reported in any income box"
        )
```

**Severity**: Warning

**Notes**: It is technically possible for a partnership to generate foreign taxes with
no net income (e.g., losses in the current year), but this is unusual. The foreign
income may also be reported in boxes not captured by our model (e.g., Box 11 codes).

---

### ARITH-011: Section 179 Deduction Reasonableness

**Description**: The Section 179 deduction (Box 12) allows partnerships to pass through
the election to expense certain depreciable business assets. While the Section 179
limitation is computed at the partner level (not the partnership level), an extremely
large Section 179 deduction relative to ordinary business income is unusual.

**IRS Basis**: IRC Section 179; Partner's Instructions for Schedule K-1 (Form 1065),
Box 12 instructions.

**Fields Involved**:
- `section_179_deduction` (Box 12)
- `ordinary_business_income` (Box 1)

**Validation Logic**:
```python
if (
    data.section_179_deduction is not None
    and data.section_179_deduction > 0
    and data.ordinary_business_income is not None
    and data.ordinary_business_income > 0
):
    if data.section_179_deduction > data.ordinary_business_income * 5:
        flag_advisory(
            f"section_179_deduction ({data.section_179_deduction}) is much larger "
            f"than ordinary_business_income ({data.ordinary_business_income}). "
            f"Deductibility is limited at the partner level."
        )
```

**Severity**: Advisory

**Notes**: The actual Section 179 limitation for tax year 2024 is $1,220,000. The
deduction at the partner level cannot exceed the partner's aggregate taxable income
from all active business sources. A very large Section 179 amount is not wrong on the
K-1 itself (it is passed through), but it signals that the deduction may be partially
or fully limited on the partner's return.

---

### ARITH-012: Boxes 1 and 2 Are Independent

**Description**: Ordinary business income (Box 1) and rental real estate income (Box 2)
represent entirely separate categories of partnership income. They are independently
determined and have no arithmetic relationship. Box 1 represents active trade or
business income; Box 2 represents rental real estate activity income, which is generally
passive.

**IRS Basis**: Partner's Instructions for Schedule K-1 (Form 1065), Boxes 1 and 2
instructions. Separate reporting on Schedule E (Form 1040).

**Fields Involved**:
- `ordinary_business_income` (Box 1)
- `rental_real_estate_income` (Box 2)

**Validation Logic**: No cross-validation needed. These fields are independent.

**Severity**: N/A (informational)

**Notes**: Implementers should NOT create validation rules that assume a relationship
between these boxes. Both can be positive, both can be negative, one can be positive
while the other is negative, or either can be zero/null. A partnership can have both
operating income and rental activities simultaneously.

---

### ARITH-013: Short-Term and Long-Term Capital Gains Are Independent

**Description**: Short-term capital gains (Box 8) and long-term capital gains (Box 9a)
are determined by the holding period of the underlying assets. They are separately
computed and have no arithmetic relationship to each other.

**IRS Basis**: IRC Sections 1222 and 1223 (holding period rules); Partner's Instructions
for Schedule K-1 (Form 1065), Boxes 8 and 9a.

**Fields Involved**:
- `short_term_capital_gains` (Box 8)
- `long_term_capital_gains` (Box 9a)

**Validation Logic**: No cross-validation needed. These fields are independent.

**Severity**: N/A (informational)

**Notes**: Both can be gains or losses independently. The distinction is based on
whether assets were held for more than one year (long-term) or one year or less
(short-term).

---

### ARITH-014: Distributions Are Independent of Income

**Description**: Distributions (Box 19) represent cash or property returned to the
partner by the partnership. They are NOT a function of current-year income. A partner
can receive distributions exceeding current-year income (drawing down capital), or
receive no distributions despite significant income.

**IRS Basis**: IRC Section 731; Partner's Instructions for Schedule K-1 (Form 1065),
Box 19 instructions.

**Fields Involved**:
- `distributions` (Box 19)
- All income fields

**Validation Logic**: No cross-validation needed. Distributions are independent of
income.

**Severity**: N/A (informational)

**Notes**: Distributions that exceed the partner's adjusted basis in the partnership
are taxable as capital gains (IRC Section 731(a)), but this is a partner-level
calculation that cannot be validated from K-1 data alone.

---

## Edge Cases

### Negative Capital Accounts
A partner's capital account (Section L) can legitimately be negative, representing a
deficit. This occurs when the partner's share of losses and distributions exceeds their
contributions and share of income over the life of the partnership. Negative beginning
and ending balances are valid.

### Zero Ordinary Dividends with Zero Qualified Dividends
Both Box 6a and Box 6b being zero (or both being null) is valid. The constraint is only
that 6b cannot exceed 6a.

### Partnership with Only Rental Activity
A partnership may have rental real estate income (Box 2) but no ordinary business income
(Box 1). This is valid for a partnership that operates only as a real estate holding
entity.

### General Partner with No Self-Employment Earnings
A general partner may have zero self-employment earnings if the partnership is not
engaged in a trade or business (e.g., an investment partnership). In this case, Box 1
may be zero and Box 14 would also be zero.

### Limited Partner with Self-Employment Earnings
While unusual, a limited partner CAN have self-employment earnings from guaranteed
payments for services. This is not an error.

### Large Unexplained Capital Account Changes
A large difference between estimated and actual capital account changes may reflect
legitimate events such as a significant capital contribution, admission of new partners
(revaluation), or Section 754 elections. These should be flagged as advisory, not
rejected.

### QBI Deduction with Zero Ordinary Income
Box 20 Code Z can report QBI amounts even when Box 1 is zero if the QBI includes items
from other boxes or carryover amounts. This is unusual but not impossible.

### First-Year Partnerships
For a first-year partnership, `capital_account_beginning` is typically zero or equal to
the initial contribution. There is no prior year to validate against, so ARITH-004
(year-over-year continuity) does not apply.

### Mid-Year Partner Admissions or Withdrawals
When a partner joins or leaves mid-year, their Box J percentages may show different
beginning and ending values. The capital account beginning may not match any prior year
ending if this is a new partner.

---

## Sources

1. IRS Partner's Instructions for Schedule K-1 (Form 1065), Tax Years 2024-2025.
   https://www.irs.gov/instructions/i1065sk1
2. IRS Instructions for Form 1065, Tax Year 2025.
   https://www.irs.gov/instructions/i1065
3. IRS Schedule K-1 (Form 1065), Tax Year 2025.
   https://www.irs.gov/pub/irs-pdf/f1065sk1.pdf
4. IRS MeF Business Rules for Form 1065.
   https://www.irs.gov/e-file-providers/valid-xml-schemas-and-business-rules-for-1065-modernized-e-file-mef
5. IRC Section 1(h)(11) -- Qualified Dividends.
6. IRC Section 179 -- Election to Expense Certain Depreciable Business Assets.
7. IRC Section 199A -- Qualified Business Income Deduction.
8. IRC Section 731 -- Extent of Recognition of Gain or Loss on Distribution.
9. IRC Section 1402(a)(13) -- Limited Partner Exception for SE Tax.
10. IRC Sections 704(a), 704(b) -- Partnership Allocation Rules.
11. TurboTax Community: "K-1 Box 6b should not be greater than the total dividend amount."
    https://ttlc.intuit.com/community/taxes/discussion/k-1-box-6b-should-not-be-greater-than-the-total-dividend-amount/00/3221240
12. TaxSlayer Support: Schedule K-1 (Form 1065) Income (Loss) Items (Boxes 1-12).
    https://support.taxslayer.com/hc/en-us/articles/4403634501005
13. OurTaxPartner: "What Your Capital Account Tells You: Understanding Box L of Schedule K-1."
    https://ourtaxpartner.com/what-your-capital-account-tells-you-understanding-box-l-of-schedule-k-1/
14. SDO CPA: Schedule K-1 (Form 1065) Guide: Partner Tax Reporting.
    https://www.sdocpa.com/schedule-k-1-form-1065-guide/
15. Linda Keith CPA: "1065 K-1 Withdrawals Do Not Match Distributions."
    https://lindakeithcpa.com/1065-k-1-withdrawals-do-not-match-distributions/
16. TaxSlayer Pro: Schedule K-1 (Form 1065) Self-Employment Earnings (Loss).
    https://support.taxslayerpro.com/hc/en-us/articles/360009304293
17. Drake Tax: 1065 K-1 Line 14 Self-Employment.
    https://kb.drakesoftware.com/kb/Drake-Tax/10149.htm
