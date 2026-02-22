# Capital Account Reconciliation and Partner Allocation Rules

## 1. Overview

This document covers validation rules related to the **capital account analysis** reported in **Section L** of IRS Schedule K-1 (Form 1065) and the **partner allocation mechanics** that govern how partnership income, loss, and distributions flow to individual partners.

Capital account validation is the most complex area of K-1 validation because:

- The capital account on Section L is **not** the same as the partner's outside tax basis.
- Multiple reporting methods exist (though tax basis is now required for tax year 2020+).
- Partner share percentages can vary by income category due to Section 704(b) special allocations.
- Negative capital accounts are structurally valid in many circumstances.
- Several fields in the K1ExtractedData model interact with capital accounts but the model does not capture all components of the reconciliation formula.

The rules in this document are organized into: capital account reconciliation, partner share percentage validation, distribution rules, self-employment earnings constraints, and loss limitation context.

---

## 2. IRS References

| Reference | Topic |
|-----------|-------|
| **IRS Form 1065, Schedule K-1, Item L** | Capital account analysis structure and reporting |
| **IRS Instructions for Form 1065 (2025)** | Partnership return instructions including Section L |
| **IRS Partner's Instructions for Schedule K-1 (Form 1065) (2025)** | Partner-level instructions for interpreting K-1 items |
| **IRS Notice 2020-43** | Tax basis capital account reporting requirement |
| **IRS Publication 541 (Partnerships)** | General partnership tax rules |
| **IRS Publication 925** | Passive activity and at-risk rules |
| **26 USC Section 704** | Partner's distributive share |
| **26 USC Section 731** | Recognition of gain/loss on distributions |
| **26 USC Section 465** | At-risk limitations |
| **26 USC Section 469** | Passive activity loss limitations |
| **26 USC Section 461(l)** | Excess business loss limitation |
| **26 USC Section 752** | Partnership liabilities and their effect on basis |
| **Treasury Regulation 1.704-1(b)(2)** | Substantial economic effect rules and capital account maintenance |
| **Treasury Regulation 1.704-1(b)(2)(iv)** | Capital account maintenance requirements |
| **Treasury Regulation 1.731-1** | Distribution gain/loss recognition rules |
| **Treasury Regulation 1.465-27** | Qualified nonrecourse financing |
| **Treasury Regulation 1.752-2** | Partner's share of recourse liabilities |
| **Treasury Regulation 1.752-3** | Partner's share of nonrecourse liabilities |

---

## 3. Rules

### Rule CAP-001: Capital Account Reconciliation Check

**Description**: The ending capital account should be approximately explainable by the beginning capital account, the partner's share of income/loss items, and distributions. Section L of Schedule K-1 reports the capital account analysis using the formula:

```
Beginning Capital Account
+ Capital Contributed During the Year
+ Current Year Net Income (Loss)
+ Other Increases (Decreases)
- Withdrawals and Distributions
= Ending Capital Account
```

Since the K1ExtractedData model does not capture "Capital Contributed" or "Other Increases/Decreases," only a soft reconciliation is possible.

**IRS Basis**: IRS Form 1065, Schedule K-1, Item L; IRS Instructions for Form 1065 (2025), "Item L. Partner's Capital Account Analysis."

**Fields Involved**:
- `capital_account_beginning` (Section L - Beginning capital account)
- `capital_account_ending` (Section L - Ending capital account)
- `distributions` (Box 19 - Distributions)
- `ordinary_business_income` (Box 1)
- `rental_real_estate_income` (Box 2)
- `guaranteed_payments` (Box 4)
- `interest_income` (Box 5)
- `ordinary_dividends` (Box 6a)
- `short_term_capital_gains` (Box 8)
- `long_term_capital_gains` (Box 9a)
- `section_179_deduction` (Box 12)
- `foreign_taxes_paid` (Box 16)
- `qbi_deduction` (Box 20z)

**Validation Logic**:
```python
# Compute net income from all captured income/loss items
net_income = sum(filter(None, [
    ordinary_business_income,     # Box 1
    rental_real_estate_income,    # Box 2
    guaranteed_payments,          # Box 4 (included in partnership income before allocation)
    interest_income,              # Box 5
    ordinary_dividends,           # Box 6a
    short_term_capital_gains,     # Box 8
    long_term_capital_gains,      # Box 9a
]))

# Note: section_179_deduction, foreign_taxes_paid reduce income
net_deductions = sum(filter(None, [
    section_179_deduction,        # Box 12
    foreign_taxes_paid,           # Box 16
]))

distributions_amt = distributions or 0.0

# Expected ending (approximate - missing contributions and other adjustments)
expected_ending = (
    (capital_account_beginning or 0.0)
    + net_income
    - net_deductions
    - distributions_amt
)

actual_ending = capital_account_ending or 0.0
discrepancy = abs(actual_ending - expected_ending)

# Use a tolerance threshold - large discrepancies suggest missing
# contributions, other adjustments, or extraction errors
tolerance = max(abs(actual_ending) * 0.25, 10000.0)

if discrepancy > tolerance:
    flag_warning("CAP-001: Capital account reconciliation discrepancy")
```

**Severity**: Warning

**Example**:
- **Valid**: Beginning = $100,000; Box 1 income = $50,000; distributions = $20,000; Ending = $130,000 (matches: 100k + 50k - 20k = 130k)
- **Valid with contribution**: Beginning = $100,000; Box 1 income = $50,000; distributions = $20,000; Ending = $180,000 (explainable by a $50,000 contribution we don't capture)
- **Suspicious**: Beginning = $100,000; Box 1 income = $50,000; distributions = $20,000; Ending = $500,000 (discrepancy of $370,000 - likely extraction error)

---

### Rule CAP-002: Negative Capital Account is Valid

**Description**: A negative ending capital account is structurally valid and should NOT be flagged as an error. Negative capital accounts commonly arise when:

1. Distributions exceed contributions plus cumulative income.
2. Allocation of partnership losses exceeds contributions plus cumulative income.
3. Partnership debt increases the partner's outside basis (under Section 752) without affecting the capital account.

A partner can have a negative capital account but a positive outside tax basis because outside basis includes the partner's share of partnership liabilities, while the capital account does not.

**IRS Basis**: IRS Instructions for Form 1065 (2025); IRS Notice 2020-43; Treasury Regulation 1.704-1(b)(2)(iv); KPMG Tax Basis Capital Account Reporting guidance.

**Fields Involved**:
- `capital_account_ending` (Section L)

**Validation Logic**:
```python
# Do NOT flag negative capital accounts as errors
if capital_account_ending is not None and capital_account_ending < 0:
    # This is valid - no error or warning
    pass
```

**Severity**: Not applicable (informational only - no validation flag)

**Example**:
- **Valid**: Ending capital account = -$50,000 (partner may have $200,000 of basis from share of partnership recourse debt)
- **Valid**: Ending capital account = -$1,000,000 (common in leveraged real estate partnerships)

---

### Rule CAP-003: Partner Share Percentage Range

**Description**: The partner's share percentage reported in Box J must be between 0% and 100% inclusive. Box J reports separate percentages for profit, loss, and capital at both beginning and end of year. Our model captures a single `partner_share_percentage` value.

**IRS Basis**: IRS Form 1065, Schedule K-1, Item J ("Partner's Share of Profit, Loss, and Capital"); IRS Instructions for Form 1065 (2025).

**Fields Involved**:
- `partner_share_percentage` (Box J)

**Validation Logic**:
```python
if partner_share_percentage is not None:
    if partner_share_percentage < 0 or partner_share_percentage > 100:
        flag_error("CAP-003: Partner share percentage out of valid range [0, 100]")
```

**Severity**: Critical

**Example**:
- **Valid**: 25.5 (25.5% share)
- **Valid**: 0.001 (very small share, common in large partnerships)
- **Valid**: 99.99 (dominant partner)
- **Invalid**: 105.0 (exceeds 100%)
- **Invalid**: -5.0 (negative percentage)

---

### Rule CAP-004: Cross-Partner Share Percentage Sum

**Description**: When processing multiple K-1s from the same partnership (same `partnership_name` and `tax_year`), the sum of all partners' share percentages should equal 100% for each category (profit, loss, capital). Since our model captures a single `partner_share_percentage`, validate that the sum across all partners for the same partnership equals 100%.

**IRS Basis**: IRS Instructions for Form 1065 (2025), Item J; TurboTax and IRS e-file validation rules require percentages to total exactly 100.00000%.

**Fields Involved**:
- `partner_share_percentage` (Box J) across all K-1s for the same partnership
- `partnership_name`
- `tax_year`

**Validation Logic**:
```python
# Group K-1s by partnership_name and tax_year
for (partnership, year), k1_group in grouped_k1s.items():
    percentages = [
        k1.partner_share_percentage
        for k1 in k1_group
        if k1.partner_share_percentage is not None
    ]
    if percentages:
        total = sum(percentages)
        if abs(total - 100.0) > 0.01:  # Allow small floating-point tolerance
            flag_warning("CAP-004: Partner share percentages sum to "
                        f"{total}%, expected 100%")
```

**Severity**: Warning (we may not have all K-1s from a partnership)

**Example**:
- **Valid**: Partners with 60%, 25%, 15% = 100%
- **Warning**: Partners with 60%, 25% = 85% (missing partner K-1 or extraction error)
- **Note**: This rule is only meaningful when we have ALL K-1s from a single partnership

---

### Rule CAP-005: Qualified Dividends Cannot Exceed Ordinary Dividends

**Description**: Qualified dividends (Box 6b) are a **subset** of ordinary dividends (Box 6a). The qualified dividends amount must always be less than or equal to the ordinary dividends amount. This is a fundamental structural constraint of the K-1 form.

**IRS Basis**: IRS Partner's Instructions for Schedule K-1 (Form 1065) (2025), Boxes 6a and 6b; IRS Form 1065 Instructions: "Line 6a should include only taxable ordinary dividends, including any qualified dividends reported on line 6b."

**Fields Involved**:
- `ordinary_dividends` (Box 6a)
- `qualified_dividends` (Box 6b)

**Validation Logic**:
```python
if (qualified_dividends is not None and ordinary_dividends is not None):
    if qualified_dividends > ordinary_dividends:
        flag_error("CAP-005: Qualified dividends (Box 6b) exceed ordinary "
                  "dividends (Box 6a) - qualified dividends are a subset")

if (qualified_dividends is not None
    and qualified_dividends > 0
    and ordinary_dividends is None):
    flag_error("CAP-005: Qualified dividends reported but no ordinary "
              "dividends - Box 6b requires Box 6a >= Box 6b")
```

**Severity**: Critical

**Example**:
- **Valid**: ordinary_dividends = $10,000, qualified_dividends = $7,000
- **Valid**: ordinary_dividends = $10,000, qualified_dividends = $10,000 (all dividends qualified)
- **Valid**: ordinary_dividends = $10,000, qualified_dividends = $0 (no qualified dividends)
- **Invalid**: ordinary_dividends = $5,000, qualified_dividends = $8,000
- **Invalid**: ordinary_dividends = None, qualified_dividends = $3,000

---

### Rule CAP-006: Self-Employment Earnings and Partner Type Consistency

**Description**: Self-employment earnings (Box 14) interact with partner type according to strict IRS rules:

- **General partners**: SE earnings include ordinary business income (Box 1) + guaranteed payments (Box 4) + other trade/business items. SE earnings should generally be >= guaranteed_payments.
- **Limited partners**: SE earnings include ONLY guaranteed payments for services rendered. SE earnings should approximately equal guaranteed_payments (or be zero).
- **Corporations**: Corporations do not have self-employment earnings. SE earnings should be zero or absent.

**IRS Basis**: 26 USC Section 1402(a)(13); IRS Partner's Instructions for Schedule K-1 (Form 1065) (2025), Box 14; IRS Publication 541; IRC Section 707(c).

**Fields Involved**:
- `self_employment_earnings` (Box 14)
- `partner_type`
- `guaranteed_payments` (Box 4)
- `ordinary_business_income` (Box 1)

**Validation Logic**:
```python
se = self_employment_earnings or 0.0
gp = guaranteed_payments or 0.0
obi = ordinary_business_income or 0.0

if partner_type and "corporation" in partner_type.lower():
    if se != 0:
        flag_error("CAP-006: Corporation partner should not have "
                  "self-employment earnings")

elif partner_type and "limited" in partner_type.lower():
    # Limited partners: SE earnings should approximate guaranteed payments only
    if se > 0 and abs(se - gp) > max(abs(gp) * 0.10, 100.0):
        flag_warning("CAP-006: Limited partner SE earnings significantly "
                    "differ from guaranteed payments - limited partners "
                    "typically only include guaranteed payments in SE")

elif partner_type and "general" in partner_type.lower():
    # General partners: SE earnings should be >= guaranteed payments
    if se > 0 and gp > 0 and se < gp:
        flag_warning("CAP-006: General partner SE earnings less than "
                    "guaranteed payments - SE should include guaranteed "
                    "payments plus ordinary business income")
```

**Severity**: Critical (corporation case) / Warning (limited/general partner cases)

**Example**:
- **Valid**: General partner, SE = $120,000, guaranteed_payments = $50,000, ordinary_income = $70,000
- **Valid**: Limited partner, SE = $50,000, guaranteed_payments = $50,000
- **Valid**: Limited partner, SE = $0, guaranteed_payments = $0
- **Invalid**: Corporation, SE = $50,000 (corporations don't have SE)
- **Warning**: Limited partner, SE = $120,000, guaranteed_payments = $50,000 (SE should only be guaranteed payments)

---

### Rule CAP-007: Distribution Magnitude Warning

**Description**: When cash distributions (Box 19) significantly exceed the capital account beginning balance plus all current-year income items, this may indicate a data extraction error or a situation where the partner is receiving distributions funded by partnership debt (which is valid but notable). This is a soft warning, not an error.

**IRS Basis**: 26 USC Section 731(a)(1) - gain recognized when money distributed exceeds adjusted basis; IRS Instructions for Form 1065, Item L.

**Fields Involved**:
- `distributions` (Box 19)
- `capital_account_beginning` (Section L)
- All income fields (Boxes 1, 2, 4, 5, 6a, 8, 9a)

**Validation Logic**:
```python
if distributions is not None and distributions > 0:
    beginning = capital_account_beginning or 0.0
    total_income = sum(filter(None, [
        ordinary_business_income,
        rental_real_estate_income,
        guaranteed_payments,
        interest_income,
        ordinary_dividends,
        short_term_capital_gains,
        long_term_capital_gains,
    ]))

    available = beginning + max(total_income, 0)

    if available > 0 and distributions > available * 2.0:
        flag_warning("CAP-007: Distributions significantly exceed beginning "
                    "capital plus current income - verify extraction accuracy")
    elif available <= 0 and distributions > 0:
        flag_advisory("CAP-007: Distributions made with zero or negative "
                     "available capital - may be funded by partnership debt")
```

**Severity**: Warning / Advisory

**Example**:
- **Valid**: Beginning = $500,000, income = $100,000, distributions = $200,000
- **Warning**: Beginning = $100,000, income = $50,000, distributions = $500,000 (distributions exceed 2x available)
- **Advisory**: Beginning = -$50,000, income = $0, distributions = $30,000 (distributions with negative capital - likely debt-funded)

---

### Rule CAP-008: Capital Account Beginning Balance for New Partners

**Description**: A new partner's beginning capital account balance should be zero for their first year in the partnership. If the K-1 indicates this is the partner's first year (which we may infer from context), a non-zero beginning balance is unexpected unless they acquired their interest from another partner.

**IRS Basis**: IRS Instructions for Form 1065 (2025), Item L; Treasury Regulation 1.704-1(b)(2)(iv)(b) - initial capital account equals cash plus FMV of property contributed.

**Fields Involved**:
- `capital_account_beginning` (Section L)

**Validation Logic**:
```python
# This rule applies only if we can determine this is the partner's first year
# In multi-year processing, check if prior-year K-1 exists for this partner
if is_first_year_partner and capital_account_beginning is not None:
    if capital_account_beginning != 0:
        flag_advisory("CAP-008: New partner has non-zero beginning capital "
                     "account - may have acquired interest mid-year or from "
                     "another partner")
```

**Severity**: Advisory

**Example**:
- **Valid**: New partner, beginning = $0
- **Advisory**: New partner, beginning = $250,000 (may have purchased interest from exiting partner)

---

### Rule CAP-009: Tax Basis Reporting Method (Post-2020)

**Description**: Starting with tax year 2020, the IRS requires all partnerships to report capital accounts on Schedule K-1 using the **tax basis method**. Previously, partnerships could choose from tax basis, GAAP, Section 704(b), or other methods. For K-1s with tax_year >= 2020, the capital account should be reported on tax basis.

Our K1ExtractedData model does not capture the reporting method checkbox, but this context is important for interpreting capital account values.

**IRS Basis**: IRS Notice 2020-43; IRS Instructions for Form 1065 (2020 and later), Item L; Draft Form 1065 Instructions (Oct 2020) mandating the transactional approach.

**Fields Involved**:
- `tax_year`
- `capital_account_beginning` (Section L)
- `capital_account_ending` (Section L)

**Validation Logic**:
```python
# Informational context - no direct validation possible without
# the reporting method checkbox
if tax_year is not None:
    year = int(tax_year)
    if year >= 2020:
        # Capital accounts SHOULD be on tax basis
        # Cannot validate directly - note for downstream consumers
        pass
    else:
        # Pre-2020: multiple methods were allowed
        # Capital account values may not be on tax basis
        pass
```

**Severity**: Advisory (informational context only)

**Example**:
- **Expected**: tax_year = "2023", capital accounts reported on tax basis
- **Note**: tax_year = "2019", capital accounts may be GAAP, 704(b), or other

---

### Rule CAP-010: Partner Share Percentage and Income Consistency

**Description**: If a partner has a very small share percentage but reports disproportionately large income amounts, this may indicate an extraction error. Conversely, if a partner has a large share percentage but reports zero across all income categories, this may indicate missing data.

Note: Due to Section 704(b) special allocations, a partner CAN have different effective allocation percentages for different income items. This rule should be advisory only.

**IRS Basis**: 26 USC Section 704(b); Treasury Regulation 1.704-1(b)(2)(iii) - substantial economic effect; IRS Instructions for Form 1065, Item J.

**Fields Involved**:
- `partner_share_percentage` (Box J)
- All income fields

**Validation Logic**:
```python
if partner_share_percentage is not None and partner_share_percentage > 0:
    total_income = sum(abs(v) for v in [
        ordinary_business_income,
        rental_real_estate_income,
        guaranteed_payments,
        interest_income,
        ordinary_dividends,
        short_term_capital_gains,
        long_term_capital_gains,
    ] if v is not None)

    # Flag if percentage is tiny but income is very large
    if partner_share_percentage < 0.01 and total_income > 1_000_000:
        flag_advisory("CAP-010: Very small partner share percentage "
                     f"({partner_share_percentage}%) with large income "
                     "amounts - verify extraction accuracy")
```

**Severity**: Advisory

**Example**:
- **Valid**: 25% share, $250,000 total income
- **Valid**: 1% share, $10,000 total income
- **Advisory**: 0.001% share, $5,000,000 total income (very unusual combination)

---

### Rule CAP-011: Guaranteed Payments and Ordinary Business Income Relationship

**Description**: Guaranteed payments (Box 4) are payments made to a partner for services or use of capital, regardless of partnership income. They are deducted from partnership income before computing the ordinary business income allocation. As a result, guaranteed payments are separate from (and in addition to) the partner's share of ordinary business income. Both can be positive simultaneously. However, if guaranteed payments are very large relative to ordinary business income, this could indicate the partnership is paying out more than it earns, which is valid but notable.

**IRS Basis**: 26 USC Section 707(c); IRS Publication 541; IRS Instructions for Form 1065, Boxes 1 and 4.

**Fields Involved**:
- `guaranteed_payments` (Box 4)
- `ordinary_business_income` (Box 1)

**Validation Logic**:
```python
gp = guaranteed_payments or 0.0
obi = ordinary_business_income or 0.0

# Guaranteed payments can exist alongside any level of ordinary income
# No hard constraint exists, but flag unusual patterns
if gp > 0 and obi < 0 and abs(obi) > gp * 3:
    flag_advisory("CAP-011: Large ordinary business loss alongside "
                 "guaranteed payments - partnership may be unprofitable "
                 "after guaranteed payment deductions")
```

**Severity**: Advisory

**Example**:
- **Valid**: guaranteed_payments = $100,000, ordinary_income = $200,000
- **Valid**: guaranteed_payments = $100,000, ordinary_income = -$50,000 (partnership profitable before GP, loss after)
- **Advisory**: guaranteed_payments = $100,000, ordinary_income = -$500,000 (large loss after GP deduction)

---

## 4. Edge Cases

### 4.1 Negative Capital Account with Positive Basis

A partner can have a negative capital account (e.g., -$500,000) and still have a positive outside tax basis (e.g., +$200,000) because outside basis includes the partner's share of partnership liabilities under Section 752, while the capital account does not. This is extremely common in leveraged real estate partnerships and should never be flagged as an error.

### 4.2 Special Allocations Under Section 704(b)

Partnership agreements can allocate different items of income, gain, loss, or deduction in different ratios to different partners. For example:
- Partner A: 80% of depreciation deductions, 20% of capital gains
- Partner B: 20% of depreciation deductions, 80% of capital gains

This means the `partner_share_percentage` from Box J may not match the effective percentage for any individual income box. Validation should not assume that `box_amount = partnership_total * partner_share_percentage`.

### 4.3 Mid-Year Partner Changes

When a partner enters or exits the partnership mid-year:
- Box J may show different beginning and ending percentages
- The beginning capital account may be zero (new partner) or the ending capital account may be zero (exiting partner)
- Income items reflect only the partner's share for the period they were a partner
- A "final K-1" checkbox indicates the partner's interest was fully liquidated

### 4.4 Deficit Restoration Obligations (DROs)

When a partner has a negative capital account, they may have a **Deficit Restoration Obligation** requiring them to restore the deficit upon partnership liquidation. This is a legal obligation in the partnership agreement, not something visible on the K-1 itself. Its existence affects whether 704(b) allocations have substantial economic effect.

### 4.5 Qualified Nonrecourse Financing and Real Estate

In real estate partnerships, **qualified nonrecourse financing** (QNF) — loans from qualified lenders secured by real property — counts toward the partner's at-risk amount even though the partner isn't personally liable. This is a carve-out from the general rule that nonrecourse debt does not increase at-risk amounts. This means:
- Real estate partnership losses may be larger than expected based on the visible capital account
- The at-risk limitation may not apply as strictly for real estate activities

### 4.6 Marketable Securities Treated as Money

Under Section 731(c), marketable securities distributed to a partner are treated as **money** for purposes of gain recognition. This means a distribution of publicly traded stock can trigger gain even though it's technically a property distribution. Our model captures only the aggregate `distributions` amount and cannot distinguish cash from marketable securities.

### 4.7 Capital Account Reporting Method Transition (2019 to 2020)

When comparing K-1s across tax years spanning the 2019-2020 boundary, the beginning capital account for 2020 may show a significantly different number than the ending capital account for 2019 because of the mandatory switch from the previous reporting method (GAAP, 704(b), or other) to tax basis. This is not an error.

### 4.8 Tax-Exempt Income and Nondeductible Expenses

The capital account is adjusted for tax-exempt income (which increases capital but is not reported as taxable income) and nondeductible expenses (which decrease capital but are not reported as deductions). These items are not captured in our K1ExtractedData model, contributing to discrepancies in the reconciliation check (Rule CAP-001).

### 4.9 Section 743(b) Adjustments

When a partner purchases their interest from another partner, a Section 754 election may create a Section 743(b) basis adjustment. This adjustment affects the partner's tax basis but does NOT affect their capital account, further contributing to basis-vs-capital-account divergence.

---

## 5. Loss Limitation Cascade (Contextual Reference)

While loss limitations cannot be fully validated from K-1 data alone (they depend on partner-level information), understanding the cascade is important for interpreting K-1 values.

### 5.1 The Four-Tier Cascade

Partnership losses must pass through these limitations in strict order at the partner level:

1. **Basis Limitation**: Partner's deductible loss cannot exceed their adjusted basis in the partnership interest. Excess losses carry forward indefinitely.

2. **At-Risk Limitation (Section 465)**: Losses passing the basis test are further limited to the partner's "amount at risk." The at-risk amount includes cash contributions, adjusted basis of property contributed, recourse debt for which the partner is personally liable, and qualified nonrecourse financing (real estate only). Disallowed losses carry forward indefinitely.

3. **Passive Activity Loss Limitation (Section 469)**: Losses passing the at-risk test are limited if the activity is passive (the partner does not materially participate). Limited partners are generally presumed not to materially participate, with narrow exceptions. Suspended passive losses can offset future passive income or are released upon complete disposition of the activity.

4. **Excess Business Loss Limitation (Section 461(l))**: For 2024, aggregate business losses exceeding $305,000 (single) / $610,000 (MFJ) are disallowed. For 2025, the thresholds are $313,000 / $626,000. Disallowed amounts are treated as net operating loss carryforwards.

### 5.2 Relevance to Validation

The K-1 reports the partner's **allocated** share of partnership items before any of these limitations are applied. The K-1 amounts are correct even if the partner cannot deduct all losses. Validation should not attempt to apply these limitations — they are the partner's responsibility on their individual return.

---

## 6. Debt Types and Basis (Contextual Reference)

Understanding debt types explains why capital accounts and basis diverge:

| Debt Type | Increases Tax Basis? | Increases Capital Account? | Increases At-Risk Amount? |
|-----------|---------------------|---------------------------|--------------------------|
| Recourse (partner personally liable) | Yes | No | Yes |
| Nonrecourse (no personal liability) | Yes | No | No |
| Qualified Nonrecourse (real estate, qualified lender) | Yes | No | Yes (exception) |

This table explains why negative capital accounts are valid — a partner with $500,000 of basis from recourse debt but $0 of contributions could have a capital account of -$300,000 after receiving $300,000 of loss allocations, while still having $200,000 of positive basis.

---

## 7. Sources

### IRS Publications and Forms
- IRS Partner's Instructions for Schedule K-1 (Form 1065) (2025): https://www.irs.gov/instructions/i1065sk1
- IRS Instructions for Form 1065 (2025): https://www.irs.gov/instructions/i1065
- IRS Publication 541 (Partnerships, 12/2025): https://www.irs.gov/publications/p541
- IRS Publication 925 (Passive Activity and At-Risk Rules, 2024): https://www.irs.gov/publications/p925
- IRS Notice 2020-43: https://www.irs.gov/pub/irs-drop/n-20-43.pdf
- IRS Form 6198 Instructions (At-Risk Limitations): https://www.irs.gov/instructions/i6198
- IRS Form 8582 Instructions (Passive Activity Loss Limitations): https://www.irs.gov/instructions/i8582

### Internal Revenue Code
- 26 USC Section 704 (Partner's distributive share): https://www.law.cornell.edu/uscode/text/26/704
- 26 USC Section 731 (Gain/loss on distributions): https://www.law.cornell.edu/uscode/text/26/731
- 26 USC Section 465 (At-risk limitations): https://www.law.cornell.edu/uscode/text/26/465
- 26 USC Section 469 (Passive activity losses): https://www.law.cornell.edu/uscode/text/26/469
- 26 USC Section 752 (Partnership liabilities): via LII / Legal Information Institute

### Treasury Regulations
- Treas. Reg. 1.704-1 (Partner's distributive share): https://www.law.cornell.edu/cfr/text/26/1.704-1
- Treas. Reg. 1.731-1 (Distribution gain/loss recognition): https://www.law.cornell.edu/cfr/text/26/1.731-1
- Treas. Reg. 1.465-27 (Qualified nonrecourse financing): https://www.law.cornell.edu/cfr/text/26/1.465-27
- Treas. Reg. 1.752-2 (Recourse liabilities): https://www.law.cornell.edu/cfr/text/26/1.752-2
- Treas. Reg. 1.752-3 (Nonrecourse liabilities): https://www.law.cornell.edu/cfr/text/26/1.752-3

### Professional Guidance
- KPMG: Tax Basis Capital Account Reporting: https://assets.kpmg.com/content/dam/kpmg/us/pdf/2021/03/ai-tax-matters-tax-basis-capital-account-reporting.pdf
- KPMG: Choosing Method for Tax Basis Capital Reporting: https://kpmg.com/kpmg-us/content/dam/kpmg/pdf/2023/ai-tax-matters-choosing-method-for-tax-basis-capital-reporting.pdf
- PKF O'Connor Davies: Complying with Tax Basis Capital Requirement: https://www.pkfod.com/insights/complying-with-the-tax-basis-capital-requirement/
- Mahoney CPAs: Reporting Partnership Schedule K-1 Capital Accounts on Tax Basis: https://mahoneycpa.com/reporting-partnership-schedule-k-1-capital-accounts-on-the-tax-basis/
- Withum: Mastering Partnership Capital Accounts: https://www.withum.com/resources/mastering-partnership-capital-accounts-navigating-tax-complexities-and-equity-valuations/
- The Tax Adviser: Partnership Interests, Sec. 465 At-Risk Limit (April 2021): https://www.thetaxadviser.com/issues/2021/apr/partnership-interests-sec-465-at-risk-limit-form-6198/
- The Tax Adviser: Partnership Distributions Rules and Exceptions (August 2024): https://www.thetaxadviser.com/issues/2024/aug/partnership-distributions-rules-and-exceptions/
- The Tax Adviser: Revisiting At-Risk Rules for Partnerships (April 2019): https://www.thetaxadviser.com/issues/2019/apr/revisiting-at-risk-rules-partnerships/
- The Tax Adviser: Partnership Allocations Lacking Substantial Economic Effect (August 2020): https://www.thetaxadviser.com/issues/2020/aug/partnership-allocations-lacking-substantial-economic-effect/
- Grant Thornton: New Method for Tax Basis Capital Reporting: https://www.grantthornton.com/insights/alerts/tax/2020/flash/new-method-provided-for-tax-basis-capital-reporting
- BDO: IRS Makes Changes to Tax Capital Reporting Requirements: https://www.bdo.com/insights/tax/irs-makes-changes-to-tax-capital-reporting-requirements
- RubinBrown: Notice 2020-43 Proposes Two New Methods: https://www.rubinbrown.com/insights-events/insight-articles/focus-on-taxation-notice-2020-43-proposes-two-new-methods-for-reporting-partner-capital-accounts-on/
- CliftonLarsonAllen: Understanding Substantial Economic Effect: https://www.claconnect.com/en/resources/blogs/understanding-substantial-economic-effect-in-partnership-agreements
