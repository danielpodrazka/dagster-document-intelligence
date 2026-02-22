# K-1 Validation System Design

## 1. Architecture Overview

The validation system uses a **two-track architecture** that separates deterministic rule checks from AI-powered judgment calls. Both tracks are Pydantic-native, leveraging `@field_validator` / `@model_validator` for Track 1 and `pydantic-ai` structured output for Track 2.

```
                        K1ExtractedData
                              |
              +---------------+---------------+
              |                               |
        Track 1: Deterministic          Track 2: AI-Powered
        (Pydantic Validators)           (Pydantic AI + DeepSeek)
              |                               |
    K1ValidationReport                K1AIValidationResult
    - DeterministicCheck[]            - reasonableness scores
    - severity: critical/             - anomaly flags
      warning/advisory                - coherence assessment
    - all rules run (non-raising)     - OCR confidence
              |                               |
              +---------------+---------------+
                              |
                    K1CombinedValidation
                    - deterministic_report
                    - ai_report
                    - overall_status
                              |
              +---------------+---------------+
              |                               |
    Cross-Partner Validation          Dagster Asset DAG
    PartnershipValidationResult       @asset_check + validation assets
    - share % sums
    - proportional allocations
    - duplicate detection
    - multi-year continuity
```

### Design Principles

1. **Non-raising validation**: All deterministic checks run to completion and collect results rather than failing on first violation. This produces a complete report on every run.
2. **Separation of concerns**: Extraction (`K1ExtractedData`) and validation (`K1ValidationReport`) are decoupled. The extraction model stays clean; validation is a downstream pass.
3. **Severity classification**: Every check has a severity level (Critical, Warning, Advisory) so consumers can filter by importance.
4. **Auditability**: AI validation results include the full conversation trail, matching the existing `ai_structured_extraction` pattern in `assets.py`.

---

## 2. Deterministic Validations Catalog

### 2.1 Validation Infrastructure Models

```python
from __future__ import annotations

import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class Severity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    ADVISORY = "advisory"


class DeterministicCheck(BaseModel):
    """Result of a single deterministic validation check."""

    rule_id: str = Field(..., description="Unique rule identifier, e.g. ARITH-001")
    rule_name: str = Field(..., description="Human-readable rule name")
    severity: Severity
    passed: bool
    message: str = Field("", description="Explanation when check fails")
    fields_involved: list[str] = Field(default_factory=list)


class K1ValidationReport(BaseModel):
    """Complete deterministic validation report for a single K-1."""

    checks: list[DeterministicCheck] = Field(default_factory=list)
    critical_count: int = 0
    warning_count: int = 0
    advisory_count: int = 0
    passed: bool = True  # True if zero critical failures

    @model_validator(mode="after")
    def compute_counts(self) -> "K1ValidationReport":
        self.critical_count = sum(
            1 for c in self.checks if c.severity == Severity.CRITICAL and not c.passed
        )
        self.warning_count = sum(
            1 for c in self.checks if c.severity == Severity.WARNING and not c.passed
        )
        self.advisory_count = sum(
            1 for c in self.checks if c.severity == Severity.ADVISORY and not c.passed
        )
        self.passed = self.critical_count == 0
        return self
```

### 2.2 Validation Runner

The validator takes a `K1ExtractedData` instance and returns a `K1ValidationReport` with all checks executed:

```python
from k1_pipeline.defs.assets import K1ExtractedData


def validate_k1(data: K1ExtractedData) -> K1ValidationReport:
    """Run all deterministic validation checks against extracted K-1 data."""
    checks: list[DeterministicCheck] = []

    checks.append(_check_arith_001(data))
    checks.append(_check_arith_002(data))
    checks.extend(_check_arith_003(data))
    checks.append(_check_arith_005(data))
    checks.append(_check_arith_006(data))
    checks.append(_check_arith_007(data))
    checks.append(_check_arith_008(data))
    checks.append(_check_arith_009(data))
    checks.append(_check_arith_010(data))
    checks.append(_check_arith_011(data))
    checks.extend(_check_fc_001_004(data))
    checks.append(_check_fc_010(data))
    checks.extend(_check_fc_021(data))
    checks.append(_check_fc_031(data))
    checks.append(_check_fc_032(data))
    checks.extend(_check_fc_040(data))
    checks.append(_check_cap_001(data))

    return K1ValidationReport(checks=checks)
```

### 2.3 Individual Check Implementations

#### Arithmetic Rules (from 01_arithmetic_rules.md)

```python
def _check_arith_001(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-001: Qualified dividends cannot exceed ordinary dividends."""
    fields = ["qualified_dividends", "ordinary_dividends"]
    if (
        data.qualified_dividends is not None
        and data.ordinary_dividends is not None
        and data.qualified_dividends > data.ordinary_dividends
    ):
        return DeterministicCheck(
            rule_id="ARITH-001",
            rule_name="Qualified dividends <= ordinary dividends",
            severity=Severity.CRITICAL,
            passed=False,
            message=(
                f"Qualified dividends ({data.qualified_dividends}) exceed "
                f"ordinary dividends ({data.ordinary_dividends})"
            ),
            fields_involved=fields,
        )
    return DeterministicCheck(
        rule_id="ARITH-001",
        rule_name="Qualified dividends <= ordinary dividends",
        severity=Severity.CRITICAL,
        passed=True,
        fields_involved=fields,
    )


def _check_arith_002(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-002: Partner share percentage must be > 0 and <= 100."""
    if data.partner_share_percentage is not None:
        if data.partner_share_percentage <= 0 or data.partner_share_percentage > 100:
            return DeterministicCheck(
                rule_id="ARITH-002",
                rule_name="Partner share percentage in valid range",
                severity=Severity.CRITICAL,
                passed=False,
                message=(
                    f"partner_share_percentage ({data.partner_share_percentage}) "
                    f"must be > 0 and <= 100"
                ),
                fields_involved=["partner_share_percentage"],
            )
    return DeterministicCheck(
        rule_id="ARITH-002",
        rule_name="Partner share percentage in valid range",
        severity=Severity.CRITICAL,
        passed=True,
        fields_involved=["partner_share_percentage"],
    )


def _check_arith_003(data: K1ExtractedData) -> list[DeterministicCheck]:
    """ARITH-003: Non-negative field constraints."""
    results = []
    non_negative_fields = [
        ("ordinary_dividends", "Box 6a"),
        ("qualified_dividends", "Box 6b"),
        ("guaranteed_payments", "Box 4"),
        ("section_179_deduction", "Box 12"),
        ("foreign_taxes_paid", "Box 16"),
        ("distributions", "Box 19"),
    ]
    for field_name, box_label in non_negative_fields:
        value = getattr(data, field_name, None)
        passed = value is None or value >= 0
        results.append(DeterministicCheck(
            rule_id=f"ARITH-003-{field_name}",
            rule_name=f"{field_name} non-negative",
            severity=Severity.CRITICAL,
            passed=passed,
            message="" if passed else f"{field_name} ({box_label}) = {value}, must be >= 0",
            fields_involved=[field_name],
        ))
    return results


def _check_arith_005(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-005: Capital account directional plausibility."""
    if data.capital_account_beginning is None or data.capital_account_ending is None:
        return DeterministicCheck(
            rule_id="ARITH-005",
            rule_name="Capital account directional plausibility",
            severity=Severity.ADVISORY,
            passed=True,
            fields_involved=["capital_account_beginning", "capital_account_ending"],
        )

    known_income = sum(
        v for v in [
            data.ordinary_business_income,
            data.rental_real_estate_income,
            data.guaranteed_payments,
            data.interest_income,
            data.ordinary_dividends,
            data.short_term_capital_gains,
            data.long_term_capital_gains,
        ] if v is not None
    )
    known_distributions = data.distributions or 0.0
    actual_change = data.capital_account_ending - data.capital_account_beginning
    estimated_change = known_income - known_distributions
    unexplained = actual_change - estimated_change
    magnitude = max(
        abs(data.capital_account_beginning), abs(data.capital_account_ending), 1.0
    )

    if abs(unexplained) > magnitude * 2.0:
        return DeterministicCheck(
            rule_id="ARITH-005",
            rule_name="Capital account directional plausibility",
            severity=Severity.ADVISORY,
            passed=False,
            message=(
                f"Large unexplained capital account change: ${unexplained:,.2f} "
                f"(may indicate contributions or other adjustments not captured)"
            ),
            fields_involved=[
                "capital_account_beginning", "capital_account_ending",
                "ordinary_business_income", "distributions",
            ],
        )
    return DeterministicCheck(
        rule_id="ARITH-005",
        rule_name="Capital account directional plausibility",
        severity=Severity.ADVISORY,
        passed=True,
        fields_involved=["capital_account_beginning", "capital_account_ending"],
    )


def _check_arith_006(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-006: SE earnings for general partners ~ Box 1 + Box 4."""
    if not (
        data.partner_type
        and "general" in data.partner_type.lower()
        and data.self_employment_earnings is not None
        and data.ordinary_business_income is not None
    ):
        return DeterministicCheck(
            rule_id="ARITH-006",
            rule_name="General partner SE earnings plausibility",
            severity=Severity.WARNING,
            passed=True,
            fields_involved=["self_employment_earnings", "ordinary_business_income",
                             "guaranteed_payments", "partner_type"],
        )

    expected_se = (data.ordinary_business_income or 0.0) + (data.guaranteed_payments or 0.0)
    tolerance = max(abs(expected_se) * 0.15, 1000.0)

    if abs(data.self_employment_earnings - expected_se) > tolerance:
        return DeterministicCheck(
            rule_id="ARITH-006",
            rule_name="General partner SE earnings plausibility",
            severity=Severity.WARNING,
            passed=False,
            message=(
                f"General partner SE earnings ({data.self_employment_earnings}) "
                f"differs significantly from Box 1 + Box 4 ({expected_se})"
            ),
            fields_involved=["self_employment_earnings", "ordinary_business_income",
                             "guaranteed_payments"],
        )
    return DeterministicCheck(
        rule_id="ARITH-006",
        rule_name="General partner SE earnings plausibility",
        severity=Severity.WARNING,
        passed=True,
        fields_involved=["self_employment_earnings", "ordinary_business_income",
                         "guaranteed_payments"],
    )


def _check_arith_007(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-007: Limited partner SE earnings should only include guaranteed payments."""
    if not (
        data.partner_type
        and "limited" in data.partner_type.lower()
        and data.self_employment_earnings is not None
        and data.guaranteed_payments is not None
    ):
        return DeterministicCheck(
            rule_id="ARITH-007",
            rule_name="Limited partner SE earnings constraint",
            severity=Severity.WARNING,
            passed=True,
            fields_involved=["self_employment_earnings", "guaranteed_payments", "partner_type"],
        )

    if data.self_employment_earnings > data.guaranteed_payments + 100.0:
        return DeterministicCheck(
            rule_id="ARITH-007",
            rule_name="Limited partner SE earnings constraint",
            severity=Severity.WARNING,
            passed=False,
            message=(
                f"Limited partner SE earnings ({data.self_employment_earnings}) "
                f"exceeds guaranteed payments ({data.guaranteed_payments})"
            ),
            fields_involved=["self_employment_earnings", "guaranteed_payments"],
        )
    return DeterministicCheck(
        rule_id="ARITH-007",
        rule_name="Limited partner SE earnings constraint",
        severity=Severity.WARNING,
        passed=True,
        fields_involved=["self_employment_earnings", "guaranteed_payments"],
    )


def _check_arith_008(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-008: QBI deduction plausibility vs Box 1 - Box 4."""
    if data.qbi_deduction is None or data.ordinary_business_income is None:
        return DeterministicCheck(
            rule_id="ARITH-008",
            rule_name="QBI deduction plausibility",
            severity=Severity.ADVISORY,
            passed=True,
            fields_involved=["qbi_deduction", "ordinary_business_income", "guaranteed_payments"],
        )

    estimated_qbi = data.ordinary_business_income - (data.guaranteed_payments or 0.0)
    tolerance = max(abs(data.ordinary_business_income) * 0.25, 5000.0)

    if abs(data.qbi_deduction - estimated_qbi) > tolerance:
        return DeterministicCheck(
            rule_id="ARITH-008",
            rule_name="QBI deduction plausibility",
            severity=Severity.ADVISORY,
            passed=False,
            message=(
                f"QBI ({data.qbi_deduction}) differs from estimated "
                f"Box 1 - Box 4 ({estimated_qbi})"
            ),
            fields_involved=["qbi_deduction", "ordinary_business_income", "guaranteed_payments"],
        )
    return DeterministicCheck(
        rule_id="ARITH-008",
        rule_name="QBI deduction plausibility",
        severity=Severity.ADVISORY,
        passed=True,
        fields_involved=["qbi_deduction", "ordinary_business_income", "guaranteed_payments"],
    )


def _check_arith_009(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-009: Ordinary dividends must be present when qualified dividends are reported."""
    if (
        data.qualified_dividends is not None
        and data.qualified_dividends > 0
        and data.ordinary_dividends is None
    ):
        return DeterministicCheck(
            rule_id="ARITH-009",
            rule_name="Ordinary dividends present when qualified reported",
            severity=Severity.CRITICAL,
            passed=False,
            message="Qualified dividends reported but ordinary dividends missing",
            fields_involved=["qualified_dividends", "ordinary_dividends"],
        )
    return DeterministicCheck(
        rule_id="ARITH-009",
        rule_name="Ordinary dividends present when qualified reported",
        severity=Severity.CRITICAL,
        passed=True,
        fields_involved=["qualified_dividends", "ordinary_dividends"],
    )


def _check_arith_010(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-010: Foreign taxes paid should have income context."""
    if data.foreign_taxes_paid is not None and data.foreign_taxes_paid > 0:
        income_fields = [
            data.ordinary_business_income, data.rental_real_estate_income,
            data.guaranteed_payments, data.interest_income,
            data.ordinary_dividends, data.short_term_capital_gains,
            data.long_term_capital_gains,
        ]
        has_income = any(v is not None and v != 0 for v in income_fields)
        if not has_income:
            return DeterministicCheck(
                rule_id="ARITH-010",
                rule_name="Foreign taxes require income context",
                severity=Severity.WARNING,
                passed=False,
                message=(
                    f"Foreign taxes paid ({data.foreign_taxes_paid}) "
                    f"but no income in any box"
                ),
                fields_involved=["foreign_taxes_paid"],
            )
    return DeterministicCheck(
        rule_id="ARITH-010",
        rule_name="Foreign taxes require income context",
        severity=Severity.WARNING,
        passed=True,
        fields_involved=["foreign_taxes_paid"],
    )


def _check_arith_011(data: K1ExtractedData) -> DeterministicCheck:
    """ARITH-011: Section 179 reasonableness relative to ordinary income."""
    if (
        data.section_179_deduction is not None
        and data.section_179_deduction > 0
        and data.ordinary_business_income is not None
        and data.ordinary_business_income > 0
        and data.section_179_deduction > data.ordinary_business_income * 5
    ):
        return DeterministicCheck(
            rule_id="ARITH-011",
            rule_name="Section 179 reasonableness",
            severity=Severity.ADVISORY,
            passed=False,
            message=(
                f"Section 179 ({data.section_179_deduction}) is much larger "
                f"than ordinary income ({data.ordinary_business_income})"
            ),
            fields_involved=["section_179_deduction", "ordinary_business_income"],
        )
    return DeterministicCheck(
        rule_id="ARITH-011",
        rule_name="Section 179 reasonableness",
        severity=Severity.ADVISORY,
        passed=True,
        fields_involved=["section_179_deduction", "ordinary_business_income"],
    )
```

#### Field Constraint Rules (from 02_field_constraints.md)

```python
def _check_fc_001_004(data: K1ExtractedData) -> list[DeterministicCheck]:
    """FC-001 through FC-004: Required field checks."""
    results = []

    # FC-001 / FC-002: tax_year present and valid format
    tax_year_valid = (
        data.tax_year is not None
        and data.tax_year != ""
        and bool(re.match(r"^\d{4}$", data.tax_year))
    )
    results.append(DeterministicCheck(
        rule_id="FC-001",
        rule_name="tax_year present and valid",
        severity=Severity.CRITICAL,
        passed=tax_year_valid,
        message="" if tax_year_valid else f"tax_year is missing or invalid: {data.tax_year!r}",
        fields_involved=["tax_year"],
    ))

    # FC-003: partnership_name present
    name_valid = (
        data.partnership_name is not None
        and len(data.partnership_name.strip()) > 0
    )
    results.append(DeterministicCheck(
        rule_id="FC-003",
        rule_name="partnership_name present",
        severity=Severity.CRITICAL,
        passed=name_valid,
        message="" if name_valid else "partnership_name is missing or empty",
        fields_involved=["partnership_name"],
    ))

    # FC-004: partner_type present and valid
    valid_types = {
        "general partner", "limited partner",
        "llc member-manager", "llc member",
        "general partner or llc member-manager",
        "limited partner or other llc member",
    }
    type_valid = (
        data.partner_type is not None
        and data.partner_type.lower().strip() in valid_types
    )
    results.append(DeterministicCheck(
        rule_id="FC-004",
        rule_name="partner_type present and valid",
        severity=Severity.CRITICAL,
        passed=type_valid,
        message="" if type_valid else f"partner_type invalid: {data.partner_type!r}",
        fields_involved=["partner_type"],
    ))

    return results


def _check_fc_010(data: K1ExtractedData) -> DeterministicCheck:
    """FC-010: partner_share_percentage in [0, 100]."""
    if data.partner_share_percentage is not None:
        if data.partner_share_percentage < 0 or data.partner_share_percentage > 100:
            return DeterministicCheck(
                rule_id="FC-010",
                rule_name="Partner share percentage range",
                severity=Severity.CRITICAL,
                passed=False,
                message=(
                    f"partner_share_percentage ({data.partner_share_percentage}) "
                    f"outside [0, 100]"
                ),
                fields_involved=["partner_share_percentage"],
            )
    return DeterministicCheck(
        rule_id="FC-010",
        rule_name="Partner share percentage range",
        severity=Severity.CRITICAL,
        passed=True,
        fields_involved=["partner_share_percentage"],
    )


def _check_fc_021(data: K1ExtractedData) -> list[DeterministicCheck]:
    """FC-021: Non-negative field enforcement."""
    results = []
    must_be_non_negative = [
        "guaranteed_payments", "interest_income", "ordinary_dividends",
        "qualified_dividends", "section_179_deduction",
        "foreign_taxes_paid", "distributions",
    ]
    for field_name in must_be_non_negative:
        value = getattr(data, field_name, None)
        passed = value is None or value >= 0
        results.append(DeterministicCheck(
            rule_id=f"FC-021-{field_name}",
            rule_name=f"{field_name} non-negative",
            severity=Severity.CRITICAL,
            passed=passed,
            message="" if passed else f"{field_name} = {value}, must be >= 0",
            fields_involved=[field_name],
        ))
    return results


def _check_fc_031(data: K1ExtractedData) -> DeterministicCheck:
    """FC-031: Section 179 statutory limit."""
    SECTION_179_LIMITS = {
        "2020": 1_040_000, "2021": 1_050_000, "2022": 1_080_000,
        "2023": 1_160_000, "2024": 1_220_000, "2025": 1_250_000,
    }
    if data.section_179_deduction is not None and data.section_179_deduction > 0:
        limit = SECTION_179_LIMITS.get(data.tax_year or "", 1_250_000)
        if data.section_179_deduction > limit:
            return DeterministicCheck(
                rule_id="FC-031",
                rule_name="Section 179 statutory limit",
                severity=Severity.CRITICAL,
                passed=False,
                message=(
                    f"Section 179 ({data.section_179_deduction}) exceeds "
                    f"statutory limit ({limit}) for {data.tax_year}"
                ),
                fields_involved=["section_179_deduction", "tax_year"],
            )
    return DeterministicCheck(
        rule_id="FC-031",
        rule_name="Section 179 statutory limit",
        severity=Severity.CRITICAL,
        passed=True,
        fields_involved=["section_179_deduction", "tax_year"],
    )


def _check_fc_032(data: K1ExtractedData) -> DeterministicCheck:
    """FC-032: SE earnings vs partner type consistency."""
    if data.partner_type and "limited" in data.partner_type.lower():
        if (
            data.self_employment_earnings is not None
            and data.guaranteed_payments is not None
            and abs(data.self_employment_earnings) > abs(data.guaranteed_payments) * 1.1
        ):
            return DeterministicCheck(
                rule_id="FC-032",
                rule_name="SE earnings vs partner type consistency",
                severity=Severity.WARNING,
                passed=False,
                message=(
                    f"Limited partner SE earnings ({data.self_employment_earnings}) "
                    f"significantly exceed guaranteed payments ({data.guaranteed_payments})"
                ),
                fields_involved=["self_employment_earnings", "guaranteed_payments", "partner_type"],
            )
    return DeterministicCheck(
        rule_id="FC-032",
        rule_name="SE earnings vs partner type consistency",
        severity=Severity.WARNING,
        passed=True,
        fields_involved=["self_employment_earnings", "guaranteed_payments", "partner_type"],
    )


def _check_fc_040(data: K1ExtractedData) -> list[DeterministicCheck]:
    """FC-040: Magnitude sanity checks for all monetary fields."""
    MAGNITUDE_THRESHOLDS = {
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
    results = []
    for field_name, (lo, hi) in MAGNITUDE_THRESHOLDS.items():
        value = getattr(data, field_name, None)
        passed = value is None or (lo <= value <= hi)
        results.append(DeterministicCheck(
            rule_id=f"FC-040-{field_name}",
            rule_name=f"{field_name} magnitude check",
            severity=Severity.ADVISORY,
            passed=passed,
            message="" if passed else (
                f"{field_name} = {value}, outside typical range [{lo:,}, {hi:,}]"
            ),
            fields_involved=[field_name],
        ))
    return results
```

#### Capital Account Rules (from 03_capital_account_rules.md)

```python
def _check_cap_001(data: K1ExtractedData) -> DeterministicCheck:
    """CAP-001: Capital account reconciliation (soft check)."""
    if data.capital_account_beginning is None or data.capital_account_ending is None:
        return DeterministicCheck(
            rule_id="CAP-001",
            rule_name="Capital account reconciliation",
            severity=Severity.WARNING,
            passed=True,
            fields_involved=["capital_account_beginning", "capital_account_ending"],
        )

    net_income = sum(filter(None, [
        data.ordinary_business_income, data.rental_real_estate_income,
        data.guaranteed_payments, data.interest_income,
        data.ordinary_dividends, data.short_term_capital_gains,
        data.long_term_capital_gains,
    ]))
    net_deductions = sum(filter(None, [
        data.section_179_deduction, data.foreign_taxes_paid,
    ]))
    distributions_amt = data.distributions or 0.0

    expected_ending = data.capital_account_beginning + net_income - net_deductions - distributions_amt
    discrepancy = abs(data.capital_account_ending - expected_ending)
    tolerance = max(abs(data.capital_account_ending) * 0.25, 10000.0)

    if discrepancy > tolerance:
        return DeterministicCheck(
            rule_id="CAP-001",
            rule_name="Capital account reconciliation",
            severity=Severity.WARNING,
            passed=False,
            message=(
                f"Capital account discrepancy: expected ~${expected_ending:,.2f}, "
                f"actual ${data.capital_account_ending:,.2f} "
                f"(difference: ${discrepancy:,.2f})"
            ),
            fields_involved=[
                "capital_account_beginning", "capital_account_ending",
                "ordinary_business_income", "distributions",
            ],
        )
    return DeterministicCheck(
        rule_id="CAP-001",
        rule_name="Capital account reconciliation",
        severity=Severity.WARNING,
        passed=True,
        fields_involved=["capital_account_beginning", "capital_account_ending"],
    )
```

### 2.4 Complete Rule Catalog

| Rule ID | Name | Severity | Track | Source Doc |
|---------|------|----------|-------|------------|
| ARITH-001 | Qualified dividends <= ordinary dividends | Critical | Deterministic | 01 |
| ARITH-002 | Partner share percentage range (0, 100] | Critical | Deterministic | 01 |
| ARITH-003 | Non-negative field constraints (6 fields) | Critical | Deterministic | 01 |
| ARITH-005 | Capital account directional plausibility | Advisory | Deterministic | 01 |
| ARITH-006 | General partner SE earnings ~ Box 1 + Box 4 | Warning | Deterministic | 01 |
| ARITH-007 | Limited partner SE <= guaranteed payments | Warning | Deterministic | 01 |
| ARITH-008 | QBI plausibility vs Box 1 - Box 4 | Advisory | Deterministic | 01 |
| ARITH-009 | Ordinary dividends present when qualified reported | Critical | Deterministic | 01 |
| ARITH-010 | Foreign taxes require income context | Warning | Deterministic | 01 |
| ARITH-011 | Section 179 reasonableness vs income | Advisory | Deterministic | 01 |
| FC-001 | tax_year present and valid | Critical | Deterministic | 02 |
| FC-003 | partnership_name present | Critical | Deterministic | 02 |
| FC-004 | partner_type present and valid | Critical | Deterministic | 02 |
| FC-010 | partner_share_percentage in [0, 100] | Critical | Deterministic | 02 |
| FC-021 | Non-negative fields (7 fields) | Critical | Deterministic | 02 |
| FC-031 | Section 179 statutory limit | Critical | Deterministic | 02 |
| FC-032 | SE earnings vs partner type | Warning | Deterministic | 02 |
| FC-040 | Magnitude sanity checks (15 fields) | Advisory | Deterministic | 02 |
| CAP-001 | Capital account reconciliation | Warning | Deterministic | 03 |
| --- | --- | --- | --- | --- |
| AI-001 | Overall data coherence | N/A | AI | Track 2 |
| AI-002 | OCR confidence assessment | N/A | AI | Track 2 |
| AI-003 | Partnership type reasonableness | N/A | AI | Track 2 |
| AI-004 | Anomaly detection | N/A | AI | Track 2 |
| AI-005 | Value reasonableness scoring | N/A | AI | Track 2 |

---

## 3. AI Validations Catalog

### 3.1 AI Validation Output Model

Following the existing `pydantic-ai` + DeepSeek pattern from `ai_structured_extraction` in `assets.py`:

```python
class AnomalyFlag(BaseModel):
    """A single anomaly detected by AI analysis."""
    field_name: str = Field(..., description="The field with the anomaly")
    description: str = Field(..., description="What is unusual about this value")
    confidence: float = Field(..., ge=0.0, le=1.0,
                              description="AI confidence that this is a real anomaly")
    suggested_correct_value: Optional[float] = Field(
        None, description="If the AI can suggest a correction"
    )


class K1AIValidationResult(BaseModel):
    """AI-generated validation assessment of extracted K-1 data."""

    overall_coherence_score: float = Field(
        ..., ge=0.0, le=1.0,
        description="0-1 score for how internally consistent all fields are"
    )
    ocr_confidence_score: float = Field(
        ..., ge=0.0, le=1.0,
        description="Estimated confidence that OCR extraction is correct"
    )
    partnership_type_assessment: str = Field(
        ..., description=(
            "Assessment of partnership type (investment, real estate, "
            "operating business, etc.) based on income patterns"
        )
    )
    partnership_type_consistency: float = Field(
        ..., ge=0.0, le=1.0,
        description="How consistent the data is with the assessed partnership type"
    )
    anomaly_flags: list[AnomalyFlag] = Field(
        default_factory=list,
        description="Fields flagged as potentially anomalous"
    )
    value_reasonableness: dict[str, float] = Field(
        default_factory=dict,
        description="Per-field reasonableness score (0-1), keyed by field name"
    )
    narrative_assessment: str = Field(
        ..., description="Free-text narrative explaining the AI's overall assessment"
    )
    potential_ocr_errors: list[str] = Field(
        default_factory=list,
        description="Fields likely affected by OCR misreads"
    )
    recommended_review_fields: list[str] = Field(
        default_factory=list,
        description="Fields that should be manually reviewed"
    )
```

### 3.2 AI Validation Agent

```python
from pydantic_ai import Agent


def run_ai_validation(
    k1_data: K1ExtractedData,
    deterministic_report: K1ValidationReport,
    sanitized_text: str,
) -> tuple[K1AIValidationResult, list[dict]]:
    """Run AI-powered validation on extracted K-1 data.

    Returns (result, ai_messages) for auditability.
    """
    system_prompt = """You are an expert tax accountant reviewing extracted K-1 data
for quality and accuracy. You have deep knowledge of IRS Schedule K-1 (Form 1065)
field relationships, typical value ranges, and common OCR extraction errors.

Your task is to assess the overall quality of the extracted data by:
1. Evaluating internal consistency across all fields
2. Identifying values that seem implausible or likely OCR errors
3. Assessing whether the income pattern matches a coherent partnership type
4. Flagging specific fields that warrant manual review
5. Estimating OCR extraction confidence based on value patterns

Common OCR errors to watch for:
- Decimal point displacement (values off by 10x, 100x, or 1000x)
- Sign errors (losses extracted as positive amounts)
- Field misalignment (values assigned to wrong boxes)
- Character substitution (0/O, 1/l/I, 5/S)
- Blank-vs-zero confusion

For the value_reasonableness dict, include a 0-1 score for every non-null field.
A score of 1.0 means the value looks correct; 0.0 means it is almost certainly wrong."""

    k1_json = k1_data.model_dump_json(indent=2)
    det_summary = deterministic_report.model_dump_json(indent=2)

    user_prompt = f"""Review the following K-1 extracted data and provide a quality assessment.

## Extracted K-1 Data
{k1_json}

## Deterministic Validation Results
{det_summary}

## Original OCR Text (sanitized)
{sanitized_text}"""

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=K1AIValidationResult,
        system_prompt=system_prompt,
    )

    result = agent.run_sync(user_prompt)
    ai_messages = _serialize_messages(result.all_messages())
    return result.output, ai_messages
```

### 3.3 AI Validation Checks

| Check | What the AI Evaluates | Output Field |
|-------|-----------------------|--------------|
| AI-001 | Are all fields internally consistent? Do income patterns tell a coherent story? | `overall_coherence_score` |
| AI-002 | Do values show signs of OCR errors (decimal displacement, sign errors, character substitution)? | `ocr_confidence_score`, `potential_ocr_errors` |
| AI-003 | Does the income pattern match a recognizable partnership type (investment, real estate, operating)? | `partnership_type_assessment`, `partnership_type_consistency` |
| AI-004 | Are any individual values anomalous given the overall context? | `anomaly_flags` |
| AI-005 | Per-field reasonableness given the partnership type and other field values | `value_reasonableness` |

---

## 4. Proposed Pydantic Models

### 4.1 Combined Validation Result

```python
class K1CombinedValidation(BaseModel):
    """Combined result of deterministic + AI validation for a single K-1."""

    k1_data: K1ExtractedData
    deterministic_report: K1ValidationReport
    ai_report: Optional[K1AIValidationResult] = None
    overall_status: str = Field(
        "pending",
        description="Overall validation status: passed, warnings, failed"
    )
    validated_at: str = Field(..., description="ISO timestamp of validation run")

    @model_validator(mode="after")
    def compute_overall_status(self) -> "K1CombinedValidation":
        if not self.deterministic_report.passed:
            self.overall_status = "failed"
        elif self.deterministic_report.warning_count > 0:
            self.overall_status = "warnings"
        elif (
            self.ai_report is not None
            and self.ai_report.overall_coherence_score < 0.5
        ):
            self.overall_status = "warnings"
        else:
            self.overall_status = "passed"
        return self
```

### 4.2 Cross-Partner Validation Models

```python
class CrossPartnerCheck(BaseModel):
    """Result of a single cross-partner validation check."""

    rule_id: str = Field(..., description="E.g. CROSS_A1_PROFIT_PCT_SUM")
    rule_name: str
    severity: Severity
    passed: bool
    message: str = ""
    partnership_name: str = ""
    tax_year: str = ""
    partners_involved: list[str] = Field(default_factory=list)


class PartnershipValidationResult(BaseModel):
    """Cross-partner validation results for a single partnership + tax year."""

    partnership_name: str
    tax_year: str
    partner_count: int
    checks: list[CrossPartnerCheck] = Field(default_factory=list)
    critical_count: int = 0
    warning_count: int = 0
    advisory_count: int = 0
    passed: bool = True

    @model_validator(mode="after")
    def compute_counts(self) -> "PartnershipValidationResult":
        self.critical_count = sum(
            1 for c in self.checks if c.severity == Severity.CRITICAL and not c.passed
        )
        self.warning_count = sum(
            1 for c in self.checks if c.severity == Severity.WARNING and not c.passed
        )
        self.advisory_count = sum(
            1 for c in self.checks if c.severity == Severity.ADVISORY and not c.passed
        )
        self.passed = self.critical_count == 0
        return self
```

### 4.3 Cross-Partner Validation Runner

```python
def validate_partnership(
    k1s: list[K1ExtractedData],
    partnership_name: str,
    tax_year: str,
) -> PartnershipValidationResult:
    """Run cross-partner validations on a group of K-1s from the same partnership."""
    checks: list[CrossPartnerCheck] = []

    # A1: Profit percentages must not exceed 100%
    percentages = [
        k1.partner_share_percentage
        for k1 in k1s
        if k1.partner_share_percentage is not None
    ]
    if percentages:
        total_pct = sum(percentages)
        checks.append(CrossPartnerCheck(
            rule_id="CROSS_A1",
            rule_name="Partner profit percentages sum <= 100%",
            severity=Severity.CRITICAL,
            passed=total_pct <= 100.0 + 0.01,
            message="" if total_pct <= 100.01 else (
                f"Partner percentages sum to {total_pct:.2f}%, exceeding 100%"
            ),
            partnership_name=partnership_name,
            tax_year=tax_year,
        ))

        # A2: Percentages sum below 100% (informational)
        if total_pct < 99.99:
            checks.append(CrossPartnerCheck(
                rule_id="CROSS_A2",
                rule_name="Partner percentages incomplete",
                severity=Severity.ADVISORY,
                passed=False,
                message=(
                    f"Partner percentages sum to {total_pct:.2f}%. "
                    f"{100 - total_pct:.2f}% of partners may not be ingested."
                ),
                partnership_name=partnership_name,
                tax_year=tax_year,
            ))

    # A3: Income allocation proportionality (when 2+ K-1s)
    if len(k1s) >= 2:
        income_fields = [
            "ordinary_business_income", "rental_real_estate_income",
            "interest_income", "ordinary_dividends",
            "short_term_capital_gains", "long_term_capital_gains",
        ]
        for field_name in income_fields:
            values = [
                (getattr(k1, field_name), k1.partner_share_percentage)
                for k1 in k1s
                if getattr(k1, field_name) is not None
                and k1.partner_share_percentage is not None
                and k1.partner_share_percentage > 0
            ]
            if len(values) >= 2:
                total_field = sum(v for v, _ in values)
                total_pct = sum(p for _, p in values)
                if total_field != 0 and total_pct > 0:
                    for value, pct in values:
                        expected_share = pct / total_pct
                        actual_share = value / total_field
                        deviation = abs(actual_share - expected_share) / max(expected_share, 0.001)
                        if deviation > 0.10:
                            checks.append(CrossPartnerCheck(
                                rule_id=f"CROSS_A3_{field_name}",
                                rule_name=f"{field_name} proportionality",
                                severity=Severity.WARNING,
                                passed=False,
                                message=(
                                    f"Partner allocation deviates "
                                    f"{deviation*100:.1f}% from pro-rata share"
                                ),
                                partnership_name=partnership_name,
                                tax_year=tax_year,
                            ))

    # A5: Partnership name consistency
    names = set(k1.partnership_name for k1 in k1s if k1.partnership_name)
    if len(names) > 1:
        checks.append(CrossPartnerCheck(
            rule_id="CROSS_A5",
            rule_name="Partnership identity consistency",
            severity=Severity.WARNING,
            passed=False,
            message=f"Inconsistent partnership names: {names}",
            partnership_name=partnership_name,
            tax_year=tax_year,
        ))

    return PartnershipValidationResult(
        partnership_name=partnership_name,
        tax_year=tax_year,
        partner_count=len(k1s),
        checks=checks,
    )
```

### 4.4 Multi-Year Continuity Validation

```python
def validate_year_over_year(
    current: K1ExtractedData,
    prior: K1ExtractedData,
) -> list[CrossPartnerCheck]:
    """Run multi-year continuity checks for the same partner-partnership pair."""
    checks = []

    # B1: Capital account continuity
    if (
        prior.capital_account_ending is not None
        and current.capital_account_beginning is not None
    ):
        if prior.capital_account_ending != current.capital_account_beginning:
            diff = abs(prior.capital_account_ending - current.capital_account_beginning)
            checks.append(CrossPartnerCheck(
                rule_id="CROSS_B1",
                rule_name="Capital account continuity",
                severity=Severity.CRITICAL,
                passed=False,
                message=(
                    f"Prior year ending ({prior.capital_account_ending}) != "
                    f"current year beginning ({current.capital_account_beginning}), "
                    f"difference: {diff}"
                ),
                tax_year=f"{prior.tax_year}->{current.tax_year}",
            ))
        else:
            checks.append(CrossPartnerCheck(
                rule_id="CROSS_B1",
                rule_name="Capital account continuity",
                severity=Severity.CRITICAL,
                passed=True,
                tax_year=f"{prior.tax_year}->{current.tax_year}",
            ))

    # B4: Partner type continuity
    if (
        prior.partner_type is not None
        and current.partner_type is not None
        and prior.partner_type.lower() != current.partner_type.lower()
    ):
        checks.append(CrossPartnerCheck(
            rule_id="CROSS_B4",
            rule_name="Partner type continuity",
            severity=Severity.WARNING,
            passed=False,
            message=(
                f"Partner type changed from {prior.partner_type} "
                f"to {current.partner_type}"
            ),
            tax_year=f"{prior.tax_year}->{current.tax_year}",
        ))

    return checks
```

### 4.5 Duplicate Detection

```python
class DuplicateCheckResult(BaseModel):
    """Result of duplicate detection for a K-1."""

    is_exact_duplicate: bool = False
    is_possible_amendment: bool = False
    existing_run_id: Optional[str] = None
    differing_fields: list[str] = Field(default_factory=list)
    message: str = ""
```

---

## 5. Dagster Integration Plan

### 5.1 Asset DAG Position

Validation fits into the existing pipeline after `ai_structured_extraction`:

```
raw_k1_pdf -> ocr_extracted_text -> sanitized_text -> ai_structured_extraction
                                                           |
                                              +------------+-----------+
                                              |                        |
                                     k1_deterministic_validation   k1_ai_validation
                                              |                        |
                                              +------------+-----------+
                                                           |
                                                  k1_combined_validation
                                                           |
                                                      final_report
```

Cross-partner validation runs as a separate downstream asset:

```
k1_combined_validation (multiple runs) -> cross_partner_validation
```

### 5.2 Deterministic Validation Asset

```python
@dg.asset(group_name="validation", deps=["ai_structured_extraction"])
def k1_deterministic_validation(
    config: K1RunConfig, s3: S3Storage
) -> dg.MaterializeResult:
    """Run deterministic Pydantic-based validation checks on extracted K-1 data."""
    structured_data = s3.read_json(
        s3.staging_key(config.run_id, "structured_k1.json")
    )
    k1_data = K1ExtractedData(**structured_data["extracted_data"])

    report = validate_k1(k1_data)
    report_dict = report.model_dump()

    staging_key = s3.staging_key(config.run_id, "deterministic_validation.json")
    s3.write_json(staging_key, {
        "report": report_dict,
        "validated_at": datetime.now(timezone.utc).isoformat(),
    })

    failed_checks = [c for c in report.checks if not c.passed]
    summary_lines = [
        f"- **{c.rule_id}** [{c.severity.value}]: {c.message}"
        for c in failed_checks
    ]
    summary_md = "\n".join(summary_lines) if summary_lines else "_All checks passed_"

    return dg.MaterializeResult(
        metadata={
            "passed": dg.MetadataValue.bool(report.passed),
            "critical_failures": dg.MetadataValue.int(report.critical_count),
            "warnings": dg.MetadataValue.int(report.warning_count),
            "advisories": dg.MetadataValue.int(report.advisory_count),
            "total_checks": dg.MetadataValue.int(len(report.checks)),
            "failed_checks": dg.MetadataValue.md(summary_md),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )
```

### 5.3 AI Validation Asset

```python
@dg.asset(
    group_name="validation",
    deps=["ai_structured_extraction", "k1_deterministic_validation", "sanitized_text"],
)
def k1_ai_validation(
    config: K1RunConfig, s3: S3Storage
) -> dg.MaterializeResult:
    """Run AI-powered validation using Pydantic AI + DeepSeek."""
    structured_data = s3.read_json(
        s3.staging_key(config.run_id, "structured_k1.json")
    )
    det_data = s3.read_json(
        s3.staging_key(config.run_id, "deterministic_validation.json")
    )
    sanitized_data = s3.read_json(
        s3.staging_key(config.run_id, "sanitized_text.json")
    )

    k1_data = K1ExtractedData(**structured_data["extracted_data"])
    det_report = K1ValidationReport(**det_data["report"])
    sanitized_text = sanitized_data["sanitized_text"]

    ai_result, ai_messages = run_ai_validation(k1_data, det_report, sanitized_text)

    staging_key = s3.staging_key(config.run_id, "ai_validation.json")
    s3.write_json(staging_key, {
        "ai_validation": ai_result.model_dump(),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "ai_interaction": {
            "model": "deepseek:deepseek-chat",
            "raw_messages": ai_messages,
        },
    })

    anomaly_lines = [
        f"- **{a.field_name}**: {a.description} (confidence: {a.confidence:.0%})"
        for a in ai_result.anomaly_flags
    ]
    anomaly_md = "\n".join(anomaly_lines) if anomaly_lines else "_No anomalies detected_"

    return dg.MaterializeResult(
        metadata={
            "coherence_score": dg.MetadataValue.float(ai_result.overall_coherence_score),
            "ocr_confidence": dg.MetadataValue.float(ai_result.ocr_confidence_score),
            "partnership_type": dg.MetadataValue.text(ai_result.partnership_type_assessment),
            "anomalies": dg.MetadataValue.md(anomaly_md),
            "review_fields": dg.MetadataValue.int(len(ai_result.recommended_review_fields)),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )
```

### 5.4 Asset Checks

Dagster `@asset_check` can be used for lightweight pass/fail gates that block downstream assets:

```python
@dg.asset_check(asset=ai_structured_extraction)
def k1_critical_validation_check(
    config: K1RunConfig, s3: S3Storage
) -> dg.AssetCheckResult:
    """Gate: block downstream if critical validation failures exist."""
    structured_data = s3.read_json(
        s3.staging_key(config.run_id, "structured_k1.json")
    )
    k1_data = K1ExtractedData(**structured_data["extracted_data"])
    report = validate_k1(k1_data)

    return dg.AssetCheckResult(
        passed=report.passed,
        metadata={
            "critical_failures": report.critical_count,
            "total_checks": len(report.checks),
        },
    )
```

---

## 6. Validation Result Schema for the Frontend

The React frontend receives validation results as part of the `pipeline_results.json`. The schema extends the existing format:

```json
{
  "pipeline_run": { "..." : "..." },
  "k1_data": { "..." : "..." },
  "financial_analysis": { "..." : "..." },
  "validation": {
    "overall_status": "passed | warnings | failed",
    "deterministic": {
      "passed": true,
      "critical_count": 0,
      "warning_count": 2,
      "advisory_count": 1,
      "checks": [
        {
          "rule_id": "ARITH-001",
          "rule_name": "Qualified dividends <= ordinary dividends",
          "severity": "critical",
          "passed": true,
          "message": "",
          "fields_involved": ["qualified_dividends", "ordinary_dividends"]
        },
        {
          "rule_id": "ARITH-006",
          "rule_name": "General partner SE earnings plausibility",
          "severity": "warning",
          "passed": false,
          "message": "General partner SE earnings (172450) differs from Box 1 + Box 4 (172450)",
          "fields_involved": ["self_employment_earnings", "ordinary_business_income", "guaranteed_payments"]
        }
      ]
    },
    "ai": {
      "overall_coherence_score": 0.92,
      "ocr_confidence_score": 0.88,
      "partnership_type_assessment": "Diversified investment partnership with real estate component",
      "partnership_type_consistency": 0.85,
      "anomaly_flags": [
        {
          "field_name": "rental_real_estate_income",
          "description": "Loss amount seems small relative to distributions",
          "confidence": 0.6,
          "suggested_correct_value": null
        }
      ],
      "value_reasonableness": {
        "ordinary_business_income": 0.95,
        "rental_real_estate_income": 0.80,
        "guaranteed_payments": 0.98
      },
      "narrative_assessment": "The K-1 data appears internally consistent...",
      "potential_ocr_errors": [],
      "recommended_review_fields": ["rental_real_estate_income"]
    }
  },
  "pii_stats": { "..." : "..." }
}
```

The frontend can use `overall_status` for a banner color (green/yellow/red), render failed checks as a table filtered by severity, and show the AI narrative as a collapsible assessment panel.

---

## 7. Implementation Priority Ordering

### Phase 1: Core Deterministic Validation (implement first)

These are the highest-value, lowest-risk checks. They catch definitive data errors with zero false positives:

1. **Required field checks** (FC-001, FC-003, FC-004) -- ensures extraction produced usable data
2. **Non-negative constraints** (ARITH-003 / FC-021) -- catches sign errors from OCR
3. **Qualified <= ordinary dividends** (ARITH-001, ARITH-009) -- hard IRS constraint
4. **Partner share percentage range** (ARITH-002 / FC-010) -- catches percentage format errors
5. **Section 179 statutory limit** (FC-031) -- catches magnitude errors

Deliverable: `K1ValidationReport` model, `validate_k1()` function, `k1_deterministic_validation` Dagster asset.

### Phase 2: Soft Deterministic Checks

Warning-level and advisory checks that flag suspicious but not definitively wrong data:

6. **Capital account reconciliation** (CAP-001) -- catches major OCR digit errors
7. **SE earnings vs partner type** (ARITH-006, ARITH-007, FC-032) -- catches partner type misclassification
8. **Magnitude sanity checks** (FC-040) -- catches decimal point displacement
9. **Capital account plausibility** (ARITH-005) -- catches beginning/ending swaps
10. **QBI plausibility** (ARITH-008) -- low-priority advisory
11. **Foreign taxes income context** (ARITH-010) -- low-priority warning
12. **Section 179 reasonableness** (ARITH-011) -- low-priority advisory

Deliverable: Additional check functions added to `validate_k1()`.

### Phase 3: AI-Powered Validation

13. **K1AIValidationResult model** and agent prompt design
14. **k1_ai_validation Dagster asset**
15. **K1CombinedValidation model** combining both tracks
16. **Frontend schema** for rendering validation results

Deliverable: AI validation asset, combined validation model, `pipeline_results.json` schema extension.

### Phase 4: Cross-Partner and Multi-Year Validation

17. **PartnershipValidationResult model** and `validate_partnership()` function
18. **Percentage sum checks** (CROSS_A1, CROSS_A2)
19. **Income proportionality** (CROSS_A3)
20. **Capital account continuity** (CROSS_B1)
21. **Duplicate detection** (CROSS_C1, C2, C3)
22. **Cross-partner SE consistency** (CROSS_D4)

Deliverable: Cross-partner validation asset (runs across accumulated K-1 data store), multi-year continuity checks.

---

## 8. Test Profile Coverage

The 10 test profiles exercise different validation paths as follows:

| Profile | Partnership Type | Key Validation Paths Exercised |
|---------|-----------------|-------------------------------|
| 1. Sunbelt Retail RE Fund II | Real estate LP, passive losses | ARITH-003 (non-negative distributions), ARITH-005 (capital account with rental losses), ARITH-007 (LP SE = 0), FC-040 (magnitude for RE income) |
| 2. Granite Peak Venture Partners | VC GP with carried interest, 20% profit / 1% capital | ARITH-006 (GP SE = Box 1 + Box 4), ARITH-008 (QBI with guaranteed payments), CROSS_A3 (disproportionate allocations from carry) |
| 3. Heartland Manufacturing | Operating business GP | ARITH-006 (GP SE), ARITH-008 (QBI), FC-032 (GP SE vs income), CAP-001 (capital reconciliation with Section 179) |
| 4. Pacific Rim International | Foreign activity LP | ARITH-010 (foreign taxes with income), ARITH-003 (non-negative foreign taxes), FC-040 (foreign tax magnitude) |
| 5. Mountain West Energy | Energy sector LP, large losses | ARITH-005 (capital account with large losses), ARITH-007 (LP SE check), FC-040 (magnitude for large capital gains/losses) |
| 6. Meridian Capital Growth | Diversified investment GP | ARITH-001 (qualified <= ordinary dividends), ARITH-006 (GP SE), ARITH-008 (QBI), CAP-001 (capital reconciliation) |
| 7. Northeast Healthcare | Healthcare LP with guaranteed payments | ARITH-007 (LP with guaranteed payments, SE should approximate GP amount), FC-032 (LP SE vs guaranteed) |
| 8. Sunbelt CRE Opportunity III | Real estate LP, different EIN from Profile 1 | CROSS_A5 (name similarity across different partnerships), CROSS_C1 (duplicate detection with Profile 1) |
| 9. Cascadia Clean Energy | LP with corporate partner, no SE | ARITH-007 (LP SE = 0), CAP-001 (capital account with tax credits), AI assessment of clean energy partnership type |
| 10. Southern Hospitality Restaurant | GP (S-Corp entity), no SE on K-1 | FC-032 (S-Corp GP has no SE -- entity type exception), CROSS_D4 (cross-partner SE consistency for entity GPs) |

### Cross-Profile Validations

- **Profiles 1 + 8**: Test duplicate detection -- both contain "Sunbelt" but have different EINs. The system must use EIN (not name) as the grouping key for cross-partner checks.
- **Profiles 2 + any LP from same fund**: Test CROSS_A3 income proportionality -- carried interest creates intentional disproportionate allocations that should be flagged as warnings, not errors.
- **Profiles 3 + 10**: Test CROSS_D4 SE consistency -- Profile 3 is an individual GP with SE earnings, Profile 10 is an S-Corp GP without SE. Both are valid but the system must handle entity type differences.
- **Any two profiles from the same partnership across years**: Test CROSS_B1 capital account continuity.

### Validation Path Matrix

| Rule Category | Profiles That Exercise It |
|---------------|--------------------------|
| Critical: Required fields | All 10 |
| Critical: Non-negative constraints | 1, 4, 6, 9 (dividends, distributions, foreign taxes) |
| Critical: Qualified <= ordinary dividends | 6 (has both), 4 (international dividends) |
| Warning: SE earnings vs partner type | 2, 3, 7, 10 (GP cases); 1, 5, 8, 9 (LP cases) |
| Warning: Capital reconciliation | 1, 3, 5, 6 (varying income/distribution patterns) |
| Advisory: Magnitude checks | 5 (large energy losses), 2 (large carried interest) |
| Advisory: QBI plausibility | 2, 3, 6 (operating businesses with QBI) |
| Cross-partner: Percentage sums | Any group from same partnership |
| Cross-partner: Duplicate detection | 1 vs 8 (similar names, different EINs) |
| Multi-year: Capital continuity | Any profile with prior-year data |

---

## Sources

This design synthesizes rules and guidelines from:

1. `docs/validation/01_arithmetic_rules.md` -- 14 arithmetic rules (ARITH-001 through ARITH-014)
2. `docs/validation/02_field_constraints.md` -- Field-level constraints (FC-001 through FC-051)
3. `docs/validation/03_capital_account_rules.md` -- Capital account rules (CAP-001 through CAP-011)
4. `docs/validation/04_cross_partner_validation.md` -- Cross-partner rules (CROSS_A1 through CROSS_D4)
5. `pipeline/src/k1_pipeline/defs/assets.py` -- Existing `K1ExtractedData`, `FinancialAnalysis` models, and `pydantic-ai` extraction pattern
