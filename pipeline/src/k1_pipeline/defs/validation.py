"""
K-1 Domain-Driven Validation System

Two-track validation architecture:
  Track 1: Deterministic Pydantic validators (arithmetic, field constraints, capital account)
  Track 2: AI-powered judgment checks via Pydantic AI + DeepSeek

Cross-partner validation (Phase 4) is out of scope — requires cross-run data storage.
"""

import logging
import re
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import dagster as dg
from pydantic import BaseModel, Field, model_validator

from k1_pipeline.defs.assets import K1ExtractedData, K1RunConfig, _serialize_messages
from k1_pipeline.defs.resources import S3Storage


# ---------------------------------------------------------------------------
# Validation Infrastructure Models
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Deterministic Check Functions — Arithmetic Rules
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Deterministic Check Functions — Field Constraint Rules
# ---------------------------------------------------------------------------


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
    section_179_limits = {
        "2020": 1_040_000, "2021": 1_050_000, "2022": 1_080_000,
        "2023": 1_160_000, "2024": 1_220_000, "2025": 1_250_000,
    }
    if data.section_179_deduction is not None and data.section_179_deduction > 0:
        limit = section_179_limits.get(data.tax_year or "", 1_250_000)
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
    magnitude_thresholds = {
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
    for field_name, (lo, hi) in magnitude_thresholds.items():
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


# ---------------------------------------------------------------------------
# Deterministic Check Functions — Capital Account Rules
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Validation Runner
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# AI Validation Models
# ---------------------------------------------------------------------------


class AnomalyFlag(BaseModel):
    """A single anomaly detected by AI analysis."""

    field_name: str = Field(..., description="The field with the anomaly")
    description: str = Field(..., description="What is unusual about this value")
    confidence: float = Field(..., ge=0.0, le=1.0,
                              description="AI confidence that this is a real anomaly")
    suggested_correct_value: float | None = Field(
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


class K1CombinedValidation(BaseModel):
    """Combined result of deterministic + AI validation for a single K-1."""

    deterministic_report: K1ValidationReport
    ai_report: K1AIValidationResult | None = None
    overall_status: str = Field(
        "pending",
        description="Overall validation status: passed, warnings, failed"
    )

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


# ---------------------------------------------------------------------------
# AI Validation Function
# ---------------------------------------------------------------------------


def _load_validation_guidelines() -> str:
    """Load minified validation guideline docs for AI context, if available."""
    try:
        import importlib.util

        repo_root = Path(__file__).resolve().parents[4]
        docs_dir = repo_root / "docs" / "validation"
        script_path = repo_root / "scripts" / "minify_instructions.py"

        if not docs_dir.exists() or not script_path.exists():
            return ""

        spec = importlib.util.spec_from_file_location("minify_instructions", script_path)
        if spec is None or spec.loader is None:
            return ""
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        result = mod.minify_directory(
            docs_dir,
            exclude={"05_validation_design.md"},
            enable_aggressive=True,
        )
        return result if isinstance(result, str) else result.text
    except Exception as e:
        logging.warning("Failed to load validation guidelines: %s", e)
        return ""


def run_ai_validation(
    k1_data: K1ExtractedData,
    deterministic_report: K1ValidationReport,
    sanitized_text: str,
) -> tuple[K1AIValidationResult, dict]:
    """Run AI-powered validation on extracted K-1 data.

    Returns (result, ai_interaction) for auditability.
    """
    from pydantic_ai import Agent

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
A score of 1.0 means the value looks correct; 0.0 means it is almost certainly wrong.

Remember: your primary task is to assess overall data quality by scoring coherence,
estimating OCR confidence, classifying partnership type, flagging anomalies,
and providing per-field reasonableness scores for every non-null field."""

    k1_json = k1_data.model_dump_json(indent=2)
    det_summary = deterministic_report.model_dump_json(indent=2)

    guidelines = _load_validation_guidelines()
    guidelines_section = (
        f"\n\n## Validation Rule Reference\n{guidelines}" if guidelines else ""
    )

    user_prompt = f"""Review the following K-1 extracted data and provide a quality assessment.

## Extracted K-1 Data
{k1_json}

## Deterministic Validation Results
{det_summary}

## Original OCR Text (sanitized)
{sanitized_text}{guidelines_section}

## Task
Based on all the data above, provide your quality assessment. You must include:
a coherence score (0-1), OCR confidence estimate (0-1), partnership type classification,
anomaly flags for any suspicious values, and a reasonableness score (0-1) for every
non-null field in value_reasonableness."""

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=K1AIValidationResult,
        system_prompt=system_prompt,
    )

    result = agent.run_sync(user_prompt)
    ai_messages = _serialize_messages(result.all_messages())

    ai_interaction = {
        "model": "deepseek:deepseek-chat",
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "output_schema": K1AIValidationResult.model_json_schema(),
        "raw_messages": ai_messages,
        "usage": result.usage().model_dump() if hasattr(result.usage(), "model_dump") else str(result.usage()),
    }

    return result.output, ai_interaction


# ---------------------------------------------------------------------------
# Dagster Assets
# ---------------------------------------------------------------------------


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
    sanitized_text_content = sanitized_data["sanitized_text"]

    ai_result, ai_interaction = run_ai_validation(k1_data, det_report, sanitized_text_content)

    staging_key = s3.staging_key(config.run_id, "ai_validation.json")
    s3.write_json(staging_key, {
        "ai_validation": ai_result.model_dump(),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "ai_interaction": ai_interaction,
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
