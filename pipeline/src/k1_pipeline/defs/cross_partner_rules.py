"""
Cross-partner validation rule functions.

Pure validation logic (no Dagster dependencies), following the pattern
of check functions in validation.py. Each function takes K-1 record dicts
(from DuckDB) and returns a list of result dicts.

Result dict format:
    rule_id, category, severity, passed, message,
    partnership_ein, tax_year, partner_tin, validated_at, run_id_trigger
"""

from __future__ import annotations

from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _result(
    rule_id: str,
    category: str,
    severity: str,
    passed: bool,
    message: str,
    partnership_ein: str | None = None,
    tax_year: str | None = None,
    partner_tin: str | None = None,
    run_id_trigger: str | None = None,
) -> dict:
    """Build a standardized validation result dict."""
    return {
        "rule_id": rule_id,
        "category": category,
        "severity": severity,
        "passed": passed,
        "message": message,
        "partnership_ein": partnership_ein,
        "tax_year": tax_year,
        "partner_tin": partner_tin,
        "validated_at": _now_iso(),
        "run_id_trigger": run_id_trigger,
    }


# ---------------------------------------------------------------------------
# Phase 1 Rules
# ---------------------------------------------------------------------------


def check_a1_profit_pct_sum(k1s: list[dict]) -> list[dict]:
    """A1: Sum of partner_share_percentage must not exceed 100%."""
    if not k1s:
        return []

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")
    pcts = [r["partner_share_percentage"] for r in k1s if r.get("partner_share_percentage") is not None]

    if not pcts:
        return []

    total = sum(pcts)

    if total > 100.0:
        return [_result(
            rule_id="CROSS_A1_PROFIT_PCT_SUM",
            category="A",
            severity="critical",
            passed=False,
            message=(
                f"Partner profit percentages sum to {total:.2f}%, exceeding 100%. "
                f"Partners: {len(pcts)}, percentages: {[f'{p:.2f}%' for p in pcts]}"
            ),
            partnership_ein=ein,
            tax_year=year,
        )]

    return [_result(
        rule_id="CROSS_A1_PROFIT_PCT_SUM",
        category="A",
        severity="critical",
        passed=True,
        message=f"Partner percentages sum to {total:.2f}% ({len(pcts)} partners)",
        partnership_ein=ein,
        tax_year=year,
    )]


def check_a2_pct_sum_incomplete(k1s: list[dict]) -> list[dict]:
    """A2: Warn if partner percentages sum to less than 100%."""
    if not k1s:
        return []

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")
    pcts = [r["partner_share_percentage"] for r in k1s if r.get("partner_share_percentage") is not None]

    if not pcts:
        return []

    total = sum(pcts)

    if total < 100.0:
        missing = 100.0 - total
        return [_result(
            rule_id="CROSS_A2_PCT_SUM_INCOMPLETE",
            category="A",
            severity="advisory",
            passed=False,
            message=(
                f"Partner percentages sum to {total:.2f}% (< 100%). "
                f"{missing:.2f}% of partners may not yet be ingested."
            ),
            partnership_ein=ein,
            tax_year=year,
        )]

    return [_result(
        rule_id="CROSS_A2_PCT_SUM_INCOMPLETE",
        category="A",
        severity="advisory",
        passed=True,
        message=f"Partner percentages sum to {total:.2f}%",
        partnership_ein=ein,
        tax_year=year,
    )]


def check_c1_exact_duplicate(k1s: list[dict]) -> list[dict]:
    """C1: Detect exact duplicate K-1 records (same key + identical amounts)."""
    results = []
    if len(k1s) < 2:
        return results

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    amount_fields = [
        "ordinary_business_income", "rental_real_estate_income",
        "guaranteed_payments", "interest_income", "ordinary_dividends",
        "qualified_dividends", "short_term_capital_gains",
        "long_term_capital_gains", "section_179_deduction", "distributions",
        "capital_account_beginning", "capital_account_ending",
        "self_employment_earnings", "foreign_taxes_paid", "qbi_deduction",
    ]

    # Group by partner_tin
    by_tin: dict[str, list[dict]] = {}
    for k1 in k1s:
        tin = k1.get("partner_tin", "")
        by_tin.setdefault(tin, []).append(k1)

    for tin, records in by_tin.items():
        if len(records) < 2:
            continue

        # Check if all records for this TIN have identical amounts
        first = records[0]
        for other in records[1:]:
            all_match = all(
                first.get(f) == other.get(f) for f in amount_fields
            )
            if all_match:
                results.append(_result(
                    rule_id="CROSS_C1_EXACT_DUPLICATE",
                    category="C",
                    severity="critical",
                    passed=False,
                    message=(
                        f"Exact duplicate K-1 detected for partner TIN {tin[:4]}*** "
                        f"at EIN {ein}, year {year}. Run IDs: "
                        f"{first.get('run_id')} and {other.get('run_id')}"
                    ),
                    partnership_ein=ein,
                    tax_year=year,
                    partner_tin=tin,
                ))

    if not results:
        results.append(_result(
            rule_id="CROSS_C1_EXACT_DUPLICATE",
            category="C",
            severity="critical",
            passed=True,
            message="No exact duplicates detected",
            partnership_ein=ein,
            tax_year=year,
        ))

    return results


# ---------------------------------------------------------------------------
# Phase 2 Rules
# ---------------------------------------------------------------------------


def check_a3_income_proportionality(k1s: list[dict]) -> list[dict]:
    """A3: Each partner's income share should be proportional to their profit %."""
    results = []
    if len(k1s) < 2:
        return results

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    income_fields = [
        "ordinary_business_income", "rental_real_estate_income",
        "interest_income", "ordinary_dividends",
        "short_term_capital_gains", "long_term_capital_gains",
    ]

    # Get records with valid percentages
    valid_k1s = [k1 for k1 in k1s if k1.get("partner_share_percentage") is not None
                 and k1["partner_share_percentage"] > 0]
    if len(valid_k1s) < 2:
        return results

    total_pct = sum(k1["partner_share_percentage"] for k1 in valid_k1s)
    all_pass = True

    for field in income_fields:
        values = [(k1, k1.get(field)) for k1 in valid_k1s if k1.get(field) is not None]
        if len(values) < 2:
            continue

        total_field = sum(v for _, v in values)
        if total_field == 0:
            continue

        for k1, value in values:
            pct = k1["partner_share_percentage"]
            expected_share = pct / total_pct
            actual_share = value / total_field
            if expected_share > 0:
                deviation = abs(actual_share - expected_share) / expected_share
                if deviation > 0.10:
                    all_pass = False
                    results.append(_result(
                        rule_id="CROSS_A3_INCOME_PROPORTIONALITY",
                        category="A",
                        severity="warning",
                        passed=False,
                        message=(
                            f"Partner {k1.get('partner_tin', '?')[:4]}***'s {field} allocation "
                            f"deviates {deviation*100:.1f}% from expected pro-rata share "
                            f"(expected {expected_share*100:.1f}%, actual {actual_share*100:.1f}%)"
                        ),
                        partnership_ein=ein,
                        tax_year=year,
                        partner_tin=k1.get("partner_tin"),
                    ))

    if all_pass:
        results.append(_result(
            rule_id="CROSS_A3_INCOME_PROPORTIONALITY",
            category="A",
            severity="warning",
            passed=True,
            message="Income allocations are proportional to profit percentages",
            partnership_ein=ein,
            tax_year=year,
        ))

    return results


def check_a5_partnership_identity(k1s: list[dict]) -> list[dict]:
    """A5: All K-1s from same EIN should have consistent partnership names."""
    if len(k1s) < 2:
        return []

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    names = set()
    for k1 in k1s:
        name = k1.get("partnership_name")
        if name:
            names.add(name.strip())

    if len(names) <= 1:
        return [_result(
            rule_id="CROSS_A5_PARTNERSHIP_IDENTITY",
            category="A",
            severity="warning",
            passed=True,
            message="Partnership name is consistent across all K-1s",
            partnership_ein=ein,
            tax_year=year,
        )]

    # Simple fuzzy matching: check if names are similar enough
    name_list = list(names)
    mismatches = []
    for i in range(len(name_list)):
        for j in range(i + 1, len(name_list)):
            similarity = _simple_similarity(name_list[i], name_list[j])
            if similarity < 0.85:
                mismatches.append((name_list[i], name_list[j], similarity))

    if mismatches:
        pairs_str = "; ".join(
            f"'{a}' vs '{b}' ({s*100:.0f}% similar)"
            for a, b, s in mismatches
        )
        return [_result(
            rule_id="CROSS_A5_PARTNERSHIP_IDENTITY",
            category="A",
            severity="warning",
            passed=False,
            message=f"Inconsistent partnership names for EIN {ein}: {pairs_str}",
            partnership_ein=ein,
            tax_year=year,
        )]

    return [_result(
        rule_id="CROSS_A5_PARTNERSHIP_IDENTITY",
        category="A",
        severity="warning",
        passed=True,
        message=f"Partnership names are consistent (minor variations): {names}",
        partnership_ein=ein,
        tax_year=year,
    )]


def check_b1_capital_continuity(prior: dict, current: dict) -> list[dict]:
    """B1: Ending capital[Y] must equal beginning capital[Y+1]."""
    ein = prior.get("partnership_ein")
    tin = prior.get("partner_tin")
    year_prior = prior.get("tax_year")
    year_current = current.get("tax_year")

    ending = prior.get("capital_account_ending")
    beginning = current.get("capital_account_beginning")

    if ending is None or beginning is None:
        return [_result(
            rule_id="CROSS_B1_CAPITAL_CONTINUITY",
            category="B",
            severity="critical",
            passed=True,
            message=f"Capital account data unavailable for continuity check ({year_prior}->{year_current})",
            partnership_ein=ein,
            tax_year=year_current,
            partner_tin=tin,
        )]

    if abs(ending - beginning) < 0.01:
        return [_result(
            rule_id="CROSS_B1_CAPITAL_CONTINUITY",
            category="B",
            severity="critical",
            passed=True,
            message=(
                f"Capital continuity verified: {year_prior} ending "
                f"(${ending:,.2f}) = {year_current} beginning (${beginning:,.2f})"
            ),
            partnership_ein=ein,
            tax_year=year_current,
            partner_tin=tin,
        )]

    difference = abs(ending - beginning)
    magnitude = max(abs(ending), abs(beginning), 1.0)
    pct_diff = (difference / magnitude) * 100

    return [_result(
        rule_id="CROSS_B1_CAPITAL_CONTINUITY",
        category="B",
        severity="critical",
        passed=False,
        message=(
            f"Capital account discontinuity: {year_prior} ending (${ending:,.2f}) != "
            f"{year_current} beginning (${beginning:,.2f}). "
            f"Difference: ${difference:,.2f} ({pct_diff:.1f}%)"
        ),
        partnership_ein=ein,
        tax_year=year_current,
        partner_tin=tin,
    )]


def check_b3_partnership_name_continuity(prior: dict, current: dict) -> list[dict]:
    """B3: Partnership name should be consistent across years."""
    ein = prior.get("partnership_ein")
    year_current = current.get("tax_year")

    name_prior = (prior.get("partnership_name") or "").strip()
    name_current = (current.get("partnership_name") or "").strip()

    if not name_prior or not name_current:
        return []

    if name_prior == name_current:
        return [_result(
            rule_id="CROSS_B3_PARTNERSHIP_IDENTITY_MULTIYEAR",
            category="B",
            severity="warning",
            passed=True,
            message=f"Partnership name consistent across years: '{name_current}'",
            partnership_ein=ein,
            tax_year=year_current,
        )]

    similarity = _simple_similarity(name_prior, name_current)
    if similarity >= 0.85:
        return [_result(
            rule_id="CROSS_B3_PARTNERSHIP_IDENTITY_MULTIYEAR",
            category="B",
            severity="warning",
            passed=True,
            message=f"Partnership name minor variation: '{name_prior}' -> '{name_current}' ({similarity*100:.0f}% similar)",
            partnership_ein=ein,
            tax_year=year_current,
        )]

    return [_result(
        rule_id="CROSS_B3_PARTNERSHIP_IDENTITY_MULTIYEAR",
        category="B",
        severity="warning",
        passed=False,
        message=(
            f"Partnership name changed across years for EIN {ein}: "
            f"'{name_prior}' ({prior.get('tax_year')}) -> "
            f"'{name_current}' ({year_current})"
        ),
        partnership_ein=ein,
        tax_year=year_current,
    )]


def check_b4_partner_type_continuity(prior: dict, current: dict) -> list[dict]:
    """B4: Partner type should remain consistent across years."""
    ein = prior.get("partnership_ein")
    tin = prior.get("partner_tin")
    year_prior = prior.get("tax_year")
    year_current = current.get("tax_year")

    type_prior = (prior.get("partner_type") or "").strip().lower()
    type_current = (current.get("partner_type") or "").strip().lower()

    if not type_prior or not type_current:
        return []

    # Normalize types for comparison
    def _is_gp(t: str) -> bool:
        return "general" in t or "member-manager" in t

    def _is_lp(t: str) -> bool:
        return "limited" in t or "other llc" in t

    prior_gp = _is_gp(type_prior)
    current_gp = _is_gp(type_current)
    prior_lp = _is_lp(type_prior)
    current_lp = _is_lp(type_current)

    if (prior_gp and current_lp) or (prior_lp and current_gp):
        prior_label = "GP" if prior_gp else "LP"
        current_label = "GP" if current_gp else "LP"
        return [_result(
            rule_id="CROSS_B4_PARTNER_TYPE_CONTINUITY",
            category="B",
            severity="warning",
            passed=False,
            message=(
                f"Partner type changed from {prior_label} ({year_prior}) to "
                f"{current_label} ({year_current}) for partner {tin[:4] if tin else '?'}*** "
                f"at EIN {ein}"
            ),
            partnership_ein=ein,
            tax_year=year_current,
            partner_tin=tin,
        )]

    return [_result(
        rule_id="CROSS_B4_PARTNER_TYPE_CONTINUITY",
        category="B",
        severity="warning",
        passed=True,
        message=f"Partner type consistent across years ({year_prior}->{year_current})",
        partnership_ein=ein,
        tax_year=year_current,
        partner_tin=tin,
    )]


# ---------------------------------------------------------------------------
# Phase 3 Rules
# ---------------------------------------------------------------------------


def check_a4_capital_account_consistency(k1s: list[dict]) -> list[dict]:
    """A4: Capital accounts should be proportional to capital percentage."""
    results = []
    if len(k1s) < 2:
        return results

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    valid_k1s = [
        k1 for k1 in k1s
        if k1.get("partner_share_percentage") is not None
        and k1.get("capital_account_ending") is not None
        and k1["partner_share_percentage"] > 0
    ]
    if len(valid_k1s) < 2:
        return results

    total_pct = sum(float(k1["partner_share_percentage"]) for k1 in valid_k1s)
    total_ending = sum(float(k1["capital_account_ending"]) for k1 in valid_k1s)

    if total_ending == 0 or total_pct == 0:
        return results

    all_pass = True
    for k1 in valid_k1s:
        expected_share = k1["partner_share_percentage"] / total_pct
        actual_share = k1["capital_account_ending"] / total_ending
        deviation = abs(actual_share - expected_share)
        if deviation > 0.15:
            all_pass = False
            results.append(_result(
                rule_id="CROSS_A4_CAPITAL_ACCOUNT_CONSISTENCY",
                category="A",
                severity="warning",
                passed=False,
                message=(
                    f"Partner {k1.get('partner_tin', '?')[:4]}***'s capital account share "
                    f"({actual_share*100:.1f}%) deviates from capital percentage "
                    f"({expected_share*100:.1f}%)"
                ),
                partnership_ein=ein,
                tax_year=year,
                partner_tin=k1.get("partner_tin"),
            ))

    if all_pass:
        results.append(_result(
            rule_id="CROSS_A4_CAPITAL_ACCOUNT_CONSISTENCY",
            category="A",
            severity="warning",
            passed=True,
            message="Capital accounts are proportional to capital percentages",
            partnership_ein=ein,
            tax_year=year,
        ))

    return results


def check_c2_amended_k1(k1s: list[dict]) -> list[dict]:
    """C2: Detect possible amended K-1s (same key, different amounts)."""
    results = []
    if len(k1s) < 2:
        return results

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    amount_fields = [
        "ordinary_business_income", "rental_real_estate_income",
        "guaranteed_payments", "interest_income", "ordinary_dividends",
        "qualified_dividends", "short_term_capital_gains",
        "long_term_capital_gains", "section_179_deduction", "distributions",
        "capital_account_beginning", "capital_account_ending",
        "self_employment_earnings", "foreign_taxes_paid", "qbi_deduction",
    ]

    by_tin: dict[str, list[dict]] = {}
    for k1 in k1s:
        tin = k1.get("partner_tin", "")
        by_tin.setdefault(tin, []).append(k1)

    for tin, records in by_tin.items():
        if len(records) < 2:
            continue

        first = records[0]
        for other in records[1:]:
            differing = [
                f for f in amount_fields
                if first.get(f) != other.get(f)
            ]
            if differing:
                results.append(_result(
                    rule_id="CROSS_C2_AMENDED_K1",
                    category="C",
                    severity="warning",
                    passed=False,
                    message=(
                        f"Possible amended K-1 for partner {tin[:4]}*** at EIN {ein}, "
                        f"year {year}. Differing fields: {differing}"
                    ),
                    partnership_ein=ein,
                    tax_year=year,
                    partner_tin=tin,
                ))

    if not results:
        results.append(_result(
            rule_id="CROSS_C2_AMENDED_K1",
            category="C",
            severity="warning",
            passed=True,
            message="No amended K-1s detected",
            partnership_ein=ein,
            tax_year=year,
        ))

    return results


def check_d1_distribution_reasonableness(k1s: list[dict]) -> list[dict]:
    """D1: Total distributions should not vastly exceed total income."""
    if len(k1s) < 2:
        return []

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    income_fields = [
        "ordinary_business_income", "rental_real_estate_income",
        "interest_income", "ordinary_dividends",
        "short_term_capital_gains", "long_term_capital_gains",
    ]

    total_distributions = sum(
        k1.get("distributions") or 0 for k1 in k1s
    )
    total_income = sum(
        sum(k1.get(f) or 0 for f in income_fields)
        for k1 in k1s
    )

    if total_income > 0 and total_distributions > total_income * 3.0:
        return [_result(
            rule_id="CROSS_D1_DISTRIBUTION_REASONABLENESS",
            category="D",
            severity="warning",
            passed=False,
            message=(
                f"Total distributions (${total_distributions:,.2f}) exceed 3x total income "
                f"(${total_income:,.2f}) for partnership {ein}. "
                f"May indicate liquidation or data error."
            ),
            partnership_ein=ein,
            tax_year=year,
        )]

    return [_result(
        rule_id="CROSS_D1_DISTRIBUTION_REASONABLENESS",
        category="D",
        severity="warning",
        passed=True,
        message=(
            f"Distribution ratio is reasonable: ${total_distributions:,.2f} distributions "
            f"vs ${total_income:,.2f} income"
        ),
        partnership_ein=ein,
        tax_year=year,
    )]


def check_d4_se_consistency(k1s: list[dict]) -> list[dict]:
    """D4: GP → SE earnings, LP → no SE (unless guaranteed payments)."""
    results = []
    if len(k1s) < 2:
        return results

    ein = k1s[0].get("partnership_ein")
    year = k1s[0].get("tax_year")

    all_pass = True

    for k1 in k1s:
        partner_type = (k1.get("partner_type") or "").lower()
        entity_type = (k1.get("entity_type") or "").lower() if k1.get("entity_type") else ""
        se = k1.get("self_employment_earnings")
        gp = k1.get("guaranteed_payments")
        income = k1.get("ordinary_business_income")
        tin = k1.get("partner_tin")

        is_gp = "general" in partner_type or "member-manager" in partner_type
        is_lp = "limited" in partner_type or "other llc" in partner_type
        is_entity = any(t in entity_type for t in ["corporation", "s corp", "llc", "trust"])

        # GP individuals with income should have SE earnings
        if is_gp and not is_entity and income is not None and income > 0 and se is None:
            all_pass = False
            results.append(_result(
                rule_id="CROSS_D4_SE_CONSISTENCY",
                category="D",
                severity="warning",
                passed=False,
                message=(
                    f"General partner {tin[:4] if tin else '?'}*** has ordinary income "
                    f"(${income:,.2f}) but no SE earnings reported"
                ),
                partnership_ein=ein,
                tax_year=year,
                partner_tin=tin,
            ))

        # LP with SE earnings but no guaranteed payments
        if is_lp and se is not None and se > 0:
            if gp is None or gp == 0:
                all_pass = False
                results.append(_result(
                    rule_id="CROSS_D4_SE_CONSISTENCY",
                    category="D",
                    severity="warning",
                    passed=False,
                    message=(
                        f"Limited partner {tin[:4] if tin else '?'}*** has SE earnings "
                        f"(${se:,.2f}) but no guaranteed payments"
                    ),
                    partnership_ein=ein,
                    tax_year=year,
                    partner_tin=tin,
                ))

    if all_pass:
        results.append(_result(
            rule_id="CROSS_D4_SE_CONSISTENCY",
            category="D",
            severity="warning",
            passed=True,
            message="Self-employment earnings are consistent with partner types",
            partnership_ein=ein,
            tax_year=year,
        ))

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_similarity(a: str, b: str) -> float:
    """Simple character-level Jaccard similarity for fuzzy name matching."""
    if not a or not b:
        return 0.0
    a_lower = a.lower()
    b_lower = b.lower()
    if a_lower == b_lower:
        return 1.0

    # Use bigram Jaccard similarity
    def bigrams(s: str) -> set[str]:
        return {s[i:i+2] for i in range(len(s) - 1)} if len(s) >= 2 else {s}

    a_bi = bigrams(a_lower)
    b_bi = bigrams(b_lower)
    intersection = len(a_bi & b_bi)
    union = len(a_bi | b_bi)
    return intersection / union if union > 0 else 0.0


# ---------------------------------------------------------------------------
# Runner: execute all rules for a partnership scope
# ---------------------------------------------------------------------------


def run_cross_partner_checks(
    k1s_by_partnership: dict[tuple[str, str], list[dict]],
    year_pairs: list[tuple[dict, dict]],
    run_id_trigger: str | None = None,
) -> list[dict]:
    """Run all cross-partner validation checks.

    Args:
        k1s_by_partnership: Dict mapping (ein, year) to list of K-1 records
        year_pairs: List of (prior, current) record pairs for consecutive years
        run_id_trigger: The run_id that triggered this validation
    """
    all_results: list[dict] = []

    # Category A: Cross-partner checks (same partnership, same year)
    for (ein, year), k1s in k1s_by_partnership.items():
        if len(k1s) >= 2:
            all_results.extend(check_a1_profit_pct_sum(k1s))
            all_results.extend(check_a2_pct_sum_incomplete(k1s))
            all_results.extend(check_a3_income_proportionality(k1s))
            all_results.extend(check_a5_partnership_identity(k1s))
            all_results.extend(check_a4_capital_account_consistency(k1s))
            all_results.extend(check_c1_exact_duplicate(k1s))
            all_results.extend(check_c2_amended_k1(k1s))
            all_results.extend(check_d1_distribution_reasonableness(k1s))
            all_results.extend(check_d4_se_consistency(k1s))
        elif len(k1s) == 1:
            # Single K-1: only run percentage sum checks (will show advisory)
            all_results.extend(check_a2_pct_sum_incomplete(k1s))

    # Category B: Multi-year continuity checks
    for prior, current in year_pairs:
        all_results.extend(check_b1_capital_continuity(prior, current))
        all_results.extend(check_b3_partnership_name_continuity(prior, current))
        all_results.extend(check_b4_partner_type_continuity(prior, current))

    # Tag all results with the trigger run_id
    if run_id_trigger:
        for r in all_results:
            if not r.get("run_id_trigger"):
                r["run_id_trigger"] = run_id_trigger

    return all_results
