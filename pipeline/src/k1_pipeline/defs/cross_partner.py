"""
Cross-partner validation assets, sensor, and job.

Provides:
  - k1_duckdb_ingest: ingests each K-1 pipeline run into DuckDB
  - cross_partner_validation: runs cross-partner checks on accumulated data
  - k1_cross_partner_on_success: sensor that triggers validation after each run
  - cross_partner_validation_job: job wrapping the validation asset
"""

import base64
import hashlib
import re
from datetime import datetime, timezone

import dagster as dg

from k1_pipeline.defs.assets import K1RunConfig
from k1_pipeline.defs.cross_partner_rules import run_cross_partner_checks
from k1_pipeline.defs.duckdb_store import DuckDBStore
from k1_pipeline.defs.resources import S3Storage
from k1_pipeline.defs.sensors import k1_dropoff_processing_job


# ---------------------------------------------------------------------------
# PII resolution helper
# ---------------------------------------------------------------------------


def resolve_pii_identifiers(
    placeholder_mapping: dict[str, str],
    pii_entities: list[dict] | None = None,
) -> tuple[str | None, str | None]:
    """Extract real EIN/SSN from the sanitized_text.json placeholder mapping.

    On the K-1 form:
      - Part I (partnership) appears first → <EIN_1> is the partnership EIN
      - Part II (partner) appears second → <US_SSN_1> is the partner SSN;
        if absent, <EIN_2> is an entity partner EIN

    Returns (partnership_ein, partner_tin) or (None, None) if unresolvable.
    """
    if not placeholder_mapping:
        return None, None

    partnership_ein = None
    partner_tin = None

    # Extract EINs and SSNs from the mapping
    ein_pattern = re.compile(r"^\d{2}-\d{7}$")
    ssn_pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")

    eins = []
    ssns = []

    for placeholder, original in placeholder_mapping.items():
        original = original.strip()
        if ein_pattern.match(original):
            eins.append((placeholder, original))
        elif ssn_pattern.match(original):
            ssns.append((placeholder, original))

    # Sort by placeholder number to get ordering
    def _placeholder_sort_key(item: tuple[str, str]) -> int:
        match = re.search(r"_(\d+)>", item[0])
        return int(match.group(1)) if match else 0

    eins.sort(key=_placeholder_sort_key)
    ssns.sort(key=_placeholder_sort_key)

    # Partnership EIN = first EIN (Part I on the form)
    if eins:
        partnership_ein = eins[0][1]

    # Partner TIN = first SSN, or second EIN if no SSN
    if ssns:
        partner_tin = ssns[0][1]
    elif len(eins) >= 2:
        partner_tin = eins[1][1]

    return partnership_ein, partner_tin


# ---------------------------------------------------------------------------
# Ingest asset
# ---------------------------------------------------------------------------


class CrossPartnerConfig(dg.Config):
    """Config for cross-partner validation scope."""
    partnership_ein: str = ""
    tax_year: str = ""


@dg.asset(
    group_name="cross_partner_validation",
    deps=["final_report"],
)
def k1_duckdb_ingest(
    context: dg.AssetExecutionContext,
    config: K1RunConfig,
    s3: S3Storage,
    duckdb_store: DuckDBStore,
) -> dg.MaterializeResult:
    """Ingest a completed K-1 pipeline run into DuckDB for cross-partner validation.

    Reads structured extraction, PII mapping, validation results, and raw PDF
    metadata from S3 staging, resolves PII identifiers, and upserts the record
    into the k1_records table.
    """
    run_id = config.run_id

    # Load staging data
    structured_data = s3.read_json(s3.staging_key(run_id, "structured_k1.json"))
    sanitized_data = s3.read_json(s3.staging_key(run_id, "sanitized_text.json"))
    raw_data = s3.read_json(s3.staging_key(run_id, "raw_pdf_bytes.json"))
    det_data = s3.read_json(s3.staging_key(run_id, "deterministic_validation.json"))

    # Try to load AI validation (may not exist if it failed)
    try:
        ai_data = s3.read_json(s3.staging_key(run_id, "ai_validation.json"))
    except Exception:
        ai_data = None

    k1_data = structured_data["extracted_data"]
    placeholder_mapping = sanitized_data.get("placeholder_mapping", {})
    det_report = det_data.get("report", {})

    # Resolve PII identifiers
    partnership_ein, partner_tin = resolve_pii_identifiers(placeholder_mapping)

    if not partnership_ein or not partner_tin:
        context.log.warning(
            f"Could not resolve PII identifiers for run {run_id}. "
            f"EIN={partnership_ein}, TIN={partner_tin}. Skipping ingest."
        )
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("skipped"),
                "reason": dg.MetadataValue.text("Could not resolve EIN/TIN from PII mapping"),
            }
        )

    # Resolve partnership_name placeholder
    partnership_name = k1_data.get("partnership_name", "")
    if partnership_name and placeholder_mapping:
        for placeholder, original in placeholder_mapping.items():
            partnership_name = partnership_name.replace(placeholder, original)

    # Compute PDF SHA-256 hash
    pdf_bytes = base64.b64decode(raw_data.get("base64_data", ""))
    pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()

    # Check for duplicate PDF (C3)
    existing_file = duckdb_store.check_and_register_file(
        pdf_sha256=pdf_sha256,
        run_id=run_id,
        file_name=raw_data.get("file_name", ""),
        file_size=raw_data.get("file_size_bytes", 0),
    )

    c3_message = ""
    if existing_file:
        c3_message = (
            f"Duplicate PDF detected: hash {pdf_sha256[:12]}... was previously "
            f"processed as run_id={existing_file['run_id']} "
            f"on {existing_file.get('ingested_at', 'unknown')}"
        )
        context.log.warning(c3_message)

    # Build K-1 record
    record = {
        "partnership_ein": partnership_ein,
        "partner_tin": partner_tin,
        "tax_year": k1_data.get("tax_year", ""),
        "run_id": run_id,
        "source_pdf_name": raw_data.get("file_name", ""),
        "pdf_sha256": pdf_sha256,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "partnership_name": partnership_name,
        "partner_type": k1_data.get("partner_type"),
        "partner_share_percentage": k1_data.get("partner_share_percentage"),
        "ordinary_business_income": k1_data.get("ordinary_business_income"),
        "rental_real_estate_income": k1_data.get("rental_real_estate_income"),
        "guaranteed_payments": k1_data.get("guaranteed_payments"),
        "interest_income": k1_data.get("interest_income"),
        "ordinary_dividends": k1_data.get("ordinary_dividends"),
        "qualified_dividends": k1_data.get("qualified_dividends"),
        "short_term_capital_gains": k1_data.get("short_term_capital_gains"),
        "long_term_capital_gains": k1_data.get("long_term_capital_gains"),
        "section_179_deduction": k1_data.get("section_179_deduction"),
        "distributions": k1_data.get("distributions"),
        "capital_account_beginning": k1_data.get("capital_account_beginning"),
        "capital_account_ending": k1_data.get("capital_account_ending"),
        "self_employment_earnings": k1_data.get("self_employment_earnings"),
        "foreign_taxes_paid": k1_data.get("foreign_taxes_paid"),
        "qbi_deduction": k1_data.get("qbi_deduction"),
        "deterministic_passed": det_report.get("passed"),
        "deterministic_critical_count": det_report.get("critical_count", 0),
        "deterministic_warning_count": det_report.get("warning_count", 0),
        "ai_coherence_score": (
            ai_data["ai_validation"]["overall_coherence_score"]
            if ai_data and ai_data.get("ai_validation")
            else None
        ),
        "ai_ocr_confidence": (
            ai_data["ai_validation"]["ocr_confidence_score"]
            if ai_data and ai_data.get("ai_validation")
            else None
        ),
    }

    duckdb_store.upsert_k1_record(record)

    # Mask EIN for metadata display
    masked_ein = f"{partnership_ein[:3]}***{partnership_ein[-3:]}" if partnership_ein else "N/A"
    record_count = duckdb_store.get_record_count()

    return dg.MaterializeResult(
        metadata={
            "partnership_ein": dg.MetadataValue.text(masked_ein),
            "tax_year": dg.MetadataValue.text(k1_data.get("tax_year", "")),
            "total_records": dg.MetadataValue.int(record_count),
            "duplicate_pdf": dg.MetadataValue.text(c3_message or "No duplicate"),
        }
    )


# ---------------------------------------------------------------------------
# Cross-partner validation asset
# ---------------------------------------------------------------------------


@dg.asset(group_name="cross_partner_validation")
def cross_partner_validation(
    context: dg.AssetExecutionContext,
    config: CrossPartnerConfig,
    s3: S3Storage,
    duckdb_store: DuckDBStore,
) -> dg.MaterializeResult:
    """Run cross-partner validation checks on accumulated K-1 data.

    Can be scoped to a specific partnership/year or validate all data.
    Stores results in the cross_partner_validations table and writes
    output/cross_partner_results.json to S3 for the frontend.
    """
    # Gather K-1 records by (ein, year)
    if config.partnership_ein and config.tax_year:
        # Scoped to one partnership/year
        k1s = duckdb_store.get_partnership_k1s(config.partnership_ein, config.tax_year)
        k1s_by_partnership = {(config.partnership_ein, config.tax_year): k1s}
        # Clear previous validations for this scope
        duckdb_store.clear_validations_for_scope(config.partnership_ein, config.tax_year)
    else:
        # Validate all partnerships
        all_partnerships = duckdb_store.get_all_partnerships()
        k1s_by_partnership = {}
        for p in all_partnerships:
            ein = p["partnership_ein"]
            year = p["tax_year"]
            k1s_by_partnership[(ein, year)] = duckdb_store.get_partnership_k1s(ein, year)
        duckdb_store.clear_validations_for_scope()

    # Get consecutive year pairs
    year_pairs = duckdb_store.get_consecutive_year_pairs()

    # Run all checks
    results = run_cross_partner_checks(
        k1s_by_partnership=k1s_by_partnership,
        year_pairs=year_pairs,
    )

    # Store results in DuckDB
    duckdb_store.insert_validation_results(results)

    # Compute summary
    total_checks = len(results)
    passed_count = sum(1 for r in results if r["passed"])
    failed_count = total_checks - passed_count
    critical_count = sum(1 for r in results if not r["passed"] and r["severity"] == "critical")
    warning_count = sum(1 for r in results if not r["passed"] and r["severity"] == "warning")
    advisory_count = sum(1 for r in results if not r["passed"] and r["severity"] == "advisory")

    # Build output for S3 / frontend
    now = datetime.now(timezone.utc).isoformat()

    # Serialize validation results (convert datetime objects to strings)
    serialized_results = []
    for r in results:
        sr = dict(r)
        for k, v in sr.items():
            if isinstance(v, datetime):
                sr[k] = v.isoformat()
        serialized_results.append(sr)

    output = {
        "generated_at": now,
        "summary": {
            "total_checks": total_checks,
            "passed": passed_count,
            "failed": failed_count,
            "critical": critical_count,
            "warnings": warning_count,
            "advisory": advisory_count,
        },
        "partnerships_validated": [
            {
                "partnership_ein": ein,
                "tax_year": year,
                "partner_count": len(k1s),
                "partnership_name": k1s[0].get("partnership_name") if k1s else None,
            }
            for (ein, year), k1s in k1s_by_partnership.items()
        ],
        "year_pairs_checked": len(year_pairs),
        "results": serialized_results,
    }

    s3.write_json("output/cross_partner_results.json", output)

    context.log.info(
        f"Cross-partner validation complete: {total_checks} checks, "
        f"{passed_count} passed, {failed_count} failed "
        f"({critical_count} critical, {warning_count} warning, {advisory_count} advisory)"
    )

    return dg.MaterializeResult(
        metadata={
            "total_checks": dg.MetadataValue.int(total_checks),
            "passed": dg.MetadataValue.int(passed_count),
            "failed": dg.MetadataValue.int(failed_count),
            "critical": dg.MetadataValue.int(critical_count),
            "warnings": dg.MetadataValue.int(warning_count),
            "advisory": dg.MetadataValue.int(advisory_count),
            "partnerships_validated": dg.MetadataValue.int(len(k1s_by_partnership)),
            "year_pairs_checked": dg.MetadataValue.int(len(year_pairs)),
        }
    )


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

cross_partner_validation_job = dg.define_asset_job(
    name="cross_partner_validation_job",
    selection=dg.AssetSelection.assets(cross_partner_validation),
    description="Run cross-partner validation checks on accumulated K-1 data.",
)


# ---------------------------------------------------------------------------
# Sensor: trigger cross-partner validation after successful pipeline run
# ---------------------------------------------------------------------------


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[k1_dropoff_processing_job],
    request_job=cross_partner_validation_job,
    description=(
        "Triggers cross-partner validation after each successful K-1 pipeline run. "
        "Passes the partnership EIN and tax year from the completed run."
    ),
)
def k1_cross_partner_on_success(context: dg.RunStatusSensorContext):
    return dg.RunRequest()
