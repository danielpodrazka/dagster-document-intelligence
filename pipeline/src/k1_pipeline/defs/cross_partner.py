"""
Cross-partner validation assets, sensor, and job.

Provides:
  - k1_parquet_upsert: ingests each K-1 pipeline run into S3 Parquet
  - cross_partner_validation: runs cross-partner checks on accumulated data
  - k1_cross_partner_on_success: sensor that triggers validation after each run
  - cross_partner_validation_job: job wrapping the validation asset
"""

import base64
import fcntl
import hashlib
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg

from k1_pipeline.defs.assets import K1RunConfig
from k1_pipeline.defs.cross_partner_rules import run_cross_partner_checks
from k1_pipeline.defs.resources import S3Storage
from k1_pipeline.defs.sensors import k1_dropoff_processing_job


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PARQUET_S3_KEY = "output/k1_records.parquet"
_PARQUET_LOCK_PATH = Path(tempfile.gettempdir()) / "k1_parquet.lock"

_CREATE_TABLE_SQL = """
    CREATE TABLE k1_records (
        partnership_ein  VARCHAR NOT NULL,
        partner_tin      VARCHAR NOT NULL,
        tax_year         VARCHAR NOT NULL,
        run_id           VARCHAR NOT NULL,
        source_pdf_name  VARCHAR,
        pdf_sha256       VARCHAR,
        ingested_at      VARCHAR,
        partnership_name VARCHAR,
        partner_type     VARCHAR,
        partner_share_percentage DOUBLE,
        ordinary_business_income DOUBLE,
        rental_real_estate_income DOUBLE,
        guaranteed_payments      DOUBLE,
        interest_income          DOUBLE,
        ordinary_dividends       DOUBLE,
        qualified_dividends      DOUBLE,
        short_term_capital_gains  DOUBLE,
        long_term_capital_gains   DOUBLE,
        section_179_deduction    DOUBLE,
        distributions            DOUBLE,
        capital_account_beginning DOUBLE,
        capital_account_ending    DOUBLE,
        self_employment_earnings  DOUBLE,
        foreign_taxes_paid       DOUBLE,
        qbi_deduction            DOUBLE,
        deterministic_passed        BOOLEAN,
        deterministic_critical_count INTEGER,
        deterministic_warning_count  INTEGER,
        ai_coherence_score          DOUBLE,
        ai_ocr_confidence           DOUBLE,
        PRIMARY KEY (partnership_ein, partner_tin, tax_year)
    )
"""


# ---------------------------------------------------------------------------
# Parquet I/O helpers
# ---------------------------------------------------------------------------


def _load_parquet(s3: S3Storage):
    """Load existing parquet from S3 into an in-memory DuckDB connection.

    Returns an in-memory DuckDB connection with a k1_records table.
    Creates an empty table if no parquet exists yet.
    """
    import duckdb

    conn = duckdb.connect()
    try:
        parquet_bytes = s3.read_bytes(PARQUET_S3_KEY)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.write(parquet_bytes)
        tmp.close()
        conn.execute(f"CREATE TABLE k1_records AS SELECT * FROM read_parquet('{tmp.name}')")
        os.unlink(tmp.name)
    except Exception:
        conn.execute(_CREATE_TABLE_SQL)
    return conn


def _write_parquet(conn, s3: S3Storage) -> None:
    """Export k1_records table to parquet and upload to S3."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    conn.execute(f"COPY k1_records TO '{tmp.name}' (FORMAT PARQUET)")
    s3.upload_from_file(tmp.name, PARQUET_S3_KEY, content_type="application/octet-stream")
    os.unlink(tmp.name)


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
def k1_parquet_upsert(
    context: dg.AssetExecutionContext,
    config: K1RunConfig,
    s3: S3Storage,
) -> dg.MaterializeResult:
    """Ingest a completed K-1 pipeline run into S3 Parquet for cross-partner validation.

    Reads structured extraction, PII mapping, validation results, and raw PDF
    metadata from S3 staging, resolves PII identifiers, and upserts the record
    into the k1_records parquet file on S3.
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

    # Locked read-modify-write of the parquet file
    fd = open(_PARQUET_LOCK_PATH, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)

        conn = _load_parquet(s3)

        # Check for duplicate PDF hash (replaces processed_files table)
        result = conn.execute(
            "SELECT run_id, ingested_at FROM k1_records WHERE pdf_sha256 = ?",
            [pdf_sha256],
        )
        existing = result.fetchone()

        c3_message = ""
        if existing:
            c3_message = (
                f"Duplicate PDF detected: hash {pdf_sha256[:12]}... was previously "
                f"processed as run_id={existing[0]} on {existing[1] or 'unknown'}"
            )
            context.log.warning(c3_message)

        # Upsert: delete existing record for this primary key, then insert
        conn.execute(
            "DELETE FROM k1_records WHERE partnership_ein = ? AND partner_tin = ? AND tax_year = ?",
            [record["partnership_ein"], record["partner_tin"], record["tax_year"]],
        )

        columns = list(record.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)
        conn.execute(
            f"INSERT INTO k1_records ({col_names}) VALUES ({placeholders})",
            list(record.values()),
        )

        record_count = conn.execute("SELECT COUNT(*) FROM k1_records").fetchone()[0]

        _write_parquet(conn, s3)
        conn.close()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()

    # Mask EIN for metadata display
    masked_ein = f"{partnership_ein[:3]}***{partnership_ein[-3:]}" if partnership_ein else "N/A"

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
) -> dg.MaterializeResult:
    """Run cross-partner validation checks on accumulated K-1 data.

    Reads k1_records.parquet from S3, loads into in-memory DuckDB, and
    runs cross-partner checks. Results are written to
    output/cross_partner_results.json on S3 for the frontend.
    """
    conn = _load_parquet(s3)

    # Gather K-1 records by (ein, year)
    if config.partnership_ein and config.tax_year:
        result = conn.execute(
            "SELECT * FROM k1_records WHERE partnership_ein = ? AND tax_year = ?",
            [config.partnership_ein, config.tax_year],
        )
        columns = [desc[0] for desc in result.description]
        k1s = [dict(zip(columns, row)) for row in result.fetchall()]
        k1s_by_partnership = {(config.partnership_ein, config.tax_year): k1s}
    else:
        # All partnerships
        groups = conn.execute(
            "SELECT DISTINCT partnership_ein, tax_year FROM k1_records "
            "ORDER BY partnership_ein, tax_year"
        ).fetchall()
        k1s_by_partnership = {}
        for ein, year in groups:
            result = conn.execute(
                "SELECT * FROM k1_records WHERE partnership_ein = ? AND tax_year = ?",
                [ein, year],
            )
            columns = [desc[0] for desc in result.description]
            k1s_by_partnership[(ein, year)] = [
                dict(zip(columns, row)) for row in result.fetchall()
            ]

    # Get consecutive year pairs
    pairs_result = conn.execute("""
        SELECT
            a.partnership_ein, a.partner_tin, a.tax_year AS year_prior,
            b.tax_year AS year_current
        FROM k1_records a
        JOIN k1_records b
            ON a.partnership_ein = b.partnership_ein
            AND a.partner_tin = b.partner_tin
            AND CAST(a.tax_year AS INTEGER) + 1 = CAST(b.tax_year AS INTEGER)
        ORDER BY a.partnership_ein, a.partner_tin, a.tax_year
    """)

    year_pairs = []
    for ein, tin, year_prior, year_current in pairs_result.fetchall():
        prior_result = conn.execute(
            "SELECT * FROM k1_records "
            "WHERE partnership_ein = ? AND tax_year = ? AND partner_tin = ?",
            [ein, year_prior, tin],
        )
        columns = [desc[0] for desc in prior_result.description]
        prior_rows = prior_result.fetchall()

        current_result = conn.execute(
            "SELECT * FROM k1_records "
            "WHERE partnership_ein = ? AND tax_year = ? AND partner_tin = ?",
            [ein, year_current, tin],
        )
        current_rows = current_result.fetchall()

        if prior_rows and current_rows:
            year_pairs.append((
                dict(zip(columns, prior_rows[0])),
                dict(zip(columns, current_rows[0])),
            ))

    conn.close()

    # Run all checks
    results = run_cross_partner_checks(
        k1s_by_partnership=k1s_by_partnership,
        year_pairs=year_pairs,
    )

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
