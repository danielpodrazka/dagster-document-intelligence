"""
Processing overview: aggregates all processed K-1 reports into a
summary JSON and PDF.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg

from k1_pipeline.defs.pdf_templates import generate_pdf, render_overview_html
from k1_pipeline.defs.resources import S3Storage
from k1_pipeline.defs.sensors import k1_dropoff_processing_job

# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


@dg.asset(group_name="output")
def processing_overview(s3: S3Storage) -> dg.MaterializeResult:
    """Scan all output directories and produce an overview JSON + PDF."""

    reports = []
    for results_key in s3.list_objects("output/", suffix="pipeline_results.json"):
        data = s3.read_json(results_key)
        # Extract directory name from key: output/<dirname>/pipeline_results.json
        parts = results_key.split("/")
        dirname = parts[1] if len(parts) >= 3 else results_key
        reports.append({
            "directory": dirname,
            "processed_at": data.get("processing_metadata", {}).get("report_generated_at", ""),
            "k1_data": data.get("k1_data", {}),
            "financial_analysis": data.get("financial_analysis", {}),
            "pii_stats": data.get("pii_stats", {}),
        })

    now = datetime.now(timezone.utc).isoformat()

    # JSON overview
    overview = {
        "generated_at": now,
        "total_reports": len(reports),
        "reports": reports,
    }
    s3.write_json("output/overview.json", overview)

    # PDF overview
    html = render_overview_html(reports, now)
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        generate_pdf(html, Path(tmp_pdf.name))
        s3.upload_from_file(tmp_pdf.name, "output/overview.pdf", content_type="application/pdf")

    return dg.MaterializeResult(
        metadata={
            "total_reports": dg.MetadataValue.int(len(reports)),
            "overview_json": dg.MetadataValue.text("output/overview.json"),
            "overview_pdf": dg.MetadataValue.text("output/overview.pdf"),
        }
    )


# ---------------------------------------------------------------------------
# Job + success sensor to auto-regenerate overview after each run
# ---------------------------------------------------------------------------

overview_job = dg.define_asset_job(
    name="overview_job",
    selection=dg.AssetSelection.assets(processing_overview),
    description="Regenerate the processing overview after a successful dropoff run.",
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[k1_dropoff_processing_job],
    request_job=overview_job,
    description="Regenerates the processing overview after each successful dropoff run.",
)
def k1_overview_on_success(context: dg.RunStatusSensorContext):
    return dg.RunRequest()
