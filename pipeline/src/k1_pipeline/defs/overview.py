"""
Processing overview: aggregates all processed K-1 reports into a
summary JSON and PDF.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg

from k1_pipeline.defs.pdf_templates import generate_pdf, render_overview_html
from k1_pipeline.defs.sensors import k1_dropoff_processing_job

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_OUTPUT = PROJECT_ROOT / "data" / "output"

# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


@dg.asset(group_name="output")
def processing_overview() -> dg.MaterializeResult:
    """Scan all output directories and produce an overview JSON + PDF."""

    reports = []
    for results_file in sorted(DATA_OUTPUT.glob("*/pipeline_results.json")):
        data = json.loads(results_file.read_text())
        reports.append({
            "directory": results_file.parent.name,
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
    overview_json_path = DATA_OUTPUT / "overview.json"
    overview_json_path.write_text(json.dumps(overview, indent=2))

    # PDF overview
    overview_pdf_path = DATA_OUTPUT / "overview.pdf"
    html = render_overview_html(reports, now)
    generate_pdf(html, overview_pdf_path)

    return dg.MaterializeResult(
        metadata={
            "total_reports": dg.MetadataValue.int(len(reports)),
            "overview_json": dg.MetadataValue.path(str(overview_json_path)),
            "overview_pdf": dg.MetadataValue.path(str(overview_pdf_path)),
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
