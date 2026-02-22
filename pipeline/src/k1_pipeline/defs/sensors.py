"""
S3-based sensor for automatic K-1 PDF processing.

Watches the dropoff/ prefix in S3 for new PDFs and triggers the
single-document processing pipeline when files appear. Supports
parallel processing via per-run staging isolation.
"""

from __future__ import annotations

import time

import dagster as dg

from k1_pipeline.defs.resources import S3Storage

# ---------------------------------------------------------------------------
# Job: select the processing pipeline, excluding irs_k1_form_fill
# ---------------------------------------------------------------------------

k1_dropoff_processing_job = dg.define_asset_job(
    name="k1_dropoff_processing_job",
    selection=dg.AssetSelection.assets(
        "raw_k1_pdf",
        "ocr_extracted_text",
        "pii_detection_report",
        "sanitized_text",
        "ai_structured_extraction",
        "ai_financial_analysis",
        "final_report",
    ),
    description="Process a single K-1 PDF dropped into the dropoff zone.",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ASSET_NAMES = [
    "raw_k1_pdf",
    "ocr_extracted_text",
    "pii_detection_report",
    "sanitized_text",
    "ai_structured_extraction",
    "ai_financial_analysis",
    "final_report",
]


def _make_run_id(pdf_key: str) -> str:
    """Generate a unique run ID from the S3 key's filename stem + current timestamp."""
    filename = pdf_key.rsplit("/", 1)[-1]
    stem = filename.rsplit(".", 1)[0]
    return f"{stem}_{int(time.time())}"


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------


@dg.sensor(
    job=k1_dropoff_processing_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Watches the S3 dropoff/ prefix for new K-1 PDF files. "
        "When new PDFs appear, triggers parallel processing runs."
    ),
)
def k1_dropoff_sensor(
    context: dg.SensorEvaluationContext, s3: S3Storage
) -> dg.SensorResult:
    pdf_keys = s3.list_objects("dropoff/", suffix=".pdf")

    # Filter out already-processed or failed files
    pdf_keys = [
        k
        for k in pdf_keys
        if not k.startswith("dropoff/processed/")
        and not k.startswith("dropoff/failed/")
    ]

    if not pdf_keys:
        return dg.SensorResult(skip_reason="No PDF files in dropoff prefix.")

    run_requests = []
    for pdf_key in pdf_keys:
        run_id = _make_run_id(pdf_key)
        filename = pdf_key.rsplit("/", 1)[-1]
        context.log.info(f"New PDF: {filename} -> run_id={run_id}")

        # Copy to input/{run_id}.pdf so assets can find it
        s3.copy_object(pdf_key, f"input/{run_id}.pdf")

        # Move original to dropoff/processed/
        s3.move_object(pdf_key, f"dropoff/processed/{filename}")

        # Build run config: pass run_id to every asset
        run_requests.append(
            dg.RunRequest(
                run_key=f"dropoff_{run_id}",
                run_config={
                    "ops": {
                        name: {"config": {"run_id": run_id}}
                        for name in ASSET_NAMES
                    },
                },
                tags={
                    "source": "dropoff_sensor",
                    "original_filename": filename,
                    "run_id": run_id,
                },
            ),
        )

    context.log.info(f"Dispatching {len(run_requests)} run(s)")
    return dg.SensorResult(run_requests=run_requests)


# ---------------------------------------------------------------------------
# Run failure sensor: move PDF to failed/ on pipeline failure
# ---------------------------------------------------------------------------


@dg.run_failure_sensor(
    monitored_jobs=[k1_dropoff_processing_job],
    description="Moves the original PDF from processed/ to failed/ when a dropoff run fails.",
)
def k1_dropoff_failure_sensor(
    context: dg.RunFailureSensorContext, s3: S3Storage
):
    filename = context.dagster_run.tags.get("original_filename")
    if not filename:
        return

    # Find the file in processed/ and move it to failed/
    source_key = f"dropoff/processed/{filename}"
    candidates = s3.list_objects("dropoff/processed/", suffix=".pdf")

    if source_key not in candidates:
        # Try to find a numbered variant
        stem = filename.rsplit(".", 1)[0]
        for candidate in candidates:
            candidate_filename = candidate.rsplit("/", 1)[-1]
            if candidate_filename.startswith(stem):
                source_key = candidate
                filename = candidate_filename
                break
        else:
            context.log.warning(
                f"Could not find {filename} in processed/ to move to failed/"
            )
            return

    s3.move_object(source_key, f"dropoff/failed/{filename}")
    context.log.info(f"Moved {filename} to failed/ due to run failure")
