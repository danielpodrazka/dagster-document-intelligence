"""
File-watching sensor for automatic K-1 PDF processing.

Watches data/dropoff/ for new PDFs and triggers the single-document
processing pipeline when files appear. Supports parallel processing
via per-run staging isolation.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import dagster as dg

# ---------------------------------------------------------------------------
# Path constants (same parents[3] logic as assets.py)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_INPUT = PROJECT_ROOT / "data" / "input"
DATA_DROPOFF = PROJECT_ROOT / "data" / "dropoff"
DATA_DROPOFF_PROCESSED = DATA_DROPOFF / "processed"
DATA_DROPOFF_FAILED = DATA_DROPOFF / "failed"

DATA_DROPOFF.mkdir(parents=True, exist_ok=True)
DATA_DROPOFF_PROCESSED.mkdir(parents=True, exist_ok=True)
DATA_DROPOFF_FAILED.mkdir(parents=True, exist_ok=True)

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


def _make_run_id(pdf_path: Path) -> str:
    """Generate a unique run ID from PDF stem + current timestamp."""
    return f"{pdf_path.stem}_{int(time.time())}"


def _move_to_processed(pdf_path: Path) -> Path:
    """Move a PDF from dropoff/ to dropoff/processed/, handling name collisions."""
    dest = DATA_DROPOFF_PROCESSED / pdf_path.name
    if dest.exists():
        stem, suffix = pdf_path.stem, pdf_path.suffix
        counter = 1
        while dest.exists():
            dest = DATA_DROPOFF_PROCESSED / f"{stem}_{counter}{suffix}"
            counter += 1
    shutil.move(str(pdf_path), str(dest))
    return dest


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------


@dg.sensor(
    job=k1_dropoff_processing_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Watches data/dropoff/ for new K-1 PDF files. "
        "When new PDFs appear, triggers parallel processing runs."
    ),
)
def k1_dropoff_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    if not DATA_DROPOFF.exists():
        return dg.SensorResult(skip_reason="Dropoff directory does not exist.")

    pdf_files = sorted(DATA_DROPOFF.glob("*.pdf"))
    if not pdf_files:
        return dg.SensorResult(skip_reason="No PDF files in dropoff directory.")

    run_requests = []
    for pdf_path in pdf_files:
        run_id = _make_run_id(pdf_path)
        context.log.info(f"New PDF: {pdf_path.name} -> run_id={run_id}")

        # Copy to data/input/{run_id}.pdf so _run_pdf_path() finds it
        dest = DATA_INPUT / f"{run_id}.pdf"
        shutil.copy2(str(pdf_path), str(dest))

        # Move original to processed/
        _move_to_processed(pdf_path)

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
                    "original_filename": pdf_path.name,
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
def k1_dropoff_failure_sensor(context: dg.RunFailureSensorContext):
    filename = context.dagster_run.tags.get("original_filename")
    if not filename:
        return

    # Find the file in processed/ and move it to failed/
    source = DATA_DROPOFF_PROCESSED / filename
    if not source.exists():
        # Try numbered variants (e.g. report_1.pdf)
        stem, suffix = Path(filename).stem, Path(filename).suffix
        for candidate in DATA_DROPOFF_PROCESSED.glob(f"{stem}*{suffix}"):
            source = candidate
            break

    if not source.exists():
        context.log.warning(f"Could not find {filename} in processed/ to move to failed/")
        return

    dest = DATA_DROPOFF_FAILED / source.name
    shutil.move(str(source), str(dest))
    context.log.info(f"Moved {source.name} to failed/ due to run failure")
