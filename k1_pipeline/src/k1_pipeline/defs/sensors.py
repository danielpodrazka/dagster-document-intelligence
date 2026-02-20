"""
File-watching sensor for automatic K-1 PDF processing.

Watches data/dropoff/ for new PDFs and triggers the single-document
processing pipeline when files appear.
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

ROUTED_PDF_PREFIX = "000_"

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
# File-routing helpers
# ---------------------------------------------------------------------------


def _route_pdf_to_input(pdf_path: Path) -> Path:
    """Copy a PDF from dropoff/ to data/input/ with a sort-friendly prefix.

    The existing _find_input_pdf() in assets.py uses
    ``sorted(DATA_INPUT.glob("*.pdf"))[0]``, so the ``000_`` prefix
    guarantees our file sorts before ``irs_k1_filled.pdf``.
    """
    dest_path = DATA_INPUT / f"{ROUTED_PDF_PREFIX}{pdf_path.name}"
    shutil.copy2(str(pdf_path), str(dest_path))
    return dest_path


def _cleanup_routed_pdfs() -> None:
    """Remove leftover 000_* PDFs from data/input/ (from previous runs)."""
    for leftover in DATA_INPUT.glob(f"{ROUTED_PDF_PREFIX}*.pdf"):
        leftover.unlink()


# ---------------------------------------------------------------------------
# Sensor
# ---------------------------------------------------------------------------


@dg.sensor(
    job=k1_dropoff_processing_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Watches data/dropoff/ for new K-1 PDF files. "
        "When a new PDF appears, copies it to data/input/ and triggers "
        "the single-document processing pipeline."
    ),
)
def k1_dropoff_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    # Clean up routed PDFs from previous evaluations
    _cleanup_routed_pdfs()

    if not DATA_DROPOFF.exists():
        return dg.SensorResult(skip_reason="Dropoff directory does not exist.")

    pdf_files = sorted(DATA_DROPOFF.glob("*.pdf"))
    if not pdf_files:
        return dg.SensorResult(skip_reason="No PDF files in dropoff directory.")

    # Process one file per evaluation (staging paths are fixed/shared)
    pdf_path = pdf_files[0]
    context.log.info(f"New PDF detected: {pdf_path.name}")

    routed_path = _route_pdf_to_input(pdf_path)
    context.log.info(f"Routed to: {routed_path}")

    # Move original to processed/
    processed_dest = DATA_DROPOFF_PROCESSED / pdf_path.name
    if processed_dest.exists():
        stem, suffix = pdf_path.stem, pdf_path.suffix
        counter = 1
        while processed_dest.exists():
            processed_dest = DATA_DROPOFF_PROCESSED / f"{stem}_{counter}{suffix}"
            counter += 1
    shutil.move(str(pdf_path), str(processed_dest))
    context.log.info(f"Moved original to: {processed_dest}")

    if len(pdf_files) > 1:
        context.log.info(
            f"{len(pdf_files) - 1} more PDF(s) pending. "
            "Will process on next sensor evaluation."
        )

    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"dropoff_{pdf_path.name}_{time.time()}",
                tags={
                    "source": "dropoff_sensor",
                    "original_filename": pdf_path.name,
                },
            ),
        ],
    )


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
