#!/usr/bin/env python3
"""Materialize all K-1 pipeline assets for all batch PDFs in parallel,
then run cross-partner validation.

Uploads each PDF to S3, materializes the full pipeline concurrently using
ThreadPoolExecutor, then triggers cross-partner validation across all
accumulated records.

Usage:
    cd pipeline && python scripts/run_all_pdfs.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dagster as dg

# Ensure project is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from k1_pipeline.defs.assets import (
    ai_financial_analysis,
    ai_structured_extraction,
    final_report,
    ocr_extracted_text,
    pii_detection_report,
    raw_k1_pdf,
    sanitized_text,
)
from k1_pipeline.defs.cross_partner import (
    cross_partner_validation,
    k1_parquet_upsert,
)
from k1_pipeline.defs.resources import S3Storage
from k1_pipeline.defs.validation import (
    k1_ai_validation,
    k1_deterministic_validation,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BATCH_DIR = PROJECT_ROOT / "data" / "input" / "batch"
CROSS_PARTNER_DIR = BATCH_DIR / "cross_partner"
MAX_WORKERS = 6  # Number of parallel pipelines

# All pipeline assets in dependency order
PIPELINE_ASSETS = [
    raw_k1_pdf,
    ocr_extracted_text,
    pii_detection_report,
    sanitized_text,
    ai_structured_extraction,
    ai_financial_analysis,
    k1_deterministic_validation,
    k1_ai_validation,
    final_report,
    k1_parquet_upsert,
]

ASSET_NAMES = [
    "raw_k1_pdf",
    "ocr_extracted_text",
    "pii_detection_report",
    "sanitized_text",
    "ai_structured_extraction",
    "ai_financial_analysis",
    "k1_deterministic_validation",
    "k1_ai_validation",
    "final_report",
    "k1_parquet_upsert",
]


def make_run_config(run_id: str) -> dict:
    """Build Dagster run_config with run_id for all ops."""
    return {
        "ops": {
            name: {"config": {"run_id": run_id}}
            for name in ASSET_NAMES
        }
    }


def collect_pdfs() -> list[Path]:
    """Collect all batch PDFs (original + cross-partner)."""
    pdfs = []
    if BATCH_DIR.exists():
        pdfs.extend(sorted(BATCH_DIR.glob("profile_*.pdf")))
    if CROSS_PARTNER_DIR.exists():
        pdfs.extend(sorted(CROSS_PARTNER_DIR.glob("profile_*.pdf")))
    return pdfs


def process_one_pdf(pdf_path: Path, idx: int) -> tuple[str, bool]:
    """Upload a PDF and materialize the full pipeline. Returns (name, success)."""
    pdf_name = pdf_path.name
    stem = pdf_path.stem
    run_id = f"{stem}_{int(time.time())}_{idx}"

    # Each worker gets its own S3 client
    resources = {
        "s3": S3Storage(),
    }
    s3 = resources["s3"]

    # Upload
    s3_key = f"input/{run_id}.pdf"
    s3.upload_from_file(str(pdf_path), s3_key, content_type="application/pdf")
    print(f"[{idx:2d}] Uploaded {pdf_name} -> {run_id}")

    # Materialize
    run_config = make_run_config(run_id)
    try:
        result = dg.materialize(
            assets=PIPELINE_ASSETS,
            run_config=run_config,
            resources=resources,
        )
        if result.success:
            print(f"[{idx:2d}] SUCCESS: {pdf_name}")
            return pdf_name, True
        else:
            print(f"[{idx:2d}] FAILED: {pdf_name}")
            for event in result.all_events:
                if event.is_failure:
                    print(f"  [{idx:2d}] {event.event_type_value}: {event.message}")
            return pdf_name, False
    except Exception as e:
        print(f"[{idx:2d}] ERROR: {pdf_name}: {e}")
        return pdf_name, False


def run_cross_partner_validation_step() -> bool:
    """Run cross-partner validation on all accumulated records."""
    print(f"\n{'='*70}")
    print("Running cross-partner validation...")
    print(f"{'='*70}")

    resources = {
        "s3": S3Storage(),
    }

    try:
        result = dg.materialize(
            assets=[cross_partner_validation],
            run_config={
                "ops": {
                    "cross_partner_validation": {
                        "config": {"partnership_ein": "", "tax_year": ""}
                    }
                }
            },
            resources=resources,
        )
        if result.success:
            print("  SUCCESS: Cross-partner validation complete")
            return True
        else:
            print("  FAILED: Cross-partner validation failed")
            return False
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def print_parquet_summary():
    """Print a summary of what's in the S3 parquet file."""
    import duckdb
    import tempfile

    from k1_pipeline.defs.cross_partner import PARQUET_S3_KEY

    s3 = S3Storage()

    print(f"\n{'='*70}")

    try:
        parquet_bytes = s3.read_bytes(PARQUET_S3_KEY)
    except Exception:
        print("No k1_records.parquet found in S3.")
        print(f"{'='*70}")
        return

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.write(parquet_bytes)
    tmp.close()

    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE k1_records AS SELECT * FROM read_parquet('{tmp.name}')")
    Path(tmp.name).unlink()

    record_count = conn.execute("SELECT COUNT(*) FROM k1_records").fetchone()[0]
    print(f"Parquet Summary: {record_count} K-1 records")
    print(f"{'='*70}")

    partnerships = conn.execute("""
        SELECT partnership_ein, tax_year, COUNT(*) as partner_count,
               MAX(partnership_name) as partnership_name
        FROM k1_records
        GROUP BY partnership_ein, tax_year
        ORDER BY partnership_ein, tax_year
    """).fetchall()

    for ein, year, count, name in partnerships:
        print(f"  {ein} | {year} | {count} partners | {name or 'N/A'}")

    conn.close()


def main():
    pdfs = collect_pdfs()
    if not pdfs:
        print("No PDFs found in batch directories.")
        sys.exit(1)

    print(f"Found {len(pdfs)} PDFs to process (max {MAX_WORKERS} parallel):\n")
    for i, p in enumerate(pdfs, 1):
        print(f"  [{i:2d}] {p.name}")

    print(f"\nStarting parallel processing...")
    start_time = time.time()

    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_one_pdf, pdf_path, idx): pdf_path
            for idx, pdf_path in enumerate(pdfs, 1)
        }

        for future in as_completed(futures):
            pdf_name, success = future.result()
            if success:
                successes += 1
            else:
                failures += 1

    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"Pipeline Results: {successes} succeeded, {failures} failed out of {len(pdfs)}")
    print(f"Total time: {elapsed:.1f}s ({elapsed/len(pdfs):.1f}s per PDF)")
    print(f"{'='*70}")

    # Run cross-partner validation
    if successes > 0:
        run_cross_partner_validation_step()

    # Print parquet summary
    print_parquet_summary()

    # Check S3 for cross-partner results
    s3 = S3Storage()
    try:
        data = s3.read_json("output/cross_partner_results.json")
        print(f"\nS3 cross_partner_results.json summary: {data['summary']}")
    except Exception:
        print("\nNo cross_partner_results.json in S3 yet.")


if __name__ == "__main__":
    main()
