"""
Batch K-1 Processing Pipeline Assets

Generates 10 filled IRS K-1 PDFs from diverse partner profiles, then
processes each through the full pipeline (OCR -> PII -> sanitize -> AI
extract -> AI analyze) and produces per-profile results plus an aggregate
batch report.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_INPUT = PROJECT_ROOT / "data" / "input"
DATA_STAGING = PROJECT_ROOT / "data" / "staging"
DATA_OUTPUT = PROJECT_ROOT / "data" / "output"

BATCH_INPUT = DATA_INPUT / "batch"
BATCH_STAGING = DATA_STAGING / "batch"
BATCH_OUTPUT = DATA_OUTPUT / "batch"

IRS_K1_BLANK = DATA_INPUT / "archive" / "irs_k1_2024.pdf"
IRS_K1_FORM_URL = "https://www.irs.gov/pub/irs-prior/f1065sk1--2024.pdf"


# ===========================================================================
# Asset: sample_k1_batch  (group=batch)
# ===========================================================================


@dg.asset(group_name="batch")
def sample_k1_batch() -> dg.MaterializeResult:
    """Generate 10 filled IRS K-1 PDFs from diverse partner profiles.

    Uses PyPDFForm to fill the official IRS Schedule K-1 (Form 1065) with
    10 distinct partner scenarios covering: real estate, venture capital,
    hedge funds, private equity, oil & gas, family LLC, medical practice,
    commercial real estate, clean energy, and restaurant operations.
    """
    from PyPDFForm import PdfWrapper

    from .k1_profiles import ALL_PROFILES, profile_to_fill_data

    # Ensure blank form exists
    if not IRS_K1_BLANK.exists():
        import urllib.request
        IRS_K1_BLANK.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(IRS_K1_FORM_URL, str(IRS_K1_BLANK))

    BATCH_INPUT.mkdir(parents=True, exist_ok=True)

    generated_files = []
    for idx, profile in enumerate(ALL_PROFILES, start=1):
        fill_data = profile_to_fill_data(profile)
        filled = PdfWrapper(str(IRS_K1_BLANK)).fill(fill_data)

        # Filename: profile_01_sunbelt_retail.pdf etc.
        slug = re.sub(r"[^a-z0-9]+", "_", profile["partnership_name"].lower())[:40].strip("_")
        filename = f"profile_{idx:02d}_{slug}.pdf"
        output_path = BATCH_INPUT / filename

        with open(output_path, "wb") as f:
            f.write(filled.read())

        generated_files.append({
            "profile_number": idx,
            "filename": filename,
            "partnership_name": profile["partnership_name"],
            "partner_name": profile["partner_name"],
            "entity_type": profile["entity_type"],
            "is_general_partner": profile["is_general_partner"],
            "fields_filled": len(fill_data),
        })

    # Save manifest
    manifest = {
        "total_profiles": len(generated_files),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": generated_files,
    }
    manifest_path = BATCH_INPUT / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    profile_summary = "\n".join(
        f"- **{f['profile_number']:02d}** {f['partnership_name']} ({f['entity_type']}, "
        f"{'GP' if f['is_general_partner'] else 'LP'})"
        for f in generated_files
    )

    return dg.MaterializeResult(
        metadata={
            "total_pdfs": dg.MetadataValue.int(len(generated_files)),
            "output_directory": dg.MetadataValue.path(str(BATCH_INPUT)),
            "profiles": dg.MetadataValue.md(profile_summary),
        }
    )


# ===========================================================================
# Helpers for per-PDF processing
# ===========================================================================


def _ocr_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF via OCR."""
    import pytesseract
    from pdf2image import convert_from_path

    images = convert_from_path(str(pdf_path), dpi=300)
    texts = [pytesseract.image_to_string(img) for img in images]
    return "\n\n--- PAGE BREAK ---\n\n".join(texts)


def _detect_pii_presidio(text: str) -> list[dict]:
    """Run Presidio PII detection (fast mode, no GLiNER)."""
    from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer

    analyzer = AnalyzerEngine()

    ein_pattern = Pattern(name="ein_pattern", regex=r"\b\d{2}-\d{7}\b", score=0.85)
    ein_recognizer = PatternRecognizer(
        supported_entity="EIN", patterns=[ein_pattern], supported_language="en",
    )
    analyzer.registry.add_recognizer(ein_recognizer)

    entities_to_detect = [
        "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN",
        "US_DRIVER_LICENSE", "LOCATION", "CREDIT_CARD", "EIN",
    ]

    results = analyzer.analyze(text=text, entities=entities_to_detect, language="en")

    return [
        {
            "entity_type": r.entity_type,
            "start": r.start,
            "end": r.end,
            "score": round(r.score, 4),
            "text_snippet": text[r.start:r.end],
        }
        for r in results
    ]


def _sanitize_text(text: str, pii_entities: list[dict]) -> str:
    """Replace PII entities with typed placeholders."""
    from presidio_analyzer import RecognizerResult
    from presidio_anonymizer import AnonymizerEngine
    from presidio_anonymizer.entities import OperatorConfig

    recognizer_results = [
        RecognizerResult(
            entity_type=e["entity_type"],
            start=e["start"],
            end=e["end"],
            score=e["score"],
        )
        for e in pii_entities
    ]

    anonymizer = AnonymizerEngine()
    entity_types = {e["entity_type"] for e in pii_entities}
    operators = {
        et: OperatorConfig("replace", {"new_value": f"<{et}>"})
        for et in entity_types
    }

    result = anonymizer.anonymize(
        text=text, analyzer_results=recognizer_results, operators=operators,
    )
    return result.text


def _ai_extract_k1(sanitized_text: str) -> dict:
    """Use PydanticAI + DeepSeek to extract structured K-1 data."""
    from pydantic_ai import Agent

    # Import the model from the main assets module
    from .assets import K1ExtractedData

    system_prompt = """You are an expert tax accountant and financial data extraction specialist.
You are given OCR-extracted text from an IRS Schedule K-1 (Form 1065 or 1120-S).
Some personally identifiable information has been replaced with placeholders like <PERSON>, <US_SSN>, etc.

Your task is to extract all available financial data from the K-1 form and return it as structured data.
For monetary amounts, use plain numbers (no dollar signs or commas). Use negative numbers for losses.
If a field is not present or not clearly readable, return null for that field.
Be thorough and accurate. Look for all box numbers and their corresponding values."""

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=K1ExtractedData,
        system_prompt=system_prompt,
    )
    result = agent.run_sync(
        f"Extract all structured K-1 financial data from the following document text:\n\n{sanitized_text}"
    )
    return result.output.model_dump()


def _ai_analyze_k1(extracted_data: dict, sanitized_text: str) -> dict:
    """Use PydanticAI + DeepSeek to perform financial analysis."""
    from pydantic_ai import Agent

    from .assets import FinancialAnalysis

    system_prompt = """You are a senior wealth management advisor and tax analyst.
You are given structured K-1 data (extracted from the form) and the raw sanitized text.
Provide a thorough financial analysis suitable for a wealth management client review.

For numerical fields (total_income, total_deductions, net_taxable_income, distribution_vs_income_ratio),
compute reasonable values from the available data. If data is insufficient, use 0.0.

For text fields, provide clear, professional analysis.

For key_observations, provide 3-5 specific observations about the K-1 data.
For tax_planning_recommendations, provide 3-5 actionable recommendations."""

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=FinancialAnalysis,
        system_prompt=system_prompt,
    )

    k1_json = json.dumps(extracted_data, indent=2)
    result = agent.run_sync(
        f"Analyze the following K-1 data and provide a comprehensive financial analysis.\n\n"
        f"## Structured K-1 Data\n{k1_json}\n\n"
        f"## Raw Sanitized Document Text\n{sanitized_text}"
    )
    return result.output.model_dump()


# ===========================================================================
# Asset: batch_process_all  (group=batch)
# ===========================================================================


def _process_single_profile(file_info: dict) -> dict:
    """Process a single K-1 PDF through the full pipeline (OCR -> PII -> sanitize -> AI).

    This function is designed to be called from a ThreadPoolExecutor.
    """
    profile_num = file_info["profile_number"]
    filename = file_info["filename"]
    pdf_path = BATCH_INPUT / filename
    slug = filename.replace(".pdf", "")
    profile_dir = BATCH_STAGING / slug
    profile_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: OCR
        ocr_text = _ocr_pdf(pdf_path)
        (profile_dir / "ocr_text.txt").write_text(ocr_text)

        # Step 2: PII Detection (Presidio only for speed)
        pii_entities = _detect_pii_presidio(ocr_text)
        (profile_dir / "pii_report.json").write_text(
            json.dumps({"total_entities": len(pii_entities), "entities": pii_entities}, indent=2)
        )

        # Step 3: Sanitize
        sanitized = _sanitize_text(ocr_text, pii_entities)
        (profile_dir / "sanitized_text.txt").write_text(sanitized)

        # Step 4: AI Extraction
        extracted = _ai_extract_k1(sanitized)
        (profile_dir / "structured_k1.json").write_text(json.dumps(extracted, indent=2))

        # Step 5: AI Analysis
        analysis = _ai_analyze_k1(extracted, sanitized)
        (profile_dir / "financial_analysis.json").write_text(json.dumps(analysis, indent=2))

        return {
            "profile_number": profile_num,
            "filename": filename,
            "partnership_name": file_info["partnership_name"],
            "partner_name": file_info["partner_name"],
            "entity_type": file_info["entity_type"],
            "is_general_partner": file_info["is_general_partner"],
            "status": "success",
            "ocr_chars": len(ocr_text),
            "pii_entities_found": len(pii_entities),
            "k1_data": extracted,
            "financial_analysis": analysis,
        }

    except Exception as e:
        return {
            "profile_number": profile_num,
            "filename": filename,
            "partnership_name": file_info["partnership_name"],
            "status": "error",
            "error": str(e),
        }


@dg.asset(group_name="batch", deps=["sample_k1_batch"])
def batch_process_all(context) -> dg.MaterializeResult:
    """Process all 10 K-1 PDFs through the full pipeline in parallel.

    For each PDF: OCR -> PII Detection (Presidio) -> Sanitization ->
    AI Structured Extraction -> AI Financial Analysis.

    Uses ThreadPoolExecutor with 5 workers so multiple profiles are
    processed concurrently (AI API calls are I/O-bound).

    Results are stored per-profile in data/staging/batch/{profile_slug}/
    and a combined batch_results.json is written to data/staging/.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    BATCH_STAGING.mkdir(parents=True, exist_ok=True)

    manifest_path = BATCH_INPUT / "manifest.json"
    manifest = json.loads(manifest_path.read_text())

    all_results: list[dict] = []
    errors: list[dict] = []

    context.log.info(f"Processing {len(manifest['files'])} profiles in parallel (5 workers)")

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_info = {
            executor.submit(_process_single_profile, fi): fi
            for fi in manifest["files"]
        }

        for future in as_completed(future_to_info):
            file_info = future_to_info[future]
            result = future.result()
            all_results.append(result)

            if result["status"] == "success":
                context.log.info(
                    f"  Profile {result['profile_number']:02d} OK — "
                    f"{result['partnership_name']}"
                )
            else:
                errors.append(result)
                context.log.warning(
                    f"  Profile {result['profile_number']:02d} FAILED — "
                    f"{result.get('error', 'unknown')}"
                )

    # Sort results by profile number for consistent output
    all_results.sort(key=lambda r: r["profile_number"])

    # Save combined results
    batch_results = {
        "total_profiles": len(manifest["files"]),
        "successful": sum(1 for r in all_results if r["status"] == "success"),
        "failed": len(errors),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "results": all_results,
        "errors": errors,
    }

    batch_results_path = BATCH_STAGING / "batch_results.json"
    batch_results_path.write_text(json.dumps(batch_results, indent=2))

    success_count = batch_results["successful"]
    fail_count = batch_results["failed"]

    return dg.MaterializeResult(
        metadata={
            "total_processed": dg.MetadataValue.int(len(all_results)),
            "successful": dg.MetadataValue.int(success_count),
            "failed": dg.MetadataValue.int(fail_count),
            "staging_path": dg.MetadataValue.path(str(batch_results_path)),
            "status": dg.MetadataValue.text(
                f"{success_count}/{len(all_results)} profiles processed successfully"
                + (f" ({fail_count} errors)" if fail_count else "")
            ),
        }
    )


# ===========================================================================
# Asset: batch_report  (group=batch)
# ===========================================================================


@dg.asset(group_name="batch", deps=["batch_process_all"])
def batch_report() -> dg.MaterializeResult:
    """Aggregate batch results into a comprehensive report for the frontend.

    Produces:
      - batch_report.json: Full per-profile results
      - batch_summary.csv: Flat CSV with key financial fields per profile
      - batch_pipeline_results.json: Frontend-ready JSON
    """
    import csv

    BATCH_OUTPUT.mkdir(parents=True, exist_ok=True)

    batch_results = json.loads((BATCH_STAGING / "batch_results.json").read_text())
    now = datetime.now(timezone.utc).isoformat()

    # ---- 1. Full JSON Report ----
    report = {
        "report_title": "Batch K-1 Processing Report (10 Profiles)",
        "generated_at": now,
        "summary": {
            "total_profiles": batch_results["total_profiles"],
            "successful": batch_results["successful"],
            "failed": batch_results["failed"],
        },
        "profiles": batch_results["results"],
    }
    report_path = BATCH_OUTPUT / "batch_report.json"
    report_path.write_text(json.dumps(report, indent=2))

    # ---- 2. CSV Summary ----
    csv_path = BATCH_OUTPUT / "batch_summary.csv"
    csv_headers = [
        "profile_number", "partnership_name", "entity_type", "is_gp", "status",
        "ordinary_income", "rental_re_income", "guaranteed_payments",
        "interest_income", "lt_capital_gains", "st_capital_gains",
        "section_179", "distributions", "capital_beginning", "capital_ending",
        "self_employment", "foreign_taxes", "qbi_deduction",
        "total_income", "net_taxable_income", "pii_entities",
    ]

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        for r in batch_results["results"]:
            k1 = r.get("k1_data", {})
            fa = r.get("financial_analysis", {})
            writer.writerow([
                r["profile_number"],
                r["partnership_name"],
                r.get("entity_type", ""),
                r.get("is_general_partner", ""),
                r["status"],
                k1.get("ordinary_business_income", ""),
                k1.get("rental_real_estate_income", ""),
                k1.get("guaranteed_payments", ""),
                k1.get("interest_income", ""),
                k1.get("long_term_capital_gains", ""),
                k1.get("short_term_capital_gains", ""),
                k1.get("section_179_deduction", ""),
                k1.get("distributions", ""),
                k1.get("capital_account_beginning", ""),
                k1.get("capital_account_ending", ""),
                k1.get("self_employment_earnings", ""),
                k1.get("foreign_taxes_paid", ""),
                k1.get("qbi_deduction", ""),
                fa.get("total_income", ""),
                fa.get("net_taxable_income", ""),
                r.get("pii_entities_found", ""),
            ])

    # ---- 3. Frontend-ready JSON ----
    frontend_profiles = []
    for r in batch_results["results"]:
        if r["status"] != "success":
            frontend_profiles.append({
                "profile_number": r["profile_number"],
                "partnership_name": r["partnership_name"],
                "status": "error",
                "error": r.get("error", "Unknown error"),
            })
            continue

        frontend_profiles.append({
            "profile_number": r["profile_number"],
            "partnership_name": r["partnership_name"],
            "partner_name": r.get("partner_name", ""),
            "entity_type": r.get("entity_type", ""),
            "is_general_partner": r.get("is_general_partner", False),
            "status": "success",
            "k1_data": r["k1_data"],
            "financial_analysis": r["financial_analysis"],
            "pii_entities_found": r.get("pii_entities_found", 0),
            "ocr_chars": r.get("ocr_chars", 0),
        })

    frontend_payload = {
        "pipeline_run": {
            "generated_at": now,
            "status": "completed",
            "pipeline_version": "1.0.0",
            "mode": "batch",
            "total_profiles": batch_results["total_profiles"],
            "successful": batch_results["successful"],
        },
        "profiles": frontend_profiles,
        "output_files": {
            "full_report": str(report_path),
            "csv_summary": str(csv_path),
        },
    }

    frontend_path = BATCH_OUTPUT / "batch_pipeline_results.json"
    frontend_path.write_text(json.dumps(frontend_payload, indent=2))

    # ---- 4. PDF Report (WeasyPrint) ----
    from k1_pipeline.defs.pdf_templates import render_batch_report_html, generate_pdf

    pdf_html = render_batch_report_html(batch_results)
    pdf_path = BATCH_OUTPUT / "batch_report.pdf"
    generate_pdf(pdf_html, pdf_path)

    frontend_payload["output_files"]["pdf_report"] = str(pdf_path)
    frontend_path.write_text(json.dumps(frontend_payload, indent=2))

    # Build summary table for metadata
    rows = []
    for r in batch_results["results"]:
        k1 = r.get("k1_data", {})
        fa = r.get("financial_analysis", {})
        income = fa.get("net_taxable_income")
        income_str = f"${income:,.0f}" if income else "N/A"
        rows.append(
            f"| {r['profile_number']:02d} | {r['partnership_name'][:35]} | "
            f"{r.get('entity_type', '')[:10]} | {r['status']} | {income_str} |"
        )

    table = (
        "| # | Partnership | Type | Status | Net Income |\n"
        "|---|---|---|---|---|\n"
        + "\n".join(rows)
    )

    return dg.MaterializeResult(
        metadata={
            "report_path": dg.MetadataValue.path(str(report_path)),
            "csv_path": dg.MetadataValue.path(str(csv_path)),
            "frontend_path": dg.MetadataValue.path(str(frontend_path)),
            "pdf_report": dg.MetadataValue.path(str(pdf_path)),
            "batch_summary": dg.MetadataValue.md(table),
        }
    )
