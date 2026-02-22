"""
K-1 Tax Document Processing Pipeline Assets

This module defines the Dagster assets for ingesting, processing, analyzing,
and reporting on K-1 partnership tax documents. The pipeline demonstrates:
  - PDF ingestion and OCR extraction
  - PII detection and sanitization (compliance)
  - AI-powered structured data extraction via DeepSeek
  - Financial analysis and reporting
"""

import base64
import csv
import io
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import dagster as dg
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from k1_pipeline.defs.resources import S3Storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------------------------
# Pydantic models for AI extraction
# ---------------------------------------------------------------------------


class K1ExtractedData(BaseModel):
    """Structured representation of data extracted from a K-1 tax form."""

    tax_year: Optional[str] = Field(None, description="Tax year for the K-1")
    partnership_name: Optional[str] = Field(None, description="Name of the partnership or S-Corp")
    partner_type: Optional[str] = Field(None, description="General or Limited partner")
    partner_share_percentage: Optional[float] = Field(
        None, description="Partner's profit/loss/capital share percentage"
    )
    ordinary_business_income: Optional[float] = Field(
        None, description="Box 1 - Ordinary business income (loss)"
    )
    rental_real_estate_income: Optional[float] = Field(
        None, description="Box 2 - Net rental real estate income (loss)"
    )
    guaranteed_payments: Optional[float] = Field(
        None, description="Box 4 - Guaranteed payments"
    )
    interest_income: Optional[float] = Field(
        None, description="Box 5 - Interest income"
    )
    ordinary_dividends: Optional[float] = Field(
        None, description="Box 6a - Ordinary dividends"
    )
    qualified_dividends: Optional[float] = Field(
        None, description="Box 6b - Qualified dividends"
    )
    short_term_capital_gains: Optional[float] = Field(
        None, description="Box 8 - Net short-term capital gain (loss)"
    )
    long_term_capital_gains: Optional[float] = Field(
        None, description="Box 9a - Net long-term capital gain (loss)"
    )
    section_179_deduction: Optional[float] = Field(
        None, description="Box 12 - Section 179 deduction"
    )
    distributions: Optional[float] = Field(
        None, description="Box 19 - Distributions"
    )
    capital_account_beginning: Optional[float] = Field(
        None, description="Beginning capital account"
    )
    capital_account_ending: Optional[float] = Field(
        None, description="Ending capital account"
    )
    self_employment_earnings: Optional[float] = Field(
        None, description="Box 14 - Self-employment earnings (loss)"
    )
    foreign_taxes_paid: Optional[float] = Field(
        None, description="Box 16 - Foreign taxes paid or accrued"
    )
    qbi_deduction: Optional[float] = Field(
        None, description="Box 20 Code Z - Qualified business income deduction"
    )


class FinancialAnalysis(BaseModel):
    """AI-generated financial analysis of the K-1 data."""

    total_income: Optional[float] = Field(
        None, description="Sum of all income items"
    )
    total_deductions: Optional[float] = Field(
        None, description="Sum of all deduction items"
    )
    net_taxable_income: Optional[float] = Field(
        None, description="Net taxable income from the K-1"
    )
    effective_tax_considerations: Optional[str] = Field(
        None, description="Summary of key tax considerations"
    )
    capital_gains_summary: Optional[str] = Field(
        None, description="Summary of capital gains/losses"
    )
    distribution_vs_income_ratio: Optional[float] = Field(
        None, description="Ratio of distributions to total income"
    )
    key_observations: list[str] = Field(
        default_factory=list,
        description="Key observations about the K-1 data",
    )
    tax_planning_recommendations: list[str] = Field(
        default_factory=list,
        description="Tax planning recommendations based on the data",
    )


# ---------------------------------------------------------------------------
# Run config: allows per-run isolation for parallel processing
# ---------------------------------------------------------------------------


class K1RunConfig(dg.Config):
    """Config passed to each asset to isolate staging paths per run."""
    run_id: str = ""


IRS_K1_FORM_URL = "https://www.irs.gov/pub/irs-prior/f1065sk1--2024.pdf"


def _run_pdf_key(s3: S3Storage, run_id: str) -> str:
    """Return the S3 key for the PDF for this run, or the first PDF in input/."""
    if run_id:
        return s3.input_key(f"{run_id}.pdf")
    pdfs = s3.list_objects("input/", suffix=".pdf")
    pdfs = [k for k in pdfs if not k.startswith("input/archive/") and not k.startswith("input/batch/")]
    if not pdfs:
        raise FileNotFoundError("No PDF files found in input/")
    return pdfs[0]


# ===========================================================================
# Asset 0: irs_k1_form_fill  (group=ingestion)
# ===========================================================================


@dg.asset(group_name="ingestion")
def irs_k1_form_fill(s3: S3Storage) -> dg.MaterializeResult:
    """Download the official IRS Schedule K-1 (Form 1065) and fill it with sample data.

    Uses PyPDFForm to programmatically fill the real IRS PDF form with
    realistic K-1 data, producing a filled PDF that exercises the full
    pipeline on a genuine government form rather than a synthetic one.
    """
    from PyPDFForm import PdfWrapper

    # Download blank form if needed
    blank_key = "input/archive/irs_k1_2024.pdf"
    if not s3.exists(blank_key):
        import urllib.request

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            urllib.request.urlretrieve(IRS_K1_FORM_URL, tmp.name)
            s3.upload_from_file(tmp.name, blank_key, content_type="application/pdf")

    fill_data = {
        # Part I: Partnership
        "f1_6[0]": "82-4571903",
        "f1_7[0]": "Meridian Capital Growth Fund, LP\n450 Park Avenue, Suite 2100\nNew York, NY 10022",
        "f1_8[0]": "Ogden, UT",
        # Part II: Partner
        "f1_9[0]": "478-93-6215",
        "f1_10[0]": "Jonathan A. Blackwell\n1847 Oakridge Drive\nGreenwich, CT 06831",
        "c1_4[0]": True,       # General partner
        "c1_5[0]": True,       # Domestic partner
        "f1_13[0]": "Individual",
        # J: Share percentages
        "f1_14[0]": "3.75", "f1_15[0]": "3.75",
        "f1_16[0]": "3.75", "f1_17[0]": "3.75",
        "f1_18[0]": "3.75", "f1_19[0]": "3.75",
        # K1: Liabilities
        "f1_20[0]": "38,750", "f1_21[0]": "38,750",
        "f1_24[0]": "12,500", "f1_25[0]": "12,500",
        # L: Capital Account
        "f1_26[0]": "542,100", "f1_27[0]": "50,000",
        "f1_28[0]": "244,145", "f1_30[0]": "95,000",
        "f1_31[0]": "741,245",
        "c1_8[0]": True,       # Tax basis
        # Part III: Income
        "f1_34[0]": "127,450",     # Box 1: Ordinary business income
        "f1_35[0]": "(18,200)",     # Box 2: Net rental real estate
        "f1_37[0]": "45,000",      # Box 4a: Guaranteed payments
        "f1_39[0]": "45,000",      # Box 4c: Total guaranteed
        "f1_40[0]": "8,325",       # Box 5: Interest income
        "f1_41[0]": "12,780",      # Box 6a: Ordinary dividends
        "f1_42[0]": "9,150",       # Box 6b: Qualified dividends
        "f1_45[0]": "(3,400)",     # Box 8: ST capital gain
        "f1_46[0]": "67,890",      # Box 9a: LT capital gain
        "f1_54[0]": "4,200",       # Box 12: Section 179
        "f1_55[0]": "15,000",      # Box 13: Other deductions
        # Box 14: Self-employment
        "f1_60[0]": "A  172,450",
        "f1_61[0]": "C  172,450",
        # Box 17: AMT
        "f1_79[0]": "A  (2,300)",
        # Box 18: Tax-exempt
        "f1_84[0]": "C  3,100",
        # Box 19: Distributions
        "f1_89[0]": "A  95,000",
        # Box 20: Other info
        "f1_92[0]": "A  8,325",
        "f1_93[0]": "B  4,500",
        "f1_94[0]": "Z  127,450",
    }

    # Download blank to temp, fill, upload result
    blank_tmp = s3.download_to_tempfile(blank_key, suffix=".pdf")
    filled = PdfWrapper(blank_tmp).fill(fill_data)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as out_tmp:
        out_tmp.write(filled.read())
        out_tmp.flush()
        output_key = s3.input_key("irs_k1_filled.pdf")
        s3.upload_from_file(out_tmp.name, output_key, content_type="application/pdf")

    return dg.MaterializeResult(
        metadata={
            "source_form": dg.MetadataValue.text("IRS Schedule K-1 (Form 1065) 2024"),
            "source_url": dg.MetadataValue.url(IRS_K1_FORM_URL),
            "output_key": dg.MetadataValue.text(output_key),
            "fields_filled": dg.MetadataValue.int(len(fill_data)),
        }
    )


# ===========================================================================
# Asset 1: raw_k1_pdf  (group=ingestion)
# ===========================================================================


@dg.asset(group_name="ingestion", deps=["irs_k1_form_fill"])
def raw_k1_pdf(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Ingest a K-1 PDF from S3 input/ and store its raw bytes (base64) in staging.

    This is the entry point of the pipeline. It reads the first PDF found in
    the input prefix, encodes it as base64, and persists it to a JSON file
    in staging/ so downstream assets have a reproducible snapshot.
    """
    pdf_key = _run_pdf_key(s3, config.run_id)
    pdf_bytes = s3.read_bytes(pdf_key)
    encoded = base64.b64encode(pdf_bytes).decode("utf-8")

    # Attempt to get page count via PyPDF if available, otherwise fall back
    page_count = 0
    try:
        from pypdf import PdfReader  # type: ignore[import-untyped]

        tmp_path = s3.download_to_tempfile(pdf_key, suffix=".pdf")
        reader = PdfReader(tmp_path)
        page_count = len(reader.pages)
    except Exception:
        try:
            from pdf2image import pdfinfo_from_path

            tmp_path = s3.download_to_tempfile(pdf_key, suffix=".pdf")
            info = pdfinfo_from_path(tmp_path)
            page_count = info.get("Pages", 0)
        except Exception:
            page_count = -1  # unknown

    file_name = pdf_key.rsplit("/", 1)[-1]

    staging_payload = {
        "file_name": file_name,
        "file_size_bytes": len(pdf_bytes),
        "page_count": page_count,
        "base64_data": encoded,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    staging_key = s3.staging_key(config.run_id, "raw_pdf_bytes.json")
    s3.write_json(staging_key, staging_payload)

    return dg.MaterializeResult(
        metadata={
            "file_name": dg.MetadataValue.text(file_name),
            "file_size_bytes": dg.MetadataValue.int(len(pdf_bytes)),
            "page_count": dg.MetadataValue.int(page_count),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )


# ===========================================================================
# Asset 2: ocr_extracted_text  (group=processing)
# ===========================================================================


@dg.asset(group_name="processing", deps=["raw_k1_pdf"])
def ocr_extracted_text(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Convert K-1 PDF pages to images and extract text via OCR (Tesseract).

    Uses pdf2image to rasterize each page and pytesseract to perform OCR.
    The extracted text (per-page and combined) is saved to staging.
    """
    import pytesseract
    from pdf2image import convert_from_path

    pdf_key = _run_pdf_key(s3, config.run_id)
    pdf_tmp = s3.download_to_tempfile(pdf_key, suffix=".pdf")

    # Convert PDF pages to PIL images
    images = convert_from_path(pdf_tmp, dpi=300)

    pages_text: list[dict] = []
    full_text_parts: list[str] = []

    for idx, img in enumerate(images):
        text = pytesseract.image_to_string(img)
        pages_text.append({"page": idx + 1, "text": text})
        full_text_parts.append(text)

    full_text = "\n\n--- PAGE BREAK ---\n\n".join(full_text_parts)
    total_chars = sum(len(p["text"]) for p in pages_text)
    file_name = pdf_key.rsplit("/", 1)[-1]

    staging_payload = {
        "source_file": file_name,
        "page_count": len(pages_text),
        "pages": pages_text,
        "full_text": full_text,
        "total_characters": total_chars,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    staging_key = s3.staging_key(config.run_id, "ocr_text.json")
    s3.write_json(staging_key, staging_payload)

    preview = full_text[:500] + ("..." if len(full_text) > 500 else "")

    return dg.MaterializeResult(
        metadata={
            "page_count": dg.MetadataValue.int(len(pages_text)),
            "total_characters": dg.MetadataValue.int(total_chars),
            "text_preview": dg.MetadataValue.md(f"```\n{preview}\n```"),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )


# ===========================================================================
# Asset 3: pii_detection_report  (group=compliance)
# ===========================================================================


def _run_presidio_only(full_text: str, entities_to_detect: list[str]) -> list:
    """Run PII detection using Presidio with spaCy NER only."""
    from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern

    analyzer = AnalyzerEngine()

    ein_pattern = Pattern(name="ein_pattern", regex=r"\b\d{2}-\d{7}\b", score=0.85)
    ein_recognizer = PatternRecognizer(
        supported_entity="EIN", patterns=[ein_pattern], supported_language="en",
    )
    analyzer.registry.add_recognizer(ein_recognizer)

    return analyzer.analyze(text=full_text, entities=entities_to_detect, language="en")


def _run_gliner_only(full_text: str) -> list:
    """Run PII detection using GLiNER zero-shot NER only."""
    from presidio_analyzer import AnalyzerEngine
    from presidio_analyzer.predefined_recognizers import GLiNERRecognizer

    gliner_recognizer = GLiNERRecognizer(
        model_name="urchade/gliner_multi_pii-v1",
        supported_language="en",
        entity_mapping={
            "person": "PERSON",
            "phone number": "PHONE_NUMBER",
            "email": "EMAIL_ADDRESS",
            "passport number": "PASSPORT",
            "social security number": "US_SSN",
            "credit card number": "CREDIT_CARD",
            "address": "ADDRESS",
            "date of birth": "DATE_OF_BIRTH",
            "driver license": "US_DRIVER_LICENSE",
            "tax identification number": "EIN",
        },
        threshold=0.3,
    )

    analyzer = AnalyzerEngine()
    analyzer.registry.add_recognizer(gliner_recognizer)

    return analyzer.analyze(text=full_text, language="en")


def _run_presidio_plus_gliner(full_text: str, entities_to_detect: list[str]) -> list:
    """Run PII detection using Presidio + GLiNER combined."""
    from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
    from presidio_analyzer.predefined_recognizers import GLiNERRecognizer

    gliner_recognizer = GLiNERRecognizer(
        model_name="urchade/gliner_multi_pii-v1",
        supported_language="en",
        entity_mapping={
            "person": "PERSON",
            "phone number": "PHONE_NUMBER",
            "email": "EMAIL_ADDRESS",
            "passport number": "PASSPORT",
            "social security number": "US_SSN",
            "credit card number": "CREDIT_CARD",
            "address": "ADDRESS",
            "date of birth": "DATE_OF_BIRTH",
            "driver license": "US_DRIVER_LICENSE",
            "tax identification number": "EIN",
        },
        threshold=0.3,
    )

    analyzer = AnalyzerEngine()

    ein_pattern = Pattern(name="ein_pattern", regex=r"\b\d{2}-\d{7}\b", score=0.85)
    ein_recognizer = PatternRecognizer(
        supported_entity="EIN", patterns=[ein_pattern], supported_language="en",
    )
    analyzer.registry.add_recognizer(ein_recognizer)
    analyzer.registry.add_recognizer(gliner_recognizer)

    all_entities = list(set(entities_to_detect + ["ADDRESS", "DATE_OF_BIRTH", "PASSPORT"]))
    return analyzer.analyze(text=full_text, entities=all_entities, language="en")


# Common K-1 form terms that PII detectors incorrectly flag as entities
K1_ALLOWLIST = {
    "partner", "partners", "partnership", "partnerships",
    "general partner", "limited partner", "domestic partner", "foreign partner",
    "schedule k-1", "k-1", "form 1065", "form 1120-s",
    "keogh", "ira", "llc", "llp", "lp",
    "individual", "corporation", "trust", "estate",
    "tax matters partner", "partnership representative",
    "er member",  # OCR fragment from "partner/other LLC member"
    "address",
}


def _filter_false_positives(results, full_text: str) -> list:
    """Remove PII detections that match common K-1 form terminology."""
    filtered = []
    for r in results:
        text_snippet = full_text[r.start:r.end].strip()
        if text_snippet.lower() in K1_ALLOWLIST:
            continue
        filtered.append(r)
    return filtered


def _results_to_report(results, full_text: str) -> dict:
    """Convert analyzer results to a structured report dict."""
    results = _filter_false_positives(results, full_text)

    entities_found: list[dict] = []
    entity_counts: dict[str, int] = {}

    for r in results:
        entities_found.append({
            "entity_type": r.entity_type,
            "start": r.start,
            "end": r.end,
            "score": round(r.score, 4),
            "text_snippet": full_text[r.start : r.end],
        })
        entity_counts[r.entity_type] = entity_counts.get(r.entity_type, 0) + 1

    return {
        "total_entities": len(entities_found),
        "entity_counts": entity_counts,
        "entities": entities_found,
    }


@dg.asset(group_name="compliance", deps=["ocr_extracted_text"])
def pii_detection_report(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Detect PII entities using three approaches: Presidio, GLiNER, and combined.

    Runs Presidio (spaCy NER + regex), GLiNER (zero-shot NER), and a hybrid of
    both, then saves individual reports and a side-by-side comparison to staging.
    The combined (Presidio+GLiNER) result is used as the primary PII report for
    downstream assets.
    """
    # Load OCR text
    ocr_data = s3.read_json(s3.staging_key(config.run_id, "ocr_text.json"))
    full_text = ocr_data["full_text"]

    entities_to_detect = [
        "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN",
        "US_DRIVER_LICENSE", "LOCATION", "CREDIT_CARD", "IBAN_CODE",
        "NRP", "EIN",
    ]

    # --- Mode 1: Presidio only ---
    presidio_results = _run_presidio_only(full_text, entities_to_detect)
    presidio_report = _results_to_report(presidio_results, full_text)

    # --- Mode 2: GLiNER only ---
    gliner_results = _run_gliner_only(full_text)
    gliner_report = _results_to_report(gliner_results, full_text)

    # --- Mode 3: Presidio + GLiNER ---
    combined_results = _run_presidio_plus_gliner(full_text, entities_to_detect)
    combined_report = _results_to_report(combined_results, full_text)

    now = datetime.now(timezone.utc).isoformat()

    # Save the combined report as the primary PII report (used by downstream assets)
    primary_report = {**combined_report, "analyzed_at": now}
    pii_key = s3.staging_key(config.run_id, "pii_report.json")
    s3.write_json(pii_key, primary_report)

    # Save the full comparison
    comparison = {
        "presidio_only": presidio_report,
        "gliner_only": gliner_report,
        "presidio_plus_gliner": combined_report,
        "analyzed_at": now,
    }

    comparison_key = s3.staging_key(config.run_id, "pii_comparison.json")
    s3.write_json(comparison_key, comparison)

    # Build comparison table for metadata
    all_types = sorted(
        set(presidio_report["entity_counts"])
        | set(gliner_report["entity_counts"])
        | set(combined_report["entity_counts"])
    )
    header = "| Entity Type | Presidio | GLiNER | Combined |\n|---|---|---|---|"
    rows = []
    for etype in all_types:
        p = presidio_report["entity_counts"].get(etype, 0)
        g = gliner_report["entity_counts"].get(etype, 0)
        c = combined_report["entity_counts"].get(etype, 0)
        rows.append(f"| {etype} | {p} | {g} | {c} |")
    rows.append(f"| **TOTAL** | **{presidio_report['total_entities']}** | **{gliner_report['total_entities']}** | **{combined_report['total_entities']}** |")
    comparison_table = header + "\n" + "\n".join(rows)

    return dg.MaterializeResult(
        metadata={
            "total_entities_combined": dg.MetadataValue.int(combined_report["total_entities"]),
            "total_entities_presidio": dg.MetadataValue.int(presidio_report["total_entities"]),
            "total_entities_gliner": dg.MetadataValue.int(gliner_report["total_entities"]),
            "comparison": dg.MetadataValue.md(comparison_table),
            "staging_key": dg.MetadataValue.text(pii_key),
            "comparison_key": dg.MetadataValue.text(comparison_key),
        }
    )


# ===========================================================================
# Asset 4: sanitized_text  (group=compliance)
# ===========================================================================


@dg.asset(group_name="compliance", deps=["ocr_extracted_text", "pii_detection_report"])
def sanitized_text(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Replace detected PII in the OCR text with instance-aware placeholder tokens.

    Each unique PII value gets a numbered placeholder (e.g., <PERSON_1>, <PERSON_2>)
    so the AI can distinguish between different entities and a mapping table enables
    reversibility. The mapping is stored alongside the sanitized text.
    """
    from presidio_analyzer import RecognizerResult

    # Load data
    ocr_data = s3.read_json(s3.staging_key(config.run_id, "ocr_text.json"))
    pii_report = s3.read_json(s3.staging_key(config.run_id, "pii_report.json"))
    full_text = ocr_data["full_text"]

    # Reconstruct RecognizerResult objects and extract original text spans
    entities_with_text: list[tuple[RecognizerResult, str]] = []
    for entity in pii_report["entities"]:
        result = RecognizerResult(
            entity_type=entity["entity_type"],
            start=entity["start"],
            end=entity["end"],
            score=entity["score"],
        )
        original_text = full_text[entity["start"]:entity["end"]]
        entities_with_text.append((result, original_text))

    # Build instance-aware mapping: same text -> same numbered placeholder
    # e.g., "John Smith" always maps to <PERSON_1>
    type_counters: dict[str, int] = {}
    text_to_placeholder: dict[tuple[str, str], str] = {}  # (entity_type, text) -> placeholder
    placeholder_to_original: dict[str, str] = {}  # <PERSON_1> -> "John Smith"

    # Assign placeholders by first occurrence order for stable numbering
    for result, original_text in entities_with_text:
        key = (result.entity_type, original_text)
        if key not in text_to_placeholder:
            entity_type = result.entity_type
            type_counters[entity_type] = type_counters.get(entity_type, 0) + 1
            placeholder = f"<{entity_type}_{type_counters[entity_type]}>"
            text_to_placeholder[key] = placeholder
            placeholder_to_original[placeholder] = original_text

    # Replace entities in reverse position order to preserve offsets.
    # Skip overlapping entities — when two detections cover the same text,
    # keep the one with the higher score (sorted first by score descending).
    sorted_entities = sorted(
        entities_with_text,
        key=lambda x: (-x[0].score, x[0].start),
    )

    # Mark which character positions are already claimed
    claimed: set[int] = set()
    non_overlapping = []
    for result, original_text in sorted_entities:
        span = set(range(result.start, result.end))
        if span & claimed:
            continue  # overlaps with a higher-scored entity
        claimed |= span
        non_overlapping.append((result, original_text))

    # Now replace in reverse position order to preserve offsets
    non_overlapping.sort(key=lambda x: x[0].start, reverse=True)
    sanitized = full_text
    for result, original_text in non_overlapping:
        key = (result.entity_type, original_text)
        placeholder = text_to_placeholder[key]
        sanitized = sanitized[:result.start] + placeholder + sanitized[result.end:]

    replacement_count = len(non_overlapping)

    staging_payload = {
        "sanitized_text": sanitized,
        "placeholder_mapping": placeholder_to_original,
        "replacements_made": replacement_count,
        "unique_entities": len(text_to_placeholder),
        "original_character_count": len(full_text),
        "sanitized_character_count": len(sanitized),
        "sanitized_at": datetime.now(timezone.utc).isoformat(),
    }

    staging_key = s3.staging_key(config.run_id, "sanitized_text.json")
    s3.write_json(staging_key, staging_payload)

    preview = sanitized[:500] + ("..." if len(sanitized) > 500 else "")

    return dg.MaterializeResult(
        metadata={
            "replacements_made": dg.MetadataValue.int(replacement_count),
            "unique_entities": dg.MetadataValue.int(len(text_to_placeholder)),
            "sanitized_text_preview": dg.MetadataValue.md(f"```\n{preview}\n```"),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )


# ===========================================================================
# Asset 5: ai_structured_extraction  (group=ai_analysis)
# ===========================================================================


def _serialize_messages(messages) -> list[dict]:
    """Serialize PydanticAI message objects to JSON-friendly dicts."""
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    raw = json.loads(ModelMessagesTypeAdapter.dump_json(messages))
    # Simplify to the fields most useful for auditing
    simplified = []
    for msg in raw:
        kind = msg.get("kind", "unknown")
        entry: dict = {"role": kind}
        if kind == "request":
            # Extract the human-readable text from parts
            texts = []
            for part in msg.get("parts", []):
                if part.get("part_kind") == "system-prompt":
                    entry["system_prompt"] = part.get("content", "")
                elif part.get("part_kind") == "user-prompt":
                    texts.append(part.get("content", ""))
                elif part.get("part_kind") == "tool-return":
                    texts.append(f"[tool-return] {part.get('content', '')}")
            if texts:
                entry["user_prompt"] = "\n".join(texts)
            if msg.get("instructions"):
                entry["instructions"] = msg["instructions"]
        elif kind == "response":
            entry["model_name"] = msg.get("model_name")
            # Extract text and tool calls
            texts = []
            tool_calls = []
            for part in msg.get("parts", []):
                pk = part.get("part_kind", "")
                if pk == "text":
                    texts.append(part.get("content", ""))
                elif pk == "tool-call":
                    tool_calls.append({
                        "tool_name": part.get("tool_name"),
                        "args": part.get("args"),
                    })
            if texts:
                entry["text"] = "\n".join(texts)
            if tool_calls:
                entry["tool_calls"] = tool_calls
        simplified.append(entry)
    return simplified


@dg.asset(group_name="ai_analysis", deps=["sanitized_text"])
def ai_structured_extraction(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Use PydanticAI with DeepSeek to extract structured K-1 data from sanitized text.

    Sends the PII-sanitized OCR text to DeepSeek (via PydanticAI) with a
    carefully crafted prompt instructing the model to extract all financial
    fields from the K-1 form into a strongly-typed Pydantic model.
    """
    from pydantic_ai import Agent

    # Load sanitized text
    sanitized_data = s3.read_json(s3.staging_key(config.run_id, "sanitized_text.json"))
    text = sanitized_data["sanitized_text"]

    system_prompt = """You are an expert tax accountant and financial data extraction specialist.
You are given OCR-extracted text from an IRS Schedule K-1 (Form 1065 or 1120-S).
Some personally identifiable information has been replaced with numbered placeholders like <PERSON_1>, <PERSON_2>, <US_SSN_1>, etc. Each number identifies a unique entity instance.

Your task is to extract all available financial data from the K-1 form and return it as structured data.
For monetary amounts, use plain numbers (no dollar signs or commas). Use negative numbers for losses.
If a field is not present or not clearly readable, return null for that field.
Be thorough and accurate. Look for all box numbers and their corresponding values."""

    user_prompt = f"Extract all structured K-1 financial data from the following document text:\n\n{text}"

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=K1ExtractedData,
        system_prompt=system_prompt,
    )

    result = agent.run_sync(user_prompt)

    extracted: K1ExtractedData = result.output
    extracted_dict = extracted.model_dump()

    # Serialize the full AI conversation for auditing
    ai_messages = _serialize_messages(result.all_messages())

    staging_payload = {
        "extracted_data": extracted_dict,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "ai_interaction": {
            "model": "deepseek:deepseek-chat",
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "output_schema": K1ExtractedData.model_json_schema(),
            "raw_messages": ai_messages,
            "usage": result.usage().model_dump() if hasattr(result.usage(), "model_dump") else str(result.usage()),
        },
    }

    staging_key = s3.staging_key(config.run_id, "structured_k1.json")
    s3.write_json(staging_key, staging_payload)

    # Build summary for metadata
    non_null_fields = {k: v for k, v in extracted_dict.items() if v is not None}
    summary_lines = [f"- **{k}**: {v}" for k, v in non_null_fields.items()]
    summary_md = "### Extracted Fields\n" + "\n".join(summary_lines) if summary_lines else "_No fields extracted_"

    return dg.MaterializeResult(
        metadata={
            "fields_extracted": dg.MetadataValue.int(len(non_null_fields)),
            "extraction_summary": dg.MetadataValue.md(summary_md),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )


# ===========================================================================
# Asset 6: ai_financial_analysis  (group=ai_analysis)
# ===========================================================================


@dg.asset(group_name="ai_analysis", deps=["ai_structured_extraction", "sanitized_text"])
def ai_financial_analysis(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Use PydanticAI with DeepSeek to perform financial analysis on the extracted K-1 data.

    Combines the structured extraction with the sanitized text to generate
    a comprehensive financial analysis including income totals, tax planning
    recommendations, and key observations for a wealth management advisor.
    """
    from pydantic_ai import Agent

    # Load inputs
    structured_data = s3.read_json(s3.staging_key(config.run_id, "structured_k1.json"))
    sanitized_data = s3.read_json(s3.staging_key(config.run_id, "sanitized_text.json"))

    k1_json = json.dumps(structured_data["extracted_data"], indent=2)
    sanitized_text_content = sanitized_data["sanitized_text"]

    system_prompt = """You are a senior wealth management advisor and tax analyst.
You are given structured K-1 data (extracted from the form) and the raw sanitized text.
Provide a thorough financial analysis suitable for a wealth management client review.

For numerical fields (total_income, total_deductions, net_taxable_income, distribution_vs_income_ratio),
compute reasonable values from the available data. If data is insufficient, use 0.0.

For text fields, provide clear, professional analysis.

For key_observations, provide 3-5 specific observations about the K-1 data.
For tax_planning_recommendations, provide 3-5 actionable recommendations."""

    user_prompt = f"""Analyze the following K-1 data and provide a comprehensive financial analysis.

## Structured K-1 Data
{k1_json}

## Raw Sanitized Document Text
{sanitized_text_content}"""

    agent = Agent(
        "deepseek:deepseek-chat",
        output_type=FinancialAnalysis,
        system_prompt=system_prompt,
    )

    result = agent.run_sync(user_prompt)
    analysis: FinancialAnalysis = result.output
    analysis_dict = analysis.model_dump()

    # Serialize the full AI conversation for auditing
    ai_messages = _serialize_messages(result.all_messages())

    staging_payload = {
        "analysis": analysis_dict,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "ai_interaction": {
            "model": "deepseek:deepseek-chat",
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "output_schema": FinancialAnalysis.model_json_schema(),
            "raw_messages": ai_messages,
            "usage": result.usage().model_dump() if hasattr(result.usage(), "model_dump") else str(result.usage()),
        },
    }

    staging_key = s3.staging_key(config.run_id, "financial_analysis.json")
    s3.write_json(staging_key, staging_payload)

    # Key findings for metadata
    findings = []
    if analysis.net_taxable_income is not None:
        findings.append(f"Net taxable income: ${analysis.net_taxable_income:,.2f}")
    if analysis.key_observations:
        findings.append(f"Key observations: {len(analysis.key_observations)}")
    if analysis.tax_planning_recommendations:
        findings.append(f"Tax recommendations: {len(analysis.tax_planning_recommendations)}")
    findings_md = "\n".join(f"- {f}" for f in findings) if findings else "_Analysis pending_"

    return dg.MaterializeResult(
        metadata={
            "key_findings": dg.MetadataValue.md(findings_md),
            "observations_count": dg.MetadataValue.int(len(analysis.key_observations)),
            "recommendations_count": dg.MetadataValue.int(len(analysis.tax_planning_recommendations)),
            "staging_key": dg.MetadataValue.text(staging_key),
        }
    )


# ===========================================================================
# Asset 7: final_report  (group=output)
# ===========================================================================


@dg.asset(group_name="output", deps=["ai_structured_extraction", "ai_financial_analysis", "pii_detection_report"])
def final_report(config: K1RunConfig, s3: S3Storage) -> dg.MaterializeResult:
    """Combine all pipeline outputs into final deliverable reports.

    Produces three output files:
      - k1_report.json: Full comprehensive JSON report
      - k1_summary.csv: Flat CSV summary of key financial fields
      - pipeline_results.json: Everything the React frontend needs
    """
    # Load all staging data
    structured_data = s3.read_json(s3.staging_key(config.run_id, "structured_k1.json"))
    analysis_data = s3.read_json(s3.staging_key(config.run_id, "financial_analysis.json"))
    pii_report = s3.read_json(s3.staging_key(config.run_id, "pii_report.json"))
    pii_comparison = s3.read_json(s3.staging_key(config.run_id, "pii_comparison.json"))

    k1_data = structured_data["extracted_data"]
    analysis = analysis_data["analysis"]
    now = datetime.now(timezone.utc)

    # Create a unique output directory per run based on source PDF and timestamp
    raw_data = s3.read_json(s3.staging_key(config.run_id, "raw_pdf_bytes.json"))
    pdf_stem = Path(raw_data["file_name"]).stem
    run_dirname = f"{pdf_stem}_{now.strftime('%Y%m%d_%H%M%S')}"

    now_iso = now.isoformat()

    # ---- 1. Full JSON Report ----
    full_report = {
        "report_title": "K-1 Tax Document Processing Report",
        "generated_at": now_iso,
        "k1_extracted_data": k1_data,
        "financial_analysis": analysis,
        "pii_detection_summary": {
            "total_entities_detected": pii_report["total_entities"],
            "entity_breakdown": pii_report["entity_counts"],
        },
        "ai_interactions": {
            "extraction": structured_data.get("ai_interaction"),
            "analysis": analysis_data.get("ai_interaction"),
        },
        "processing_metadata": {
            "extraction_timestamp": structured_data.get("extracted_at"),
            "analysis_timestamp": analysis_data.get("analyzed_at"),
            "pii_scan_timestamp": pii_report.get("analyzed_at"),
        },
    }

    report_key = s3.output_key(run_dirname, "k1_report.json")
    s3.write_json(report_key, full_report)

    # ---- 2. CSV Summary ----
    csv_fields = [
        ("tax_year", k1_data.get("tax_year")),
        ("partnership_name", k1_data.get("partnership_name")),
        ("partner_type", k1_data.get("partner_type")),
        ("partner_share_percentage", k1_data.get("partner_share_percentage")),
        ("ordinary_business_income", k1_data.get("ordinary_business_income")),
        ("rental_real_estate_income", k1_data.get("rental_real_estate_income")),
        ("guaranteed_payments", k1_data.get("guaranteed_payments")),
        ("interest_income", k1_data.get("interest_income")),
        ("ordinary_dividends", k1_data.get("ordinary_dividends")),
        ("qualified_dividends", k1_data.get("qualified_dividends")),
        ("short_term_capital_gains", k1_data.get("short_term_capital_gains")),
        ("long_term_capital_gains", k1_data.get("long_term_capital_gains")),
        ("section_179_deduction", k1_data.get("section_179_deduction")),
        ("distributions", k1_data.get("distributions")),
        ("capital_account_beginning", k1_data.get("capital_account_beginning")),
        ("capital_account_ending", k1_data.get("capital_account_ending")),
        ("self_employment_earnings", k1_data.get("self_employment_earnings")),
        ("foreign_taxes_paid", k1_data.get("foreign_taxes_paid")),
        ("qbi_deduction", k1_data.get("qbi_deduction")),
        ("total_income", analysis.get("total_income")),
        ("total_deductions", analysis.get("total_deductions")),
        ("net_taxable_income", analysis.get("net_taxable_income")),
    ]

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["field", "value"])
    for field_name, field_value in csv_fields:
        writer.writerow([field_name, field_value if field_value is not None else ""])

    csv_key = s3.output_key(run_dirname, "k1_summary.csv")
    s3.write_text(csv_key, csv_buffer.getvalue(), content_type="text/csv")

    # ---- 3. Pipeline Results for React Frontend ----
    pipeline_results = {
        "pipeline_run": {
            "generated_at": now_iso,
            "status": "completed",
            "pipeline_version": "1.0.0",
        },
        "k1_data": k1_data,
        "financial_analysis": analysis,
        "pii_stats": {
            "total_entities_detected": pii_report["total_entities"],
            "entity_counts": pii_report["entity_counts"],
            "entities_redacted": pii_report["total_entities"],
        },
        "pii_comparison": {
            "presidio_only": {
                "total": pii_comparison["presidio_only"]["total_entities"],
                "counts": pii_comparison["presidio_only"]["entity_counts"],
                "entities": pii_comparison["presidio_only"]["entities"],
            },
            "gliner_only": {
                "total": pii_comparison["gliner_only"]["total_entities"],
                "counts": pii_comparison["gliner_only"]["entity_counts"],
                "entities": pii_comparison["gliner_only"]["entities"],
            },
            "combined": {
                "total": pii_comparison["presidio_plus_gliner"]["total_entities"],
                "counts": pii_comparison["presidio_plus_gliner"]["entity_counts"],
                "entities": pii_comparison["presidio_plus_gliner"]["entities"],
            },
        },
        "processing_metadata": {
            "ingestion_timestamp": structured_data.get("extracted_at"),
            "extraction_timestamp": structured_data.get("extracted_at"),
            "analysis_timestamp": analysis_data.get("analyzed_at"),
            "pii_scan_timestamp": pii_report.get("analyzed_at"),
            "report_generated_at": now_iso,
        },
        "output_files": {
            "full_report": report_key,
            "csv_summary": csv_key,
        },
    }

    pipeline_results_key = s3.output_key(run_dirname, "pipeline_results.json")
    s3.write_json(pipeline_results_key, pipeline_results)

    # ---- 4. PDF Report (WeasyPrint) ----
    from k1_pipeline.defs.pdf_templates import render_single_report_html, generate_pdf

    pdf_html = render_single_report_html(
        k1_data=k1_data,
        analysis=analysis,
        pii_stats={
            "total_entities_detected": pii_report["total_entities"],
            "entities_redacted": pii_report["total_entities"],
        },
        metadata={"report_generated_at": now_iso},
    )

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        generate_pdf(pdf_html, Path(tmp_pdf.name))
        pdf_key = s3.output_key(run_dirname, "k1_report.pdf")
        s3.upload_from_file(tmp_pdf.name, pdf_key, content_type="application/pdf")

    pipeline_results["output_files"]["pdf_report"] = pdf_key
    s3.write_json(pipeline_results_key, pipeline_results)

    return dg.MaterializeResult(
        metadata={
            "report_key": dg.MetadataValue.text(report_key),
            "csv_key": dg.MetadataValue.text(csv_key),
            "pipeline_results_key": dg.MetadataValue.text(pipeline_results_key),
            "k1_fields_populated": dg.MetadataValue.int(
                sum(1 for v in k1_data.values() if v is not None)
            ),
            "observations": dg.MetadataValue.int(len(analysis.get("key_observations", []))),
            "recommendations": dg.MetadataValue.int(
                len(analysis.get("tax_planning_recommendations", []))
            ),
            "pdf_report_key": dg.MetadataValue.text(pdf_key),
        }
    )
