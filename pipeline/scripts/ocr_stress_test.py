#!/usr/bin/env python3
"""OCR stress test: benchmark Surya accuracy across scan degradation levels.

Generates progressively degraded versions of a filled K-1 PDF and measures
how many ground-truth field values Surya OCR can recover at each level.

Usage:
    cd pipeline
    uv run python scripts/ocr_stress_test.py                    # uses S3
    uv run python scripts/ocr_stress_test.py path/to/filled.pdf # local file
"""

from __future__ import annotations

import io
import json
import random
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from pdf2image import convert_from_path
from PIL import Image, ImageEnhance, ImageFilter

# Ground truth: exact values placed into the PDF by irs_k1_form_fill
GROUND_TRUTH = {
    "EIN": "82-4571903",
    "SSN": "478-93-6215",
    "Box 1 (Ordinary income)": "127,450",
    "Box 2 (Rental)": "18,200",
    "Box 4a (Guaranteed payments)": "45,000",
    "Box 5 (Interest)": "8,325",
    "Box 6a (Dividends)": "12,780",
    "Box 6b (Qualified dividends)": "9,150",
    "Box 8 (ST capital gain)": "3,400",
    "Box 9a (LT capital gain)": "67,890",
    "Box 12 (Section 179)": "4,200",
    "Box 14 (SE earnings)": "172,450",
    "Box 19 (Distributions)": "95,000",
    "Box 20 Code Z (QBI)": "127,450",
    "Capital begin": "542,100",
    "Capital end": "741,245",
    "Partner share %": "3.75",
    "Partnership name": "Meridian Capital Growth Fund",
    "City": "Greenwich",
}


@dataclass
class DegradationProfile:
    name: str
    rotation: float  # max degrees (±)
    blur_radius: float
    noise_amplitude: int  # pixel intensity ±
    contrast: float  # 1.0 = unchanged
    jpeg_quality: int


PROFILES = [
    DegradationProfile("clean", 0, 0, 0, 1.0, 95),
    DegradationProfile("mild", 0.5, 0.5, 5, 0.95, 70),
    DegradationProfile("moderate", 1.5, 1.0, 10, 0.85, 50),
    DegradationProfile("heavy", 3.0, 1.5, 20, 0.75, 35),
    DegradationProfile("extreme", 5.0, 2.5, 35, 0.6, 20),
]


def degrade_image(img: Image.Image, profile: DegradationProfile, rng: random.Random) -> Image.Image:
    """Apply degradation effects to a PIL Image, parameterized by profile."""
    # 1. Rotation / skew
    if profile.rotation > 0:
        angle = rng.uniform(-profile.rotation, profile.rotation)
        img = img.rotate(angle, resample=Image.Resampling.BICUBIC, expand=False, fillcolor=(255, 255, 255))

    # 2. Gaussian blur
    if profile.blur_radius > 0:
        img = img.filter(ImageFilter.GaussianBlur(radius=profile.blur_radius))

    # 3. Additive noise
    if profile.noise_amplitude > 0:
        arr = np.array(img, dtype=np.int16)
        noise = np.random.default_rng(42).integers(
            -profile.noise_amplitude, profile.noise_amplitude + 1, size=arr.shape, dtype=np.int16
        )
        arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
        img = Image.fromarray(arr)

    # 4. Contrast reduction
    if profile.contrast != 1.0:
        img = ImageEnhance.Contrast(img).enhance(profile.contrast)

    # 5. JPEG compression artifacts
    jpeg_buf = io.BytesIO()
    img.save(jpeg_buf, format="JPEG", quality=profile.jpeg_quality)
    jpeg_buf.seek(0)
    img = Image.open(jpeg_buf).convert("RGB")

    return img


def run_surya_ocr(images: list[Image.Image], det_predictor, rec_predictor) -> str:
    """Run Surya OCR on a list of PIL images, return concatenated text."""
    predictions = rec_predictor(images, det_predictor=det_predictor)
    return "\n".join(
        "\n".join(line.text for line in page.text_lines)
        for page in predictions
    )


def score_ocr(text: str, ground_truth: dict[str, str]) -> dict[str, bool]:
    """Check which ground-truth values appear in OCR text."""
    return {label: value in text for label, value in ground_truth.items()}


def get_pdf_path() -> str:
    """Resolve the filled PDF: CLI arg, or download from S3."""
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if not Path(path).exists():
            print(f"Error: file not found: {path}")
            sys.exit(1)
        return path

    # Fall back to S3 (LocalStack)
    print("Downloading filled K-1 PDF from S3...")
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    bucket = "dagster-document-intelligence-etl"
    key = "input/irs_k1_filled.pdf"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    s3.download_fileobj(bucket, key, tmp)
    tmp.close()
    print(f"  Downloaded s3://{bucket}/{key}")
    return tmp.name


def print_results_table(
    all_scores: dict[str, dict[str, bool]],
    timings: dict[str, float],
) -> None:
    """Print a formatted results table to stdout."""
    profile_names = list(all_scores.keys())
    field_names = list(GROUND_TRUTH.keys())

    # Column widths
    label_w = max(len(f) for f in field_names) + 2
    col_w = max(max(len(n) for n in profile_names) + 2, 10)

    try:
        import surya
        version = getattr(surya, "__version__", "unknown")
    except Exception:
        version = "unknown"

    print(f"\nOCR Stress Test Results — Surya v{version}")
    print("=" * (label_w + col_w * len(profile_names)))

    # Header
    header = "Field".ljust(label_w) + "".join(n.center(col_w) for n in profile_names)
    print(header)
    print("-" * (label_w + col_w * len(profile_names)))

    # Rows
    for field in field_names:
        row = field.ljust(label_w)
        for pname in profile_names:
            found = all_scores[pname][field]
            marker = "\u2713" if found else "\u2717"
            row += marker.center(col_w)
        print(row)

    print("-" * (label_w + col_w * len(profile_names)))

    # Score row
    total = len(field_names)
    score_row = "SCORE".ljust(label_w)
    for pname in profile_names:
        hit = sum(1 for v in all_scores[pname].values() if v)
        score_row += f"{hit}/{total}".center(col_w)
    print(score_row)

    # Time row
    time_row = "TIME".ljust(label_w)
    for pname in profile_names:
        time_row += f"{timings[pname]:.1f}s".center(col_w)
    print(time_row)

    print()


def upload_report(report: dict) -> None:
    """Write JSON report to S3."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    bucket = "dagster-document-intelligence-etl"
    key = "output/ocr_stress_test.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2).encode(),
        ContentType="application/json",
    )
    print(f"Report written to s3://{bucket}/{key}")


def main() -> None:
    pdf_path = get_pdf_path()

    # Convert PDF to images once
    print("Converting PDF to images (300 DPI)...")
    base_images = convert_from_path(pdf_path, dpi=300)
    print(f"  {len(base_images)} page(s)")

    # Load Surya models once (expensive)
    print("Loading Surya OCR models...")
    from surya.detection import DetectionPredictor
    from surya.foundation import FoundationPredictor
    from surya.recognition import RecognitionPredictor

    foundation = FoundationPredictor()
    det_predictor = DetectionPredictor()
    rec_predictor = RecognitionPredictor(foundation)
    print("  Models loaded")

    all_scores: dict[str, dict[str, bool]] = {}
    timings: dict[str, float] = {}
    profile_details: list[dict] = []

    for profile in PROFILES:
        print(f"\n--- {profile.name} ---")
        rng = random.Random(42)

        # Degrade each page
        degraded = [degrade_image(img.copy(), profile, rng) for img in base_images]

        # OCR
        t0 = time.perf_counter()
        ocr_text = run_surya_ocr(degraded, det_predictor, rec_predictor)
        elapsed = time.perf_counter() - t0

        # Score
        scores = score_ocr(ocr_text, GROUND_TRUTH)
        hit = sum(1 for v in scores.values() if v)
        total = len(scores)
        print(f"  Score: {hit}/{total}  ({elapsed:.1f}s)")

        all_scores[profile.name] = scores
        timings[profile.name] = elapsed
        profile_details.append({
            "profile": asdict(profile),
            "scores": scores,
            "hit_count": hit,
            "total_fields": total,
            "accuracy": hit / total if total else 0,
            "ocr_time_seconds": round(elapsed, 2),
            "ocr_text_length": len(ocr_text),
        })

    # Print summary table
    print_results_table(all_scores, timings)

    # Build and upload JSON report
    report = {
        "test": "ocr_stress_test",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ground_truth": GROUND_TRUTH,
        "profiles": profile_details,
    }
    upload_report(report)


if __name__ == "__main__":
    main()
