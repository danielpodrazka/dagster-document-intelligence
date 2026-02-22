#!/usr/bin/env python3
"""OCR stress test: benchmark Surya vs Tesseract accuracy across scan degradation levels.

Generates progressively degraded versions of a filled K-1 PDF and measures
how many ground-truth field values each OCR engine can recover at each level.
Saves degraded page images locally for visual inspection.

Usage:
    cd pipeline
    uv run python scripts/ocr_stress_test.py                    # uses S3
    uv run python scripts/ocr_stress_test.py path/to/filled.pdf # local file

Degraded images are saved to: pipeline/data/output/ocr_stress_test/
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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "ocr_stress_test"

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


def run_tesseract_ocr(images: list[Image.Image]) -> str:
    """Run Tesseract OCR on a list of PIL images, return concatenated text."""
    import pytesseract

    parts = []
    for img in images:
        text = pytesseract.image_to_string(img)
        parts.append(text)
    return "\n".join(parts)


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


def save_degraded_images(
    degraded: list[Image.Image], profile_name: str,
) -> list[str]:
    """Save degraded images to local output dir for visual inspection."""
    profile_dir = OUTPUT_DIR / profile_name
    profile_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, img in enumerate(degraded):
        path = profile_dir / f"page_{i + 1}.png"
        img.save(str(path))
        paths.append(str(path))
    return paths


def print_results_table(
    engine_scores: dict[str, dict[str, dict[str, bool]]],
    engine_timings: dict[str, dict[str, float]],
) -> None:
    """Print a formatted comparison table for all engines."""
    engines = list(engine_scores.keys())
    profile_names = list(next(iter(engine_scores.values())).keys())
    field_names = list(GROUND_TRUTH.keys())

    # Build composite column names: "clean/S" "clean/T" etc
    engine_abbrevs = {e: e[0].upper() for e in engines}
    # Handle collision (unlikely but safe)
    if len(set(engine_abbrevs.values())) < len(engines):
        engine_abbrevs = {e: e[:3].capitalize() for e in engines}

    col_headers = []
    for pname in profile_names:
        for engine in engines:
            col_headers.append(f"{pname}/{engine_abbrevs[engine]}")

    label_w = max(len(f) for f in field_names) + 2
    col_w = max(max(len(h) for h in col_headers) + 2, 8)

    total_w = label_w + col_w * len(col_headers)

    print(f"\nOCR Stress Test — Surya (S) vs Tesseract (T)")
    print("=" * total_w)

    # Header
    header = "Field".ljust(label_w) + "".join(h.center(col_w) for h in col_headers)
    print(header)
    print("-" * total_w)

    # Rows
    for field in field_names:
        row = field.ljust(label_w)
        for pname in profile_names:
            for engine in engines:
                found = engine_scores[engine][pname][field]
                marker = "\u2713" if found else "\u2717"
                row += marker.center(col_w)
        print(row)

    print("-" * total_w)

    # Score row
    total = len(field_names)
    score_row = "SCORE".ljust(label_w)
    for pname in profile_names:
        for engine in engines:
            hit = sum(1 for v in engine_scores[engine][pname].values() if v)
            score_row += f"{hit}/{total}".center(col_w)
    print(score_row)

    # Time row
    time_row = "TIME".ljust(label_w)
    for pname in profile_names:
        for engine in engines:
            time_row += f"{engine_timings[engine][pname]:.1f}s".center(col_w)
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

    surya_scores: dict[str, dict[str, bool]] = {}
    surya_timings: dict[str, float] = {}
    tess_scores: dict[str, dict[str, bool]] = {}
    tess_timings: dict[str, float] = {}
    profile_details: list[dict] = []

    for profile in PROFILES:
        print(f"\n--- {profile.name} ---")
        rng = random.Random(42)

        # Degrade each page
        degraded = [degrade_image(img.copy(), profile, rng) for img in base_images]

        # Save degraded images for visual inspection
        saved = save_degraded_images(degraded, profile.name)
        print(f"  Saved {len(saved)} image(s) to {OUTPUT_DIR / profile.name}/")

        # Surya OCR
        t0 = time.perf_counter()
        surya_text = run_surya_ocr(degraded, det_predictor, rec_predictor)
        surya_elapsed = time.perf_counter() - t0
        surya_result = score_ocr(surya_text, GROUND_TRUTH)
        surya_hit = sum(1 for v in surya_result.values() if v)

        # Tesseract OCR
        t0 = time.perf_counter()
        tess_text = run_tesseract_ocr(degraded)
        tess_elapsed = time.perf_counter() - t0
        tess_result = score_ocr(tess_text, GROUND_TRUTH)
        tess_hit = sum(1 for v in tess_result.values() if v)

        total = len(GROUND_TRUTH)
        print(f"  Surya:     {surya_hit}/{total}  ({surya_elapsed:.1f}s)")
        print(f"  Tesseract: {tess_hit}/{total}  ({tess_elapsed:.1f}s)")

        surya_scores[profile.name] = surya_result
        surya_timings[profile.name] = surya_elapsed
        tess_scores[profile.name] = tess_result
        tess_timings[profile.name] = tess_elapsed

        profile_details.append({
            "profile": asdict(profile),
            "surya": {
                "scores": surya_result,
                "hit_count": surya_hit,
                "total_fields": total,
                "accuracy": surya_hit / total if total else 0,
                "ocr_time_seconds": round(surya_elapsed, 2),
                "ocr_text_length": len(surya_text),
            },
            "tesseract": {
                "scores": tess_result,
                "hit_count": tess_hit,
                "total_fields": total,
                "accuracy": tess_hit / total if total else 0,
                "ocr_time_seconds": round(tess_elapsed, 2),
                "ocr_text_length": len(tess_text),
            },
            "saved_images": saved,
        })

    # Print summary table
    print_results_table(
        {"surya": surya_scores, "tesseract": tess_scores},
        {"surya": surya_timings, "tesseract": tess_timings},
    )

    print(f"Degraded images saved to: {OUTPUT_DIR}/")

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
