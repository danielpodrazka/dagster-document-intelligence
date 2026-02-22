# Future Enhancements

## Qwen2.5-VL: Unified Vision-Language OCR + Extraction

[Qwen2.5-VL](https://huggingface.co/Qwen/Qwen2.5-VL-3B-Instruct) is a vision-language model that could replace both the OCR and structured extraction steps in a single pass. Instead of OCR → text → LLM extraction, the model reads the PDF page image directly and outputs structured data.

**Why consider it:**
- Eliminates OCR errors propagating to extraction — the model "sees" the form
- Handles tables, checkboxes, and complex layouts natively
- The 3B parameter variant runs on a single GPU (~6 GB VRAM)

**Trade-offs:**
- Requires a GPU (not CPU-feasible at interactive speeds)
- Larger model footprint vs. current Surya OCR + DeepSeek API approach
- Less auditable intermediate output (no separate OCR text to inspect)

**When to adopt:**
- When the pipeline moves to GPU-equipped infrastructure
- When OCR accuracy on degraded scans becomes a bottleneck that Surya alone can't solve
