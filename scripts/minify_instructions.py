"""Minify markdown agent instructions to reduce token count while preserving semantics.

Applies a configurable pipeline of rule-based transformations that are safe for
LLM consumption. Each pass is independently toggleable and reports its own savings.

Usage:
    from scripts.minify_instructions import minify, minify_file, minify_directory

    # Single string
    text = minify(raw_markdown)

    # Single file
    text = minify_file(Path("docs/validation/01_arithmetic_rules.md"))

    # All .md files in a directory, concatenated
    text = minify_directory(Path("docs/validation"))

    # With stats
    result = minify(raw_markdown, stats=True)
    print(result.text, result.original_len, result.minified_len, result.ratio)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class MinifyResult:
    text: str
    original_len: int
    minified_len: int
    pass_stats: list[PassStat] = field(default_factory=list)

    @property
    def ratio(self) -> float:
        """Compression ratio -- 0.70 means 30% smaller."""
        if self.original_len == 0:
            return 1.0
        return self.minified_len / self.original_len

    @property
    def savings_pct(self) -> float:
        return (1 - self.ratio) * 100

    def summary(self) -> str:
        lines = [
            f"Original:  {self.original_len:>8,} chars",
            f"Minified:  {self.minified_len:>8,} chars",
            f"Savings:   {self.savings_pct:>7.1f}%",
            "",
            "Per-pass breakdown:",
        ]
        for ps in self.pass_stats:
            lines.append(f"  {ps.name:<40s}  -{ps.savings_pct:5.1f}%  ({ps.chars_removed:>6,} chars)")
        return "\n".join(lines)


@dataclass
class PassStat:
    name: str
    before: int
    after: int

    @property
    def chars_removed(self) -> int:
        return self.before - self.after

    @property
    def savings_pct(self) -> float:
        if self.before == 0:
            return 0.0
        return (self.chars_removed / self.before) * 100


# ---------------------------------------------------------------------------
# Individual minification passes (ordered roughly by safety / impact)
# ---------------------------------------------------------------------------

# ---- Tier 1: Zero-risk whitespace and formatting ----

def collapse_blank_lines(text: str) -> str:
    """Collapse 2+ consecutive blank lines into a single blank line."""
    return re.sub(r"\n{3,}", "\n\n", text)


def strip_trailing_whitespace(text: str) -> str:
    """Remove trailing spaces/tabs from every line."""
    return re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE)


def remove_horizontal_rules(text: str) -> str:
    """Remove decorative markdown horizontal rules (---, ***, ___) and surrounding blank lines."""
    text = re.sub(r"\n*^-{3,}\s*$\n*", "\n", text, flags=re.MULTILINE)
    text = re.sub(r"\n*^\*{3,}\s*$\n*", "\n", text, flags=re.MULTILINE)
    text = re.sub(r"\n*^_{3,}\s*$\n*", "\n", text, flags=re.MULTILINE)
    return text


def compact_bold_markers(text: str) -> str:
    """Strip bold markers from structured field labels (e.g. **Description**: -> Description:).

    Preserves bold in headings and prose. Targets the recurring pattern of
    **Label**: used as field keys in structured rule documentation.
    """
    return re.sub(r"\*\*([A-Za-z][A-Za-z /'-]+)\*\*(\s*:)", r"\1\2", text)


def strip_all_bold(text: str) -> str:
    """Remove all **bold** markers, keeping the text inside."""
    return re.sub(r"\*\*([^*]+)\*\*", r"\1", text)


# ---- Tier 1: Table compaction ----

def compact_tables(text: str) -> str:
    """Remove excess padding from markdown table cells."""
    def _compact_row(match: re.Match[str]) -> str:
        row = match.group(0)
        cells = row.split("|")
        cells = [c.strip() for c in cells]
        return "|".join(cells)

    return re.sub(r"^\|.*\|$", _compact_row, text, flags=re.MULTILINE)


def simplify_separator_rows(text: str) -> str:
    """Simplify table separator rows to minimal dashes (|---|---|---|)."""
    def _simplify(match: re.Match[str]) -> str:
        row = match.group(0)
        cells = row.split("|")
        simplified = []
        for cell in cells:
            stripped = cell.strip()
            if re.fullmatch(r":?-{2,}:?", stripped):
                prefix = ":" if stripped.startswith(":") else ""
                suffix = ":" if stripped.endswith(":") else ""
                simplified.append(f"{prefix}---{suffix}")
            else:
                simplified.append(stripped)
        return "|".join(simplified)

    return re.sub(r"^\|[\s|:-]+\|$", _simplify, text, flags=re.MULTILINE)


def compress_example_tables(text: str) -> str:
    """Compress example tables (with Valid? column) into compact inline notation.

    | 10000 | 8000 | Yes | 6b<6a |  -->  Valid: 10000,8000 (6b<6a)
    | 5000  | 7000 | No  | 6b>6a |  -->  Invalid: 5000,7000 (6b>6a)
    """
    def _compress_block(match: re.Match[str]) -> str:
        block = match.group(0)
        lines = block.strip().splitlines()
        if len(lines) < 3:
            return block

        header_cells = [c.strip() for c in lines[0].split("|") if c.strip()]
        valid_col = reason_col = -1
        for i, h in enumerate(header_cells):
            if h.lower() in ("valid?", "valid"):
                valid_col = i
            if h.lower() == "reason":
                reason_col = i
        if valid_col == -1:
            return block

        data_cols = [i for i in range(len(header_cells)) if i not in (valid_col, reason_col)]
        valid_entries: list[str] = []
        invalid_entries: list[str] = []

        for line in lines[2:]:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) <= valid_col:
                continue
            is_valid = cells[valid_col].lower() in ("yes", "true", "valid")
            values = ",".join(cells[i] for i in data_cols if i < len(cells))
            reason = cells[reason_col].strip() if reason_col != -1 and reason_col < len(cells) else ""
            entry = f"{values} ({reason})" if reason else values
            (valid_entries if is_valid else invalid_entries).append(entry)

        parts: list[str] = []
        if valid_entries:
            parts.append("Valid: " + "; ".join(valid_entries))
        if invalid_entries:
            parts.append("Invalid: " + "; ".join(invalid_entries))
        return "\n".join(parts) + "\n"

    table_pattern = re.compile(
        r"^(\|[^\n]*[Vv]alid[?\s]*\|[^\n]*\n)"
        r"(\|[\s|:-]+\|\s*\n)"
        r"((?:\|[^\n]*\n)+)",
        re.MULTILINE,
    )
    return table_pattern.sub(_compress_block, text)


# ---- Tier 1: Phrase and filler word compression ----

def replace_verbose_phrases(text: str) -> str:
    """Replace common verbose phrases with concise equivalents."""
    replacements = [
        (r"\bin order to\b", "to"),
        (r"\bdue to the fact that\b", "because"),
        (r"\bat this point in time\b", "now"),
        (r"\bfor the purpose of\b", "for"),
        (r"\bin the event that\b", "if"),
        (r"\bwith the exception of\b", "except"),
        (r"\bprior to\b", "before"),
        (r"\bsubsequent to\b", "after"),
        (r"\bin the case of\b", "for"),
        (r"\bit is important to note that\b", "note:"),
        (r"\bit should be noted that\b", "note:"),
        (r"\bas a result of\b", "because of"),
        (r"\bin conjunction with\b", "with"),
        (r"\bwith respect to\b", "regarding"),
        (r"\bin accordance with\b", "per"),
        (r"\bwhether or not\b", "whether"),
        (r"\bas well as\b", "and"),
        (r"\bin addition to\b", "besides"),
        (r"\ba large number of\b", "many"),
        (r"\ba significant number of\b", "many"),
        (r"\bin most cases\b", "usually"),
        (r"\bin some cases\b", "sometimes"),
        (r"\bis a subset of\b", "is subset of"),
        (r"\bfor the same reason\b", "similarly"),
        (r"\bin other words\b", "i.e."),
        (r"\bfor example\b", "e.g."),
        (r"\bthat is to say\b", "i.e."),
        (r"\bon the other hand\b", "conversely"),
        (r"\bhas the ability to\b", "can"),
        (r"\bin the context of\b", "in"),
        (r"\bthe fact that\b", "that"),
    ]
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def remove_filler_words(text: str) -> str:
    """Remove filler/hedge words that add no semantic value for an LLM."""
    fillers = [
        r"\bessentially\b",
        r"\bbasically\b",
        r"\bgenerally speaking\b",
        r"\bin reality\b",
        r"\bof course\b",
        r"\bvery\b",
    ]
    for filler in fillers:
        text = re.sub(filler + r",?\s*", "", text, flags=re.IGNORECASE)
    return text


# ---- Tier 2: Code block compression ----

def strip_code_comments(text: str) -> str:
    """Remove comment-only lines and inline comments from code blocks."""
    in_code_block = False
    lines: list[str] = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            lines.append(line)
            continue

        if in_code_block:
            # Skip comment-only lines
            if stripped.startswith("#") and not stripped.startswith("#!"):
                continue
            # Strip inline comments (after two spaces + #)
            if "  #" in line:
                code_part, _, _ = line.partition("  #")
                code_trimmed = code_part.rstrip()
                if code_trimmed:
                    lines.append(code_trimmed + "\n")
                    continue
        lines.append(line)
    return "".join(lines)


def reduce_code_indentation(text: str) -> str:
    """Reduce 4-space indentation to 2-space inside code blocks.

    Saves ~1 char per indent level per line across thousands of indented lines.
    """
    in_code_block = False
    lines: list[str] = []
    for line in text.splitlines(keepends=True):
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            lines.append(line)
            continue

        if in_code_block:
            # Count leading groups of 4 spaces and replace with 2
            match = re.match(r"^( +)", line)
            if match:
                spaces = match.group(1)
                indent_level = len(spaces) // 4
                remainder = len(spaces) % 4
                new_indent = "  " * indent_level + " " * remainder
                lines.append(new_indent + line[len(spaces):])
                continue
        lines.append(line)
    return "".join(lines)


def compact_none_checks(text: str) -> str:
    """Shorten `x is not None` to `x!=None` and `x is None` to `x==None` in code blocks.

    LLMs understand both forms equally well. Saves ~5 chars per occurrence.
    """
    in_code_block = False
    lines: list[str] = []
    for line in text.splitlines(keepends=True):
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            lines.append(line)
            continue

        if in_code_block:
            line = line.replace(" is not None", "!=None")
            line = line.replace(" is None", "==None")
        lines.append(line)
    return "".join(lines)


def collapse_multiline_ifs(text: str) -> str:
    """Collapse multi-line if conditions onto fewer lines inside code blocks.

    Turns:
        if (
            data.x is not None
            and data.y is not None
        ):
    Into:
        if (data.x is not None and data.y is not None):
    """
    in_code_block = False
    result_lines: list[str] = []
    i = 0
    lines = text.splitlines(keepends=True)

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block
            result_lines.append(line)
            i += 1
            continue

        if in_code_block and stripped in ("if (", "elif ("):
            # Collect continuation lines until we hit "):"
            indent = len(line) - len(line.lstrip())
            keyword = stripped.rstrip(" (")
            condition_parts: list[str] = []
            i += 1
            while i < len(lines):
                cline = lines[i].strip()
                if cline == "):":
                    break
                condition_parts.append(cline)
                i += 1
            joined = " ".join(condition_parts)
            result_lines.append(" " * indent + f"{keyword} ({joined}):\n")
            i += 1
            continue

        result_lines.append(line)
        i += 1

    return "".join(result_lines)


def remove_code_block_fences(text: str) -> str:
    """Remove code block fences (``` markers), keeping content as-is.

    For agent context, the LLM understands code without explicit fences.
    Indentation already signals code structure.
    """
    return re.sub(r"^```[a-z]*\s*$\n?", "", text, flags=re.MULTILINE)


# ---- Tier 2: Structural compression ----

def flatten_heading_depth(text: str) -> str:
    """Reduce heading depth by one level (#### -> ###, ### -> ##).

    Fewer # chars, and agents don't need deep heading hierarchies.
    Stops at ## (doesn't promote to #).
    """
    # Process from deepest to shallowest to avoid double-promotion
    text = re.sub(r"^#{5,6}\s+", "### ", text, flags=re.MULTILINE)
    text = re.sub(r"^####\s+", "### ", text, flags=re.MULTILINE)
    text = re.sub(r"^###\s+", "## ", text, flags=re.MULTILINE)
    return text


def abbreviate_irs_references(text: str) -> str:
    """Shorten frequently repeated IRS reference names.

    "Partner's Instructions for Schedule K-1 (Form 1065)" -> "K-1 Partner Instructions"
    "Instructions for Form 1065"                          -> "Form 1065 Instructions"
    """
    replacements = [
        ("Partner's Instructions for Schedule K-1 (Form 1065)", "K-1 Partner Instructions"),
        ("Instructions for Form 1065", "Form 1065 Instructions"),
        ("IRS Form 1065 Schedule K-1", "Form 1065 K-1"),
        ("Schedule K-1 (Form 1065)", "K-1 (1065)"),
        ("IRS Modernized e-File (MeF)", "IRS MeF"),
        ("Internal Revenue Code", "IRC"),
    ]
    for long, short in replacements:
        text = text.replace(long, short)
    return text


def deduplicate_severity_definitions(text: str) -> str:
    """Remove repeated severity classification blocks after the first occurrence."""
    severity_block = re.compile(
        r"(?:Each rule is classified by severity:\n\n)?"
        r"- \*?\*?Critical\*?\*? --[^\n]+\n"
        r"- \*?\*?Warning\*?\*? --[^\n]+\n"
        r"- \*?\*?Advisory\*?\*? --[^\n]+\n",
        re.IGNORECASE,
    )
    matches = list(severity_block.finditer(text))
    if len(matches) <= 1:
        return text
    for m in reversed(matches[1:]):
        text = text[:m.start()] + text[m.end():]
    return text


def strip_link_urls(text: str) -> str:
    """Convert markdown links [text](url) to plain text, removing the URL."""
    return re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)


def remove_list_item_descriptions(text: str) -> str:
    """Shorten wordy list item descriptions that follow a dash-bold pattern.

    Turns:
        - **Cross-field validation** -- relationships between fields that must hold
    Into:
        - Cross-field validation
    """
    return re.sub(
        r"^(\s*[-*]\s+)\*\*([^*]+)\*\*\s*--\s*[^\n]+$",
        r"\1\2",
        text,
        flags=re.MULTILINE,
    )


def strip_irs_reference_tables(text: str) -> str:
    """Remove IRS References sections entirely.

    These are citation metadata (URLs, publication names) that an LLM agent
    does not need to execute validation logic. The IRS Basis field on each
    rule already provides the specific citation needed.
    """
    return re.sub(
        r"^#{1,3}\s*(?:\d+\.\s*)?IRS References\s*\n.*?(?=^#{1,3}\s|\Z)",
        "",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )


def strip_note_blocks(text: str) -> str:
    """Remove Note/Notes paragraphs at the end of rules.

    These provide context for human readers (e.g. 'Tax software like TurboTax
    validates this...') but do not add actionable information for the agent.
    """
    # Match Note/Notes: followed by text until the next rule heading or section
    return re.sub(
        r"^(?:[-*]\s*)?Notes?:\s*.*?(?=\n(?:#{1,4}\s|[-*]\s\*\*|$))",
        "",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )


def compact_rule_structure(text: str) -> str:
    """Compact the repeated rule field labels into a denser format.

    Converts:
        **Description**: Box 6b is a subset of Box 6a...
        **IRS Basis**: Partner's Instructions...
        **Fields Involved**:
        - field_a
        - field_b
        **Validation Logic**:
        ...
        **Severity**: Critical
    Into:
        Box 6b is a subset of Box 6a...
        IRS: Partner's Instructions...
        Fields: field_a, field_b
        Severity: Critical
    """
    # Shorten label names (handles both `- **Label**:` and standalone `**Label**:`)
    text = re.sub(r"^(\s*(?:[-*]\s*)?)IRS Basis:", r"\1IRS:", text, flags=re.MULTILINE)
    text = re.sub(r"^(\s*(?:[-*]\s*)?)Fields Involved:", r"\1Fields:", text, flags=re.MULTILINE)
    text = re.sub(r"^(\s*(?:[-*]\s*)?)Validation Logic:", r"\1Logic:", text, flags=re.MULTILINE)

    # Collapse multi-line field lists onto one line:
    # "Fields:\n- `field_a` (Box 1)\n- `field_b` (Box 2)" -> "Fields: field_a (Box 1), field_b (Box 2)"
    def _collapse_field_list(match: re.Match[str]) -> str:
        block = match.group(0)
        items = re.findall(r"`(\w+)`([^`\n]*)", block)
        if items:
            parts = [f"{name}{ann.rstrip()}" for name, ann in items]
            prefix = match.group(1) or ""
            return f"{prefix}Fields: {', '.join(parts)}\n"
        return block

    text = re.sub(
        r"^(\s*(?:[-*]\s*)?)Fields:\s*\n(?:\s*[-*]\s*`\w+`[^\n]*\n)+",
        _collapse_field_list,
        text,
        flags=re.MULTILINE,
    )

    return text


def remove_overview_prose(text: str) -> str:
    """Trim verbose overview sections to just the first paragraph.

    Overview sections in these docs repeat context about the K-1 form
    that the agent already knows from the rule definitions themselves.
    """
    def _trim_overview(match: re.Match[str]) -> str:
        heading = match.group(1)
        body = match.group(2)
        # Keep the heading + first paragraph only
        paragraphs = re.split(r"\n\n+", body.strip(), maxsplit=1)
        if paragraphs:
            return heading + "\n" + paragraphs[0] + "\n\n"
        return heading + "\n\n"

    return re.sub(
        r"(^#{1,3}\s*(?:\d+\.\s*)?Overview\s*\n)(.*?)(?=^#{1,3}\s)",
        _trim_overview,
        text,
        flags=re.MULTILINE | re.DOTALL,
    )


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------

# Each pass: (name, function, default_enabled)
DEFAULT_PASSES: list[tuple[str, Callable[[str], str], bool]] = [
    # -- Whitespace and formatting --
    ("collapse_blank_lines", collapse_blank_lines, True),
    ("strip_trailing_whitespace", strip_trailing_whitespace, True),
    ("remove_horizontal_rules", remove_horizontal_rules, True),
    ("compact_bold_markers", compact_bold_markers, True),
    # -- Tables --
    ("compact_tables", compact_tables, True),
    ("simplify_separator_rows", simplify_separator_rows, True),
    ("compress_example_tables", compress_example_tables, True),
    # -- Phrases and filler --
    ("replace_verbose_phrases", replace_verbose_phrases, True),
    ("remove_filler_words", remove_filler_words, True),
    # -- Code blocks --
    ("strip_code_comments", strip_code_comments, True),
    ("reduce_code_indentation", reduce_code_indentation, True),
    ("compact_none_checks", compact_none_checks, True),
    ("collapse_multiline_ifs", collapse_multiline_ifs, True),
    # -- Structural --
    ("abbreviate_irs_references", abbreviate_irs_references, True),
    ("deduplicate_severity_definitions", deduplicate_severity_definitions, True),
    ("strip_link_urls", strip_link_urls, True),
    ("remove_list_item_descriptions", remove_list_item_descriptions, True),
    ("compact_rule_structure", compact_rule_structure, True),
]

# More aggressive passes -- higher token savings but some semantic risk.
# Enable selectively via minify(enable_aggressive=True) or by name.
AGGRESSIVE_PASSES: list[tuple[str, Callable[[str], str], bool]] = [
    ("strip_irs_reference_tables", strip_irs_reference_tables, True),
    ("strip_note_blocks", strip_note_blocks, True),
    ("remove_overview_prose", remove_overview_prose, True),
    ("strip_all_bold", strip_all_bold, True),
    ("flatten_heading_depth", flatten_heading_depth, True),
    ("remove_code_block_fences", remove_code_block_fences, True),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def minify(
    text: str,
    *,
    stats: bool = False,
    passes: list[tuple[str, Callable[[str], str], bool]] | None = None,
    disable: set[str] | None = None,
    enable_only: set[str] | None = None,
    enable_aggressive: bool = False,
) -> str | MinifyResult:
    """Minify markdown instruction text through a pipeline of transformations.

    Args:
        text: Raw markdown text.
        stats: If True, return a MinifyResult with per-pass statistics.
        passes: Override the default pass list. Each entry is (name, fn, enabled).
        disable: Set of pass names to skip.
        enable_only: If set, run only these passes.
        enable_aggressive: If True, also run aggressive passes after defaults.

    Returns:
        Minified text string, or MinifyResult if stats=True.
    """
    if passes is None:
        passes = list(DEFAULT_PASSES)
        if enable_aggressive:
            passes.extend(AGGRESSIVE_PASSES)

    disable = disable or set()
    original_len = len(text)
    pass_stats: list[PassStat] = []

    for name, fn, enabled in passes:
        if not enabled:
            continue
        if name in disable:
            continue
        if enable_only is not None and name not in enable_only:
            continue

        before = len(text)
        text = fn(text)
        after = len(text)
        pass_stats.append(PassStat(name=name, before=before, after=after))

    # Final cleanup: collapse any blank lines introduced by earlier passes
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip() + "\n"

    if stats:
        return MinifyResult(
            text=text,
            original_len=original_len,
            minified_len=len(text),
            pass_stats=pass_stats,
        )
    return text


def minify_file(path: Path, **kwargs: object) -> str | MinifyResult:
    """Read and minify a single markdown file."""
    return minify(path.read_text(encoding="utf-8"), **kwargs)  # type: ignore[arg-type]


def minify_directory(
    directory: Path,
    *,
    pattern: str = "*.md",
    exclude: set[str] | None = None,
    separator: str = "\n\n",
    **kwargs: object,
) -> str | MinifyResult:
    """Read, minify, and concatenate all matching files in a directory.

    Files are sorted by name to ensure deterministic ordering.

    Args:
        directory: Path to directory containing instruction files.
        pattern: Glob pattern for matching files.
        exclude: Set of filenames to skip (e.g. {"05_validation_design.md"}).
        separator: String inserted between files when concatenating.
    """
    exclude = exclude or set()
    files = sorted(f for f in directory.glob(pattern) if f.name not in exclude)
    if not files:
        msg = f"No {pattern} files found in {directory}"
        raise FileNotFoundError(msg)

    raw_parts: list[str] = []
    for f in files:
        raw_parts.append(f.read_text(encoding="utf-8"))
    combined = separator.join(raw_parts)

    return minify(combined, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    aggressive = "--aggressive" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    target = Path(args[0]) if args else Path("docs/validation")

    if target.is_dir():
        result = minify_directory(target, stats=True, enable_aggressive=aggressive)
    elif target.is_file():
        result = minify_file(target, stats=True, enable_aggressive=aggressive)
    else:
        print(f"Error: {target} not found", file=sys.stderr)
        sys.exit(1)

    assert isinstance(result, MinifyResult)
    print(result.summary(), file=sys.stderr)
    print(result.text)
