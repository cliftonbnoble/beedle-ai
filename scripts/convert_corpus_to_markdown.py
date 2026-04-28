#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable

try:
    from markitdown import MarkItDown
except ImportError:
    print(
        "MarkItDown is not installed in this Python environment.\n"
        "Run: bash scripts/setup-markitdown.sh",
        file=sys.stderr,
    )
    raise SystemExit(1)

SUPPORTED_EXTENSIONS = {".doc", ".docx", ".dotx", ".txt"}
PDF_EXTENSION = ".pdf"
SKIP_PREFIXES = ("~$",)
NON_ASCII_RE = re.compile(r"[^\x09\x0A\x0D\x20-\x7E]")
GOOD_PHRASE_RE = re.compile(
    r"(#{1,6}\s+)?[A-Za-z][A-Za-z'()./&:-]{1,}(?:\s+[A-Za-z][A-Za-z'().,&;:/#()-]{1,})+"
)
KNOWN_TOP_HEADINGS = (
    "Law Construed:",
    "Rules and Regulations",
    "Ordinance Sections:",
    "Index Code:",
    "Index Codes:",
    "RESIDENTIAL RENT STABILIZATION AND ARBITRATION BOARD",
    "CITY AND COUNTY OF SAN FRANCISCO",
    "IN RE:",
)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Convert a folder tree of Word/text documents into Markdown using Microsoft MarkItDown."
    )
    parser.add_argument("--source", required=True, help="Source corpus root")
    parser.add_argument(
        "--output-root",
        default=str(repo_root / "import-batches" / "markdown-corpus"),
        help="Output root for Markdown files",
    )
    parser.add_argument(
        "--stage-root",
        default=str(repo_root / "import-batches" / "legacy-docx-stage"),
        help="Where converted .doc -> .docx intermediates should go",
    )
    parser.add_argument(
        "--report-path",
        default=str(repo_root / "import-batches" / "markdown-corpus-report.json"),
        help="JSON report path",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional file limit for test runs")
    parser.add_argument("--force", action="store_true", help="Rebuild existing Markdown files")
    parser.add_argument("--include-pdf", action="store_true", help="Also try converting PDFs via MarkItDown")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first conversion error")
    return parser.parse_args()


def should_skip(path: Path) -> bool:
    return path.name.startswith(SKIP_PREFIXES) or path.name == ".DS_Store"


def iter_source_files(source_root: Path, include_pdf: bool) -> Iterable[Path]:
    allowed = set(SUPPORTED_EXTENSIONS)
    if include_pdf:
        allowed.add(PDF_EXTENSION)
    for path in sorted(source_root.rglob("*")):
        if not path.is_file():
            continue
        if should_skip(path):
            continue
        if path.suffix.lower() in allowed:
            yield path


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def convert_doc_to_docx(source: Path, stage_path: Path, force: bool) -> Path:
    if stage_path.exists() and not force:
        return stage_path
    ensure_parent(stage_path)
    subprocess.run(
        ["textutil", "-convert", "docx", "-output", str(stage_path), str(source)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    return stage_path


def find_known_heading_start(line: str) -> int | None:
    starts = [line.find(heading) for heading in KNOWN_TOP_HEADINGS if heading in line]
    starts = [start for start in starts if start >= 0]
    return min(starts) if starts else None


def clean_legacy_doc_markdown(content: str) -> str:
    lines = content.splitlines()
    scan_limit = min(12, len(lines))

    for index in range(scan_limit):
        line = lines[index]
        if not line.strip():
            continue

        heading_start = find_known_heading_start(line)
        if heading_start is not None:
            lines[index] = line[heading_start:].lstrip()
            continue

        if NON_ASCII_RE.search(line) is not None:
            lines[index] = ""
            continue

        match = GOOD_PHRASE_RE.search(line)
        if match and match.start() > 0:
            lines[index] = line[match.start() :].lstrip()

    return "\n".join(lines).lstrip("\ufeff").lstrip()


def convert_with_markitdown(md: MarkItDown, input_path: Path, output_path: Path, *, legacy_doc: bool) -> int:
    ensure_parent(output_path)
    result = md.convert(str(input_path))
    content = result.text_content if hasattr(result, "text_content") else str(result)
    if legacy_doc:
        content = clean_legacy_doc_markdown(content)
    output_path.write_text(content, encoding="utf-8")
    return len(content)


def main() -> int:
    args = parse_args()
    source_root = Path(args.source).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    stage_root = Path(args.stage_root).expanduser().resolve()
    report_path = Path(args.report_path).expanduser().resolve()

    if not source_root.exists():
        print(f"Source root does not exist: {source_root}", file=sys.stderr)
        return 1

    files = list(iter_source_files(source_root, args.include_pdf))
    if args.limit > 0:
        files = files[: args.limit]

    md = MarkItDown(enable_plugins=False)
    report: list[dict[str, object]] = []
    succeeded = 0
    failed = 0
    skipped = 0

    for index, source in enumerate(files, start=1):
        rel = source.relative_to(source_root)
        suffix = source.suffix.lower()
        output_path = output_root / rel.with_suffix(".md")
        staged_path: Path | None = None

        if output_path.exists() and not args.force:
            skipped += 1
            report.append(
                {
                    "status": "skipped",
                    "reason": "output_exists",
                    "source": str(source),
                    "output": str(output_path),
                }
            )
            print(f"[{index}/{len(files)}] SKIP {rel}")
            continue

        try:
            convert_input = source
            if suffix == ".doc":
                staged_path = stage_root / rel.with_suffix(".docx")
                convert_input = convert_doc_to_docx(source, staged_path, args.force)

            chars = convert_with_markitdown(
                md,
                convert_input,
                output_path,
                legacy_doc=suffix == ".doc",
            )
            succeeded += 1
            report.append(
                {
                    "status": "ok",
                    "source": str(source),
                    "staged_input": str(staged_path) if staged_path else None,
                    "converted_input": str(convert_input),
                    "output": str(output_path),
                    "chars": chars,
                }
            )
            print(f"[{index}/{len(files)}] OK   {rel} -> {output_path.relative_to(output_root)}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            report.append(
                {
                    "status": "failed",
                    "source": str(source),
                    "staged_input": str(staged_path) if staged_path else None,
                    "output": str(output_path),
                    "error": str(exc),
                }
            )
            print(f"[{index}/{len(files)}] FAIL {rel} -> {exc}", file=sys.stderr)
            if args.fail_fast:
                break

    ensure_parent(report_path)
    report_path.write_text(
        json.dumps(
            {
                "source_root": str(source_root),
                "output_root": str(output_root),
                "stage_root": str(stage_root),
                "include_pdf": args.include_pdf,
                "limit": args.limit,
                "force": args.force,
                "summary": {
                    "total_selected": len(files),
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": skipped,
                },
                "files": report,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nReport written to {report_path}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
