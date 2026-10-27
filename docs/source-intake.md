# Source intake limits

Sources must be a real PDF (`law_pdf`), a DOCX ZIP (`decision_docx`), or explicit
UTF-8 `.md`/`.markdown` text. Client MIME hints never select the format. HTML/XML
documents disguised as Markdown are rejected. Originals download as attachments;
the application provides a separate extracted-text preview.

The source-file limit is 15 MiB. Multipart accepts a 16 MiB envelope, at most 16
parts, exactly one `file`, and at most 8 KiB per metadata field. Both declared and
streamed sizes are checked. Chunked uploads do not bypass the envelope limit.
Multipart bytes pass directly to ingestion; JSON clients retain base64 intake.

DOCX preflight limits: 512 entries, 10 MiB per decompressed entry, 40 MiB total
declared expansion, and a maximum 200:1 expansion ratio. Duplicate/unsafe paths,
overlapping entries, inconsistent headers, encryption, macros, multi-disk and
ZIP64 archives are rejected. Only `word/document.xml` is expanded, in small input
pieces, with actual output-size and CRC checks. Unused images are not inflated.
Extraction failures propagate as validation/size errors, never binary-as-text
fallback after a corrupt archive.

These limits are deliberately conservative for a Worker. Large documents should
be converted/split through a reviewed preprocessing workflow, retaining originals
and provenance; do not raise limits without representative CPU/memory testing.
The archive checks bound memory but do not establish textual or legal quality.
The extraction gate below must also pass.

No data migration is required for these intake changes. Previously accepted
malformed files may now fail reprocessing; preserve their originals for review.
Do not revert to unrestricted decompression as a compatibility workaround.

## Extraction and review (H05)

PDF text comes from pinned unpdf/PDF.js, including compressed streams and font
mappings. Page numbers, dimensions, text coordinates, character counts, and parser
versions are retained in `metadata_json.extraction`. DOCX uses strict,
namespace-aware SAX XML parsing; DTDs and malformed XML are rejected. No binary
format is decoded as printable text after extraction fails.

Inline extraction is intentionally bounded: PDF 3 MiB, 50 pages, 5,000 text items,
250,000 characters, and a 10-second cancellation deadline. The timer cannot
preempt synchronous parser CPU; platform resource limits still apply. DOCX also
limits XML nesting and element count. Text must have at least 40 letters/digits,
a plausible visible-character ratio, and no replacement/NUL characters. Every PDF
page must contain meaningful extractable text, including at least 20 letters or
digits outside the header/footer margin bands (the smaller of 72 points or 12%
of the page height). A selectable header on an otherwise scanned page must not
be treated as a successfully extracted body. This conservative check can hold
legitimate very short pages for manual review; it does not measure complete
body coverage or replace comparison against the original. These checks detect extraction
failure; they are not legal accuracy or reading-order verification.

A valid source whose extraction fails is retained with `extractionStatus:
needs_review`, a reason in the extraction warnings, no chunks, no vector job,
no approval, no metadata confirmation, and no search publication. A malformed
archive or disallowed format fails intake validation before storage. Successful
extraction does not imply human metadata confirmation. Existing corpus records
are not silently marked as verified or automatically re-extracted.

For scans, oversized files, complex layouts, or poor reading order, download the
original from ingestion review, perform OCR/extraction in a controlled offline
environment, compare the text to **every original page**, and retain the original
and correction provenance. Submit the reviewed text through explicit Markdown
intake and follow normal review/publication gates. The retained original stays
unpublished; do not change its status merely to bypass extraction. Automatic OCR
and a general-purpose background extraction service are not implemented. The
bounded inline path plus quarantine is the current safe operating envelope.

Before release, sample representative actual legal originals (including scans,
embedded fonts, columns, and tables). Synthetic fixtures verify parser behavior
but cannot establish accuracy for the production corpus. Reprocessing old records
must use the versioned publication workflow from H12; do not roll back to the
permissive binary fallbacks if compatibility issues are found.
