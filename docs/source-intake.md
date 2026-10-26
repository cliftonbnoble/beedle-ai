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
H05 adds the extraction/publication gate.

No data migration is required for these intake changes. Previously accepted
malformed files may now fail reprocessing; preserve their originals for review.
Do not revert to unrestricted decompression as a compatibility workaround.
