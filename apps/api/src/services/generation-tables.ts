// Explicit projection columns shared by generation snapshots and publication.
export const generationTables = {
  "document_sections": [
    "id",
    "document_id",
    "canonical_key",
    "heading",
    "section_order",
    "section_text"
  ],
  "section_paragraphs": [
    "id",
    "section_id",
    "anchor",
    "paragraph_order",
    "text"
  ],
  "document_chunks": [
    "id",
    "document_id",
    "section_id",
    "paragraph_id",
    "paragraph_anchor",
    "citation_anchor",
    "section_label",
    "chunk_order",
    "chunk_text",
    "token_estimate",
    "created_at",
    "paragraph_anchor_end",
    "chunk_warnings_json"
  ],
  "document_reference_links": [
    "id",
    "document_id",
    "reference_type",
    "raw_value",
    "normalized_value",
    "canonical_value",
    "is_valid",
    "created_at",
    "catalog_version",
    "edition_id",
    "edition_provision_id",
    "edition_body_sha256",
    "applicability_status"
  ],
  "document_reference_issues": [
    "id",
    "document_id",
    "reference_type",
    "raw_value",
    "normalized_value",
    "message",
    "severity",
    "created_at",
    "catalog_version"
  ]
} as const;
