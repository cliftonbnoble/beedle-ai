// Exact columns for retained legal catalog snapshots and atomic projection.
export const catalogTables = {
  "legal_reference_sources": [
    "source_key",
    "source_path",
    "updated_at"
  ],
  "legal_index_codes": [
    "id",
    "code_identifier",
    "normalized_code",
    "family",
    "label",
    "description",
    "is_reserved",
    "is_legacy_pre_1002",
    "linked_ordinance_sections_json",
    "linked_rules_sections_json",
    "source_page_anchor",
    "active",
    "created_at",
    "updated_at"
  ],
  "legal_ordinance_sections": [
    "id",
    "section_number",
    "subsection_path",
    "citation",
    "normalized_citation",
    "heading",
    "body_text",
    "source_page_anchor",
    "active",
    "created_at",
    "updated_at"
  ],
  "legal_rules_sections": [
    "id",
    "part",
    "section_number",
    "citation",
    "normalized_citation",
    "canonical_bare_citation",
    "normalized_bare_citation",
    "heading",
    "body_text",
    "source_page_anchor",
    "active",
    "created_at",
    "updated_at"
  ],
  "legal_reference_crosswalk": [
    "id",
    "index_code_id",
    "ordinance_citation",
    "rules_citation",
    "source",
    "created_at"
  ]
} as const;
export type CatalogTable = keyof typeof catalogTables;
