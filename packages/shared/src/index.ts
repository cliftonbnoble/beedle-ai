import { z } from "zod";
export { canonicalIndexCodeOptions, type CanonicalIndexCodeOption } from "./index-codes";

const isoDateSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "Expected YYYY-MM-DD");

export const canonicalJudgeNames = [
  "René Juárez",
  "Andrew Yick",
  "Connie Brandon",
  "Deborah K. Lim",
  "Dorothy Chou Proudfoot",
  "Erin E. Katayama",
  "Harrison Nam",
  "Jeffrey Eckber",
  "Jill Figg Dayal",
  "Joseph Koomas",
  "Michael J. Berg",
  "Peter Kearns"
] as const;

export const fileTypeSchema = z.enum(["decision_docx", "law_pdf"]);

export const sourceFileSchema = z.object({
  filename: z.string().min(1),
  mimeType: z.string().min(1),
  bytesBase64: z.string().min(1)
});

export const paragraphSchema = z.object({
  anchor: z.string().min(1),
  order: z.number().int().nonnegative(),
  text: z.string().min(1)
});

export const sectionSchema = z.object({
  canonicalKey: z.string().min(1),
  heading: z.string().min(1),
  order: z.number().int().nonnegative(),
  paragraphs: z.array(paragraphSchema)
});

export const ingestDocumentSchema = z.object({
  jurisdiction: z.string().min(1),
  title: z.string().min(1),
  citation: z.string().min(1),
  decisionDate: isoDateSchema.optional(),
  fileType: fileTypeSchema,
  sourceFile: sourceFileSchema,
  performVectorUpsert: z.boolean().default(true)
});

export const searchFiltersSchema = z.object({
  documentId: z.string().optional(),
  jurisdiction: z.string().optional(),
  fileType: fileTypeSchema.optional(),
  chunkType: z.string().optional(),
  indexCode: z.string().optional(),
  indexCodes: z.array(z.string().min(1)).max(50).optional(),
  rulesSection: z.string().optional(),
  ordinanceSection: z.string().optional(),
  partyName: z.string().optional(),
  judgeName: z.string().optional(),
  judgeNames: z.array(z.string().min(1)).max(12).optional(),
  fromDate: isoDateSchema.optional(),
  toDate: isoDateSchema.optional(),
  approvedOnly: z.boolean().default(true)
});

export const retrievalCorpusModeSchema = z.enum(["trusted_only", "trusted_plus_provisional"]);

export const searchRequestSchema = z.object({
  query: z.string().min(2),
  limit: z.number().int().positive().max(5000).default(20),
  offset: z.number().int().nonnegative().max(5000).default(0),
  snippetMaxLength: z.number().int().min(120).max(1200).default(260),
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  filters: searchFiltersSchema.default({ approvedOnly: true })
});

export const searchResultPassageSchema = z.object({
  chunkId: z.string(),
  snippet: z.string(),
  sectionLabel: z.string(),
  sectionHeading: z.string(),
  citationAnchor: z.string(),
  paragraphAnchor: z.string(),
  chunkType: z.string().default(""),
  score: z.number()
});

export const searchSupportingFactDebugSchema = z.object({
  source: z.enum(["matched_pool", "fallback_findings_background_pool"]),
  factualAnchorScore: z.number(),
  anchorHits: z.number().int().nonnegative(),
  secondaryHits: z.number().int().nonnegative(),
  coverageRatio: z.number()
});

export const searchResultSchema = z.object({
  documentId: z.string(),
  chunkId: z.string(),
  title: z.string(),
  citation: z.string(),
  authorName: z.string().nullable().default(null),
  decisionDate: isoDateSchema.nullable().default(null),
  fileType: fileTypeSchema,
  snippet: z.string(),
  sectionLabel: z.string(),
  sourceFileRef: z.string(),
  sourceLink: z.string(),
  citationAnchor: z.string(),
  sectionHeading: z.string(),
  paragraphAnchor: z.string(),
  corpusTier: z.enum(["trusted", "provisional"]).default("trusted"),
  chunkType: z.string().default(""),
  retrievalReason: z.array(z.string()).default([]),
  matchedPassage: searchResultPassageSchema.optional(),
  primaryAuthorityPassage: searchResultPassageSchema.optional(),
  supportingFactPassage: searchResultPassageSchema.optional(),
  supportingFactDebug: searchSupportingFactDebugSchema.optional(),
  score: z.number(),
  lexicalScore: z.number(),
  vectorScore: z.number()
});

export const searchResponseSchema = z.object({
  query: z.string(),
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  offset: z.number().int().nonnegative().default(0),
  limit: z.number().int().positive().default(20),
  hasMore: z.boolean().default(false),
  tierCounts: z.object({
    trusted: z.number().int().nonnegative(),
    provisional: z.number().int().nonnegative()
  }),
  total: z.number().int().nonnegative(),
  results: z.array(searchResultSchema)
});

export const retrievalQueryTypeSchema = z.enum([
  "keyword",
  "exact_phrase",
  "citation_lookup",
  "party_name",
  "index_code",
  "rules_ordinance"
]);

export const searchDebugRequestSchema = z.object({
  query: z.string().min(2),
  limit: z.number().int().positive().max(5000).default(20),
  offset: z.number().int().nonnegative().max(5000).default(0),
  snippetMaxLength: z.number().int().min(120).max(1200).default(260),
  queryType: retrievalQueryTypeSchema.default("keyword"),
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  filters: searchFiltersSchema.default({ approvedOnly: true })
});

export const searchDebugResultSchema = searchResultSchema.extend({
  diagnostics: z.object({
    lexicalScore: z.number(),
    vectorScore: z.number(),
    exactPhraseBoost: z.number(),
    citationBoost: z.number(),
    metadataBoost: z.number(),
    sectionBoost: z.number(),
    partyNameBoost: z.number(),
    judgeNameBoost: z.number(),
    trustTierBoost: z.number(),
    rerankScore: z.number(),
    why: z.array(z.string())
  })
});

export const searchDebugResponseSchema = z.object({
  query: z.string(),
  queryType: retrievalQueryTypeSchema,
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  offset: z.number().int().nonnegative().default(0),
  limit: z.number().int().positive().default(20),
  hasMore: z.boolean().default(false),
  combinedFilterZeroHitRecoveryUsed: z.boolean().default(false),
  tierCounts: z.object({
    trusted: z.number().int().nonnegative(),
    provisional: z.number().int().nonnegative()
  }),
  filters: searchFiltersSchema,
  runtimeDiagnostics: z.object({
    aiAvailable: z.boolean(),
    vectorQueryAttempted: z.boolean(),
    vectorMatchCount: z.number().int().nonnegative(),
    vectorNamespace: z.string(),
    lexicalScopeDocumentCount: z.number().int().nonnegative().default(0),
    lexicalRowCount: z.number().int().nonnegative().default(0),
    mergedChunkCount: z.number().int().nonnegative().default(0),
    scoredCount: z.number().int().nonnegative().default(0),
    rerankedCount: z.number().int().nonnegative().default(0),
    decisionScopeDocumentCount: z.number().int().nonnegative().default(0),
    decisionScopeChunkCount: z.number().int().nonnegative().default(0),
    stageTimingsMs: z.object({
      scopeBuild: z.number().nonnegative().default(0),
      lexicalScopeFetch: z.number().nonnegative().default(0),
      lexicalSearch: z.number().nonnegative().default(0),
      vectorSearch: z.number().nonnegative().default(0),
      vectorChunkFetch: z.number().nonnegative().default(0),
      initialScoring: z.number().nonnegative().default(0),
      decisionScopeFetch: z.number().nonnegative().default(0),
      decisionScopeBuild: z.number().nonnegative().default(0),
      finalizeResults: z.number().nonnegative().default(0),
      total: z.number().nonnegative().default(0)
    })
  }),
  total: z.number().int().nonnegative(),
  results: z.array(searchDebugResultSchema)
});

export const caseAssistantRequestSchema = z.object({
  findings_text: z.string().min(1),
  law_text: z.string().min(1),
  index_codes: z.array(z.string().min(1)).default([]),
  rules_sections: z.array(z.string().min(1)).default([]),
  ordinance_sections: z.array(z.string().min(1)).default([]),
  uploaded_doc_ids: z.array(z.string().min(1)).default([]),
  issue_tags: z.array(z.string().min(1)).default([])
});

export const draftConclusionsRequestSchema = z.object({
  findings_text: z.string().min(1),
  law_text: z.string().default(""),
  index_codes: z.array(z.string().min(1)).default([]),
  rules_sections: z.array(z.string().min(1)).default([]),
  ordinance_sections: z.array(z.string().min(1)).default([]),
  uploaded_doc_ids: z.array(z.string().min(1)).default([]),
  issue_tags: z.array(z.string().min(1)).default([]),
  style_mode: z.string().min(1).optional()
});

export const templateModeSchema = z.enum(["blank_scaffold", "guided_scaffold", "lightly_contextualized"]);
export const canonicalSectionNameSchema = z.enum([
  "Introduction",
  "Findings of Fact",
  "Related Case / Procedural History",
  "Conclusions of Law",
  "Order"
]);

export const taxonomyCanonicalSectionSchema = z.object({
  name: canonicalSectionNameSchema,
  purpose: z.string().min(1),
  placeholder: z.string().min(1),
  base_prompts: z.array(z.string()),
  example_structure: z.array(z.string()).default([])
});

export const taxonomyCaseTypeSchema = z.object({
  id: z.string().min(1),
  label: z.string().min(1),
  description: z.string().min(1),
  aliases: z.array(z.string()).default([]),
  focus_prompts: z.array(z.string()).default([]),
  default_issue_tags: z.array(z.string()).default([]),
  template_section_hints: z
    .object({
      Introduction: z.array(z.string()).optional(),
      "Findings of Fact": z.array(z.string()).optional(),
      "Related Case / Procedural History": z.array(z.string()).optional(),
      "Conclusions of Law": z.array(z.string()).optional(),
      Order: z.array(z.string()).optional()
    })
    .default({}),
  index_code_mappings: z.array(z.string()).default([]),
  rules_hints: z.array(z.string()).default([]),
  ordinance_hints: z.array(z.string()).default([])
});

export const taxonomyTemplateDefaultsSchema = z.object({
  default_template_mode: templateModeSchema.default("guided_scaffold"),
  guardrails: z.array(z.string()).default([])
});

export const taxonomyConfigSchema = z.object({
  version: z.string().min(1),
  default_case_type_id: z.string().min(1),
  canonical_sections: z.array(taxonomyCanonicalSectionSchema).min(1),
  template_defaults: taxonomyTemplateDefaultsSchema,
  issue_tag_catalog: z.array(z.string()).default([]),
  case_types: z.array(taxonomyCaseTypeSchema).min(1)
});

export const draftTemplateRequestSchema = z.object({
  case_type: z.string().min(1),
  index_codes: z.array(z.string().min(1)).default([]),
  rules_sections: z.array(z.string().min(1)).default([]),
  ordinance_sections: z.array(z.string().min(1)).default([]),
  issue_tags: z.array(z.string().min(1)).default([]),
  findings_text: z.string().optional(),
  law_text: z.string().optional(),
  template_mode: templateModeSchema.default("guided_scaffold"),
  style_mode: z.string().min(1).optional()
});

export const caseAssistantDirectionSchema = z.enum(["grant", "deny", "partial", "unclear"]);
export const caseAssistantConfidenceSchema = z.enum(["low", "medium", "high"]);
export const supportTypeSchema = z.enum(["explicit", "inference"]);

export const caseAssistantCitationSchema = z.object({
  id: z.string(),
  title: z.string(),
  citation: z.string(),
  citation_anchor: z.string(),
  source_link: z.string().url(),
  snippet: z.string(),
  support_type: supportTypeSchema
});

export const caseAssistantAuthoritySchema = z.object({
  document_id: z.string(),
  title: z.string(),
  citation: z.string(),
  why_it_matches: z.string(),
  snippet: z.string(),
  section_label: z.string(),
  citation_anchor: z.string(),
  source_link: z.string().url(),
  source_file_ref: z.string(),
  support_type: supportTypeSchema,
  citation_id: z.string()
});

export const caseAssistantResponseSchema = z.object({
  query_summary: z.string(),
  similar_cases: z.array(caseAssistantAuthoritySchema),
  relevant_law: z.array(caseAssistantAuthoritySchema),
  outcome_guidance: z.object({
    direction: caseAssistantDirectionSchema,
    rationale: z.string()
  }),
  reasoning_themes: z.array(
    z.object({
      theme: z.string(),
      explanation: z.string(),
      citation_ids: z.array(z.string())
    })
  ),
  vulnerabilities: z.array(
    z.object({
      issue: z.string(),
      impact: z.string(),
      citation_ids: z.array(z.string())
    })
  ),
  strengthening_suggestions: z.array(
    z.object({
      suggestion: z.string(),
      why: z.string(),
      citation_ids: z.array(z.string())
    })
  ),
  confidence: caseAssistantConfidenceSchema,
  citations: z.array(caseAssistantCitationSchema),
  guardrails: z.array(z.string())
});

export const assistantChatMessageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.string().min(1).max(12000)
});

export const assistantChatRequestSchema = z.object({
  messages: z.array(assistantChatMessageSchema).min(1).max(20),
  judgeNames: z.array(z.string().min(1)).max(12).default([]),
  indexCodes: z.array(z.string().min(1)).max(50).default([]),
  limit: z.number().int().min(1).max(8).default(6)
});

export const assistantChatCitationSchema = z.object({
  documentId: z.string(),
  title: z.string(),
  citation: z.string(),
  authorName: z.string().nullable().default(null),
  sourceLink: z.string().url(),
  primaryAuthorityPassage: searchResultPassageSchema.optional(),
  supportingFactPassage: searchResultPassageSchema.optional()
});

export const assistantChatResponseSchema = z.object({
  answer: z.string(),
  citations: z.array(assistantChatCitationSchema),
  scopeLabel: z.string(),
  model: z.string(),
  retrievedCount: z.number().int().nonnegative()
});

export const draftSectionSchema = z.object({
  id: z.string(),
  heading: z.string(),
  text: z.string(),
  citation_ids: z.array(z.string())
});

export const paragraphSupportLevelSchema = z.enum(["strong", "mixed", "weak", "unsupported"]);

export const draftParagraphSupportSchema = z.object({
  paragraph_id: z.string(),
  section_id: z.string(),
  text: z.string(),
  support_level: paragraphSupportLevelSchema,
  citation_ids: z.array(z.string()),
  support_notes: z.array(z.string())
});

export const draftConfidenceSignalsSchema = z.object({
  retrieval_strength: z.number().min(0).max(1),
  authority_count: z.number().int().nonnegative(),
  direct_conclusions_count: z.number().int().nonnegative(),
  explicit_support_ratio: z.number().min(0).max(1),
  findings_coverage: z.number().min(0).max(1),
  law_coverage: z.number().min(0).max(1),
  conflict_index: z.number().min(0).max(1),
  paragraph_support_ratio: z.number().min(0).max(1),
  confidence_score: z.number().min(0).max(1)
});

export const draftConclusionsResponseSchema = z.object({
  query_summary: z.string(),
  draft_text: z.string(),
  draft_sections: z.array(draftSectionSchema),
  paragraph_support: z.array(draftParagraphSupportSchema),
  supporting_authorities: z.array(caseAssistantAuthoritySchema),
  reasoning_notes: z.array(z.string()),
  confidence: caseAssistantConfidenceSchema,
  confidence_signals: draftConfidenceSignalsSchema,
  limitations: z.array(z.string()),
  citations: z.array(caseAssistantCitationSchema)
});

export const draftConclusionsDebugResponseSchema = z.object({
  request: draftConclusionsRequestSchema,
  draft: draftConclusionsResponseSchema,
  debug: z.object({
    paragraph_support: z.array(draftParagraphSupportSchema),
    confidence_signals: draftConfidenceSignalsSchema,
    triggered_limitations: z.array(z.string()),
    chosen_citation_ids: z.array(z.string()),
    unsupported_paragraphs: z.array(z.string())
  })
});

export const draftTemplateSectionSchema = z.object({
  section_name: z.string(),
  section_purpose: z.string(),
  placeholder_text: z.string(),
  drafting_prompts: z.array(z.string()),
  example_structure: z.array(z.string()).optional(),
  citation_ids: z.array(z.string()).default([])
});

export const draftTemplateResponseSchema = z.object({
  case_type: z.string(),
  template_title: z.string(),
  template_mode: templateModeSchema,
  template_sections: z.array(draftTemplateSectionSchema),
  suggested_prompts: z.array(z.string()),
  guidance_notes: z.array(z.string()).default([]),
  supporting_authorities: z.array(caseAssistantAuthoritySchema).default([]),
  confidence_or_completeness_note: z.string(),
  citations: z.array(caseAssistantCitationSchema).default([])
});

export const taxonomyResolveRequestSchema = z.object({
  case_type: z.string().min(1)
});

export const taxonomyResolveResponseSchema = z.object({
  requested_case_type: z.string(),
  resolved_case_type_id: z.string(),
  resolved_case_type_label: z.string(),
  match_type: z.enum(["id", "alias", "fallback"]),
  warnings: z.array(z.string())
});

export const taxonomyConfigInspectResponseSchema = z.object({
  config: taxonomyConfigSchema,
  stats: z.object({
    case_type_count: z.number().int().nonnegative(),
    canonical_section_count: z.number().int().nonnegative(),
    issue_tag_count: z.number().int().nonnegative()
  })
});

export const draftExportKindSchema = z.enum(["conclusions", "template"]);
export const draftExportFormatSchema = z.enum(["markdown", "text", "html"]);

export const draftExportRequestSchema = z
  .object({
    kind: draftExportKindSchema,
    format: draftExportFormatSchema,
    document_title: z.string().min(1).optional(),
    conclusions: draftConclusionsResponseSchema.optional(),
    template: draftTemplateResponseSchema.optional()
  })
  .superRefine((value, ctx) => {
    if (value.kind === "conclusions" && !value.conclusions) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "conclusions payload is required for kind=conclusions" });
    }
    if (value.kind === "template" && !value.template) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "template payload is required for kind=template" });
    }
  });

export const draftExportResponseSchema = z.object({
  kind: draftExportKindSchema,
  format: draftExportFormatSchema,
  filename: z.string(),
  mime_type: z.string(),
  content: z.string(),
  metadata: z.object({
    generated_at: z.string(),
    citation_count: z.number().int().nonnegative(),
    support_item_count: z.number().int().nonnegative()
  })
});

export const taxonomyValidateResponseSchema = z.object({
  ok: z.boolean(),
  error: z.string().optional(),
  config: taxonomyConfigSchema.optional()
});

export const legalIndexCodeSchema = z.object({
  code_identifier: z.string().min(1),
  family: z.string().optional().nullable(),
  label: z.string().optional().nullable(),
  description: z.string().optional().nullable(),
  reserved: z.boolean().default(false),
  legacy_pre_1002: z.boolean().default(false),
  linked_ordinance_sections: z.array(z.string()).default([]),
  linked_rules_sections: z.array(z.string()).default([]),
  source_page_anchor: z.string().optional().nullable()
});

export const legalOrdinanceSectionSchema = z.object({
  section_number: z.string().min(1),
  subsection_path: z.string().optional().nullable(),
  heading: z.string().optional().nullable(),
  body_text: z.string().min(1),
  page_anchor: z.string().optional().nullable()
});

export const legalRulesSectionSchema = z.object({
  part: z.string().optional().nullable(),
  section_number: z.string().min(1),
  canonical_bare_citation: z.string().optional().nullable(),
  normalized_bare_citation: z.string().optional().nullable(),
  heading: z.string().optional().nullable(),
  body_text: z.string().min(1),
  page_anchor: z.string().optional().nullable()
});

export const legalReferenceRebuildRequestSchema = z.object({
  source_trace: z.object({
    index_codes: z.string(),
    ordinance: z.string(),
    rules: z.string()
  }),
  index_codes: z.array(legalIndexCodeSchema),
  ordinance_sections: z.array(legalOrdinanceSectionSchema),
  rules_sections: z.array(legalRulesSectionSchema),
  crosswalk: z
    .array(
      z.object({
        index_code: z.string().optional(),
        ordinance_section: z.string().optional(),
        rules_section: z.string().optional(),
        source: z.string().default("normalized_import")
      })
    )
    .default([]),
  coverage_report: z
    .object({
      ordinance: z
        .object({
          parser_used: z.enum(["true_text", "layout_text", "pdf"]),
          source_path: z.string().optional().nullable(),
          fallback_reason: z.string().optional().nullable(),
          expected_section_count: z.number().int().nonnegative(),
          parsed_section_count: z.number().int().nonnegative(),
          duplicate_collisions_avoided: z.number().int().nonnegative(),
          low_confidence_sections: z.number().int().nonnegative(),
          duplicate_normalized_citations_encountered: z.number().int().nonnegative().optional(),
          duplicates_merged: z.number().int().nonnegative().optional(),
          duplicates_dropped: z.number().int().nonnegative().optional(),
          committed_section_count: z.number().int().nonnegative().optional(),
          sample_collisions: z.array(z.object({ normalized_citation: z.string(), duplicate_count: z.number().int().positive() })).optional()
        })
        .optional(),
      rules: z
        .object({
          parser_used: z.enum(["true_text", "layout_text", "pdf"]),
          source_path: z.string().optional().nullable(),
          fallback_reason: z.string().optional().nullable(),
          expected_section_count: z.number().int().nonnegative(),
          parsed_section_count: z.number().int().nonnegative(),
          duplicate_collisions_avoided: z.number().int().nonnegative(),
          low_confidence_sections: z.number().int().nonnegative(),
          duplicate_normalized_citations_encountered: z.number().int().nonnegative().optional(),
          duplicates_merged: z.number().int().nonnegative().optional(),
          duplicates_dropped: z.number().int().nonnegative().optional(),
          committed_section_count: z.number().int().nonnegative().optional(),
          sample_collisions: z.array(z.object({ normalized_citation: z.string(), duplicate_count: z.number().int().positive() })).optional()
        })
        .optional(),
      crosswalk: z
        .object({
          total_candidates: z.number().int().nonnegative(),
          resolved_links: z.number().int().nonnegative(),
          unresolved_links: z.number().int().nonnegative()
        })
        .optional()
    })
    .optional()
});

export const legalReferenceInspectResponseSchema = z.object({
  source_trace: z.object({
    index_codes: z.string().optional(),
    ordinance: z.string().optional(),
    rules: z.string().optional()
  }),
  summary: z.object({
    index_code_count: z.number().int().nonnegative(),
    ordinance_section_count: z.number().int().nonnegative(),
    rules_section_count: z.number().int().nonnegative(),
    crosswalk_count: z.number().int().nonnegative(),
    unmatched_reference_issue_count: z.number().int().nonnegative()
  }),
  coverage_report: z
    .object({
      ordinance: z
        .object({
          parser_used: z.enum(["true_text", "layout_text", "pdf"]),
          source_path: z.string().optional().nullable(),
          fallback_reason: z.string().optional().nullable(),
          expected_section_count: z.number().int().nonnegative(),
          parsed_section_count: z.number().int().nonnegative(),
          duplicate_collisions_avoided: z.number().int().nonnegative(),
          low_confidence_sections: z.number().int().nonnegative(),
          duplicate_normalized_citations_encountered: z.number().int().nonnegative().optional(),
          duplicates_merged: z.number().int().nonnegative().optional(),
          duplicates_dropped: z.number().int().nonnegative().optional(),
          committed_section_count: z.number().int().nonnegative().optional(),
          sample_collisions: z.array(z.object({ normalized_citation: z.string(), duplicate_count: z.number().int().positive() })).optional()
        })
        .optional(),
      rules: z
        .object({
          parser_used: z.enum(["true_text", "layout_text", "pdf"]),
          source_path: z.string().optional().nullable(),
          fallback_reason: z.string().optional().nullable(),
          expected_section_count: z.number().int().nonnegative(),
          parsed_section_count: z.number().int().nonnegative(),
          duplicate_collisions_avoided: z.number().int().nonnegative(),
          low_confidence_sections: z.number().int().nonnegative(),
          duplicate_normalized_citations_encountered: z.number().int().nonnegative().optional(),
          duplicates_merged: z.number().int().nonnegative().optional(),
          duplicates_dropped: z.number().int().nonnegative().optional(),
          committed_section_count: z.number().int().nonnegative().optional(),
          sample_collisions: z.array(z.object({ normalized_citation: z.string(), duplicate_count: z.number().int().positive() })).optional()
        })
        .optional(),
      crosswalk: z
        .object({
          total_candidates: z.number().int().nonnegative(),
          resolved_links: z.number().int().nonnegative(),
          unresolved_links: z.number().int().nonnegative()
        })
        .optional()
    })
    .optional(),
  readiness_status: z
    .object({
      ordinance_coverage_ok: z.boolean(),
      rules_coverage_ok: z.boolean(),
      crosswalk_resolvable: z.boolean(),
      counts_consistent: z.boolean(),
      critical_citations_ok: z.boolean().optional(),
      crosswalk_candidates_meaningful: z.boolean().optional(),
      readiness_recommendation: z.enum(["blocked", "safe_for_limited_pilot_import", "safe_for_broader_import"]).optional()
    })
    .optional(),
  samples: z.object({
    index_codes: z.array(legalIndexCodeSchema.extend({ normalized_code: z.string() })),
    ordinance_sections: z.array(legalOrdinanceSectionSchema.extend({ citation: z.string(), normalized_citation: z.string() })),
    rules_sections: z.array(legalRulesSectionSchema.extend({ citation: z.string(), normalized_citation: z.string() }))
  }),
  unmatched_reference_issues: z.array(
    z.object({
      document_id: z.string(),
      reference_type: z.string(),
      raw_value: z.string(),
      normalized_value: z.string(),
      message: z.string(),
      created_at: z.string()
    })
  ),
  unresolved_crosswalks: z.array(
    z.object({
      index_code: z.string().optional(),
      ordinance_citation: z.string().optional(),
      rules_citation: z.string().optional(),
      source: z.string(),
      reason: z.string()
    })
  ),
  critical_citation_checks: z.array(
    z.object({
      citation: z.string(),
      normalized: z.string(),
      status: z.enum(["resolved", "unresolved", "ambiguous"]),
      diagnostic: z.enum(["exact_match", "parent_or_related_only", "not_found", "multiple_exact"]).optional(),
      ordinance_matches: z.array(z.object({ citation: z.string(), heading: z.string().nullable() })),
      rules_matches: z.array(z.object({ citation: z.string(), heading: z.string().nullable() }))
    })
  ),
  critical_citation_exceptions: z
    .array(
      z.object({
        citation: z.string(),
        status: z.enum(["resolved", "unresolved", "ambiguous"]),
        classification: z.enum([
          "exact_resolved",
          "cross_context_ambiguity",
          "parent_or_related_only",
          "not_found_in_committed_set",
          "multiple_exact_matches",
          "other"
        ]),
        recommendation: z.string()
      })
    )
    .optional()
});

export const legalCitationVerifyRequestSchema = z.object({
  citations: z.array(z.string().min(1)).min(1)
});

export const legalCitationVerifyResponseSchema = z.object({
  checks: z.array(
    z.object({
      citation: z.string(),
      normalized: z.string(),
      status: z.enum(["resolved", "unresolved", "ambiguous"]),
      diagnostic: z.enum(["exact_match", "parent_or_related_only", "not_found", "multiple_exact"]).optional(),
      ordinance_matches: z.array(z.object({ citation: z.string(), heading: z.string().nullable() })),
      rules_matches: z.array(z.object({ citation: z.string(), heading: z.string().nullable() }))
    })
  )
});

export const adminIngestionMetadataUpdateSchema = z.object({
  index_codes: z.array(z.string().min(1)).optional(),
  rules_sections: z.array(z.string().min(1)).optional(),
  ordinance_sections: z.array(z.string().min(1)).optional(),
  case_number: z.string().optional().nullable(),
  decision_date: isoDateSchema.optional().nullable(),
  author_name: z.string().optional().nullable(),
  outcome_label: z.enum(["grant", "deny", "partial", "unclear"]).optional(),
  confirm_required_metadata: z.boolean().optional()
});

export const adminIngestionRejectSchema = z.object({
  reason: z.string().min(3)
});

export type FileType = z.infer<typeof fileTypeSchema>;
export type IngestDocumentInput = z.infer<typeof ingestDocumentSchema>;
export type SearchRequest = z.infer<typeof searchRequestSchema>;
export type SearchResponse = z.infer<typeof searchResponseSchema>;
export type SearchDebugRequest = z.infer<typeof searchDebugRequestSchema>;
export type SearchDebugResponse = z.infer<typeof searchDebugResponseSchema>;
export type CaseAssistantRequest = z.infer<typeof caseAssistantRequestSchema>;
export type CaseAssistantResponse = z.infer<typeof caseAssistantResponseSchema>;
export type AssistantChatRequest = z.infer<typeof assistantChatRequestSchema>;
export type AssistantChatResponse = z.infer<typeof assistantChatResponseSchema>;
export type DraftConclusionsRequest = z.infer<typeof draftConclusionsRequestSchema>;
export type DraftConclusionsResponse = z.infer<typeof draftConclusionsResponseSchema>;
export type DraftConclusionsDebugResponse = z.infer<typeof draftConclusionsDebugResponseSchema>;
export type DraftTemplateRequest = z.infer<typeof draftTemplateRequestSchema>;
export type DraftTemplateResponse = z.infer<typeof draftTemplateResponseSchema>;
export type TemplateMode = z.infer<typeof templateModeSchema>;
export type TaxonomyConfig = z.infer<typeof taxonomyConfigSchema>;
export type TaxonomyResolveResponse = z.infer<typeof taxonomyResolveResponseSchema>;
export type DraftExportRequest = z.infer<typeof draftExportRequestSchema>;
export type DraftExportResponse = z.infer<typeof draftExportResponseSchema>;
export type AdminIngestionMetadataUpdate = z.infer<typeof adminIngestionMetadataUpdateSchema>;
export type LegalReferenceRebuildRequest = z.infer<typeof legalReferenceRebuildRequestSchema>;
export type LegalCitationVerifyRequest = z.infer<typeof legalCitationVerifyRequestSchema>;

export const REQUIRED_QC_KEYS = ["index_codes", "rules", "ordinance"] as const;
