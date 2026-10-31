export {ingestionReceiptResultSchemaV1,type IngestionReceiptResultV1} from "./ingestion-contract";
import { z } from "zod";
import { assistantContextLimits, assistantQuestionError } from "./assistant-context";
export { assistantContextLimits, assistantQuestionError, assistantContextWindow, boundAssistantRetrievalQuery } from "./assistant-context";
import { canonicalIndexCodeOptions } from "./index-codes";
export { canonicalIndexCodeOptions, type CanonicalIndexCodeOption } from "./index-codes";
export { conceptVariantsForToken, searchConceptVariantRules, searchIrregularTokenVariants, type SearchConceptVariantPurpose } from "./search-concepts";

export function knownCalendarDate(value:unknown):string|null {
 if(typeof value!=="string"||!/^\d{4}-\d{2}-\d{2}$/.test(value)||value.startsWith("0000"))return null;
 const date=new Date(`${value}T00:00:00.000Z`);
 return Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===value?value:null;
}
export const legalEffectiveDateSchema=z.string().refine(value=>knownCalendarDate(value)!==null,"Expected a valid YYYY-MM-DD calendar date");
const readableDateSchema=z.preprocess(knownCalendarDate,legalEffectiveDateSchema.nullable());
const isoDateSchema=legalEffectiveDateSchema;
export const mutationCommandSchema=z.object({}).strict();
export const referenceValidationInputSchema=z.object({
 indexCodes:z.array(z.string().trim().min(1).max(200)).max(50),
 rulesSections:z.array(z.string().trim().min(1).max(200)).max(50),
 ordinanceSections:z.array(z.string().trim().min(1).max(200)).max(50)
}).strict();
export const bulkSearchabilitySchema=z.object({limit:z.number().int().min(1).max(1000),realOnly:z.boolean().default(true),dryRun:z.boolean().default(true),mode:z.enum(["qcPassed","missingIndexOnlyTextReady","singleContextTextReady","decisionLikeTextReady"]).default("qcPassed")}).strict();
const editionSourceSchema=z.object({uri:z.string().trim().min(1).max(2000),sha256:z.string().regex(/^[a-f0-9]{64}$/),retrieved_at:z.string().datetime()}).strict();
export const legalEditionDescriptorSchema=z.object({
  supersedes:z.array(z.string().min(1).max(128).regex(/^[A-Za-z0-9-]+$/)).max(8).optional(),
  policy:z.enum(["unknown","reviewed_interval"]),jurisdiction:z.string().trim().min(1).max(200).nullable(),
  effective_from:legalEffectiveDateSchema.nullable(),effective_to:legalEffectiveDateSchema.nullable(),
  sources:z.object({index_codes:editionSourceSchema,rules:editionSourceSchema,ordinance:editionSourceSchema}).strict().nullable(),
  reviewed_by:z.string().trim().min(1).max(200).nullable(),reviewed_at:z.string().datetime().nullable(),
  notes:z.string().max(2000).default("")
}).strict().superRefine((value,ctx)=>{
  if(value.supersedes?.length && (value.policy!=="reviewed_interval"||new Set(value.supersedes).size!==value.supersedes.length))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Supersession requires a reviewed interval and distinct earlier edition IDs"});
  if(value.effective_to && (!value.effective_from || value.effective_to<=value.effective_from))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Edition end must follow its start (end is exclusive)"});
  if(value.policy==="reviewed_interval" && (!value.jurisdiction || !value.effective_from || !value.sources || !value.reviewed_by || !value.reviewed_at))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Reviewed intervals require jurisdiction, start, three source hashes/provenance and a declared reviewer/time"});
});
export const legalResearchContextSchema=z.object({jurisdiction:z.string().trim().min(1).max(200).optional(),as_of_date:legalEffectiveDateSchema.optional(),edition_id:z.string().min(1).max(128).regex(/^[A-Za-z0-9-]+$/).optional()}).strict().superRefine((value,ctx)=>{
  if(Boolean(value.jurisdiction)!==Boolean(value.as_of_date))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Supply both jurisdiction and applicable date, or leave both unknown"});
  if(!value.jurisdiction&&!value.edition_id)ctx.addIssue({code:z.ZodIssueCode.custom,message:"Supply a jurisdiction/date or an explicit edition"});
});
export const referenceEditionBindingSchema=z.object({
  date_basis:z.enum(["user_supplied","decision_date_proxy","unknown"]).default("unknown"),
  registry_epoch:z.number().int().nonnegative().nullable().default(null),
  edition_id:z.string().nullable(),catalog_version:z.string().nullable(),descriptor_sha256:z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  status:z.enum(["unknown","matched","unavailable","ambiguous","not_applicable"]),reason:z.string(),
  jurisdiction:z.string().nullable(),as_of_date:z.string().nullable(),effective_from:z.string().nullable(),effective_to:z.string().nullable(),
  provision_id:z.string().nullable().default(null),body_sha256:z.string().regex(/^[a-f0-9]{64}$/).nullable().default(null),
  descriptor:legalEditionDescriptorSchema.nullable().default(null),review_required:z.literal(true).default(true)
});


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
  filename: z.string().min(1).max(255),
  mimeType: z.string().min(1).max(255),
  bytesBase64: z.string().min(1).max(20 * 1024 * 1024)
}).strict();

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

export const ingestionKeySchema = z.string().uuid().transform(value=>value.toLowerCase());
export const ingestDocumentSchema = z.object({
  idempotencyKey: ingestionKeySchema.optional(),
  revisionOf: z.object({documentId:z.string().min(1).max(200),expectedRevision:z.number().int().positive()}).strict().optional(),
  jurisdiction: z.string().min(1).max(200).refine(value=>value.trim().length>0,"Must contain text"),
  title: z.string().min(1).max(1000).refine(value=>value.trim().length>0,"Must contain text"),
  citation: z.string().min(1).max(200).refine(value=>value.trim().length>0,"Must contain text"),
  decisionDate: isoDateSchema.optional(),
  fileType: fileTypeSchema,
  sourceFile: sourceFileSchema,
  performVectorUpsert: z.boolean().default(true)
}).strict();

// Multipart and background callers pass bytes directly, avoiding a base64 copy.
export const ingestDocumentMetadataSchema = ingestDocumentSchema.extend({
  sourceFile: sourceFileSchema.omit({ bytesBase64: true })
});

export const searchFiltersSchema = z.object({
  documentId: z.string().max(200).optional(),
  documentIds:z.array(z.string().trim().min(1).max(200)).min(1).max(8).optional(),
  jurisdiction: z.string().max(200).optional(),
  fileType: fileTypeSchema.optional(),
  chunkType: z.string().max(200).optional(),
  indexCode: z.string().max(200).optional(),
  indexCodes: z.array(z.string().min(1).max(200)).max(canonicalIndexCodeOptions.length).optional(),
  indexCodeOperator: z.enum(["and", "or"]).optional(),
  rulesSection: z.string().max(200).optional(),
  ordinanceSection: z.string().max(200).optional(),
  partyName: z.string().max(200).optional(),
  judgeName: z.string().max(200).optional(),
  judgeNames: z.array(z.string().min(1).max(200)).max(12).optional(),
  decisionSeries: z.enum(["T", "L"]).optional(),
  fromDate: isoDateSchema.optional(),
  toDate: isoDateSchema.optional(),
  approvedOnly: z.boolean().default(true)
});

export const retrievalCorpusModeSchema = z.enum(["trusted_only", "trusted_plus_provisional"]);

export const searchRequestSchema = z.object({
  query: z.string().min(2).max(2000),
  limit: z.number().int().positive().max(100).default(20),
  offset: z.number().int().nonnegative().max(5000).default(0),
  cursor:z.string().min(1).max(200).optional(),
  snippetMaxLength: z.number().int().min(120).max(1200).default(260),
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  filters: searchFiltersSchema.default({ approvedOnly: true })
}).refine(value=>!value.filters.fromDate||!value.filters.toDate||value.filters.fromDate<=value.filters.toDate,{message:"fromDate must not follow toDate",path:["filters","toDate"]});

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
  decisionDate: readableDateSchema.default(null),
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
  nextCursor:z.string().nullable().optional(),
  sessionExpiresAt:z.string().nullable().optional(),
  countKind:z.literal("bounded_candidates").default("bounded_candidates"),
  corpusExhaustive:z.literal(false).default(false),
  candidateExhausted:z.boolean().default(false),
  rankingVersion:z.string().optional(),
  corpusRevision:z.string().optional(),

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

export const retrievalPreviewDocumentSchema = z.object({
  documentId: z.string(),
  corpusTier: z.enum(["trusted", "provisional", "held"]).optional(),
  title: z.string(),
  citation: z.string(),
  jurisdiction: z.string(),
  authorName: z.string().nullable(),
  decisionDate: readableDateSchema,
  sourceFileRef: z.string(),
  sourceLink: z.string(),
  fileType: z.literal("decision_docx"),
  sections: z.array(
    z.object({
      sectionId: z.string(),
      canonicalKey: z.string(),
      heading: z.string(),
      sectionOrder: z.number().int(),
      paragraphCount: z.number().int().nonnegative()
    })
  ),
  validReferences: z.object({
    indexCodes: z.array(z.string()),
    rulesSections: z.array(z.string()),
    ordinanceSections: z.array(z.string())
  })
});

export const retrievalPreviewChunkSchema = z.object({
  chunkId: z.string(),
  documentId: z.string(),
  title: z.string(),
  citation: z.string(),
  chunkType: z.string(),
  chunkOrdinal: z.number().int().nonnegative(),
  sectionLabel: z.string(),
  paragraphAnchorStart: z.string(),
  paragraphAnchorEnd: z.string(),
  sourceText: z.string(),
  provenance: z.object({
    sourceFileRef: z.string(),
    sourceLink: z.string(),
    sectionId: z.string(),
    sectionLabel: z.string()
  })
});

export const retrievalPreviewResponseSchema = z
  .object({
    document: retrievalPreviewDocumentSchema,
    chunks: z.array(retrievalPreviewChunkSchema),
    corpusRevision: z.string().regex(/^\d{1,20}$/).optional()
  })
  .passthrough();

export const dashboardSummarySchema = z.object({
  searchableDecisionCount: z.number().int().nonnegative()
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
  query: z.string().min(2).max(2000),
  limit: z.number().int().positive().max(100).default(20),
  offset: z.number().int().nonnegative().max(5000).default(0),
  cursor:z.string().min(1).max(200).optional(),
  snippetMaxLength: z.number().int().min(120).max(1200).default(260),
  queryType: retrievalQueryTypeSchema.default("keyword"),
  corpusMode: retrievalCorpusModeSchema.default("trusted_only"),
  filters: searchFiltersSchema.default({ approvedOnly: true })
}).refine(value=>!value.filters.fromDate||!value.filters.toDate||value.filters.fromDate<=value.filters.toDate,{message:"fromDate must not follow toDate",path:["filters","toDate"]});

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
  nextCursor:z.string().nullable().optional(),
  sessionExpiresAt:z.string().nullable().optional(),
  countKind:z.literal("bounded_candidates").default("bounded_candidates"),
  corpusExhaustive:z.literal(false).default(false),
  candidateExhausted:z.boolean().default(false),
  rankingVersion:z.string().optional(),
  corpusRevision:z.string().optional(),

  query: z.string(),
  queryType: retrievalQueryTypeSchema,
  debugProfile: z.object({
    endpoint: z.literal("admin_retrieval_debug"),
    requestedQueryType: retrievalQueryTypeSchema,
    productionSearchQueryType: z.literal("keyword"),
    matchesProductionSearchPath: z.boolean()
  }),
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
    workBudget:z.record(z.number()).default({}),
    aiAvailable: z.boolean(),
    vectorQueryAttempted: z.boolean(),
    vectorMatchCount: z.number().int().nonnegative(),
    vectorErrored: z.boolean().default(false),
    vectorErrorMessage: z.string().default(""),
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
  law_context:legalResearchContextSchema.optional(),
  findings_text: z.string().min(1).max(24000),
  law_text: z.string().min(1).max(24000),
  index_codes: z.array(z.string().min(1).max(200)).max(50).default([]),
  rules_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  ordinance_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  uploaded_doc_ids: z.array(z.string().min(1).max(200)).max(20).default([]),
  issue_tags: z.array(z.string().min(1).max(200)).max(50).default([])
});

export const draftConclusionsRequestSchema = z.object({
  law_context:legalResearchContextSchema.optional(),
  findings_text: z.string().max(24000).default(""),
  law_text: z.string().max(24000).default(""),
  index_codes: z.array(z.string().min(1).max(200)).max(50).default([]),
  rules_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  ordinance_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  uploaded_doc_ids: z.array(z.string().min(1).max(200)).max(0, "Uploaded document context is unavailable until document access controls are enabled. Paste reviewed findings instead.").default([]),
  issue_tags: z.array(z.string().min(1).max(200)).max(50).default([]),
  style_mode: z.string().min(1).max(100).optional()
}).superRefine((value, ctx) => {
  if (!value.findings_text.trim() && !value.law_text.trim() && !value.rules_sections.length && !value.ordinance_sections.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Provide findings, law text, or an exact rule/ordinance citation." });
  }
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
  id: z.string().min(1).max(100).regex(/^[a-z0-9_]+$/),
  label: z.string().min(1).max(200),
  description: z.string().min(1),
  aliases: z.array(z.string().trim().min(1).max(200)).max(20).default([]),
  focus_prompts: z.array(z.string().max(1000)).max(50).default([]),
  default_issue_tags: z.array(z.string().max(1000)).max(50).default([]),
  template_section_hints: z
    .object({
      Introduction: z.array(z.string()).optional(),
      "Findings of Fact": z.array(z.string()).optional(),
      "Related Case / Procedural History": z.array(z.string()).optional(),
      "Conclusions of Law": z.array(z.string()).optional(),
      Order: z.array(z.string()).optional()
    })
    .default({}),
  index_code_mappings: z.array(z.string().max(1000)).max(50).default([]),
  rules_hints: z.array(z.string().max(1000)).max(50).default([]),
  ordinance_hints: z.array(z.string().max(1000)).max(50).default([])
});

export const taxonomyTemplateDefaultsSchema = z.object({
  default_template_mode: templateModeSchema.default("guided_scaffold"),
  guardrails: z.array(z.string()).default([])
});

export const taxonomyInferencePolicySchema=z.object({
  review_status:z.enum(["unreviewed","reviewed_for_inference"]).default("unreviewed"),
  jurisdictions:z.array(z.string().trim().min(1).max(200)).max(20).default([]),
  reviewed_by:z.string().trim().min(1).max(200).nullable().default(null),
  reviewed_at:z.string().datetime().nullable().default(null),
  evaluation_ref:z.string().trim().min(1).max(1000).nullable().default(null),
  min_score:z.number().min(0.55).max(0.95).default(0.55),
  min_margin:z.number().min(0.1).max(0.5).default(0.15)
}).strict().superRefine((value,ctx)=>{
 if(value.review_status==="reviewed_for_inference"&&(!value.reviewed_by||!value.reviewed_at||!value.evaluation_ref||value.jurisdictions.length===0))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Inference review requires a declared reviewer, date, jurisdiction scope and evaluation evidence"});
});
export const taxonomyConfigSchema = z.object({
  inference_policy:taxonomyInferencePolicySchema.default({}),
  version: z.string().min(1),
  default_case_type_id: z.string().min(1),
  canonical_sections: z.array(taxonomyCanonicalSectionSchema).min(1),
  template_defaults: taxonomyTemplateDefaultsSchema,
  issue_tag_catalog: z.array(z.string()).default([]),
  case_types: z.array(taxonomyCaseTypeSchema).min(1).max(20)
}).superRefine((value,ctx)=>{
 const ids=value.case_types.map(entry=>entry.id);
 if(new Set(ids).size!==ids.length)ctx.addIssue({code:z.ZodIssueCode.custom,message:"Case type IDs must be unique"});
 if(!ids.includes(value.default_case_type_id))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Default case type must exist"});
 if(value.inference_policy.review_status==="reviewed_for_inference"&&!['general','unknown'].includes(value.default_case_type_id))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Reviewed inference requires a general/unknown fallback"});
 const normalize=(text:string)=>text.toLowerCase().trim().replace(/\s+/g,'_');const owners=new Map<string,string>();
 for(const entry of value.case_types)for(const alias of [entry.id,...entry.aliases]){const key=normalize(alias);const owner=owners.get(key);if(owner&&owner!==entry.id)ctx.addIssue({code:z.ZodIssueCode.custom,message:"Case type aliases and IDs must resolve unambiguously"});owners.set(key,entry.id);}

});

export const draftTemplateRequestSchema = z.object({
  case_type: z.string().min(1).max(200),
  index_codes: z.array(z.string().min(1).max(200)).max(50).default([]),
  rules_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  ordinance_sections: z.array(z.string().min(1).max(200)).max(50).default([]),
  issue_tags: z.array(z.string().min(1).max(200)).max(50).default([]),
  findings_text: z.string().max(24000).optional(),
  law_text: z.string().max(24000).optional(),
  template_mode: templateModeSchema.default("guided_scaffold"),
  style_mode: z.string().min(1).max(100).optional()
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
  reference_edition:referenceEditionBindingSchema.optional(),
  source_kind: z.enum(["similar_decision", "provision_candidate","unversioned_law_excerpt"]).optional(),
  source_revision: z.string().optional(),
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
  content: z.string().trim().min(1).max(assistantContextLimits.messageChars)
});

const assistantScopeText=z.string().trim().min(1).max(200);
export const assistantScopeFiltersSchema=searchFiltersSchema.omit({fileType:true,approvedOnly:true,chunkType:true}).extend({
  documentId:assistantScopeText.optional(),jurisdiction:assistantScopeText.optional(),indexCode:assistantScopeText.optional(),
  rulesSection:assistantScopeText.optional(),ordinanceSection:assistantScopeText.optional(),partyName:assistantScopeText.optional(),
  judgeName:assistantScopeText.optional(),judgeNames:z.array(assistantScopeText).max(12).optional(),
  indexCodes:z.array(assistantScopeText).max(50).optional()
}).strict().refine(value=>!value.fromDate||!value.toDate||value.fromDate<=value.toDate,{message:"fromDate must not follow toDate",path:["toDate"]});
export const assistantReferenceContextSchema=z.object({
  version:z.literal("reference-context-1"),scopeKey:z.string().regex(/^[a-f0-9]{64}$/),subjectQuery:z.string().min(1).max(2000).refine(value=>!assistantQuestionError(value),"A reference subject needs searchable text"),
  referenceDocumentIds:z.array(assistantScopeText).max(8),activeDocumentIds:z.array(assistantScopeText).max(8)
}).strict().superRefine((value,ctx)=>{
  if(new Set(value.referenceDocumentIds).size!==value.referenceDocumentIds.length || new Set(value.activeDocumentIds).size!==value.activeDocumentIds.length
    || value.activeDocumentIds.some(id=>!value.referenceDocumentIds.includes(id)))ctx.addIssue({code:z.ZodIssueCode.custom,message:"Invalid case-reference membership"});
});

export const assistantChatRequestSchema = z.object({
  messages: z.array(assistantChatMessageSchema).min(1).max(assistantContextLimits.transportMessages).superRefine((messages,ctx)=>{
    const latest=messages.at(-1);
    if(latest?.role!=="user")ctx.addIssue({code:z.ZodIssueCode.custom,message:"The request must end with a user question"});
    else {const error=assistantQuestionError(latest.content);if(error)ctx.addIssue({code:z.ZodIssueCode.custom,message:error});}
  }),
  judgeNames: z.array(assistantScopeText).max(12).default([]),
  indexCodes: z.array(assistantScopeText).max(50).default([]),
  filters:assistantScopeFiltersSchema.optional(),
  previousResearch:assistantReferenceContextSchema.optional(),
  limit: z.number().int().min(1).max(8).default(6)
}).strict();

export const assistantChatCitationSchema = z.object({
  documentId: z.string(),
  referenceNumber:z.number().int().min(1).max(8).optional(),
  additionalPassages:z.array(searchResultPassageSchema).max(2).default([]),
  title: z.string(),
  citation: z.string(),
  authorName: z.string().nullable().default(null),
  sourceLink: z.string().url(),
  primaryAuthorityPassage: searchResultPassageSchema.optional(),
  supportingFactPassage: searchResultPassageSchema.optional()
});

export const assistantClaimSchema = z.object({
  id:z.string(),text:z.string(),status:z.enum(["unverified","source_excerpt"]),flags:z.array(z.string()),
  evidence:z.array(z.object({evidenceId:z.string(),documentId:z.string(),chunkId:z.string(),sectionLabel:z.string(),citationAnchor:z.string(),
    paragraphAnchor:z.string(),quote:z.string(),excerpt:z.string(),start:z.number().int().nonnegative(),end:z.number().int().nonnegative(),
    offsetBasis:z.literal("normalized_retrieved_excerpt"),occurrenceVerified:z.literal(true)}))
});

export const assistantChatResponseSchema = z.object({
  answer: z.string(),
  assessmentVersion:z.enum(["claims-1","legacy-1"]).default("legacy-1"),
  claims:z.array(assistantClaimSchema).default([]),
  retrievedSources:z.array(assistantChatCitationSchema).default([]),
  evidenceStatus:z.enum(["unverified_claims","retrieved_excerpts","no_evidence"]).default("no_evidence"),
  evidenceCorpusRevision:z.string().nullable().default(null),
  evidenceCapturedAt:z.string().datetime().nullable().default(null),
  effectiveScope:searchFiltersSchema.nullable().default(null),
  referenceContext:assistantReferenceContextSchema.nullable().default(null),
  retrievalContext:z.object({resolution:z.enum(["standalone","ordinal","selected_case","pronoun","previous_issue","more_cases","clarification"]).default("standalone"),
    resolvedDocumentIds:z.array(z.string()).max(8).default([]),unrepresentedDocumentIds:z.array(z.string()).max(8).default([]),scopeChanged:z.boolean().default(false),clarification:z.string().nullable().default(null),
    query:z.string().max(assistantContextLimits.retrievalQueryChars),queryTruncated:z.boolean(),
    retainedMessages:z.number().int().nonnegative(),retainedChars:z.number().int().nonnegative(),requestMessagesOmitted:z.number().int().nonnegative()}).nullable().default(null),
  citations: z.array(assistantChatCitationSchema),
  scopeLabel: z.string(),
  model: z.string(),
  retrievedCount: z.number().int().nonnegative(),
  fallbackReason: z.string().nullable().default(null)
});

export const draftSectionSchema = z.object({
  id: z.string(),
  heading: z.string(),
  text: z.string(),
  citation_ids: z.array(z.string())
});

// Legacy levels remain readable for historical drafts. New assessments emit only
// unverified/unsupported until a calibrated claim verifier is introduced.
export const paragraphSupportLevelSchema = z.enum(["strong", "mixed", "weak", "unverified", "unsupported"]);

export const draftClaimEvidenceSchema = z.object({
  claim_id: z.string(),
  text: z.string(),
  kind: z.enum(["provided_fact", "legal_proposition", "unclassified"]),
  status: z.enum(["unverified", "unsupported"]),
  flags: z.array(z.string()),
  evidence: z.array(z.object({
    citation_id: z.string(), document_id: z.string(),
    text: z.string(), start: z.number().int().nonnegative(), end: z.number().int().nonnegative(),
    relation: z.enum(["exact_quote", "retrieval_similarity"]),
    flags: z.array(z.string())
  }))
});

export const draftParagraphSupportSchema = z.object({
  paragraph_id: z.string(),
  section_id: z.string(),
  text: z.string(),
  support_level: paragraphSupportLevelSchema,
  citation_ids: z.array(z.string()),
  support_notes: z.array(z.string()),
  claims: z.array(draftClaimEvidenceSchema).default([])
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
  generated_at: z.string().datetime().optional(),
  provenance: z.object({
    corpus_version: z.string().nullable(),
    sources: z.array(z.object({
      citation_id: z.string(), document_id: z.string(),
      document_revision: z.string().nullable(), extraction_version: z.string().nullable(),
      revision_status: z.enum(["captured_with_excerpt", "observed_after_retrieval", "unavailable"]).optional(),
      reference_edition:referenceEditionBindingSchema.optional(),
      excerpt_sha256: z.string()
    }))
  }).optional(),
  research_context: z.object({
    provided_findings: z.string(), supplied_law: z.string(), requested_rules: z.array(z.string()), requested_ordinance: z.array(z.string()),
    law_context:legalResearchContextSchema.optional(),
    law_scope: z.literal("jurisdiction_and_effective_edition_unverified")
  }).optional(),
  assessment_version: z.enum(["legacy-1", "retrieval-only-2"]).default("legacy-1"),
  review_required: z.boolean().default(true),
  query_summary: z.string(),
  draft_text: z.string(),
  generation_mode: z.enum(["llm", "heuristic_fallback"]).default("llm"),
  fallback_reason: z.string().nullable().default(null),
  model: z.string().nullable().default(null),
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
  code_identifier: z.string().min(1).max(200),
  family: z.string().max(2000).optional().nullable(),
  label: z.string().max(2000).optional().nullable(),
  description: z.string().max(16000).optional().nullable(),
  reserved: z.boolean().default(false),
  legacy_pre_1002: z.boolean().default(false),
  linked_ordinance_sections: z.array(z.string().max(200)).max(50).default([]),
  linked_rules_sections: z.array(z.string().max(200)).max(50).default([]),
  source_page_anchor: z.string().max(2000).optional().nullable()
}).strict();

export const legalOrdinanceSectionSchema = z.object({
  section_number: z.string().min(1).max(200),
  subsection_path: z.string().max(2000).optional().nullable(),
  heading: z.string().max(2000).optional().nullable(),
  body_text: z.string().min(1).max(200000),
  page_anchor: z.string().max(2000).optional().nullable()
}).strict();

export const legalRulesSectionSchema = z.object({
  part: z.string().max(2000).optional().nullable(),
  section_number: z.string().min(1).max(200),
  canonical_bare_citation: z.string().max(2000).optional().nullable(),
  normalized_bare_citation: z.string().max(2000).optional().nullable(),
  heading: z.string().max(2000).optional().nullable(),
  body_text: z.string().min(1).max(200000),
  page_anchor: z.string().max(2000).optional().nullable()
}).strict();

export const legalReferenceRebuildRequestSchema = z.object({
  edition:legalEditionDescriptorSchema.optional(),
  source_trace: z.object({
    index_codes: z.string().max(2000),
    ordinance: z.string().max(2000),
    rules: z.string().max(2000)
  }),
  index_codes: z.array(legalIndexCodeSchema).min(1).max(10000),
  ordinance_sections: z.array(legalOrdinanceSectionSchema).min(1).max(10000),
  rules_sections: z.array(legalRulesSectionSchema).min(1).max(10000),
  crosswalk: z
    .array(
      z.object({
        index_code: z.string().max(200).optional(),
        ordinance_section: z.string().max(200).optional(),
        rules_section: z.string().max(200).optional(),
        source: z.string().max(2000).default("normalized_import")
      }).strict()
    ).max(10000)
    .default([]),
  coverage_report: z
    .object({
      ordinance: z
        .object({
          parser_used: z.enum(["true_text", "layout_text", "pdf"]),
          source_path: z.string().max(2000).optional().nullable(),
          fallback_reason: z.string().max(2000).optional().nullable(),
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
          source_path: z.string().max(2000).optional().nullable(),
          fallback_reason: z.string().max(2000).optional().nullable(),
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
}).strict();

export const legalReferenceInspectResponseSchema = z.object({
  referenceEdition:referenceEditionBindingSchema.optional(),
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
  citations: z.array(z.string().trim().min(1).max(200)).min(1).max(50)
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

export const documentApprovalSchema = z.object({ expectedRevision: z.number().int().positive().safe() }).strict();

export const adminIngestionMetadataUpdateSchema = z.object({
  expectedRevision: z.number().int().positive().safe(),
  mutationId: z.string().uuid(),
  index_codes: z.array(z.string().min(1).max(200).refine(value=>value.trim().length>0,"Must contain text")).max(12).optional(),
  rules_sections: z.array(z.string().min(1).max(200).refine(value=>value.trim().length>0,"Must contain text")).max(12).optional(),
  ordinance_sections: z.array(z.string().min(1).max(200).refine(value=>value.trim().length>0,"Must contain text")).max(12).optional(),
  case_number: z.string().max(200).optional().nullable(),
  decision_date: isoDateSchema.optional().nullable(),
  author_name: z.string().max(200).optional().nullable(),
  outcome_label: z.enum(["grant", "deny", "partial", "unclear"]).optional(),
  confirm_required_metadata: z.boolean().optional()
}).strict().refine(value => Object.keys(value).some(key => key !== "expectedRevision" && key !== "mutationId"), {
  message: "Supply at least one metadata field or confirmation decision"
});

export const adminIngestionRejectSchema = z.object({
  expectedRevision: z.number().int().positive().safe(),
  mutationId: z.string().uuid(),
  reason: z.string().min(3).max(2000).refine(value=>value.trim().length>=3,"Supply a reason with at least three non-space characters")
}).strict();

export type FileType = z.infer<typeof fileTypeSchema>;
export type IngestDocumentInput = z.infer<typeof ingestDocumentSchema>;
export type SearchRequest = z.infer<typeof searchRequestSchema>;
export type SearchResponse = z.infer<typeof searchResponseSchema>;
export type SearchDebugRequest = z.infer<typeof searchDebugRequestSchema>;
export type SearchDebugResponse = z.infer<typeof searchDebugResponseSchema>;
export type RetrievalPreviewDocument = z.infer<typeof retrievalPreviewDocumentSchema>;
export type RetrievalPreviewChunk = z.infer<typeof retrievalPreviewChunkSchema>;
export type RetrievalPreviewResponse = z.infer<typeof retrievalPreviewResponseSchema>;
export type DashboardSummary = z.infer<typeof dashboardSummarySchema>;
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

export type LegalEditionDescriptor=z.infer<typeof legalEditionDescriptorSchema>;
export type LegalResearchContext=z.infer<typeof legalResearchContextSchema>;
export type ReferenceEditionBinding=z.infer<typeof referenceEditionBindingSchema>;

/** Display only fixed operational categories, never an upstream error payload. */
export function assistantGenerationStatus(reason:string|null|undefined):string|null {
 if(!reason)return null;
 const labels:Record<string,string>={
  unconfigured:"Model generation is not configured; showing retrieved excerpts.",
  no_evidence:"No usable evidence was retrieved.",
  clarification_required:"Clarification is needed before searching.",
  timeout:"The model request timed out; showing retrieved excerpts.",
  cancelled:"Model generation was cancelled; showing retrieved excerpts.",
  provider_rate_limited:"The model provider is rate limited; showing retrieved excerpts.",
  provider_rejected:"The model provider rejected the request; showing retrieved excerpts.",
  network_failure:"The model provider could not be reached; showing retrieved excerpts."
 };
 return labels[reason]||"The model response could not be used; showing retrieved excerpts for review.";
}
