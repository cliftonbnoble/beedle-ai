import {
  assistantChatRequestSchema,
  assistantChatResponseSchema,
  caseAssistantRequestSchema,
  caseAssistantResponseSchema,
  draftConclusionsDebugResponseSchema,
  draftConclusionsRequestSchema,
  draftConclusionsResponseSchema,
  draftTemplateRequestSchema,
  draftTemplateResponseSchema,
  draftExportRequestSchema,
  draftExportResponseSchema,
  taxonomyConfigInspectResponseSchema,
  taxonomyResolveResponseSchema,
  legalReferenceInspectResponseSchema,
  searchRequestSchema,
  searchResponseSchema,
  type AssistantChatRequest,
  type AssistantChatResponse,
  type CaseAssistantRequest,
  type CaseAssistantResponse,
  type DraftConclusionsDebugResponse,
  type DraftConclusionsRequest,
  type DraftConclusionsResponse,
  type DraftTemplateRequest,
  type DraftTemplateResponse,
  type DraftExportRequest,
  type DraftExportResponse,
  type SearchDebugRequest,
  type SearchDebugResponse,
  type SearchRequest,
  type SearchResponse
} from "@beedle/shared";

export const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL || "https://beedle-api.clifton23.workers.dev";

export type RetrievalPreviewDocument = {
  documentId: string;
  title: string;
  citation: string;
  jurisdiction: string;
  authorName: string | null;
  decisionDate: string | null;
  sourceFileRef: string;
  sourceLink: string;
  fileType: "decision_docx";
  sections: Array<{
    sectionId: string;
    canonicalKey: string;
    heading: string;
    sectionOrder: number;
    paragraphCount: number;
  }>;
  validReferences: {
    indexCodes: string[];
    rulesSections: string[];
    ordinanceSections: string[];
  };
};

export type RetrievalPreviewChunk = {
  chunkId: string;
  documentId: string;
  title: string;
  citation: string;
  chunkType: string;
  chunkOrdinal: number;
  sectionLabel: string;
  paragraphAnchorStart: string;
  paragraphAnchorEnd: string;
  sourceText: string;
  provenance: {
    sourceFileRef: string;
    sourceLink: string;
    sectionId: string;
    sectionLabel: string;
  };
};

export type RetrievalPreviewResponse = {
  document: RetrievalPreviewDocument;
  chunks: RetrievalPreviewChunk[];
};

export type DashboardSummary = {
  searchableDecisionCount: number;
};

async function fetchJson(path: string, init?: RequestInit) {
  const response = await fetch(`${apiBase}${path}`, {
    credentials: "include",
    ...init
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`API failed (${response.status}) for ${path}: ${body}`);
  }
  return response.json();
}

export async function runSearch(input: SearchRequest): Promise<SearchResponse> {
  const payload = searchRequestSchema.parse(input);
  const json = await fetchJson("/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return searchResponseSchema.parse(json);
}

export async function getDecisionRetrievalPreview(documentId: string): Promise<RetrievalPreviewResponse> {
  return fetchJson(`/admin/retrieval/documents/${encodeURIComponent(documentId)}/chunks?includeText=1`) as Promise<RetrievalPreviewResponse>;
}

export async function getDashboardSummary(): Promise<DashboardSummary> {
  return fetchJson("/admin/dashboard/summary") as Promise<DashboardSummary>;
}

export async function runCaseAssistant(input: CaseAssistantRequest): Promise<CaseAssistantResponse> {
  const payload = caseAssistantRequestSchema.parse(input);
  const json = await fetchJson("/api/case-assistant", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return caseAssistantResponseSchema.parse(json);
}

export async function runAssistantChat(input: AssistantChatRequest): Promise<AssistantChatResponse> {
  const payload = assistantChatRequestSchema.parse(input);
  const json = await fetchJson("/api/assistant/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return assistantChatResponseSchema.parse(json);
}

export async function runDraftConclusions(input: DraftConclusionsRequest): Promise<DraftConclusionsResponse> {
  const payload = draftConclusionsRequestSchema.parse(input);
  const json = await fetchJson("/api/draft/conclusions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return draftConclusionsResponseSchema.parse(json);
}

export async function runDraftConclusionsDebug(input: DraftConclusionsRequest): Promise<DraftConclusionsDebugResponse> {
  const payload = draftConclusionsRequestSchema.parse(input);
  const json = await fetchJson("/admin/draft/debug", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return draftConclusionsDebugResponseSchema.parse(json);
}

export async function runDraftTemplate(input: DraftTemplateRequest): Promise<DraftTemplateResponse> {
  const payload = draftTemplateRequestSchema.parse(input);
  const json = await fetchJson("/api/draft/template", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return draftTemplateResponseSchema.parse(json);
}

export async function runDraftExport(input: DraftExportRequest): Promise<DraftExportResponse> {
  const payload = draftExportRequestSchema.parse(input);
  const json = await fetchJson("/api/draft/export", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return draftExportResponseSchema.parse(json);
}

export async function getTaxonomyConfig() {
  const json = await fetchJson("/admin/config/taxonomy");
  return taxonomyConfigInspectResponseSchema.parse(json);
}

export async function resolveTaxonomyCaseType(caseType: string) {
  const json = await fetchJson("/admin/config/taxonomy/resolve", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: caseType })
  });
  return taxonomyResolveResponseSchema.parse(json);
}

export async function listIngestionDocuments(params?: {
  status?: "all" | "staged" | "searchable" | "approved" | "rejected" | "pending";
  fileType?: "decision_docx" | "law_pdf";
  hasWarnings?: boolean;
  missingRequired?: boolean;
  unresolvedReferencesOnly?: boolean;
  criticalExceptionsOnly?: boolean;
  filteredNoiseOnly?: boolean;
  lowConfidenceTaxonomyOnly?: boolean;
  missingRulesOnly?: boolean;
  missingOrdinanceOnly?: boolean;
  approvalReadyOnly?: boolean;
  reviewerReadyOnly?: boolean;
  unresolvedTriageBucket?: string;
  recurringCitationFamily?: string;
  blocked37xOnly?: boolean;
  blocked37xFamily?: string;
  blocked37xBatchKey?: string;
  safeToBatchReviewOnly?: boolean;
  estimatedReviewerEffort?: "low" | "medium" | "high";
  reviewerRiskLevel?: "low" | "medium" | "high";
  blocker?: string;
  runtimeManualCandidatesOnly?: boolean;
  realOnly?: boolean;
  taxonomyCaseTypeId?: string;
  query?: string;
  sort?:
    | "createdAtDesc"
    | "createdAtAsc"
    | "confidenceDesc"
    | "confidenceAsc"
    | "titleAsc"
    | "titleDesc"
    | "warningCountDesc"
    | "unresolvedReferenceDesc"
    | "criticalExceptionDesc"
    | "approvalReadinessDesc"
    | "reviewerReadinessDesc"
    | "reviewerEffortAsc"
    | "batchabilityDesc"
    | "unresolvedLeverageDesc"
    | "blocked37xBatchKeyAsc";
  limit?: number;
}) {
  const search = new URLSearchParams();
  if (params?.status) search.set("status", params.status);
  if (params?.fileType) search.set("fileType", params.fileType);
  if (params?.hasWarnings) search.set("hasWarnings", "1");
  if (params?.missingRequired) search.set("missingRequired", "1");
  if (params?.unresolvedReferencesOnly) search.set("unresolvedReferencesOnly", "1");
  if (params?.criticalExceptionsOnly) search.set("criticalExceptionsOnly", "1");
  if (params?.filteredNoiseOnly) search.set("filteredNoiseOnly", "1");
  if (params?.lowConfidenceTaxonomyOnly) search.set("lowConfidenceTaxonomyOnly", "1");
  if (params?.missingRulesOnly) search.set("missingRulesOnly", "1");
  if (params?.missingOrdinanceOnly) search.set("missingOrdinanceOnly", "1");
  if (params?.approvalReadyOnly) search.set("approvalReadyOnly", "1");
  if (params?.reviewerReadyOnly) search.set("reviewerReadyOnly", "1");
  if (params?.unresolvedTriageBucket) search.set("unresolvedTriageBucket", params.unresolvedTriageBucket);
  if (params?.recurringCitationFamily) search.set("recurringCitationFamily", params.recurringCitationFamily);
  if (params?.blocked37xOnly) search.set("blocked37xOnly", "1");
  if (params?.blocked37xFamily) search.set("blocked37xFamily", params.blocked37xFamily);
  if (params?.blocked37xBatchKey) search.set("blocked37xBatchKey", params.blocked37xBatchKey);
  if (params?.safeToBatchReviewOnly) search.set("safeToBatchReviewOnly", "1");
  if (params?.estimatedReviewerEffort) search.set("estimatedReviewerEffort", params.estimatedReviewerEffort);
  if (params?.reviewerRiskLevel) search.set("reviewerRiskLevel", params.reviewerRiskLevel);
  if (params?.blocker) search.set("blocker", params.blocker);
  if (params?.runtimeManualCandidatesOnly) search.set("runtimeManualCandidatesOnly", "1");
  if (params?.realOnly) search.set("realOnly", "1");
  if (params?.taxonomyCaseTypeId) search.set("taxonomyCaseTypeId", params.taxonomyCaseTypeId);
  if (params?.query) search.set("query", params.query);
  if (params?.sort) search.set("sort", params.sort);
  if (typeof params?.limit === "number") search.set("limit", String(params.limit));
  const query = search.toString();
  return fetchJson(`/admin/ingestion/documents${query ? `?${query}` : ""}`);
}

export async function getIngestionDocument(documentId: string) {
  return fetchJson(`/admin/ingestion/documents/${documentId}`);
}

export async function updateIngestionMetadata(documentId: string, payload: Record<string, unknown>) {
  return fetchJson(`/admin/ingestion/documents/${documentId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function approveIngestionDocument(documentId: string) {
  return fetchJson(`/admin/ingestion/documents/${documentId}/approve`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({})
  });
}

export async function rejectIngestionDocument(documentId: string, reason: string) {
  return fetchJson(`/admin/ingestion/documents/${documentId}/reject`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ reason })
  });
}

export async function runRetrievalDebug(input: SearchDebugRequest): Promise<SearchDebugResponse> {
  const json = await fetchJson("/admin/retrieval/debug", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input)
  });
  return json as SearchDebugResponse;
}

export async function inspectNormalizedReferences() {
  const json = await fetchJson("/admin/references");
  return legalReferenceInspectResponseSchema.parse(json);
}

function buildReviewerExportSearch(params?: {
  realOnly?: boolean;
  unresolvedTriageBucket?: string;
  blocked37xFamily?: string;
  estimatedReviewerEffort?: "low" | "medium" | "high";
  reviewerRiskLevel?: "low" | "medium" | "high";
  safeToBatchReviewOnly?: boolean;
  blocked37xBatchKey?: string;
  blocked37xOnly?: boolean;
  limit?: number;
  format?: "json" | "csv" | "markdown";
}) {
  const search = new URLSearchParams();
  if (typeof params?.realOnly === "boolean") search.set("realOnly", params.realOnly ? "1" : "0");
  if (params?.unresolvedTriageBucket) search.set("unresolvedTriageBucket", params.unresolvedTriageBucket);
  if (params?.blocked37xFamily) search.set("blocked37xFamily", params.blocked37xFamily);
  if (params?.estimatedReviewerEffort) search.set("estimatedReviewerEffort", params.estimatedReviewerEffort);
  if (params?.reviewerRiskLevel) search.set("reviewerRiskLevel", params.reviewerRiskLevel);
  if (params?.safeToBatchReviewOnly) search.set("safeToBatchReviewOnly", "1");
  if (params?.blocked37xBatchKey) search.set("blocked37xBatchKey", params.blocked37xBatchKey);
  if (params?.blocked37xOnly) search.set("blocked37xOnly", "1");
  if (typeof params?.limit === "number") search.set("limit", String(params.limit));
  if (params?.format) search.set("format", params.format);
  return search.toString();
}

export function reviewerExportUrl(params?: {
  realOnly?: boolean;
  unresolvedTriageBucket?: string;
  blocked37xFamily?: string;
  estimatedReviewerEffort?: "low" | "medium" | "high";
  reviewerRiskLevel?: "low" | "medium" | "high";
  safeToBatchReviewOnly?: boolean;
  blocked37xBatchKey?: string;
  blocked37xOnly?: boolean;
  limit?: number;
  format?: "json" | "csv" | "markdown";
}) {
  const query = buildReviewerExportSearch(params);
  return `${apiBase}/admin/ingestion/reviewer-export${query ? `?${query}` : ""}`;
}

export function reviewerAdjudicationTemplateUrl(params?: {
  realOnly?: boolean;
  unresolvedTriageBucket?: string;
  blocked37xFamily?: string;
  estimatedReviewerEffort?: "low" | "medium" | "high";
  reviewerRiskLevel?: "low" | "medium" | "high";
  safeToBatchReviewOnly?: boolean;
  blocked37xBatchKey?: string;
  blocked37xOnly?: boolean;
  limit?: number;
  format?: "json" | "csv";
}) {
  const query = buildReviewerExportSearch(params);
  return `${apiBase}/admin/ingestion/reviewer-adjudication-template${query ? `?${query}` : ""}`;
}
