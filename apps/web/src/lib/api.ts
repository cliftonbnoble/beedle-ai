import {
  assistantChatRequestSchema,
  assistantChatResponseSchema,
  draftConclusionsRequestSchema,
  draftConclusionsResponseSchema,
  draftExportRequestSchema,
  draftExportResponseSchema,
  dashboardSummarySchema,
  taxonomyConfigInspectResponseSchema,
  taxonomyResolveResponseSchema,
  legalReferenceInspectResponseSchema,
  retrievalPreviewResponseSchema,
  searchDebugResponseSchema,
  searchRequestSchema,
  searchResponseSchema,
  type AssistantChatRequest,
  type AssistantChatResponse,
  type DraftConclusionsRequest,
  type DraftConclusionsResponse,
  type DraftExportRequest,
  type DraftExportResponse,
  type DashboardSummary,
  type RetrievalPreviewResponse,
  type SearchDebugRequest,
  type SearchDebugResponse,
  type SearchRequest,
  type SearchResponse
} from "@beedle/shared";

export type { DashboardSummary, RetrievalPreviewResponse };

export const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL || "https://beedle-api.clifton23.workers.dev";

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

// Pages render err.message directly, so it must be something a user can act on — never a raw status
// line, response body, or zod dump (the technical detail goes to console.error for debugging). 4xx
// bodies carry crafted validation messages from the API (see its toErrorResponse), so those pass
// through; 5xx bodies are generic by design and map to a retry message.
function userSafeApiMessage(status: number, body: string): string {
  let serverError = "";
  try {
    const parsed = JSON.parse(body) as { error?: unknown };
    if (parsed && typeof parsed.error === "string") serverError = parsed.error;
  } catch {
    // non-JSON body (HTML error page etc.) — never show it
  }
  if (status >= 500) return "The service hit a temporary problem. Please try again.";
  return serverError || "The request couldn't be processed. Please check your input and try again.";
}

// Response-shape validation failures (schema drift) are developer errors, not user errors: log the
// zod detail, show a plain sentence.
function parseResponse<T>(schema: { parse: (value: unknown) => T }, json: unknown, label: string): T {
  try {
    return schema.parse(json);
  } catch (error) {
    console.error(`Unexpected ${label} response shape:`, error);
    throw new Error(`Received an unexpected ${label} response from the service. Please retry.`);
  }
}

async function fetchJson(path: string, init?: RequestInit) {
  const response = await fetch(`${apiBase}${path}`, {
    credentials: "include",
    ...init
  });
  if (!response.ok) {
    const body = await response.text();
    console.error(`API failed (${response.status}) for ${path}: ${body}`);
    throw new ApiError(response.status, userSafeApiMessage(response.status, body));
  }
  return response.json();
}

// Lightweight shape guard for the large, evolving admin-ingestion responses. A full zod
// schema for these payloads would be brittle (and would reject valid responses on any field
// drift); instead we validate the top-level structure the UI relies on so a null/array/error
// shaped or `documents`-less response fails loudly here rather than as an opaque crash deep in
// render. Returns the original value (typed `any`) so consuming components keep their own types.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function expectObjectResponse(json: unknown, label: string, requireArrayKey?: string): any {
  const isPlainObject = json !== null && typeof json === "object" && !Array.isArray(json);
  const hasRequiredArray =
    !requireArrayKey || (isPlainObject && Array.isArray((json as Record<string, unknown>)[requireArrayKey]));
  if (!isPlainObject || !hasRequiredArray) {
    throw new Error(`Unexpected ${label} response shape`);
  }
  return json;
}

export async function runSearch(input: SearchRequest, options: { signal?: AbortSignal } = {}): Promise<SearchResponse> {
  const payload = searchRequestSchema.parse(input);
  const json = await fetchJson("/search", {
    method: "POST",
    signal: options.signal,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return parseResponse(searchResponseSchema, json, "search");
}

export async function getDecisionRetrievalPreview(documentId: string): Promise<RetrievalPreviewResponse> {
  const json = await fetchJson(`/admin/retrieval/documents/${encodeURIComponent(documentId)}/chunks?includeText=1`);
  return parseResponse(retrievalPreviewResponseSchema, json, "retrieval preview");
}

export async function getDashboardSummary(): Promise<DashboardSummary> {
  const json = await fetchJson("/admin/dashboard/summary");
  return parseResponse(dashboardSummarySchema, json, "dashboard summary");
}

export async function runAssistantChat(input: AssistantChatRequest): Promise<AssistantChatResponse> {
  const payload = assistantChatRequestSchema.parse(input);
  const json = await fetchJson("/api/assistant/chat", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return parseResponse(assistantChatResponseSchema, json, "assistant chat");
}

export async function runDraftConclusions(input: DraftConclusionsRequest): Promise<DraftConclusionsResponse> {
  const payload = draftConclusionsRequestSchema.parse(input);
  const json = await fetchJson("/api/draft/conclusions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return parseResponse(draftConclusionsResponseSchema, json, "draft conclusions");
}

export async function runDraftExport(input: DraftExportRequest): Promise<DraftExportResponse> {
  const payload = draftExportRequestSchema.parse(input);
  const json = await fetchJson("/api/draft/export", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  return parseResponse(draftExportResponseSchema, json, "draft export");
}

export async function getTaxonomyConfig() {
  const json = await fetchJson("/admin/config/taxonomy");
  return parseResponse(taxonomyConfigInspectResponseSchema, json, "taxonomy config");
}

export async function resolveTaxonomyCaseType(caseType: string) {
  const json = await fetchJson("/admin/config/taxonomy/resolve", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: caseType })
  });
  return parseResponse(taxonomyResolveResponseSchema, json, "taxonomy resolve");
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
  const json = await fetchJson(`/admin/ingestion/documents${query ? `?${query}` : ""}`);
  return expectObjectResponse(json, "ingestion documents list", "documents");
}

export async function getIngestionDocument(documentId: string) {
  const json = await fetchJson(`/admin/ingestion/documents/${documentId}`);
  return expectObjectResponse(json, "ingestion document");
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
  return parseResponse(searchDebugResponseSchema, json, "search debug");
}

export async function inspectNormalizedReferences() {
  const json = await fetchJson("/admin/references");
  return parseResponse(legalReferenceInspectResponseSchema, json, "reference inspect");
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
