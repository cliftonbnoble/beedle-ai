import { AutoRouter } from "itty-router";
import { handleIngest, handleIngestMultipart, handleDecisionApproval } from "./routes/ingest";
import { handleSearch } from "./routes/search";
import { handleCaseAssistant } from "./routes/case-assistant";
import { handleAssistantChat } from "./routes/assistant-chat";
import { handleDraftConclusions, handleDraftConclusionsDebug } from "./routes/draft-conclusions";
import { handleDraftTemplate } from "./routes/draft-template";
import { handleDraftExport } from "./routes/draft-export";
import { handleDecisionRetrievalChunks, handleDecisionRetrievalChunksDebug, handleRetrievalDebug } from "./routes/retrieval-debug";
import { handleRollbackRetrievalActivation, handleWriteRetrievalActivation } from "./routes/admin-retrieval-activation";
import { handleBackfillRetrievalVectors } from "./routes/admin-retrieval-vectors";
import { handleProbeRetrievalVectors } from "./routes/admin-retrieval-vector-probe";
import { handleGetSource } from "./routes/source";
import {
  handleGetTaxonomyConfig,
  handleResolveTaxonomyCaseType,
  handleValidateTaxonomyConfig
} from "./routes/admin-config";
import {
  handleBackfillReferenceValidation,
  handleInspectReferences,
  handleRulesCitationInventory,
  handleRebuildReferences,
  handleVerifyReferenceCitations
} from "./routes/admin-references";
import {
  handleApproveIngestionDocument,
  handleBulkEnableSearchability,
  handleReviewerAdjudicationTemplate,
  handleReviewerBatchExport,
  handleGetIngestionDocument,
  handleListBulkSearchabilityCandidates,
  handleListIngestionDocuments,
  handleReprocessIngestionDocument,
  handleRejectIngestionDocument,
  handleUpdateIngestionMetadata
} from "./routes/admin-ingestion";
import { handleDashboardSummary } from "./routes/admin-dashboard";
import { json } from "./lib/http";
import type { Env } from "./lib/types";

const router = AutoRouter({
  before: [
    (request: Request, env: Env) => {
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
          headers: corsHeaders(request, env)
        });
      }
      return undefined;
    }
  ],
  finally: []
});

router.get("/health", (_request: Request, env: Env) =>
  json({
    ok: true,
    aiAvailable: Boolean(env.AI),
    vectorNamespace: env.VECTOR_NAMESPACE,
    vectorBindingPresent: Boolean(env.VECTOR_INDEX),
    embeddingModel: env.AI_EMBEDDING_MODEL
  })
);
router.post("/ingest", (request: Request, env: Env) => handleIngest(request, env));
router.post("/ingest/decision", (request: Request, env: Env) => handleIngest(request, env, "decision_docx"));
router.post("/ingest/decision-upload", (request: Request, env: Env) => handleIngestMultipart(request, env, "decision_docx"));
router.post("/ingest/law", (request: Request, env: Env) => handleIngest(request, env, "law_pdf"));
router.post("/decisions/:documentId/approve", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) {
    return json({ error: "documentId is required" }, { status: 400 });
  }
  return handleDecisionApproval(request, env, documentId);
});
router.post("/search", (request: Request, env: Env) => handleSearch(request, env));
router.post("/admin/retrieval/debug", (request: Request, env: Env) => handleRetrievalDebug(request, env));
router.get("/admin/retrieval/documents/:documentId/chunks", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleDecisionRetrievalChunks(request, env, documentId);
});
router.get("/admin/retrieval/documents/:documentId/chunks-debug", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleDecisionRetrievalChunksDebug(request, env, documentId);
});
router.post("/admin/retrieval/activation/write", (request: Request, env: Env) => handleWriteRetrievalActivation(request, env));
router.post("/admin/retrieval/activation/rollback", (request: Request, env: Env) => handleRollbackRetrievalActivation(request, env));
router.post("/admin/retrieval/vectors/backfill", (request: Request, env: Env) => handleBackfillRetrievalVectors(request, env));
router.post("/admin/retrieval/vectors/probe", (request: Request, env: Env) => handleProbeRetrievalVectors(request, env));
router.post("/api/case-assistant", (request: Request, env: Env) => handleCaseAssistant(request, env));
router.post("/case-assistant", (request: Request, env: Env) => handleCaseAssistant(request, env));
router.post("/api/assistant/chat", (request: Request, env: Env) => handleAssistantChat(request, env));
router.post("/assistant/chat", (request: Request, env: Env) => handleAssistantChat(request, env));
router.post("/api/draft/conclusions", (request: Request, env: Env) => handleDraftConclusions(request, env));
router.post("/draft/conclusions", (request: Request, env: Env) => handleDraftConclusions(request, env));
router.post("/api/draft/template", (request: Request, env: Env) => handleDraftTemplate(request, env));
router.post("/draft/template", (request: Request, env: Env) => handleDraftTemplate(request, env));
router.post("/api/draft/export", (request: Request, env: Env) => handleDraftExport(request, env));
router.post("/draft/export", (request: Request, env: Env) => handleDraftExport(request, env));
router.post("/admin/draft/debug", (request: Request, env: Env) => handleDraftConclusionsDebug(request, env));
router.get("/admin/config/taxonomy", (request: Request, env: Env) => handleGetTaxonomyConfig(request, env));
router.post("/admin/config/taxonomy/resolve", (request: Request, env: Env) => handleResolveTaxonomyCaseType(request, env));
router.post("/admin/config/taxonomy/validate", (request: Request, env: Env) => handleValidateTaxonomyConfig(request, env));
router.get("/admin/references", (request: Request, env: Env) => handleInspectReferences(request, env));
router.get("/admin/references/rules", (request: Request, env: Env) => handleRulesCitationInventory(request, env));
router.post("/admin/references/rebuild", (request: Request, env: Env) => handleRebuildReferences(request, env));
router.post("/admin/references/backfill", (request: Request, env: Env) => handleBackfillReferenceValidation(request, env));
router.post("/admin/references/verify-citations", (request: Request, env: Env) => handleVerifyReferenceCitations(request, env));
router.get("/source/:documentId", (request: Request, env: Env) => handleGetSource(request, env));
router.get("/admin/ingestion/documents", (request: Request, env: Env) => handleListIngestionDocuments(request, env));
router.get("/admin/dashboard/summary", (request: Request, env: Env) => handleDashboardSummary(request, env));
router.get("/admin/ingestion/searchability/candidates", (request: Request, env: Env) =>
  handleListBulkSearchabilityCandidates(request, env)
);
router.post("/admin/ingestion/searchability/enable", (request: Request, env: Env) =>
  handleBulkEnableSearchability(request, env)
);
router.get("/admin/ingestion/reviewer-export", (request: Request, env: Env) => handleReviewerBatchExport(request, env));
router.get("/admin/ingestion/reviewer-adjudication-template", (request: Request, env: Env) =>
  handleReviewerAdjudicationTemplate(request, env)
);
router.get("/admin/ingestion/documents/:documentId", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleGetIngestionDocument(request, env, documentId);
});
router.post("/admin/ingestion/documents/:documentId/metadata", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleUpdateIngestionMetadata(request, env, documentId);
});
router.post("/admin/ingestion/documents/:documentId/approve", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleApproveIngestionDocument(request, env, documentId);
});
router.post("/admin/ingestion/documents/:documentId/reject", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleRejectIngestionDocument(request, env, documentId);
});
router.post("/admin/ingestion/documents/:documentId/reprocess", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleReprocessIngestionDocument(request, env, documentId);
});

export default {
  fetch: async (request: Request, env: Env, ctx: ExecutionContext) => {
    const response = await router.fetch(request, env, ctx);
    return withCors(response, request, env);
  }
};

function corsHeaders(request: Request, env: Env) {
  const headers: Record<string, string> = {
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers":
      request.headers.get("access-control-request-headers") ||
      "content-type, cf-access-client-id, cf-access-client-secret"
  };

  const origin = request.headers.get("origin");
  const configuredOrigins = parseAllowedOrigins(env.CORS_ALLOWED_ORIGINS);
  if (!configuredOrigins.length) {
    headers["access-control-allow-origin"] = "*";
    return headers;
  }

  const normalizedOrigin = normalizeOrigin(origin);
  if (normalizedOrigin && configuredOrigins.includes(normalizedOrigin)) {
    headers["access-control-allow-origin"] = normalizedOrigin;
    headers["access-control-allow-credentials"] = "true";
    headers.vary = "Origin";
  }

  return headers;
}

function withCors(response: Response, request: Request, env: Env): Response {
  const headers = new Headers(response.headers);
  for (const [key, value] of Object.entries(corsHeaders(request, env))) {
    headers.set(key, value);
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

function parseAllowedOrigins(input: string | undefined): string[] {
  return String(input || "")
    .split(",")
    .map((value) => normalizeOrigin(value))
    .filter((value): value is string => Boolean(value));
}

function normalizeOrigin(origin: string | null | undefined): string | null {
  if (!origin) return null;
  return origin.trim().replace(/\/+$/, "") || null;
}
