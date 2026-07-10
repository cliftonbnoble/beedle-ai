import { AutoRouter } from "itty-router";
import { handleIngest, handleIngestMultipart, handleDecisionApproval } from "./routes/ingest";
import { handleSearch } from "./routes/search";
import { handleCaseAssistant } from "./routes/case-assistant";
import { handleAssistantChat } from "./routes/assistant-chat";
import { handleDraftConclusions, handleDraftConclusionsDebug } from "./routes/draft-conclusions";
import { handleDraftTemplate } from "./routes/draft-template";
import { handleDraftExport } from "./routes/draft-export";
import { handleDecisionRetrievalChunks, handleRetrievalDebug } from "./routes/retrieval-debug";
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
import {
  authActorKey,
  authorizeRequest,
  handleAuthLogin,
  handleAuthLogout,
  handleAuthSession,
  type AuthContext
} from "./lib/auth";
import { json } from "./lib/http";
import type { Env } from "./lib/types";
import {
  markVectorJobFailed,
  processDocumentVectorJob,
  recordVectorJobRetry,
  requeueStaleVectorJobs,
  type VectorJobMessage
} from "./services/vector-jobs";

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
router.get("/auth/session", (request: Request, env: Env) => handleAuthSession(request, env));
router.post("/auth/login", (request: Request, env: Env) => handleAuthLogin(request, env));
router.post("/auth/logout", (_request: Request, env: Env) => handleAuthLogout(env));
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
router.post("/admin/retrieval/activation/write", (request: Request, env: Env) => handleWriteRetrievalActivation(request, env));
router.post("/admin/retrieval/activation/rollback", (request: Request, env: Env) => handleRollbackRetrievalActivation(request, env));
router.post("/admin/retrieval/vectors/backfill", (request: Request, env: Env) => handleBackfillRetrievalVectors(request, env));
router.post("/admin/retrieval/vectors/probe", (request: Request, env: Env) => handleProbeRetrievalVectors(request, env));
router.post("/api/case-assistant", (request: Request, env: Env) => handleCaseAssistant(request, env));
router.post("/api/assistant/chat", (request: Request, env: Env) => handleAssistantChat(request, env));
router.post("/api/draft/conclusions", (request: Request, env: Env) => handleDraftConclusions(request, env));
router.post("/api/draft/template", (request: Request, env: Env) => handleDraftTemplate(request, env));
router.post("/api/draft/export", (request: Request, env: Env) => handleDraftExport(request, env));
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
    const auth = await authorizeRequest(request, env);
    if (!auth.ok) return withCors(auth.response, request, env);
    const limited = await enforceCostControls(request, env, auth.user);
    if (limited) return withCors(limited, request, env);
    const response = await router.fetch(request, env, ctx);
    return withCors(response, request, env);
  },
  scheduled: async (_controller: ScheduledController, env: Env, ctx: ExecutionContext) => {
    ctx.waitUntil(requeueStaleVectorJobs(env));
  },
  queue: async (batch: MessageBatch<VectorJobMessage>, env: Env) => {
    for (const message of batch.messages) {
      try {
        await processDocumentVectorJob(env, message.body.documentId);
        message.ack();
      } catch (error) {
        if (message.attempts >= 4) {
          await markVectorJobFailed(env, message.body.documentId, error);
          message.ack();
          continue;
        }
        await recordVectorJobRetry(env, message.body.documentId, error);
        message.retry({ delaySeconds: Math.min(60 * 2 ** message.attempts, 3600) });
      }
    }
  }
};

type CostControl = {
  limiter: RateLimit;
  bucket: string;
  retryAfterSeconds: number;
};

function costControlFor(request: Request, env: Env): CostControl | null {
  if (request.method !== "POST") return null;
  const path = new URL(request.url).pathname;

  if (path === "/search" || path === "/admin/retrieval/debug") {
    return { limiter: env.SEARCH_RATE_LIMIT, bucket: "search", retryAfterSeconds: 60 };
  }
  if (
    path === "/ingest/decision" ||
    path === "/ingest/decision-upload" ||
    path === "/ingest/law" ||
    path === "/admin/retrieval/vectors/backfill" ||
    path === "/admin/retrieval/vectors/probe"
  ) {
    return { limiter: env.INGEST_RATE_LIMIT, bucket: "ingest", retryAfterSeconds: 60 };
  }
  if (path === "/api/case-assistant" || path === "/api/assistant/chat" || path === "/api/draft/conclusions") {
    return { limiter: env.LLM_RATE_LIMIT, bucket: "llm", retryAfterSeconds: 60 };
  }
  if (
    path === "/admin/retrieval/activation/write" ||
    path === "/admin/retrieval/activation/rollback" ||
    path === "/admin/references/rebuild" ||
    path === "/admin/references/backfill" ||
    path === "/admin/references/verify-citations" ||
    path === "/admin/ingestion/searchability/enable" ||
    /^\/admin\/ingestion\/documents\/[^/]+\/(?:metadata|approve|reject|reprocess)$/.test(path)
  ) {
    return { limiter: env.ADMIN_WRITE_RATE_LIMIT, bucket: "admin-write", retryAfterSeconds: 60 };
  }
  return null;
}

async function enforceCostControls(request: Request, env: Env, user: AuthContext | null): Promise<Response | null> {
  const control = costControlFor(request, env);
  if (!control) return null;
  try {
    const outcome = await control.limiter.limit({ key: authActorKey(request, user, control.bucket) });
    if (outcome.success) return null;
    return json(
      { error: "Request quota exceeded. Please retry later." },
      { status: 429, headers: { "retry-after": String(control.retryAfterSeconds) } }
    );
  } catch (error) {
    // These routes incur external compute/storage cost. Serving them without an active enforcement
    // binding defeats the control, so fail closed rather than silently allowing unlimited traffic.
    console.error("Rate limiter unavailable", { bucket: control.bucket, error });
    return json(
      { error: "Cost-control service is temporarily unavailable. Please retry later." },
      { status: 503, headers: { "retry-after": String(control.retryAfterSeconds) } }
    );
  }
}

function corsHeaders(request: Request, env: Env) {
  const headers: Record<string, string> = {
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers":
      request.headers.get("access-control-request-headers") ||
      "content-type, x-beedle-csrf, cf-access-client-id, cf-access-client-secret"
  };

  const origin = request.headers.get("origin");
  // Fail closed: only origins explicitly listed in CORS_ALLOWED_ORIGINS are allowed. When the allowlist is
  // unset/empty, no cross-origin is permitted — there is no hardcoded fallback (which previously baked the
  // prod origin into source). `wrangler.toml [vars]` sets CORS_ALLOWED_ORIGINS for every environment.
  const allowedOrigins = parseAllowedOrigins(env.CORS_ALLOWED_ORIGINS);

  const normalizedOrigin = normalizeOrigin(origin);
  if (normalizedOrigin && allowedOrigins.includes(normalizedOrigin)) {
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
