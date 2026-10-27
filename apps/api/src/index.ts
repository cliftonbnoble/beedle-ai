import {inspectOperationalHealth} from "./services/operational-health";
import {newTelemetry,telemetryEnvironment,observedTask,operationalReason} from "./lib/telemetry";
import { inspectReleaseReadiness } from "./services/release-readiness";
import { handleIndexRecovery } from "./routes/search-index-recovery";
import { inspectSearchReadiness } from "./services/search-readiness";
import { expireSearchSessions } from "./services/search-sessions";
import { advanceReviewerQueueIndex } from "./services/admin-ingestion";
import { handleReviewerQueueIndex, handleReviewerPeers } from "./routes/admin-ingestion";
import { SearchWorkBudget } from "./services/search-work-budget";
import { handleSearchIndex } from "./routes/search-index";
import { handleVectorLifecycle } from "./routes/vector-lifecycle";
import { runVectorLifecycleMaintenance } from "./services/vector-generations";
import { handleMaintenanceJob } from "./routes/maintenance-jobs";
import { runScheduledMaintenance } from "./services/maintenance-jobs";
import { inspectComputeBudgets } from "./services/compute-budget";
import { handleListCatalogVersions, handleRestoreCatalog } from "./routes/admin-references";
import { json, toErrorResponse } from "./lib/http";
import { deploymentEnvironment } from "./lib/deployment";
import type { Env } from "./lib/types";
import { handleListDocumentGenerations, handleRestoreDocumentGeneration } from "./routes/admin-ingestion";
import { handleGetProvision } from "./routes/provision";
import { AutoRouter, type IRequest } from "itty-router";
import { handleIngest, handleIngestMultipart, handleDecisionApproval, handleIngestionStatus } from "./routes/ingest";
import { handleSearch } from "./routes/search";
import { handleCaseAssistant } from "./routes/case-assistant";
import { handleAssistantChat } from "./routes/assistant-chat";
import { handleDraftConclusions, handleDraftConclusionsDebug } from "./routes/draft-conclusions";
import { handleDraftTemplate } from "./routes/draft-template";
import { handleDraftExport } from "./routes/draft-export";
import { handleDecisionRetrievalChunks, handleRetrievalDebug } from "./routes/retrieval-debug";
import { handlePrepareRetrievalActivation, handleRollbackRetrievalActivation, handleWriteRetrievalActivation } from "./routes/admin-retrieval-activation";
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
import { consumeVectorJobBatch, listVectorJobs, requeueStaleVectorJobs, type VectorJobMessage } from "./services/vector-jobs";

const router = AutoRouter<IRequest, [Env, ExecutionContext], Response>({
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

export type CostClass = "free" | "search" | "ingest" | "llm" | "admin-write" | "review-read" | "export";
type RouteHandler = (request:Request,env:Env,ctx:ExecutionContext)=>Response|Promise<Response>;
const declaredRoutes: Array<{method:string;path:string;cost:CostClass;pattern:RegExp}> = [];
type RouteOptions={vectorMaintenance?:boolean;reviewerMaintenance?:boolean};
function declareRoute(method:"get"|"post",path:string,cost:CostClass,handler:RouteHandler,options:RouteOptions={}) {
  const pattern=new RegExp("^"+path.split("/").map(segment=>segment.startsWith(":") ? "[^/]+" : segment.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")).join("/")+"/?$");
  declaredRoutes.push({method:method.toUpperCase(),path,cost,pattern});
  router[method](path,async (request:Request,env:Env,ctx:ExecutionContext)=> {
    const limited = await enforceCostControls(request, env, cost);
    if(limited) return limited;
    const budget=(cost==="search"||cost==="llm")?new SearchWorkBudget(request.signal):undefined;
    if(budget)env.TELEMETRY?.workBudget(()=>budget.snapshot());
    const response=await handler(request,budget?{...env,SEARCH_WORK_BUDGET:budget}:env,ctx);
    if(options.vectorMaintenance!==false && method==='post' && response.ok && env.VECTOR_INDEX && (cost==='admin-write'||cost==='ingest')) ctx.waitUntil(observedTask(env,"vector_maintenance",observed=>runVectorLifecycleMaintenance(observed)));
    if(options.reviewerMaintenance && method==='post' && response.ok)ctx.waitUntil(observedTask(env,"reviewer_maintenance",observed=>advanceReviewerQueueIndex(observed)));
    return response;
  });
}
const get=(path:string,cost:CostClass,handler:RouteHandler)=>declareRoute("get",path,cost,handler);
const post=(path:string,cost:CostClass,handler:RouteHandler,options:RouteOptions={})=>declareRoute("post",path,cost,handler,options);

get("/admin/search-index/recovery", "review-read", (request,env)=>handleIndexRecovery(request,env,"inspect"));
get("/admin/search-index/coverage", "review-read", (request,env)=>handleIndexRecovery(request,env,"coverage"));
post("/admin/search-index/rebuild", "admin-write", (request,env)=>handleIndexRecovery(request,env,"start"),{vectorMaintenance:false});
post("/admin/search-index/rebuild/step", "admin-write", (request,env)=>handleIndexRecovery(request,env,"step"),{vectorMaintenance:false});
get("/admin/search-readiness", "review-read", async (_request,env)=>{try{return json(await inspectSearchReadiness(env));}catch(error){return toErrorResponse(error);}});
get("/admin/search-index", "review-read", (request,env)=>handleSearchIndex(request,env,false));
post("/admin/search-index/step", "admin-write", (request,env)=>handleSearchIndex(request,env,true));

get("/admin/operations/health","review-read",async(_request,env)=>{try{return json(await inspectOperationalHealth(env));}catch{return json({error:"Operational health is unavailable"},{status:503});}});
get("/admin/release-readiness", "review-read", async (_request,env)=>{const report=await inspectReleaseReadiness(env);return json(report,{status:report.ready?200:503});});
get("/health", "free", (_request: Request, env: Env) =>
  json({
    ok: true,
    environment: env.APP_ENV,
    aiAvailable: Boolean(env.AI),
    vectorNamespace: env.VECTOR_NAMESPACE,
    vectorBindingPresent: Boolean(env.VECTOR_INDEX),
    embeddingModel: env.AI_EMBEDDING_MODEL
  })
);
get("/ingest/requests/:requestId", "review-read", (request:Request,env:Env)=>handleIngestionStatus(request,env,(request as Request & {params:{requestId:string}}).params.requestId));
post("/ingest/decision", "ingest", (request: Request, env: Env) => handleIngest(request, env, "decision_docx"));
post("/ingest/decision-upload", "ingest", (request: Request, env: Env) => handleIngestMultipart(request, env, "decision_docx"));
post("/ingest/law", "ingest", (request: Request, env: Env) => handleIngest(request, env, "law_pdf"));
post("/decisions/:documentId/approve", "admin-write", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) {
    return json({ error: "documentId is required" }, { status: 400 });
  }
  return handleDecisionApproval(request, env, documentId);
});
get("/decisions/:documentId/chunks", "review-read", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleDecisionRetrievalChunks(request, env, documentId);
});
get("/admin/ingestion/:documentId/source", "review-read", (request: Request, env: Env) => handleGetSource(request, env, true));
post("/search", "search", (request: Request, env: Env) => handleSearch(request, env));
post("/admin/retrieval/debug", "search", (request: Request, env: Env) => handleRetrievalDebug(request, env));
get("/admin/retrieval/documents/:documentId/chunks", "review-read", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleDecisionRetrievalChunks(request, env, documentId, true);
});
post("/admin/retrieval/activation/prepare", "admin-write", (request: Request, env: Env) => handlePrepareRetrievalActivation(request, env));
post("/admin/retrieval/activation/write", "admin-write", (request: Request, env: Env) => handleWriteRetrievalActivation(request, env));
post("/admin/retrieval/activation/rollback", "admin-write", (request: Request, env: Env) => handleRollbackRetrievalActivation(request, env));
post("/admin/retrieval/vectors/backfill", "ingest", (request: Request, env: Env) => handleBackfillRetrievalVectors(request, env));
post("/admin/retrieval/vectors/probe", "ingest", (request: Request, env: Env) => handleProbeRetrievalVectors(request, env));
post("/api/case-assistant", "llm", (request: Request, env: Env) => handleCaseAssistant(request, env));
post("/api/assistant/chat", "llm", (request: Request, env: Env) => handleAssistantChat(request, env));
post("/api/draft/conclusions", "llm", (request: Request, env: Env) => handleDraftConclusions(request, env));
post("/api/draft/template", "llm", (request: Request, env: Env) => handleDraftTemplate(request, env));
post("/api/draft/export", "export", (request: Request, env: Env) => handleDraftExport(request, env));
post("/admin/draft/debug", "llm", (request: Request, env: Env) => handleDraftConclusionsDebug(request, env));
get("/admin/config/taxonomy", "free", (request: Request, env: Env) => handleGetTaxonomyConfig(request, env));
post("/admin/config/taxonomy/resolve", "review-read", (request: Request, env: Env) => handleResolveTaxonomyCaseType(request, env));
post("/admin/config/taxonomy/validate", "admin-write", (request: Request, env: Env) => handleValidateTaxonomyConfig(request, env));
get("/references/:kind/:id", "review-read", (request: Request, env: Env) => handleGetProvision(request, env));
get("/admin/references", "review-read", (request: Request, env: Env) => handleInspectReferences(request, env));
get("/admin/references/rules", "review-read", (request: Request, env: Env) => handleRulesCitationInventory(request, env));
post("/admin/references/rebuild", "admin-write", (request: Request, env: Env) => handleRebuildReferences(request, env));
post("/admin/references/backfill", "admin-write", (request: Request, env: Env) => handleBackfillReferenceValidation(request, env));
post("/admin/references/verify-citations", "admin-write", (request: Request, env: Env) => handleVerifyReferenceCitations(request, env));
get("/source/:documentId", "review-read", (request: Request, env: Env) => handleGetSource(request, env));
get("/admin/ingestion/documents", "review-read", (request: Request, env: Env) => handleListIngestionDocuments(request, env));
get("/admin/reviewer-index", "review-read", (request,env)=>handleReviewerQueueIndex(request,env));
post("/admin/reviewer-index/step", "admin-write", (request,env)=>handleReviewerQueueIndex(request,env,true),{vectorMaintenance:false});
get("/admin/ingestion/reviewer-peers", "review-read", handleReviewerPeers);
get("/admin/dashboard/summary", "review-read", (request: Request, env: Env) => handleDashboardSummary(request, env));
get("/admin/ingestion/searchability/candidates", "review-read", (request: Request, env: Env) =>
  handleListBulkSearchabilityCandidates(request, env)
);
post("/admin/ingestion/searchability/enable", "admin-write", (request: Request, env: Env) =>
  handleBulkEnableSearchability(request, env)
);
get("/admin/ingestion/reviewer-export", "export", (request: Request, env: Env) => handleReviewerBatchExport(request, env));
get("/admin/ingestion/reviewer-adjudication-template", "export", (request: Request, env: Env) =>
  handleReviewerAdjudicationTemplate(request, env)
);
get("/admin/ingestion/documents/:documentId", "review-read", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleGetIngestionDocument(request, env, documentId);
});
post("/admin/ingestion/documents/:documentId/metadata", "admin-write", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleUpdateIngestionMetadata(request, env, documentId);
},{reviewerMaintenance:true});
post("/admin/ingestion/documents/:documentId/approve", "admin-write", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleApproveIngestionDocument(request, env, documentId);
},{reviewerMaintenance:true});
post("/admin/ingestion/documents/:documentId/reject", "admin-write", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleRejectIngestionDocument(request, env, documentId);
},{reviewerMaintenance:true});
post("/admin/ingestion/documents/:documentId/reprocess", "admin-write", (request: Request, env: Env) => {
  const documentId = (request as Request & { params?: { documentId?: string } }).params?.documentId;
  if (!documentId) return json({ error: "documentId is required" }, { status: 400 });
  return handleReprocessIngestionDocument(request, env, documentId);
},{reviewerMaintenance:true});

get("/admin/ingestion/documents/:documentId/generations", "review-read", (request: Request, env: Env) => {
  const params = (request as Request & {params: {documentId: string}}).params;
  return handleListDocumentGenerations(request, env, params.documentId);
});
post("/admin/ingestion/documents/:documentId/generations/:generationId/restore", "admin-write", (request: Request, env: Env) => {
  const params = (request as Request & {params: {documentId: string; generationId: string}}).params;
  return handleRestoreDocumentGeneration(request, env, params.documentId, params.generationId);
});

get("/admin/vector-jobs", "review-read", async (request: Request, env: Env) => {
  try {
    const params = new URL(request.url).searchParams;
    return json(await listVectorJobs(env, {state:params.get("state") || undefined,after:params.get("after") || undefined,
      limit:params.has("limit") ? Number(params.get("limit")) : undefined}));
  } catch (error) { return toErrorResponse(error); }
});

get("/admin/references/versions", "review-read", handleListCatalogVersions);
post("/admin/references/restore", "admin-write", handleRestoreCatalog);

get("/admin/cost-budgets", "review-read", async (_request,env)=> {
  try { return json(await inspectComputeBudgets(env)); }
  catch(error) { return toErrorResponse(error); }
});

get("/admin/maintenance/jobs/:jobId", "review-read", (request,env)=>handleMaintenanceJob(request,env,"inspect"));
post("/admin/maintenance/jobs/:jobId/step", "ingest", (request,env)=>handleMaintenanceJob(request,env,"step"));
post("/admin/maintenance/jobs/:jobId/cancel", "admin-write", (request,env)=>handleMaintenanceJob(request,env,"cancel"));
post("/admin/maintenance/jobs/:jobId/retry", "admin-write", (request,env)=>handleMaintenanceJob(request,env,"retry"));

get("/admin/vector-generations", "review-read", (request,env)=>handleVectorLifecycle(request,env,"inspect"));
post("/admin/vector-generations/reconcile", "ingest", (request,env)=>handleVectorLifecycle(request,env,"reconcile"));
post("/admin/vector-generations/sync", "ingest", (request,env)=>handleVectorLifecycle(request,env,"sync"));
post("/admin/vector-generations/cleanup", "admin-write", (request,env)=>handleVectorLifecycle(request,env,"cleanup"));

export const routeInventory=Object.freeze(declaredRoutes.map(({method,path,cost})=>Object.freeze({method,path,cost})));

export default {
  fetch: async (request: Request, env: Env, ctx: ExecutionContext) => {
    const method=request.method==='HEAD'?'GET':request.method,path=new URL(request.url).pathname;
    const route=declaredRoutes.find(route=>route.method===method&&route.pattern.test(path))?.path||'unmatched';
    const span=newTelemetry(env,'http',route);let response:Response;let configured=false;
    try{
      env=telemetryEnvironment(deploymentEnvironment(env),span);configured=true;
      if(env.RELEASE_MAINTENANCE_ONLY==='true'&&path!=='/health'&&!path.startsWith('/admin/')&&!path.startsWith('/ingest/'))response=json({error:'Preview preparation is incomplete'},{status:503});
      else response=await router.fetch(request,env,ctx);
      response=withCors(response,request,env);
    }catch(error){span.tag('error',operationalReason(error));response=json({error:configured?'Internal error':'Deployment environment is not configured'},{status:configured?500:503});}
    const headers=new Headers(response.headers);headers.set('x-request-id',span.id);headers.set('server-timing',`app;dur=${span.elapsed().toFixed(2)}`);
    span.finish(response.status);
    return new Response(response.body,{status:response.status,statusText:response.statusText,headers});
  },
  scheduled: async (_controller: ScheduledController, env: Env, ctx: ExecutionContext) => {
    const span=newTelemetry(env,'scheduled','scheduled_maintenance');
    ctx.waitUntil((async()=>{
      try{
        env=telemetryEnvironment(deploymentEnvironment(env),span);
        const results=await Promise.allSettled([
          observedTask(env,'vector_dispatch',observed=>requeueStaleVectorJobs(observed)),
          observedTask(env,'durable_maintenance',observed=>runScheduledMaintenance(observed)),
          observedTask(env,'session_expiry',observed=>expireSearchSessions(observed)),
          observedTask(env,'reviewer_maintenance',observed=>advanceReviewerQueueIndex(observed)),
          observedTask(env,'vector_maintenance',observed=>runVectorLifecycleMaintenance(observed))]);
        const health=await inspectOperationalHealth(env);span.health(health.metrics);
        if(results.some(result=>result.status==='rejected')||!health.healthy)span.tag('error','operation_failed');
        span.finish(results.some(result=>result.status==='rejected')?500:200);
        if(results.some(result=>result.status==='rejected'))throw new Error('Scheduled maintenance failed; inspect correlated telemetry');
      }catch(error){span.tag('error',operationalReason(error));span.finish(500);throw new Error('Scheduled operation failed; inspect correlated telemetry');}
    })());
  },
  queue: async (batch: MessageBatch<VectorJobMessage>, env: Env) => {
    const span=newTelemetry(env,'queue','vector_delivery');
    try{env=telemetryEnvironment(deploymentEnvironment(env),span);await consumeVectorJobBatch(batch,env);await runVectorLifecycleMaintenance(env);span.finish(200);}
    catch(error){span.tag('error',operationalReason(error));span.finish(500);throw new Error('Queue operation failed; delivery will retry');}
  }
} satisfies ExportedHandler<Env,VectorJobMessage>;

type CostControl = {
  limiter: RateLimit;
  bucket: string;
  retryAfterSeconds: number;
};

export function costClassFor(request:Request):CostClass|null {
  if(request.method==="OPTIONS") return null;
  const method=request.method==="HEAD"?"GET":request.method;
  const path=new URL(request.url).pathname;
  return declaredRoutes.find(route=>route.method===method && route.pattern.test(path))?.cost ?? null;
}
function costControlFor(cost: CostClass, env: Env): CostControl | null {
  if(cost==="free") return null;
  const bindings={search:env.SEARCH_RATE_LIMIT,ingest:env.INGEST_RATE_LIMIT,llm:env.LLM_RATE_LIMIT,
    "admin-write":env.ADMIN_WRITE_RATE_LIMIT,"review-read":env.REVIEW_READ_RATE_LIMIT,export:env.EXPORT_RATE_LIMIT};
  return {limiter:bindings[cost],bucket:cost,retryAfterSeconds:60};
}

function rateLimitKey(request: Request, bucket: string): string {
  // AUTH-01 will supply a stable user/tenant subject. Until then the connecting IP is the only
  // available actor key; including a bucket keeps independent quotas for distinct cost classes.
  const client = request.headers.get("cf-connecting-ip") || "unknown-client";
  return `${bucket}:${client}`;
}

async function enforceCostControls(request: Request, env: Env, cost:CostClass): Promise<Response | null> {
  const control = costControlFor(cost, env);
  if (!control) return null;
  try {
    const outcome = await control.limiter.limit({ key: rateLimitKey(request, control.bucket) });
    if (outcome.success) return null;
    env.TELEMETRY?.tag("quota","request_limited");
    return json(
      { error: "Request quota exceeded. Please retry later." },
      { status: 429, headers: { "retry-after": String(control.retryAfterSeconds) } }
    );
  } catch {
    // These routes incur external compute/storage cost. Serving them without an active enforcement
    // binding defeats the control, so fail closed rather than silently allowing unlimited traffic.
    env.TELEMETRY?.tag("quota","budget_unavailable");
    return json(
      { error: "Cost-control service is temporarily unavailable. Please retry later." },
      { status: 503, headers: { "retry-after": String(control.retryAfterSeconds) } }
    );
  }
}

function corsHeaders(request: Request, env: Env) {
  const headers: Record<string, string> = {
    "access-control-expose-headers": "x-request-id, server-timing, retry-after",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers":
      request.headers.get("access-control-request-headers") ||
      "content-type, cf-access-client-id, cf-access-client-secret"
  };

  const origin = request.headers.get("origin");
  // Fail closed: only origins explicitly listed in CORS_ALLOWED_ORIGINS are allowed. When the allowlist is
  // unset/empty, no cross-origin is permitted — there is no hardcoded fallback (which previously baked the
  // prod origin into source). `wrangler.json vars` sets CORS_ALLOWED_ORIGINS for every environment.
  const allowedOrigins = parseAllowedOrigins(env.CORS_ALLOWED_ORIGINS);

  const normalizedOrigin = normalizeOrigin(origin);
  if (normalizedOrigin && allowedOrigins.includes(normalizedOrigin)) {
    headers["access-control-allow-origin"] = normalizedOrigin;
    headers["access-control-allow-credentials"] = "true";
    headers.vary = "Origin";
    headers["timing-allow-origin"] = normalizedOrigin;
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
