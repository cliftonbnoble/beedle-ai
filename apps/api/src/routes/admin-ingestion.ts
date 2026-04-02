import {
  approveIngestionDocument,
  type BulkSearchabilityCandidateMode,
  bulkEnableSearchability,
  getIngestionDocumentDetail,
  listBulkSearchabilityCandidates,
  listIngestionDocuments,
  reprocessIngestionDocument,
  rejectIngestionDocument,
  updateIngestionMetadata
} from "../services/admin-ingestion";
import { ReviewerExportBuildError, buildReviewerExportPacket, reviewerExportFilename } from "../services/reviewer-export";
import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleListIngestionDocuments(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const status = (url.searchParams.get("status") || "all") as
      | "all"
      | "staged"
      | "searchable"
      | "approved"
      | "rejected"
      | "pending";
    const fileType = (url.searchParams.get("fileType") || undefined) as "decision_docx" | "law_pdf" | undefined;
    const hasWarnings = url.searchParams.get("hasWarnings") === "1";
    const missingRequired = url.searchParams.get("missingRequired") === "1";
    const unresolvedReferencesOnly = url.searchParams.get("unresolvedReferencesOnly") === "1";
    const criticalExceptionsOnly = url.searchParams.get("criticalExceptionsOnly") === "1";
    const filteredNoiseOnly = url.searchParams.get("filteredNoiseOnly") === "1";
    const lowConfidenceTaxonomyOnly = url.searchParams.get("lowConfidenceTaxonomyOnly") === "1";
    const missingRulesOnly = url.searchParams.get("missingRulesOnly") === "1";
    const missingOrdinanceOnly = url.searchParams.get("missingOrdinanceOnly") === "1";
    const approvalReadyOnly = url.searchParams.get("approvalReadyOnly") === "1";
    const reviewerReadyOnly = url.searchParams.get("reviewerReadyOnly") === "1";
    const unresolvedTriageBucket = url.searchParams.get("unresolvedTriageBucket") || undefined;
    const recurringCitationFamily = url.searchParams.get("recurringCitationFamily") || undefined;
    const blocked37xOnly = url.searchParams.get("blocked37xOnly") === "1";
    const blocked37xFamily = url.searchParams.get("blocked37xFamily") || undefined;
    const blocked37xBatchKey = url.searchParams.get("blocked37xBatchKey") || undefined;
    const safeToBatchReviewOnly = url.searchParams.get("safeToBatchReviewOnly") === "1";
    const estimatedReviewerEffort = (url.searchParams.get("estimatedReviewerEffort") || undefined) as "low" | "medium" | "high" | undefined;
    const reviewerRiskLevel = (url.searchParams.get("reviewerRiskLevel") || undefined) as "low" | "medium" | "high" | undefined;
    const blocker = url.searchParams.get("blocker") || undefined;
    const runtimeManualCandidatesOnly = url.searchParams.get("runtimeManualCandidatesOnly") === "1";
    const realOnly = url.searchParams.get("realOnly") === "1";
    const taxonomyCaseTypeId = url.searchParams.get("taxonomyCaseTypeId") || undefined;
    const query = url.searchParams.get("query") || undefined;
    const sort = (url.searchParams.get("sort") || "createdAtDesc") as
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
    const limitRaw = Number(url.searchParams.get("limit") || "200");
    const limit = Number.isFinite(limitRaw) ? limitRaw : 200;

    const result = await listIngestionDocuments(env, {
      status,
      fileType,
      hasWarnings,
      missingRequired,
      unresolvedReferencesOnly,
      criticalExceptionsOnly,
      filteredNoiseOnly,
      lowConfidenceTaxonomyOnly,
      missingRulesOnly,
      missingOrdinanceOnly,
      approvalReadyOnly,
      reviewerReadyOnly,
      unresolvedTriageBucket,
      recurringCitationFamily,
      blocked37xOnly,
      blocked37xFamily,
      blocked37xBatchKey,
      safeToBatchReviewOnly,
      estimatedReviewerEffort,
      reviewerRiskLevel,
      blocker,
      runtimeManualCandidatesOnly,
      realOnly,
      taxonomyCaseTypeId,
      query,
      sort,
      limit
    });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleGetIngestionDocument(_request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const result = await getIngestionDocumentDetail(env, documentId);
    if (!result) {
      return json({ error: "Document not found" }, { status: 404 });
    }
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleUpdateIngestionMetadata(request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await updateIngestionMetadata(env, documentId, payload);
    if (!result) {
      return json({ error: "Document not found" }, { status: 404 });
    }
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRejectIngestionDocument(request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await rejectIngestionDocument(env, documentId, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleApproveIngestionDocument(_request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const result = await approveIngestionDocument(env, documentId);
    return json(result, { status: result.approved ? 200 : 422 });
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleReprocessIngestionDocument(_request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const result = await reprocessIngestionDocument(env, documentId);
    if (!result) {
      return json({ error: "Document not found" }, { status: 404 });
    }
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleListBulkSearchabilityCandidates(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const limitRaw = Number(url.searchParams.get("limit") || "200");
    const limit = Number.isFinite(limitRaw) ? limitRaw : 200;
    const realOnly = url.searchParams.get("realOnly") !== "0";
    const mode = (url.searchParams.get("mode") || "qcPassed") as BulkSearchabilityCandidateMode;
    const result = await listBulkSearchabilityCandidates(env, { limit, realOnly, mode });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleBulkEnableSearchability(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request).catch(() => ({}));
    const limit = Number(payload?.limit || 200);
    const realOnly = payload?.realOnly !== false;
    const dryRun = payload?.dryRun !== false;
    const mode = (payload?.mode || "qcPassed") as BulkSearchabilityCandidateMode;
    const result = await bulkEnableSearchability(env, { limit, realOnly, dryRun, mode });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

function parseReviewerExportFilters(url: URL) {
  return {
    realOnly: url.searchParams.get("realOnly") !== "0",
    unresolvedTriageBucket: url.searchParams.get("unresolvedTriageBucket") || undefined,
    blocked37xFamily: url.searchParams.get("blocked37xFamily") || undefined,
    reviewerEffort: (url.searchParams.get("estimatedReviewerEffort") || undefined) as "low" | "medium" | "high" | undefined,
    reviewerRisk: (url.searchParams.get("reviewerRiskLevel") || undefined) as "low" | "medium" | "high" | undefined,
    safeToBatchReview: url.searchParams.get("safeToBatchReviewOnly") === "1",
    batchKey: url.searchParams.get("blocked37xBatchKey") || undefined,
    blocked37xOnly: url.searchParams.get("blocked37xOnly") === "1",
    limit: Number(url.searchParams.get("limit") || "800")
  };
}

function asAttachment(contentType: string, filename: string, body: string): Response {
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": contentType,
      "content-disposition": `attachment; filename="${filename}"`
    }
  });
}

function toReviewerExportErrorResponse(
  endpoint: "reviewer-export" | "reviewer-adjudication-template",
  error: unknown
): Response {
  if (error instanceof ReviewerExportBuildError) {
    return json(
      {
        error: error.causeMessage,
        endpoint,
        operation: error.operation,
        subOperation: error.subOperation || null,
        selectedDocCount: error.selectedDocCount,
        chunkingEnabled: error.chunkingEnabled,
        chunkSize: error.chunkSize || null,
        chunkCount: error.chunkCount || null,
        currentChunkIndex: error.currentChunkIndex ?? null,
        idsInCurrentChunk: error.idsInCurrentChunk ?? null,
        queryKind: error.queryKind || null,
        chunksAttempted: error.chunksAttempted,
        errorClass: error.name
      },
      { status: 500 }
    );
  }
  return toErrorResponse(error);
}

export async function handleReviewerBatchExport(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const format = (url.searchParams.get("format") || "json") as "json" | "csv" | "markdown";
    const packet = await buildReviewerExportPacket(env, parseReviewerExportFilters(url));
    if (format === "csv") {
      return asAttachment("text/csv; charset=utf-8", reviewerExportFilename("csv"), packet.csv);
    }
    if (format === "markdown") {
      return asAttachment("text/markdown; charset=utf-8", reviewerExportFilename("markdown"), packet.markdownSummary);
    }
    return json(packet);
  } catch (error) {
    return toReviewerExportErrorResponse("reviewer-export", error);
  }
}

export async function handleReviewerAdjudicationTemplate(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const format = (url.searchParams.get("format") || "json") as "json" | "csv";
    const packet = await buildReviewerExportPacket(env, parseReviewerExportFilters(url));
    if (format === "csv") {
      return asAttachment("text/csv; charset=utf-8", "reviewer-adjudication-template.csv", packet.adjudicationTemplate.csv);
    }
    return json({
      generatedAt: packet.generatedAt,
      filters: packet.filters,
      rows: packet.adjudicationTemplate.rows
    });
  } catch (error) {
    return toReviewerExportErrorResponse("reviewer-adjudication-template", error);
  }
}
