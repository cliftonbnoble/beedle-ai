import type { Env } from "../lib/types";
import { IngestionListBuildError, listIngestionDocuments, type ListIngestionDocumentsOptions } from "./admin-ingestion";

type ReviewerExportFormat = "json" | "csv" | "markdown";

type ReviewerExportFilters = {
  realOnly?: boolean;
  unresolvedTriageBucket?: string;
  blocked37xFamily?: string;
  reviewerEffort?: "low" | "medium" | "high";
  reviewerRisk?: "low" | "medium" | "high";
  safeToBatchReview?: boolean;
  batchKey?: string;
  blocked37xOnly?: boolean;
  limit?: number;
};

type UnresolvedIssue = {
  documentId: string;
  referenceType: string;
  rawValue: string;
  normalizedValue: string;
  message: string;
  severity: string;
};

export class ReviewerExportBuildError extends Error {
  endpoint: "reviewer-export" | "reviewer-adjudication-template" | "shared";
  operation: string;
  selectedDocCount: number;
  chunkingEnabled: boolean;
  chunksAttempted: number;
  subOperation?: string;
  chunkSize?: number;
  chunkCount?: number;
  currentChunkIndex?: number;
  idsInCurrentChunk?: number;
  queryKind?: string;
  causeMessage: string;

  constructor(params: {
    endpoint?: "reviewer-export" | "reviewer-adjudication-template" | "shared";
    operation: string;
    selectedDocCount: number;
    chunkingEnabled: boolean;
    chunksAttempted: number;
    subOperation?: string;
    chunkSize?: number;
    chunkCount?: number;
    currentChunkIndex?: number;
    idsInCurrentChunk?: number;
    queryKind?: string;
    cause: unknown;
  }) {
    const causeMessage = params.cause instanceof Error ? params.cause.message : String(params.cause ?? "unknown");
    super(`reviewer export build failed in ${params.operation}: ${causeMessage}`);
    this.name = "ReviewerExportBuildError";
    this.endpoint = params.endpoint ?? "shared";
    this.operation = params.operation;
    this.selectedDocCount = params.selectedDocCount;
    this.chunkingEnabled = params.chunkingEnabled;
    this.chunksAttempted = params.chunksAttempted;
    this.subOperation = params.subOperation;
    this.chunkSize = params.chunkSize;
    this.chunkCount = params.chunkCount;
    this.currentChunkIndex = params.currentChunkIndex;
    this.idsInCurrentChunk = params.idsInCurrentChunk;
    this.queryKind = params.queryKind;
    this.causeMessage = causeMessage;
  }
}

export const ADJUDICATION_TEMPLATE_FIELD_ORDER = [
  "documentId",
  "title",
  "batchKey",
  "reviewerDecision",
  "reviewerNotes",
  "citationActionType",
  "citationOriginal",
  "citationReplacement",
  "confirmMetadata",
  "escalate",
  "doNotApprove",
  "reviewedBy",
  "reviewedAt"
] as const;

const DB_IN_CHUNK_SIZE = 75;

function chunkArray<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function csvEscape(value: unknown): string {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, "\"\"")}"`;
  return text;
}

function normalizedCitation(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^rule/, "")
    .replace(/^ordinance/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function stripSubsections(input: string): string {
  return normalizedCitation(input).replace(/\([a-z0-9]+\)/g, "");
}

function classifyRootCause(
  issue: UnresolvedIssue,
  validSets: { index_code: Set<string>; rules_section: Set<string>; ordinance_section: Set<string> },
  duplicateCount: number
) {
  if (duplicateCount > 1) return "duplicate";
  if (/malformed|invalid|unable to parse|unparseable|format/i.test(issue.message || "")) return "malformed";
  const type = String(issue.referenceType || "");
  const normalized = normalizedCitation(issue.normalizedValue || issue.rawValue || "");
  if (
    /cross[_\s-]?context/i.test(issue.message || "") ||
    (type === "rules_section" && normalized.startsWith("37.")) ||
    (type === "ordinance_section" && normalized && !normalized.startsWith("37."))
  ) {
    return "cross_context";
  }
  const parent = stripSubsections(normalized);
  const valid = validSets[type as keyof typeof validSets] ?? new Set<string>();
  if (valid.has(parent)) return "parent_child";
  for (const value of valid.values()) {
    if (value.startsWith(`${parent}(`) || parent.startsWith(`${value}(`)) return "parent_child";
  }
  return "not_found";
}

function toCsv(rows: Array<Record<string, unknown>>): string {
  if (rows.length === 0) return "";
  const headers = Object.keys(rows[0]!);
  const lines = [headers.map(csvEscape).join(",")];
  for (const row of rows) {
    lines.push(headers.map((key) => csvEscape(row[key])).join(","));
  }
  return lines.join("\n");
}

function toMarkdownSummary(packet: ReturnType<typeof makeSummary>) {
  const lines = [
    "# Reviewer Batch Summary",
    "",
    `- Total staged real docs: ${packet.totalStagedRealDocs}`,
    `- Highest-leverage batches: ${packet.highestLeverageBatches.length}`,
    "",
    "## Grouped By Effort",
    ...packet.byEffort.map((item) => `- ${item.key}: ${item.count}`),
    "",
    "## Grouped By Triage Bucket",
    ...packet.byBucket.map((item) => `- ${item.key}: ${item.count}`),
    "",
    "## Grouped By Blocked 37.x Family",
    ...packet.byBlocked37x.map((item) => `- ${item.key}: ${item.count}`),
    "",
    "## Top Recurring Citation Families",
    ...packet.topRecurringFamilies.map((item) => `- ${item.key}: ${item.count}`),
    "",
    "## Top Recommended Actions",
    ...packet.topActions.map((item) => `- ${item.key}: ${item.count}`),
    "",
    "## Explicitly Blocked Families",
    "- 37.3",
    "- 37.7",
    "- 37.9",
    "- cross-context ambiguous citations",
    "- true not_found citations"
  ];
  return lines.join("\n");
}

function deriveBatchKey(doc: {
  blocked37xBatchKey?: string | null;
  recurringCitationFamilies?: string[];
  unresolvedBuckets?: string[];
}) {
  const explicit = String(doc.blocked37xBatchKey || "").trim();
  if (explicit) return explicit;
  const families = (doc.recurringCitationFamilies || []).map((item) => String(item || "").trim()).filter(Boolean).sort();
  if (families.length > 0) return `family:${families.join("+")}`;
  const buckets = (doc.unresolvedBuckets || []).map((item) => String(item || "").trim()).filter(Boolean).sort();
  if (buckets.length > 0) return `bucket:${buckets.join("+")}`;
  return "";
}

function countBy<T>(items: T[], keyFn: (item: T) => string) {
  const map = new Map<string, number>();
  for (const item of items) {
    const key = keyFn(item);
    map.set(key, (map.get(key) || 0) + 1);
  }
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

function makeSummary(rows: Array<Record<string, unknown>>) {
  const byEffort = countBy(rows, (row) => String(row.estimatedReviewerEffort || "unknown"));
  const byBucket = countBy(
    rows.flatMap((row) => String(row.unresolvedTriageBuckets || "").split(";").filter(Boolean)),
    (value) => value
  );
  const byBlocked37x = countBy(
    rows.flatMap((row) => String(row.blocked37xFamily || "").split(";").filter(Boolean)),
    (value) => value
  );
  const byRecurring = countBy(
    rows.flatMap((row) => String(row.recurringCitationFamily || "").split(";").filter(Boolean)),
    (value) => value
  );
  const topActions = countBy(rows, (row) => String(row.topRecommendedReviewerAction || "none"));
  const byBatch = countBy(rows.filter((row) => String(row.batchKey || "").length > 0), (row) => String(row.batchKey));
  return {
    totalStagedRealDocs: rows.length,
    byEffort,
    byBucket,
    byBlocked37x,
    topRecurringFamilies: byRecurring.slice(0, 10),
    topActions: topActions.slice(0, 10),
    highestLeverageBatches: byBatch.slice(0, 12).map((item) => ({ batchKey: item.key, count: item.count }))
  };
}

function adjudicationTemplateRows(rows: Array<Record<string, unknown>>) {
  return rows.map((row) => {
    const shell = {
      documentId: String(row.documentId || ""),
      title: String(row.title || ""),
      batchKey: String(row.batchKey || ""),
      reviewerDecision: "",
      reviewerNotes: "",
      citationActionType: "",
      citationOriginal: "",
      citationReplacement: "",
      confirmMetadata: "",
      escalate: "",
      doNotApprove: "",
      reviewedBy: "",
      reviewedAt: ""
    } as Record<string, unknown>;
    const ordered: Record<string, unknown> = {};
    for (const key of ADJUDICATION_TEMPLATE_FIELD_ORDER) ordered[key] = shell[key];
    return ordered;
  });
}

export function formatAdjudicationTemplateJson(
  rows: Array<Record<string, unknown>>,
  meta: { generatedAt?: string; filters?: Record<string, unknown> } = {}
) {
  return {
    generatedAt: meta.generatedAt || new Date().toISOString(),
    filters: meta.filters || {},
    rows: adjudicationTemplateRows(rows)
  };
}

export function formatAdjudicationTemplateCsv(rows: Array<Record<string, unknown>>) {
  return toCsv(adjudicationTemplateRows(rows));
}

export async function buildReviewerExportPacket(env: Env, filters: ReviewerExportFilters = {}) {
  const listOptions: ListIngestionDocumentsOptions = {
    status: "staged",
    fileType: "decision_docx",
    realOnly: filters.realOnly ?? true,
    unresolvedTriageBucket: filters.unresolvedTriageBucket,
    blocked37xFamily: filters.blocked37xFamily,
    estimatedReviewerEffort: filters.reviewerEffort,
    reviewerRiskLevel: filters.reviewerRisk,
    safeToBatchReviewOnly: filters.safeToBatchReview,
    blocked37xBatchKey: filters.batchKey,
    blocked37xOnly: filters.blocked37xOnly,
    sort: "unresolvedLeverageDesc",
    limit: Math.min(Math.max(filters.limit ?? 800, 1), 2000)
  };

  let list;
  try {
    list = await listIngestionDocuments(env, listOptions);
  } catch (error) {
    const listError = error instanceof IngestionListBuildError ? error : null;
    throw new ReviewerExportBuildError({
      operation: "list_ingestion_documents",
      selectedDocCount: 0,
      chunkingEnabled: true,
      chunksAttempted: 0,
      subOperation: listError?.subOperation,
      chunkSize: listError?.chunkSize,
      chunkCount: listError?.chunkCount,
      currentChunkIndex: listError?.currentChunkIndex,
      idsInCurrentChunk: listError?.idsInCurrentChunk,
      queryKind: listError?.queryKind,
      cause: error
    });
  }
  const docs = list.documents || [];
  const docIds = docs.map((doc) => String(doc.id));
  const idChunks = chunkArray(docIds, DB_IN_CHUNK_SIZE);
  if (docIds.length === 0) {
    const summary = makeSummary([]);
    return {
      generatedAt: new Date().toISOString(),
      filters: { ...filters, realOnly: filters.realOnly ?? true },
      rows: [],
      summary,
      markdownSummary: toMarkdownSummary(summary),
      csv: "",
      debug: {
        docs_selected: 0,
        chunks_used: 0,
        unresolved_detail_rows_loaded: 0,
        output_row_count: 0
      },
      adjudicationTemplate: { rows: [], csv: "" }
    };
  }

  let unresolvedDetailRowsLoaded = 0;
  const mergedIssues: UnresolvedIssue[] = [];
  const mergedLinks: Array<{
    documentId: string;
    referenceType: "index_code" | "rules_section" | "ordinance_section";
    canonicalValue: string;
  }> = [];
  let chunksAttempted = 0;
  for (const chunk of idChunks) {
    const placeholders = chunk.map(() => "?").join(",");
    chunksAttempted += 1;
    try {
      const issuesResult = await env.DB.prepare(
        `SELECT document_id as documentId, reference_type as referenceType, raw_value as rawValue,
                normalized_value as normalizedValue, message, severity
         FROM document_reference_issues
         WHERE document_id IN (${placeholders})
         ORDER BY created_at DESC`
      )
        .bind(...chunk)
        .all<UnresolvedIssue>();
      const linksResult = await env.DB.prepare(
        `SELECT document_id as documentId, reference_type as referenceType, canonical_value as canonicalValue
         FROM document_reference_links
         WHERE document_id IN (${placeholders}) AND is_valid = 1`
      )
        .bind(...chunk)
        .all<{ documentId: string; referenceType: "index_code" | "rules_section" | "ordinance_section"; canonicalValue: string }>();
      const issuesChunk = issuesResult.results ?? [];
      const linksChunk = linksResult.results ?? [];
      unresolvedDetailRowsLoaded += issuesChunk.length;
      mergedIssues.push(...issuesChunk);
      mergedLinks.push(...linksChunk);
    } catch (error) {
      throw new ReviewerExportBuildError({
        operation: "load_reference_rows_for_export",
        selectedDocCount: docIds.length,
        chunkingEnabled: true,
        chunksAttempted,
        cause: error
      });
    }
  }

  const issuesByDoc = new Map<string, UnresolvedIssue[]>();
  for (const issue of mergedIssues) {
    const list = issuesByDoc.get(issue.documentId) ?? [];
    list.push(issue);
    issuesByDoc.set(issue.documentId, list);
  }
  const validByDoc = new Map<
    string,
    {
      indexCodes: Set<string>;
      rulesSections: Set<string>;
      ordinanceSections: Set<string>;
      normalized: { index_code: Set<string>; rules_section: Set<string>; ordinance_section: Set<string> };
    }
  >();
  for (const link of mergedLinks) {
    const entry =
      validByDoc.get(link.documentId) ??
      {
        indexCodes: new Set<string>(),
        rulesSections: new Set<string>(),
        ordinanceSections: new Set<string>(),
        normalized: { index_code: new Set<string>(), rules_section: new Set<string>(), ordinance_section: new Set<string>() }
      };
    if (link.referenceType === "index_code") entry.indexCodes.add(link.canonicalValue);
    if (link.referenceType === "rules_section") entry.rulesSections.add(link.canonicalValue);
    if (link.referenceType === "ordinance_section") entry.ordinanceSections.add(link.canonicalValue);
    if (link.referenceType === "index_code") entry.normalized.index_code.add(normalizedCitation(link.canonicalValue));
    if (link.referenceType === "rules_section") entry.normalized.rules_section.add(normalizedCitation(link.canonicalValue));
    if (link.referenceType === "ordinance_section") entry.normalized.ordinance_section.add(normalizedCitation(link.canonicalValue));
    validByDoc.set(link.documentId, entry);
  }

  const derivedBatchKeys = new Map<string, string>();
  const batchSizes = new Map<string, number>();
  for (const doc of docs) {
    const key = deriveBatchKey(doc as { blocked37xBatchKey?: string | null; recurringCitationFamilies?: string[]; unresolvedBuckets?: string[] });
    derivedBatchKeys.set(String(doc.id), key);
    if (!key) continue;
    batchSizes.set(key, (batchSizes.get(key) || 0) + 1);
  }

  const rows = docs
    .map((doc) => {
      const unresolved = issuesByDoc.get(doc.id) ?? [];
      const valid =
        validByDoc.get(doc.id) ??
        {
          indexCodes: new Set<string>(),
          rulesSections: new Set<string>(),
          ordinanceSections: new Set<string>(),
          normalized: { index_code: new Set<string>(), rules_section: new Set<string>(), ordinance_section: new Set<string>() }
        };
      const duplicateCounts = new Map<string, number>();
      for (const issue of unresolved) {
        const key = `${issue.referenceType}::${normalizedCitation(issue.normalizedValue || issue.rawValue || "")}`;
        duplicateCounts.set(key, (duplicateCounts.get(key) || 0) + 1);
      }
      const exactUnresolved = unresolved.map((issue) => {
        const dupKey = `${issue.referenceType}::${normalizedCitation(issue.normalizedValue || issue.rawValue || "")}`;
        const rootCause = classifyRootCause(
          issue,
          {
            index_code: valid.normalized.index_code,
            rules_section: valid.normalized.rules_section,
            ordinance_section: valid.normalized.ordinance_section
          },
          duplicateCounts.get(dupKey) || 0
        );
        return {
          referenceType: issue.referenceType,
          rawValue: issue.rawValue,
          normalizedValue: issue.normalizedValue,
          rootCause,
          message: issue.message
        };
      });
      const batchKey = derivedBatchKeys.get(String(doc.id)) || "";
      const reviewerReasons = (doc as { reviewerReadyReasons?: string[] }).reviewerReadyReasons || [];
      const unresolvedBuckets = (doc as { unresolvedBuckets?: string[] }).unresolvedBuckets || [];
      const safeAfterManualConfirmation =
        Boolean((doc as { metadataConfirmationWouldUnlock?: boolean }).metadataConfirmationWouldUnlock) ||
        (reviewerReasons.includes("confirmation_plus_one_manual_citation_fix") &&
          !unresolvedBuckets.some((bucket) =>
            ["unsafe_37x_structural_block", "cross_context_ambiguous", "structurally_blocked_not_found"].includes(bucket)
          ));
      return {
        documentId: String(doc.id),
        title: String(doc.title),
        citation: String(doc.citation || ""),
        decisionDate: String(doc.decisionDate || ""),
        reviewerRiskLevel: String((doc as { reviewerRiskLevel?: string }).reviewerRiskLevel || "high"),
        estimatedReviewerEffort: String((doc as { estimatedReviewerEffort?: string }).estimatedReviewerEffort || "high"),
        blockers: ((doc as { approvalReadiness?: { blockers?: string[] } }).approvalReadiness?.blockers || []).join(";"),
        unresolvedCount: Number(doc.unresolvedReferenceCount || 0),
        unresolvedTriageBuckets: unresolvedBuckets.join(";"),
        blocked37xFamily: ((doc as { blocked37xReferences?: Array<{ family?: string }> }).blocked37xReferences || [])
          .map((row) => String(row.family || ""))
          .filter(Boolean)
          .join(";"),
        recurringCitationFamily: ((doc as { recurringCitationFamilies?: string[] }).recurringCitationFamilies || []).join(";"),
        topRecommendedReviewerAction: String((doc as { topRecommendedReviewerAction?: string }).topRecommendedReviewerAction || ""),
        candidateManualFixes: ((doc as { candidateManualFixes?: string[] }).candidateManualFixes || []).join(" | "),
        exactUnresolvedReferences: exactUnresolved,
        validatedReferencesPresent: {
          indexCodes: Array.from(valid.indexCodes),
          rulesSections: Array.from(valid.rulesSections),
          ordinanceSections: Array.from(valid.ordinanceSections)
        },
        metadataConfirmationWouldUnlock: Boolean((doc as { metadataConfirmationWouldUnlock?: boolean }).metadataConfirmationWouldUnlock),
        safeAfterManualConfirmation,
        batchKey,
        batchSize: batchKey ? batchSizes.get(batchKey) || 1 : 1,
        isRealDoc: !Boolean((doc as { isLikelyFixture?: boolean }).isLikelyFixture)
      };
    })
    .sort((a, b) => {
      const keyCmp = String(a.batchKey).localeCompare(String(b.batchKey));
      if (keyCmp !== 0) return keyCmp;
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  const csvRows = rows.map((row) => ({
    documentId: row.documentId,
    title: row.title,
    citation: row.citation,
    decisionDate: row.decisionDate,
    reviewerRiskLevel: row.reviewerRiskLevel,
    estimatedReviewerEffort: row.estimatedReviewerEffort,
    blockers: row.blockers,
    unresolvedCount: row.unresolvedCount,
    unresolvedTriageBuckets: row.unresolvedTriageBuckets,
    blocked37xFamily: row.blocked37xFamily,
    recurringCitationFamily: row.recurringCitationFamily,
    topRecommendedReviewerAction: row.topRecommendedReviewerAction,
    candidateManualFixes: row.candidateManualFixes,
    exactUnresolvedReferences: JSON.stringify(row.exactUnresolvedReferences),
    validatedReferencesPresent: JSON.stringify(row.validatedReferencesPresent),
    metadataConfirmationWouldUnlock: row.metadataConfirmationWouldUnlock,
    safeAfterManualConfirmation: row.safeAfterManualConfirmation,
    batchKey: row.batchKey,
    batchSize: row.batchSize,
    isRealDoc: row.isRealDoc
  }));
  const summary = makeSummary(csvRows as Array<Record<string, unknown>>);
  const adjudicationRows = adjudicationTemplateRows(csvRows as Array<Record<string, unknown>>);

  return {
    generatedAt: new Date().toISOString(),
    filters: { ...filters, realOnly: filters.realOnly ?? true },
    rows,
    summary,
    markdownSummary: toMarkdownSummary(summary),
    csv: toCsv(csvRows as Array<Record<string, unknown>>),
    debug: {
      docs_selected: docs.length,
      chunks_used: idChunks.length,
      unresolved_detail_rows_loaded: unresolvedDetailRowsLoaded,
      output_row_count: rows.length
    },
    adjudicationTemplate: {
      rows: adjudicationRows,
      csv: formatAdjudicationTemplateCsv(csvRows as Array<Record<string, unknown>>)
    }
  };
}

export function reviewerExportFilename(format: ReviewerExportFormat) {
  const stamp = new Date().toISOString().slice(0, 10);
  return `reviewer-batch-export-${stamp}.${format === "markdown" ? "md" : format}`;
}
