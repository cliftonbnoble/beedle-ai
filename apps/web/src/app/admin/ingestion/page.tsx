"use client";

import { useEffect, useMemo, useState } from "react";
import {
  approveIngestionDocument,
  getIngestionDocument,
  listIngestionDocuments,
  rejectIngestionDocument,
  reviewerAdjudicationTemplateUrl,
  reviewerExportUrl,
  updateIngestionMetadata
} from "@/lib/api";

function parseList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

type DocSummary = {
  id: string;
  fileType: "decision_docx" | "law_pdf";
  title: string;
  citation: string;
  createdAt: string;
  approvedAt: string | null;
  rejectedAt: string | null;
  searchableAt: string | null;
  qcPassed: number;
  qcRequiredConfirmed: number;
  extractionConfidence: number;
  extractionWarnings: string[];
  warningCount: number;
  filteredNoiseCount: number;
  unresolvedReferenceCount: number;
  criticalExceptionCount: number;
  lowConfidenceTaxonomy: boolean;
  missingRulesDetection: boolean;
  missingOrdinanceDetection: boolean;
  failedQcRequirements: string[];
  isLikelyFixture: boolean;
  approvalReadiness: {
    eligible: boolean;
    score: number;
    blockers: string[];
    cautions: string[];
  };
  reviewerReady: boolean;
  reviewerReadyReasons: string[];
  reviewerRequiredActions: string[];
  reviewerRiskLevel: "low" | "medium" | "high";
  runtimeDisposition: "keep_blocked" | "possible_manual_context_fix_but_no_auto_apply";
  runtimeManualReasonCode: "none" | "parenthetical_prefix_fix_candidate" | "low_risk_not_found_residue";
  runtimeManualReasonSummary: string;
  runtimeSuggestedOperatorAction: string;
  runtimeOperatorReviewSummary: string;
  runtimeReviewDiagnostic: Record<string, unknown> | null;
  runtimePolicyReason: string;
  runtimeSurfaceForManualReview: boolean;
  runtimeManualReviewRequired: boolean;
  runtimeDoNotAutoApply: boolean;
  metadataConfirmationWouldUnlock: boolean;
  unresolvedBlockersAfterConfirmation: string[];
  unresolvedBuckets: string[];
  topRecommendedReviewerAction: string;
  estimatedReviewerEffort: "low" | "medium" | "high";
  candidateManualFixes: string[];
  recurringCitationFamilies: string[];
  canBatchReviewWith: string[];
  blocked37xReferences: Array<{
    family: "37.3" | "37.7" | "37.9";
    referenceType: string;
    rawValue: string;
    normalizedValue: string;
    message: string;
  }>;
  blocked37xReason: string;
  blocked37xReviewerHint: string;
  blocked37xSafeToBatchReview: boolean;
  blocked37xBatchKey: string | null;
  indexCodes: string[];
  status: "staged" | "searchable" | "approved" | "rejected";
  taxonomySuggestion: {
    caseTypeId: string | null;
    caseTypeLabel: string | null;
    confidence: number | null;
    fallback: boolean;
  };
};

type SectionParagraph = {
  id: string;
  anchor: string;
  paragraphOrder: number;
  text: string;
};

type SectionDetail = {
  id: string;
  heading: string;
  canonicalKey: string;
  sectionOrder: number;
  paragraphs: SectionParagraph[];
};

type ChunkDetail = {
  id: string;
  paragraphAnchor: string;
  paragraphAnchorEnd: string;
  citationAnchor: string;
  sectionLabel: string;
  chunkText: string;
};

type DocDetail = {
  id: string;
  fileType: "decision_docx" | "law_pdf";
  title: string;
  citation: string;
  sourceFileRef: string;
  caseNumber: string | null;
  decisionDate: string | null;
  authorName: string | null;
  outcomeLabel: "grant" | "deny" | "partial" | "unclear" | null;
  qcRequiredConfirmed: number;
  extractionWarnings: string[];
  taxonomySuggestion: {
    caseTypeId: string | null;
    caseTypeLabel: string | null;
    confidence: number | null;
    signals: string[];
    fallback: boolean;
  };
  referenceIssues: Array<{
    referenceType: string;
    rawValue: string;
    normalizedValue: string;
    message: string;
    severity: string;
    createdAt: string;
  }>;
  unresolvedReferenceCount: number;
  criticalExceptionReferences: string[];
  filteredNoiseCount: number;
  lowConfidenceTaxonomy: boolean;
  missingRulesDetection: boolean;
  missingOrdinanceDetection: boolean;
  failedQcRequirements: string[];
  isLikelyFixture: boolean;
  qcGateDiagnostics: {
    hasIndexCodes: boolean;
    hasRulesSection: boolean;
    hasOrdinanceSection: boolean;
    passed: boolean;
  };
  approvalReadiness: {
    eligible: boolean;
    score: number;
    blockers: string[];
    cautions: string[];
  };
  reviewerReady: boolean;
  reviewerReadyReasons: string[];
  reviewerRequiredActions: string[];
  reviewerRiskLevel: "low" | "medium" | "high";
  runtimeDisposition: "keep_blocked" | "possible_manual_context_fix_but_no_auto_apply";
  runtimeManualReasonCode: "none" | "parenthetical_prefix_fix_candidate" | "low_risk_not_found_residue";
  runtimeManualReasonSummary: string;
  runtimeSuggestedOperatorAction: string;
  runtimeOperatorReviewSummary: string;
  runtimeReviewDiagnostic: Record<string, unknown> | null;
  runtimePolicyReason: string;
  runtimeSurfaceForManualReview: boolean;
  runtimeManualReviewRequired: boolean;
  runtimeDoNotAutoApply: boolean;
  metadataConfirmationWouldUnlock: boolean;
  unresolvedBlockersAfterConfirmation: string[];
  unresolvedBuckets: string[];
  topRecommendedReviewerAction: string;
  estimatedReviewerEffort: "low" | "medium" | "high";
  candidateManualFixes: string[];
  recurringCitationFamilies: string[];
  canBatchReviewWith: string[];
  blocked37xReferences: Array<{
    family: "37.3" | "37.7" | "37.9";
    referenceType: string;
    rawValue: string;
    normalizedValue: string;
    message: string;
  }>;
  blocked37xReason: string;
  blocked37xReviewerHint: string;
  blocked37xSafeToBatchReview: boolean;
  blocked37xBatchKey: string | null;
  sections: SectionDetail[];
  chunks: ChunkDetail[];
};

export default function IngestionAdminPage() {
  const [documents, setDocuments] = useState<DocSummary[]>([]);
  const [summary, setSummary] = useState<{
    total: number;
    approved: number;
    rejected: number;
    searchable: number;
    staged: number;
    missingRequired: number;
    withWarnings: number;
    withUnresolvedReferences: number;
    withCriticalExceptions: number;
    withFilteredNoise: number;
    withLowConfidenceTaxonomy: number;
    withMissingRulesDetection: number;
    withMissingOrdinanceDetection: number;
    approvalReady: number;
    reviewerReady: number;
    likelyFixtures: number;
    realDocs: number;
    realApprovalReady: number;
    realReviewerReady: number;
    realApproved: number;
    realSearchable: number;
    surfacedRuntimeManualCandidates: number;
    surfacedRuntimeManualRealCandidates: number;
    surfacedRuntimeManualFixtureCandidates: number;
    unsafeRuntimeManualSurfacedViolations: number;
    unsafeRuntimeManualSuppressedCount: number;
    blockerBreakdown?: Array<{ blocker: string; count: number }>;
  } | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<DocDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveState, setSaveState] = useState<string | null>(null);
  const [diagnosticCopyState, setDiagnosticCopyState] = useState<string | null>(null);

  const [indexCodes, setIndexCodes] = useState("");
  const [rulesSections, setRulesSections] = useState("");
  const [ordinanceSections, setOrdinanceSections] = useState("");
  const [caseNumber, setCaseNumber] = useState("");
  const [decisionDate, setDecisionDate] = useState("");
  const [authorName, setAuthorName] = useState("");
  const [outcomeLabel, setOutcomeLabel] = useState("unclear");
  const [confirmRequired, setConfirmRequired] = useState(false);
  const [rejectReason, setRejectReason] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "staged" | "searchable" | "approved" | "rejected" | "pending">("all");
  const [fileTypeFilter, setFileTypeFilter] = useState<"all" | "decision_docx" | "law_pdf">("all");
  const [hasWarningsOnly, setHasWarningsOnly] = useState(false);
  const [missingRequiredOnly, setMissingRequiredOnly] = useState(false);
  const [unresolvedOnly, setUnresolvedOnly] = useState(false);
  const [criticalExceptionsOnly, setCriticalExceptionsOnly] = useState(false);
  const [filteredNoiseOnly, setFilteredNoiseOnly] = useState(false);
  const [lowConfidenceOnly, setLowConfidenceOnly] = useState(false);
  const [missingRulesOnly, setMissingRulesOnly] = useState(false);
  const [missingOrdinanceOnly, setMissingOrdinanceOnly] = useState(false);
  const [approvalReadyOnly, setApprovalReadyOnly] = useState(false);
  const [reviewerReadyOnly, setReviewerReadyOnly] = useState(false);
  const [runtimeManualCandidatesOnly, setRuntimeManualCandidatesOnly] = useState(false);
  const [realOnly, setRealOnly] = useState(true);
  const [blockerFilter, setBlockerFilter] = useState<string>("all");
  const [unresolvedTriageBucketFilter, setUnresolvedTriageBucketFilter] = useState<string>("all");
  const [recurringCitationFamilyFilter, setRecurringCitationFamilyFilter] = useState<string>("");
  const [blocked37xOnly, setBlocked37xOnly] = useState(false);
  const [blocked37xFamilyFilter, setBlocked37xFamilyFilter] = useState<string>("all");
  const [blocked37xBatchKeyFilter, setBlocked37xBatchKeyFilter] = useState<string>("");
  const [safeToBatchReviewOnly, setSafeToBatchReviewOnly] = useState(false);
  const [estimatedReviewerEffortFilter, setEstimatedReviewerEffortFilter] = useState<"all" | "low" | "medium" | "high">("all");
  const [reviewerRiskFilter, setReviewerRiskFilter] = useState<"all" | "low" | "medium" | "high">("all");
  const [queryText, setQueryText] = useState("");
  const [sortMode, setSortMode] = useState<
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
    | "blocked37xBatchKeyAsc"
  >("createdAtDesc");

  async function refreshList() {
    const response = await listIngestionDocuments({
      status: statusFilter,
      fileType: fileTypeFilter === "all" ? undefined : fileTypeFilter,
      hasWarnings: hasWarningsOnly,
      missingRequired: missingRequiredOnly,
      unresolvedReferencesOnly: unresolvedOnly,
      criticalExceptionsOnly,
      filteredNoiseOnly,
      lowConfidenceTaxonomyOnly: lowConfidenceOnly,
      missingRulesOnly,
      missingOrdinanceOnly,
      approvalReadyOnly,
      reviewerReadyOnly,
      runtimeManualCandidatesOnly,
      realOnly,
      blocker: blockerFilter === "all" ? undefined : blockerFilter,
      unresolvedTriageBucket: unresolvedTriageBucketFilter === "all" ? undefined : unresolvedTriageBucketFilter,
      recurringCitationFamily: recurringCitationFamilyFilter.trim() || undefined,
      blocked37xOnly,
      blocked37xFamily: blocked37xFamilyFilter === "all" ? undefined : blocked37xFamilyFilter,
      blocked37xBatchKey: blocked37xBatchKeyFilter.trim() || undefined,
      safeToBatchReviewOnly,
      estimatedReviewerEffort: estimatedReviewerEffortFilter === "all" ? undefined : estimatedReviewerEffortFilter,
      reviewerRiskLevel: reviewerRiskFilter === "all" ? undefined : reviewerRiskFilter,
      query: queryText.trim() || undefined,
      sort: sortMode,
      limit: 600
    });
    setDocuments(response.documents || []);
    setSummary(response.summary || null);
  }

  async function loadDetail(documentId: string) {
    setLoading(true);
    setError(null);
    try {
      const response = await getIngestionDocument(documentId);
      setDetail(response);
      setIndexCodes((response.indexCodes || []).join(", "));
      setRulesSections((response.rulesSections || []).join(", "));
      setOrdinanceSections((response.ordinanceSections || []).join(", "));
      setCaseNumber(response.caseNumber || "");
      setDecisionDate(response.decisionDate || "");
      setAuthorName(response.authorName || "");
      setOutcomeLabel(response.outcomeLabel || "unclear");
      setConfirmRequired(Boolean(response.qcRequiredConfirmed));
      setRejectReason("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load document");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshList().catch((err) => setError(err instanceof Error ? err.message : "Failed to load ingestion list"));
  }, [
    statusFilter,
    fileTypeFilter,
    hasWarningsOnly,
    missingRequiredOnly,
    unresolvedOnly,
    criticalExceptionsOnly,
    filteredNoiseOnly,
    lowConfidenceOnly,
    missingRulesOnly,
    missingOrdinanceOnly,
    approvalReadyOnly,
    reviewerReadyOnly,
    runtimeManualCandidatesOnly,
    realOnly,
    blockerFilter,
    unresolvedTriageBucketFilter,
    recurringCitationFamilyFilter,
    blocked37xOnly,
    blocked37xFamilyFilter,
    blocked37xBatchKeyFilter,
    safeToBatchReviewOnly,
    estimatedReviewerEffortFilter,
    reviewerRiskFilter,
    queryText,
    sortMode
  ]);

  useEffect(() => {
    if (selectedId) {
      loadDetail(selectedId).catch((err) => setError(err instanceof Error ? err.message : "Failed to load details"));
    }
  }, [selectedId]);

  const selectedSummary = useMemo(() => documents.find((doc) => doc.id === selectedId) || null, [documents, selectedId]);

  function currentReviewerExportParams(overrides?: Partial<{
    blocked37xOnly: boolean;
    blocked37xBatchKey: string;
    format: "json" | "csv" | "markdown";
  }>) {
    return {
      realOnly,
      unresolvedTriageBucket: unresolvedTriageBucketFilter === "all" ? undefined : unresolvedTriageBucketFilter,
      blocked37xFamily: blocked37xFamilyFilter === "all" ? undefined : blocked37xFamilyFilter,
      estimatedReviewerEffort: estimatedReviewerEffortFilter === "all" ? undefined : estimatedReviewerEffortFilter,
      reviewerRiskLevel: reviewerRiskFilter === "all" ? undefined : reviewerRiskFilter,
      safeToBatchReviewOnly,
      blocked37xBatchKey: overrides?.blocked37xBatchKey ?? (blocked37xBatchKeyFilter.trim() || undefined),
      blocked37xOnly: overrides?.blocked37xOnly ?? blocked37xOnly,
      limit: 1200,
      format: overrides?.format
    };
  }

  async function downloadFromUrl(url: string, fallbackFilename: string) {
    const response = await fetch(url);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Export failed (${response.status}): ${text}`);
    }
    const blob = await response.blob();
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const disposition = response.headers.get("content-disposition") || "";
    const matched = disposition.match(/filename=\"?([^\";]+)\"?/i);
    a.href = href;
    a.download = matched?.[1] || fallbackFilename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
  }

  async function copyRuntimeDiagnosticBlob() {
    if (!detail?.runtimeReviewDiagnostic) return;
    const payload = JSON.stringify(detail.runtimeReviewDiagnostic, null, 2);
    try {
      await navigator.clipboard.writeText(payload);
      setDiagnosticCopyState("Runtime diagnostic copied.");
    } catch {
      setDiagnosticCopyState("Copy failed. Select and copy manually.");
    }
    setTimeout(() => setDiagnosticCopyState(null), 2200);
  }

  async function exportCurrentQueueAllFormats() {
    setSaveState("Exporting reviewer batch package...");
    try {
      await downloadFromUrl(reviewerExportUrl(currentReviewerExportParams({ format: "json" })), "reviewer-batch-export.json");
      await downloadFromUrl(reviewerExportUrl(currentReviewerExportParams({ format: "csv" })), "reviewer-batch-export.csv");
      await downloadFromUrl(reviewerExportUrl(currentReviewerExportParams({ format: "markdown" })), "reviewer-batch-export.md");
      await downloadFromUrl(
        reviewerAdjudicationTemplateUrl({ ...currentReviewerExportParams(), format: "csv" }),
        "reviewer-adjudication-template.csv"
      );
      setSaveState("Reviewer batch package exported.");
    } catch (error) {
      setSaveState(error instanceof Error ? error.message : "Export failed");
    }
  }

  async function exportSelectedBatch() {
    if (!selectedSummary?.blocked37xBatchKey) {
      setSaveState("Select a row with a blocked 37.x batch key first.");
      return;
    }
    setSaveState("Exporting selected reviewer batch...");
    try {
      const params = currentReviewerExportParams({
        blocked37xOnly: true,
        blocked37xBatchKey: selectedSummary.blocked37xBatchKey,
        format: "json"
      });
      await downloadFromUrl(reviewerExportUrl(params), "reviewer-selected-batch.json");
      await downloadFromUrl(reviewerExportUrl({ ...params, format: "csv" }), "reviewer-selected-batch.csv");
      await downloadFromUrl(reviewerExportUrl({ ...params, format: "markdown" }), "reviewer-selected-batch.md");
      setSaveState("Selected reviewer batch exported.");
    } catch (error) {
      setSaveState(error instanceof Error ? error.message : "Export failed");
    }
  }

  async function exportBlocked37xOnlyBatch() {
    setSaveState("Exporting blocked 37.x reviewer queue...");
    try {
      const params = currentReviewerExportParams({ blocked37xOnly: true, format: "json" });
      await downloadFromUrl(reviewerExportUrl(params), "reviewer-blocked-37x.json");
      await downloadFromUrl(reviewerExportUrl({ ...params, format: "csv" }), "reviewer-blocked-37x.csv");
      await downloadFromUrl(reviewerExportUrl({ ...params, format: "markdown" }), "reviewer-blocked-37x.md");
      setSaveState("Blocked 37.x reviewer queue exported.");
    } catch (error) {
      setSaveState(error instanceof Error ? error.message : "Export failed");
    }
  }

  async function onSaveMetadata() {
    if (!selectedId) return;
    setSaveState("Saving metadata...");
    try {
      await updateIngestionMetadata(selectedId, {
        index_codes: parseList(indexCodes),
        rules_sections: parseList(rulesSections),
        ordinance_sections: parseList(ordinanceSections),
        case_number: caseNumber || null,
        decision_date: decisionDate || null,
        author_name: authorName || null,
        outcome_label: outcomeLabel,
        confirm_required_metadata: confirmRequired
      });
      await refreshList();
      await loadDetail(selectedId);
      setSaveState("Metadata saved.");
    } catch (err) {
      setSaveState(err instanceof Error ? err.message : "Save failed");
    }
  }

  async function onApprove() {
    if (!selectedId) return;
    setSaveState("Approving document...");
    try {
      await approveIngestionDocument(selectedId);
      await refreshList();
      await loadDetail(selectedId);
      setSaveState("Document approved.");
    } catch (err) {
      setSaveState(err instanceof Error ? err.message : "Approval failed");
    }
  }

  async function onReject() {
    if (!selectedId || rejectReason.trim().length < 3) return;
    setSaveState("Rejecting document...");
    try {
      await rejectIngestionDocument(selectedId, rejectReason.trim());
      await refreshList();
      await loadDetail(selectedId);
      setSaveState("Document rejected.");
    } catch (err) {
      setSaveState(err instanceof Error ? err.message : "Rejection failed");
    }
  }

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Ingestion QC Admin</h1>
      <p style={{ color: "var(--muted)" }}>
        Review extracted metadata, section detection, anchors, and chunk boundaries before approving pilot historical decisions.
      </p>
      {summary ? (
        <p style={{ color: "var(--muted)" }}>
          Showing {summary.total} docs · staged {summary.staged} · searchable {summary.searchable} · approved {summary.approved} · rejected {summary.rejected} · warnings {summary.withWarnings}
          {" · "}unresolved refs {summary.withUnresolvedReferences}
          {" · "}critical exceptions {summary.withCriticalExceptions}
          {" · "}filtered noise {summary.withFilteredNoise}
          {" · "}low taxonomy {summary.withLowConfidenceTaxonomy}
          {" · "}approval ready {summary.approvalReady}
          {" · "}reviewer ready {summary.reviewerReady}
          {" · "}fixtures {summary.likelyFixtures}
          {" · "}real docs {summary.realDocs}
          {" · "}real ready {summary.realApprovalReady}
          {" · "}real reviewer ready {summary.realReviewerReady}
          {" · "}real approved {summary.realApproved}
          {" · "}runtime surfaced {summary.surfacedRuntimeManualCandidates}
          {" · "}runtime surfaced (real) {summary.surfacedRuntimeManualRealCandidates}
          {" · "}runtime surfaced (fixtures) {summary.surfacedRuntimeManualFixtureCandidates}
          {" · "}unsafe surfaced violations {summary.unsafeRuntimeManualSurfacedViolations}
          {" · "}unsafe suppressed {summary.unsafeRuntimeManualSuppressedCount}
        </p>
      ) : null}

      {error ? <p style={{ color: "#8b2a2a" }}>{error}</p> : null}
      {saveState ? <p style={{ color: "var(--muted)" }}>{saveState}</p> : null}

      <div style={{ display: "grid", gridTemplateColumns: "minmax(280px, 0.9fr) minmax(420px, 1.3fr)", gap: "1rem" }}>
        <section className="card" style={{ padding: "0.8rem", maxHeight: "74vh", overflow: "auto" }}>
          <h2 style={{ marginTop: 0 }}>Pilot Documents</h2>
          <div style={{ display: "flex", gap: "0.45rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
            <button
              type="button"
              onClick={() => void exportCurrentQueueAllFormats()}
              style={{ border: "1px solid var(--border)", borderRadius: "8px", background: "#fff", padding: "0.35rem 0.55rem", cursor: "pointer" }}
            >
              Export Current Queue
            </button>
            <button
              type="button"
              onClick={() => void exportSelectedBatch()}
              style={{ border: "1px solid var(--border)", borderRadius: "8px", background: "#fff", padding: "0.35rem 0.55rem", cursor: "pointer" }}
            >
              Export Selected Batch
            </button>
            <button
              type="button"
              onClick={() => void exportBlocked37xOnlyBatch()}
              style={{ border: "1px solid var(--border)", borderRadius: "8px", background: "#fff", padding: "0.35rem 0.55rem", cursor: "pointer" }}
            >
              Export Blocked 37.x Only
            </button>
          </div>
          <div style={{ display: "grid", gap: "0.4rem", marginBottom: "0.7rem" }}>
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value as typeof statusFilter)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All statuses</option>
              <option value="staged">Staged only</option>
              <option value="searchable">Searchable only</option>
              <option value="approved">Approved only</option>
              <option value="rejected">Rejected only</option>
              <option value="pending">Pending only</option>
            </select>
            <select value={fileTypeFilter} onChange={(e) => setFileTypeFilter(e.target.value as typeof fileTypeFilter)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All file types</option>
              <option value="decision_docx">Decision DOCX</option>
              <option value="law_pdf">Law PDF</option>
            </select>
            <select value={sortMode} onChange={(e) => setSortMode(e.target.value as typeof sortMode)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="createdAtDesc">Newest first</option>
              <option value="createdAtAsc">Oldest first</option>
              <option value="confidenceDesc">Highest confidence</option>
              <option value="confidenceAsc">Lowest confidence</option>
              <option value="titleAsc">Title A-Z</option>
              <option value="titleDesc">Title Z-A</option>
              <option value="warningCountDesc">Most warnings</option>
              <option value="unresolvedReferenceDesc">Most unresolved refs</option>
              <option value="criticalExceptionDesc">Most critical exceptions</option>
              <option value="approvalReadinessDesc">Highest approval readiness</option>
              <option value="reviewerReadinessDesc">Highest reviewer readiness</option>
              <option value="reviewerEffortAsc">Lowest reviewer effort</option>
              <option value="batchabilityDesc">Highest batchability</option>
              <option value="unresolvedLeverageDesc">Highest unresolved leverage</option>
              <option value="blocked37xBatchKeyAsc">Blocked 37.x batch key</option>
            </select>
            <input value={queryText} onChange={(e) => setQueryText(e.target.value)} placeholder="Search title/citation/case #" style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={hasWarningsOnly} onChange={(e) => setHasWarningsOnly(e.target.checked)} />
              Warnings only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={missingRequiredOnly} onChange={(e) => setMissingRequiredOnly(e.target.checked)} />
              Missing required QC metadata only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={unresolvedOnly} onChange={(e) => setUnresolvedOnly(e.target.checked)} />
              Unresolved references only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={criticalExceptionsOnly} onChange={(e) => setCriticalExceptionsOnly(e.target.checked)} />
              Critical-reference exceptions only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={filteredNoiseOnly} onChange={(e) => setFilteredNoiseOnly(e.target.checked)} />
              Filtered-noise docs only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={lowConfidenceOnly} onChange={(e) => setLowConfidenceOnly(e.target.checked)} />
              Low-confidence taxonomy only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={missingRulesOnly} onChange={(e) => setMissingRulesOnly(e.target.checked)} />
              Missing rules detection only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={missingOrdinanceOnly} onChange={(e) => setMissingOrdinanceOnly(e.target.checked)} />
              Missing ordinance detection only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={approvalReadyOnly} onChange={(e) => setApprovalReadyOnly(e.target.checked)} />
              Approval-ready only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={reviewerReadyOnly} onChange={(e) => setReviewerReadyOnly(e.target.checked)} />
              Reviewer-ready only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={runtimeManualCandidatesOnly} onChange={(e) => setRuntimeManualCandidatesOnly(e.target.checked)} />
              Runtime manual candidates only
            </label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={realOnly} onChange={(e) => setRealOnly(e.target.checked)} />
              Real docs only
            </label>
            <select value={blockerFilter} onChange={(e) => setBlockerFilter(e.target.value)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All blockers</option>
              <option value="metadata_not_confirmed">Blocker: metadata_not_confirmed</option>
              <option value="qc_gate_not_passed">Blocker: qc_gate_not_passed</option>
              <option value="unresolved_references_above_threshold">Blocker: unresolved_references_above_threshold</option>
              <option value="critical_reference_exception_present">Blocker: critical_reference_exception_present</option>
              <option value="warnings_above_threshold">Blocker: warnings_above_threshold</option>
            </select>
            <select
              value={unresolvedTriageBucketFilter}
              onChange={(e) => setUnresolvedTriageBucketFilter(e.target.value)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All unresolved triage buckets</option>
              <option value="safe_manual_drop_candidate">safe_manual_drop_candidate</option>
              <option value="likely_context_relabel_candidate">likely_context_relabel_candidate</option>
              <option value="likely_parenthetical_or_prefix_fix">likely_parenthetical_or_prefix_fix</option>
              <option value="duplicate_or_redundant_reference">duplicate_or_redundant_reference</option>
              <option value="structurally_blocked_not_found">structurally_blocked_not_found</option>
              <option value="cross_context_ambiguous">cross_context_ambiguous</option>
              <option value="unsafe_37x_structural_block">unsafe_37x_structural_block</option>
            </select>
            <input
              value={recurringCitationFamilyFilter}
              onChange={(e) => setRecurringCitationFamilyFilter(e.target.value)}
              placeholder="Recurring citation family (e.g. 37.2)"
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            />
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={blocked37xOnly} onChange={(e) => setBlocked37xOnly(e.target.checked)} />
              Blocked 37.x only
            </label>
            <select
              value={blocked37xFamilyFilter}
              onChange={(e) => setBlocked37xFamilyFilter(e.target.value)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All blocked 37.x families</option>
              <option value="37.3">37.3</option>
              <option value="37.7">37.7</option>
              <option value="37.9">37.9</option>
            </select>
            <input
              value={blocked37xBatchKeyFilter}
              onChange={(e) => setBlocked37xBatchKeyFilter(e.target.value)}
              placeholder="Blocked 37.x batch key"
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            />
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={safeToBatchReviewOnly} onChange={(e) => setSafeToBatchReviewOnly(e.target.checked)} />
              Safe to batch review only
            </label>
            <select
              value={estimatedReviewerEffortFilter}
              onChange={(e) => setEstimatedReviewerEffortFilter(e.target.value as typeof estimatedReviewerEffortFilter)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All reviewer effort</option>
              <option value="low">Effort: low</option>
              <option value="medium">Effort: medium</option>
              <option value="high">Effort: high</option>
            </select>
            <select
              value={reviewerRiskFilter}
              onChange={(e) => setReviewerRiskFilter(e.target.value as typeof reviewerRiskFilter)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All reviewer risk</option>
              <option value="low">Risk: low</option>
              <option value="medium">Risk: medium</option>
              <option value="high">Risk: high</option>
            </select>
          </div>
          <div style={{ overflowX: "auto", border: "1px solid var(--border)", borderRadius: "8px" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
              <thead>
                <tr style={{ background: "#f7f8fa" }}>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Doc</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Blockers</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Unresolved</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Buckets</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Blocked 37.x</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Effort</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Risk</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Batch Key</th>
                  <th style={{ textAlign: "left", padding: "0.45rem", borderBottom: "1px solid var(--border)" }}>Top Action</th>
                </tr>
              </thead>
              <tbody>
                {documents.map((doc) => (
                  <tr
                    key={doc.id}
                    onClick={() => setSelectedId(doc.id)}
                    style={{
                      cursor: "pointer",
                      background: selectedId === doc.id ? "#eef6ff" : "#fff",
                      borderBottom: "1px solid var(--border)"
                    }}
                  >
                    <td style={{ padding: "0.45rem" }}>
                      <div style={{ fontWeight: 600 }}>{doc.title}</div>
                      <div style={{ color: "var(--muted)" }}>{doc.citation}</div>
                      <div style={{ marginTop: "0.2rem", display: "flex", gap: "0.3rem", flexWrap: "wrap" }}>
                        <span style={{ fontSize: "0.72rem", border: "1px solid var(--border)", borderRadius: "999px", padding: "0.08rem 0.4rem", background: doc.isLikelyFixture ? "#fff7ed" : "#ecfdf3" }}>
                          {doc.isLikelyFixture ? "Fixture" : "Real candidate"}
                        </span>
                        {(doc.blocked37xReferences || []).some((ref) => ["37.3", "37.7", "37.9"].includes(ref.family)) && !doc.runtimeSurfaceForManualReview ? (
                          <span style={{ fontSize: "0.72rem", border: "1px solid var(--border)", borderRadius: "999px", padding: "0.08rem 0.4rem", background: "#f1f5f9" }}>
                            Unsafe suppressed
                          </span>
                        ) : null}
                      </div>
                    </td>
                    <td style={{ padding: "0.45rem" }}>{(doc.approvalReadiness?.blockers || []).join(", ") || "none"}</td>
                    <td style={{ padding: "0.45rem" }}>{doc.unresolvedReferenceCount}</td>
                    <td style={{ padding: "0.45rem" }}>{(doc.unresolvedBuckets || []).join(", ") || "none"}</td>
                    <td style={{ padding: "0.45rem" }}>
                      {(doc.blocked37xReferences || []).map((r) => r.family).join(", ") || "none"}
                      <div style={{ color: "var(--muted)" }}>{doc.blocked37xReason || "none"}</div>
                    </td>
                    <td style={{ padding: "0.45rem" }}>{doc.estimatedReviewerEffort}</td>
                    <td style={{ padding: "0.45rem" }}>{doc.reviewerRiskLevel}</td>
                    <td style={{ padding: "0.45rem" }}>{doc.blocked37xBatchKey || "none"}</td>
                    <td style={{ padding: "0.45rem" }}>{doc.topRecommendedReviewerAction || "none"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="card" style={{ padding: "1rem", maxHeight: "74vh", overflow: "auto" }}>
          {!selectedSummary ? <p>Select a document to review.</p> : null}
          {loading ? <p>Loading detail...</p> : null}

          {detail ? (
            <div style={{ display: "grid", gap: "0.9rem" }}>
              <div>
                <h2 style={{ marginTop: 0, marginBottom: "0.25rem" }}>{detail.title}</h2>
                <p style={{ margin: 0, color: "var(--muted)" }}>
                  {detail.citation} · {detail.fileType} · Source: <code>{detail.sourceFileRef}</code>
                </p>
                <p style={{ margin: "0.2rem 0 0", color: "var(--muted)" }}>
                  Suggested case type: <strong>{detail.taxonomySuggestion.caseTypeId || "unknown"}</strong> {detail.taxonomySuggestion.confidence !== null ? `(${Math.round(detail.taxonomySuggestion.confidence * 100)}%)` : ""} {detail.taxonomySuggestion.fallback ? "· fallback used" : ""}
                </p>
              </div>

              <div>
                <h3 style={{ marginBottom: "0.35rem" }}>Extracted Metadata</h3>
                <input value={indexCodes} onChange={(e) => setIndexCodes(e.target.value)} placeholder="Index Codes (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <input value={rulesSections} onChange={(e) => setRulesSections(e.target.value)} placeholder="Rules Sections (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <input value={ordinanceSections} onChange={(e) => setOrdinanceSections(e.target.value)} placeholder="Ordinance Sections (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <input value={caseNumber} onChange={(e) => setCaseNumber(e.target.value)} placeholder="Case Number" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <input type="date" value={decisionDate || ""} onChange={(e) => setDecisionDate(e.target.value)} style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <input value={authorName} onChange={(e) => setAuthorName(e.target.value)} placeholder="Author / Judge / ALJ" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} />
                <select value={outcomeLabel} onChange={(e) => setOutcomeLabel(e.target.value)} style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }}>
                  <option value="unclear">Outcome unclear</option>
                  <option value="grant">Grant</option>
                  <option value="deny">Deny</option>
                  <option value="partial">Partial</option>
                </select>
                <label style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginBottom: "0.4rem" }}>
                  <input type="checkbox" checked={confirmRequired} onChange={(e) => setConfirmRequired(e.target.checked)} />
                  Confirm required metadata for approval gate
                </label>
                <button onClick={onSaveMetadata} style={{ border: 0, background: "var(--accent)", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
                  Save Metadata
                </button>
              </div>

              <div>
                <h3 style={{ marginBottom: "0.35rem" }}>Warnings</h3>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Unresolved references: {detail.unresolvedReferenceCount || 0} · Critical exceptions: {(detail.criticalExceptionReferences || []).join(", ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Filtered noise: {detail.filteredNoiseCount || 0} · Missing rules: {detail.missingRulesDetection ? "yes" : "no"} · Missing ordinance: {detail.missingOrdinanceDetection ? "yes" : "no"} · Low taxonomy confidence: {detail.lowConfidenceTaxonomy ? "yes" : "no"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Approval readiness: score {detail.approvalReadiness?.score ?? 0} · eligible {detail.approvalReadiness?.eligible ? "yes" : "no"} · blockers {(detail.approvalReadiness?.blockers || []).join(", ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Reviewer readiness: {detail.reviewerReady ? "yes" : "no"} · risk {detail.reviewerRiskLevel} · metadata unlock {detail.metadataConfirmationWouldUnlock ? "yes" : "no"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Runtime disposition: {detail.runtimeDisposition} · manual surface {detail.runtimeSurfaceForManualReview ? "yes" : "no"} · doNotAutoApply {detail.runtimeDoNotAutoApply ? "true" : "false"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Runtime policy reason: {detail.runtimePolicyReason || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Runtime manual reason: {detail.runtimeManualReasonCode || "none"} · {detail.runtimeManualReasonSummary || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Runtime suggested operator action: {detail.runtimeSuggestedOperatorAction || "none"}
                </p>
                {detail.runtimeSurfaceForManualReview ? (
                  <div style={{ margin: "0.5rem 0", padding: "0.55rem", border: "1px solid var(--border)", borderRadius: "8px", background: "#f8fafc" }}>
                    <p style={{ margin: "0 0 0.35rem", fontWeight: 600 }}>Why this surfaced</p>
                    <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>{detail.runtimeOperatorReviewSummary || "none"}</p>
                    <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                      Buckets: {(detail.unresolvedBuckets || []).join(", ") || "none"} · Families: {(detail.recurringCitationFamilies || []).join(", ") || "none"}
                    </p>
                    <button
                      type="button"
                      onClick={() => copyRuntimeDiagnosticBlob()}
                      style={{ marginTop: "0.2rem", border: "1px solid var(--border)", borderRadius: "6px", padding: "0.3rem 0.55rem", background: "#fff" }}
                    >
                      Copy Runtime Diagnostic
                    </button>
                    {diagnosticCopyState ? <p style={{ margin: "0.35rem 0 0", color: "var(--muted)" }}>{diagnosticCopyState}</p> : null}
                  </div>
                ) : null}
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Reviewer reasons: {(detail.reviewerReadyReasons || []).join(", ") || "none"} · required actions: {(detail.reviewerRequiredActions || []).join(" | ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Blockers after confirmation: {(detail.unresolvedBlockersAfterConfirmation || []).join(", ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Unresolved triage buckets: {(detail.unresolvedBuckets || []).join(", ") || "none"} · effort {detail.estimatedReviewerEffort}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Top reviewer action: {detail.topRecommendedReviewerAction || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Candidate manual fixes: {(detail.candidateManualFixes || []).join(" | ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Recurring citation families: {(detail.recurringCitationFamilies || []).join(", ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Blocked 37.x refs: {(detail.blocked37xReferences || []).map((r) => `${r.family}:${r.referenceType}`).join(", ") || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Blocked 37.x reason: {detail.blocked37xReason || "none"} · safe batch review {detail.blocked37xSafeToBatchReview ? "yes" : "no"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  Blocked 37.x reviewer hint: {detail.blocked37xReviewerHint || "none"} · batch key {detail.blocked37xBatchKey || "none"}
                </p>
                <p style={{ margin: "0 0 0.25rem", color: "var(--muted)" }}>
                  QC diagnostics: index {detail.qcGateDiagnostics?.hasIndexCodes ? "yes" : "no"} · rules {detail.qcGateDiagnostics?.hasRulesSection ? "yes" : "no"} · ordinance {detail.qcGateDiagnostics?.hasOrdinanceSection ? "yes" : "no"} · failed {(detail.failedQcRequirements || []).join(", ") || "none"}
                </p>
                {(detail.extractionWarnings || []).length === 0 ? <p style={{ margin: 0 }}>No warnings.</p> : null}
                {(detail.extractionWarnings || []).map((warning) => (
                  <p key={warning} style={{ margin: "0.1rem 0", color: "#8b2a2a" }}>{warning}</p>
                ))}
                {(detail.referenceIssues || []).map((issue) => (
                  <p key={`${issue.referenceType}-${issue.normalizedValue}-${issue.createdAt}`} style={{ margin: "0.1rem 0", color: issue.severity === "error" ? "#8b2a2a" : "#7a5524" }}>
                    [{issue.referenceType}] {issue.rawValue} - {issue.message}
                  </p>
                ))}
              </div>

              <div>
                <h3 style={{ marginBottom: "0.35rem" }}>Section + Anchor Preview</h3>
                {(detail.sections || []).map((section) => (
                  <div key={section.id} style={{ borderTop: "1px solid var(--border)", paddingTop: "0.35rem", marginTop: "0.35rem" }}>
                    <p style={{ margin: 0 }}><strong>{section.heading}</strong> ({section.canonicalKey})</p>
                    {section.paragraphs.slice(0, 4).map((paragraph) => (
                      <p key={paragraph.id} style={{ margin: "0.2rem 0", fontSize: "0.88rem" }}>
                        <code>{paragraph.anchor}</code> · {paragraph.text.slice(0, 140)}
                      </p>
                    ))}
                  </div>
                ))}
              </div>

              <div>
                <h3 style={{ marginBottom: "0.35rem" }}>Chunk Boundary Preview</h3>
                {(detail.chunks || []).slice(0, 20).map((chunk) => (
                  <p key={chunk.id} style={{ margin: "0.2rem 0", fontSize: "0.88rem" }}>
                    <code>{chunk.paragraphAnchor}</code> - <code>{chunk.paragraphAnchorEnd}</code> · {chunk.sectionLabel} · {chunk.chunkText.slice(0, 150)}
                  </p>
                ))}
              </div>

              <div style={{ borderTop: "1px solid var(--border)", paddingTop: "0.6rem" }}>
                <h3 style={{ marginBottom: "0.35rem" }}>QC Decision</h3>
                <button onClick={onApprove} style={{ marginRight: "0.6rem", border: 0, background: "#1f6d4f", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
                  Approve for Search
                </button>
                <input value={rejectReason} onChange={(e) => setRejectReason(e.target.value)} placeholder="Reject reason" style={{ marginRight: "0.4rem", padding: "0.45rem", border: "1px solid var(--border)", borderRadius: "8px", minWidth: "220px" }} />
                <button onClick={onReject} style={{ border: 0, background: "#8b2a2a", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
                  Reject
                </button>
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </main>
  );
}
