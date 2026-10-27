"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  apiBase,
  approveIngestionDocument,
  advanceReviewerQueueIndex,
  getReviewerPeers,
  getIngestionDocument,
  listIngestionDocuments,
  rejectIngestionDocument,
  reviewerAdjudicationTemplateUrl,
  reviewerExportUrl,
  updateIngestionMetadata
} from "@/lib/api";
import { ReviewerSession, type ReviewIdentity } from "@/lib/reviewer-session";
import { reviewerReferencePatch } from "@/lib/reviewer-reference-patch";
import { downloadFromUrl } from "@/lib/ui-helpers";

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
  triageGroupKey: string;
  peerCount: number;
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
    previousSuggestion?: {caseTypeId:string|null;confidence:number|null};
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
  referenceEditionContext?:{status?:string;reason?:string;edition_id?:string|null;as_of_date?:string|null;date_basis?:string;registry_epoch?:number|null};
  referenceEditionBindings?:Array<{referenceLinkId:string;referenceType:string;canonicalValue:string;editionId:string|null;editionProvisionId:string|null;editionBodySha256:string|null;applicabilityStatus:string}>;
  documentRevision: number;
  reviewerOverrides?: Record<string, {value: unknown; provenance: string; recordedAt: string}>;
  id: string;
  fileType: "decision_docx" | "law_pdf";
  title: string;
  citation: string;
  sourceFileRef: string;
  caseNumber: string | null;
  decisionDate: string | null;
  unverifiedDecisionDate?:string|null;
  authorName: string | null;
  outcomeLabel: "grant" | "deny" | "partial" | "unclear" | null;
  qcRequiredConfirmed: number;
  extractionWarnings: string[];
  taxonomySuggestion: {
    caseTypeId: string | null;
    caseTypeLabel: string | null;
    previousSuggestion?: {caseTypeId:string|null;confidence:number|null};
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

type ReviewValues = {
  indexCodes: string; rulesSections: string; ordinanceSections: string; caseNumber: string;
  decisionDate: string; authorName: string; outcomeLabel: string; confirmRequired: boolean; rejectReason: string;
};
type ReviewForm = ReviewIdentity & { values: ReviewValues; baseline: ReviewValues };
const emptyValues: ReviewValues = { indexCodes: "", rulesSections: "", ordinanceSections: "", caseNumber: "",
  decisionDate: "", authorName: "", outcomeLabel: "unclear", confirmRequired: false, rejectReason: "" };

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
    requestedLimit?: number;
    candidatePoolSize?: number;
    candidatePoolLimit?: number;
    filteredCandidateCount?: number;
    derivedProcessingApplied?: boolean;
    derivedCandidatePoolExhausted?: boolean;
    derivedCandidatePoolLimited?: boolean;
    blockerBreakdown?: Array<{ blocker: string; count: number }>;
  } | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<DocDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveState, setSaveState] = useState<string | null>(null);
  const [diagnosticCopyState, setDiagnosticCopyState] = useState<string | null>(null);

  const sessionRef = useRef(new ReviewerSession());
  const [form, setForm] = useState<ReviewForm | null>(null);
  const [mutating, setMutating] = useState(false);
  const [mutationNotice, setMutationNotice] = useState<string | null>(null);
  const [listRefresh, setListRefresh] = useState(0);
  const values = form?.values || emptyValues;
  const { indexCodes, rulesSections, ordinanceSections, caseNumber, decisionDate, authorName, outcomeLabel, confirmRequired, rejectReason } = values;
  const dirty = Boolean(form && JSON.stringify(form.values) !== JSON.stringify(form.baseline));
  const metadataDirty = Boolean(form && JSON.stringify({ ...form.values, rejectReason: "" }) !== JSON.stringify({ ...form.baseline, rejectReason: "" }));
  const canMutate = !mutating && !loading && sessionRef.current.canMutate(form);
  function edit<K extends keyof ReviewValues>(field: K, value: ReviewValues[K]) {
    if (!canMutate) return;
    setForm(current => current ? { ...current, values: { ...current.values, [field]: value } } : null);
  }
  function selectDocument(documentId: string) {
    if (documentId === sessionRef.current.selectedId) return;
    if (dirty && !mutating && !window.confirm("Discard unsaved changes for this document?")) return;
    sessionRef.current.select(documentId);
    setDetail(null);
    setForm(null);
    setLoading(true);
    setError(null);
    setDiagnosticCopyState(null);
    setSelectedId(documentId);
  }

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
  // The list effect keys off the debounced mirror so typing doesn't fire a full 600-row request per
  // keystroke; the input itself stays on queryText for instant feedback.
  const [debouncedQueryText, setDebouncedQueryText] = useState("");

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQueryText(queryText), 300);
    return () => clearTimeout(timer);
  }, [queryText]);
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

  const [listLoading,setListLoading]=useState(false);
  const [listError,setListError]=useState("");
  const [listCursor,setListCursor]=useState<string|undefined>();
  const [previousCursors,setPreviousCursors]=useState<string[]>([]);
  const [nextCursor,setNextCursor]=useState<string|null>(null);
  const [peerIds,setPeerIds]=useState<string[]>([]);
  const [peerCursor,setPeerCursor]=useState<string|undefined>();
  const [peerGroup,setPeerGroup]=useState<string|undefined>();
  const [peerDocument,setPeerDocument]=useState<string>("");
  const [peerLoading,setPeerLoading]=useState(false);
  const peerRequestRef=useRef(0);
  const listRequestRef = useRef(0);

  async function refreshList(cursor?:string,direction:"reset"|"next"|"previous"="reset") {
    const requestId = ++listRequestRef.current;
    setListLoading(true);setListError("");
    try {
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
      query: debouncedQueryText.trim() || undefined,
      sort: sortMode,
      limit: 50, cursor
    });
    // Only the latest refresh may apply: filters/typing can fire refreshes faster than responses
    // return, and an out-of-order response would display rows for a stale filter combination.
    if (listRequestRef.current !== requestId) return;
    setDocuments(response.documents || []);
    peerRequestRef.current++;setPeerGroup(undefined);setPeerIds([]);setPeerLoading(false);
    setSummary(response.summary || null);
    setNextCursor(response.nextCursor || null);
    setPreviousCursors(previous=>direction==='reset'?[]:direction==='next'?[...previous,listCursor||'']:previous.slice(0,-1));
    setListCursor(cursor);
    } catch(error) {
      if(listRequestRef.current!==requestId)return;
      peerRequestRef.current++;setPeerGroup(undefined);setPeerIds([]);setPeerLoading(false);
      setNextCursor(null);setPreviousCursors([]);setDocuments([]);setSummary(null);setListCursor(undefined);
      setListError(error instanceof Error?error.message:"Failed to load reviewer queue");
    } finally {if(listRequestRef.current===requestId)setListLoading(false);}
  }

  async function refreshQueueIndex() {
    if(listLoading)return;
    setListLoading(true);
    const expectedList=listRequestRef.current;
    try {
      await advanceReviewerQueueIndex();
      if(listRequestRef.current===expectedList)await refreshList();
    } catch(error){
      if(listRequestRef.current===expectedList){setListError(error instanceof Error?error.message:"Reviewer queue refresh failed");setListLoading(false);}
    }
  }
  async function showPeers(groupKey:string,documentId:string,cursor?:string) {
    const ticket=++peerRequestRef.current;setPeerLoading(true);
    try {
      const response=await getReviewerPeers(groupKey,documentId,cursor);
      if(ticket!==peerRequestRef.current)return;
      setPeerGroup(groupKey);setPeerDocument(documentId);setPeerIds(response.ids||[]);setPeerCursor(response.nextCursor||undefined);
    } catch(error){if(ticket===peerRequestRef.current){setPeerIds([]);setPeerGroup(undefined);setListError(error instanceof Error?error.message:"Peer list changed; restart it.");}}
    finally{if(ticket===peerRequestRef.current)setPeerLoading(false);}
  }

  async function loadDetail(documentId: string) {
    const ticket = sessionRef.current.startLoad(documentId);
    if (!ticket) return;
    setLoading(true);
    setDetail(null);
    setForm(null);
    setError(null);
    try {
      const response = await getIngestionDocument(documentId);
      if (!sessionRef.current.acceptLoad(ticket, response)) return;
      const values: ReviewValues = {
        indexCodes: (response.indexCodes || []).join(", "), rulesSections: (response.rulesSections || []).join(", "),
        ordinanceSections: (response.ordinanceSections || []).join(", "), caseNumber: response.caseNumber || "",
        decisionDate: response.decisionDate || "", authorName: response.authorName || "",
        outcomeLabel: response.outcomeLabel || "unclear", confirmRequired: Boolean(response.qcRequiredConfirmed), rejectReason: ""
      };
      setDetail(response);
      setForm({ documentId: response.id, revision: response.documentRevision, values, baseline: values });
    } catch (err) {
      if (!sessionRef.current.failLoad(ticket)) return;
      setError(err instanceof Error ? err.message : "Failed to load document");
    } finally {
      if (sessionRef.current.isCurrent(ticket)) setLoading(false);
    }
  }

  function reloadSelection() {
    if (!selectedId || mutating || loading) return;
    if (dirty && !window.confirm("Discard unsaved changes and reload this document?")) return;
    void loadDetail(selectedId);
  }

  useEffect(() => {
    if (!dirty || mutating) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ""; };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty, mutating]);

  useEffect(() => {
    refreshList().catch((err) => setError(err instanceof Error ? err.message : "Failed to load ingestion list"));
  }, [
    listRefresh,
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
    debouncedQueryText,
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

  async function mutateDocument(action: "save" | "approve" | "reject") {
    if (!form || !detail || !canMutate || detail.id !== form.documentId || detail.documentRevision !== form.revision) return;
    if (action !== "save" && metadataDirty) return;
    if (action === "approve" && !detail.approvalReadiness?.eligible) return;
    if (action === "reject" && form.values.rejectReason.trim().length < 3) return;
    const ticket = sessionRef.current.beginMutation(form);
    if (!ticket) return; // synchronous: a second click cannot beat a React rerender
    const label = `${detail.title} (${form.documentId})`;
    const snapshot = form.values;
    setMutating(true);
    setMutationNotice(`${label}: ${action === "save" ? "saving metadata" : action === "approve" ? "approving" : "rejecting"}...`);
    let committed = false;
    try {
      if (action === "save") await updateIngestionMetadata(ticket.documentId, {
        expectedRevision: ticket.revision, mutationId: crypto.randomUUID(),
        ...reviewerReferencePatch(snapshot, form.baseline), case_number: snapshot.caseNumber || null,
        decision_date: snapshot.decisionDate || null, author_name: snapshot.authorName || null,
        outcome_label: snapshot.outcomeLabel, confirm_required_metadata: snapshot.confirmRequired
      });
      else if (action === "approve") await approveIngestionDocument(ticket.documentId, ticket.revision);
      else await rejectIngestionDocument(ticket.documentId, snapshot.rejectReason.trim(), ticket.revision, crypto.randomUUID());
      committed = true;
      setMutationNotice(`${label}: ${action === "save" ? "metadata saved" : action === "approve" ? "approved" : "rejected"}.`);
      setListRefresh(value => value + 1);
    } catch (err) {
      setMutationNotice(`${label}: ${(err instanceof Error ? err.message : "Update failed").replace(/[.!]+$/, "")}. Review the current document before retrying.`);
    } finally {
      const stillCurrent = sessionRef.current.finishMutation(ticket);
      // The write has settled. Its follow-up read must not keep an unrelated
      // document locked, or later clear that document's own mutation state.
      setMutating(false);
      // Success for A may refresh the list, but can only replace A's original
      // session. A different selection (including A -> B -> A) owns its pane.
      if (committed && stillCurrent) await loadDetail(ticket.documentId);
      else if (!committed && stillCurrent) {
        sessionRef.current.invalidateReview();
        setError("The review update did not return a confirmed result. Reload the selected document before making another change.");
      }
      else if (committed && sessionRef.current.selectedId === ticket.documentId) {
        sessionRef.current.invalidateReview();
        setLoading(false);
        setError("This document changed in an earlier review session. Reload the selected document before making another change.");
      }
    }
  }

  const onSaveMetadata = () => mutateDocument("save");
  const onApprove = () => mutateDocument("approve");
  const onReject = () => mutateDocument("reject");

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Ingestion QC Admin</h1>
      <p style={{ color: "var(--muted)" }}>
        Review extracted metadata, section detection, anchors, and chunk boundaries before approving pilot historical decisions.
      </p>
      {summary ? (
        <p style={{ color: "var(--muted)" }}>
          Queue {summary.total} matching docs · page {documents.length} docs · staged {summary.staged} · searchable {summary.searchable} · approved {summary.approved} · rejected {summary.rejected} · warnings {summary.withWarnings}
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
          {summary.derivedCandidatePoolExhausted ? (
            <>
              {" · "}candidate pool {summary.candidatePoolSize ?? "?"}/{summary.candidatePoolLimit ?? "?"}
              {summary.derivedCandidatePoolLimited ? " (may have more matches)" : ""}
            </>
          ) : null}
        </p>
      ) : null}

      {error ? <p role="alert" style={{ color: "#8b2a2a" }}>{error}</p> : null}
      {listError ? <p role="alert">{listError}</p> : null}
      <div style={{display:"flex",gap:"0.5rem",marginBottom:"0.5rem"}}>
        <button type="button" disabled={listLoading} onClick={()=>void refreshList()}>Restart queue</button>
        <button type="button" disabled={listLoading} onClick={()=>void refreshQueueIndex()}>Refresh queue index (25 documents)</button>
        <button type="button" disabled={listLoading||!previousCursors.length} onClick={()=>void refreshList(previousCursors.at(-1)||undefined,"previous")}>Previous page</button>
        <button type="button" disabled={listLoading||!nextCursor} onClick={()=>void refreshList(nextCursor||undefined,"next")}>Next page</button>
        {listLoading?<span role="status">Loading reviewer queue…</span>:null}
      </div>
      {peerGroup ? <p>Peer IDs for the selected triage group: {peerIds.join(", ")}
        {peerCursor?<button disabled={peerLoading} onClick={()=>void showPeers(peerGroup,peerDocument,peerCursor)}>Next peer page</button>:null}
        <button onClick={()=>{peerRequestRef.current++;setPeerGroup(undefined);setPeerIds([]);setPeerLoading(false);}}>Close peers</button>
      </p>:null}
      {mutationNotice ? <p role="status" style={{ color: "var(--muted)" }}>{mutationNotice}</p> : null}
      {saveState ? <p role="status" style={{ color: "var(--muted)" }}>{saveState}</p> : null}

      <div className="reviewer-panels">
        <section className="card reviewer-panel" aria-label="Reviewer queue" style={{ padding: "0.8rem" }}>
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
          <p style={{fontSize:"0.85rem",color:"var(--muted)"}}>CSV marks formula-like text with [text]. JSON keeps the exact values.</p>
          <div style={{ display: "grid", gap: "0.4rem", marginBottom: "0.7rem" }}>
            <label className="reviewer-field"><span>Review status</span><select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value as typeof statusFilter)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All statuses</option>
              <option value="staged">Staged only</option>
              <option value="searchable">Searchable only</option>
              <option value="approved">Approved only</option>
              <option value="rejected">Rejected only</option>
              <option value="pending">Pending only</option>
            </select></label>
            <label className="reviewer-field"><span>File type</span><select value={fileTypeFilter} onChange={(e) => setFileTypeFilter(e.target.value as typeof fileTypeFilter)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All file types</option>
              <option value="decision_docx">Decision DOCX</option>
              <option value="law_pdf">Law PDF</option>
            </select></label>
            <label className="reviewer-field"><span>Queue sort order</span><select value={sortMode} onChange={(e) => setSortMode(e.target.value as typeof sortMode)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
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
            </select></label>
            <label className="reviewer-field"><span>Search reviewer queue</span><input value={queryText} onChange={(e) => setQueryText(e.target.value)} placeholder="Search title/citation/case #" style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }} /></label>
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
            <label className="reviewer-field"><span>Approval blocker</span><select value={blockerFilter} onChange={(e) => setBlockerFilter(e.target.value)} style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
              <option value="all">All blockers</option>
              <option value="metadata_not_confirmed">Blocker: metadata_not_confirmed</option>
              <option value="qc_gate_not_passed">Blocker: qc_gate_not_passed</option>
              <option value="unresolved_references_above_threshold">Blocker: unresolved_references_above_threshold</option>
              <option value="critical_reference_exception_present">Blocker: critical_reference_exception_present</option>
              <option value="warnings_above_threshold">Blocker: warnings_above_threshold</option>
            </select></label>
            <label className="reviewer-field"><span>Unresolved triage bucket</span><select
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
            </select></label>
            <label className="reviewer-field"><span>Recurring citation family</span><input
              value={recurringCitationFamilyFilter}
              onChange={(e) => setRecurringCitationFamilyFilter(e.target.value)}
              placeholder="Recurring citation family (e.g. 37.2)"
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            /></label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={blocked37xOnly} onChange={(e) => setBlocked37xOnly(e.target.checked)} />
              Blocked 37.x only
            </label>
            <label className="reviewer-field"><span>Blocked 37.x family</span><select
              value={blocked37xFamilyFilter}
              onChange={(e) => setBlocked37xFamilyFilter(e.target.value)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All blocked 37.x families</option>
              <option value="37.3">37.3</option>
              <option value="37.7">37.7</option>
              <option value="37.9">37.9</option>
            </select></label>
            <label className="reviewer-field"><span>Blocked 37.x batch key</span><input
              value={blocked37xBatchKeyFilter}
              onChange={(e) => setBlocked37xBatchKeyFilter(e.target.value)}
              placeholder="Blocked 37.x batch key"
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            /></label>
            <label style={{ display: "flex", gap: "0.45rem", alignItems: "center", fontSize: "0.88rem" }}>
              <input type="checkbox" checked={safeToBatchReviewOnly} onChange={(e) => setSafeToBatchReviewOnly(e.target.checked)} />
              Safe to batch review only
            </label>
            <label className="reviewer-field"><span>Reviewer effort</span><select
              value={estimatedReviewerEffortFilter}
              onChange={(e) => setEstimatedReviewerEffortFilter(e.target.value as typeof estimatedReviewerEffortFilter)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All reviewer effort</option>
              <option value="low">Effort: low</option>
              <option value="medium">Effort: medium</option>
              <option value="high">Effort: high</option>
            </select></label>
            <label className="reviewer-field"><span>Reviewer risk</span><select
              value={reviewerRiskFilter}
              onChange={(e) => setReviewerRiskFilter(e.target.value as typeof reviewerRiskFilter)}
              style={{ padding: "0.45rem", borderRadius: "8px", border: "1px solid var(--border)" }}
            >
              <option value="all">All reviewer risk</option>
              <option value="low">Risk: low</option>
              <option value="medium">Risk: medium</option>
              <option value="high">Risk: high</option>
            </select></label>
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
                    onClick={() => selectDocument(doc.id)}
                    style={{
                      cursor: "pointer",
                      background: selectedId === doc.id ? "#eef6ff" : "#fff",
                      borderBottom: "1px solid var(--border)"
                    }}
                  >
                    <td style={{ padding: "0.45rem" }}>
                      {/* The row's onClick is mouse-only; this button makes opening a document — the
                          primary QC flow — reachable by keyboard (Tab + Enter/Space). */}
                      <button
                        type="button"
                        aria-pressed={selectedId === doc.id}
                        onClick={(event) => { event.stopPropagation(); selectDocument(doc.id); }}
                        style={{
                          background: "none",
                          border: "none",
                          padding: 0,
                          font: "inherit",
                          textAlign: "left",
                          cursor: "pointer",
                          fontWeight: 600,
                          display: "block"
                        }}
                      >
                        {doc.title}
                      </button>
                      <div style={{ color: "var(--muted)" }}>{doc.citation}</div>
                      <button type="button" disabled={peerLoading} onClick={event=>{event.stopPropagation();void showPeers(doc.triageGroupKey,doc.id);}}>{doc.peerCount} peers across all docs</button>
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

        <section className="card reviewer-panel" aria-label="Selected document" style={{ padding: "1rem" }}>
          {!selectedId ? <p>Select a document to review.</p> : null}
          {loading ? <p role="status">Loading detail...</p> : null}

          {selectedId ? <button type="button" disabled={mutating || loading} onClick={reloadSelection}>Reload selected document</button> : null}
          {detail && form && detail.id === selectedId && form.documentId === selectedId && !loading ? (
            <fieldset aria-label="Document review" disabled={!canMutate} style={{ display: "grid", gap: "0.9rem", border: 0, padding: 0, margin: 0, minWidth: 0 }}>
              {dirty ? <p role="status">{metadataDirty ? `Unsaved metadata for ${detail.title}. Save before approval or rejection.` : `Unsaved rejection reason for ${detail.title}.`}</p> : null}
              <div>
                <h2 style={{ marginTop: 0, marginBottom: "0.25rem" }}>{detail.title}</h2>
                <p style={{ margin: 0, color: "var(--muted)" }}>
                  {detail.citation} · {detail.fileType} · Source: <code>{detail.sourceFileRef}</code>
                </p>
                <p style={{ margin: "0.2rem 0 0", color: "var(--muted)" }}>
                  Suggested case type: <strong>{detail.taxonomySuggestion.caseTypeId || "unknown"}</strong> {!detail.taxonomySuggestion.fallback && detail.taxonomySuggestion.confidence !== null ? `(evidence score ${detail.taxonomySuggestion.confidence.toFixed(2)}; uncalibrated)` : ""} {detail.taxonomySuggestion.fallback ? "· fallback used" : ""}
                  {detail.taxonomySuggestion.previousSuggestion?.caseTypeId ? <span> · prior suggestion: {detail.taxonomySuggestion.previousSuggestion.caseTypeId} (retained for comparison)</span> : null}
                </p>
              </div>

              <div>
                <h3 style={{ marginBottom: "0.35rem" }}>Document metadata</h3>
                {Object.keys(detail.reviewerOverrides || {}).length > 0 ? <details style={{marginBottom:"0.7rem"}}>
                  <summary>Preserved metadata and provenance</summary>
                  <p>Reprocessing preserves these values. Legacy values have no reliable edit history and require review.</p>
                  <ul>{Object.entries(detail.reviewerOverrides || {}).map(([field, override]) => <li key={field}>
                    <strong>{field}</strong>: {JSON.stringify(override.value)} ({override.provenance === "reviewer" ? "reviewer edit" : "legacy value preserved"})
                  </li>)}</ul>
                </details> : null}
                <label className="reviewer-field"><span>Index Codes (comma-separated)</span><input value={indexCodes} onChange={(e) => edit("indexCodes", e.target.value)} placeholder="Index Codes (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                <label className="reviewer-field"><span>Rules Sections (comma-separated)</span><input value={rulesSections} onChange={(e) => edit("rulesSections", e.target.value)} placeholder="Rules Sections (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                <label className="reviewer-field"><span>Ordinance Sections (comma-separated)</span><input value={ordinanceSections} onChange={(e) => edit("ordinanceSections", e.target.value)} placeholder="Ordinance Sections (comma-separated)" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                <label className="reviewer-field"><span>Case Number</span><input value={caseNumber} onChange={(e) => edit("caseNumber", e.target.value)} placeholder="Case Number" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                {detail.unverifiedDecisionDate ? <p role="status">The stored date is invalid and is treated as unknown. Review the original source before assigning a date.</p> : null}
                <label className="reviewer-field"><span>Decision date</span><input type="date" value={decisionDate || ""} onChange={(e) => edit("decisionDate", e.target.value)} style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                <label className="reviewer-field"><span>Author / Judge / ALJ</span><input value={authorName} onChange={(e) => edit("authorName", e.target.value)} placeholder="Author / Judge / ALJ" style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }} /></label>
                <label className="reviewer-field"><span>Outcome</span><select value={outcomeLabel} onChange={(e) => edit("outcomeLabel", e.target.value)} style={{ width: "100%", marginBottom: "0.4rem", padding: "0.5rem", border: "1px solid var(--border)", borderRadius: "8px" }}>
                  <option value="unclear">Outcome unclear</option>
                  <option value="grant">Grant</option>
                  <option value="deny">Deny</option>
                  <option value="partial">Partial</option>
                </select></label>
                <label style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginBottom: "0.4rem" }}>
                  <input type="checkbox" checked={confirmRequired} onChange={(e) => edit("confirmRequired", e.target.checked)} />
                  Confirm required metadata for approval gate
                </label>
                <button disabled={!canMutate} onClick={onSaveMetadata} style={{ border: 0, background: "var(--accent)", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
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
                    {diagnosticCopyState ? <p role="status" style={{ margin: "0.35rem 0 0", color: "var(--muted)" }}>{diagnosticCopyState}</p> : null}
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
                <div role="status"><strong>Historical legal applicability: {detail.referenceEditionContext?.status||"unknown"} — review required</strong>
                  <p>Catalog membership validates citation presence. It does not establish that the text governed this decision. Edition: {detail.referenceEditionContext?.edition_id||"unknown"}; date: {detail.referenceEditionContext?.as_of_date||"unknown"} ({detail.referenceEditionContext?.date_basis?.replaceAll("_"," ")||"unassessed"}).</p>
                  <p>{detail.referenceEditionContext?.reason?.replaceAll("_"," ")||"Legacy validation has no edition context."}</p>
                  {detail.referenceEditionBindings?.filter(binding=>binding.referenceType!=="index_code").map(binding=><p key={binding.referenceLinkId}>{binding.canonicalValue}: captured edition metadata {binding.applicabilityStatus} (review required); edition {binding.editionId||"unknown"}; body SHA-256 {binding.editionBodySha256||"unknown"}
                    {binding.editionId&&binding.editionProvisionId&&binding.editionBodySha256?<a href={`${apiBase.replace(/\/$/,"")}/references/${binding.referenceType==="rules_section"?"rule":"ordinance"}/${encodeURIComponent(binding.editionProvisionId)}?${new URLSearchParams({catalog:binding.editionId,sha256:binding.editionBodySha256})}`} target="_blank" rel="noopener noreferrer"> Review captured provision</a>:null}
                  </p>)}
                </div>
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
                <button disabled={!canMutate || metadataDirty || !detail.approvalReadiness?.eligible} onClick={onApprove} style={{ marginRight: "0.6rem", border: 0, background: "#1f6d4f", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
                  Approve for Search
                </button>
                <label className="reviewer-field"><span>Reject reason</span><input value={rejectReason} onChange={(e) => edit("rejectReason", e.target.value)} placeholder="Reject reason" style={{ marginRight: "0.4rem", padding: "0.45rem", border: "1px solid var(--border)", borderRadius: "8px", minWidth: "220px" }} /></label>
                <button disabled={!canMutate || metadataDirty || rejectReason.trim().length < 3} onClick={onReject} style={{ border: 0, background: "#8b2a2a", color: "#fff", padding: "0.55rem 0.8rem", borderRadius: "8px", cursor: "pointer" }}>
                  Reject
                </button>
              </div>
            </fieldset>
          ) : null}
        </section>
      </div>
    </main>
  );
}
