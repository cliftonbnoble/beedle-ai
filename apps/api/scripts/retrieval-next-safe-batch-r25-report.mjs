import fs from "node:fs/promises";
import path from "node:path";
import {
  buildRetrievalBatchExpansionReport,
  formatRetrievalBatchExpansionMarkdown
} from "./retrieval-batch-expansion-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const limit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);

const reportName = process.env.RETRIEVAL_NEXT_SAFE_BATCH_R25_REPORT_NAME || "retrieval-next-safe-batch-r25-report.json";
const markdownName = process.env.RETRIEVAL_NEXT_SAFE_BATCH_R25_MARKDOWN_NAME || "retrieval-next-safe-batch-r25-report.md";
const manifestName = process.env.RETRIEVAL_NEXT_SAFE_BATCH_R25_MANIFEST_NAME || "retrieval-next-safe-batch-r25-manifest.json";

const TARGET_BASELINE = Number(process.env.RETRIEVAL_R25_BASELINE_SCORE || "65.22");
const MIN_ALLOWED_SCORE = Number(process.env.RETRIEVAL_R25_MIN_SCORE || "64.72");
const LOW_SIGNAL_BASELINE = Number(process.env.RETRIEVAL_R25_LOW_SIGNAL_BASELINE || "0.0167");
const SIZE_PRIMARY = Number.parseInt(process.env.RETRIEVAL_R25_BATCH_SIZE_PRIMARY || "5", 10);
const SIZE_FALLBACK = Number.parseInt(process.env.RETRIEVAL_R25_BATCH_SIZE_FALLBACK || "3", 10);

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON response.`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignal(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

async function resolveDocuments() {
  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  return (payload.documents || [])
    .map((doc) => ({ id: doc.id, isLikelyFixture: Boolean(doc.isLikelyFixture) }))
    .filter((doc) => doc.id);
}

async function loadPreviews(documentRows) {
  const previews = [];
  for (const doc of documentRows || []) {
    const detailUrl = `${apiBase}/admin/retrieval/documents/${doc.id}/chunks?includeText=1`;
    const preview = await fetchJson(detailUrl);
    previews.push({ ...preview, isLikelyFixture: doc.isLikelyFixture });
  }
  return previews;
}

function mapPreviewLowSignal(previews) {
  const map = new Map();
  for (const preview of previews || []) {
    const id = String(preview?.document?.documentId || "");
    if (!id) continue;
    const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
    const labels = chunks.map((c) => String(c?.chunkType || c?.sectionLabel || ""));
    const lowSignalLabels = labels.filter((label) => isLowSignal(label));
    const lowSignalShare = chunks.length ? Number((lowSignalLabels.length / chunks.length).toFixed(4)) : 0;
    map.set(id, {
      documentId: id,
      title: String(preview?.document?.title || ""),
      chunkCount: chunks.length,
      lowSignalChunkCount: lowSignalLabels.length,
      lowSignalChunkShare: lowSignalShare,
      lowSignalLabels: unique(lowSignalLabels)
    });
  }
  return map;
}

function applyR25Filtering(corpusAdmissionReport, previewLowSignalMap, trustedDocIds) {
  const trusted = new Set((trustedDocIds || []).map(String));
  const docs = (corpusAdmissionReport?.documents || []).map((row) => ({ ...row }));
  const filteredDocs = [];
  const exclusionReasons = [];

  for (const row of docs) {
    const id = String(row?.documentId || "");
    const reasons = [];
    if (!id) reasons.push("missing_document_id");
    if (trusted.has(id)) reasons.push("already_trusted_excluded");
    if (row?.isLikelyFixture) reasons.push("fixture_excluded");
    if (String(row?.corpusAdmissionStatus || "") !== "hold_for_repair_review") reasons.push("not_hold_status_excluded");
    if (/retrieval messy headings/i.test(String(row?.title || ""))) reasons.push("previous_regression_family_excluded");
    const low = previewLowSignalMap.get(id);
    if (low && Number(low.lowSignalChunkShare || 0) > LOW_SIGNAL_BASELINE) reasons.push("r25_low_signal_share_exceeds_baseline");
    if (low && Number(low.lowSignalChunkCount || 0) > 0) reasons.push("r25_contains_low_signal_structural_chunks");

    const include = reasons.length === 0;
    filteredDocs.push({ ...row, include });
    if (!include) exclusionReasons.push(...reasons);
  }

  return {
    report: { ...(corpusAdmissionReport || {}), documents: filteredDocs },
    exclusionReasonCounts: countBy(exclusionReasons)
  };
}

function buildR25Markdown(report) {
  const baseMd = formatRetrievalBatchExpansionMarkdown(report);
  const lines = [baseMd.trimEnd(), "", "## R25 Regression Drivers"];
  if (!(report.regressionDrivers || []).length) {
    lines.push("- none");
  } else {
    for (const d of report.regressionDrivers) {
      lines.push(`- ${d.documentId} | ${d.title} | lowSignalShare=${d.lowSignalChunkShare} | labels=${d.lowSignalLabels.join(", ") || "<none>"}`);
    }
  }
  lines.push("");
  lines.push("## R25 Strict Gate");
  for (const [k, v] of Object.entries(report.r25Gate || {})) lines.push(`- ${k}: ${typeof v === "object" ? JSON.stringify(v) : v}`);
  return `${lines.join("\n")}\n`;
}

async function runSimulation({
  documents,
  corpusAdmissionReport,
  referenceEnrichmentReport,
  promotionRehearsalReport,
  trustedDocumentIds,
  batchSize
}) {
  return buildRetrievalBatchExpansionReport({
    apiBase,
    documents,
    corpusAdmissionReport,
    referenceEnrichmentReport,
    promotionRehearsalReport,
    trustedDocumentIds,
    batchSize,
    strictSafeMode: true,
    baselineTargetScore: TARGET_BASELINE,
    maxBaselineRegression: Number((TARGET_BASELINE - MIN_ALLOWED_SCORE).toFixed(4)),
    fetchSearchDebug: (payload) =>
      fetchJson(`${apiBase}/admin/retrieval/debug`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      })
  });
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [corpusAdmission, referenceEnrichment, promotionRehearsal, previousR24, previousManifest] = await Promise.all([
    readJson(path.resolve(reportsDir, "retrieval-corpus-admission-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-reference-enrichment-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-promotion-rehearsal-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-next-safe-batch-r24-report.json")),
    readJson(path.resolve(reportsDir, "retrieval-next-safe-batch-r24-manifest.json"))
  ]);

  const trustedDocumentIds = unique(previousManifest?.baselineTrustedDocIds || []);
  if (!trustedDocumentIds.length) throw new Error("No baseline trusted docs from R24 manifest.");

  const documents = await loadPreviews(await resolveDocuments());
  const previewLowSignalMap = mapPreviewLowSignal(documents);

  const r24Proposed = (previousR24?.proposedNextBatch || []).map((row) => String(row.documentId || "")).filter(Boolean);
  const regressionDrivers = r24Proposed
    .map((id) => previewLowSignalMap.get(id))
    .filter((row) => row && row.lowSignalChunkCount > 0)
    .map((row) => ({
      documentId: row.documentId,
      title: row.title,
      lowSignalChunkCount: row.lowSignalChunkCount,
      lowSignalChunkShare: row.lowSignalChunkShare,
      lowSignalLabels: row.lowSignalLabels
    }));

  const filtered = applyR25Filtering(corpusAdmission, previewLowSignalMap, trustedDocumentIds);

  const primary = await runSimulation({
    documents,
    corpusAdmissionReport: filtered.report,
    referenceEnrichmentReport: referenceEnrichment,
    promotionRehearsalReport: promotionRehearsal,
    trustedDocumentIds,
    batchSize: SIZE_PRIMARY
  });

  const selectedSimulation = primary?.summary?.activationRecommendation === "yes"
    ? primary
    : await runSimulation({
        documents,
        corpusAdmissionReport: filtered.report,
        referenceEnrichmentReport: referenceEnrichment,
        promotionRehearsalReport: promotionRehearsal,
        trustedDocumentIds,
        batchSize: SIZE_FALLBACK
      });

  const r25Gate = {
    baselineTargetScore: TARGET_BASELINE,
    minAllowedScore: MIN_ALLOWED_SCORE,
    baselineLowSignalShare: LOW_SIGNAL_BASELINE,
    simulatedAverageQualityScore: Number(selectedSimulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.averageQualityScore || 0),
    simulatedLowSignalShare: Number(selectedSimulation?.concentrationDeltas?.expandedLowSignalStructuralShare || 0),
    passesQualityFloor:
      Number(selectedSimulation?.beforeVsAfterQa?.expanded?.simulatedSummary?.averageQualityScore || 0) >= MIN_ALLOWED_SCORE,
    passesLowSignalShareCeiling:
      Number(selectedSimulation?.concentrationDeltas?.expandedLowSignalStructuralShare || 0) <= LOW_SIGNAL_BASELINE
  };

  if (!selectedSimulation?.summary) {
    throw new Error("R25 simulation did not produce a valid summary payload.");
  }

  const finalRecommendation =
    selectedSimulation.summary.activationRecommendation === "yes" && r25Gate.passesQualityFloor && r25Gate.passesLowSignalShareCeiling
      ? "yes"
      : "no";

  const report = {
    ...selectedSimulation,
    phase: "R25",
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      ...selectedSimulation.summary,
      activationRecommendation: finalRecommendation
    },
    r25Gate,
    regressionDrivers,
    r25Filtering: {
      exclusionReasonCounts: filtered.exclusionReasonCounts
    }
  };

  const reportPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const manifestPath = path.resolve(reportsDir, manifestName);

  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, buildR25Markdown(report)),
    fs.writeFile(
      manifestPath,
      JSON.stringify(
        {
          generatedAt: report.generatedAt,
          phase: "R25",
          readOnly: true,
          baselineTargetScore: TARGET_BASELINE,
          minAllowedScore: MIN_ALLOWED_SCORE,
          baselineTrustedDocIds: report.manifests.baselineTrustedDocIds,
          nextBatchDocIds: report.manifests.nextBatchDocIds,
          expandedTrustedDocIds: report.manifests.expandedTrustedDocIds,
          activationRecommendation: report.summary.activationRecommendation,
          regressionGate: report.regressionGate,
          r25Gate: report.r25Gate,
          concentrationDeltas: report.concentrationDeltas,
          regressionDrivers: report.regressionDrivers
        },
        null,
        2
      )
    )
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval next safe batch R25 JSON report written to ${reportPath}`);
  console.log(`Retrieval next safe batch R25 Markdown report written to ${markdownPath}`);
  console.log(`Retrieval next safe batch R25 manifest written to ${manifestPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
