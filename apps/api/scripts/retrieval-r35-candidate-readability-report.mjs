import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const realOnly = (process.env.RETRIEVAL_REAL_ONLY || "1") !== "0";
const docLimit = Number.parseInt(process.env.RETRIEVAL_DOC_LIMIT || "300", 10);
const lowSignalDominatedThreshold = Number(process.env.RETRIEVAL_R35_LOW_SIGNAL_DOMINATED_THRESHOLD || "0.4");

const outputJsonName =
  process.env.RETRIEVAL_R35_CANDIDATE_READABILITY_REPORT_NAME || "retrieval-r35-candidate-readability-report.json";
const outputMdName =
  process.env.RETRIEVAL_R35_CANDIDATE_READABILITY_MARKDOWN_NAME || "retrieval-r35-candidate-readability-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function normalizeChunkType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignalStructural(value) {
  const t = normalizeChunkType(value);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

async function readJson(fileName) {
  const raw = await fs.readFile(path.resolve(reportsDir, fileName), "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = null;
  }
  return {
    ok: response.ok,
    status: response.status,
    statusText: response.statusText,
    parsed,
    raw
  };
}

async function resolveRealDecisionDocs() {
  const url = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx${realOnly ? "&realOnly=1" : ""}&sort=createdAtDesc&limit=${docLimit}`;
  const payload = await fetchJson(url);
  if (!payload.ok || !payload.parsed) {
    throw new Error(`Failed to load documents list: status=${payload.status}`);
  }
  return (payload.parsed.documents || []).map((row) => ({
    id: String(row.id || ""),
    title: String(row.title || ""),
    isLikelyFixture: Boolean(row.isLikelyFixture)
  })).filter((row) => row.id);
}

function computeTrustedIds({ r27Manifest, r34Activation }) {
  const baseline = unique((r27Manifest?.baselineTrustedDocIds || []).map(String));
  const r34Kept = String(r34Activation?.summary?.keepOrRollbackDecision || "") === "keep_batch_active";
  const r34DocId = String(r34Activation?.docActivatedExact || "").trim();
  return unique([...baseline, ...(r34Kept && r34DocId ? [r34DocId] : [])]);
}

function classifyFailureMode({ include0, include1, debug0, debug1 }) {
  if (!include0.ok) return "chunks_includeText0_failure";
  if (!include1.ok) return "chunks_includeText1_failure";
  if (!include0.parsed) return "chunks_includeText0_non_json";
  if (!include1.parsed) return "chunks_includeText1_non_json";
  if (debug0 && !debug0.ok) return "chunks_debug_includeText0_failure";
  if (debug1 && !debug1.ok) return "chunks_debug_includeText1_failure";
  if (debug0 && !debug0.parsed) return "chunks_debug_includeText0_non_json";
  if (debug1 && !debug1.parsed) return "chunks_debug_includeText1_non_json";
  return "ok";
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R35 Candidate Readability Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Failure Reason Counts");
  for (const row of report.failureReasonCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.failureReasonCounts || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Failing Docs");
  for (const row of report.failingDocs || []) {
    lines.push(`- ${row.documentId} | ${row.failureMode} | ${row.title}`);
  }
  if (!(report.failingDocs || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Shared Root Cause");
  lines.push(`- ${report.sharedRootCause || "none_detected"}`);
  lines.push("");
  lines.push(`- canRerunR35Safely: ${report.summary?.canRerunR35Safely}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r27Manifest, r34Activation, corpusAdmission, allDocs] = await Promise.all([
    readJson("retrieval-r27-next-manifest.json"),
    readJson("retrieval-r34-gate-revision-activation-report.json"),
    readJson("retrieval-corpus-admission-report.json"),
    resolveRealDecisionDocs()
  ]);

  const priorRegressionDocIds = unique([
    ...((await readJson("retrieval-r26-batch-activation-report.json").catch(() => ({ docsActivatedExact: [] })))?.docsActivatedExact || []),
    ...((await readJson("retrieval-r28-batch-activation-report.json").catch(() => ({ docsActivatedExact: [] })))?.docsActivatedExact || [])
  ]);

  const trustedIds = new Set(computeTrustedIds({ r27Manifest, r34Activation }));
  const admissionById = new Map((corpusAdmission?.documents || []).map((row) => [String(row.documentId || ""), row]));
  const docMetaById = new Map((allDocs || []).map((row) => [row.id, row]));

  const candidateDocIds = [];
  for (const [docId, row] of admissionById.entries()) {
    const title = String(row?.title || docMetaById.get(docId)?.title || "");
    if (String(row?.corpusAdmissionStatus || "") !== "hold_for_repair_review") continue;
    if (trustedIds.has(docId)) continue;
    if (Boolean(row?.isLikelyFixture || docMetaById.get(docId)?.isLikelyFixture)) continue;
    if (priorRegressionDocIds.includes(docId)) continue;
    if (/retrieval messy headings/i.test(title)) continue;
    candidateDocIds.push(docId);
  }

  const probeRows = [];
  for (const documentId of unique(candidateDocIds)) {
    const title = String(admissionById.get(documentId)?.title || docMetaById.get(documentId)?.title || "");

    const include0 = await fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=0`);
    const include1 = await fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=1`);

    const shouldDebug = !include0.ok || !include1.ok || !include0.parsed || !include1.parsed;
    const debug0 = shouldDebug
      ? await fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks-debug?includeText=0&maxParagraphRows=20`)
      : null;
    const debug1 = shouldDebug
      ? await fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks-debug?includeText=1&maxParagraphRows=20`)
      : null;

    const failureMode = classifyFailureMode({ include0, include1, debug0, debug1 });

    let lowSignalChunkShare = null;
    if (include0.ok && include0.parsed && Array.isArray(include0.parsed?.chunks)) {
      const chunks = include0.parsed.chunks;
      const lowSignal = chunks.filter((chunk) => isLowSignalStructural(chunk?.chunkType || chunk?.sectionLabel || "")).length;
      lowSignalChunkShare = chunks.length ? Number((lowSignal / chunks.length).toFixed(4)) : 0;
    }

    probeRows.push({
      documentId,
      title,
      lowSignalChunkShare,
      lowSignalDominated: typeof lowSignalChunkShare === "number" ? lowSignalChunkShare > lowSignalDominatedThreshold : false,
      includeText0: {
        ok: include0.ok,
        status: include0.status,
        parseableJson: Boolean(include0.parsed),
        error: include0.parsed?.error || (include0.ok ? "" : include0.raw.slice(0, 300))
      },
      includeText1: {
        ok: include1.ok,
        status: include1.status,
        parseableJson: Boolean(include1.parsed),
        error: include1.parsed?.error || (include1.ok ? "" : include1.raw.slice(0, 300))
      },
      rawDebugIncludeText0: debug0
        ? {
            ok: debug0.ok,
            status: debug0.status,
            parseableJson: Boolean(debug0.parsed),
            sectionParagraphFallbackUsed: Boolean(debug0.parsed?.sectionParagraphFallbackUsed || false),
            error: debug0.parsed?.error || (debug0.ok ? "" : debug0.raw.slice(0, 300))
          }
        : null,
      rawDebugIncludeText1: debug1
        ? {
            ok: debug1.ok,
            status: debug1.status,
            parseableJson: Boolean(debug1.parsed),
            sectionParagraphFallbackUsed: Boolean(debug1.parsed?.sectionParagraphFallbackUsed || false),
            error: debug1.parsed?.error || (debug1.ok ? "" : debug1.raw.slice(0, 300))
          }
        : null,
      failureMode
    });
  }

  const failingDocs = probeRows.filter((row) => row.failureMode !== "ok");
  const failureReasonCounts = sortCountEntries(countBy(failingDocs.map((row) => row.failureMode)));

  let sharedRootCause = "none_detected";
  if (failingDocs.length) {
    const top = failureReasonCounts[0]?.key || "unknown";
    if (/includeText0/.test(top) || /includeText1/.test(top)) {
      sharedRootCause = "shared_chunks_endpoint_readability_failure";
    } else if (/chunks_debug/.test(top)) {
      sharedRootCause = "shared_raw_debug_readability_failure";
    } else {
      sharedRootCause = `mixed_or_unknown_failure:${top}`;
    }
  }

  const previous = await readJson(outputJsonName).catch(() => null);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    summary: {
      candidateDocsConsideredCount: probeRows.length,
      readableDocsCount: probeRows.length - failingDocs.length,
      failingDocsCount: failingDocs.length,
      beforeFailingDocsCount: Number(previous?.summary?.failingDocsCount || 0),
      afterFailingDocsCount: failingDocs.length,
      beforeReadableDocsCount: Number(previous?.summary?.readableDocsCount || 0),
      afterReadableDocsCount: probeRows.length - failingDocs.length,
      canRerunR35Safely: failingDocs.length === 0
    },
    failingDocIds: failingDocs.map((row) => row.documentId),
    failingDocs,
    failureReasonCounts,
    sharedRootCause,
    lowSignalDominatedThreshold,
    candidateDocs: probeRows
  };

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, buildMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`R35 candidate readability report written to ${jsonPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
