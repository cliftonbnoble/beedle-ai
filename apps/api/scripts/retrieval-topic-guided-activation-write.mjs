import fs from "node:fs/promises";
import path from "node:path";
import { buildBatchActivationArtifacts } from "./retrieval-batch-activation-utils.mjs";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), process.env.RETRIEVAL_REPORTS_DIR || "reports");
const docIds = String(process.env.RETRIEVAL_DOC_IDS || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const topicLabel = String(process.env.RETRIEVAL_TOPIC_LABEL || "topic-guided").trim() || "topic-guided";
const topicTerms = String(process.env.RETRIEVAL_TOPIC_TERMS || "")
  .split(",")
  .map((value) => normalizeWhitespace(value).toLowerCase())
  .filter(Boolean);
const dryRun = ["1", "true", "yes"].includes(String(process.env.RETRIEVAL_DRY_RUN || "0").toLowerCase());
const maxChunksPerDoc = Math.max(1, Number.parseInt(process.env.RETRIEVAL_MAX_CHUNKS_PER_DOC || "8", 10));
const minChars = Math.max(20, Number.parseInt(process.env.RETRIEVAL_MIN_CHARS || "80", 10));
const jsonName = process.env.RETRIEVAL_TOPIC_GUIDED_REPORT_NAME || `retrieval-topic-guided-${topicLabel}.json`;

if (!docIds.length) {
  throw new Error("RETRIEVAL_DOC_IDS is required");
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeLabel(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function countTermHits(text, terms) {
  if (!terms.length) return 0;
  const haystack = normalizeWhitespace(text).toLowerCase();
  let hits = 0;
  for (const term of terms) {
    if (term && haystack.includes(term)) hits += 1;
  }
  return hits;
}

function hasAnyPattern(text, patterns) {
  return patterns.some((pattern) => pattern.test(text));
}

function isSection8UdTopic(terms) {
  const section8Patterns = [/\bsection 8\b/i, /\bhousing choice voucher\b/i, /\bhousing authority\b/i, /\bvoucher\b/i];
  const udPatterns = [/\bunlawful detainer\b/i, /\beviction action\b/i, /\bnotice to quit\b/i, /\bsummons and complaint\b/i];
  const joinedTerms = terms.join(" ");
  return hasAnyPattern(joinedTerms, section8Patterns) && hasAnyPattern(joinedTerms, udPatterns);
}

function hasSection8Signal(text) {
  return hasAnyPattern(text, [/\bsection 8\b/i, /\bhousing choice voucher\b/i, /\bhousing authority\b/i, /\bvoucher\b/i]);
}

function hasUdSignal(text) {
  return hasAnyPattern(text, [/\bunlawful detainer\b/i, /\beviction action\b/i, /\bnotice to quit\b/i, /\bsummons and complaint\b/i]);
}

function isConclusionChunk(chunk) {
  const sectionKey = normalizeLabel(chunk?.sectionCanonicalKey || "");
  const sectionLabel = normalizeLabel(chunk?.sectionLabel || "");
  const headings = Array.isArray(chunk?.headingPath) ? chunk.headingPath.map(normalizeLabel) : [];
  return (
    sectionKey.includes("conclusions of law") ||
    sectionLabel.includes("conclusions of law") ||
    sectionLabel === "conclusion" ||
    headings.includes("conclusions of law") ||
    /conclusions? of law|legal standard|application of law/.test(sectionLabel)
  );
}

function isIndexCodeChunk(chunk) {
  const text = String(chunk?.sourceText || "");
  const sectionLabel = normalizeLabel(chunk?.sectionLabel || "");
  return (
    /\bindex codes?:/i.test(text) ||
    /\brent board rules? and regulations? sections?:/i.test(text) ||
    /\brent ordinance sections?:/i.test(text) ||
    sectionLabel.includes("index code") ||
    sectionLabel.includes("rent ordinance sections")
  );
}

function isFindingsChunk(chunk) {
  const sectionKey = normalizeLabel(chunk?.sectionCanonicalKey || "");
  const sectionLabel = normalizeLabel(chunk?.sectionLabel || "");
  return (
    String(chunk?.chunkType || "") === "findings" ||
    sectionKey.includes("findings of fact") ||
    /findings?( of fact)?/.test(sectionLabel)
  );
}

function hasLegalReferenceSignals(chunk) {
  const text = String(chunk?.sourceText || "");
  return /\b(rule|rules?|ordinance|section|index codes?)\b/i.test(text);
}

function scoreChunk(chunk, terms) {
  const text = normalizeWhitespace(chunk?.sourceText || "");
  const textLength = text.length;
  const conclusion = isConclusionChunk(chunk);
  const indexCode = isIndexCodeChunk(chunk);
  const findings = isFindingsChunk(chunk);
  const legalRef = hasLegalReferenceSignals(chunk);
  const termHits = countTermHits(text, terms);
  const section8UdTopic = isSection8UdTopic(terms);
  const section8Signal = hasSection8Signal(text);
  const udSignal = hasUdSignal(text);
  const hasCanonicalReferenceAlignment = Boolean(chunk?.hasCanonicalReferenceAlignment);

  if (!text) return null;
  if (textLength < minChars && !indexCode) return null;
  if (textLength < 12) return null;

  let score = 0;
  const reasons = [];
  let forcedPriority = String(chunk?.retrievalPriority || "low");

  if (conclusion) {
    score += 300;
    reasons.push("conclusions_of_law");
    forcedPriority = "high";
  }
  if (indexCode) {
    score += 260;
    reasons.push("index_code_block");
    forcedPriority = "high";
  }
  if (findings) {
    score += 180;
    reasons.push("findings_of_fact");
    if (forcedPriority !== "high") forcedPriority = "medium";
  }
  if (termHits > 0) {
    score += termHits * 70;
    reasons.push(`topic_hits:${termHits}`);
  }
  if (hasCanonicalReferenceAlignment) {
    score += 65;
    reasons.push("canonical_reference_alignment");
  }
  if (legalRef) {
    score += 40;
    reasons.push("legal_reference_signal");
  }
  if (String(chunk?.chunkType || "") === "authority_discussion") {
    score += 25;
    reasons.push("authority_discussion");
  }
  if (String(chunk?.chunkType || "") === "analysis_reasoning") {
    score += 15;
    reasons.push("analysis_reasoning");
  }

  if (section8UdTopic && findings && section8Signal) {
    score += 140;
    reasons.push("section8_ud_findings_section8_signal");
    forcedPriority = "high";
  }
  if (section8UdTopic && findings && udSignal) {
    score += 140;
    reasons.push("section8_ud_findings_ud_signal");
    forcedPriority = "high";
  }
  if (section8UdTopic && findings && section8Signal && udSignal) {
    score += 120;
    reasons.push("section8_ud_paired_findings_signal");
  }
  if (section8UdTopic && conclusion && !findings && !indexCode && termHits === 0 && !section8Signal && !udSignal) {
    score -= 190;
    reasons.push("section8_ud_generic_conclusion_penalty");
  }

  if (score <= 0) return null;

  return {
    score,
    reasons,
    forcedPriority,
    textLength,
    chunk: {
      ...chunk,
      retrievalPriority: forcedPriority
    }
  };
}

function selectPreviewChunks(preview, terms) {
  const scored = (preview?.chunks || [])
    .map((chunk) => scoreChunk(chunk, terms))
    .filter(Boolean)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (b.textLength !== a.textLength) return b.textLength - a.textLength;
      return String(a.chunk.chunkId || "").localeCompare(String(b.chunk.chunkId || ""));
    });

  const seen = new Set();
  const selected = [];
  const details = [];
  for (const row of scored) {
    const chunkId = String(row.chunk?.chunkId || "");
    if (!chunkId || seen.has(chunkId)) continue;
    seen.add(chunkId);
    selected.push(row.chunk);
    details.push({
      chunkId,
      score: row.score,
      reasons: row.reasons,
      retrievalPriority: row.forcedPriority,
      chunkType: row.chunk?.chunkType || "general_body",
      sectionLabel: row.chunk?.sectionLabel || "",
      citationAnchorStart: row.chunk?.citationAnchorStart || "",
      textPreview: normalizeWhitespace(row.chunk?.sourceText || "").slice(0, 220)
    });
    if (selected.length >= maxChunksPerDoc) break;
  }

  return { selected, details };
}

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const filteredPreviews = [];
  const selectedDocs = [];

  for (const documentId of docIds) {
    const preview = await fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=1`);
    const { selected, details } = selectPreviewChunks(preview, topicTerms);
    if (!selected.length) continue;
    filteredPreviews.push({ ...preview, chunks: selected });
    selectedDocs.push({
      documentId,
      title: String(preview?.document?.title || "Untitled"),
      citation: String(preview?.document?.citation || ""),
      selectedChunkCount: selected.length,
      selectedChunks: details
    });
  }

  const selectedDocIds = selectedDocs.map((row) => row.documentId);
  const artifacts = buildBatchActivationArtifacts({
    previews: filteredPreviews,
    nextBatchDocIds: selectedDocIds,
    existingTrustedDocIds: [],
    activationManifestSource: jsonName
  });

  let writeReport = null;
  if (!dryRun && selectedDocIds.length) {
    writeReport = await fetchJson(`${apiBase}/admin/retrieval/activation/write`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(artifacts.payload)
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: dryRun,
    apiBase,
    topicLabel,
    topicTerms,
    controls: {
      requestedDocCount: docIds.length,
      selectedDocCount: selectedDocIds.length,
      maxChunksPerDoc,
      minChars,
      dryRun
    },
    summary: {
      selectedDocCount: selectedDocIds.length,
      selectedChunkCount: artifacts.nextBatchChunkIds.length,
      docsMissingPreview: artifacts.docsMissingPreview,
      activationBatchId: artifacts.activationBatchId,
      activatedDocumentCount: Number(writeReport?.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(writeReport?.summary?.activatedChunkCount || 0),
      provenanceFailuresCount: Number(writeReport?.summary?.provenanceFailuresCount || 0)
    },
    documents: selectedDocs,
    payloadSummary: {
      nextBatchDocIds: artifacts.nextBatchDocIds,
      nextBatchChunkIds: artifacts.nextBatchChunkIds,
      trustedAfter: artifacts.trustedAfter,
      embeddingRowCount: Number(artifacts.payload?.embeddingPayload?.rowCount || 0),
      searchRowCount: Number(artifacts.payload?.searchPayload?.rowCount || 0)
    },
    writeReport
  };

  const reportPath = path.resolve(reportsDir, jsonName);
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Topic-guided activation report written to ${reportPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
