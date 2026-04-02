import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const targetDocumentId = process.env.RETRIEVAL_R35_DEBUG_DOCUMENT_ID || "doc_15040c83-53f2-490b-b2dc-199ff128ac90";
const outputJsonName = process.env.RETRIEVAL_R35_DOC_DEBUG_REPORT_NAME || "retrieval-r35-doc-debug-report.json";
const outputMdName = process.env.RETRIEVAL_R35_DOC_DEBUG_MARKDOWN_NAME || "retrieval-r35-doc-debug-report.md";

async function fetchCapture(url) {
  const startedAt = new Date().toISOString();
  let response;
  let raw = "";
  let parsed = null;
  try {
    response = await fetch(url);
    raw = await response.text();
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = null;
    }
  } catch (error) {
    return {
      url,
      startedAt,
      ok: false,
      status: 0,
      statusText: "fetch_error",
      parseableJson: false,
      error: error instanceof Error ? error.message : String(error),
      bodyPreview: ""
    };
  }

  return {
    url,
    startedAt,
    ok: response.ok,
    status: response.status,
    statusText: response.statusText,
    parseableJson: parsed !== null,
    error: parsed?.error || (!response.ok ? raw.slice(0, 500) : ""),
    bodyPreview: raw.slice(0, 500),
    body: parsed
  };
}

function summarizeEndpoint(row) {
  const body = row?.body || {};
  return {
    ok: Boolean(row?.ok),
    status: Number(row?.status || 0),
    parseableJson: Boolean(row?.parseableJson),
    error: String(row?.error || ""),
    chunkCount: Number(body?.stats?.chunkCount || 0),
    paragraphCount: Number(body?.stats?.paragraphCount || body?.sectionParagraphCount || 0),
    sectionCount: Number(body?.stats?.sectionCount || 0)
  };
}

function classifyRootCause({ chunks0, chunks1, raw0, raw1 }) {
  const c0 = summarizeEndpoint(chunks0);
  const c1 = summarizeEndpoint(chunks1);
  const r0 = summarizeEndpoint(raw0);
  const r1 = summarizeEndpoint(raw1);

  if (c0.ok && !c1.ok && r0.ok && !r1.ok) {
    return {
      rootCause: "include_text_specific_db_or_serialization_failure",
      detail:
        "includeText=0 succeeds while includeText=1 fails in both retrieval and raw-debug paths; text-bearing paragraph rows likely trigger DB/serialization parse failure."
    };
  }

  if (!c0.ok && !c1.ok && !r0.ok && !r1.ok) {
    return {
      rootCause: "document_or_route_level_failure",
      detail: "Both includeText modes fail across retrieval and raw-debug endpoints."
    };
  }

  if (c0.ok && c1.ok && r0.ok && r1.ok) {
    return {
      rootCause: "no_current_repro",
      detail:
        "Could not reproduce the previous failure now. Endpoint is healthy for includeText=0/1 and raw DB row debug endpoints for this document."
    };
  }

  if (c0.ok && c1.ok && r0.ok && !r1.ok) {
    return {
      rootCause: "raw_debug_include_text_only_failure",
      detail: "Main chunks endpoint succeeds, but raw-debug includeText=1 fails while raw-debug includeText=0 succeeds."
    };
  }

  return {
    rootCause: "mixed_failure_pattern",
    detail: "Observed a mixed failure pattern; inspect per-endpoint captures for exact failing stage."
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R35 Single-Doc Debug Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Root Cause");
  lines.push(`- rootCause: ${report.rootCauseFindings?.rootCause || "unknown"}`);
  lines.push(`- detail: ${report.rootCauseFindings?.detail || ""}`);
  lines.push("");
  lines.push("## Endpoint Comparison");
  for (const row of report.endpointComparisons || []) {
    lines.push(`- ${row.label}`);
    lines.push(`  - status: ${row.status}`);
    lines.push(`  - ok: ${row.ok}`);
    lines.push(`  - parseableJson: ${row.parseableJson}`);
    lines.push(`  - error: ${row.error || "<none>"}`);
    lines.push(`  - chunkCount: ${row.chunkCount}`);
  }
  lines.push("");
  lines.push("## R35 Rerun Safety");
  lines.push(`- canRerunR35Safely: ${report.summary?.canRerunR35Safely}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const chunksInclude0 = await fetchCapture(`${apiBase}/admin/retrieval/documents/${targetDocumentId}/chunks?includeText=0`);
  const chunksInclude1 = await fetchCapture(`${apiBase}/admin/retrieval/documents/${targetDocumentId}/chunks?includeText=1`);
  const rawInclude0 = await fetchCapture(
    `${apiBase}/admin/retrieval/documents/${targetDocumentId}/chunks-debug?includeText=0&maxParagraphRows=20`
  );
  const rawInclude1 = await fetchCapture(
    `${apiBase}/admin/retrieval/documents/${targetDocumentId}/chunks-debug?includeText=1&maxParagraphRows=20`
  );

  const rootCauseFindings = classifyRootCause({
    chunks0: chunksInclude0,
    chunks1: chunksInclude1,
    raw0: rawInclude0,
    raw1: rawInclude1
  });

  const endpointComparisons = [
    { label: "chunks includeText=0", ...summarizeEndpoint(chunksInclude0) },
    { label: "chunks includeText=1", ...summarizeEndpoint(chunksInclude1) },
    { label: "chunks-debug includeText=0", ...summarizeEndpoint(rawInclude0) },
    { label: "chunks-debug includeText=1", ...summarizeEndpoint(rawInclude1) }
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    documentId: targetDocumentId,
    summary: {
      chunksIncludeText0Ok: endpointComparisons[0].ok,
      chunksIncludeText1Ok: endpointComparisons[1].ok,
      rawIncludeText0Ok: endpointComparisons[2].ok,
      rawIncludeText1Ok: endpointComparisons[3].ok,
      canRerunR35Safely:
        endpointComparisons[0].ok && endpointComparisons[1].ok && endpointComparisons[2].ok && endpointComparisons[3].ok
    },
    rootCauseFindings,
    endpointComparisons,
    rawDbDebug: {
      includeText0: rawInclude0.body || null,
      includeText1: rawInclude1.body || null
    },
    captures: {
      chunksIncludeText0: chunksInclude0,
      chunksIncludeText1: chunksInclude1,
      chunksDebugIncludeText0: rawInclude0,
      chunksDebugIncludeText1: rawInclude1
    }
  };

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, buildMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`R35 doc debug report written to ${jsonPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
