export const BENCHMARK_DEFAULT_FILTERS = Object.freeze({
  approvedOnly: true,
  fileType: "decision_docx"
});

export const BENCHMARK_INTENT_TO_QUERY_TYPE = Object.freeze({
  authority_lookup: "rules_ordinance",
  findings: "keyword",
  procedural_history: "keyword",
  issue_holding_disposition: "keyword",
  analysis_reasoning: "keyword",
  comparative_reasoning: "keyword",
  citation_direct: "citation_lookup"
});

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function ensureNumberStatus(response) {
  const status = Number(response?.status);
  if (Number.isFinite(status) && status > 0) return status;
  if (response?.ok === false) return 500;
  return 200;
}

export function buildBenchmarkDebugPayload({
  query,
  intent,
  queryType,
  limit = 10,
  filters = BENCHMARK_DEFAULT_FILTERS
}) {
  return {
    query: String(query || ""),
    queryType: String(queryType || BENCHMARK_INTENT_TO_QUERY_TYPE[String(intent || "")] || "keyword"),
    limit: Number(limit),
    filters: {
      ...BENCHMARK_DEFAULT_FILTERS,
      ...(filters || {})
    }
  };
}

function responseShape(parsed) {
  const topLevelKeys = parsed && typeof parsed === "object" ? Object.keys(parsed).sort((a, b) => a.localeCompare(b)) : [];
  const hasResultsArray = Array.isArray(parsed?.results);
  const first = hasResultsArray && parsed.results.length ? parsed.results[0] : null;
  const firstResultKeys = first && typeof first === "object" ? Object.keys(first).sort((a, b) => a.localeCompare(b)) : [];
  return { topLevelKeys, hasResultsArray, firstResultKeys };
}

export async function callBenchmarkDebug({
  apiBaseUrl,
  payload,
  fetchImpl = fetch
}) {
  const url = `${apiBaseUrl}/admin/retrieval/debug`;
  try {
    const response = await fetchImpl(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const status = ensureNumberStatus(response);
    const rawText = await response.text();
    let parsedBody = null;
    let parseSucceeded = true;
    try {
      parsedBody = JSON.parse(rawText);
    } catch {
      parseSucceeded = false;
    }
    const parsedResults = parseSucceeded && Array.isArray(parsedBody?.results) ? parsedBody.results : [];
    return {
      requestPayload: payload,
      fetchSucceeded: true,
      httpStatus: status,
      responseOk: Boolean(response.ok ?? (status >= 200 && status < 300)),
      parseSucceeded,
      parsedBody,
      parsedResults,
      endpointInputsObserved: {
        query: parseSucceeded ? String(parsedBody?.query || "") : "",
        queryType: parseSucceeded ? String(parsedBody?.queryType || "") : "",
        filters: parseSucceeded && parsedBody?.filters && typeof parsedBody.filters === "object" ? parsedBody.filters : {}
      },
      responseShape: responseShape(parsedBody),
      errorTextSnippet: parseSucceeded ? "" : String(rawText || "").slice(0, 200)
    };
  } catch (error) {
    return {
      requestPayload: payload,
      fetchSucceeded: false,
      httpStatus: 0,
      responseOk: false,
      parseSucceeded: false,
      parsedBody: null,
      parsedResults: [],
      endpointInputsObserved: { query: "", queryType: "", filters: {} },
      responseShape: responseShape(null),
      errorTextSnippet: String(error instanceof Error ? error.message : error).slice(0, 200)
    };
  }
}

export function benchmarkResponseToBody(contract) {
  const body = contract?.parsedBody && typeof contract.parsedBody === "object" ? contract.parsedBody : {};
  const results = Array.isArray(contract?.parsedResults) ? contract.parsedResults : [];
  return {
    ...body,
    query: String(body.query || contract?.requestPayload?.query || ""),
    queryType: String(body.queryType || contract?.requestPayload?.queryType || ""),
    filters: body.filters && typeof body.filters === "object" ? body.filters : contract?.requestPayload?.filters || {},
    results
  };
}

export function normalizeSectionTypeRuntime(value) {
  const s = String(value || "");
  const lower = s.toLowerCase();
  if (lower === "analysis" || lower === "body") return "analysis_reasoning";
  if (lower === "order") return "holding_disposition";
  if (lower === "findings") return "findings";
  return lower.replace(/\s+/g, "_");
}

export function toTrustedRows(parsedResults, trustedSet, limit = 10) {
  return (parsedResults || [])
    .filter((row) => trustedSet.has(String(row?.documentId || "")))
    .slice(0, Number(limit))
    .map((row) => ({
      documentId: String(row?.documentId || ""),
      chunkId: String(row?.chunkId || ""),
      title: String(row?.title || ""),
      sectionType: normalizeSectionTypeRuntime(row?.sectionLabel || row?.chunkType || ""),
      score: Number(row?.diagnostics?.rerankScore ?? row?.score ?? 0),
      sourceLink: String(row?.sourceLink || ""),
      citationAnchor: String(row?.citationAnchor || "")
    }));
}

export function buildScoringInputs({ task, trustedRows, topK = 5 }) {
  const rows = (trustedRows || []).slice(0, Number(topK));
  const expectedDecisionIds = new Set((task?.expectedDecisionIds || []).map(String));
  const expectedSectionTypes = new Set((task?.expectedSectionTypes || []).map((value) => normalizeSectionTypeRuntime(value)));
  const topDecisionIds = unique(rows.map((row) => row.documentId));
  const topSectionTypes = unique(rows.map((row) => row.sectionType));
  const top1Hit = rows.slice(0, 1).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const top3Hit = rows.slice(0, 3).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const top5Hit = rows.slice(0, 5).some((row) => expectedDecisionIds.has(String(row.documentId || "")));
  const sectionTypeHit = rows.slice(0, 5).some((row) => expectedSectionTypes.has(String(row.sectionType || "")));
  let firstExpectedRank = null;
  for (let i = 0; i < rows.length; i += 1) {
    if (expectedDecisionIds.has(String(rows[i]?.documentId || ""))) {
      firstExpectedRank = i + 1;
      break;
    }
  }
  return {
    isEmpty: rows.length === 0,
    topRows: rows,
    topDecisionIds,
    topSectionTypes,
    top1Hit,
    top3Hit,
    top5Hit,
    sectionTypeHit,
    firstExpectedRank
  };
}
