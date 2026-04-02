function truthy(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function detectRuntimeMode(apiBase) {
  const value = String(apiBase || "");
  if (/127\.0\.0\.1|localhost/i.test(value)) return "local_http_runtime";
  if (/workers\.dev|cloudflare/i.test(value)) return "remote_worker_runtime";
  return "unknown_runtime_mode";
}

async function safeGet(fetchImpl, url) {
  try {
    const response = await fetchImpl(url);
    const text = await response.text();
    let parsed = null;
    let parseSucceeded = true;
    try {
      parsed = JSON.parse(text);
    } catch {
      parseSucceeded = false;
    }
    return {
      httpStatus: response.status,
      fetchSucceeded: true,
      parseSucceeded,
      parsed,
      rawText: String(text || "").slice(0, 200)
    };
  } catch (error) {
    return {
      httpStatus: 0,
      fetchSucceeded: false,
      parseSucceeded: false,
      parsed: null,
      rawText: String(error instanceof Error ? error.message : error).slice(0, 200)
    };
  }
}

async function safePost(fetchImpl, url, payload) {
  try {
    const response = await fetchImpl(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const text = await response.text();
    let parsed = null;
    let parseSucceeded = true;
    try {
      parsed = JSON.parse(text);
    } catch {
      parseSucceeded = false;
    }
    return {
      httpStatus: response.status,
      fetchSucceeded: true,
      parseSucceeded,
      parsed,
      rawText: String(text || "").slice(0, 200)
    };
  } catch (error) {
    return {
      httpStatus: 0,
      fetchSucceeded: false,
      parseSucceeded: false,
      parsed: null,
      rawText: String(error instanceof Error ? error.message : error).slice(0, 200)
    };
  }
}

function responseShapeFromProbeResult(result) {
  const parsed = result.parsed;
  const hasResultsArray = Array.isArray(parsed?.results);
  const topLevelKeys = parsed && typeof parsed === "object" ? Object.keys(parsed).sort((a, b) => a.localeCompare(b)) : [];
  return {
    hasResultsArray,
    topLevelKeys
  };
}

export async function runRuntimePreflight({
  apiBaseUrl,
  fetchImpl = fetch,
  minimalQueryPayload = {
    query: "analysis standard",
    queryType: "keyword",
    limit: 5,
    filters: { approvedOnly: true, fileType: "decision_docx" }
  }
}) {
  const healthUrl = `${apiBaseUrl}/health`;
  const benchmarkUrl = `${apiBaseUrl}/admin/retrieval/debug`;
  const endpointReachabilityPayload = { query: "", queryType: "keyword", limit: 1, filters: {} };

  const [healthProbe, endpointProbe, minimalProbe] = await Promise.all([
    safeGet(fetchImpl, healthUrl),
    safePost(fetchImpl, benchmarkUrl, endpointReachabilityPayload),
    safePost(fetchImpl, benchmarkUrl, minimalQueryPayload)
  ]);

  const baseApiReachable = healthProbe.fetchSucceeded && healthProbe.httpStatus > 0;
  const benchmarkEndpointReachable = endpointProbe.fetchSucceeded && endpointProbe.httpStatus > 0;
  const minimalKnownGoodQueryWorks =
    minimalProbe.fetchSucceeded &&
    minimalProbe.httpStatus > 0 &&
    minimalProbe.parseSucceeded &&
    responseShapeFromProbeResult(minimalProbe).hasResultsArray;

  const preflightPassed = baseApiReachable && benchmarkEndpointReachable && minimalKnownGoodQueryWorks;
  const selectedBaseUrl = String(apiBaseUrl || "");
  const runtimeModeDetected = detectRuntimeMode(selectedBaseUrl);
  const recommendedRunCommand = `API_BASE_URL=${selectedBaseUrl} RETRIEVAL_PREFLIGHT_FAIL_FAST=1 pnpm report:retrieval-r60-goldset-eval`;

  return {
    baseApiReachable,
    healthEndpointStatus: healthProbe.httpStatus,
    benchmarkEndpointReachable,
    minimalKnownGoodQueryWorks,
    selectedBaseUrl,
    runtimeModeDetected,
    recommendedRunCommand,
    preflightPassed,
    probes: [
      {
        probeName: "base_api_health_endpoint",
        requestPayload: null,
        httpStatus: healthProbe.httpStatus,
        fetchSucceeded: healthProbe.fetchSucceeded,
        parseSucceeded: healthProbe.parseSucceeded,
        responseShape: responseShapeFromProbeResult(healthProbe),
        errorTextSnippet: healthProbe.parseSucceeded ? "" : healthProbe.rawText
      },
      {
        probeName: "benchmark_endpoint_reachability",
        requestPayload: endpointReachabilityPayload,
        httpStatus: endpointProbe.httpStatus,
        fetchSucceeded: endpointProbe.fetchSucceeded,
        parseSucceeded: endpointProbe.parseSucceeded,
        responseShape: responseShapeFromProbeResult(endpointProbe),
        errorTextSnippet: endpointProbe.parseSucceeded ? "" : endpointProbe.rawText
      },
      {
        probeName: "minimal_known_good_retrieval_query",
        requestPayload: minimalQueryPayload,
        httpStatus: minimalProbe.httpStatus,
        fetchSucceeded: minimalProbe.fetchSucceeded,
        parseSucceeded: minimalProbe.parseSucceeded,
        responseShape: responseShapeFromProbeResult(minimalProbe),
        errorTextSnippet: minimalProbe.parseSucceeded ? "" : minimalProbe.rawText
      }
    ]
  };
}

export function shouldFailFast() {
  return truthy(process.env.RETRIEVAL_PREFLIGHT_FAIL_FAST);
}

export function assertPreflightOrThrow(preflight, scriptName = "benchmark_script") {
  if (!preflight?.preflightPassed) {
    throw new Error(
      `${scriptName}: preflight failed (baseApiReachable=${Boolean(preflight?.baseApiReachable)}, benchmarkEndpointReachable=${Boolean(preflight?.benchmarkEndpointReachable)}, minimalKnownGoodQueryWorks=${Boolean(preflight?.minimalKnownGoodQueryWorks)}).`
    );
  }
}
