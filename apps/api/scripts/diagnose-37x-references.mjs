import fs from "node:fs/promises";
import path from "node:path";
import { classify37xDiagnostic, extract37xKey } from "./citation-37x-diagnostics-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const listLimit = Number(process.env.DIAGNOSE_37X_LIST_LIMIT || "300");
const topLimit = Number(process.env.DIAGNOSE_37X_TOP_LIMIT || "25");
const reportName = process.env.DIAGNOSE_37X_REPORT_NAME || "staged-real-37x-diagnostics-report.json";
const experimentalAliasMode = process.env.EXPERIMENTAL_37X_ALIAS_MODE === "1";

async function fetchJson(endpoint, init) {
  const response = await fetch(`${apiBase}${endpoint}`, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  return { status: response.status, body };
}

async function verifyCitation(citation) {
  const response = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: [citation] })
  });
  if (response.status !== 200) return null;
  return response.body?.checks?.[0] || null;
}

function toSortedCounts(map, keyName = "key") {
  return Array.from(map.entries())
    .map(([key, count]) => ({ [keyName]: key, count }))
    .sort((a, b) => b.count - a.count);
}

async function main() {
  const list = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  if (list.status !== 200) {
    throw new Error(`failed to list staged real docs: ${list.status} ${JSON.stringify(list.body)}`);
  }

  const docs = list.body.documents || [];
  const topDocs = docs.slice(0, Math.max(1, topLimit));
  const byCitation = new Map();
  const byRootDoc = [];

  for (const doc of topDocs) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;
    const issues = detail.body?.referenceIssues || [];
    const local37x = [];
    for (const issue of issues) {
      const key = extract37xKey(issue);
      if (!key) continue;
      const entry = byCitation.get(key.base) || {
        citation: key.base,
        occurrences: 0,
        ordinance_referenceType_count: 0,
        rules_referenceType_count: 0,
        samples: []
      };
      entry.occurrences += 1;
      if (String(issue.referenceType) === "ordinance_section") entry.ordinance_referenceType_count += 1;
      if (String(issue.referenceType) === "rules_section") entry.rules_referenceType_count += 1;
      if (entry.samples.length < 8) {
        entry.samples.push({
          documentId: doc.id,
          title: doc.title,
          referenceType: issue.referenceType,
          rawValue: issue.rawValue,
          normalizedValue: issue.normalizedValue,
          message: issue.message
        });
      }
      byCitation.set(key.base, entry);
      local37x.push({
        referenceType: issue.referenceType,
        rawValue: issue.rawValue,
        normalizedValue: issue.normalizedValue,
        message: issue.message,
        base: key.base
      });
    }
    byRootDoc.push({
      id: doc.id,
      title: doc.title,
      score: doc.approvalReadiness?.score ?? 0,
      blockers: doc.approvalReadiness?.blockers || [],
      unresolvedReferenceCount: detail.body?.unresolvedReferenceCount || 0,
      refs37x: local37x
    });
  }

  const citations = Array.from(byCitation.keys()).sort();
  const evaluated = [];
  const classificationCounts = new Map();
  for (const citation of citations) {
    const aggregate = byCitation.get(citation);
    const verify = await verifyCitation(citation);
    const evaluatedClassification = classify37xDiagnostic({
      verifyCheck: verify,
      ordinanceIssueCount: aggregate.ordinance_referenceType_count,
      rulesIssueCount: aggregate.rules_referenceType_count,
      experimentalAliasMode
    });
    classificationCounts.set(
      evaluatedClassification.classification,
      (classificationCounts.get(evaluatedClassification.classification) || 0) + 1
    );
    evaluated.push({
      citation,
      occurrences: aggregate.occurrences,
      ordinance_referenceType_count: aggregate.ordinance_referenceType_count,
      rules_referenceType_count: aggregate.rules_referenceType_count,
      verify: verify
        ? {
            status: verify.status,
            diagnostic: verify.diagnostic,
            ordinance_matches: verify.ordinance_matches || [],
            rules_matches: verify.rules_matches || []
          }
        : null,
      classification: evaluatedClassification.classification,
      rationale: evaluatedClassification.rationale,
      experimental_alias_mode: experimentalAliasMode,
      experimental_would_resolve: evaluatedClassification.experimentalWouldResolve,
      nearest_candidates: {
        ordinance: (verify?.ordinance_matches || []).slice(0, 6),
        rules: (verify?.rules_matches || []).slice(0, 6)
      },
      samples: aggregate.samples
    });
  }

  const unresolved37xByType = new Map();
  for (const row of evaluated) {
    unresolved37xByType.set("ordinance", (unresolved37xByType.get("ordinance") || 0) + row.ordinance_referenceType_count);
    unresolved37xByType.set("rules", (unresolved37xByType.get("rules") || 0) + row.rules_referenceType_count);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    topLimit,
    experimental_alias_mode: experimentalAliasMode,
    summary: {
      staged_real_docs_scanned: topDocs.length,
      recurring_37x_citations: evaluated.length
    },
    aggregate: {
      unresolved_37x_by_reference_type: toSortedCounts(unresolved37xByType, "reference_type"),
      classification_counts: toSortedCounts(classificationCounts, "classification")
    },
    recurring_37x: evaluated,
    docs: byRootDoc
  };

  const outputPath = path.resolve(process.cwd(), "reports", reportName);
  await fs.writeFile(outputPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});

