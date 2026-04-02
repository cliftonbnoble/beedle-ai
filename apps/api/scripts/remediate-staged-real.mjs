import fs from "node:fs/promises";
import path from "node:path";
import { buildContentProbe, buildLegalQueryVariants, buildSelfQueryVariants } from "./rollout-probe-utils.mjs";
import { classifyUnresolvedReferenceRisk, isConservativeRemediationCandidate } from "./staged-remediation-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dryRun = process.env.REMEDIATE_DRY_RUN !== "0";
const limit = Number(process.env.REMEDIATE_LIMIT || "20");
const listLimit = Number(process.env.REMEDIATE_LIST_LIMIT || "200");
const reprocessFirst = process.env.REMEDIATE_REPROCESS_FIRST === "1";
const reportName = process.env.REMEDIATE_REPORT_NAME || "staged-real-remediation-report.json";

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

function blockerBreakdown(docs) {
  const counts = new Map();
  for (const doc of docs) {
    for (const blocker of doc.approvalReadiness?.blockers || []) {
      counts.set(blocker, (counts.get(blocker) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .map(([blocker, count]) => ({ blocker, count }))
    .sort((a, b) => b.count - a.count);
}

async function runApprovedSearch(query) {
  return fetchJson("/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, limit: 10, filters: { approvedOnly: true } })
  });
}

function findRank(responseBody, documentId) {
  const index = (responseBody?.results || []).findIndex((item) => item.documentId === documentId);
  return index >= 0 ? index + 1 : null;
}

async function runPromotedValidation(doc, detail) {
  const selfVariants = buildSelfQueryVariants({ title: doc.title, citation: doc.citation || "" });
  const contentProbe = buildContentProbe(detail, selfVariants[0] || doc.id);
  const legalProbe = buildLegalQueryVariants(detail);

  let selfRes = { status: 204, body: { total: 0, results: [] } };
  let selfQueryUsed = selfVariants[0] || doc.id;
  for (const query of selfVariants) {
    const res = await runApprovedSearch(query);
    selfRes = res;
    selfQueryUsed = query;
    if ((res.body?.results || []).some((item) => item.documentId === doc.id)) break;
  }

  const contentRes = contentProbe.query ? await runApprovedSearch(contentProbe.query) : { status: 204, body: { total: 0, results: [] } };

  let legalRes = { status: 204, body: { total: 0, results: [] } };
  let legalQueryUsed = null;
  for (const query of legalProbe.variants || []) {
    const res = await runApprovedSearch(query);
    legalRes = res;
    legalQueryUsed = query;
    if ((res.body?.results || []).some((item) => item.documentId === doc.id)) break;
  }

  return {
    id: doc.id,
    title: doc.title,
    selfQuery: selfQueryUsed,
    selfFound: (selfRes.body?.results || []).some((item) => item.documentId === doc.id),
    selfRank: findRank(selfRes.body, doc.id),
    contentQuery: contentProbe.query,
    contentProbeSkipped: contentProbe.skipped,
    contentProbeSkippedReason: contentProbe.skippedReason,
    contentFound: (contentRes.body?.results || []).some((item) => item.documentId === doc.id),
    contentRank: findRank(contentRes.body, doc.id),
    legalReferenceQuery: legalQueryUsed,
    legalProbeSkipped: legalProbe.skipped,
    legalProbeSkippedReason: legalProbe.skippedReason,
    legalFound: (legalRes.body?.results || []).some((item) => item.documentId === doc.id),
    legalRank: findRank(legalRes.body, doc.id)
  };
}

async function main() {
  const staged = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  if (staged.status !== 200) {
    throw new Error(`failed to list staged real docs: ${staged.status} ${JSON.stringify(staged.body)}`);
  }
  const stagedDocs = staged.body.documents || [];
  const beforeBreakdown = blockerBreakdown(stagedDocs);

  const reviewed = [];
  const candidates = [];
  const promoted = [];
  const skipped = [];

  for (const doc of stagedDocs.slice(0, Math.max(1, limit * 4))) {
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    if (detail.status !== 200) continue;
    const risk = classifyUnresolvedReferenceRisk(detail.body);
    const eligibility = isConservativeRemediationCandidate(doc, detail.body);
    reviewed.push({
      id: doc.id,
      title: doc.title,
      score: doc.approvalReadiness?.score ?? 0,
      blockers: doc.approvalReadiness?.blockers || [],
      unresolvedReferenceCount: doc.unresolvedReferenceCount || 0,
      extractionConfidence: doc.extractionConfidence || 0,
      triageCategory: risk.category,
      triageReasons: risk.reasons,
      eligible: eligibility.eligible,
      ineligibleReason: eligibility.reason
    });
    if (eligibility.eligible && candidates.length < Math.max(1, limit)) {
      candidates.push({ doc, detail: detail.body, risk });
    } else if (!eligibility.eligible) {
      skipped.push({ id: doc.id, title: doc.title, reason: eligibility.reason });
    }
  }

  if (!dryRun) {
    for (const item of candidates) {
      const doc = item.doc;
      if (reprocessFirst) {
        await fetchJson(`/admin/ingestion/documents/${doc.id}/reprocess`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}"
        });
      }
      const latest = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
      if (latest.status !== 200) {
        skipped.push({ id: doc.id, title: doc.title, reason: "detail_fetch_failed_after_reprocess" });
        continue;
      }
      const refs = latest.body.validReferences || {};
      const indexCodes = (refs.indexCodes || []).length > 0 ? refs.indexCodes : latest.body.indexCodes || [];
      const rulesSections = (refs.rulesSections || []).length > 0 ? refs.rulesSections : latest.body.rulesSections || [];
      const ordinanceSections = (refs.ordinanceSections || []).length > 0 ? refs.ordinanceSections : latest.body.ordinanceSections || [];
      const confirmRequired = indexCodes.length > 0 && rulesSections.length > 0 && ordinanceSections.length > 0;
      const meta = await fetchJson(`/admin/ingestion/documents/${doc.id}/metadata`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          index_codes: indexCodes,
          rules_sections: rulesSections,
          ordinance_sections: ordinanceSections,
          case_number: latest.body.caseNumber || null,
          decision_date: latest.body.decisionDate || null,
          author_name: latest.body.authorName || null,
          outcome_label: latest.body.outcomeLabel || "unclear",
          confirm_required_metadata: confirmRequired
        })
      });
      if (meta.status !== 200) {
        skipped.push({ id: doc.id, title: doc.title, reason: "metadata_update_failed" });
        continue;
      }

      const approve = await fetchJson(`/admin/ingestion/documents/${doc.id}/approve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}"
      });
      if (approve.status === 200 && approve.body?.approved) {
        const validated = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
        const searchChecks = validated.status === 200 ? await runPromotedValidation(doc, validated.body) : null;
        promoted.push({ id: doc.id, title: doc.title, triage: item.risk.category, searchChecks });
      } else {
        skipped.push({ id: doc.id, title: doc.title, reason: "approve_failed", status: approve.status });
      }
    }
  }

  const post = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  const afterDocs = post.status === 200 ? post.body.documents || [] : [];
  const afterBreakdown = blockerBreakdown(afterDocs);
  const postApproved = await fetchJson(
    `/admin/ingestion/documents?status=approved&fileType=decision_docx&realOnly=1&sort=createdAtDesc&limit=${Math.max(40, listLimit)}`
  );
  const approvedRealTotal = postApproved.status === 200 ? (postApproved.body.documents || []).length : null;

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dryRun,
    limit,
    reprocessFirst,
    summary: {
      reviewed: reviewed.length,
      conservative_candidates: candidates.length,
      promoted_real_docs: promoted.length,
      skipped: skipped.length,
      approved_real_docs_total: approvedRealTotal
    },
    before_blocker_breakdown: beforeBreakdown,
    after_blocker_breakdown: afterBreakdown,
    reviewed,
    candidates: candidates.map((item) => ({
      id: item.doc.id,
      title: item.doc.title,
      score: item.doc.approvalReadiness?.score ?? 0,
      blockers: item.doc.approvalReadiness?.blockers || [],
      unresolvedReferenceCount: item.doc.unresolvedReferenceCount || 0,
      triageCategory: item.risk.category,
      triageReasons: item.risk.reasons
    })),
    promoted,
    skipped
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

