import fs from "node:fs/promises";
import path from "node:path";
import { buildContentProbe, buildLegalQueryVariants, buildSelfQueryVariants } from "./rollout-probe-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dryRun = process.env.ROLLOUT_DRY_RUN !== "0";
const promoteLimit = Number(process.env.PROMOTE_LIMIT || "25");
const listLimit = Number(process.env.ROLLOUT_LIST_LIMIT || "600");
const reportName = process.env.ROLLOUT_REPORT_NAME || "pilot-approval-rollout-report.json";
const autoConfirmRequired = process.env.AUTO_CONFIRM_REQUIRED === "1";
const includeFixtures = process.env.ROLLOUT_INCLUDE_FIXTURES === "1";
const approvedRealCheckLimit = Number(process.env.APPROVED_REAL_CHECK_LIMIT || "20");

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

function findRank(responseBody, documentId) {
  const index = (responseBody?.results || []).findIndex((item) => item.documentId === documentId);
  return index >= 0 ? index + 1 : null;
}

async function runApprovedSearch(query) {
  return fetchJson("/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, limit: 10, filters: { approvedOnly: true } })
  });
}

async function runSearch(query) {
  const response = await fetchJson("/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, limit: 8, filters: { approvedOnly: true } })
  });
  return {
    query,
    status: response.status,
    total: response.body?.total ?? null,
    top: (response.body?.results || []).slice(0, 3).map((item) => ({
      title: item.title,
      citation: item.citation,
      citationAnchor: item.citationAnchor
    }))
  };
}

async function runPromotedRealDocSearchChecks(promotedRealDocs) {
  const checks = [];
  for (const doc of promotedRealDocs.slice(0, 8)) {
    const selfQueries = buildSelfQueryVariants({ title: doc.title, citation: doc.citation || "" });
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    const contentProbe = buildContentProbe(detail.body, selfQueries[0] || doc.id);
    const legalProbe = buildLegalQueryVariants(detail.body);

    let selfResponse = { status: 204, body: { total: 0, results: [] } };
    let selfQueryUsed = selfQueries[0] || doc.id;
    for (const query of selfQueries) {
      const res = await runApprovedSearch(query);
      selfResponse = res;
      selfQueryUsed = query;
      if ((res.body?.results || []).some((item) => item.documentId === doc.id)) {
        break;
      }
    }

    const contentResponse = contentProbe.query ? await runApprovedSearch(contentProbe.query) : { status: 204, body: { total: 0, results: [] } };

    let legalResponse = { status: 204, body: { total: 0, results: [] } };
    let legalQueryUsed = null;
    for (const query of legalProbe.variants || []) {
      const res = await runApprovedSearch(query);
      legalResponse = res;
      legalQueryUsed = query;
      if ((res.body?.results || []).some((item) => item.documentId === doc.id)) {
        break;
      }
    }
    checks.push({
      id: doc.id,
      title: doc.title,
      selfQuery: selfQueryUsed,
      selfQueryVariants: selfQueries,
      selfStatus: selfResponse.status,
      selfFound: (selfResponse.body?.results || []).some((item) => item.documentId === doc.id),
      selfRank: findRank(selfResponse.body, doc.id),
      selfTotal: selfResponse.body?.total ?? null,
      contentQuery: contentProbe.query,
      contentProbeSkipped: contentProbe.skipped,
      contentProbeSkippedReason: contentProbe.skippedReason,
      contentStatus: contentResponse.status,
      contentFound: (contentResponse.body?.results || []).some((item) => item.documentId === doc.id),
      contentRank: findRank(contentResponse.body, doc.id),
      contentTotal: contentResponse.body?.total ?? null,
      legalReferenceQuery: legalQueryUsed,
      legalQueryVariants: legalProbe.variants || [],
      legalProbeSkipped: legalProbe.skipped,
      legalProbeSkippedReason: legalProbe.skippedReason,
      legalStatus: legalResponse.status,
      legalFound: (legalResponse.body?.results || []).some((item) => item.documentId === doc.id),
      legalRank: findRank(legalResponse.body, doc.id),
      legalTotal: legalResponse.body?.total ?? null
    });
  }
  return checks;
}

async function runApprovedRealDocSearchChecks(approvedRealDocs) {
  const checks = [];
  for (const doc of approvedRealDocs.slice(0, Math.max(1, approvedRealCheckLimit))) {
    const selfQueries = buildSelfQueryVariants({ title: doc.title, citation: doc.citation || "" });
    const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
    const contentProbe = buildContentProbe(detail.body, selfQueries[0] || doc.id);
    const legalProbe = buildLegalQueryVariants(detail.body);

    let selfResponse = { status: 204, body: { total: 0, results: [] } };
    let selfQueryUsed = selfQueries[0] || doc.id;
    for (const query of selfQueries) {
      const res = await runApprovedSearch(query);
      selfResponse = res;
      selfQueryUsed = query;
      if ((res.body?.results || []).some((item) => item.documentId === doc.id)) {
        break;
      }
    }
    const contentResponse = contentProbe.query ? await runApprovedSearch(contentProbe.query) : { status: 204, body: { total: 0, results: [] } };
    let legalResponse = { status: 204, body: { total: 0, results: [] } };
    let legalQueryUsed = null;
    for (const query of legalProbe.variants || []) {
      const res = await runApprovedSearch(query);
      legalResponse = res;
      legalQueryUsed = query;
      if ((res.body?.results || []).some((item) => item.documentId === doc.id)) {
        break;
      }
    }
    checks.push({
      id: doc.id,
      title: doc.title,
      selfQuery: selfQueryUsed,
      selfQueryVariants: selfQueries,
      selfFound: (selfResponse.body?.results || []).some((item) => item.documentId === doc.id),
      selfRank: findRank(selfResponse.body, doc.id),
      contentQuery: contentProbe.query,
      contentProbeSkipped: contentProbe.skipped,
      contentProbeSkippedReason: contentProbe.skippedReason,
      contentFound: (contentResponse.body?.results || []).some((item) => item.documentId === doc.id),
      contentRank: findRank(contentResponse.body, doc.id),
      legalReferenceQuery: legalQueryUsed,
      legalQueryVariants: legalProbe.variants || [],
      legalProbeSkipped: legalProbe.skipped,
      legalProbeSkippedReason: legalProbe.skippedReason,
      legalFound: (legalResponse.body?.results || []).some((item) => item.documentId === doc.id),
      legalRank: findRank(legalResponse.body, doc.id)
    });
  }
  return checks;
}

async function main() {
  const list = await fetchJson(
    `/admin/ingestion/documents?status=staged&fileType=decision_docx&sort=approvalReadinessDesc&realOnly=${includeFixtures ? "0" : "1"}&limit=${Math.max(1, listLimit)}`
  );
  if (list.status !== 200) {
    throw new Error(`failed listing staged docs: ${list.status} ${JSON.stringify(list.body)}`);
  }

  const staged = list.body.documents || [];
  const stagedReal = staged.filter((doc) => !doc.isLikelyFixture);
  const beforeRealBlockers = blockerBreakdown(stagedReal);
  const eligible = staged.filter((doc) => doc.approvalReadiness?.eligible);
  const eligibleReal = eligible.filter((doc) => !doc.isLikelyFixture);
  const nearReady = staged.filter((doc) => {
    const blockers = doc.approvalReadiness?.blockers || [];
    return blockers.length === 1 && blockers[0] === "metadata_not_confirmed";
  });
  const nearReadyReal = nearReady.filter((doc) => !doc.isLikelyFixture);
  const stagedReasons = new Map();
  for (const doc of staged) {
    for (const reason of doc.approvalReadiness?.blockers || []) {
      stagedReasons.set(reason, (stagedReasons.get(reason) || 0) + 1);
    }
  }

  const baseCandidates = eligibleReal.length > 0 ? eligibleReal : autoConfirmRequired ? nearReadyReal : [];
  const fallbackCandidates = includeFixtures ? (eligible.length > 0 ? eligible : autoConfirmRequired ? nearReady : []) : [];
  const candidates = (baseCandidates.length > 0 ? baseCandidates : fallbackCandidates).slice(0, Math.max(0, promoteLimit));
  const promoted = [];
  const failedPromotions = [];
  const confirmedInRollout = [];
  if (!dryRun) {
    for (const doc of candidates) {
      if ((doc.approvalReadiness?.blockers || [])[0] === "metadata_not_confirmed" && autoConfirmRequired) {
        const detail = await fetchJson(`/admin/ingestion/documents/${doc.id}`);
        if (detail.status === 200) {
          const confirm = await fetchJson(`/admin/ingestion/documents/${doc.id}/metadata`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              index_codes: detail.body.indexCodes || [],
              rules_sections: detail.body.rulesSections || [],
              ordinance_sections: detail.body.ordinanceSections || [],
              case_number: detail.body.caseNumber || null,
              decision_date: detail.body.decisionDate || null,
              author_name: detail.body.authorName || null,
              outcome_label: detail.body.outcomeLabel || "unclear",
              confirm_required_metadata: true
            })
          });
          if (confirm.status === 200) {
            confirmedInRollout.push({ id: doc.id, title: doc.title });
          }
        }
      }
      const approve = await fetchJson(`/admin/ingestion/documents/${doc.id}/approve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}"
      });
      if (approve.status === 200 && approve.body?.approved) {
        promoted.push({ id: doc.id, title: doc.title, isLikelyFixture: Boolean(doc.isLikelyFixture) });
      } else {
        failedPromotions.push({ id: doc.id, title: doc.title, status: approve.status, body: approve.body });
      }
    }
  }

  const postList = await fetchJson(
    `/admin/ingestion/documents?status=all&fileType=decision_docx&sort=approvalReadinessDesc&limit=${Math.max(1, listLimit)}`
  );
  if (postList.status !== 200) {
    throw new Error(`failed listing post rollout docs: ${postList.status} ${JSON.stringify(postList.body)}`);
  }
  const allDocs = postList.body.documents || [];
  const postStagedReal = allDocs.filter((doc) => !doc.isLikelyFixture && !doc.searchableAt);
  const afterRealBlockers = blockerBreakdown(postStagedReal);
  const approvedCount = allDocs.filter((doc) => Boolean(doc.approvedAt)).length;
  const searchableCount = allDocs.filter((doc) => Boolean(doc.searchableAt)).length;
  const approvedRealCount = allDocs.filter((doc) => Boolean(doc.approvedAt) && !doc.isLikelyFixture).length;
  const approvedFixtureCount = allDocs.filter((doc) => Boolean(doc.approvedAt) && doc.isLikelyFixture).length;

  const retrievalChecks = [];
  for (const query of ["variance", "ordinance", "rule", "notice"]) {
    retrievalChecks.push(await runSearch(query));
  }

  const promotedReal = promoted.filter((doc) => !doc.isLikelyFixture);
  const promotedRealSearchChecks = await runPromotedRealDocSearchChecks(promotedReal);
  const promotedRealSelfFindPass = promotedRealSearchChecks.filter((item) => item.selfFound).length;
  const promotedRealContentFindPass = promotedRealSearchChecks.filter((item) => item.contentFound).length;
  const promotedRealLegalFindPass = promotedRealSearchChecks.filter((item) => item.legalFound).length;

  const approvedRealList = await fetchJson(
    `/admin/ingestion/documents?status=approved&fileType=decision_docx&realOnly=1&sort=createdAtDesc&limit=${Math.max(1, approvedRealCheckLimit)}`
  );
  const approvedRealDocs = approvedRealList.status === 200 ? approvedRealList.body.documents || [] : [];
  const approvedRealSearchChecks = await runApprovedRealDocSearchChecks(approvedRealDocs);
  const approvedRealSelfFindPass = approvedRealSearchChecks.filter((item) => item.selfFound).length;
  const approvedRealContentFindPass = approvedRealSearchChecks.filter((item) => item.contentFound).length;
  const approvedRealLegalFindPass = approvedRealSearchChecks.filter((item) => item.legalFound).length;
  const approvedRealSkippedContentQueries = approvedRealSearchChecks.filter((item) => item.contentProbeSkipped).length;
  const approvedRealSkippedLegalQueries = approvedRealSearchChecks.filter((item) => item.legalProbeSkipped).length;

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dryRun,
    autoConfirmRequired,
    includeFixtures,
    thresholds: staged[0]?.approvalReadiness?.thresholds || null,
    summary: {
      staged_considered: staged.length,
      staged_real_considered: stagedReal.length,
      real_candidates_reviewed: baseCandidates.length,
      approval_candidates: eligible.length,
      real_approval_candidates: eligibleReal.length,
      near_ready_candidates: nearReady.length,
      real_near_ready_candidates: nearReadyReal.length,
      promote_limit: promoteLimit,
      promoted: promoted.length,
      promoted_real_docs: promotedReal.length,
      confirmed_in_rollout: confirmedInRollout.length,
      failed_promotions: failedPromotions.length,
      approved_total: approvedCount,
      approved_real_docs_total: approvedRealCount,
      approved_fixture_docs_total: approvedFixtureCount,
      searchable_total: searchableCount,
      promoted_real_doc_self_find_pass: promotedRealSelfFindPass,
      promoted_real_doc_content_find_pass: promotedRealContentFindPass,
      promoted_real_doc_legal_find_pass: promotedRealLegalFindPass,
      approved_real_docs_checked: approvedRealSearchChecks.length,
      approved_real_doc_self_find_pass: approvedRealSelfFindPass,
      approved_real_doc_content_find_pass: approvedRealContentFindPass,
      approved_real_doc_legal_find_pass: approvedRealLegalFindPass,
      approved_real_doc_skipped_low_signal_content_queries: approvedRealSkippedContentQueries,
      approved_real_doc_skipped_unavailable_legal_queries: approvedRealSkippedLegalQueries
    },
    candidate_docs: candidates.map((doc) => ({
      id: doc.id,
      title: doc.title,
      score: doc.approvalReadiness?.score ?? 0,
      blockers: doc.approvalReadiness?.blockers || [],
      cautions: doc.approvalReadiness?.cautions || [],
      warningCount: doc.warningCount,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      extractionConfidence: doc.extractionConfidence
    })),
    promoted_docs: promoted,
    promoted_real_docs: promotedReal,
    failed_promotions: failedPromotions,
    confirmed_in_rollout: confirmedInRollout,
    before_real_blocker_breakdown: beforeRealBlockers,
    after_real_blocker_breakdown: afterRealBlockers,
    remain_staged_reasons: Array.from(stagedReasons.entries())
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count),
    retrieval_checks: retrievalChecks,
    promoted_real_doc_search_checks: promotedRealSearchChecks,
    approved_real_doc_search_checks: approvedRealSearchChecks
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
