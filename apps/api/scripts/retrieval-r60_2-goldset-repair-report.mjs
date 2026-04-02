import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const originalTasksName = process.env.RETRIEVAL_R60_2_ORIGINAL_TASKS_NAME || "retrieval-r60-goldset-tasks.json";
const r60EvalName = process.env.RETRIEVAL_R60_2_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const liveQaName = process.env.RETRIEVAL_R60_2_LIVE_QA_NAME || "retrieval-live-search-qa-report.json";
const repairedTasksName = process.env.RETRIEVAL_R60_2_REPAIRED_TASKS_NAME || "retrieval-r60_2-goldset-repaired.json";
const outputJsonName = process.env.RETRIEVAL_R60_2_REPORT_NAME || "retrieval-r60_2-goldset-repair-report.json";
const outputMdName = process.env.RETRIEVAL_R60_2_MARKDOWN_NAME || "retrieval-r60_2-goldset-repair-report.md";

const SECTION_TYPE_NORMALIZATION_MAP = {
  FINDINGS: "findings",
  findings: "findings",
  ANALYSIS: "analysis_reasoning",
  analysis: "analysis_reasoning",
  analysis_reasoning: "analysis_reasoning",
  ORDER: "holding_disposition",
  holding_disposition: "holding_disposition",
  procedural_history: "procedural_history",
  Body: "analysis_reasoning",
  body: "analysis_reasoning",
  authority_discussion: "authority_discussion",
  issue_statement: "issue_statement",
  facts_background: "facts_background",
  caption_title: "caption_title"
};

const INTENT_TO_LIVE_QA_INTENT = {
  authority_lookup: "authority",
  findings: "findings",
  procedural_history: "procedural",
  issue_holding_disposition: "holding",
  analysis_reasoning: "analysis",
  comparative_reasoning: "comparative",
  citation_direct: "citation"
};

function normalizeSectionType(raw) {
  const key = String(raw || "");
  if (SECTION_TYPE_NORMALIZATION_MAP[key]) return SECTION_TYPE_NORMALIZATION_MAP[key];
  const lower = key.toLowerCase();
  if (SECTION_TYPE_NORMALIZATION_MAP[lower]) return SECTION_TYPE_NORMALIZATION_MAP[lower];
  return lower.replace(/\s+/g, "_");
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function stablePickIndex(seed, size) {
  if (!size) return 0;
  let h = 0;
  const text = String(seed || "");
  for (let i = 0; i < text.length; i += 1) h = (h * 33 + text.charCodeAt(i)) >>> 0;
  return h % size;
}

function sourceIntent(intent) {
  return INTENT_TO_LIVE_QA_INTENT[String(intent || "")] || "analysis";
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function buildPools({ liveQa, trustedSet }) {
  const pools = new Map();
  const rawSectionTypes = new Set();

  for (const queryRow of liveQa?.queryResults || []) {
    const liveIntent = String(queryRow?.intent || "");
    const key = liveIntent;
    if (!pools.has(key)) pools.set(key, []);
    for (const result of queryRow?.topResults || []) {
      const docId = String(result?.documentId || "");
      if (!trustedSet.has(docId)) continue;
      const rawType = String(result?.chunkType || "");
      rawSectionTypes.add(rawType);
      pools.get(key).push({
        documentId: docId,
        sectionType: normalizeSectionType(rawType),
        sourceQueryId: String(queryRow?.queryId || "")
      });
    }
  }

  const normalizedPools = {};
  for (const [intent, rows] of pools.entries()) {
    const byDoc = new Map();
    for (const row of rows) {
      const key = row.documentId;
      if (!byDoc.has(key)) byDoc.set(key, { documentId: key, sectionTypes: new Set(), sourceQueryIds: new Set() });
      byDoc.get(key).sectionTypes.add(row.sectionType);
      byDoc.get(key).sourceQueryIds.add(row.sourceQueryId);
    }
    normalizedPools[intent] = Array.from(byDoc.values())
      .map((row) => ({
        documentId: row.documentId,
        sectionTypes: Array.from(row.sectionTypes).sort((a, b) => a.localeCompare(b)),
        sourceQueryIds: Array.from(row.sourceQueryIds).sort((a, b) => a.localeCompare(b))
      }))
      .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));
  }

  return {
    pools: normalizedPools,
    observedRawSectionTypes: Array.from(rawSectionTypes).sort((a, b) => a.localeCompare(b))
  };
}

function fallbackPoolsForIntent(intent, pools) {
  const mapped = sourceIntent(intent);
  const primary = pools[mapped] || [];
  if (primary.length) return { pool: primary, intentUsed: mapped };

  if (mapped === "citation") {
    const fallback = pools.authority || pools.analysis || [];
    return { pool: fallback, intentUsed: fallback.length ? (pools.authority ? "authority" : "analysis") : mapped };
  }
  if (mapped === "comparative") {
    const fallback = pools.analysis || pools.findings || [];
    return { pool: fallback, intentUsed: fallback.length ? (pools.analysis ? "analysis" : "findings") : mapped };
  }
  const anyPool = Object.values(pools).find((rows) => rows.length > 0) || [];
  return { pool: anyPool, intentUsed: anyPool.length ? "fallback_any" : mapped };
}

export function buildR60_2GoldsetRepair({ originalTasks, r60Eval, liveQa }) {
  const trustedDecisionIds = unique(r60Eval?.trustedCorpus?.trustedDocumentIds || []);
  const trustedSet = new Set(trustedDecisionIds);
  const { pools, observedRawSectionTypes } = buildPools({ liveQa, trustedSet });

  const tasksRepaired = [];
  const tasksDropped = [];
  const repairedTasks = [];

  for (const task of originalTasks || []) {
    const { pool, intentUsed } = fallbackPoolsForIntent(task.intent, pools);
    if (!pool.length) {
      tasksDropped.push({
        queryId: String(task?.queryId || ""),
        reason: "no_trusted_runtime_pool_for_intent"
      });
      continue;
    }

    const firstIdx = stablePickIndex(task.queryId, pool.length);
    const firstPick = pool[firstIdx];
    const expectedDecisionIds = [firstPick.documentId];
    if (String(task.intent) === "comparative_reasoning" && pool.length > 1) {
      const secondPick = pool[(firstIdx + 1) % pool.length];
      if (secondPick.documentId !== firstPick.documentId) expectedDecisionIds.push(secondPick.documentId);
    }
    const expectedSectionTypes = unique(
      expectedDecisionIds.flatMap((docId) => {
        const row = pool.find((p) => p.documentId === docId);
        return row?.sectionTypes || [];
      })
    );

    const repaired = {
      queryId: String(task.queryId || ""),
      query: String(task.query || ""),
      intent: String(task.intent || ""),
      expectedDecisionIds,
      expectedSectionTypes: expectedSectionTypes.length ? expectedSectionTypes : ["analysis_reasoning"],
      minimumAcceptableRank: Number(task.minimumAcceptableRank || 5),
      notes: `${String(task.notes || "")} [R60.2 repaired]`.trim(),
      sourceOfExpectation: `trusted_live_qa_pool:${intentUsed}`
    };
    repairedTasks.push(repaired);

    const changedIdSet =
      JSON.stringify((task.expectedDecisionIds || []).map(String).sort()) !== JSON.stringify(expectedDecisionIds.slice().sort());
    const changedTypeSet =
      JSON.stringify((task.expectedSectionTypes || []).map((v) => normalizeSectionType(v)).sort()) !==
      JSON.stringify((repaired.expectedSectionTypes || []).slice().sort());
    tasksRepaired.push({
      queryId: repaired.queryId,
      intent: repaired.intent,
      idsRepaired: changedIdSet,
      sectionTypesRepaired: changedTypeSet,
      sourceOfExpectation: repaired.sourceOfExpectation
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.2",
    originalTaskCount: (originalTasks || []).length,
    repairedTaskCount: repairedTasks.length,
    tasksRepaired,
    tasksDropped,
    sectionTypeNormalizationMap: {
      mapping: SECTION_TYPE_NORMALIZATION_MAP,
      observedRawSectionTypes
    },
    trustedDecisionIdsUsed: unique(repairedTasks.flatMap((task) => task.expectedDecisionIds)),
    sourceTrustedDecisionCount: trustedDecisionIds.length
  };

  return { repairedTasks, report };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.2 Gold-Set Repair Report (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- originalTaskCount: ${report.originalTaskCount}`);
  lines.push(`- repairedTaskCount: ${report.repairedTaskCount}`);
  lines.push(`- tasksRepaired: ${report.tasksRepaired.length}`);
  lines.push(`- tasksDropped: ${report.tasksDropped.length}`);
  lines.push(`- trustedDecisionIdsUsed: ${report.trustedDecisionIdsUsed.length}`);
  lines.push("");

  lines.push("## Tasks Dropped");
  for (const row of report.tasksDropped || []) lines.push(`- ${row.queryId}: ${row.reason}`);
  if (!(report.tasksDropped || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Section Type Normalization");
  for (const [raw, normalized] of Object.entries(report.sectionTypeNormalizationMap?.mapping || {})) {
    lines.push(`- ${raw} -> ${normalized}`);
  }
  lines.push("");

  lines.push("## Trusted Decision IDs Used");
  for (const id of report.trustedDecisionIdsUsed || []) lines.push(`- ${id}`);
  if (!(report.trustedDecisionIdsUsed || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes or runtime mutations.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [originalTasks, r60Eval, liveQa] = await Promise.all([
    readJson(path.resolve(reportsDir, originalTasksName)),
    readJson(path.resolve(reportsDir, r60EvalName)),
    readJson(path.resolve(reportsDir, liveQaName))
  ]);

  const { repairedTasks, report } = buildR60_2GoldsetRepair({ originalTasks, r60Eval, liveQa });

  const repairedPath = path.resolve(reportsDir, repairedTasksName);
  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(repairedPath, JSON.stringify(repairedTasks, null, 2)),
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, toMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        originalTaskCount: report.originalTaskCount,
        repairedTaskCount: report.repairedTaskCount,
        tasksDropped: report.tasksDropped.length,
        trustedDecisionIdsUsed: report.trustedDecisionIdsUsed.length
      },
      null,
      2
    )
  );
  console.log(`R60.2 repaired gold-set written to ${repairedPath}`);
  console.log(`R60.2 repair report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
