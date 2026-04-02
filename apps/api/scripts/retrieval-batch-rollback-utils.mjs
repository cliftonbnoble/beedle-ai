function normalizeLabel(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignalStructuralLabel(label) {
  const t = normalizeLabel(label);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function toRows(report) {
  return Array.isArray(report?.queryResults) ? report.queryResults : [];
}

export function summarizeLowSignalPressure(report) {
  const queryRows = toRows(report);
  const perQuery = [];
  let lowSignalTop10Hits = 0;
  let top10Slots = 0;

  for (const row of queryRows) {
    const top = Array.isArray(row.topResults) ? row.topResults.slice(0, 10) : [];
    const lowSignal = top.filter((item) => isLowSignalStructuralLabel(item.sectionLabel));
    lowSignalTop10Hits += lowSignal.length;
    top10Slots += top.length;
    perQuery.push({
      queryId: row.queryId,
      query: row.query,
      lowSignalTop10Hits: lowSignal.length,
      top10Count: top.length,
      lowSignalTop10Share: top.length ? Number((lowSignal.length / top.length).toFixed(4)) : 0
    });
  }

  return {
    lowSignalTop10Hits,
    top10Slots,
    lowSignalTop10Share: top10Slots ? Number((lowSignalTop10Hits / top10Slots).toFixed(4)) : 0,
    perQuery
  };
}

export function buildStructuralChunkGuardReport({ preRollbackQa, postRollbackQa, rollbackReport }) {
  const pre = summarizeLowSignalPressure(preRollbackQa || {});
  const post = summarizeLowSignalPressure(postRollbackQa || {});

  const guardLogic = [
    "intent_chunk_type_penalty for low-signal structural chunks",
    "lexical_low_signal_chunk_penalty for lexical-only dominance",
    "post-diversify low-signal structural hard cap for non-structural intents"
  ];

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      preRollbackLowSignalTop10Share: pre.lowSignalTop10Share,
      postRollbackLowSignalTop10Share: post.lowSignalTop10Share,
      deltaLowSignalTop10Share: Number((post.lowSignalTop10Share - pre.lowSignalTop10Share).toFixed(4)),
      preRollbackAverageQualityScore: Number(preRollbackQa?.summary?.averageQualityScore || 0),
      postRollbackAverageQualityScore: Number(postRollbackQa?.summary?.averageQualityScore || 0),
      rollbackVerificationPassed: Boolean(rollbackReport?.summary?.rollbackVerificationPassed)
    },
    guardLogic,
    preRollback: pre,
    postRollback: post
  };
}

export function buildRollbackMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Batch Rollback Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Verification");
  for (const [k, v] of Object.entries(report.verification || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Removal Details");
  lines.push(`- docsMissingFromRollbackTarget: ${(report.removalDetails?.docsMissingFromRollbackTarget || []).join(", ") || "<none>"}`);
  lines.push(`- chunksMissingFromRollbackTarget: ${(report.removalDetails?.chunksMissingFromRollbackTarget || []).join(", ") || "<none>"}`);
  lines.push(`- remainingActiveChunkIds: ${(report.removalDetails?.remainingActiveChunkIds || []).slice(0, 20).join(", ") || "<none>"}`);
  return `${lines.join("\n")}\n`;
}

export function buildStructuralGuardMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Structural Chunk Guard Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Guard Logic");
  for (const item of report.guardLogic || []) lines.push(`- ${item}`);
  lines.push("");
  lines.push("## Pre-Rollback Per Query");
  for (const row of report.preRollback?.perQuery || []) {
    lines.push(`- ${row.queryId}: lowSignalTop10Hits=${row.lowSignalTop10Hits}, lowSignalTop10Share=${row.lowSignalTop10Share}`);
  }
  lines.push("");
  lines.push("## Post-Rollback Per Query");
  for (const row of report.postRollback?.perQuery || []) {
    lines.push(`- ${row.queryId}: lowSignalTop10Hits=${row.lowSignalTop10Hits}, lowSignalTop10Share=${row.lowSignalTop10Share}`);
  }
  return `${lines.join("\n")}\n`;
}

