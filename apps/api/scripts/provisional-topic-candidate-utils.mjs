const REAL_DOC_TITLE_PATTERNS = [/^Harness\b/i, /^Retrieval\b/i, /^R\d+\b/i, /^Demo\b/i];
const REAL_DOC_CITATION_PATTERNS = [/^BEE-/i];
const FIXTURE_SOURCE_KEY_PATTERNS = [
  /(^|\/)[^/]*retrieval-/i,
  /(^|\/)[^/]*r\d+-/i,
  /(^|\/)[^/]*harness-/i,
  /(^|\/)[^/]*demo-/i,
  /decision_(?:fail|missing|pass)\.docx$/i,
  /\/decision-(?:fail|missing)\.docx$/i
];

export const TOPIC_FAMILIES = {
  cooling: ["cooling", "cooled", "air conditioning", "air-conditioning", "ventilation", "ventilated", "ventilate", "air flow", "airflow", "air circulation", "circulation", "fan", "exhaust"],
  ventilation: ["ventilation", "ventilated", "ventilate", "air flow", "airflow", "air circulation", "circulation", "exhaust", "fresh air", "fan"],
  mold: ["mold", "mould", "mildew", "fungus", "fungal", "damp", "dampness", "moisture", "water intrusion", "water damage"]
};

export function isLikelyFixtureDoc(row) {
  const title = String(row?.title || "");
  const citation = String(row?.citation || "");
  return REAL_DOC_TITLE_PATTERNS.some((pattern) => pattern.test(title)) || REAL_DOC_CITATION_PATTERNS.some((pattern) => pattern.test(citation));
}

export function isLikelyFixtureSourceKey(key) {
  const value = String(key || "");
  return FIXTURE_SOURCE_KEY_PATTERNS.some((pattern) => pattern.test(value));
}

export function normalizeTopicScore(rawValue) {
  const value = Number(rawValue || 0);
  return Number.isFinite(value) ? value : 0;
}

export function computeTopicSignals(row) {
  const topicSignals = {};
  for (const [topic, terms] of Object.entries(TOPIC_FAMILIES)) {
    const directHits = normalizeTopicScore(row[`${topic}DirectHits`]);
    const synonymHits = normalizeTopicScore(row[`${topic}SynonymHits`]);
    const totalHits = directHits + synonymHits;
    topicSignals[topic] = {
      directHits,
      synonymHits,
      totalHits,
      matchedTerms: terms.filter((term, index) => {
        const key = `${topic}_${index}_${term.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase()}_hit`;
        return normalizeTopicScore(row[key]) > 0;
      })
    };
  }
  return topicSignals;
}

export function computeWorthReprocessing(row) {
  const chunkCount = normalizeTopicScore(row.chunkCount);
  const xmlChunkCount = normalizeTopicScore(row.xmlChunkCount);
  const tinyChunkCount = normalizeTopicScore(row.tinyChunkCount);
  const lowValueSectionCount = normalizeTopicScore(row.lowValueSectionCount);
  const usefulSectionCount = normalizeTopicScore(row.usefulSectionCount);
  const unresolvedReferenceCount = normalizeTopicScore(row.unresolvedReferenceCount);
  const unsafe37xReferenceCount = normalizeTopicScore(row.unsafe37xReferenceCount);
  const extractionConfidence = Number(row.extractionConfidence || 0);
  const r2ObjectPresent = Boolean(row.r2ObjectPresent);
  const topicSignals = computeTopicSignals(row);
  const strongestTopic = Object.entries(topicSignals).sort((a, b) => b[1].totalHits - a[1].totalHits)[0] || ["none", { totalHits: 0 }];
  const topicHitCount = strongestTopic[1].totalHits;

  const xmlRatio = chunkCount > 0 ? xmlChunkCount / chunkCount : 1;
  const tinyRatio = chunkCount > 0 ? tinyChunkCount / chunkCount : 1;
  const lowValueRatio = chunkCount > 0 ? lowValueSectionCount / chunkCount : 1;

  let score = 0;
  if (topicHitCount > 0) score += Math.min(45, topicHitCount * 8);
  if (strongestTopic[1].directHits > 0) score += 12;
  if (usefulSectionCount > 0) score += Math.min(15, usefulSectionCount);
  if (r2ObjectPresent) score += 8;
  score += Math.max(0, Math.min(15, Math.round(extractionConfidence * 15)));
  score -= Math.round(xmlRatio * 45);
  score -= Math.round(tinyRatio * 18);
  score -= Math.round(lowValueRatio * 12);
  score -= Math.min(15, unresolvedReferenceCount * 2);
  score -= Math.min(12, unsafe37xReferenceCount * 4);
  if (chunkCount === 0) score -= 20;
  if (chunkCount > 140) score -= 8;

  const reasons = [];
  const blockers = [];
  if (!r2ObjectPresent) blockers.push("missing_r2_object");
  if (topicHitCount === 0) blockers.push("no_topic_signal");
  if (xmlRatio > 0.45) blockers.push("xml_artifact_ratio_too_high");
  if (tinyRatio > 0.35) blockers.push("tiny_chunk_ratio_too_high");
  if (extractionConfidence < 0.5) blockers.push("low_extraction_confidence");
  if (chunkCount < 3) blockers.push("too_few_chunks");
  if (usefulSectionCount === 0) blockers.push("no_useful_sections");
  if (blockers.length === 0 && score < 35) blockers.push("score_below_threshold");

  if (topicHitCount > 0) reasons.push(`topic_signal:${strongestTopic[0]}:${topicHitCount}`);
  if (strongestTopic[1].directHits > 0) reasons.push("direct_topic_hits_present");
  if (usefulSectionCount > 0) reasons.push(`useful_sections:${usefulSectionCount}`);
  if (xmlRatio > 0) reasons.push(`xml_ratio:${xmlRatio.toFixed(2)}`);
  if (tinyRatio > 0) reasons.push(`tiny_ratio:${tinyRatio.toFixed(2)}`);
  if (unresolvedReferenceCount > 0) reasons.push(`unresolved_refs:${unresolvedReferenceCount}`);

  return {
    score,
    strongestTopic: strongestTopic[0],
    strongestTopicHitCount: topicHitCount,
    xmlRatio: Number(xmlRatio.toFixed(3)),
    tinyRatio: Number(tinyRatio.toFixed(3)),
    lowValueRatio: Number(lowValueRatio.toFixed(3)),
    worthReprocessing: blockers.length === 0 && score >= 35,
    reasons,
    blockers
  };
}

export function formatCandidateMarkdown(report) {
  const lines = [
    "# Provisional Topic Candidate Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Real staged decision docs reviewed: \`${report.summary.realStagedDecisionCount}\``,
    `- Topic-likely docs: \`${report.summary.topicLikelyCount}\``,
    `- Worth reprocessing now: \`${report.summary.worthReprocessingCount}\``,
    ""
  ];

  lines.push("## Heuristic");
  lines.push("");
  lines.push("- Require a real staged `decision_docx` with an R2 object.");
  lines.push("- Prefer documents with direct chunk-text hits for `cooling`, `ventilation`, or `mold` families.");
  lines.push("- Penalize Word XML artifact ratio, tiny-chunk ratio, low-value section dominance, and unresolved reference load.");
  lines.push("- Mark `worthReprocessing` only when topic signal exists and structure is clean enough to plausibly become searchable.");
  lines.push("");

  for (const [topic, rows] of Object.entries(report.topicBuckets)) {
    lines.push(`## ${topic}`);
    lines.push("");
    if (!rows.length) {
      lines.push("- No real staged candidates found.");
      lines.push("");
      continue;
    }
    for (const row of rows) {
      lines.push(
        `- \`${row.id}\` | ${row.title} | score=${row.heuristic.score} | worthReprocessing=${row.heuristic.worthReprocessing} | direct=${row.topic.directHits} | synonym=${row.topic.synonymHits} | xmlRatio=${row.heuristic.xmlRatio} | blockers=${row.heuristic.blockers.join(", ") || "<none>"}`
      );
    }
    lines.push("");
  }

  lines.push("## Recommended Next Batch");
  lines.push("");
  if (!report.recommendedBatch.length) {
    lines.push("- No candidates met the `worthReprocessing` threshold.");
  } else {
    report.recommendedBatch.forEach((row, index) => {
      lines.push(
        `${index + 1}. \`${row.id}\` - \`${row.title}\` (${row.heuristic.strongestTopic}, score=${row.heuristic.score}, xmlRatio=${row.heuristic.xmlRatio})`
      );
    });
  }
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export function scoreSourceImportCandidate(row) {
  const key = String(row?.key || "");
  const lower = key.toLowerCase();
  const topicHits = Object.fromEntries(
    Object.entries(TOPIC_FAMILIES).map(([topic, terms]) => [topic, terms.filter((term) => lower.includes(term.replace(/\s+/g, "-")) || lower.includes(term.replace(/\s+/g, "_")) || lower.includes(term)).length])
  );
  const strongest = Object.entries(topicHits).sort((a, b) => b[1] - a[1])[0] || ["none", 0];
  const score = strongest[1] * 20 - (isLikelyFixtureSourceKey(key) ? 50 : 0);
  return {
    strongestTopic: strongest[0],
    strongestTopicHitCount: strongest[1],
    score,
    likelyFixture: isLikelyFixtureSourceKey(key),
    worthImporting: !isLikelyFixtureSourceKey(key) && strongest[1] > 0
  };
}
