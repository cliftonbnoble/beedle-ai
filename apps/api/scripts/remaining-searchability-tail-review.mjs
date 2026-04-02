import path from 'node:path';
import {
  buildRealDecisionPredicate,
  countBy,
  defaultDbPath,
  runSqlJson,
  writeJson,
  writeText
} from './lib/overnight-corpus-lift-utils.mjs';

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.REMAINING_TAIL_REVIEW_BUSY_TIMEOUT_MS || '5000', 10);
const outputDir = path.resolve(process.cwd(), process.env.REMAINING_TAIL_REVIEW_OUTPUT_DIR || 'reports');
const jsonName = process.env.REMAINING_TAIL_REVIEW_JSON_NAME || 'remaining-searchability-tail-review-report.json';
const markdownName = process.env.REMAINING_TAIL_REVIEW_MARKDOWN_NAME || 'remaining-searchability-tail-review-report.md';
const sampleLimit = Math.max(1, Number.parseInt(process.env.REMAINING_TAIL_REVIEW_SAMPLE_LIMIT || '10', 10));

function normalize(value) {
  return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function parseWarnings(raw) {
  try {
    const parsed = JSON.parse(raw || '[]');
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

function isUnknownReferenceLike(row) {
  const citation = String(row.citation || '');
  const title = String(row.title || '');
  return citation.startsWith('UNK-REF-') || /^unknown reference\b/i.test(title);
}

function isProceduralAttachmentLike(row) {
  const text = normalize(`${row.title || ''} ${row.citation || ''} ${row.sourceFileRef || ''}`);
  return /tech\.? corr|technical correction|ext open record|memorandum|table \d|notice of tech|open record/.test(text);
}

function isDecisionLike(row) {
  const text = normalize(`${row.title || ''} ${row.citation || ''}`);
  return /decision|dismissal|order|remand|appeal|hearing/.test(text);
}

function classifyRow(row) {
  const warnings = parseWarnings(row.warningsJson);
  const plainTextLength = Number(row.plainTextLength || 0);
  const chunkCount = Number(row.chunkCount || 0);
  const hasIndex = Number(row.hasIndex || 0) === 1;
  const hasRules = Number(row.hasRules || 0) === 1;
  const hasOrdinance = Number(row.hasOrdinance || 0) === 1;
  const confirmed = Number(row.confirmed || 0) === 1;

  let recommendedAction = 'manual_tail_review';
  let rationale = 'Needs manual review to decide whether it belongs in searchable corpus.';

  if (isUnknownReferenceLike(row)) {
    recommendedAction = 'exclude_unknown_reference_like';
    rationale = 'Looks like a synthetic or placeholder unknown-reference row rather than a real decision.';
  } else if (hasIndex && hasRules && hasOrdinance && !confirmed && plainTextLength >= 3000 && chunkCount >= 3) {
    recommendedAction = 'manual_confirm_and_enable';
    rationale = 'Metadata looks complete; likely blocked only because final confirmation flag was never applied.';
  } else if (!hasIndex && !hasRules && !hasOrdinance && isProceduralAttachmentLike(row)) {
    recommendedAction = plainTextLength >= 3000 && chunkCount >= 3
      ? 'procedural_attachment_review'
      : 'leave_out_low_value_attachment';
    rationale = plainTextLength >= 3000 && chunkCount >= 3
      ? 'Substantial text exists, but the document looks procedural or attachment-like and should be reviewed before admission.'
      : 'Very likely low-value procedural attachment or correction notice.';
  } else if (!hasIndex && !hasRules && !hasOrdinance && isDecisionLike(row) && plainTextLength >= 3000 && chunkCount >= 3) {
    recommendedAction = 'candidate_loose_text_ready_review';
    rationale = 'Looks decision-like and text-rich, but lacks structured metadata.';
  } else if (((hasRules ? 1 : 0) + (hasOrdinance ? 1 : 0) + (hasIndex ? 1 : 0)) === 1 && plainTextLength >= 1500 && chunkCount >= 2) {
    recommendedAction = 'single_context_manual_review';
    rationale = 'Has one meaningful metadata context and enough text to justify a targeted admission decision.';
  } else if (plainTextLength < 1000 || chunkCount <= 1) {
    recommendedAction = 'leave_out_short_sparse';
    rationale = 'Too short or too sparse to improve search in a meaningful way.';
  }

  return {
    documentId: String(row.documentId || ''),
    citation: String(row.citation || ''),
    title: String(row.title || ''),
    decisionDate: row.decisionDate || '',
    sourceFileRef: String(row.sourceFileRef || ''),
    plainTextLength,
    chunkCount,
    hasIndex,
    hasRules,
    hasOrdinance,
    confirmed,
    warnings,
    recommendedAction,
    rationale,
    isUnknownReferenceLike: isUnknownReferenceLike(row),
    isProceduralAttachmentLike: isProceduralAttachmentLike(row),
    isDecisionLike: isDecisionLike(row)
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push('# Remaining Searchability Tail Review');
  lines.push('');
  lines.push(`- generatedAt: ${report.generatedAt}`);
  lines.push(`- blockedDecisionCount: ${report.blockedDecisionCount}`);
  lines.push(`- uniqueRecommendedActions: ${report.recommendedActions.length}`);
  lines.push('');
  lines.push('## Action Breakdown');
  lines.push('');
  for (const action of report.recommendedActions) {
    lines.push(`- ${action.key}: ${action.count}`);
  }
  lines.push('');
  lines.push('## Sample Rows');
  lines.push('');
  for (const action of report.rowsByAction) {
    lines.push(`### ${action.key}`);
    lines.push('');
    for (const row of action.rows.slice(0, sampleLimit)) {
      lines.push(`- ${row.citation || row.documentId} | ${row.title || '<untitled>'} | text=${row.plainTextLength} | chunks=${row.chunkCount} | action=${row.recommendedAction}`);
      lines.push(`  rationale: ${row.rationale}`);
    }
    lines.push('');
  }
  return `${lines.join('\n')}\n`;
}

async function main() {
  const rows = await runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      WITH chunk_stats AS (
        SELECT document_id, COUNT(*) AS chunkCount
        FROM document_chunks
        GROUP BY 1
      )
      SELECT
        d.id AS documentId,
        d.citation,
        d.title,
        d.source_r2_key AS sourceFileRef,
        d.decision_date AS decisionDate,
        COALESCE(d.qc_has_index_codes, 0) AS hasIndex,
        COALESCE(d.qc_has_rules_section, 0) AS hasRules,
        COALESCE(d.qc_has_ordinance_section, 0) AS hasOrdinance,
        COALESCE(d.qc_required_confirmed, 0) AS confirmed,
        COALESCE(json_extract(d.metadata_json, '$.plainTextLength'), 0) AS plainTextLength,
        COALESCE(cs.chunkCount, 0) AS chunkCount,
        COALESCE(d.extraction_warnings_json, '[]') AS warningsJson
      FROM documents d
      LEFT JOIN chunk_stats cs ON cs.document_id = d.id
      WHERE ${buildRealDecisionPredicate('d')}
        AND d.searchable_at IS NULL
        AND d.qc_passed = 0
      ORDER BY plainTextLength DESC, chunkCount DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC
    `
  });

  const classifiedRows = rows.map(classifyRow);
  const recommendedActions = countBy(classifiedRows.map((row) => row.recommendedAction));
  const rowsByAction = recommendedActions.map((action) => ({
    key: action.key,
    count: action.count,
    rows: classifiedRows.filter((row) => row.recommendedAction === action.key)
  }));

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    blockedDecisionCount: classifiedRows.length,
    recommendedActions,
    rowsByAction,
    rows: classifiedRows
  };

  const jsonPath = path.resolve(outputDir, jsonName);
  const markdownPath = path.resolve(outputDir, markdownName);
  await writeJson(jsonPath, report);
  await writeText(markdownPath, toMarkdown(report));

  console.log(JSON.stringify({
    blockedDecisionCount: report.blockedDecisionCount,
    recommendedActions: report.recommendedActions
  }, null, 2));
  console.log(`Remaining searchability tail review JSON written to ${jsonPath}`);
  console.log(`Remaining searchability tail review Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
