import {
  buildRealDecisionPredicate,
  defaultDbPath,
  runSqlJson
} from './overnight-corpus-lift-utils.mjs';

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

export const DEFAULT_MIN_PHRASE_ONLY_SCORE = 7;
export const DEFAULT_MIN_PHRASE_ONLY_MARGIN = 0.25;
export const DEFAULT_MIN_CROSSWALK_PHRASE_SCORE = 7;
export const DEFAULT_MIN_CROSSWALK_PHRASE_MARGIN = 0.75;
export const DEFAULT_MIN_CROSSWALK_ONLY_SCORE = 6.5;
export const DEFAULT_MIN_CROSSWALK_ONLY_MARGIN = 1;
export const DEFAULT_MIN_STRONG_PHRASE_SCORE = 7.5;
export const DEFAULT_MIN_STRONG_PHRASE_MARGIN = 0.5;
export const DEFAULT_MIN_PHRASE_STRENGTH_SCORE = 7;
export const DEFAULT_MIN_PHRASE_STRENGTH_MARGIN = 0.25;

export function extractCaseTokens(value) {
  return uniqueSorted(String(value || '').match(/[A-Z]{0,2}\d{6}/g) || []);
}

export function hasEvidencePrefix(candidate, prefix) {
  return (candidate?.evidence || []).some((value) => String(value).startsWith(prefix));
}

export function hasAnyEvidencePrefix(candidate, prefixes) {
  return (prefixes || []).some((prefix) => hasEvidencePrefix(candidate, prefix));
}

export function hasSource(candidate, source) {
  return (candidate?.sources || []).map(String).includes(String(source));
}

export function phraseEvidenceCount(candidate) {
  return (candidate?.evidence || []).filter((value) => /^(exact|findings|conclusions)_phrase:/.test(String(value))).length;
}

export function scoreMargin(topCandidate, secondCandidate) {
  if (!topCandidate || !secondCandidate) return null;
  return Number((Number(topCandidate.score || 0) - Number(secondCandidate.score || 0)).toFixed(2));
}

export function evaluateTailInferenceCandidate(row, options = {}) {
  const topCandidate = Array.isArray(row?.candidateCodes) ? row.candidateCodes[0] || null : null;
  const secondCandidate = Array.isArray(row?.candidateCodes) ? row.candidateCodes[1] || null : null;

  if (!topCandidate?.code) {
    return {
      eligible: false,
      reason: 'no_candidate',
      topCandidate: null,
      secondCandidate: secondCandidate || null,
      selectedIndexCodes: []
    };
  }

  const margin = scoreMargin(topCandidate, secondCandidate);
  const topScore = Number(topCandidate.score || 0);
  const topExactPhrase = hasEvidencePrefix(topCandidate, 'exact_phrase:');
  const topPhraseSignals = hasAnyEvidencePrefix(topCandidate, ['exact_phrase:', 'findings_phrase:', 'conclusions_phrase:']);
  const secondPhraseSignals = hasAnyEvidencePrefix(secondCandidate, ['exact_phrase:', 'findings_phrase:', 'conclusions_phrase:']);
  const topPhraseOnly = hasSource(topCandidate, 'phrase') && !hasSource(topCandidate, 'crosswalk') && !hasSource(topCandidate, 'dhs_phrase');
  const topCrosswalkOnly = hasSource(topCandidate, 'crosswalk') && !hasSource(topCandidate, 'phrase') && !hasSource(topCandidate, 'dhs_phrase');
  const topCrosswalkPhrase = hasSource(topCandidate, 'crosswalk') && hasSource(topCandidate, 'phrase');
  const phraseOnlyScoreFloor = Number(options.minPhraseOnlyScore ?? DEFAULT_MIN_PHRASE_ONLY_SCORE);
  const phraseOnlyMarginFloor = Number(options.minPhraseOnlyMargin ?? DEFAULT_MIN_PHRASE_ONLY_MARGIN);
  const crosswalkPhraseScoreFloor = Number(options.minCrosswalkPhraseScore ?? DEFAULT_MIN_CROSSWALK_PHRASE_SCORE);
  const crosswalkPhraseMarginFloor = Number(options.minCrosswalkPhraseMargin ?? DEFAULT_MIN_CROSSWALK_PHRASE_MARGIN);
  const crosswalkOnlyScoreFloor = Number(options.minCrosswalkOnlyScore ?? DEFAULT_MIN_CROSSWALK_ONLY_SCORE);
  const crosswalkOnlyMarginFloor = Number(options.minCrosswalkOnlyMargin ?? DEFAULT_MIN_CROSSWALK_ONLY_MARGIN);
  const strongPhraseScoreFloor = Number(options.minStrongPhraseScore ?? DEFAULT_MIN_STRONG_PHRASE_SCORE);
  const strongPhraseMarginFloor = Number(options.minStrongPhraseMargin ?? DEFAULT_MIN_STRONG_PHRASE_MARGIN);
  const phraseStrengthScoreFloor = Number(options.minPhraseStrengthScore ?? DEFAULT_MIN_PHRASE_STRENGTH_SCORE);
  const phraseStrengthMarginFloor = Number(options.minPhraseStrengthMargin ?? DEFAULT_MIN_PHRASE_STRENGTH_MARGIN);

  if (topPhraseOnly && topExactPhrase && topScore >= phraseOnlyScoreFloor && (margin === null || margin >= phraseOnlyMarginFloor) && !secondPhraseSignals) {
    return {
      eligible: true,
      reason: 'phrase_only_exact_beats_generic',
      selectedCode: String(topCandidate.code),
      selectedIndexCodes: [String(topCandidate.code)],
      topCandidate,
      secondCandidate,
      margin,
      selectedSources: uniqueSorted(topCandidate.sources || [])
    };
  }

  if (topCrosswalkPhrase && topExactPhrase && topScore >= crosswalkPhraseScoreFloor && (margin === null || margin >= crosswalkPhraseMarginFloor)) {
    return {
      eligible: true,
      reason: 'crosswalk_phrase_exact',
      selectedCode: String(topCandidate.code),
      selectedIndexCodes: [String(topCandidate.code)],
      topCandidate,
      secondCandidate,
      margin,
      selectedSources: uniqueSorted(topCandidate.sources || [])
    };
  }

  if (topCrosswalkOnly && topScore >= crosswalkOnlyScoreFloor && margin !== null && margin >= crosswalkOnlyMarginFloor) {
    return {
      eligible: true,
      reason: 'crosswalk_margin',
      selectedCode: String(topCandidate.code),
      selectedIndexCodes: [String(topCandidate.code)],
      topCandidate,
      secondCandidate,
      margin,
      selectedSources: uniqueSorted(topCandidate.sources || [])
    };
  }

  if (topPhraseSignals && topScore >= strongPhraseScoreFloor && (margin === null || margin >= strongPhraseMarginFloor)) {
    return {
      eligible: true,
      reason: 'strong_phrase_family',
      selectedCode: String(topCandidate.code),
      selectedIndexCodes: [String(topCandidate.code)],
      topCandidate,
      secondCandidate,
      margin,
      selectedSources: uniqueSorted(topCandidate.sources || [])
    };
  }

  if (
    secondCandidate &&
    hasSource(topCandidate, 'phrase') &&
    hasSource(secondCandidate, 'phrase') &&
    phraseEvidenceCount(topCandidate) >= phraseEvidenceCount(secondCandidate) + 1 &&
    topScore >= phraseStrengthScoreFloor &&
    margin !== null &&
    margin >= phraseStrengthMarginFloor
  ) {
    return {
      eligible: true,
      reason: 'phrase_strength_advantage',
      selectedCode: String(topCandidate.code),
      selectedIndexCodes: [String(topCandidate.code)],
      topCandidate,
      secondCandidate,
      margin,
      selectedSources: uniqueSorted(topCandidate.sources || [])
    };
  }

  return {
    eligible: false,
    reason: 'no_tail_rule_matched',
    selectedCode: null,
    selectedIndexCodes: [],
    topCandidate,
    secondCandidate,
    margin
  };
}

export function selectTailInferenceCandidates(rows, options = {}) {
  const evaluated = (rows || []).map((row) => ({
    row,
    evaluation: evaluateTailInferenceCandidate(row, options)
  }));

  const selected = evaluated
    .filter((item) => item.evaluation.eligible)
    .sort((a, b) => {
      const scoreDelta = Number(b.evaluation.topCandidate?.score || 0) - Number(a.evaluation.topCandidate?.score || 0);
      if (scoreDelta !== 0) return scoreDelta;
      const marginA = Number(a.evaluation.margin ?? Number.POSITIVE_INFINITY);
      const marginB = Number(b.evaluation.margin ?? Number.POSITIVE_INFINITY);
      if (marginB !== marginA) return marginB - marginA;
      const dateA = String(a.row?.decisionDate || '');
      const dateB = String(b.row?.decisionDate || '');
      return dateB.localeCompare(dateA) || String(a.row?.citation || '').localeCompare(String(b.row?.citation || ''));
    });

  return {
    selected,
    skipped: evaluated.filter((item) => !item.evaluation.eligible)
  };
}

export async function buildSiblingInheritanceCandidates(rows, { dbPath = defaultDbPath, busyTimeoutMs = 5000 } = {}) {
  const siblingRows = await runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        d.id,
        d.citation,
        d.title,
        d.index_codes_json as indexCodesJson
      FROM documents d
      WHERE ${buildRealDecisionPredicate('d')}
        AND COALESCE(d.index_codes_json, '') NOT IN ('', '[]')
    `
  });

  const siblingDocs = siblingRows.map((row) => {
    let indexCodes = [];
    try {
      indexCodes = JSON.parse(row.indexCodesJson || '[]').map(String).sort();
    } catch {
      indexCodes = [];
    }
    return {
      documentId: String(row.id),
      citation: String(row.citation || ''),
      title: String(row.title || ''),
      indexCodes
    };
  }).filter((row) => row.indexCodes.length > 0);

  const docsByToken = new Map();
  for (const doc of siblingDocs) {
    for (const token of extractCaseTokens(`${doc.citation} ${doc.title}`)) {
      const current = docsByToken.get(token) || [];
      current.push(doc);
      docsByToken.set(token, current);
    }
  }

  return (rows || [])
    .map((row) => {
      const tokens = extractCaseTokens(`${row.citation || ''} ${row.title || ''}`);
      const siblingMatches = [];
      const seen = new Set();
      for (const token of tokens) {
        for (const doc of docsByToken.get(token) || []) {
          if (doc.documentId === row.documentId || seen.has(doc.documentId)) continue;
          siblingMatches.push(doc);
          seen.add(doc.documentId);
        }
      }

      const siblingVariants = uniqueSorted(
        siblingMatches
          .map((doc) => JSON.stringify(doc.indexCodes))
          .filter((raw) => raw !== '[]')
      ).map((raw) => JSON.parse(raw));

      if (siblingVariants.length !== 1 || siblingVariants[0].length === 0) {
        return null;
      }

      return {
        row,
        selectionMethod: 'sibling_inheritance',
        selectedIndexCodes: siblingVariants[0].map(String),
        selectedCode: siblingVariants[0][0],
        siblingCaseTokens: tokens,
        siblingCount: siblingMatches.length,
        siblingSample: siblingMatches.slice(0, 3).map((doc) => ({
          documentId: doc.documentId,
          citation: doc.citation,
          title: doc.title,
          indexCodes: doc.indexCodes
        }))
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      const dateA = String(a.row?.decisionDate || '');
      const dateB = String(b.row?.decisionDate || '');
      return dateB.localeCompare(dateA) || String(a.row?.citation || '').localeCompare(String(b.row?.citation || ''));
    });
}
