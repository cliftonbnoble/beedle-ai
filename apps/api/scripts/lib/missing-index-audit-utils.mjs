import fs from "node:fs/promises";
import path from "node:path";
import {
  TARGET_CODES,
  loadDhsTargetCatalog,
  scanDhsLegacyCandidates,
  normalizeCode,
  parseJsonArray,
  uniqueSorted
} from "./dhs-index-code-remediation.mjs";
import {
  countBy,
  extractOrdinanceFamily,
  extractRulesFamily,
  normalizeCitation,
  normalizeOrdinanceCitation,
  normalizeText,
  normalizeWhitespace,
  normalizeBareRulesCitation,
  sqlQuote as sqlQuoteShared,
  runSqlJson,
  defaultDbPath,
  buildRealDecisionPredicate
} from "./overnight-corpus-lift-utils.mjs";

const sharedIndexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");
const chunkBatchSize = 25;

const STOPWORDS = new Set([
  "a",
  "an",
  "and",
  "any",
  "at",
  "be",
  "by",
  "for",
  "from",
  "in",
  "into",
  "is",
  "of",
  "on",
  "or",
  "the",
  "to",
  "with"
]);

const ISSUE_FAMILY_RULES = [
  { family: "heat", phrases: ["no heat", "lack of heat", "inadequate heat", "heat outage", "insufficient heat"] },
  { family: "hot_water", phrases: ["no hot water", "lack of hot water", "inadequate hot water", "lukewarm water"] },
  { family: "mold", phrases: ["mold", "mould", "mildew", "fungal growth"] },
  { family: "cockroach", phrases: ["cockroach", "cockroaches", "roach", "roaches"] },
  { family: "rodent", phrases: ["rodent", "rats", "rat", "mice", "mouse droppings"] },
  { family: "bed_bugs", phrases: ["bed bug", "bed bugs", "bedbug", "bedbugs"] },
  { family: "infestation", phrases: ["infestation", "infested", "vermin"] },
  { family: "leaks", phrases: ["leak", "leaks", "water leak", "ceiling leak", "leaking"] },
  { family: "repairs", phrases: ["repair", "repairs", "broken", "defective", "needed repairs"] },
  { family: "security", phrases: ["lock", "locks", "security", "front door lock", "door lock"] },
  { family: "noise", phrases: ["noise", "noisy", "loud", "construction noise"] },
  { family: "decrease_services", phrases: ["decrease in services", "reduction in services", "services reduced"] },
  { family: "rent_reduction", phrases: ["rent reduction", "reduced rent", "abatement", "rent abatement"] }
];

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function containsWholePhrase(normalizedText, phrase) {
  const normalizedPhrase = normalizeText(phrase);
  if (!normalizedPhrase) return false;
  const pattern = `(^|[^a-z0-9])${escapeRegex(normalizedPhrase).replace(/\s+/g, "\\s+")}([^a-z0-9]|$)`;
  return new RegExp(pattern, "i").test(normalizedText);
}

function loadJsonArrayFromTs(raw) {
  const match = raw.match(/export const canonicalIndexCodeOptions = (\[[\s\S]*?\]) as const;/);
  if (!match) {
    throw new Error(`Could not parse shared index catalog JSON from ${sharedIndexCatalogPath}`);
  }
  return JSON.parse(match[1]);
}

function significantTokens(value) {
  return uniqueSorted(
    normalizeText(value)
      .split(/[^a-z0-9.()\-]+/g)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3 && !STOPWORDS.has(token))
  );
}

function buildPhraseCandidates(option) {
  const rawPhrases = [];
  if (option.label) rawPhrases.push(String(option.label));
  if (option.description) rawPhrases.push(String(option.description));
  const clean = uniqueSorted(
    rawPhrases
      .map((phrase) => normalizeWhitespace(phrase))
      .filter((phrase) => phrase.length >= 8 && phrase.length <= 120)
  );

  return clean.map((phrase) => ({
    phrase,
    normalizedPhrase: normalizeText(phrase),
    tokens: significantTokens(phrase)
  }));
}

export async function loadSharedIndexCatalog() {
  const raw = await fs.readFile(sharedIndexCatalogPath, "utf8");
  return loadJsonArrayFromTs(raw);
}

export async function loadActiveIndexMetadata({ dbPath = defaultDbPath, busyTimeoutMs = 5000 } = {}) {
  const rows = await runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        code_identifier AS codeIdentifier,
        normalized_code AS normalizedCode,
        family,
        label,
        description,
        is_reserved AS isReserved,
        linked_ordinance_sections_json AS linkedOrdinanceSectionsJson,
        linked_rules_sections_json AS linkedRulesSectionsJson
      FROM legal_index_codes
      WHERE active = 1
    `
  });

  return rows.map((row) => ({
    codeIdentifier: String(row.codeIdentifier || "").trim(),
    normalizedCode: String(row.normalizedCode || "").trim(),
    family: String(row.family || "").trim(),
    label: String(row.label || "").trim(),
    description: String(row.description || "").trim(),
    isReserved: Number(row.isReserved || 0) === 1,
    linkedOrdinanceSections: parseJsonArray(row.linkedOrdinanceSectionsJson),
    linkedRulesSections: parseJsonArray(row.linkedRulesSectionsJson)
  }));
}

export function buildLinkedSectionMaps(indexRows) {
  const byOrdinance = new Map();
  const byRules = new Map();
  const byRulesBare = new Map();

  for (const row of indexRows || []) {
    if (!row?.codeIdentifier || row?.isReserved) continue;
    const code = String(row.codeIdentifier);
    for (const citation of row.linkedOrdinanceSections || []) {
      const key = normalizeOrdinanceCitation(citation);
      if (!key) continue;
      const current = byOrdinance.get(key) || [];
      current.push(code);
      byOrdinance.set(key, current);
    }
    for (const citation of row.linkedRulesSections || []) {
      const key = normalizeCitation(citation);
      if (key) {
        const current = byRules.get(key) || [];
        current.push(code);
        byRules.set(key, current);
      }
      const bareKey = normalizeBareRulesCitation(citation);
      if (bareKey) {
        const currentBare = byRulesBare.get(bareKey) || [];
        currentBare.push(code);
        byRulesBare.set(bareKey, currentBare);
      }
    }
  }

  return { byOrdinance, byRules, byRulesBare };
}

export async function loadCrosswalkMaps({ dbPath = defaultDbPath, busyTimeoutMs = 5000, activeIndexRows = [] } = {}) {
  const rows = await runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        index_code_id AS indexCodeId,
        ordinance_citation AS ordinanceCitation,
        rules_citation AS rulesCitation
      FROM legal_reference_crosswalk
    `
  });

  const canonicalByNormalized = new Map(
    (activeIndexRows || [])
      .filter((row) => row.codeIdentifier && !row.isReserved)
      .map((row) => [normalizeCode(row.codeIdentifier), row.codeIdentifier])
  );

  const byOrdinance = new Map();
  const byRules = new Map();
  const byRulesBare = new Map();

  for (const row of rows) {
    const indexCodeId = String(row.indexCodeId || "").trim();
    if (!indexCodeId) continue;
    const canonical = canonicalByNormalized.get(normalizeCode(indexCodeId)) || indexCodeId;
    const ordinanceKey = normalizeOrdinanceCitation(row.ordinanceCitation || "");
    const rulesKey = normalizeCitation(row.rulesCitation || "");
    const rulesBareKey = normalizeBareRulesCitation(row.rulesCitation || "");

    if (ordinanceKey) {
      const current = byOrdinance.get(ordinanceKey) || [];
      current.push(canonical);
      byOrdinance.set(ordinanceKey, current);
    }
    if (rulesKey) {
      const current = byRules.get(rulesKey) || [];
      current.push(canonical);
      byRules.set(rulesKey, current);
    }
    if (rulesBareKey) {
      const current = byRulesBare.get(rulesBareKey) || [];
      current.push(canonical);
      byRulesBare.set(rulesBareKey, current);
    }
  }

  return { byOrdinance, byRules, byRulesBare };
}

export function buildCatalogMatchers({ sharedCatalog, activeIndexRows, includeDhsTargets = true }) {
  const activeByCode = new Map(
    (activeIndexRows || [])
      .filter((row) => row.codeIdentifier && !row.isReserved)
      .map((row) => [String(row.codeIdentifier).toUpperCase(), row])
  );

  const catalog = (sharedCatalog || [])
    .map((option) => {
      const code = String(option.code || option.codeIdentifier || "").toUpperCase();
      const active = activeByCode.get(code);
      if (!code || !active) return null;
      const phraseCandidates = buildPhraseCandidates({
        label: active.label || option.label,
        description: active.description || option.description
      });
      if (!phraseCandidates.length && !includeDhsTargets) return null;
      return {
        code,
        family: active.family || option.family || "",
        label: active.label || option.label || "",
        description: active.description || option.description || "",
        phraseCandidates,
        significantLabelTokens: significantTokens(active.label || option.label || "")
      };
    })
    .filter(Boolean);

  if (!includeDhsTargets) return catalog;

  return catalog.sort((a, b) => a.code.localeCompare(b.code));
}

export function detectIssueFamilies(normalizedText) {
  const matches = [];
  for (const rule of ISSUE_FAMILY_RULES) {
    const hits = (rule.phrases || []).filter((phrase) => containsWholePhrase(normalizedText, phrase));
    if (!hits.length) continue;
    matches.push({
      family: rule.family,
      hits: uniqueSorted(hits),
      score: hits.length
    });
  }
  return matches.sort((a, b) => b.score - a.score || a.family.localeCompare(b.family));
}

export function inferCrosswalkCandidates({ rulesSections, ordinanceSections, crosswalkMaps, linkedSectionMaps }) {
  const evidence = [];
  const byCode = new Map();

  const add = (code, reason, score) => {
    if (!code) return;
    const current = byCode.get(code) || { code, score: 0, evidence: [] };
    current.score += score;
    current.evidence.push(reason);
    byCode.set(code, current);
  };

  for (const citation of rulesSections || []) {
    const normalized = normalizeCitation(citation);
    const bare = normalizeBareRulesCitation(citation);
    for (const code of crosswalkMaps?.byRules?.get(normalized) || []) add(code, `crosswalk_rules:${citation}`, 5);
    for (const code of crosswalkMaps?.byRulesBare?.get(bare) || []) add(code, `crosswalk_rules_bare:${citation}`, 4.5);
    for (const code of linkedSectionMaps?.byRules?.get(normalized) || []) add(code, `linked_rules:${citation}`, 3.5);
    for (const code of linkedSectionMaps?.byRulesBare?.get(bare) || []) add(code, `linked_rules_bare:${citation}`, 3.25);
  }

  for (const citation of ordinanceSections || []) {
    const normalized = normalizeOrdinanceCitation(citation);
    for (const code of crosswalkMaps?.byOrdinance?.get(normalized) || []) add(code, `crosswalk_ordinance:${citation}`, 5);
    for (const code of linkedSectionMaps?.byOrdinance?.get(normalized) || []) add(code, `linked_ordinance:${citation}`, 3.5);
  }

  return Array.from(byCode.values())
    .map((row) => ({
      code: row.code,
      score: Number(row.score.toFixed(2)),
      evidence: uniqueSorted(row.evidence)
    }))
    .sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));
}

export function inferPhraseCandidates({ normalizedText, findingsText, conclusionsText, catalogMatchers }) {
  const findingsNormalized = normalizeText(findingsText);
  const conclusionsNormalized = normalizeText(conclusionsText);
  const out = [];

  for (const matcher of catalogMatchers || []) {
    let score = 0;
    const evidence = [];

    for (const candidate of matcher.phraseCandidates || []) {
      if (candidate.normalizedPhrase && normalizedText.includes(candidate.normalizedPhrase)) {
        score += 4.5;
        evidence.push(`exact_phrase:${candidate.phrase}`);
      } else if (
        candidate.tokens.length >= 2 &&
        candidate.tokens.every((token) => normalizedText.includes(token))
      ) {
        score += 1.75;
        evidence.push(`token_cover:${candidate.tokens.join(",")}`);
      }

      if (candidate.normalizedPhrase && findingsNormalized.includes(candidate.normalizedPhrase)) {
        score += 1.25;
        evidence.push(`findings_phrase:${candidate.phrase}`);
      }
      if (candidate.normalizedPhrase && conclusionsNormalized.includes(candidate.normalizedPhrase)) {
        score += 1.5;
        evidence.push(`conclusions_phrase:${candidate.phrase}`);
      }
    }

    if (score >= 4.5) {
      out.push({
        code: matcher.code,
        score: Number(score.toFixed(2)),
        evidence: uniqueSorted(evidence)
      });
    }
  }

  return out.sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));
}

function summarizeSectionText(chunks, test) {
  return normalizeWhitespace(
    (chunks || [])
      .filter((chunk) => test(chunk.sectionLabel || ""))
      .map((chunk) => chunk.chunkText || "")
      .join(" ")
  );
}

function isFindingsLike(label) {
  const normalized = normalizeText(label || "");
  return normalized.includes("findings of fact") || normalized === "findings_of_fact" || normalized === "findings";
}

function isConclusionsLike(label) {
  const normalized = normalizeText(label || "");
  return normalized.includes("conclusions of law") || normalized === "conclusions_of_law" || normalized === "authority_discussion";
}

export async function loadAuditDocuments({ dbPath = defaultDbPath, busyTimeoutMs = 5000, limit = 0 } = {}) {
  const limitClause = limit > 0 ? `LIMIT ${Number(limit)}` : "";
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        d.id,
        d.title,
        d.citation,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.rules_sections_json AS rulesSectionsJson,
        d.ordinance_sections_json AS ordinanceSectionsJson,
        d.extraction_warnings_json AS extractionWarningsJson,
        d.source_link AS sourceLink,
        d.source_r2_key AS sourceR2Key
      FROM documents d
      WHERE ${buildRealDecisionPredicate("d")}
        AND d.qc_has_index_codes = 0
        AND d.qc_has_rules_section = 1
        AND d.qc_has_ordinance_section = 1
        AND d.searchable_at IS NULL
      ORDER BY COALESCE(d.decision_date, '') DESC, d.citation ASC
      ${limitClause}
    `
  });
}

export async function loadChunksByDocumentId({ dbPath = defaultDbPath, busyTimeoutMs = 5000, documentIds }) {
  const map = new Map();
  const ids = uniqueSorted((documentIds || []).map(String).filter(Boolean));

  for (let index = 0; index < ids.length; index += chunkBatchSize) {
    const batch = ids.slice(index, index + chunkBatchSize);
    if (!batch.length) continue;
    const idList = batch.map(sqlQuoteShared).join(", ");
    const trustedRows = await runSqlJson({
      dbPath,
      busyTimeoutMs,
      sql: `
        SELECT
          rs.document_id AS documentId,
          rs.chunk_id AS chunkId,
          rs.section_label AS sectionLabel,
          rs.chunk_text AS chunkText,
          1 AS trusted
        FROM retrieval_search_chunks rs
        WHERE rs.active = 1
          AND rs.document_id IN (${idList})
      `
    });

    const trustedDocIds = new Set(trustedRows.map((row) => row.documentId));
    const fallbackBatch = batch.filter((documentId) => !trustedDocIds.has(documentId));
    const fallbackRows = fallbackBatch.length
      ? await runSqlJson({
          dbPath,
          busyTimeoutMs,
          sql: `
            SELECT
              c.document_id AS documentId,
              c.id AS chunkId,
              c.section_label AS sectionLabel,
              c.chunk_text AS chunkText,
              0 AS trusted
            FROM document_chunks c
            WHERE c.document_id IN (${fallbackBatch.map(sqlQuoteShared).join(", ")})
          `
        })
      : [];

    for (const row of [...trustedRows, ...fallbackRows]) {
      const current = map.get(row.documentId) || [];
      current.push({
        chunkId: String(row.chunkId || ""),
        sectionLabel: String(row.sectionLabel || ""),
        chunkText: String(row.chunkText || ""),
        trusted: Number(row.trusted || 0) === 1
      });
      map.set(row.documentId, current);
    }
  }

  return map;
}

export function buildAuditRow({ doc, chunks = [], crosswalkCandidates = [], phraseCandidates = [], dhsDocMap = new Map() }) {
  const rulesSections = parseJsonArray(doc.rulesSectionsJson).map((item) => normalizeWhitespace(item));
  const ordinanceSections = parseJsonArray(doc.ordinanceSectionsJson).map((item) => normalizeWhitespace(item));
  const normalizedChunkTexts = (chunks || [])
    .map((chunk) => normalizeWhitespace(chunk.chunkText || ""))
    .filter(Boolean);
  const aggregateText = normalizeWhitespace(normalizedChunkTexts.join(" "));
  const normalizedText = normalizeText(aggregateText);
  const findingsText = summarizeSectionText(chunks, isFindingsLike);
  const conclusionsText = summarizeSectionText(chunks, isConclusionsLike);
  const issueFamilies = detectIssueFamilies(normalizedText);
  const dhsMatch = dhsDocMap.get(doc.id) || null;

  const candidateMap = new Map();
  const addCandidate = (candidate, source) => {
    if (!candidate?.code) return;
    const current = candidateMap.get(candidate.code) || { code: candidate.code, score: 0, evidence: [], sources: [] };
    current.score += Number(candidate.score || 0);
    current.evidence.push(...(candidate.evidence || []));
    current.sources.push(source);
    candidateMap.set(candidate.code, current);
  };

  for (const candidate of crosswalkCandidates) addCandidate(candidate, "crosswalk");
  for (const candidate of phraseCandidates) addCandidate(candidate, "phrase");
  for (const detection of dhsMatch?.detections || []) {
    addCandidate({ code: detection.code, score: Number(detection.score || 0), evidence: detection.reasons || [] }, "dhs_phrase");
  }

  const candidateCodes = Array.from(candidateMap.values())
    .map((row) => ({
      code: row.code,
      score: Number(row.score.toFixed(2)),
      evidence: uniqueSorted(row.evidence),
      sources: uniqueSorted(row.sources)
    }))
    .filter((row) => row.score >= 4.5)
    .sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));

  return {
    documentId: doc.id,
    citation: String(doc.citation || ""),
    title: String(doc.title || ""),
    authorName: String(doc.authorName || ""),
    decisionDate: String(doc.decisionDate || ""),
    rulesSections,
    ordinanceSections,
    rulesFamilies: uniqueSorted(rulesSections.map(extractRulesFamily)),
    ordinanceFamilies: uniqueSorted(ordinanceSections.map(extractOrdinanceFamily)),
    issueFamilies,
    candidateCodes,
    hasHighConfidenceCandidate: candidateCodes.length > 0,
    findingsTextExcerpt: findingsText.slice(0, 320),
    conclusionsTextExcerpt: conclusionsText.slice(0, 320),
    textExcerpt: aggregateText.slice(0, 320),
    chunkCount: chunks.length,
    trustedChunkCount: (chunks || []).filter((chunk) => chunk.trusted).length,
    extractionWarnings: parseJsonArray(doc.extractionWarningsJson).map((item) => normalizeWhitespace(item)),
    sourceLink: String(doc.sourceLink || ""),
    sourceR2Key: String(doc.sourceR2Key || "")
  };
}

export function buildMissingIndexAuditSummary(rows) {
  const issueFamilyValues = [];
  const candidateCodeValues = [];
  const rulesFamilyValues = [];
  const ordinanceFamilyValues = [];

  for (const row of rows || []) {
    issueFamilyValues.push(...(row.issueFamilies || []).map((item) => item.family));
    candidateCodeValues.push(...(row.candidateCodes || []).map((item) => item.code));
    rulesFamilyValues.push(...(row.rulesFamilies || []));
    ordinanceFamilyValues.push(...(row.ordinanceFamilies || []));
  }

  return {
    byJudge: countBy((rows || []).map((row) => row.authorName || "<unknown>")),
    byRulesFamily: countBy(rulesFamilyValues),
    byOrdinanceFamily: countBy(ordinanceFamilyValues),
    byIssueFamily: countBy(issueFamilyValues),
    byCandidateCode: countBy(candidateCodeValues)
  };
}

export function formatMissingIndexAuditMarkdown(report) {
  const lines = [];
  lines.push("# Missing Index Code Full Audit");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- No write: \`${report.noWrite}\``);
  lines.push(`- Database: \`${report.dbPath}\``);
  lines.push("");

  lines.push("## Summary");
  lines.push(`- Missing-index-only docs: ${report.summary.missingIndexOnlyCount}`);
  lines.push(`- Candidate docs: ${report.summary.candidateDocCount}`);
  lines.push(`- Unresolved docs: ${report.summary.unresolvedDocCount}`);
  lines.push(`- DHS candidate overlaps: ${report.summary.dhsOverlapCandidateCount}`);
  lines.push("");

  const sections = [
    ["By Judge", report.summaryBreakdowns.byJudge],
    ["By Rules Family", report.summaryBreakdowns.byRulesFamily],
    ["By Ordinance Family", report.summaryBreakdowns.byOrdinanceFamily],
    ["By Issue Family", report.summaryBreakdowns.byIssueFamily],
    ["By Candidate Code", report.summaryBreakdowns.byCandidateCode]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    for (const row of (rows || []).slice(0, 20)) {
      lines.push(`- \`${row.key}\`: \`${row.count}\``);
    }
    if (!(rows || []).length) lines.push("- none");
    lines.push("");
  }

  lines.push("## High-confidence Review Candidates");
  for (const row of (report.candidates || []).slice(0, 100)) {
    const top = row.candidateCodes[0];
    lines.push(
      `- \`${row.citation}\` | candidate=\`${top?.code || "<none>"}\` | score=\`${top?.score || 0}\` | sources=\`${(top?.sources || []).join(", ") || "<none>"}\``
    );
  }
  if (!(report.candidates || []).length) lines.push("- none");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export async function buildMissingIndexAuditReport({ dbPath = defaultDbPath, busyTimeoutMs = 5000, limit = 0 } = {}) {
  const [sharedCatalog, activeIndexRows, auditDocs, dhsReport] = await Promise.all([
    loadSharedIndexCatalog(),
    loadActiveIndexMetadata({ dbPath, busyTimeoutMs }),
    loadAuditDocuments({ dbPath, busyTimeoutMs, limit }),
    scanDhsLegacyCandidates({ dbPath, busyTimeoutMs, docLimit: 0 })
  ]);

  const [crosswalkMaps, chunksByDocumentId, dhsCatalog] = await Promise.all([
    loadCrosswalkMaps({ dbPath, busyTimeoutMs, activeIndexRows }),
    loadChunksByDocumentId({ dbPath, busyTimeoutMs, documentIds: auditDocs.map((doc) => doc.id) }),
    loadDhsTargetCatalog()
  ]);
  const linkedSectionMaps = buildLinkedSectionMaps(activeIndexRows);
  const catalogMatchers = buildCatalogMatchers({ sharedCatalog, activeIndexRows, includeDhsTargets: true });
  const dhsDocMap = new Map((dhsReport?.candidates || []).map((row) => [row.id, row]));

  const rows = auditDocs.map((doc) => {
    const chunks = chunksByDocumentId.get(doc.id) || [];
    const rulesSections = parseJsonArray(doc.rulesSectionsJson);
    const ordinanceSections = parseJsonArray(doc.ordinanceSectionsJson);
    const aggregateText = normalizeText(chunks.map((chunk) => chunk.chunkText || "").join(" "));
    const findingsText = chunks
      .filter((chunk) => isFindingsLike(chunk.sectionLabel))
      .map((chunk) => chunk.chunkText || "")
      .join(" ");
    const conclusionsText = chunks
      .filter((chunk) => isConclusionsLike(chunk.sectionLabel))
      .map((chunk) => chunk.chunkText || "")
      .join(" ");
    const crosswalkCandidates = inferCrosswalkCandidates({ rulesSections, ordinanceSections, crosswalkMaps, linkedSectionMaps });
    const phraseCandidates = inferPhraseCandidates({
      normalizedText: aggregateText,
      findingsText,
      conclusionsText,
      catalogMatchers
    });

    return buildAuditRow({
      doc,
      chunks,
      crosswalkCandidates,
      phraseCandidates,
      dhsDocMap
    });
  });

  const summaryBreakdowns = buildMissingIndexAuditSummary(rows);
  const candidates = rows.filter((row) => row.hasHighConfidenceCandidate);
  const unresolved = rows.filter((row) => !row.hasHighConfidenceCandidate);

  return {
    generatedAt: new Date().toISOString(),
    noWrite: true,
    dbPath,
    limit,
    summary: {
      missingIndexOnlyCount: rows.length,
      candidateDocCount: candidates.length,
      unresolvedDocCount: unresolved.length,
      dhsOverlapCandidateCount: candidates.filter((row) => row.candidateCodes.some((candidate) => TARGET_CODES.includes(candidate.code))).length,
      dhsCatalogFamilies: TARGET_CODES.map((code) => ({ code, description: dhsCatalog.get(code)?.description || "" }))
    },
    summaryBreakdowns,
    candidates,
    unresolved: unresolved.slice(0, 250),
    rows
  };
}
