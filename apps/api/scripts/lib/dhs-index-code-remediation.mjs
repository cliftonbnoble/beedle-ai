import fs from "node:fs/promises";
import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

function resolveDefaultDbPath() {
  if (process.env.D1_DB_PATH) {
    return process.env.D1_DB_PATH;
  }

  const candidateDirs = [
    path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject"),
    path.resolve(process.cwd(), "apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject")
  ];

  for (const dir of candidateDirs) {
    if (!existsSync(dir)) continue;
    const sqliteFiles = readdirSync(dir)
      .filter((entry) => entry.endsWith(".sqlite") && !entry.includes("backup"))
      .sort();
    if (sqliteFiles.length > 0) {
      return path.join(dir, sqliteFiles[0]);
    }
  }

  return path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject/local.sqlite");
}

export const defaultDbPath = resolveDefaultDbPath();
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

export const TARGET_CODES = ["G40.1", "G44", "G49", "G50", "G52", "G53", "G54", "G64", "G76"];
const targetCodeSet = new Set(TARGET_CODES.map(normalizeCode));
const docBatchSize = 25;
const sqlBatchSize = 30;

const CODE_RULES = {
  "G40.1": {
    family: "infestation",
    threshold: 5,
    directPhrases: ["bed bug", "bed bugs", "bedbug", "bedbugs"],
    genericTerms: ["bed bug", "bed bugs", "bedbug", "bedbugs"]
  },
  G44: {
    family: "infestation",
    threshold: 5,
    directPhrases: ["cockroach", "cockroaches", "roach", "roaches", "cockroach infestation", "roach infestation"],
    genericTerms: ["cockroach", "cockroaches", "roach", "roaches"]
  },
  G49: {
    family: "heat",
    threshold: 4.5,
    directPhrases: ["lack of heat", "no heat", "without heat", "heat was not provided", "heat not provided"],
    serviceTerms: ["heat", "heating", "heater", "boiler", "radiator"],
    deprivationTerms: ["lack", "lacked", "no", "none", "without", "absent", "outage", "shut off", "not provided"]
  },
  G50: {
    family: "heat",
    threshold: 4.5,
    directPhrases: ["inadequate heat", "insufficient heat", "heat inadequate", "not enough heat", "low heat"],
    serviceTerms: ["heat", "heating", "heater", "boiler", "radiator"],
    inadequacyTerms: ["inadequate", "insufficient", "not enough", "too low", "unable to maintain", "temperature below", "low temperature"]
  },
  G52: {
    family: "hot_water",
    threshold: 4.5,
    directPhrases: ["lack of hot water", "no hot water", "without hot water", "hot water was not provided", "hot water not provided"],
    serviceTerms: ["hot water", "water heater"],
    deprivationTerms: ["lack", "lacked", "no", "none", "without", "absent", "outage", "shut off", "not provided"]
  },
  G53: {
    family: "hot_water",
    threshold: 4.5,
    directPhrases: ["inadequate hot water", "insufficient hot water", "hot water inadequate", "not enough hot water", "lukewarm water"],
    serviceTerms: ["hot water", "water heater"],
    inadequacyTerms: ["inadequate", "insufficient", "not enough", "lukewarm", "too low", "unable to maintain"]
  },
  G54: {
    family: "infestation",
    threshold: 4.5,
    directPhrases: ["insect infestation", "bug infestation", "infestation of insects", "insect problem", "bug problem"],
    genericTerms: ["infestation", "infested", "insects", "bugs", "bug"]
  },
  G64: {
    family: "mold",
    threshold: 4.5,
    directPhrases: ["mold", "mould", "mildew", "mold infestation", "mildew infestation"],
    genericTerms: ["mold", "mould", "mildew", "fungus", "fungal"]
  },
  G76: {
    family: "infestation",
    threshold: 5,
    directPhrases: ["rodent infestation", "rodent", "rats", "rat infestation", "mice", "mouse infestation", "mouse droppings"],
    genericTerms: ["rodent", "rats", "rat", "mice", "mouse"]
  }
};

export function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function normalize(input) {
  return String(input || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeCode(input) {
  return normalize(input).replace(/[\s_]+/g, "").replace(/[^a-z0-9.()\-]/g, "");
}

export function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

export function parseJsonArray(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

export function uniqueSorted(values) {
  return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

export function countBy(values) {
  const counts = new Map();
  for (const value of values) {
    const key = normalizeWhitespace(value || "<unknown>");
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

function isConclusionsLike(sectionLabel) {
  const label = normalize(sectionLabel || "");
  return /conclusions of law/.test(label) || label === "conclusions_of_law" || label === "authority_discussion";
}

function isFindingsLike(sectionLabel) {
  const label = normalize(sectionLabel || "");
  return /findings of fact/.test(label) || label === "findings_of_fact" || label === "findings";
}

function excerpt(text, max = 260) {
  return normalizeWhitespace(String(text || "")).slice(0, max);
}

function includesAny(text, values) {
  return values.some((value) => normalize(text).includes(normalize(value)));
}

function countMatches(text, values) {
  const normalizedText = normalize(text);
  return values.filter((value) => normalizedText.includes(normalize(value))).length;
}

function sectionBoost(sectionLabel) {
  if (isConclusionsLike(sectionLabel)) return 1.1;
  if (isFindingsLike(sectionLabel)) return 0.8;
  if (/order|decision/i.test(String(sectionLabel || ""))) return 0.3;
  return 0;
}

function scoreChunkForCode(code, chunk) {
  const rule = CODE_RULES[code];
  if (!rule) return null;
  const text = normalize(chunk.chunkText || "");
  if (!text) return null;

  let score = sectionBoost(chunk.sectionLabel);
  const reasons = [];

  const directHits = countMatches(text, rule.directPhrases || []);
  if (directHits > 0) {
    score += 4 + Math.min(2, directHits * 0.7);
    reasons.push(`direct_phrase_hits:${directHits}`);
  }

  if (code === "G49") {
    const service = countMatches(text, rule.serviceTerms || []);
    const deprivation = countMatches(text, rule.deprivationTerms || []);
    const inadequate = countMatches(text, CODE_RULES.G50.inadequacyTerms || []);
    if (service > 0 && deprivation > 0) {
      score += 3 + Math.min(1.5, service * 0.2 + deprivation * 0.3);
      reasons.push("heat_plus_deprivation");
    }
    if (inadequate > 0 && directHits === 0) score -= 0.8;
  } else if (code === "G50") {
    const service = countMatches(text, rule.serviceTerms || []);
    const inadequate = countMatches(text, rule.inadequacyTerms || []);
    const deprivation = countMatches(text, CODE_RULES.G49.deprivationTerms || []);
    if (service > 0 && inadequate > 0) {
      score += 3 + Math.min(1.5, service * 0.2 + inadequate * 0.3);
      reasons.push("heat_plus_inadequacy");
    }
    if (deprivation > 0 && directHits === 0) score -= 0.6;
  } else if (code === "G52") {
    const service = countMatches(text, rule.serviceTerms || []);
    const deprivation = countMatches(text, rule.deprivationTerms || []);
    const inadequate = countMatches(text, CODE_RULES.G53.inadequacyTerms || []);
    if (service > 0 && deprivation > 0) {
      score += 3 + Math.min(1.5, service * 0.2 + deprivation * 0.3);
      reasons.push("hot_water_plus_deprivation");
    }
    if (inadequate > 0 && directHits === 0) score -= 0.8;
  } else if (code === "G53") {
    const service = countMatches(text, rule.serviceTerms || []);
    const inadequate = countMatches(text, rule.inadequacyTerms || []);
    const deprivation = countMatches(text, CODE_RULES.G52.deprivationTerms || []);
    if (service > 0 && inadequate > 0) {
      score += 3 + Math.min(1.5, service * 0.2 + inadequate * 0.3);
      reasons.push("hot_water_plus_inadequacy");
    }
    if (deprivation > 0 && directHits === 0) score -= 0.6;
  } else if (code === "G54") {
    const genericHits = countMatches(text, rule.genericTerms || []);
    const specificInfestationHits =
      countMatches(text, CODE_RULES["G40.1"].genericTerms || []) +
      countMatches(text, CODE_RULES.G44.genericTerms || []) +
      countMatches(text, CODE_RULES.G76.genericTerms || []);
    if (genericHits >= 2) {
      score += 2.8;
      reasons.push(`generic_infestation_hits:${genericHits}`);
    } else if (genericHits >= 1 && text.includes("infestation")) {
      score += 2.1;
      reasons.push("generic_infestation_context");
    }
    if (specificInfestationHits > 0 && directHits === 0) score -= 1.2;
  } else {
    const genericHits = countMatches(text, rule.genericTerms || []);
    if (genericHits > 0) {
      score += Math.min(2.8, 2 + genericHits * 0.35);
      reasons.push(`generic_hits:${genericHits}`);
    }
  }

  if (score < 0) score = 0;
  if (score < rule.threshold) return null;

  return {
    code,
    score: Number(score.toFixed(3)),
    sectionLabel: chunk.sectionLabel || "",
    excerpt: excerpt(chunk.chunkText || ""),
    reasons
  };
}

async function loadIndexCatalog() {
  const raw = await fs.readFile(indexCatalogPath, "utf8");
  const match = raw.match(/export const canonicalIndexCodeOptions = (\[[\s\S]*?\]) as const;/);
  if (!match) {
    throw new Error(`Could not parse canonical index code catalog from ${indexCatalogPath}`);
  }
  return JSON.parse(match[1]);
}

export async function runSqlJson(dbPath, busyTimeoutMs, sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function buildRealDecisionPredicate(alias = "d") {
  return `
    ${alias}.file_type = 'decision_docx'
    AND (${alias}.citation IS NULL OR ${alias}.citation NOT LIKE 'BEE-%')
    AND (${alias}.citation IS NULL OR ${alias}.citation NOT LIKE 'KNOWN-REF-%')
    AND (${alias}.citation IS NULL OR ${alias}.citation NOT LIKE 'PILOT-%')
    AND (${alias}.citation IS NULL OR ${alias}.citation NOT LIKE 'HISTORICAL-%')
  `;
}

export async function loadDhsTargetCatalog() {
  const catalog = await loadIndexCatalog();
  const byCode = new Map(
    catalog
      .filter((option) => targetCodeSet.has(normalizeCode(option.code)))
      .map((option) => [String(option.code).toUpperCase(), option])
  );
  return byCode;
}

export async function loadExactCoverageCounts(dbPath, busyTimeoutMs) {
  const rows = await runSqlJson(
    dbPath,
    busyTimeoutMs,
    `
      SELECT
        upper(coalesce(l.canonical_value, l.normalized_value)) AS code,
        COUNT(DISTINCT d.id) AS docCount
      FROM document_reference_links l
      JOIN documents d ON d.id = l.document_id
      WHERE ${buildRealDecisionPredicate("d")}
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
        AND upper(coalesce(l.canonical_value, l.normalized_value)) IN (${TARGET_CODES.map(sqlQuote).join(", ")})
      GROUP BY upper(coalesce(l.canonical_value, l.normalized_value))
      ORDER BY code ASC
    `
  );

  const counts = new Map(TARGET_CODES.map((code) => [code, 0]));
  for (const row of rows) {
    const code = String(row.code || "").toUpperCase();
    if (counts.has(code)) counts.set(code, Number(row.docCount || 0));
  }
  return counts;
}

async function loadLegacy13Docs(dbPath, busyTimeoutMs, docLimit) {
  const limitClause = docLimit > 0 ? `LIMIT ${Number(docLimit)}` : "";
  return runSqlJson(
    dbPath,
    busyTimeoutMs,
    `
      SELECT
        d.id,
        d.citation,
        d.title,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.index_codes_json AS indexCodesJson
      FROM documents d
      WHERE ${buildRealDecisionPredicate("d")}
        AND EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'index_code'
            AND l.is_valid = 1
            AND l.normalized_value = '13'
        )
      ORDER BY COALESCE(d.decision_date, '') DESC, d.citation ASC
      ${limitClause}
    `
  );
}

async function loadIndexLinksByDocumentId(dbPath, busyTimeoutMs, documentIds) {
  const map = new Map();
  for (let index = 0; index < documentIds.length; index += sqlBatchSize) {
    const batch = documentIds.slice(index, index + sqlBatchSize);
    if (!batch.length) continue;
    const rows = await runSqlJson(
      dbPath,
      busyTimeoutMs,
      `
        SELECT
          document_id AS documentId,
          coalesce(canonical_value, raw_value, normalized_value) AS codeValue,
          normalized_value AS normalizedValue
        FROM document_reference_links
        WHERE reference_type = 'index_code'
          AND is_valid = 1
          AND document_id IN (${batch.map(sqlQuote).join(", ")})
      `
    );
    for (const row of rows) {
      const current = map.get(row.documentId) || [];
      current.push(normalizeWhitespace(row.codeValue || row.normalizedValue));
      map.set(row.documentId, current);
    }
  }
  return map;
}

async function loadChunksByDocumentId(dbPath, busyTimeoutMs, documentIds) {
  const map = new Map();
  for (let index = 0; index < documentIds.length; index += docBatchSize) {
    const batch = documentIds.slice(index, index + docBatchSize);
    if (!batch.length) continue;
    const idList = batch.map(sqlQuote).join(", ");

    const trustedRows = await runSqlJson(
      dbPath,
      busyTimeoutMs,
      `
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
    );

    const trustedDocIds = new Set(trustedRows.map((row) => row.documentId).filter(Boolean));
    const fallbackBatch = batch.filter((documentId) => !trustedDocIds.has(documentId));
    const fallbackRows =
      fallbackBatch.length > 0
        ? await runSqlJson(
            dbPath,
            busyTimeoutMs,
            `
              SELECT
                c.document_id AS documentId,
                c.id AS chunkId,
                c.section_label AS sectionLabel,
                c.chunk_text AS chunkText,
                0 AS trusted
              FROM document_chunks c
              WHERE c.document_id IN (${fallbackBatch.map(sqlQuote).join(", ")})
            `
          )
        : [];

    for (const row of [...trustedRows, ...fallbackRows]) {
      const current = map.get(row.documentId) || [];
      current.push({
        chunkId: row.chunkId,
        sectionLabel: row.sectionLabel || "",
        chunkText: row.chunkText || "",
        trusted: Number(row.trusted || 0) === 1
      });
      map.set(row.documentId, current);
    }
  }
  return map;
}

function detectDocumentCandidates(doc, currentCodes, chunks) {
  const currentNormalized = new Set(currentCodes.map(normalizeCode));
  const detections = [];

  for (const code of TARGET_CODES) {
    if (currentNormalized.has(normalizeCode(code))) continue;
    let best = null;
    for (const chunk of chunks) {
      const scored = scoreChunkForCode(code, chunk);
      if (!scored) continue;
      if (!best || scored.score > best.score) best = scored;
    }
    if (best) detections.push(best);
  }

  return detections.sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));
}

export async function scanDhsLegacyCandidates({
  dbPath = defaultDbPath,
  busyTimeoutMs = 5000,
  docLimit = Number(process.env.DHS_REMEDIATION_DOC_LIMIT || "0")
} = {}) {
  const catalogByCode = await loadDhsTargetCatalog();
  const exactCoverageCounts = await loadExactCoverageCounts(dbPath, busyTimeoutMs);
  const legacyDocs = await loadLegacy13Docs(dbPath, busyTimeoutMs, docLimit);
  const linkCodesByDocumentId = await loadIndexLinksByDocumentId(
    dbPath,
    busyTimeoutMs,
    legacyDocs.map((doc) => doc.id)
  );
  const chunksByDocumentId = await loadChunksByDocumentId(
    dbPath,
    busyTimeoutMs,
    legacyDocs.map((doc) => doc.id)
  );

  const candidates = [];
  const byCode = new Map(TARGET_CODES.map((code) => [code, 0]));

  for (const doc of legacyDocs) {
    const currentCodes = uniqueSorted(parseJsonArray(doc.indexCodesJson).map(normalizeWhitespace));
    const currentLinkCodes = uniqueSorted((linkCodesByDocumentId.get(doc.id) || []).map(normalizeWhitespace));
    const detections = detectDocumentCandidates(doc, [...currentCodes, ...currentLinkCodes], chunksByDocumentId.get(doc.id) || []);
    if (!detections.length) continue;

    for (const detection of detections) {
      byCode.set(detection.code, (byCode.get(detection.code) || 0) + 1);
    }

    candidates.push({
      id: doc.id,
      citation: doc.citation,
      title: doc.title,
      authorName: doc.authorName || "",
      decisionDate: doc.decisionDate || "",
      currentCodes,
      currentLinkCodes,
      detectedCodes: detections.map((item) => item.code),
      nextCodes: uniqueSorted([...currentCodes, ...currentLinkCodes, ...detections.map((item) => item.code)]),
      detections: detections.map((item) => ({
        code: item.code,
        score: item.score,
        sectionLabel: item.sectionLabel,
        excerpt: item.excerpt,
        reasons: item.reasons
      })),
      bestScore: detections[0]?.score || 0
    });
  }

  candidates.sort((a, b) => b.bestScore - a.bestScore || String(b.decisionDate || "").localeCompare(String(a.decisionDate || "")) || String(a.citation || "").localeCompare(String(b.citation || "")));

  return {
    generatedAt: new Date().toISOString(),
    dbPath,
    docLimit,
    targetCodes: TARGET_CODES.map((code) => ({
      code,
      description: catalogByCode.get(code)?.description || "",
      exactCoverageDocCount: exactCoverageCounts.get(code) || 0,
      candidateDocCount: byCode.get(code) || 0
    })),
    summary: {
      legacy13DocCount: legacyDocs.length,
      candidateDocCount: candidates.length,
      exactCoverageDocCount: Array.from(exactCoverageCounts.values()).reduce((sum, value) => sum + Number(value || 0), 0),
      byCode: TARGET_CODES.map((code) => ({
        code,
        exactCoverageDocCount: exactCoverageCounts.get(code) || 0,
        candidateDocCount: byCode.get(code) || 0
      })),
      byJudge: countBy(candidates.map((row) => row.authorName || "<unknown>"))
    },
    candidates
  };
}
