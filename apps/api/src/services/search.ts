import {
  canonicalIndexCodeOptions,
  searchDebugRequestSchema,
  searchDebugResponseSchema,
  searchRequestSchema,
  searchResponseSchema,
  type SearchDebugRequest,
  type SearchRequest
} from "@beedle/shared";
import type { Env } from "../lib/types";
import { embed } from "./embeddings";
import { canonicalizeJudgeName, inferJudgeFromTextFragments, judgeSearchTerms, normalizeJudgeLookupKey, queryReferencesJudge, sanitizeDisplayJudgeName } from "./judges";
import { effectiveSourceLink } from "./storage";
import { normalizeFilterValue } from "./legal-references";

interface ChunkRow {
  chunkId: string;
  documentId: string;
  title: string;
  citation: string;
  authorName: string | null;
  decisionDate: string | null;
  fileType: "decision_docx" | "law_pdf";
  sourceFileRef: string;
  sourceLink: string;
  sectionLabel: string;
  paragraphAnchor: string;
  citationAnchor: string;
  chunkText: string;
  createdAt: string;
  indexCodesJson: string;
  rulesSectionsJson: string;
  ordinanceSectionsJson: string;
  isTrustedTier: number;
  searchableAt?: string;
  orderRank?: number;
  lexicalRank?: number;
}

interface RankingDiagnostics {
  lexicalScore: number;
  vectorScore: number;
  exactPhraseBoost: number;
  citationBoost: number;
  metadataBoost: number;
  sectionBoost: number;
  partyNameBoost: number;
  judgeNameBoost: number;
  trustTierBoost: number;
  rerankScore: number;
  why: string[];
}

interface SearchContext {
  query: string;
  retrievalQuery: string;
  vectorQuery: string;
  queryType: SearchDebugRequest["queryType"];
  filters: SearchRequest["filters"];
  snippetMaxLength: number;
}

type SearchResultPassage = {
  chunkId: string;
  snippet: string;
  sectionLabel: string;
  sectionHeading: string;
  citationAnchor: string;
  paragraphAnchor: string;
  chunkType: string;
  score: number;
};

type SupportingFactDebug = {
  source: "matched_pool" | "fallback_findings_background_pool";
  factualAnchorScore: number;
  anchorHits: number;
  secondaryHits: number;
  coverageRatio: number;
};

// D1 can hit bind-variable limits sooner than stock SQLite in these UNION-heavy queries.
// Keep batches small so paged retrieval does not fail on broader searches.
const maxSqliteIdBatchSize = 30;
const maxScopedLexicalDocumentBatchSize = 4;
const maxKeywordCandidateDocumentBatchSize = 12;
let searchRuntimeIndexesEnsured = false;
let searchRuntimeIndexesPromise: Promise<void> | null = null;

function normalizeWhitespace(value: string): string {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function inferDocumentJudgeNames(rows: ChunkRow[]): Map<string, string> {
  const fragmentsByDocument = new Map<string, string[]>();
  for (const row of rows) {
    if (sanitizeDisplayJudgeName(row.authorName)) continue;
    const current = fragmentsByDocument.get(row.documentId) || [];
    current.push(row.chunkText || "");
    fragmentsByDocument.set(row.documentId, current);
  }

  const inferred = new Map<string, string>();
  for (const [documentId, fragments] of fragmentsByDocument.entries()) {
    const judgeName = inferJudgeFromTextFragments(fragments);
    const sanitized = sanitizeDisplayJudgeName(judgeName);
    if (sanitized) inferred.set(documentId, sanitized);
  }
  return inferred;
}

async function ensureSearchRuntimeIndexes(env: Env) {
  if (searchRuntimeIndexesEnsured) return;
  if (searchRuntimeIndexesPromise) return searchRuntimeIndexesPromise;

  searchRuntimeIndexesPromise = (async () => {
    const statements = [
      `CREATE INDEX IF NOT EXISTS idx_documents_author_name_lookup
        ON documents (lower(coalesce(author_name, '')), file_type, rejected_at, approved_at, decision_date, searchable_at)`,
      `CREATE INDEX IF NOT EXISTS idx_documents_search_runtime
        ON documents (file_type, rejected_at, approved_at, searchable_at, decision_date)`,
      `CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
        ON retrieval_search_chunks (document_id, active, batch_id)`
    ];

    for (const sql of statements) {
      try {
        await env.DB.prepare(sql).run();
      } catch (error) {
        if (!isRetryableSearchError(error)) throw error;
      }
    }
    searchRuntimeIndexesEnsured = true;
  })();

  try {
    await searchRuntimeIndexesPromise;
  } finally {
    searchRuntimeIndexesPromise = null;
  }
}

type IndexCodeFilterContext = {
  requestedCodes: string[];
  normalizedCodes: string[];
  legacyCodeAliases: string[];
  relatedRulesSections: string[];
  relatedOrdinanceSections: string[];
  searchPhrases: string[];
};

type IndexCodeFilterContextOptions = {
  includeGenericDhsFamilyAlias?: boolean;
};

const canonicalIndexCodeByNormalized = new Map(
  canonicalIndexCodeOptions.map((option) => [normalizeFilterValue("index_code", option.code), option] as const)
);

function isDhsFamilyIndexCodeOption(option: (typeof canonicalIndexCodeOptions)[number] | undefined) {
  if (!option) return false;
  const description = normalizeWhitespace(option.description || "");
  return /^DHS\s*--/i.test(description);
}

const MANUAL_INDEX_CODE_ALIAS_MAP: Record<string, { legacyCodes?: string[]; searchPhrases?: string[] }> = {
  g27: {
    legacyCodes: ["13"],
    searchPhrases: ["substantial decrease in housing services", "code violation substantial decrease in housing services"]
  },
  g28: {
    legacyCodes: ["13"],
    searchPhrases: [
      "not substantial decrease in housing services",
      "decrease in service not substantial",
      "not a substantial decrease in housing services",
      "does not constitute a substantial decrease in housing services",
      "did not constitute a substantial decrease in housing services"
    ]
  },
  g93: {
    searchPhrases: [
      "uniform hotel visitor policy",
      "visitor policy for residential hotel",
      "uniform visitor policy",
      "rent reduction for noncompliance with uniform policy",
      "supplemental visitor policy",
      "chapter 41d"
    ]
  }
};

function requestedJudgeFilters(filters: SearchRequest["filters"]): string[] {
  const raw = uniq([...(filters.judgeNames || []), filters.judgeName || ""].filter(Boolean));
  return raw
    .map((value) => canonicalizeJudgeName(value))
    .filter((value): value is string => Boolean(value));
}

function requestedIndexCodeFilters(filters: SearchRequest["filters"]): string[] {
  return uniq([...(filters.indexCodes || []), filters.indexCode || ""].filter(Boolean).map((value) => String(value).trim())).filter(Boolean);
}

function extractCatalogReferenceCitations(raw: string): string[] {
  const text = String(raw || "");
  const matches = new Set<string>();

  for (const match of text.matchAll(/\b\d+\.\d+[a-z]?(?:\([a-z0-9]+\))*[a-z]?\b/gi)) {
    matches.add(match[0]);
  }

  for (const match of text.matchAll(/§\s*([0-9]+(?:\.[0-9]+)?(?:\([a-z0-9]+\))*)/gi)) {
    if (match[1]) matches.add(match[1]);
  }

  return Array.from(matches);
}

function extractBaseCitation(value: string): string | null {
  const match = String(value || "").match(/^(\d+\.\d+[a-z]?)/i);
  return match?.[1] || null;
}

function buildCitationVariants(values: string[]): string[] {
  const variants = new Set<string>();
  for (const value of values) {
    const trimmed = String(value || "").trim();
    if (!trimmed) continue;
    variants.add(trimmed);
    const base = extractBaseCitation(trimmed);
    if (base) variants.add(base);
  }
  return Array.from(variants);
}

function buildIndexCodeDescriptionPhrases(description: string): string[] {
  const text = String(description || "").replace(/\s+/g, " ").trim();
  if (!text) return [];
  const phrases = new Set<string>();
  const addPhrase = (value: string) => {
    const phrase = String(value || "").replace(/\s+/g, " ").trim();
    if (!phrase || /\[reserved\]/i.test(phrase)) return;
    if (/^(code violation|dhs|other)$/i.test(phrase)) return;
    const wordCount = phrase.split(/\s+/).length;
    if (wordCount <= 0) return;
    if (wordCount === 1 && phrase.length < 20) return;
    if (wordCount === 2 && phrase.length < 12) return;
    phrases.add(phrase);
  };

  addPhrase(text);

  const doubleDashParts = text
    .split("--")
    .map((part) => part.trim())
    .filter(Boolean);
  for (const part of doubleDashParts) {
    addPhrase(part);
    for (const subPart of part.split(" - ").map((item) => item.trim()).filter(Boolean)) {
      addPhrase(subPart);
    }
  }

  return Array.from(phrases).slice(0, 4);
}

function buildIndexCodeFilterContext(
  filters: SearchRequest["filters"],
  options: IndexCodeFilterContextOptions = {}
): IndexCodeFilterContext {
  const includeGenericDhsFamilyAlias = options.includeGenericDhsFamilyAlias !== false;
  const requestedCodes = requestedIndexCodeFilters(filters);
  const normalizedCodes = uniq(requestedCodes.map((value) => normalizeFilterValue("index_code", value))).filter(Boolean);
  const legacyCodeAliases = new Set<string>();
  const relatedRulesSections = new Set<string>();
  const relatedOrdinanceSections = new Set<string>();
  const searchPhrases = new Set<string>();

  for (const normalizedCode of normalizedCodes) {
    const option = canonicalIndexCodeByNormalized.get(normalizedCode);
    const manualAliases = MANUAL_INDEX_CODE_ALIAS_MAP[normalizedCode];
    for (const legacyCode of manualAliases?.legacyCodes || []) legacyCodeAliases.add(String(legacyCode).trim());
    if (includeGenericDhsFamilyAlias && isDhsFamilyIndexCodeOption(option)) legacyCodeAliases.add("13");
    for (const phrase of manualAliases?.searchPhrases || []) searchPhrases.add(phrase);
    if (!option) continue;

    for (const ruleCitation of buildCitationVariants(extractCatalogReferenceCitations(option.rules || ""))) {
      relatedRulesSections.add(ruleCitation);
    }

    for (const ordinanceCitation of buildCitationVariants(extractCatalogReferenceCitations(option.ordinance || ""))) {
      relatedOrdinanceSections.add(ordinanceCitation);
    }

    for (const phrase of buildIndexCodeDescriptionPhrases(option.description || "")) {
      searchPhrases.add(phrase);
    }
  }

  return {
    requestedCodes,
    normalizedCodes,
    legacyCodeAliases: Array.from(legacyCodeAliases).filter(Boolean),
    relatedRulesSections: Array.from(relatedRulesSections),
    relatedOrdinanceSections: Array.from(relatedOrdinanceSections),
    searchPhrases: Array.from(searchPhrases).filter(Boolean)
  };
}

function isGenericDecisionQuery(query: string): boolean {
  return new Set(["decision", "decisions", "document", "documents", "case", "cases", "search"]).has(normalize(query));
}

function shouldUseSoftIndexCodeScope(query: string, filters: SearchRequest["filters"]): boolean {
  const requestedCodes = requestedIndexCodeFilters(filters);
  const trimmedQuery = String(query || "").trim();
  if (!requestedCodes.length || !trimmedQuery) return false;
  if (isGenericDecisionQuery(trimmedQuery)) return false;
  // Index-code filters should now apply at the document level by default.
  // Broadening happens later through recovery paths if the filtered universe underperforms.
  return false;
}

function shouldSkipVectorSearch(
  query: string,
  filters: SearchRequest["filters"],
  queryType: SearchContext["queryType"]
): boolean {
  if (queryType === "citation_lookup" || queryType === "party_name" || queryType === "rules_ordinance") {
    return false;
  }

  const tokenCount = tokenize(query).length;
  if (tokenCount === 0) return true;

  const normalizedQuery = normalize(query);
  const broadIssueTerms = [
    "rent reduction",
    "heat",
    "hot water",
    "mold",
    "owner move in",
    "owner move-in",
    "relative move in",
    "relative move-in",
    "harassment",
    "buyout",
    "habitability",
    "capital improvement",
    "cockroach",
    "rodent",
    "bed bug",
    "bed bugs",
    "infestation",
    "noise",
    "leak",
    "leaks",
    "water leak",
    "decrease in services"
  ];
  const vectorFirstIssueTerms = [
    "harassment",
    "buyout",
    "capital improvement"
  ];

  if (
    requiresOwnerMoveInFollowThroughSpecificity(query) ||
    isBuyoutPressureQuery(query) ||
    isSection8UnlawfulDetainerQuery(query) ||
    isCameraPrivacyQuery(query) ||
    isPackageSecurityQuery(query) ||
    isDogQuery(query) ||
    isIntercomQuery(query) ||
    isGarageSpaceQuery(query) ||
    isCommonAreasQuery(query) ||
    isStairsQuery(query) ||
    isCoLivingQuery(query) ||
    isHomeownersExemptionQuery(query) ||
    isCollegeQuery(query) ||
    isDivorceQuery(query)
  ) return false;
  if (tokenCount <= 2) return true;
  if (tokenCount <= 3 && vectorFirstIssueTerms.some((term) => normalizedQuery.includes(normalize(term)))) return false;
  if (inferIssueTerms(query).length > 0 && tokenCount <= 12) return true;
  if (tokenCount <= 3 && broadIssueTerms.some((term) => normalizedQuery.includes(normalize(term)))) return true;
  if (tokenCount <= 3 && shouldUseSoftIndexCodeScope(query, filters)) return true;
  return false;
}

function isVectorFirstIssueSearch(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bharassment|buyout|capital improvement\b/.test(normalized);
}

function enhanceQueryWithIndexCodeContext(query: string, filters: SearchRequest["filters"]): string {
  const trimmedQuery = String(query || "").trim();
  const indexContext = buildIndexCodeFilterContext(filters);
  if (!trimmedQuery || !indexContext.requestedCodes.length) return trimmedQuery;

  const contextualPhrases = uniq(indexContext.searchPhrases).filter(Boolean);
  const referenceHints = uniq([...indexContext.relatedRulesSections, ...indexContext.relatedOrdinanceSections]).filter(Boolean);

  if (isGenericDecisionQuery(trimmedQuery)) {
    const extras = uniq([...indexContext.requestedCodes, ...referenceHints, ...contextualPhrases]).slice(0, 8);
    return uniq([trimmedQuery, ...extras].filter(Boolean)).join(" ");
  }

  const normalizedQuery = normalize(trimmedQuery);
  const queryTokenCount = tokenize(trimmedQuery).length;
  const alreadyContainsContext = contextualPhrases
    .map((item) => normalize(item))
    .some((phrase) => phrase && normalizedQuery.includes(phrase));

  const allowShortFilteredExpansion =
    indexContext.requestedCodes.length >= 1 && indexContext.requestedCodes.length <= 3 && queryTokenCount <= 4 && !alreadyContainsContext;

  if (!allowShortFilteredExpansion) {
    return trimmedQuery;
  }

  const phraseExtras = contextualPhrases
    .filter((item) => {
      const normalizedPhrase = normalize(item);
      return normalizedPhrase && !normalizedQuery.includes(normalizedPhrase);
    })
    .slice(0, indexContext.requestedCodes.length === 1 ? 2 : 4);
  const referenceExtras = referenceHints
    .filter((item) => {
      const normalizedReference = normalize(item);
      return normalizedReference && !normalizedQuery.includes(normalizedReference);
    })
    .slice(0, 1);
  const extras = uniq([...phraseExtras, ...referenceExtras]).slice(0, 3);

  if (!extras.length) return trimmedQuery;

  return uniq([trimmedQuery, ...extras]).join(" ");
}

function chunkTypeMatchesFilter(sectionLabel: string, chunkTypeFilter?: string): boolean {
  const normalizedFilter = normalizeChunkTypeLabel(chunkTypeFilter || "");
  if (!normalizedFilter) return true;
  return normalizeChunkTypeLabel(sectionLabel || "") === normalizedFilter;
}

function normalize(input: string): string {
  return input
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function tokenize(input: string): string[] {
  return normalize(input)
    .split(/[^a-z0-9_:-]+/)
    .map((item) => item.trim())
    .filter((item) => item.length > 1);
}

const STOPWORD_TOKENS = new Set([
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "but",
  "by",
  "for",
  "from",
  "if",
  "in",
  "into",
  "is",
  "it",
  "no",
  "not",
  "of",
  "on",
  "or",
  "such",
  "that",
  "the",
  "their",
  "then",
  "there",
  "these",
  "they",
  "this",
  "to",
  "was",
  "will",
  "with"
]);

type CuratedKeywordFamily = {
  triggers: string[];
  expansions: string[];
};

const IRREGULAR_TOKEN_VARIANTS: Record<string, string[]> = {
  mouse: ["mice"],
  mice: ["mouse"],
  child: ["children", "kid", "kids"],
  children: ["child", "kid", "kids"],
  kid: ["kids", "child", "children"],
  kids: ["kid", "child", "children"],
  person: ["people"],
  people: ["person"],
  ant: ["ants"],
  ants: ["ant"],
  flea: ["fleas"],
  fleas: ["flea"],
  package: ["packages"],
  packages: ["package"],
  mailbox: ["mailboxes"],
  mailboxes: ["mailbox"],
  window: ["windows"],
  windows: ["window"],
  stair: ["stairs"],
  stairs: ["stair"],
  supply: ["supplies"],
  supplies: ["supply"],
  allergy: ["allergies"],
  allergies: ["allergy"]
};

const CURATED_KEYWORD_FAMILIES: CuratedKeywordFamily[] = [
  {
    triggers: ["infestation", "infestations", "pest", "pests"],
    expansions: [
      "infestation",
      "rodent",
      "mouse",
      "cockroach",
      "bed bug",
      "flea",
      "pest",
      "rat",
      "ant"
    ]
  },
  {
    triggers: ["rodent", "rodents", "rat", "rats", "mouse", "mice"],
    expansions: ["rodent", "mouse", "rat", "infestation", "pest"]
  },
  {
    triggers: ["cockroach", "cockroaches", "roach", "roaches"],
    expansions: ["cockroach", "infestation", "pest"]
  },
  {
    triggers: ["ant", "ants"],
    expansions: ["ant", "infestation", "pest"]
  },
  {
    triggers: ["flea", "fleas"],
    expansions: ["flea", "infestation", "pest"]
  },
  {
    triggers: ["bed bug", "bed bugs"],
    expansions: ["bed bug", "infestation", "pest"]
  },
  {
    triggers: ["mold", "mildew"],
    expansions: ["mold", "mildew"]
  },
  {
    triggers: ["heat", "heater", "space heater", "excessive heat"],
    expansions: ["heat", "heater", "heating", "boiler", "radiator", "space heater", "excessive heat"]
  },
  {
    triggers: ["internet", "cable"],
    expansions: ["internet", "cable", "wifi", "wi fi", "wi-fi"]
  },
  {
    triggers: ["common area", "common areas"],
    expansions: ["common area", "common areas"]
  },
  {
    triggers: ["common area", "common areas", "janitorial service"],
    expansions: ["common area", "common areas", "janitorial service", "unclean common areas", "clean common areas"]
  },
  {
    triggers: ["cleaning service", "cleaning services"],
    expansions: ["cleaning service", "cleaning services"]
  },
  {
    triggers: ["stairs", "stair", "handrail"],
    expansions: ["stairs", "stair", "loose stairs", "handrail", "back stairs", "stairwell"]
  },
  {
    triggers: ["porch", "landing"],
    expansions: ["porch", "front porch", "back porch", "landing", "storage room", "porch door"]
  },
  {
    triggers: ["window", "windows"],
    expansions: ["window", "windows", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows"]
  },
  {
    triggers: ["cleaning supply", "cleaning supplies"],
    expansions: ["cleaning supply", "cleaning supplies"]
  },
  {
    triggers: ["co living", "co-living"],
    expansions: ["co living", "co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "common areas", "shared kitchen"]
  },
  {
    triggers: ["coin operated", "coin-operated"],
    expansions: ["coin operated", "coin-operated"]
  },
  {
    triggers: ["garage space", "parking space", "garage parking"],
    expansions: ["garage space", "parking space", "garage parking", "carport parking", "tandem space"]
  },
  {
    triggers: ["self employed", "self-employed"],
    expansions: ["self employed", "self-employed"]
  },
  {
    triggers: ["on site resident manager", "on-site resident manager", "onsite resident manager"],
    expansions: ["on site resident manager", "on-site resident manager", "onsite resident manager"]
  },
  {
    triggers: ["homeowner's exemption", "homeowners exemption", "homeowner s exemption"],
    expansions: ["homeowner's exemption", "homeowners exemption", "homeowner s exemption"]
  },
  {
    triggers: ["director's hearing", "directors hearing", "director s hearing"],
    expansions: ["director's hearing", "directors hearing", "director s hearing"]
  },
  {
    triggers: ["lock box", "lockbox"],
    expansions: ["lock box", "lockbox"]
  },
  {
    triggers: ["service animal", "reasonable accommodation"],
    expansions: ["service animal", "reasonable accommodation", "emotional support animal", "support animal"]
  },
  {
    triggers: ["notice of violation", "written notice", "oral notice", "insufficient notice"],
    expansions: ["notice of violation", "written notice", "oral notice", "insufficient notice"]
  },
  {
    triggers: ["section 8", "hud"],
    expansions: ["section 8", "hud"]
  },
  {
    triggers: ["poop"],
    expansions: ["poop", "feces", "dog waste", "animal waste", "human feces", "sewage"]
  }
];

function phraseSurfaceVariants(value: string): string[] {
  const normalized = normalize(value || "");
  if (!normalized) return [];
  const variants = new Set<string>([normalized]);
  variants.add(normalized.replace(/['’]/g, ""));
  variants.add(normalized.replace(/['’]/g, " "));
  if (normalized.includes("-")) variants.add(normalized.replace(/-/g, " "));
  if (normalized.includes(" ")) variants.add(normalized.replace(/\s+/g, "-"));
  if (normalized.includes(" ")) variants.add(normalized.replace(/\s+/g, ""));
  return Array.from(variants)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

function tokenSurfaceVariants(token: string): string[] {
  const normalized = normalize(token || "");
  if (!normalized) return [];
  const variants = new Set<string>([normalized, ...(IRREGULAR_TOKEN_VARIANTS[normalized] || [])]);
  if (normalized.endsWith("ies") && normalized.length > 4) variants.add(`${normalized.slice(0, -3)}y`);
  if (normalized.endsWith("y") && normalized.length > 3) variants.add(`${normalized.slice(0, -1)}ies`);
  if (normalized.endsWith("es") && normalized.length > 4) variants.add(normalized.slice(0, -2));
  if (normalized.endsWith("s") && normalized.length > 3) variants.add(normalized.slice(0, -1));
  if (!normalized.endsWith("s")) variants.add(`${normalized}s`);
  if (!normalized.endsWith("es") && /(s|x|z|ch|sh)$/.test(normalized)) variants.add(`${normalized}es`);
  return Array.from(variants).filter(Boolean);
}

function keywordSurfaceVariants(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  const tokens = tokenize(normalized);
  const variants = new Set<string>(phraseSurfaceVariants(normalized));
  const singleToken = tokens.length === 1 ? tokens[0] : null;
  if (singleToken) {
    for (const variant of tokenSurfaceVariants(singleToken)) variants.add(variant);
  }
  return Array.from(variants).filter(Boolean);
}

function matchedCuratedKeywordFamilies(query: string): CuratedKeywordFamily[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  const matches = CURATED_KEYWORD_FAMILIES.filter((family) =>
    family.triggers.some((trigger) => phraseSurfaceVariants(trigger).some((variant) => containsWholeWord(normalized, variant)))
  );
  if (isAntInfestationQuery(normalized)) {
    return matches.filter((family) => family.triggers.some((trigger) => /\b(?:ant|ants)\b/.test(normalize(trigger))));
  }
  return matches;
}

function curatedKeywordExpansionTerms(query: string): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query)) {
    for (const expansion of family.expansions) {
      for (const variant of phraseSurfaceVariants(expansion)) expansions.add(variant);
      const normalizedExpansion = normalize(expansion || "");
      const tokens = tokenize(normalizedExpansion);
      if (tokens.length === 1 && tokens[0]) {
        for (const variant of tokenSurfaceVariants(tokens[0])) expansions.add(variant);
      }
    }
  }
  return Array.from(expansions).filter(Boolean);
}

function curatedKeywordLexicalExpansionTerms(query: string): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query)) {
    for (const expansion of family.expansions) {
      const normalizedExpansion = normalize(expansion || "");
      if (!normalizedExpansion) continue;
      expansions.add(normalizedExpansion);
      const tokens = tokenize(normalizedExpansion);
      if (tokens.length === 1 && tokens[0]) {
        for (const variant of tokenSurfaceVariants(tokens[0])) expansions.add(variant);
      } else if (normalizedExpansion.includes("-")) {
        expansions.add(normalizedExpansion.replace(/-/g, " "));
      } else if (normalizedExpansion.includes(" ")) {
        expansions.add(normalizedExpansion.replace(/\s+/g, "-"));
      }
    }
  }
  return Array.from(expansions).filter(Boolean);
}

function curatedKeywordWholeWordExpansionTerms(query: string): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query)) {
    for (const expansion of family.expansions) {
      const normalizedExpansion = normalizeWhitespace(normalize(expansion || ""));
      if (normalizedExpansion) expansions.add(normalizedExpansion);
    }
  }
  return Array.from(expansions).filter(Boolean);
}

function keywordBoundaryGuardTerms(query: string): string[] {
  const tokens = tokenize(query || "");
  if (tokens.length === 0 || tokens.length > 4) return [];
  const curatedExpansions = curatedKeywordExpansionTerms(query);
  if (curatedExpansions.length > 0) {
    return uniq([...keywordSurfaceVariants(query), ...curatedExpansions]).slice(0, 32);
  }
  if (tokens.length === 1) {
    return keywordSurfaceVariants(query).slice(0, 12);
  }
  const normalized = normalize(query || "");
  if (/[-'’]/.test(String(query || "")) || /(self employed|co living|coin operated|garage space|parking space|garage parking|on ?site resident manager|homeowner.?s exemption|director.?s hearing|lock box)/.test(normalized)) {
    return keywordSurfaceVariants(query).slice(0, 12);
  }
  return [];
}

function meaningfulLexicalTokens(query: string): string[] {
  const tokens = uniq(tokenize(query)).filter((token) => token.length >= 2 && !STOPWORD_TOKENS.has(token));
  const hasLongToken = tokens.some((token) => token.length >= 4 && !/^\d+$/.test(token));
  return tokens
    .filter((token) => token.length >= 4 || !hasLongToken || /\d/.test(token))
    .slice(0, 8);
}

function meaningfulPhraseTokens(query: string): string[] {
  return uniq(tokenize(query))
    .filter((token) => token.length >= 3 && !STOPWORD_TOKENS.has(token))
    .slice(0, 8);
}

function sentencePhraseOverlapScore(query: string, text: string): number {
  const queryTokens = tokenize(query).filter((token) => token.length > 2 && !STOPWORD_TOKENS.has(token));
  if (queryTokens.length < 5) return 0;

  const textTokenSet = new Set(tokenize(text).filter((token) => token.length > 2 && !STOPWORD_TOKENS.has(token)));
  if (!textTokenSet.size) return 0;

  let longestRun = 0;
  let currentRun = 0;
  for (const token of queryTokens) {
    if (textTokenSet.has(token)) {
      currentRun += 1;
      longestRun = Math.max(longestRun, currentRun);
    } else {
      currentRun = 0;
    }
  }

  if (longestRun >= 8) return 0.12;
  if (longestRun >= 6) return 0.08;
  if (longestRun >= 5) return 0.05;
  return 0;
}

function exactMultiWordPhraseScore(query: string, text: string): number {
  const tokens = meaningfulPhraseTokens(query);
  if (tokens.length < 2) return 0;
  const normalizedText = normalize(text).replace(/[^a-z0-9]+/g, " ");
  const normalizedPhrase = tokens.join(" ");
  if (!normalizedText || !normalizedPhrase) return 0;
  if (normalizedText.includes(normalizedPhrase)) return 0.2;
  return 0;
}

function isMarketConditionReasoningQuery(context: SearchContext): boolean {
  const normalized = normalize(context.query || "");
  if (!normalized) return false;
  if (!isSentenceStyleReasoningQuery(context)) return false;
  return (
    normalized.includes("market conditions") ||
    normalized.includes("new agreement") ||
    normalized.includes("new base rent") ||
    normalized.includes("anniversary date")
  );
}

function marketConditionReasoningScore(query: string, text: string): number {
  const normalizedQuery = normalize(query);
  const normalizedText = normalize(text);
  let hits = 0;

  const signals = ["market conditions", "new agreement", "new base rent", "anniversary date"];
  for (const signal of signals) {
    if (normalizedQuery.includes(signal) && normalizedText.includes(signal)) {
      hits += 1;
    }
  }

  if (hits >= 3) return 0.12;
  if (hits === 2) return 0.08;
  if (hits === 1) return 0.04;
  return 0;
}

function metadataTerms(row: ChunkRow): string[] {
  return [
    row.title,
    row.citation,
    row.authorName || "",
    row.sectionLabel,
    ...judgeSearchTerms(row.authorName),
    ...parseJsonList(row.indexCodesJson),
    ...parseJsonList(row.rulesSectionsJson),
    ...parseJsonList(row.ordinanceSectionsJson)
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
}

function combinedSearchableText(row: ChunkRow): string {
  return [row.chunkText, ...metadataTerms(row)].join(" ");
}

function isShortAlphabeticQuery(query: string): boolean {
  const trimmed = normalize(query);
  return /^[a-z]{1,2}$/.test(trimmed);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isInfestationAliasQuery(query: string): boolean {
  return /\binfestation|infestations\b/.test(normalize(query));
}

function isAntQuery(query: string): boolean {
  return /\b(?:ant|ants)\b/.test(normalize(query));
}

function isAntInfestationQuery(query: string): boolean {
  const normalized = normalize(query);
  return isAntQuery(normalized) && isInfestationAliasQuery(normalized);
}

function isKeywordFamilyRecallQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    isLiteralKeywordQuery(normalized) ||
    isInfestationAliasQuery(normalized) ||
    matchedCuratedKeywordFamilies(normalized).length > 0
  );
}

function literalKeywordTokens(query: string): string[] {
  const rawTokens = tokenize(query);
  const lexicalTokens = meaningfulLexicalTokens(query);
  const normalized = normalize(query);
  if (rawTokens.length !== 1 || lexicalTokens.length !== 1) return [];
  if (isInfestationAliasQuery(normalized)) return [];
  if (containsWholeWord(normalized, "omi") || containsWholeWord(normalized, "awe")) return [];
  const token = lexicalTokens[0];
  if (!token || token.length < 3) return [];
  return [token];
}

function isLiteralKeywordQuery(query: string): boolean {
  return literalKeywordTokens(query).length > 0;
}

function keywordCandidateTerms(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];

  const curated = curatedKeywordWholeWordExpansionTerms(normalized);
  if (curated.length > 0) {
    return uniq([normalized, ...curated]).filter(Boolean).slice(0, 6);
  }

  const literal = literalKeywordTokens(normalized);
  if (literal.length > 0) return literal;

  return meaningfulLexicalTokens(normalized).slice(0, 4);
}

function keywordExecutionTerms(query: string): string[] {
  return keywordCandidateTerms(query).slice(0, 5);
}

function rowHasLiteralKeywordMatch(row: ChunkRow, query: string): boolean {
  const tokens = literalKeywordTokens(query);
  if (!tokens.length) return false;
  const text = normalize(combinedSearchableText(row));
  return tokens.every((token) => new RegExp(`(^|[^a-z0-9])${escapeRegex(token)}([^a-z0-9]|$)`, "i").test(text));
}

function rowMatchesQueryGuard(row: ChunkRow, query: string): boolean {
  const searchableText = combinedSearchableText(row);
  if (isAntInfestationQuery(query)) {
    return containsWholeWord(searchableText, "ant") || containsWholeWord(searchableText, "ants") || containsWholeWord(searchableText, "ant infestation");
  }
  if (isHomeownersExemptionQuery(query)) {
    return hasHomeownersExemptionContext(searchableText);
  }
  const boundaryGuardTerms = keywordBoundaryGuardTerms(query);
  if (boundaryGuardTerms.length > 0) {
    return boundaryGuardTerms.some((term) => containsWholeWord(searchableText, term));
  }
  if (isLiteralKeywordQuery(query)) {
    return rowHasLiteralKeywordMatch(row, query);
  }
  if (!isShortAlphabeticQuery(query)) return true;
  const trimmed = normalize(query);
  if (!trimmed) return true;
  const regex = new RegExp(`(^|[^a-z0-9])${escapeRegex(trimmed)}([^a-z0-9]|$)`, "i");
  return regex.test(combinedSearchableText(row));
}

function lexicalTerms(query: string): string[] {
  const full = String(query || "").slice(0, 260).trim();
  const normalizedFull = normalize(full);
  const tokens = meaningfulLexicalTokens(full);
  const surfaceVariants = keywordSurfaceVariants(full);
  const curatedExpansions = curatedKeywordLexicalExpansionTerms(full);
  const curatedKeywordFamilyQuery = matchedCuratedKeywordFamilies(full).length > 0;
  const prioritizedCuratedExpansions = curatedKeywordFamilyQuery
    ? uniq([
        ...curatedExpansions.filter((term) => tokenize(term).length === 1),
        ...curatedExpansions.filter((term) => tokenize(term).length > 1)
      ])
    : curatedExpansions;
  const inferredIssueTerms = inferIssueTerms(full);
  const issueTerms = inferredIssueTerms
    .flatMap((term) => meaningfulLexicalTokens(term))
    .slice(0, 6);
  const judgeTerms = queryReferencesJudge(full).flatMap((judge) => [judge, normalizeJudgeLookupKey(judge)]);
  const fullTokens = tokenize(full);
  const fullIsStopwordOnly = fullTokens.length > 0 && tokens.length === 0 && fullTokens.every((token) => STOPWORD_TOKENS.has(token));
  if (isOwnerMoveInIssueSearch(full)) {
    const ownerMoveTerms = uniq([
      fullIsStopwordOnly ? "" : full,
      "owner move-in",
      "owner move in",
      "relative move-in",
      "relative move in",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "owner occupancy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupy the unit" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupied the unit" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupancy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupied" : "",
      /\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalizedFull) ? "reside" : "",
      /\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalizedFull) ? "resided" : "",
      /\bnotice\b/.test(normalizedFull) ? "notice" : "",
      containsWholeWord(normalizedFull, "omi") ? "omi" : ""
    ].filter(Boolean));
    return ownerMoveTerms.slice(0, fullTokens.length <= 3 ? 4 : 6);
  }
  if (hasWrongfulEvictionPhrase(full)) {
    const wrongfulEvictionTerms = uniq([
      fullIsStopwordOnly ? "" : full,
      "wrongful eviction",
      "report of alleged wrongful eviction",
      "unlawful eviction",
      /\block(?:ed)? out|lockout\b/.test(normalizedFull) ? "lockout" : "",
      /\block(?:ed)? out|lockout\b/.test(normalizedFull) ? "locked out" : "",
      /\bself[-\s]?help\b/.test(normalizedFull) ? "self-help eviction" : "",
      /\bself[-\s]?help\b/.test(normalizedFull) ? "self help eviction" : "",
      /\brepair|repairs\b/.test(normalizedFull) ? "repair" : "",
      /\brepair|repairs\b/.test(normalizedFull) ? "repairs" : "",
      /\bcomplain(?:ed|ing)?\b/.test(normalizedFull) ? "complaining" : "",
      containsWholeWord(normalizedFull, "awe") ? "awe" : ""
    ].filter(Boolean));
    return wrongfulEvictionTerms.slice(0, fullTokens.length <= 3 ? 4 : 7);
  }
  const broadIssueQuery = fullTokens.length <= 12 && inferIssueTerms(full).length > 0;
  const shortBroadIssueQuery = broadIssueQuery && fullTokens.length <= 3;
  if (broadIssueQuery && !shortBroadIssueQuery) {
    const presentIssueTerms = inferredIssueTerms
      .filter((term) => normalizedFull.includes(normalize(term)))
      .flatMap((term) => meaningfulLexicalTokens(term))
      .slice(0, 3);
    const remainingTokens = tokens.filter((token) => !presentIssueTerms.includes(token));
    return uniq(
      [fullIsStopwordOnly ? "" : full, ...surfaceVariants, ...prioritizedCuratedExpansions, ...presentIssueTerms, ...remainingTokens, ...judgeTerms].filter(Boolean)
    ).slice(0, curatedKeywordFamilyQuery ? 10 : 10);
  }
  const terms = uniq([fullIsStopwordOnly ? "" : full, ...surfaceVariants, ...prioritizedCuratedExpansions, ...issueTerms, ...tokens, ...judgeTerms].filter(Boolean));
  const limit = curatedKeywordFamilyQuery ? 10 : shortBroadIssueQuery ? 6 : broadIssueQuery ? 8 : 8;
  return terms.slice(0, limit);
}

function wholeWordLexicalTerms(query: string): string[] {
  const full = String(query || "").slice(0, 260).trim();
  const surfaceVariants = keywordSurfaceVariants(full);
  const curatedExpansions = curatedKeywordWholeWordExpansionTerms(full);
  return uniq([full, ...surfaceVariants, ...curatedExpansions].filter(Boolean)).slice(0, 5);
}

function expandQueryForRetrieval(query: string): string {
  const q = normalize(query);
  if (!q) return query;

  const additions: string[] = [];
  const add = (...values: string[]) => additions.push(...values);
  const hasOmiAcronym = containsWholeWord(q, "omi");
  const hasAweAcronym = containsWholeWord(q, "awe");

  if (/\bheat|heating|heater|boiler|radiator\b/.test(q)) {
    add("heat", "heating", "heater", "boiler", "radiator", "hot water");
  }
  if (/\bcool|cooling|ventilation|air\b/.test(q)) {
    add("cooling", "ventilation", "air flow", "air circulation", "overheating", "temperature control");
  }
  if (/\bnotice|service|served|mail\b/.test(q)) {
    add("notice", "service", "served", "mailing", "posting", "repair request", "work order", "written notice");
  }
  if (/\brepair|maintenance|condition|habitability\b/.test(q)) {
    add("repair", "maintenance", "habitability", "condition", "defect");
  }
  if (/\bbuyout\b/.test(q)) {
    add("buyout", "buyout agreement", "buyout negotiations", "disclosure", "rescission", "settlement");
    if (/\b(?:pressure|pressured|pressuring|harass|harassing|harassment|coerce|coerced|coercion|coercive|threat|threaten|threatened)\b/.test(q)) {
      add(
        "pressure",
        "pressured",
        "pressuring",
        "harass",
        "harassing",
        "harassment",
        "coerce",
        "coerced",
        "coercion",
        "coercive",
        "buyout coercion",
        "coercive buyout",
        "payment to vacate",
        "payments to vacate",
        "offer to vacate",
        "offers to vacate",
        "offer of payment to vacate",
        "offers of payment to vacate",
        "threats or intimidation",
        "fraud intimidation or coercion",
        "threat",
        "threaten",
        "threatened"
      );
    }
  }
  if (isInfestationAliasQuery(q)) {
    if (isAntInfestationQuery(q)) {
      add("ant infestation", "ants", "ant", "pest", "pests");
    } else {
      add(
        "infestation",
        "infestations",
        "rodent infestation",
        "cockroach infestation",
        "bed bug infestation",
        "rodent",
        "rodents",
        "cockroach",
        "cockroaches",
        "roach",
        "roaches",
        "bed bug",
        "bed bugs",
        "mouse",
        "mice",
        "rat",
        "rats",
        "pest",
        "pests"
      );
    }
  }
  if (hasOwnerMoveInPhrase(q) || hasOmiAcronym || /\bowner occupancy\b/.test(q)) {
    add(
      "owner move-in",
      "owner move in",
      "relative move-in",
      "relative move in",
      "recover possession",
      "owner occupancy",
      "recover possession for owner",
      "occupy the unit",
      "occupied the unit",
      "principal place of residence",
      "tenant in occupancy",
      "actually resides in a rental unit"
    );
    if (requiresOwnerMoveInFollowThroughSpecificity(q)) {
      add("never moved in", "did not move in", "never occupied", "did not occupy", "never resided", "did not reside");
    }
    if (hasOmiAcronym) add("omi");
  }
  if (/\bnuisance\b/.test(q)) {
    add("nuisance", "substantial nuisance", "tenant conduct", "disturbance");
  }
  if (hasWrongfulEvictionPhrase(q) || hasAweAcronym) {
    add(
      "wrongful eviction",
      "report of alleged wrongful eviction",
      "eviction",
      "unlawful eviction",
      "wrongfully evicted",
      "lockout",
      "locked out",
      "self-help eviction"
    );
    if (hasAweAcronym) add("awe");
  }
  if (/\brent reduction\b/.test(q)) {
    add("rent reduction", "decrease in services", "reduction in housing services", "corresponding rent reduction");
  }
  if (/\bharassment|retaliation\b/.test(q)) {
    add("harassment", "retaliation", "tenant harassment", "landlord conduct", "37.10b", "wrongful endeavor");
  }
  if (isSection8UnlawfulDetainerQuery(q)) {
    add(
      "section 8 eviction",
      "section 8 eviction action",
      "section 8 notice to quit",
      "voucher eviction",
      "housing choice voucher eviction",
      "housing choice voucher program",
      "unlawful detainer complaint",
      "housing choice voucher program unlawful detainer complaint",
      "eviction action",
      "eviction"
    );
  }
  if (isCameraPrivacyQuery(q)) {
    add("camera privacy", "security camera privacy", "surveillance camera privacy", "security cameras", "surveillance", "invasion of privacy");
  }
  if (isPackageSecurityQuery(q)) {
    add("package security", "package theft", "stolen packages", "package safety", "mail theft", "mailroom security");
  }
  if (isDogQuery(q)) {
    add("dog", "dogs", "dog-free building", "no pets", "pet policy", "dog park", "service animal");
  }
  if (isIntercomQuery(q)) {
    add("intercom", "broken intercom", "door buzzer", "entry system", "security gate", "buzz in");
  }
  if (isGarageSpaceQuery(q)) {
    add("garage space", "parking space", "garage parking", "carport parking", "tandem space", "parking housing service");
  }
  if (isCommonAreasQuery(q)) {
    add("common areas", "common area", "janitorial service", "unclean common areas", "clean common areas", "common-area cleanliness");
  }
  if (isStairsQuery(q)) {
    add("stairs", "loose stairs", "handrail", "back stairs", "stairwell", "fall hazard");
  }
  if (isPorchQuery(q)) {
    add("porch", "front porch", "back porch", "landing", "storage room", "porch door");
  }
  if (isWindowsQuery(q)) {
    add("windows", "window", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows");
  }
  if (isCollegeQuery(q)) {
    add("college", "attend college", "student housing", "school breaks", "temporary absence", "return to live in the unit");
  }
  if (isSelfEmployedQuery(q)) {
    add("self employed", "self-employed", "1099", "schedule c", "tax returns", "principal residence");
  }
  if (isAdjudicatedQuery(q)) {
    add("adjudicated", "adjudicate", "already decided", "previously decided", "precluded", "state court");
  }
  if (isSocialMediaQuery(q)) {
    add("social media", "facebook", "instagram", "nextdoor", "facebook marketplace", "posted on social media");
  }
  if (isCaregiverQuery(q)) {
    add("caregiver", "caregiving", "primary caregiver", "care for", "return to live in the unit", "principal residence");
  }
  if (isPoopQuery(q)) {
    add("poop", "feces", "faeces", "dog waste", "animal waste", "human feces", "sewage");
  }
  if (isMootQuery(q)) {
    add("moot", "rendered moot", "null and void", "rescinded", "administratively dismissed", "withdrawn");
  }
  if (isRemoteWorkQuery(q)) {
    add("remote work", "work from home", "working from home", "unable to work from home", "construction noise", "utility outage");
  }
  if (isDivorceQuery(q)) {
    add("divorce", "divorced", "separated", "separation", "spouse moved out", "marital issues", "live separately");
  }
  if (isCoLivingQuery(q)) {
    add("co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "common areas");
  }

  const expanded = uniq([query.trim(), ...additions].filter(Boolean)).join(" ");
  return expanded || query;
}

function chooseVectorQuery(originalQuery: string): string {
  if (isCameraPrivacyQuery(originalQuery)) {
    return "camera privacy security camera surveillance invasion of privacy video monitoring";
  }
  if (isPackageSecurityQuery(originalQuery)) {
    return "package security package theft stolen packages mail theft mailroom security delivery security secure package delivery";
  }
  if (isDogQuery(originalQuery)) {
    return "dog dogs dog-free building no pets pet policy service animal emotional support animal dog park housing service";
  }
  if (isIntercomQuery(originalQuery)) {
    return "intercom broken intercom door buzzer entry system security gate housing service";
  }
  if (isGarageSpaceQuery(originalQuery)) {
    return "garage space parking space garage parking carport parking tandem space housing service";
  }
  if (isCommonAreasQuery(originalQuery)) {
    return "common areas common area janitorial service clean common areas housing service";
  }
  if (isStairsQuery(originalQuery)) {
    return "stairs loose stairs handrail back stairs stairwell fall hazard housing service";
  }
  if (isPorchQuery(originalQuery)) {
    return "porch front porch back porch landing porch door storage room housing service hazard leak";
  }
  if (isWindowsQuery(originalQuery)) {
    return "windows inoperable windows broken windows window latch window sash operable windows housing service draft leak";
  }
  if (isCollegeQuery(originalQuery)) {
    return "college attend college student housing school breaks temporary absence return to live in the unit permanent residence";
  }
  if (isSelfEmployedQuery(originalQuery)) {
    return "self employed self-employed 1099 schedule c tax returns principal residence address";
  }
  if (isAdjudicatedQuery(originalQuery)) {
    return "adjudicated adjudicate already decided previously decided precluded preclusion state court";
  }
  if (isSocialMediaQuery(originalQuery)) {
    return "social media facebook instagram nextdoor facebook marketplace posted online residency occupancy roommate search";
  }
  if (isCaregiverQuery(originalQuery)) {
    return "caregiver caregiving primary caregiver care for principal residence return to live in the unit family assistance";
  }
  if (isPoopQuery(originalQuery)) {
    return "poop feces faeces dog waste animal waste human feces sewage sanitation contamination";
  }
  if (isMootQuery(originalQuery)) {
    return "moot rendered moot null and void rescinded administratively dismissed withdrawn";
  }
  if (isRemoteWorkQuery(originalQuery)) {
    return "remote work work from home working from home unable to work from home construction noise utility outage peaceful enjoyment";
  }
  if (isDivorceQuery(originalQuery)) {
    return "divorce divorced separated separation spouse moved out marital issues live separately residence occupancy";
  }
  if (isCoLivingQuery(originalQuery)) {
    return "co-living separate tenancy individual room separate rental agreements common areas shared kitchen";
  }
  return String(originalQuery || "").trim();
}

function inferIssueTerms(query: string): string[] {
  const q = normalize(query);
  if (!q) return [];
  const out: string[] = [];
  const add = (...values: string[]) => out.push(...values);
  const hasOmiAcronym = containsWholeWord(q, "omi");
  const hasAweAcronym = containsWholeWord(q, "awe");

  if (/\bheat|heating|heater|boiler|radiator|hot water\b/.test(q)) {
    add("heat", "heating", "heater", "boiler", "radiator", "hot water");
  }
  if (/\bcool|cooling|ventilation|air flow|air circulation|overheating|temperature control\b/.test(q)) {
    add("cooling", "ventilation", "air flow", "air circulation", "overheating", "temperature control");
  }
  if (/\brepair|maintenance|habitability|condition|defect\b/.test(q)) {
    add("repair", "maintenance", "habitability", "condition", "defect");
  }
  if (/\bmold|leak|water intrusion|plumbing|sewage\b/.test(q)) {
    add("mold", "leak", "water intrusion", "plumbing", "sewage");
  }
  if (isInfestationAliasQuery(q)) {
    if (isAntInfestationQuery(q)) {
      add("ant infestation", "ants", "ant", "pest", "pests");
    } else {
      add(
        "infestation",
        "infestations",
        "rodent infestation",
        "cockroach infestation",
        "bed bug infestation",
        "rodent",
        "rodents",
        "cockroach",
        "cockroaches",
        "bed bug",
        "bed bugs",
        "mouse",
        "mice",
        "rat",
        "rats",
        "pest",
        "pests"
      );
    }
  }
  if (hasOwnerMoveInPhrase(q) || hasOmiAcronym || /\bowner occupancy\b/.test(q)) {
    add("owner move-in", "relative move-in", "recover possession", "owner occupancy", "occupy the unit", "occupied the unit", "principal place of residence", "tenant in occupancy", "actually resides in a rental unit");
    if (requiresOwnerMoveInFollowThroughSpecificity(q)) {
      add("never moved in", "did not move in", "never occupied", "did not occupy", "never resided", "did not reside");
    }
    if (hasOmiAcronym) add("omi");
  }
  if (hasWrongfulEvictionPhrase(q) || hasAweAcronym) {
    add("wrongful eviction", "report of alleged wrongful eviction", "unlawful eviction", "lockout", "locked out", "eviction");
    if (hasAweAcronym) add("awe");
  }
  if (/\bharassment|retaliation\b/.test(q)) {
    add("harassment", "harass", "harassed", "harassing", "retaliation", "tenant harassment", "landlord conduct");
  }
  if (isAccommodationQuery(q)) {
    add(
      "reasonable accommodation",
      "accommodation request",
      "service animal",
      "support animal",
      "emotional support animal",
      "assistance animal",
      "disability accommodation"
    );
  }
  if (isHomeownersExemptionQuery(q)) {
    add(
      "homeowner's exemption",
      "homeowners exemption",
      "homeowner s exemption",
      "property tax exemption",
      "principal place of residence",
      "principal residence"
    );
  }
  if (isCameraPrivacyQuery(q)) {
    add("camera privacy", "security camera", "security cameras", "surveillance", "invasion of privacy", "video monitoring");
  }
  if (isPackageSecurityQuery(q)) {
    add("package security", "package theft", "stolen packages", "mail theft", "mailroom security", "delivery security");
  }
  if (isDogQuery(q)) {
    add("dog", "dogs", "dog-free building", "pet policy", "no pets", "service animal", "dog park", "housing service");
  }
  if (isIntercomQuery(q)) {
    add("intercom", "broken intercom", "door buzzer", "entry system", "security gate", "buzz in", "housing service");
  }
  if (isGarageSpaceQuery(q)) {
    add("garage space", "parking space", "garage parking", "carport parking", "tandem space", "parking", "housing service");
  }
  if (isCommonAreasQuery(q)) {
    add("common areas", "common area", "janitorial service", "unclean common areas", "clean common areas", "hallways", "housing service");
  }
  if (isStairsQuery(q)) {
    add("stairs", "loose stairs", "handrail", "back stairs", "stairwell", "fall hazard", "housing service");
  }
  if (isPorchQuery(q)) {
    add("porch", "front porch", "back porch", "landing", "storage room", "porch door", "housing service");
  }
  if (isWindowsQuery(q)) {
    add("windows", "window", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows", "housing service");
  }
  if (isCollegeQuery(q)) {
    add("college", "school", "student housing", "school breaks", "temporary absence", "attend college", "permanent residence", "return");
  }
  if (isSelfEmployedQuery(q)) {
    add("self employed", "self-employed", "1099", "schedule c", "tax return", "principal residence", "subject unit address");
  }
  if (isAdjudicatedQuery(q)) {
    add("adjudicated", "adjudicate", "already decided", "previously decided", "precluded", "preclusion", "state court");
  }
  if (isSocialMediaQuery(q)) {
    add("social media", "facebook", "instagram", "nextdoor", "facebook marketplace", "posted online", "roommate search", "principal residence");
  }
  if (isCaregiverQuery(q)) {
    add("caregiver", "caregiving", "primary caregiver", "care for", "care for mother", "care for father", "principal residence", "return");
  }
  if (isPoopQuery(q)) {
    add("poop", "feces", "faeces", "dog waste", "animal waste", "human feces", "sewage", "contamination");
  }
  if (isMootQuery(q)) {
    add("moot", "rendered moot", "null and void", "rescinded", "administratively dismissed", "withdrawn");
  }
  if (isRemoteWorkQuery(q)) {
    add("remote work", "work from home", "working from home", "unable to work from home", "construction noise", "utility outage", "peaceful enjoyment");
  }
  if (isDivorceQuery(q)) {
    add("divorce", "divorced", "separated", "separation", "spouse moved out", "marital issues", "live separately", "residence");
  }
  if (isCoLivingQuery(q)) {
    add("co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "shared kitchen", "common areas");
  }
  if (isSection8Query(q)) {
    add("section 8", "hud", "voucher", "housing choice voucher", "subsidized tenancy", "subsidized tenant");
  }
  if (isUnlawfulDetainerQuery(q)) {
    add("unlawful detainer", "notice to quit", "three day notice", "detainer action", "eviction lawsuit", "eviction action", "eviction");
  }
  if (isSection8UnlawfulDetainerQuery(q)) {
    add("section 8 eviction", "section 8 eviction action", "voucher eviction", "housing choice voucher eviction");
  }

  return uniq(out);
}

function primaryIssueSignals(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  const signals = new Set<string>();

  if (/\bmold\b/.test(normalized)) signals.add("mold");
  if (/\bheat|heating|heater|boiler|radiator\b/.test(normalized)) signals.add("heat");
  if (/\bhot water\b/.test(normalized)) signals.add("hot water");
  if (/\brodent\b/.test(normalized)) signals.add("rodent");
  if (/\bcockroach\b/.test(normalized)) signals.add("cockroach");
  if (/\bbed bug|bed bugs\b/.test(normalized)) signals.add("bed bug");
  if (isInfestationAliasQuery(normalized)) signals.add("infestation");
  if (hasOwnerMoveInPhrase(normalized) || containsWholeWord(normalized, "omi") || /\bowner occupancy\b/.test(normalized)) signals.add("owner move in");
  if (hasWrongfulEvictionPhrase(normalized) || containsWholeWord(normalized, "awe")) signals.add("wrongful eviction");
  if (/\block(?:ed)? out|lockout|changed locks?|denied access|self[-\s]?help eviction|shut off utilities\b/.test(normalized)) {
    signals.add("lockout");
  }
  if (/\bharassment|retaliation\b/.test(normalized)) signals.add("harassment");
  if (/\bbuyout\b/.test(normalized)) signals.add("buyout");
  if (isBuyoutPressureQuery(normalized)) {
    signals.add("harassment");
    signals.add("coercion");
    signals.add("vacate");
    signals.add("intimidation");
  }
  if (/\bcapital improvement\b/.test(normalized)) signals.add("capital improvement");
  if (isAccommodationQuery(normalized)) {
    signals.add("reasonable accommodation");
    signals.add("service animal");
  }
  if (isHomeownersExemptionQuery(normalized)) {
    signals.add("homeowner's exemption");
    signals.add("principal residence");
  }
  if (isCameraPrivacyQuery(normalized)) {
    signals.add("camera privacy");
    signals.add("privacy");
    signals.add("camera");
  }
  if (isPackageSecurityQuery(normalized)) {
    signals.add("package security");
    signals.add("package theft");
    signals.add("mail theft");
  }
  if (isDogQuery(normalized)) {
    signals.add("dog");
    signals.add("pet policy");
    signals.add("dog-free building");
  }
  if (isCollegeQuery(normalized)) {
    signals.add("college");
    signals.add("student housing");
    signals.add("temporary absence");
  }
  if (isSelfEmployedQuery(normalized)) {
    signals.add("self employed");
    signals.add("1099");
    signals.add("tax return");
  }
  if (isAdjudicatedQuery(normalized)) {
    signals.add("adjudicated");
    signals.add("already decided");
    signals.add("precluded");
  }
  if (isSocialMediaQuery(normalized)) {
    signals.add("social media");
    signals.add("facebook");
    signals.add("instagram");
  }
  if (isCaregiverQuery(normalized)) {
    signals.add("caregiver");
    signals.add("caregiving");
    signals.add("primary caregiver");
  }
  if (isPoopQuery(normalized)) {
    signals.add("poop");
    signals.add("feces");
    signals.add("animal waste");
  }
  if (isMootQuery(normalized)) {
    signals.add("moot");
    signals.add("null and void");
    signals.add("rescinded");
  }
  if (isRemoteWorkQuery(normalized)) {
    signals.add("remote work");
    signals.add("work from home");
    signals.add("construction noise");
  }
  if (isDivorceQuery(normalized)) {
    signals.add("divorce");
    signals.add("separation");
    signals.add("spouse");
  }
  if (isIntercomQuery(normalized)) {
    signals.add("intercom");
    signals.add("door buzzer");
    signals.add("entry system");
  }
  if (isGarageSpaceQuery(normalized)) {
    signals.add("garage space");
    signals.add("parking space");
    signals.add("garage parking");
  }
  if (isCommonAreasQuery(normalized)) {
    signals.add("common areas");
    signals.add("common area");
    signals.add("janitorial service");
  }
  if (isStairsQuery(normalized)) {
    signals.add("stairs");
    signals.add("handrail");
    signals.add("stairwell");
  }
  if (isPorchQuery(normalized)) {
    signals.add("porch");
    signals.add("landing");
    signals.add("storage room");
  }
  if (isWindowsQuery(normalized)) {
    signals.add("windows");
    signals.add("window");
    signals.add("window latch");
    signals.add("window sash");
  }
  if (isCoLivingQuery(normalized)) {
    signals.add("co-living");
    signals.add("common areas");
    signals.add("individual room");
  }
  if (isSection8Query(normalized)) {
    signals.add("section 8");
  }
  if (isUnlawfulDetainerQuery(normalized)) {
    signals.add("unlawful detainer");
  }

  return Array.from(signals);
}

function sentenceIssueAnchorTerms(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];

  const anchors = new Set<string>();
  if (/\bmold\b/.test(normalized) && /\brepair|repairs\b/.test(normalized)) {
    anchors.add("repair");
    anchors.add("repairs");
    anchors.add("failed to repair");
    anchors.add("reported");
  }
  if (hasOwnerMoveInPhrase(normalized) || containsWholeWord(normalized, "omi")) {
    if (/\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalized)) {
      anchors.add("owner occupancy");
      anchors.add("occupancy");
      anchors.add("occupy");
      anchors.add("occupied");
      anchors.add("reside");
      anchors.add("resided");
      anchors.add("principal place of residence");
      anchors.add("tenant in occupancy");
    }
    if (/\bnotice\b/.test(normalized)) anchors.add("notice");
    if (requiresOwnerMoveInFollowThroughSpecificity(normalized)) {
      anchors.add("never moved in");
      anchors.add("did not move in");
      anchors.add("never occupied");
      anchors.add("did not occupy");
      anchors.add("never resided");
      anchors.add("did not reside");
    }
  }
  if (hasWrongfulEvictionPhrase(normalized) || containsWholeWord(normalized, "awe")) {
    if (/\block(?:ed)? out|lockout\b/.test(normalized)) {
      anchors.add("lockout");
      anchors.add("locked out");
    }
    if (/\bchanged locks?\b/.test(normalized)) anchors.add("changed locks");
    if (/\bdenied access\b/.test(normalized)) anchors.add("denied access");
    if (/\bshut off utilities|utility shutoff|utilities shut off\b/.test(normalized)) {
      anchors.add("shut off utilities");
      anchors.add("utility shutoff");
    }
    if (/\bself[-\s]?help\b/.test(normalized)) {
      anchors.add("self-help eviction");
      anchors.add("self help eviction");
    }
    if (/\brepair|repairs\b/.test(normalized)) {
      anchors.add("repair");
      anchors.add("repairs");
    }
    if (/\bcomplain(?:ed|ing)?\b/.test(normalized)) {
      anchors.add("complain");
      anchors.add("complaining");
    }
    if (/\bnotice\b/.test(normalized)) anchors.add("notice");
  }
  if (/\bharassment|retaliation\b/.test(normalized)) {
    anchors.add("harassment");
    anchors.add("retaliation");
    if (/\bnotice|notices\b/.test(normalized)) anchors.add("notice");
    if (/\bentr(?:y|ies)\b/.test(normalized)) {
      anchors.add("entry");
      anchors.add("entries");
    }
  }
  if (isBuyoutPressureQuery(normalized)) {
    anchors.add("harassment");
    anchors.add("harass");
    anchors.add("harassing");
    anchors.add("pressure");
    anchors.add("pressured");
    anchors.add("pressuring");
    anchors.add("coerce");
    anchors.add("coerced");
    anchors.add("coercion");
    anchors.add("coercive");
    anchors.add("threat");
    anchors.add("threaten");
    anchors.add("threatened");
    anchors.add("intimidation");
    anchors.add("fraud");
    anchors.add("vacate");
    anchors.add("payment to vacate");
    anchors.add("payments to vacate");
    anchors.add("offer to vacate");
    anchors.add("offers to vacate");
    anchors.add("offer of payment to vacate");
    anchors.add("offers of payment to vacate");
    anchors.add("threats or intimidation");
  }
  if (
    /\bmold|hot water|heat|heating|heater|boiler|radiator|rodent|cockroach|bed bug|ventilation|leak|water intrusion|plumbing|sewage\b/.test(
      normalized
    )
  ) {
    if (/\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(normalized)) {
      anchors.add("reported");
      anchors.add("complained");
      anchors.add("notified");
      anchors.add("notice");
    }
    if (/\brepair|repairs|restore|restored|service|services\b/.test(normalized)) {
      anchors.add("repair");
      anchors.add("repairs");
      anchors.add("failed to repair");
      anchors.add("restore service");
      anchors.add("failed to restore service");
      anchors.add("service restoration");
    }
  }
  if (isAccommodationQuery(normalized)) {
    anchors.add("reasonable accommodation");
    anchors.add("service animal");
    anchors.add("support animal");
    anchors.add("emotional support animal");
    if (/\brequest(?:ed|ing)?|ask(?:ed|ing)? for\b/.test(normalized)) {
      anchors.add("requested");
      anchors.add("request");
    }
    if (/\bdoctor|medical|disab(?:ility|led)\b/.test(normalized) || /\bservice animal\b/.test(normalized)) {
      anchors.add("medical provider");
      anchors.add("doctor");
      anchors.add("disability");
    }
  }
  if (isCameraPrivacyQuery(normalized)) {
    anchors.add("camera");
    anchors.add("cameras");
    anchors.add("security camera");
    anchors.add("surveillance");
    anchors.add("privacy");
    anchors.add("invasion of privacy");
    anchors.add("video monitoring");
  }
  if (isPackageSecurityQuery(normalized)) {
    anchors.add("package security");
    anchors.add("package theft");
    anchors.add("stolen packages");
    anchors.add("mail theft");
    anchors.add("mailroom security");
    anchors.add("housing service");
  }
  if (isDogQuery(normalized)) {
    anchors.add("dog");
    anchors.add("dogs");
    anchors.add("dog-free building");
    anchors.add("no pets");
    anchors.add("pet policy");
    anchors.add("dog park");
    anchors.add("service animal");
    anchors.add("housing service");
  }
  if (isCollegeQuery(normalized)) {
    anchors.add("college");
    anchors.add("attend college");
    anchors.add("student housing");
    anchors.add("school breaks");
    anchors.add("temporary absence");
    anchors.add("return to live in the unit");
    anchors.add("permanent residence");
  }
  if (isSelfEmployedQuery(normalized)) {
    anchors.add("self employed");
    anchors.add("self-employed");
    anchors.add("1099");
    anchors.add("tax return");
    anchors.add("tax returns");
    anchors.add("subject unit as his address");
    anchors.add("files his tax returns");
    anchors.add("principal residence");
  }
  if (isAdjudicatedQuery(normalized)) {
    anchors.add("adjudicated");
    anchors.add("adjudicate");
    anchors.add("already decided");
    anchors.add("previously decided");
    anchors.add("precluded");
    anchors.add("preclusion");
    anchors.add("state court");
  }
  if (isSocialMediaQuery(normalized)) {
    anchors.add("social media");
    anchors.add("facebook");
    anchors.add("instagram");
    anchors.add("nextdoor");
    anchors.add("facebook marketplace");
    anchors.add("posted online");
    anchors.add("principal residence");
    anchors.add("roommate search");
  }
  if (isCaregiverQuery(normalized)) {
    anchors.add("caregiver");
    anchors.add("caregiving");
    anchors.add("primary caregiver");
    anchors.add("care for");
    anchors.add("return to live in the unit");
    anchors.add("principal residence");
    anchors.add("family assistance");
  }
  if (isPoopQuery(normalized)) {
    anchors.add("poop");
    anchors.add("feces");
    anchors.add("faeces");
    anchors.add("dog waste");
    anchors.add("animal waste");
    anchors.add("human feces");
    anchors.add("sewage");
    anchors.add("contamination");
  }
  if (isMootQuery(normalized)) {
    anchors.add("moot");
    anchors.add("rendered moot");
    anchors.add("null and void");
    anchors.add("rescinded");
    anchors.add("administratively dismissed");
    anchors.add("withdrawn");
  }
  if (isRemoteWorkQuery(normalized)) {
    anchors.add("remote work");
    anchors.add("work from home");
    anchors.add("working from home");
    anchors.add("unable to work from home");
    anchors.add("construction noise");
    anchors.add("power was turned off");
    anchors.add("telephone conversations");
    anchors.add("quiet enjoyment");
  }
  if (isDivorceQuery(normalized)) {
    anchors.add("divorce");
    anchors.add("divorced");
    anchors.add("separated");
    anchors.add("separation");
    anchors.add("spouse moved out");
    anchors.add("marital issues");
    anchors.add("live separately");
    anchors.add("residence");
  }
  if (isIntercomQuery(normalized)) {
    anchors.add("intercom");
    anchors.add("broken intercom");
    anchors.add("door buzzer");
    anchors.add("entry system");
    anchors.add("security gate");
    anchors.add("housing service");
  }
  if (isGarageSpaceQuery(normalized)) {
    anchors.add("garage space");
    anchors.add("parking space");
    anchors.add("garage parking");
    anchors.add("carport parking");
    anchors.add("tandem space");
    anchors.add("housing service");
  }
  if (isCommonAreasQuery(normalized)) {
    anchors.add("common areas");
    anchors.add("common area");
    anchors.add("janitorial service");
    anchors.add("unclean common areas");
    anchors.add("clean common areas");
    anchors.add("housing service");
  }
  if (isStairsQuery(normalized)) {
    anchors.add("stairs");
    anchors.add("loose stairs");
    anchors.add("handrail");
    anchors.add("back stairs");
    anchors.add("stairwell");
    anchors.add("housing service");
  }
  if (isPorchQuery(normalized)) {
    anchors.add("porch");
    anchors.add("front porch");
    anchors.add("back porch");
    anchors.add("landing");
    anchors.add("storage room");
    anchors.add("porch door");
    anchors.add("housing service");
  }
  if (isWindowsQuery(normalized)) {
    anchors.add("windows");
    anchors.add("window");
    anchors.add("inoperable windows");
    anchors.add("broken windows");
    anchors.add("window latch");
    anchors.add("window sash");
    anchors.add("operable windows");
    anchors.add("housing service");
  }
  if (isCoLivingQuery(normalized)) {
    anchors.add("separate tenancy");
    anchors.add("separate rental agreement");
    anchors.add("separate rental agreements");
    anchors.add("individual room");
    anchors.add("separately rented");
    anchors.add("common areas");
    anchors.add("shared kitchen");
  }
  if (isSection8Query(normalized)) {
    anchors.add("section 8");
    anchors.add("hud");
    anchors.add("voucher");
    anchors.add("housing choice voucher");
  }
  if (isUnlawfulDetainerQuery(normalized)) {
    anchors.add("unlawful detainer");
    anchors.add("notice to quit");
    anchors.add("three day notice");
    anchors.add("detainer");
    anchors.add("eviction action");
    anchors.add("eviction");
  }
  if (isSection8UnlawfulDetainerQuery(normalized)) {
    anchors.add("section 8 eviction");
    anchors.add("section 8 eviction action");
    anchors.add("voucher eviction");
  }

  return Array.from(anchors);
}

function sentenceSecondaryFactTokens(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  if (isBuyoutPressureQuery(normalized)) {
    return ["payment to vacate", "payments to vacate", "vacate", "intimidation", "coercion"];
  }

  const issueTokenSet = new Set(
    inferIssueTerms(query)
      .flatMap((term) => meaningfulPhraseTokens(term))
      .map((token) => normalize(token))
  );

  const dropTokens = new Set([
    "tenant",
    "landlord",
    "unit",
    "eviction",
    "owner",
    "move",
    "wrongful",
    "mold",
    "repair",
    "repairs",
    "notice",
    "service",
    "services"
  ]);

  return meaningfulPhraseTokens(query)
    .filter((token) => {
      const normalizedToken = normalize(token);
      return normalizedToken && !issueTokenSet.has(normalizedToken) && !dropTokens.has(normalizedToken);
    })
    .slice(0, 6);
}

function sentenceFactualTokenMetrics(query: string, text: string): {
  matchedCount: number;
  totalCount: number;
  coverageRatio: number;
  proximityBoost: number;
} {
  const normalizedText = normalize(text || "");
  if (!normalizedText) {
    return { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  }

  const factualTokens = uniq([...sentenceIssueAnchorTerms(query), ...sentenceSecondaryFactTokens(query)])
    .map((token) => normalize(token))
    .filter(Boolean)
    .slice(0, 8);

  if (factualTokens.length === 0) {
    return { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  }

  const matchedPositions = factualTokens
    .map((token) => ({ token, index: normalizedText.indexOf(token) }))
    .filter((item) => item.index >= 0)
    .sort((a, b) => a.index - b.index);

  const matchedCount = matchedPositions.length;
  const coverageRatio = matchedCount > 0 ? matchedCount / factualTokens.length : 0;

  let proximityBoost = 0;
  if (matchedPositions.length >= 2) {
    const firstMatch = matchedPositions[0];
    const lastMatch = matchedPositions[matchedPositions.length - 1];
    if (firstMatch && lastMatch) {
      const span = lastMatch.index - firstMatch.index;
      if (span <= 180) proximityBoost = 0.12;
      else if (span <= 320) proximityBoost = 0.08;
      else if (span <= 520) proximityBoost = 0.04;
    }
  }

  return {
    matchedCount,
    totalCount: factualTokens.length,
    coverageRatio: Number(coverageRatio.toFixed(4)),
    proximityBoost
  };
}

function textContainsIssueSignal(text: string, signal: string): boolean {
  const normalizedText = normalize(text);
  const normalizedSignal = normalize(signal);
  if (!normalizedText || !normalizedSignal) return false;
  if (normalizedSignal === "reasonable accommodation") {
    return hasAccommodationContext(normalizedText);
  }
  if (normalizedSignal === "service animal") {
    return /service animal|support animal|emotional support animal|assistance animal/.test(normalizedText);
  }
  if (normalizedSignal === "camera privacy") {
    return hasCameraPrivacyContext(normalizedText);
  }
  if (normalizedSignal === "package security") {
    return hasPackageSecurityContext(normalizedText);
  }
  if (normalizedSignal === "package theft") {
    return hasPackageSecurityContext(normalizedText);
  }
  if (normalizedSignal === "mail theft") {
    return hasPackageSecurityContext(normalizedText);
  }
  if (normalizedSignal === "intercom") {
    return hasIntercomContext(normalizedText);
  }
  if (normalizedSignal === "door buzzer") {
    return hasIntercomContext(normalizedText);
  }
  if (normalizedSignal === "entry system") {
    return hasIntercomContext(normalizedText);
  }
  if (normalizedSignal === "garage space") {
    return hasGarageSpaceContext(normalizedText);
  }
  if (normalizedSignal === "parking space") {
    return hasGarageSpaceContext(normalizedText);
  }
  if (normalizedSignal === "garage parking") {
    return hasGarageSpaceContext(normalizedText);
  }
  if (normalizedSignal === "common areas") {
    return hasCommonAreasContext(normalizedText);
  }
  if (normalizedSignal === "common area") {
    return hasCommonAreasContext(normalizedText);
  }
  if (normalizedSignal === "janitorial service") {
    return hasCommonAreasContext(normalizedText);
  }
  if (normalizedSignal === "stairs") {
    return hasStairsContext(normalizedText);
  }
  if (normalizedSignal === "handrail") {
    return hasStairsContext(normalizedText);
  }
  if (normalizedSignal === "stairwell") {
    return hasStairsContext(normalizedText);
  }
  if (normalizedSignal === "porch") {
    return hasPorchContext(normalizedText);
  }
  if (normalizedSignal === "landing") {
    return hasPorchContext(normalizedText);
  }
  if (normalizedSignal === "storage room") {
    return hasPorchContext(normalizedText);
  }
  if (normalizedSignal === "windows") {
    return hasWindowsContext(normalizedText);
  }
  if (normalizedSignal === "window") {
    return hasWindowsContext(normalizedText);
  }
  if (normalizedSignal === "window latch") {
    return hasWindowsContext(normalizedText);
  }
  if (normalizedSignal === "window sash") {
    return hasWindowsContext(normalizedText);
  }
  if (normalizedSignal === "co-living") {
    return hasCoLivingContext(normalizedText);
  }
  if (normalizedSignal === "homeowner's exemption") {
    return hasHomeownersExemptionContext(normalizedText);
  }
  if (normalizedSignal === "section 8") {
    return hasSection8Context(normalizedText);
  }
  if (normalizedSignal === "unlawful detainer") {
    return hasUnlawfulDetainerContext(normalizedText);
  }
  if (normalizedSignal === "owner move in") {
    return (
      hasOwnerMoveInPhrase(normalizedText) ||
      normalizedText.includes("owner occupancy") ||
      normalizedText.includes("occupy the unit") ||
      normalizedText.includes("occupied the unit")
    );
  }
  if (normalizedSignal === "wrongful eviction") {
    return hasWrongfulEvictionContext(normalizedText);
  }
  if (normalizedSignal === "lockout") {
    return hasWrongfulEvictionLockoutContext(normalizedText);
  }
  if (normalizedSignal === "infestation") {
    return /\binfestation|infestations|rodent|rodents|cockroach|cockroaches|roach|roaches|bed bug|bed bugs|mouse|mice|rat|rats|pest|pests\b/.test(
      normalizedText
    );
  }
  return containsWholeWord(normalizedText, normalizedSignal) || normalizedText.includes(normalizedSignal);
}

function isConditionIssueQuery(query: string): boolean {
  return inferIssueTerms(query).length > 0;
}

function inferProceduralTerms(query: string): string[] {
  const q = normalize(query);
  if (!q) return [];
  const out: string[] = [];
  const add = (...values: string[]) => out.push(...values);

  if (/\bnotice|service|served|mail|mailing|posting\b/.test(q)) {
    add("notice", "service", "served", "mail", "mailing", "posting", "repair request", "work order", "written notice");
  }
  if (/\bhearing|continuance|appearance|filing|deadline|extension\b/.test(q)) {
    add("hearing", "continuance", "appearance", "filing", "deadline", "extension");
  }
  if (/\bharassment|retaliation\b/.test(q)) {
    add("tenant petition", "petition", "claim", "retaliation", "harassment", "section 37.10b");
  }

  return uniq(out);
}

function isNoticeProceduralQuery(query: string): boolean {
  return inferProceduralTerms(query).length > 0;
}

function isCoolingIssueQuery(query: string): boolean {
  return /\bcool|cooling|ventilation|air flow|air circulation|overheating|temperature control\b/.test(normalize(query));
}

function isJudgeDrivenQuery(query: string): boolean {
  return queryReferencesJudge(query).length > 0 && inferIssueTerms(query).length === 0 && inferProceduralTerms(query).length === 0;
}

function isAccommodationQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\breasonable accommodation|service animal|support animal|emotional support animal|assistance animal\b/.test(normalized);
}

function isCameraPrivacyQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    (/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b/.test(normalized) && /\bprivacy\b|\binvasion of privacy\b/.test(normalized)) ||
    /\bsurveillance camera privacy\b/.test(normalized)
  );
}

function isPackageSecurityQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    (/\bpackage\b|\bpackages\b/.test(normalized) && /\bsecurity\b|\btheft\b|\bstolen\b|\bsafety\b/.test(normalized)) ||
    /\bmail theft\b|\bmailroom security\b/.test(normalized)
  );
}

function isLockBoxQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\block box\b|\blockbox\b/.test(normalized);
}

function isDogQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bdogs?\b/.test(normalized);
}

function isIntercomQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bintercom\b/.test(normalized) ||
    /\bdoor buzzer\b/.test(normalized) ||
    (/\bentry system\b/.test(normalized) && /\bbroken|inoperable|not working|security gate|buzz\b/.test(normalized))
  );
}

function isGarageSpaceQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bgarage space\b/.test(normalized) ||
    /\bparking space\b/.test(normalized) ||
    /\bgarage parking\b/.test(normalized) ||
    (/\bcarport\b/.test(normalized) && /\bparking\b/.test(normalized)) ||
    /\btandem space\b/.test(normalized)
  );
}

function isCommonAreasQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bcommon areas?\b/.test(normalized) ||
    /\bjanitorial service\b/.test(normalized) ||
    (/\bclean(?:ing)?\b|\bunclean\b|\bdirty\b/.test(normalized) && /\bcommon areas?\b|\bhallways?\b/.test(normalized))
  );
}

function isStairsQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bstairs?\b/.test(normalized) ||
    /\bhandrail\b/.test(normalized) ||
    /\bstairwell\b/.test(normalized) ||
    /\bback stairs\b/.test(normalized)
  );
}

function isPorchQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bporch\b/.test(normalized) ||
    /\bfront porch\b/.test(normalized) ||
    /\bback porch\b/.test(normalized) ||
    /\blanding\b/.test(normalized)
  );
}

function isWindowsQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bwindows?\b/.test(normalized) || /\bwindow sash\b|\bwindow latch\b|\binoperable windows?\b|\bbroken windows?\b/.test(normalized);
}

function isCollegeQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bcollege\b/.test(normalized);
}

function isSelfEmployedQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bself employed\b|\bself-employed\b|\bschedule c\b|\b1099\b/.test(normalized);
}

function isAdjudicatedQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\badjudicat(?:ed|e)\b|\balready decided\b|\bpreviously decided\b|\bprecluded\b|\bpreclusion\b/.test(normalized);
}

function isSocialMediaQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bsocial media\b|\bfacebook\b|\binstagram\b|\bnextdoor\b|\bfacebook marketplace\b/.test(normalized);
}

function isCaregiverQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bcaregiver\b|\bcaregiving\b|\bcaretaker\b/.test(normalized);
}

function isPoopQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bpoop\b|\bfeces\b|\bfaeces\b|\bdog waste\b|\banimal waste\b|\bhuman feces\b/.test(normalized);
}

function isMootQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bmoot\b|\brendered moot\b/.test(normalized);
}

function isRemoteWorkQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bremote work\b|\bwork from home\b|\bworking from home\b|\btelework\b|\btelecommut(?:e|ing)\b/.test(normalized);
}

function isDivorceQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bdivorce\b|\bdivorced\b|\bseparation\b|\bseparated\b/.test(normalized);
}

function isCoLivingQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bco[-\s]?living\b|\bcoliving\b/.test(normalized) ||
    (/\bseparate rental agreements?\b/.test(normalized) && /\bindividual room\b|\bcommon areas?\b/.test(normalized))
  );
}

function isHomeownersExemptionQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    /\bhomeowner'?s exemption\b|\bhomeowners exemption\b|\bhomeowner s exemption\b/.test(normalized) ||
    (/\bproperty tax exemption\b/.test(normalized) && /\bprincipal place of residence\b|\bprincipal residence\b/.test(normalized))
  );
}

function hasAccommodationContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\breasonable accommodation|accommodation request|service animal|support animal|emotional support animal|assistance animal|disability accommodation\b/.test(
      normalizedText
    ) ||
    ((/doctor|medical provider|physician|therapist|disability/.test(normalizedText) ||
      /medical letter|doctor s letter|provider letter/.test(normalizedText)) &&
      /service animal|support animal|emotional support animal|assistance animal|animal/.test(normalizedText))
  );
}

function hasEmploymentAccommodationDrift(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const employmentSignal =
    /\bjob\b|\bemployment\b|\bemployee\b|\bapplicant\b|\bhired\b|\bworkplace\b|\bposition\b|\bperformance\b|\bpermanent appointment\b|\btrial basis\b/.test(
      normalizedText
    );
  const housingSignal =
    /\btenant\b|\blandlord\b|\brental unit\b|\bsubject unit\b|\bhousing service\b|\brent board\b|\bcivil court\b|\bservice animal\b|\bsupport animal\b|\bemotional support animal\b|\bassistance animal\b/.test(
      normalizedText
    );
  return employmentSignal && !housingSignal;
}

function hasCameraPrivacyContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasCamera =
    /\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b|\bvideo camera\b|\bvideo monitoring\b/.test(normalizedText);
  const hasPrivacy =
    /\bprivacy\b|\binvasion of privacy\b|\bprivate\b|\bmonitoring\b|\brecorded\b|\brecording\b|\bwatching\b|\bwatched\b/.test(
      normalizedText
    );
  return hasCamera && hasPrivacy;
}

function hasPackageSecurityContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasPackageSignal =
    /\bpackage\b|\bpackages\b|\bmail\b|\bmailroom\b|\bdelivery\b/.test(normalizedText);
  const hasSecuritySignal =
    /\bsecurity\b|\btheft\b|\bstolen\b|\bthief\b|\bapprehend\b|\bsafe(?:ty)?\b/.test(normalizedText);
  const securityFeeDrift = /\bsecurity fee\b|\bsecurity fees\b|\bcharge for a security\b|\bunlawful charges? for security fees?\b/.test(
    normalizedText
  );
  const securityDepositDrift =
    /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
      normalizedText
    );
  return hasPackageSignal && hasSecuritySignal && !securityFeeDrift && !securityDepositDrift;
}

function hasPackageDeliverySecurityContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasPackageSignal =
    /\bpackage\b|\bpackages\b|\bmail\b|\bmailroom\b|\bdelivery\b/.test(normalizedText);
  const hasSpecificSecuritySignal =
    /\btheft\b|\bstolen\b|\bthief\b|\bapprehend\b|\bmail theft\b|\bpackage theft\b|\bmailroom security\b|\bpackage security\b|\bsecure\b|\baccess\b|\bbuzz\b|\bintercom\b|\bentry\b/.test(
      normalizedText
    );
  const hasDeliveryOrAccessContext =
    /\bdelivery\b|\bmailroom\b|\bmail\b|\baccess\b|\bintercom\b|\bbuzz\b|\bentry\b|\bsign for packages\b|\bshipped\b/.test(
      normalizedText
    );
  const securityFeeDrift = /\bsecurity fee\b|\bsecurity fees\b|\bcharge for a security\b|\bunlawful charges? for security fees?\b/.test(
    normalizedText
  );
  const securityDepositDrift =
    /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
      normalizedText
    );
  return hasPackageSignal && hasSpecificSecuritySignal && hasDeliveryOrAccessContext && !securityFeeDrift && !securityDepositDrift;
}

function hasLockBoxContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\block box\b|\blockbox\b|\block box with a key\b|\bkey to a lockbox\b|\bcode to the tenant\b|\bentrust a car key\b/.test(normalizedText) &&
    /\bkey\b|\bcode\b|\baccess\b|\btenant\b|\blandlord\b|\bhousing service\b|\bcar\b/.test(normalizedText)
  );
}

function hasDogContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasDogSignal = /\bdogs?\b|\bdog-free building\b|\bdog park\b|\bpet(?:s)?\b|\bservice animal\b|\bemotional support animal\b/.test(normalizedText);
  const hasRelevantContext = /\bhousing service\b|\bno pets\b|\bpet policy\b|\bpets? prohibited\b|\bpet clause\b|\bdog-free building\b|\bservice animal\b|\bemotional support animal\b|\bdog park\b|\bcommon area\b|\bcommon areas\b|\bbark(?:ing)?\b|\bnoise\b/.test(normalizedText);
  return hasDogSignal && hasRelevantContext;
}

function hasDogPolicyContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\bdog-free building\b|\bno pets\b|\bpet policy\b|\bpets? prohibited\b|\bpet clause\b|\bservice animal\b|\bemotional support animal\b/.test(normalizedText);
}

function hasDogParkContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\bdog park\b/.test(normalizedText);
}

function hasCollegeContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasCollegeSignal =
    /\bcollege\b|\bschool\b|\bstudent housing\b|\bschool breaks\b|\battend(?:ing)? school\b|\battend(?:ing)? college\b/.test(normalizedText);
  const hasResidencySignal =
    /\btemporary absence\b|\btemporarily\b|\breturn(?:ing)?\b|\breturn to live\b|\bpermanent(?:ly)? resid(?:e|es|ed|ing)\b|\bpermanent residence\b|\bintends? to return\b|\broom is being kept\b|\bkept vacant\b/.test(
      normalizedText
    );
  return hasCollegeSignal && hasResidencySignal;
}

function hasSelfEmployedContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasEmploymentSignal =
    /\bself employed\b|\bself-employed\b|\b1099\b|\bschedule c\b|\bclients\b|\bbusiness\b/.test(normalizedText);
  const hasResidencyEvidenceSignal =
    /\btax return\b|\btax returns\b|\bsubject unit\b|\baddress\b|\bprincipal residence\b|\bfiles? .*tax returns?\b|\b1099s reporting income\b/.test(
      normalizedText
    );
  return hasEmploymentSignal && hasResidencyEvidenceSignal;
}

function hasAdjudicatedContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\badjudicat(?:ed|e)\b|\balready decided\b|\bpreviously decided\b|\bprecluded\b|\bpreclusion\b|\bstate court\b/.test(normalizedText);
}

function hasSocialMediaContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasPlatformSignal =
    /\bsocial media\b|\bfacebook\b|\binstagram\b|\bnextdoor\b|\bfacebook marketplace\b/.test(normalizedText);
  const hasUseSignal =
    /\bprincipal residence\b|\bresid(?:e|ed|ence)\b|\boccup(?:y|ancy)\b|\broommate\b|\bsublet\b|\bposted\b|\bprofile\b|\bfriends\b|\bonline search\b/.test(
      normalizedText
    );
  return hasPlatformSignal && hasUseSignal;
}

function hasCaregiverContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasCaregiverSignal =
    /\bcaregiver\b|\bcaregiving\b|\bcaretaker\b|\bprimary caregiver\b|\bcare for\b/.test(normalizedText);
  const hasResidencySignal =
    /\bprincipal residence\b|\breturn to live in the unit\b|\breturn\b|\blive in the subject unit\b|\bresid(?:e|ence)\b|\bfamily would need to work out a schedule\b/.test(
      normalizedText
    );
  return hasCaregiverSignal && hasResidencySignal;
}

function hasPoopContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasWasteSignal =
    /\bpoop\b|\bfeces\b|\bfaeces\b|\bdog waste\b|\banimal waste\b|\bhuman feces\b/.test(normalizedText);
  const hasSanitationSignal =
    /\bsewage\b|\bcontamination\b|\bbackyard\b|\byard\b|\bcommon areas?\b|\bkitchen\b|\blaundry\b|\bhealth\b|\bsanitation\b/.test(normalizedText);
  return hasWasteSignal && hasSanitationSignal;
}

function hasStrongPoopDecisionContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const strongWasteSignal =
    /\bdog waste\b|\banimal waste\b|\bhuman feces\b|\bsewage\b|\braw sewage\b|\bcontamination\b/.test(normalizedText);
  const locationOrHarmSignal =
    /\bbackyard\b|\byard\b|\bcommon areas?\b|\bwalkway\b|\bkitchen\b|\blaundry\b|\bhealth\b|\bsanitation\b/.test(normalizedText);
  return strongWasteSignal && locationOrHarmSignal;
}

function hasWeakRodentPoopContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\brat feces\b|\brodent urine\/feces\b|\bmouse feces\b|\bmice feces\b/.test(normalizedText) &&
    !/\bdog waste\b|\banimal waste\b|\bhuman feces\b|\bsewage\b|\braw sewage\b|\bcontamination\b/.test(normalizedText)
  );
}

function hasMootContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\bmoot\b|\brendered moot\b|\bnull and void\b|\brescinded\b|\badministratively dismissed\b|\bwithdrawn\b/.test(normalizedText);
}

function hasRemoteWorkContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasRemoteWorkSignal =
    /\bremote work\b|\bwork from home\b|\bworking from home\b|\btelework\b|\btelecommut(?:e|ing)\b/.test(normalizedText);
  const hasInterferenceSignal =
    /\bconstruction noise\b|\bnoise\b|\bquiet enjoyment\b|\btelephone conversations?\b|\bunable to work\b|\bpower was turned off\b|\butility service\b|\bdisrupt(?:ed|ion)\b|\bunlivable\b/.test(
      normalizedText
    );
  return hasRemoteWorkSignal && hasInterferenceSignal;
}

function hasDivorceContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasDivorceSignal =
    /\bdivorce\b|\bdivorced\b|\bseparation\b|\bseparated\b|\bmarital issues\b|\blive separately\b/.test(normalizedText);
  const hasRelationshipSignal =
    /\bspouse\b|\bhusband\b|\bwife\b|\bpartner\b|\bex-wife\b|\bex husband\b|\bmarriage counselor\b|\bmoved out\b/.test(normalizedText);
  return hasDivorceSignal || (hasRelationshipSignal && /\bseparated\b|\bdivorc/.test(normalizedText));
}

function hasIntercomContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasIntercomSignal =
    /\bintercom\b|\bdoor buzzer\b|\bentry system\b|\bbuzz(?:ed|er|ing)?\b/.test(normalizedText);
  const hasAccessSignal =
    /\bhousing service\b|\bentry\b|\baccess\b|\bdoor\b|\bgate\b|\bsecurity gate\b|\bprogrammed\b|\btelephone number\b/.test(
      normalizedText
    );
  return hasIntercomSignal && hasAccessSignal;
}

function hasGarageSpaceContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasParkingSignal =
    /\bgarage space\b|\bparking space\b|\bgarage parking\b|\bcarport parking\b|\btandem space\b|\bparking garage\b/.test(
      normalizedText
    );
  const hasHousingServiceSignal =
    /\bhousing service\b|\bexclusive use\b|\buse of the garage\b|\bright to park\b|\bparking\b|\bcarport\b|\bgarage\b/.test(
      normalizedText
    );
  return hasParkingSignal && hasHousingServiceSignal;
}

function hasCommonAreasContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasAreaSignal =
    /\bcommon areas?\b|\bcommon hallway\b|\bcommon hallways\b|\bhallways?\b|\bback alley\b|\bpatio\b/.test(normalizedText);
  const hasServiceSignal =
    /\bhousing service\b|\bjanitorial service\b|\bclean\b|\bunclean\b|\bdirty\b|\bmaintained\b|\bmaintain\b|\bclean condition\b/.test(
      normalizedText
    );
  return hasAreaSignal && hasServiceSignal;
}

function hasStairsContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasStairSignal =
    /\bstairs?\b|\bstairwell\b|\bback stairs\b|\bfront stairs\b|\bhandrail\b/.test(normalizedText);
  const hasServiceSignal =
    /\bhousing service\b|\bloose\b|\bwobbly\b|\bfall\b|\bunsafe\b|\bmove when\b|\bmaintained\b|\bconnected with the use or occupancy\b/.test(
      normalizedText
    );
  return hasStairSignal && hasServiceSignal;
}

function hasPorchContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasPorchSignal =
    /\bporch\b|\bfront porch\b|\bback porch\b|\blanding\b|\bporch door\b|\bstorage room\b/.test(normalizedText);
  const hasServiceSignal =
    /\bhousing service\b|\bleak\b|\bleaking\b|\bunsafe\b|\bhazard\b|\bdoor\b|\brail(?:ing)?\b|\bhandrail\b|\bstorage\b|\bmaintained\b|\bconnected with the use or occupancy\b/.test(
      normalizedText
    );
  return hasPorchSignal && hasServiceSignal;
}

function hasWindowsContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const hasWindowSignal =
    /\bwindows?\b|\bwindow sash\b|\bwindow latch\b|\bwindow lock\b|\binoperable windows?\b|\bbroken windows?\b/.test(normalizedText);
  const hasServiceSignal =
    /\bhousing service\b|\binoperable\b|\bbroken\b|\boperable\b|\bwon t open\b|\bwould not open\b|\bwould not close\b|\bclose properly\b|\bweatherstrip\b|\bleak\b|\bdraft\b|\bunsafe\b|\bmaintained\b|\bconnected with the use or occupancy\b/.test(
      normalizedText
    );
  return hasWindowSignal && hasServiceSignal;
}

function hasCoLivingContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\bco[-\s]?living\b|\bcoliving\b/.test(normalizedText) ||
    /\bseparate rental agreements?\b/.test(normalizedText) ||
    ((/\bindividual room\b|\bseparate bedroom\b|\bseparately rented\b|\bseparate tenancy\b|\bseparate tenancies\b/.test(normalizedText)) &&
      /\bcommon areas?\b|\bshared kitchen\b|\bshared bathroom\b|\bshared living room\b|\bsubtenant\b|\bsubtenants\b|\broommate\b|\broommates\b/.test(
        normalizedText
      ))
  );
}

function hasHomeownersExemptionContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\bhomeowner'?s exemption\b|\bhomeowners exemption\b|\bhomeowner s exemption\b|\bproperty tax exemption\b/.test(normalizedText) ||
    ((/\bprincipal place of residence\b|\bprincipal residence\b|\bprimary residence\b|\bowner occupancy\b/.test(normalizedText)) &&
      /\bproperty tax\b|\bexemption\b/.test(normalizedText))
  );
}

function hasPetPolicyDrift(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /no pet|no pets|pet clause|pets prohibited|pet deposit|pet policy|animals prohibited/.test(normalizedText) &&
    !hasAccommodationContext(normalizedText)
  );
}

function isSection8Query(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bsection 8\b|\bhud\b|housing choice voucher|\bvoucher\b|subsidized tenant|subsidized tenancy/.test(normalized);
}

function hasSection8Context(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\bsection 8\b(?!\.\d)|\bhud\b|housing choice voucher|\bvoucher\b|subsidized tenant|subsidized tenancy|housing assistance payment|federally subsidized housing/.test(
      normalizedText
    )
  );
}

function hasSection8RehabDrift(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /substantial rehabilitation|certificate of final completion|rules and regulations section 8\.12|section 8\.12|department of public works|current assessment/.test(
      normalizedText
    ) && !/\bhud\b|housing choice voucher|\bvoucher\b|housing assistance payment/.test(normalizedText)
  );
}

function isUnlawfulDetainerQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bunlawful detainer\b|notice to quit|three day notice|detainer action|eviction lawsuit/.test(normalized);
}

function hasUnlawfulDetainerContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /\bunlawful detainer\b|notice to quit|three day notice|detainer action|eviction lawsuit|summons and complaint|filed an unlawful detainer|eviction action/.test(
      normalizedText
    )
  );
}

function isSection8UnlawfulDetainerQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return isSection8Query(normalized) && (isUnlawfulDetainerQuery(normalized) || /\beviction action\b|\beviction\b/.test(normalized));
}

function hasSection827RentIncreaseDrift(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    /civil code section 827|section 827|rent increase|banked increase|capital improvement|passthrough/.test(normalizedText) &&
    !hasSection8Context(normalizedText) &&
    !hasUnlawfulDetainerContext(normalizedText)
  );
}

function rowMatchesReferencedJudge(row: ChunkRow, query: string, explicitJudgeFilters?: string[]): boolean {
  const rowJudge = canonicalizeJudgeName(row.authorName);
  if (!rowJudge) return false;
  const candidates = explicitJudgeFilters && explicitJudgeFilters.length > 0 ? explicitJudgeFilters : queryReferencesJudge(query);
  return candidates.some((judge) => normalizeJudgeLookupKey(judge) === normalizeJudgeLookupKey(rowJudge));
}

function isEvictionProtectionQuery(query: string): boolean {
  return (
    /\b(?:omi|wrongful eviction|awe|harassment|retaliation)\b/.test(normalize(query)) ||
    hasOwnerMoveInPhrase(query) ||
    isUnlawfulDetainerQuery(query)
  );
}

function isBuyoutQuery(query: string): boolean {
  return /\bbuyout\b/.test(normalize(query));
}

function isBuyoutPressureQuery(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bbuyout\b/.test(normalized) && /\b(?:pressure|pressured|pressuring|harass|harassing|harassment|coerce|coerced|coercion|coercive|threat|threaten|threatened)\b/.test(normalized);
}

function isRentReductionQuery(query: string): boolean {
  return /\brent reduction|decrease in services|housing services\b/.test(normalize(query));
}

function isNuisanceQuery(query: string): boolean {
  return /\bnuisance\b/.test(normalize(query));
}

function requiresStrongIssueEvidence(query: string): boolean {
  return (
    isCoolingIssueQuery(query) ||
    isEvictionProtectionQuery(query) ||
    isAccommodationQuery(query) ||
    isHomeownersExemptionQuery(query) ||
    isSection8Query(query) ||
    isBuyoutQuery(query) ||
    isRentReductionQuery(query) ||
    isNuisanceQuery(query) ||
    /\brepair notice|notice\b/.test(normalize(query))
  );
}

function containsWholeWord(text: string, term: string): boolean {
  const normalizedText = normalize(text);
  const normalizedTerm = normalize(term);
  if (!normalizedText || !normalizedTerm) return false;
  const regex = new RegExp(`(^|[^a-z0-9])${escapeRegex(normalizedTerm)}([^a-z0-9]|$)`, "i");
  return regex.test(normalizedText);
}

function hasOwnerMoveInPhrase(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\b(?:owner move(?:-|\s)?in|relative move(?:-|\s)?in)\b/.test(normalizedText);
}

function hasWrongfulEvictionPhrase(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return /\b(?:wrongful eviction|unlawful eviction|lockout|locked out|self[-\s]?help eviction)\b/.test(normalizedText);
}

function hasMoldCollision(text: string): boolean {
  const normalizedText = normalize(text);
  return normalizedText.includes("molding") && !containsWholeWord(normalizedText, "mold");
}

function hasCoolingProxyDrift(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const proxyTerms = [
    "replace fan",
    "bathroom fan",
    "ceiling fan",
    "exhaust fan",
    "capital improvement",
    "passthrough"
  ];
  const supportTerms = [
    "habitability",
    "heat",
    "overheating",
    "temperature",
    "air flow",
    "air circulation",
    "ventilation",
    "cooling",
    "stuffy"
  ];
  return proxyTerms.some((term) => normalizedText.includes(term)) && !supportTerms.some((term) => normalizedText.includes(term));
}

function isHousingServicesDefinitionBoilerplate(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    normalizedText.includes("housing services are those services provided by the landlord") ||
    normalizedText.includes("housing services are defined as those services provided by the landlord") ||
    normalizedText.includes("the use or occupancy of a rental unit including, but not limited to") ||
    normalizedText.includes("a decreased housing service petition can not be heard by the rent board") ||
    normalizedText.includes("decreased housing service petition cannot be heard by the rent board")
  );
}

function isOwnerMoveInLegalStandardBoilerplate(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    normalizedText.includes("with certain limited exceptions it shall be a defense to an owner move in eviction") ||
    normalizedText.includes("allows a landlord to recover possession of a rental unit for owner move in") ||
    normalizedText.includes("recover possession of the unit for owner occupancy")
  );
}

function hasBuyoutContext(text: string): boolean {
  const normalizedText = normalize(text);
  return (
    /buyout|buy out|settlement|rescission|disclosure/.test(normalizedText) ||
    /payment to vacate|payments to vacate|offer to vacate|offers to vacate|offer of payment to vacate|offers of payment to vacate/.test(
      normalizedText
    )
  );
}

function hasBuyoutPressureContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  const vacatePaymentContext =
    /payment to vacate|payments to vacate|offer to vacate|offers to vacate|offer of payment to vacate|offers of payment to vacate/.test(
      normalizedText
    );
  return (
    (hasBuyoutContext(normalizedText) || vacatePaymentContext) &&
    /pressure|pressured|pressuring|harass|harassing|harassment|coerce|coerced|coercion|coercive|threat|threaten|threatened|intimidation|fraud|cease and desist/.test(
      normalizedText
    )
  );
}

function hasOwnerMoveInContext(text: string): boolean {
  const normalizedText = normalize(text);
  return (
    /\b(?:omi|recover possession|owner occupancy|occupy the unit|occupied the unit|owner move(?:-|\s)?in eviction|relative move(?:-|\s)?in eviction)\b/.test(
      normalizedText
    ) ||
    hasOwnerMoveInPhrase(normalizedText)
  );
}

function hasOwnerMoveInFollowThroughContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /\b(?:never occupied|did not occupy|failed to occupy|never resided|did not reside|never moved in|did not move in|not occupy|not reside)\b/.test(
    normalizedText
  );
}

function hasOwnerMoveInOccupancyStandardContext(text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  return (
    normalizedText.includes("principal place of residence") ||
    normalizedText.includes("tenant in occupancy") ||
    normalizedText.includes("actually resides in a rental unit") ||
    normalizedText.includes("actually reside in a rental unit") ||
    normalizedText.includes("greater credibility to the finding of principal place of residence") ||
    normalizedText.includes("a landlord who seeks a determination") ||
    normalizedText.includes("definition of tenant")
  );
}

function requiresOwnerMoveInFollowThroughSpecificity(query: string): boolean {
  const normalizedText = normalize(query || "");
  if (!normalizedText) return false;
  return (
    (hasOwnerMoveInPhrase(normalizedText) || containsWholeWord(normalizedText, "omi")) &&
    /\b(?:never occupied|did not occupy|failed to occupy|never resided|did not reside|never moved in|did not move in|not occupy|not reside)\b/.test(
      normalizedText
    )
  );
}

function hasWrongfulEvictionContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /\b(?:wrongful eviction|report of alleged wrongful eviction|awe|unlawful eviction|lockout|locked out|self[-\s]?help eviction)\b/.test(normalizedText);
}

function hasWrongfulEvictionLockoutContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /\b(?:lockout|locked out|changed locks?|denied access|self[-\s]?help eviction|shut off utilities|utility shutoff)\b/.test(
    normalizedText
  );
}

function requiresLockoutSpecificity(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return (
    isWrongfulEvictionIssueSearch(normalized) &&
    /\b(?:lockout|locked out|changed locks?|denied access|self[-\s]?help|shut off utilities|utility shutoff)\b/.test(normalized)
  );
}

function hasHarassmentContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /harassment|harass|harassed|harassing|retaliation|37\.10b|wrongful endeavor/.test(normalizedText);
}

function hasRentReductionContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /rent reduction|decrease in services|housing services|corresponding rent reduction/.test(normalizedText);
}

function hasRepairNoticeContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /repair request|work order|notice|written notice|requested repairs|requests for repairs/.test(normalizedText);
}

function hasNuisanceContext(text: string): boolean {
  const normalizedText = normalize(text);
  return /nuisance|substantial nuisance|noise|waste|disturbance|tenant conduct/.test(normalizedText);
}

function hasWrongContextForQuery(query: string, text: string): boolean {
  const normalizedText = normalize(text);
  if (!normalizedText) return false;
  if (isPackageSecurityQuery(query)) {
    const packageSpecificSignal =
      /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom security\b|\bdelivery\b|\bpackage\b|\bpackages\b|\bmailroom\b|\bmail\b/.test(
        normalizedText
      );
    const packageCollateralDrift =
      /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
        normalizedText
      ) ||
      (
        /\bplanning code section 207\b|\baccessory dwelling unit\b|\badu\b/.test(normalizedText) &&
        !hasPackageDeliverySecurityContext(normalizedText) &&
        !/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bdelivered\b|\bsign for packages\b|\bapprehend\b/.test(
          normalizedText
        )
      ) ||
      (/loss of any tenant housing services|housing services reasonably expected|planning code section 207/.test(normalizedText) &&
        !packageSpecificSignal);
    if (packageCollateralDrift) return true;
  }
  if (isSection8UnlawfulDetainerQuery(query)) {
    return hasSection827RentIncreaseDrift(normalizedText);
  }
  if (isDogQuery(query)) {
    return /\bdogs?\b/.test(normalizedText) && !hasDogContext(normalizedText);
  }
  if (isCollegeQuery(query)) {
    return (
      /\bcommunity college district\b|\bschool district\b|\bgeneral obligation bonds?\b|\bbond passthrough\b|\bpassthrough\b/.test(normalizedText) &&
      !hasCollegeContext(normalizedText)
    );
  }
  if (isSelfEmployedQuery(query)) {
    return /\b1099\b|\btax return\b|\btax returns\b|\bbusiness\b/.test(normalizedText) && !hasSelfEmployedContext(normalizedText);
  }
  if (isAdjudicatedQuery(query)) {
    return /\bdecid(?:ed|e)\b|\bcourt\b/.test(normalizedText) && !hasAdjudicatedContext(normalizedText);
  }
  if (isSocialMediaQuery(query)) {
    const socialSecurityDrift =
      /\bsocial security\b|\bsocial security number\b|\bsupplemental security income\b|\bssi\b/.test(normalizedText);
    if (socialSecurityDrift) return true;
    return /\bfacebook\b|\binstagram\b|\bonline\b|\bposted\b/.test(normalizedText) && !hasSocialMediaContext(normalizedText);
  }
  if (isCaregiverQuery(query)) {
    return /\bcaregiver\b|\bcaretaker\b|\bcare\b/.test(normalizedText) && !hasCaregiverContext(normalizedText);
  }
  if (isPoopQuery(query)) {
    return /\bfeces\b|\bpoop\b|\bwaste\b/.test(normalizedText) && !hasPoopContext(normalizedText);
  }
  if (isMootQuery(query)) {
    return /\bnull and void\b|\brescinded\b|\bdismissed\b/.test(normalizedText) && !hasMootContext(normalizedText);
  }
  if (isRemoteWorkQuery(query)) {
    return /\bremote\b|\bwork\b/.test(normalizedText) && !hasRemoteWorkContext(normalizedText);
  }
  if (isDivorceQuery(query)) {
    return /\bspouse\b|\bhusband\b|\bwife\b/.test(normalizedText) && !hasDivorceContext(normalizedText);
  }
  if (isAccommodationQuery(query)) {
    return (
      hasPetPolicyDrift(normalizedText) ||
      (/reasonable costs?|reasonable time|reasonable period/.test(normalizedText) && !hasAccommodationContext(normalizedText))
    );
  }
  if (isCoolingIssueQuery(query)) {
    return hasCoolingProxyDrift(normalizedText);
  }
  if (isBuyoutQuery(query)) {
    return isCapitalImprovementBoilerplate(normalizedText) || /capital improvement|passthrough/.test(normalizedText);
  }
  if (isEvictionProtectionQuery(query) && !hasOwnerMoveInContext(normalizedText) && !hasWrongfulEvictionContext(normalizedText) && !hasHarassmentContext(normalizedText)) {
    return /condominium|tenants-in-common|homeowners association|capital improvement|passthrough/.test(normalizedText);
  }
  if (isRentReductionQuery(query)) {
    return /capital improvement|certified for|petitioned cost/.test(normalizedText) && !hasRentReductionContext(normalizedText);
  }
  if (isNuisanceQuery(query)) {
    return /notice to abate plumbing nuisance|abatement remediation/.test(normalizedText) && !/tenant conduct|noise|waste|disturbance/.test(normalizedText);
  }
  return false;
}

function hasStrongIssueEvidence(query: string, row: ChunkRow, issueTermHits: number, proceduralTermHits: number): boolean {
  const searchableText = combinedSearchableText(row);
  const normalizedText = normalize(searchableText);
  if (issueTermHits >= 2 || proceduralTermHits >= 2) return true;
  if (containsWholeWord(searchableText, query)) return true;
  if (isSection8UnlawfulDetainerQuery(query)) {
    return hasSection8Context(searchableText) && hasUnlawfulDetainerContext(searchableText);
  }
  if (isCameraPrivacyQuery(query)) return hasCameraPrivacyContext(searchableText);
  if (isPackageSecurityQuery(query)) {
    return (
      hasPackageDeliverySecurityContext(searchableText) ||
      ((/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bpackages\b/.test(normalizedText) &&
        /\btheft\b|\bstolen\b|\bthief\b|\bapprehend\b|\bsign for packages\b|\bsecure\b|\bdelivery person\b|\bextra keys?\b|\bentry\b|\baccess\b/.test(
          normalizedText
        )))
    );
  }
  if (isDogQuery(query)) return hasDogContext(searchableText);
  if (isCollegeQuery(query)) return hasCollegeContext(searchableText);
  if (isSelfEmployedQuery(query)) return hasSelfEmployedContext(searchableText);
  if (isAdjudicatedQuery(query)) return hasAdjudicatedContext(searchableText);
  if (isSocialMediaQuery(query)) return hasSocialMediaContext(searchableText);
  if (isCaregiverQuery(query)) return hasCaregiverContext(searchableText);
  if (isPoopQuery(query)) return hasPoopContext(searchableText);
  if (isMootQuery(query)) return hasMootContext(searchableText);
  if (isRemoteWorkQuery(query)) return hasRemoteWorkContext(searchableText);
  if (isDivorceQuery(query)) return hasDivorceContext(searchableText);
  if (isIntercomQuery(query)) return hasIntercomContext(searchableText);
  if (isGarageSpaceQuery(query)) return hasGarageSpaceContext(searchableText);
  if (isCommonAreasQuery(query)) return hasCommonAreasContext(searchableText);
  if (isStairsQuery(query)) return hasStairsContext(searchableText);
  if (isCoLivingQuery(query)) return hasCoLivingContext(searchableText);
  if (isHomeownersExemptionQuery(query)) return hasHomeownersExemptionContext(searchableText);
  if (isSection8Query(query)) return hasSection8Context(searchableText);
  if (isUnlawfulDetainerQuery(query)) return hasUnlawfulDetainerContext(searchableText);
  if (isAccommodationQuery(query)) return hasAccommodationContext(searchableText);
  if (isBuyoutPressureQuery(query)) return hasBuyoutPressureContext(searchableText);
  if (isBuyoutQuery(query)) return hasBuyoutContext(searchableText);
  if (/\brepair notice|notice\b/.test(normalize(query))) return hasRepairNoticeContext(searchableText);
  if (isRentReductionQuery(query)) return hasRentReductionContext(searchableText);
  if (isNuisanceQuery(query)) return hasNuisanceContext(searchableText);
  if (hasOwnerMoveInPhrase(query) || containsWholeWord(normalize(query), "omi") || /\bowner occupancy\b/.test(normalize(query))) {
    const conclusionsOccupancyProxy =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") && hasOwnerMoveInOccupancyStandardContext(searchableText);
    if (requiresOwnerMoveInFollowThroughSpecificity(query)) {
      return (
        (hasOwnerMoveInContext(searchableText) || conclusionsOccupancyProxy) &&
        (
          hasOwnerMoveInFollowThroughContext(searchableText) ||
          containsWholeWord(searchableText, "owner occupancy") ||
          conclusionsOccupancyProxy
        )
      );
    }
    return hasOwnerMoveInContext(searchableText);
  }
  if (hasWrongfulEvictionPhrase(query) || containsWholeWord(normalize(query), "awe")) return hasWrongfulEvictionContext(searchableText);
  if (/harassment|retaliation/.test(normalize(query))) return hasHarassmentContext(searchableText);
  return issueTermHits > 0 || proceduralTermHits > 0;
}

function buildSection8UdDocumentSupportSet(rows: ChunkRow[]): Set<string> {
  const byDocument = new Map<string, { hasSection8: boolean; hasUd: boolean }>();
  for (const row of rows) {
    const searchableText = combinedSearchableText(row);
    const current = byDocument.get(row.documentId) || { hasSection8: false, hasUd: false };
    if (hasSection8Context(searchableText)) current.hasSection8 = true;
    if (hasUnlawfulDetainerContext(searchableText)) current.hasUd = true;
    byDocument.set(row.documentId, current);
  }
  const supported = new Set<string>();
  for (const [documentId, state] of byDocument.entries()) {
    if (state.hasSection8 && state.hasUd) supported.add(documentId);
  }
  return supported;
}

function chunkMatchesSection8UdDocumentSupport(row: ChunkRow, section8UdDocumentSupportIds: Set<string>): boolean {
  if (!section8UdDocumentSupportIds.has(row.documentId)) return false;
  const searchableText = combinedSearchableText(row);
  return (
    hasSection8Context(searchableText) ||
    hasUnlawfulDetainerContext(searchableText) ||
    isConclusionsLikeSectionLabel(row.sectionLabel || "") ||
    normalizeChunkTypeLabel(row.sectionLabel || "") === "authority_discussion"
  );
}

function chunkQualifiesForSection8UdDocumentSupport(
  row: ChunkRow,
  diagnostics: RankingDiagnostics,
  section8UdDocumentSupportIds: Set<string>
): boolean {
  if (!chunkMatchesSection8UdDocumentSupport(row, section8UdDocumentSupportIds)) return false;
  if (isConclusionsLikeSectionLabel(row.sectionLabel || "")) {
    return diagnostics.sectionBoost >= 0.1 || diagnostics.lexicalScore >= 0.05 || diagnostics.vectorScore >= 0.3;
  }
  return (
    isFindingsLikeSectionLabel(row.sectionLabel || "") ||
    diagnostics.sectionBoost >= 0.12 ||
    diagnostics.lexicalScore >= 0.1 ||
    diagnostics.vectorScore >= 0.45
  );
}

function chunkMatchesIssueTerms(row: ChunkRow, query: string): boolean {
  const issueTerms = inferIssueTerms(query);
  if (!issueTerms.length) return false;
  const text = normalize(combinedSearchableText(row));
  return issueTerms.some((term) => text.includes(term));
}

function chunkMatchesProceduralTerms(row: ChunkRow, query: string): boolean {
  const proceduralTerms = inferProceduralTerms(query);
  if (!proceduralTerms.length) return false;
  const text = normalize(combinedSearchableText(row));
  return proceduralTerms.some((term) => text.includes(term));
}

function isCapitalImprovementBoilerplate(text: string): boolean {
  const normalized = normalize(text);
  if (!normalized) return false;
  return (
    normalized.includes("capital improvement shall be divided equally among all units") ||
    normalized.includes("cost attributable to vacant units") ||
    normalized.includes("rent cannot be raised") ||
    normalized.includes("improvements which materially add to the value of the property") ||
    normalized.includes("useful life of the improvement") ||
    normalized.includes("within six months of the commencement of capital improvement work")
  );
}

function buildLexicalMatchClause(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  terms: string[]
): { clause: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  const chunks = safeTerms.map(
    () =>
      `(instr(lower(${chunkExpr}), lower(?)) > 0 OR instr(lower(${citationExpr}), lower(?)) > 0 OR instr(lower(${titleExpr}), lower(?)) > 0 OR instr(lower(coalesce(${authorExpr}, '')), lower(?)) > 0)`
  );
  return {
    clause: `(${chunks.join(" OR ")})`,
    params: safeTerms.flatMap((term) => [term, term, term, term])
  };
}

function buildLexicalRankExpr(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  sectionExpr: string,
  terms: string[]
): { expr: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  return {
    expr: safeTerms
      .map(
        () =>
          `(
            CASE WHEN instr(lower(${titleExpr}), lower(?)) > 0 THEN 2.4 ELSE 0 END +
            CASE WHEN instr(lower(${citationExpr}), lower(?)) > 0 THEN 2.0 ELSE 0 END +
            CASE WHEN instr(lower(coalesce(${authorExpr}, '')), lower(?)) > 0 THEN 1.9 ELSE 0 END +
            CASE WHEN instr(lower(${sectionExpr}), lower(?)) > 0 THEN 1.4 ELSE 0 END +
            CASE WHEN instr(lower(${chunkExpr}), lower(?)) > 0 THEN 1.0 ELSE 0 END
          )`
      )
      .join(" + "),
    params: safeTerms.flatMap((term) => [term, term, term, term, term])
  };
}

function normalizedWholeWordExpr(expr: string): string {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' '), '/', ' ')
  ) || ' ')`;
}

function buildWholeWordLexicalMatchClause(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  terms: string[]
): { clause: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  const chunks = safeTerms.map(
    () =>
      `(instr(${normalizedWholeWordExpr(chunkExpr)}, ' ' || lower(?) || ' ') > 0 OR instr(lower(${citationExpr}), lower(?)) > 0 OR instr(${normalizedWholeWordExpr(titleExpr)}, ' ' || lower(?) || ' ') > 0 OR instr(${normalizedWholeWordExpr(authorExpr)}, ' ' || lower(?) || ' ') > 0)`
  );
  return {
    clause: `(${chunks.join(" OR ")})`,
    params: safeTerms.flatMap((term) => [term, term, term, term])
  };
}

function buildWholeWordLexicalRankExpr(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  sectionExpr: string,
  terms: string[]
): { expr: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  return {
    expr: safeTerms
      .map(
        () =>
          `(
            CASE WHEN instr(${normalizedWholeWordExpr(titleExpr)}, ' ' || lower(?) || ' ') > 0 THEN 2.4 ELSE 0 END +
            CASE WHEN instr(lower(${citationExpr}), lower(?)) > 0 THEN 2.0 ELSE 0 END +
            CASE WHEN instr(${normalizedWholeWordExpr(authorExpr)}, ' ' || lower(?) || ' ') > 0 THEN 1.9 ELSE 0 END +
            CASE WHEN instr(lower(${sectionExpr}), lower(?)) > 0 THEN 1.4 ELSE 0 END +
            CASE WHEN instr(${normalizedWholeWordExpr(chunkExpr)}, ' ' || lower(?) || ' ') > 0 THEN 1.0 ELSE 0 END
          )`
      )
      .join(" + "),
    params: safeTerms.flatMap((term) => [term, term, term, term, term])
  };
}

function uniq<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}

function isRetryableSearchError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return message.includes("error code: 1031") || message.includes("fetch failed");
}

function countBy(values: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function parseJsonList(input: string): string[] {
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function extractReferenceFamilyToken(value: string): string {
  const normalized = normalize(value || "");
  const direct = normalized.match(/\b\d+\.\d+\b/);
  if (direct?.[0]) return direct[0];
  return normalized;
}

function buildCitationFamilySignature(row: ChunkRow): string {
  const families = uniq([
    ...parseJsonList(row.ordinanceSectionsJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("ordinance_section", item))),
    ...parseJsonList(row.rulesSectionsJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("rules_section", item))),
    ...parseJsonList(row.indexCodesJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("index_code", item)))
  ].filter(Boolean)).sort((a, b) => a.localeCompare(b));
  if (!families.length) return "<none>";
  return families.join("|");
}

type QueryIntent =
  | "authority"
  | "findings"
  | "procedural"
  | "analysis"
  | "disposition"
  | "citation"
  | "comparative"
  | "unknown";

function normalizeChunkTypeLabel(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function inferQueryIntent(context: SearchContext): QueryIntent {
  if (context.queryType === "citation_lookup" || context.queryType === "rules_ordinance" || context.queryType === "index_code") {
    return "citation";
  }
  const q = normalize(context.query || "");
  if (!q) return "unknown";
  if (/ordinance|rule|rules|authority|section|citation/.test(q)) return "authority";
  if (/findings?|credibility|evidence|fact/.test(q)) return "findings";
  if (/procedural|history|hearing|notice|continuance|appearance/.test(q)) return "procedural";
  if (/issue|holding|disposition|order|decision/.test(q)) return "disposition";
  if (/analysis|reasoning|legal standard|application|conclusions?/.test(q)) return "analysis";
  if (/compare|comparison|across decisions|prior decisions/.test(q)) return "comparative";
  return "unknown";
}

function isConclusionsLikeSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  const normalized = normalizeChunkTypeLabel(raw);
  return (
    /conclusions? of law/i.test(raw) ||
    normalized === "conclusions_of_law" ||
    normalized === "authority_discussion" ||
    normalized === "analysis_reasoning"
  );
}

function isFindingsLikeSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  const normalized = normalizeChunkTypeLabel(raw);
  return (
    /findings? of fact/i.test(raw) ||
    normalized === "findings_of_fact" ||
    normalized === "findings" ||
    normalized === "factual_findings" ||
    normalized === "credibility_findings"
  );
}

function isSupportingFactSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  if (isFindingsLikeSectionLabel(raw)) return true;
  return /summary\s+of\s+the\s+evidence|factual\s+background|background|history|evidence|testimony/i.test(raw);
}

function sectionPriorityBoost(sectionLabel: string): { boost: number; why: string | null } {
  if (isConclusionsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.24, why: "section_priority_conclusions_of_law" };
  }
  if (isFindingsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.14, why: "section_priority_findings_of_fact" };
  }
  return { boost: 0, why: null };
}

function isSentenceStyleReasoningQuery(context: SearchContext): boolean {
  const raw = String(context.query || "").trim();
  const normalized = normalize(raw);
  if (!normalized) return false;
  if (
    context.queryType === "citation_lookup" ||
    context.queryType === "party_name" ||
    context.queryType === "rules_ordinance" ||
    context.queryType === "index_code"
  ) {
    return false;
  }

  const tokens = tokenize(raw).filter((token) => token.length > 2);
  if (tokens.length >= 9) return true;
  if (tokens.length >= 7) return true;
  if (raw.length >= 70) return true;
  if (/[,:;]/.test(raw) && tokens.length >= 6) return true;

  return /\b(?:burden of proving|failed to prove|did not establish|based on all of the evidence|credible testimony|substantial decrease(?: in housing services)?|good faith efforts|not justified|is justified|not entitled|entitled to|rent reduction|corresponding rent reduction|code violation)\b/.test(
    normalized
  );
}

function intentBoostForChunkType(intent: QueryIntent, chunkType: string): number {
  if (intent === "unknown" || intent === "comparative") return 0;
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return 0;

  if (intent === "citation" || intent === "authority") {
    if (/conclusions?_of_law/.test(t)) return 0.22;
    if (/(authority|analysis)/.test(t)) return 0.16;
    if (/findings?_of_fact|findings/.test(t)) return 0.08;
    if (/(caption|issue_statement)/.test(t)) return -0.2;
  }
  if (intent === "findings") {
    if (/findings?_of_fact|findings/.test(t)) return 0.2;
    if (/conclusions?_of_law/.test(t)) return 0.12;
    if (/analysis/.test(t)) return 0.1;
    if (/(caption|issue_statement)/.test(t)) return -0.16;
  }
  if (intent === "procedural") {
    if (/(procedural|background|history|order|conclusions?_of_law)/.test(t)) return 0.2;
    if (/(caption|issue_statement|allowable_rent_increase_s|cap_imp_pass_through|landlord_petitioner|tenant_petitioner)/.test(t)) {
      return -0.18;
    }
  }
  if (intent === "analysis") {
    if (/conclusions?_of_law/.test(t)) return 0.22;
    if (/(analysis|authority)/.test(t)) return 0.16;
    if (/findings?_of_fact|findings/.test(t)) return 0.1;
    if (/(caption|issue_statement)/.test(t)) return -0.18;
  }
  if (intent === "disposition") {
    if (/(holding|disposition|order|decision)/.test(t)) return 0.2;
    if (/(caption|issue_statement)/.test(t)) return -0.14;
  }
  return 0;
}

function isLowSignalStructuralChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function isLowSignalTabularChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(base|monthly|total|cost|allowable_rent_increase_s|cap_imp_pass_through)$/.test(t);
}

function isIssuePreferredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(findings_of_fact|order|conclusions_of_law|procedural_history|background)$/.test(t);
}

function isIssueDisfavoredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(minute_order|allowable_rent_increase_s|cap_imp_pass_through|base|monthly|total|cost|introduction|body)$/.test(t);
}

function isLowValueIssueIntentChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  if (isIssueDisfavoredChunkType(t) || isLowSignalTabularChunkType(t) || isLowSignalStructuralChunkType(t)) {
    return true;
  }
  if (/^(landlord_petitioner|tenant_petitioner|total_all_capital_improvements|caption_title)$/.test(t)) {
    return true;
  }
  return /^[a-z]?\d+[a-z]?$/.test(t);
}

function isLowSignalVectorOnlyChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return isLowSignalTabularChunkType(t) || /^(introduction|body|minute_order)$/.test(t);
}

function hasMalformedDocxArtifact(text: string): boolean {
  return /<w:[^>]+>/.test(String(text || ""));
}

function hasSevereExtractionArtifact(text: string): boolean {
  const raw = String(text || "");
  if (!raw) return false;
  return (
    /<w:[^>]+>/.test(raw) ||
    /w:(?:rPr|tabs|spacing|ind|rFonts|sz|lang|tab)\b/.test(raw) ||
    /<\/w:[^>]+>/.test(raw)
  );
}

function parseAnchorOrdinal(anchor: string): number | null {
  const raw = String(anchor || "");
  if (!raw) return null;
  const match = raw.match(/(?:^|[#:_-])p(\d+)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1] || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function isStructuralIntent(context: SearchContext): boolean {
  const q = normalize(context.query || "");
  if (!q) return false;
  if (context.queryType === "party_name") return true;
  return /appearance|appearances|caption|questions presented|parties/.test(q);
}

function chooseSnippet(text: string, context: SearchContext): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));

  if (isPackageSecurityQuery(context.query)) {
    const packageTargets = uniq([
      "package theft",
      "stolen packages",
      "mail theft",
      "mailroom security",
      "delivery security",
      "secure package delivery",
      "sign for packages",
      "delivery person",
      "packages shipped",
      "package",
      "packages",
      "mailroom",
      ...inferIssueTerms(context.query).filter((term) => normalize(term) !== "housing service"),
      ...sentenceIssueAnchorTerms(context.query),
      ...sentenceSecondaryFactTokens(context.query)
    ]).filter((value): value is string => Boolean(value));

    return chooseSnippetForTargets(normalized, packageTargets, maxSnippetChars);
  }

  const targets = uniq([
    context.query,
    context.retrievalQuery,
    context.filters.indexCode,
    ...(context.filters.indexCodes || []),
    context.filters.rulesSection,
    context.filters.ordinanceSection,
    context.filters.partyName,
    ...inferIssueTerms(context.query),
    ...inferProceduralTerms(context.query),
    ...tokenize(context.query).filter((token) => token.length > 3)
  ])
    .filter((value): value is string => Boolean(value));

  return chooseSnippetForTargets(normalized, targets, maxSnippetChars);
}

function chooseSnippetForTargets(normalizedText: string, rawTargets: string[], maxSnippetChars: number): string {
  const normalized = String(normalizedText || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const targets = uniq(rawTargets)
    .filter((value): value is string => Boolean(value))
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);
  if (targets.length === 0) return normalized.slice(0, maxSnippetChars);

  const lower = normalized.toLowerCase();
  let bestSnippet = "";
  let bestScore = -1;
  const lead = Math.max(60, Math.floor(maxSnippetChars * 0.35));
  const trail = Math.max(60, maxSnippetChars - lead);

  for (const target of targets) {
    const loweredTarget = target.toLowerCase();
    let fromIndex = 0;
    while (fromIndex < lower.length) {
      const idx = lower.indexOf(loweredTarget, fromIndex);
      if (idx < 0) break;
      const start = Math.max(0, idx - lead);
      const end = Math.min(normalized.length, idx + target.length + trail);
      const candidate = normalized.slice(start, end);
      const candidateLower = candidate.toLowerCase();
      const hitCount = targets.filter((item) => candidateLower.includes(item.toLowerCase())).length;
      const score = hitCount * 10 + Math.min(5, target.length / 12);
      if (score > bestScore) {
        bestScore = score;
        bestSnippet = candidate;
      }
      fromIndex = idx + loweredTarget.length;
    }
  }

  if (bestSnippet) return bestSnippet;
  return normalized.slice(0, maxSnippetChars);
}

function chooseSupportingFactSnippet(text: string, context: SearchContext): string {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const factTargets = new Set<string>([
    ...sentenceIssueAnchorTerms(context.query),
    ...sentenceSecondaryFactTokens(context.query),
    ...primaryIssueSignals(context.query),
    ...inferIssueTerms(context.query)
  ]);

  const normalizedQuery = normalize(context.query || "");
  if (requiresLockoutSpecificity(context.query)) {
    [
      "lockout",
      "locked out",
      "changed locks",
      "denied access",
      "self-help eviction",
      "shut off utilities",
      "utility shutoff"
    ].forEach((item) => factTargets.add(item));
  }
  if (hasHabitabilityServiceRestorationSignals(context.query)) {
    [
      "reported",
      "complained",
      "notified",
      "notice",
      "repair request",
      "failed to repair",
      "did not repair",
      "refused to repair",
      "not repaired",
      "restore service",
      "failed to restore",
      "service restoration",
      "restored service"
    ].forEach((item) => factTargets.add(item));
  }
  if (hasOwnerMoveInPhrase(normalizedQuery) || containsWholeWord(normalizedQuery, "omi")) {
    [
      "owner occupancy",
      "occupancy",
      "occupy",
      "occupied",
      "reside",
      "resided",
      "never occupied",
      "did not occupy",
      "failed to occupy",
      "never resided",
      "did not reside",
      "never moved in",
      "did not move in"
    ].forEach((item) => factTargets.add(item));
  }

  return chooseSnippetForTargets(normalized, Array.from(factTargets), maxSnippetChars);
}

function buildLayeredResultSnippet(
  context: SearchContext,
  primaryAuthorityPassage?: SearchResultPassage,
  supportingFactPassage?: SearchResultPassage,
  supportingFactDebug?: SupportingFactDebug,
  fallbackText?: string
): string {
  const fallbackSnippet = fallbackText ? chooseSnippet(fallbackText, context) : "";
  const authoritySnippet = String(primaryAuthorityPassage?.snippet || "").trim();
  const factSnippet = String(supportingFactPassage?.snippet || "").trim();
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const sentenceAnchors = sentenceIssueAnchorTerms(context.query);
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query);
  const authorityAnchorHits = sentenceAnchors.filter((term) => normalize(authoritySnippet).includes(normalize(term))).length;
  const authoritySecondaryHits = sentenceSecondaryTokens.filter((term) => normalize(authoritySnippet).includes(normalize(term))).length;
  const factAnchorHits = sentenceAnchors.filter((term) => normalize(factSnippet).includes(normalize(term))).length;
  const factSecondaryHits = sentenceSecondaryTokens.filter((term) => normalize(factSnippet).includes(normalize(term))).length;
  const authorityFactualMetrics = authoritySnippet ? sentenceFactualTokenMetrics(context.query, authoritySnippet) : { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  const factFactualMetrics = factSnippet ? sentenceFactualTokenMetrics(context.query, factSnippet) : { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  const factSupportStrong =
    Boolean(factSnippet) &&
    Boolean(
      (supportingFactDebug?.anchorHits ?? 0) > 0 ||
        (supportingFactDebug?.secondaryHits ?? 0) > 0 ||
        (supportingFactDebug?.coverageRatio ?? 0) >= 0.25 ||
        (supportingFactDebug?.factualAnchorScore ?? 0) >= 0.18
    );
  const authoritySupportScore =
    authorityAnchorHits * 0.32 +
    authoritySecondaryHits * 0.22 +
    authorityFactualMetrics.coverageRatio * 0.55 +
    (authorityFactualMetrics.matchedCount >= 2 ? 0.12 : 0);
  const factSupportScore =
    factAnchorHits * 0.2 +
    factSecondaryHits * 0.16 +
    factFactualMetrics.coverageRatio * 0.42 +
    factFactualMetrics.proximityBoost +
    ((supportingFactDebug?.factualAnchorScore ?? 0) * 0.18) +
    ((supportingFactDebug?.anchorHits ?? 0) * 0.05) +
    ((supportingFactDebug?.secondaryHits ?? 0) * 0.04) +
    (isFindingsLikeSectionLabel(supportingFactPassage?.sectionLabel || "") ? 0.1 : 0) +
    (/procedural|background|history/i.test(supportingFactPassage?.sectionLabel || "") ? 0.05 : 0);
  const authorityConclusionLike = isConclusionsLikeSectionLabel(primaryAuthorityPassage?.sectionLabel || "");
  const factEvidenceLike =
    isFindingsLikeSectionLabel(supportingFactPassage?.sectionLabel || "") ||
    /procedural|background|history/i.test(supportingFactPassage?.sectionLabel || "");
  const authorityHasQueryFamilyContext =
    (isAccommodationQuery(context.query) && hasAccommodationContext(authoritySnippet)) ||
    (isBuyoutQuery(context.query) && hasBuyoutContext(authoritySnippet)) ||
    (isSection8UnlawfulDetainerQuery(context.query) &&
      hasSection8Context(authoritySnippet) &&
      hasUnlawfulDetainerContext(authoritySnippet)) ||
    (hasHabitabilityServiceRestorationSignals(context.query) &&
      habitabilityCoverageSignals(authoritySnippet, context.query).conditionSignalHits > 0) ||
    (sentenceAnchors.length > 0 && authorityAnchorHits > 0);
  const factHasQueryFamilyContext =
    (isAccommodationQuery(context.query) && hasAccommodationContext(factSnippet)) ||
    (isBuyoutQuery(context.query) && hasBuyoutContext(factSnippet)) ||
    (isSection8UnlawfulDetainerQuery(context.query) && hasSection8Context(factSnippet) && hasUnlawfulDetainerContext(factSnippet)) ||
    (hasHabitabilityServiceRestorationSignals(context.query) && habitabilityCoverageSignals(factSnippet, context.query).conditionSignalHits > 0) ||
    factAnchorHits > 0 ||
    factSecondaryHits > 0;
  const authorityHasComparableFactualSupport =
    authoritySupportScore >= Math.max(0.3, factSupportScore - 0.1) ||
    authorityAnchorHits > factAnchorHits ||
    authoritySecondaryHits > factSecondaryHits;

  if (!isSentenceStyleReasoningQuery(context)) {
    if (isLiteralKeywordQuery(context.query)) {
      return fallbackSnippet || authoritySnippet || factSnippet;
    }
    return authoritySnippet || factSnippet || fallbackSnippet;
  }

  const shouldLeadWithFactSnippet =
    requiresLockoutSpecificity(context.query) &&
    Boolean(factSnippet) &&
    hasWrongfulEvictionLockoutContext(factSnippet) &&
    Boolean(authoritySnippet) &&
    (
      isHousingServicesDefinitionBoilerplate(authoritySnippet) ||
      isGenericAweDecisionLayer({
        primaryAuthorityPassage,
        supportingFactPassage,
        supportingFactDebug
      }) ||
      (!hasWrongfulEvictionLockoutContext(authoritySnippet) &&
        (hasWrongfulEvictionContext(authoritySnippet) || hasHarassmentContext(authoritySnippet) || hasRepairNoticeContext(authoritySnippet)))
    );
  const sentenceFactFirstPreferred =
    factSupportStrong &&
    Boolean(authoritySnippet) &&
    !authorityHasComparableFactualSupport &&
    (
      hasHabitabilityServiceRestorationSignals(context.query) ||
      isAccommodationQuery(context.query) ||
      isBuyoutQuery(context.query) ||
      isSection8UnlawfulDetainerQuery(context.query) ||
      sentenceAnchors.length > 0 ||
      (factEvidenceLike && factSupportScore >= authoritySupportScore + 0.14) ||
      (authorityConclusionLike && factSupportScore >= authoritySupportScore + 0.08) ||
      (!authorityHasQueryFamilyContext && factHasQueryFamilyContext) ||
      (authorityConclusionLike && factEvidenceLike && factSupportScore >= 0.32 && factHasQueryFamilyContext)
    );
  const forceFactFirstSnippet =
    factSupportStrong &&
    Boolean(factSnippet) &&
    factHasQueryFamilyContext &&
    (
      !authorityHasQueryFamilyContext ||
      (factEvidenceLike && factSupportScore >= authoritySupportScore + 0.22) ||
      ((supportingFactDebug?.factualAnchorScore ?? 0) >= 0.5 && factEvidenceLike && authorityConclusionLike)
    );

  if (shouldLeadWithFactSnippet || sentenceFactFirstPreferred || forceFactFirstSnippet) {
    const separator = "  ";
    const combined = `${factSnippet}${separator}${authoritySnippet}`.replace(/\s+/g, " ").trim();
    if (combined.length <= maxSnippetChars) return combined;
    const factBudget = Math.max(90, Math.floor(maxSnippetChars * 0.62));
    const authorityBudget = Math.max(35, maxSnippetChars - factBudget - separator.length);
    const factPart = factSnippet.slice(0, factBudget).trim();
    const authorityPart = authoritySnippet.slice(0, authorityBudget).trim();
    return `${factPart}${separator}${authorityPart}`.trim();
  }

  if (authoritySnippet && factSnippet && authoritySnippet !== factSnippet) {
    const separator = "  ";
    const combined = `${authoritySnippet}${separator}${factSnippet}`.replace(/\s+/g, " ").trim();
    if (combined.length <= maxSnippetChars) return combined;
    const authorityBudget = Math.max(90, Math.floor(maxSnippetChars * 0.58));
    const factBudget = Math.max(40, maxSnippetChars - authorityBudget - separator.length);
    const authorityPart = authoritySnippet.slice(0, authorityBudget).trim();
    const factPart = factSnippet.slice(0, factBudget).trim();
    return `${authorityPart}${separator}${factPart}`.trim();
  }

  return authoritySnippet || factSnippet || fallbackSnippet;
}

function lexicalScore(text: string, query: string): number {
  const terms = meaningfulLexicalTokens(query);
  if (terms.length === 0) return 0;
  const lower = normalize(text);
  let hits = 0;
  let occurrences = 0;
  for (const term of terms) {
    if (lower.includes(term)) {
      hits += 1;
      occurrences += lower.split(term).length - 1;
    }
  }
  const coverage = hits / terms.length;
  const density = Math.min(1, occurrences / Math.max(2, terms.length * 2));
  return Number((coverage * 0.75 + density * 0.25).toFixed(6));
}

function buildSearchScope(
  parsed: SearchRequest,
  corpusMode: SearchRequest["corpusMode"],
  options: SearchScopeOptions = {}
) {
  const hasActiveRetrievalChunkClause =
    "EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const hasBasicChunkedDecisionClause =
    "EXISTS (SELECT 1 FROM document_chunks c_basic WHERE c_basic.document_id = d.id) AND d.source_r2_key IS NOT NULL AND COALESCE(d.title, '') != ''";
  const clauses: string[] = [
    corpusMode === "trusted_plus_provisional"
      ? `(d.file_type != 'decision_docx' OR ${hasBasicChunkedDecisionClause} OR ${hasActiveRetrievalChunkClause})`
      : `(d.file_type != 'decision_docx' OR ${hasActiveRetrievalChunkClause})`,
    "d.rejected_at IS NULL"
  ];
  const params: Array<string | number> = [];

  if (parsed.filters.jurisdiction) {
    clauses.push("d.jurisdiction = ?");
    params.push(parsed.filters.jurisdiction);
  }

  if (parsed.filters.documentId) {
    clauses.push("d.id = ?");
    params.push(parsed.filters.documentId);
  }

  if (parsed.filters.fileType) {
    clauses.push("d.file_type = ?");
    params.push(parsed.filters.fileType);
  } else {
    // Product search is decision-first by default; other file types remain available via explicit filter.
    clauses.push("d.file_type = 'decision_docx'");
  }

  const indexCodeFilterContext = buildIndexCodeFilterContext(parsed.filters, { includeGenericDhsFamilyAlias: false });
  const useSoftIndexCodeScope = Boolean(options.useSoftIndexCodeScope);
  if (indexCodeFilterContext.requestedCodes.length > 0 && !useSoftIndexCodeScope) {
    const compatibilityClauses: string[] = [];

    const directIndexCodeValues = uniq([...indexCodeFilterContext.requestedCodes, ...indexCodeFilterContext.legacyCodeAliases]).filter(Boolean);

    if (directIndexCodeValues.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'index_code'
            AND l.is_valid = 1
            AND (${directIndexCodeValues
              .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))")
              .join(" OR ")})
        )`
      );
      for (const code of directIndexCodeValues) {
        params.push(normalizeFilterValue("index_code", code), code);
      }
    }

    if (indexCodeFilterContext.relatedRulesSections.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'rules_section'
            AND l.is_valid = 1
            AND (${indexCodeFilterContext.relatedRulesSections
              .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))")
              .join(" OR ")})
        )`
      );
      for (const rulesCitation of indexCodeFilterContext.relatedRulesSections) {
        params.push(normalizeFilterValue("rules_section", rulesCitation), rulesCitation);
      }
    }

    if (indexCodeFilterContext.relatedOrdinanceSections.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'ordinance_section'
            AND l.is_valid = 1
            AND (${indexCodeFilterContext.relatedOrdinanceSections
              .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))")
              .join(" OR ")})
        )`
      );
      for (const ordinanceCitation of indexCodeFilterContext.relatedOrdinanceSections) {
        params.push(normalizeFilterValue("ordinance_section", ordinanceCitation), ordinanceCitation);
      }
    }

    // Phrase-based compatibility is intentionally applied later during reranking/filtering.
    // Keeping it out of the SQL scope avoids expensive chunk-text EXISTS scans that can timeout
    // on broad local searches while still allowing phrase evidence to drive ranking.
    if (compatibilityClauses.length > 0) {
      clauses.push(`(${compatibilityClauses.join(" OR ")})`);
    }
  }

  if (parsed.filters.rulesSection) {
    clauses.push(
      `EXISTS (
        SELECT 1 FROM document_reference_links l
        WHERE l.document_id = d.id
          AND l.reference_type = 'rules_section'
          AND l.is_valid = 1
          AND (l.normalized_value = ? OR lower(l.canonical_value) = lower(?))
      )`
    );
    params.push(normalizeFilterValue("rules_section", parsed.filters.rulesSection), parsed.filters.rulesSection);
  }

  if (parsed.filters.ordinanceSection) {
    clauses.push(
      `EXISTS (
        SELECT 1 FROM document_reference_links l
        WHERE l.document_id = d.id
          AND l.reference_type = 'ordinance_section'
          AND l.is_valid = 1
          AND (l.normalized_value = ? OR lower(l.canonical_value) = lower(?))
      )`
    );
    params.push(normalizeFilterValue("ordinance_section", parsed.filters.ordinanceSection), parsed.filters.ordinanceSection);
  }

  if (parsed.filters.partyName) {
    clauses.push("instr(lower(d.title), lower(?)) > 0");
    params.push(parsed.filters.partyName);
  }

  const judgeFilters = requestedJudgeFilters(parsed.filters);
  if (judgeFilters.length > 0) {
    clauses.push(`(${judgeFilters.map(() => "lower(coalesce(d.author_name, '')) = lower(?)").join(" OR ")})`);
    params.push(...judgeFilters);
  }

  if (parsed.filters.fromDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.decision_date >= ?)");
    params.push(parsed.filters.fromDate);
  }

  if (parsed.filters.toDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.decision_date <= ?)");
    params.push(parsed.filters.toDate);
  }

  if (parsed.filters.approvedOnly) {
    clauses.push(`(d.file_type != 'decision_docx' OR d.approved_at IS NOT NULL OR ${hasActiveRetrievalChunkClause})`);
  }

  return { where: `WHERE ${clauses.join(" AND ")}`, params };
}

function activeStructuredFilterKinds(filters: SearchRequest["filters"]): string[] {
  const kinds: string[] = [];
  if (requestedJudgeFilters(filters).length > 0) kinds.push("judge");
  if (requestedIndexCodeFilters(filters).length > 0) kinds.push("index_code");
  if (filters.rulesSection) kinds.push("rules_section");
  if (filters.ordinanceSection) kinds.push("ordinance_section");
  if (filters.partyName) kinds.push("party_name");
  if (filters.fromDate || filters.toDate) kinds.push("date_range");
  return kinds;
}

function isShortBroadIssueSearch(parsed: SearchRequest): boolean {
  if (isKeywordFamilyRecallQuery(parsed.query || "")) return false;
  const tokens = tokenize(parsed.query || "");
  if (tokens.length === 0 || tokens.length > 3) return false;
  return inferIssueTerms(parsed.query || "").length > 0;
}

function isIssueGuidedSearch(parsed: SearchRequest): boolean {
  if (isKeywordFamilyRecallQuery(parsed.query || "")) return false;
  const tokens = tokenize(parsed.query || "");
  if (tokens.length === 0 || tokens.length > 16) return false;
  return inferIssueTerms(parsed.query || "").length > 0;
}

function hasExplicitOrdinance379Mention(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\b(?:ordinance|section)?\s*37\.9\b/.test(normalized);
}

function isOwnerMoveInIssueSearch(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return hasOwnerMoveInPhrase(normalized) || containsWholeWord(normalized, "omi");
}

function isWrongfulEvictionIssueSearch(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return hasWrongfulEvictionPhrase(normalized) || containsWholeWord(normalized, "awe");
}

type SearchScopeOptions = {
  useSoftIndexCodeScope?: boolean;
};

function issueQueryIndexCodeHints(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  const hints = new Set<string>();

  if (/\bheat|heating|heater|boiler|radiator\b/.test(normalized)) {
    hints.add("G49");
    hints.add("G50");
  }
  if (/\bhot water\b/.test(normalized)) {
    hints.add("G52");
    hints.add("G53");
  }
  if (/\bmold|leak|water intrusion|plumbing|sewage\b/.test(normalized)) {
    hints.add("G64");
  }
  if (/\bcockroach\b/.test(normalized)) {
    hints.add("G44");
    hints.add("G54");
  }
  if (/\brodent\b/.test(normalized)) {
    hints.add("G76");
    hints.add("G54");
  }
  if (/\bbed bug|bed bugs\b/.test(normalized)) {
    hints.add("G40.1");
    hints.add("G54");
  }
  if (isInfestationAliasQuery(normalized)) {
    hints.add("G54");
    hints.add("G44");
    hints.add("G76");
    hints.add("G40.1");
  }
  if (/\brent reduction|decrease in services|housing services\b/.test(normalized)) {
    hints.add("G27");
    hints.add("G28");
  }

  return Array.from(hints);
}

function issueQueryPhraseHints(query: string): string[] {
  const normalized = normalize(query || "");
  if (!normalized) return [];
  const hints = new Set<string>(inferIssueTerms(query));
  const hasOmiAcronym = containsWholeWord(normalized, "omi");
  const hasAweAcronym = containsWholeWord(normalized, "awe");

  if (hasOwnerMoveInPhrase(normalized) || hasOmiAcronym) {
    hints.add("owner move-in");
    hints.add("relative move-in");
    hints.add("owner occupancy");
    hints.add("recover possession");
    if (requiresOwnerMoveInFollowThroughSpecificity(normalized)) {
      hints.add("never moved in");
      hints.add("did not move in");
      hints.add("never occupied");
      hints.add("did not occupy");
      hints.add("never resided");
      hints.add("did not reside");
    }
    if (hasExplicitOrdinance379Mention(query)) hints.add("section 37.9");
    if (hasOmiAcronym) hints.add("omi");
  }
  if (/\bharassment|retaliation\b/.test(normalized)) {
    hints.add("harassment");
    hints.add("tenant harassment");
    hints.add("retaliation");
    hints.add("section 37.10b");
  }
  if (/\bbuyout\b/.test(normalized)) {
    hints.add("buyout");
    hints.add("buyout agreement");
    hints.add("buyout negotiations");
    hints.add("disclosure");
    hints.add("rescission");
    if (isBuyoutPressureQuery(normalized)) {
      hints.add("pressure");
      hints.add("pressured");
      hints.add("pressuring");
      hints.add("harassment");
      hints.add("harassing");
      hints.add("coerce");
      hints.add("coerced");
      hints.add("coercion");
      hints.add("coercive");
      hints.add("buyout coercion");
      hints.add("coercive buyout");
      hints.add("payment to vacate");
      hints.add("payments to vacate");
      hints.add("offer to vacate");
      hints.add("offers to vacate");
      hints.add("offer of payment to vacate");
      hints.add("offers of payment to vacate");
      hints.add("threats or intimidation");
      hints.add("fraud intimidation or coercion");
      hints.add("threat");
      hints.add("threaten");
      hints.add("threatened");
    }
  }
  if (isInfestationAliasQuery(normalized)) {
    hints.add("infestation");
    hints.add("infestations");
    hints.add("rodent infestation");
    hints.add("cockroach infestation");
    hints.add("bed bug infestation");
    hints.add("rodent");
    hints.add("cockroach");
    hints.add("bed bug");
    hints.add("mice");
    hints.add("rats");
    hints.add("pest control");
  }
  if (hasWrongfulEvictionPhrase(normalized) || hasAweAcronym) {
    hints.add("wrongful eviction");
    hints.add("report of alleged wrongful eviction");
    hints.add("unlawful eviction");
    hints.add("lockout");
    hints.add("locked out");
    hints.add("changed locks");
    hints.add("denied access");
    hints.add("shut off utilities");
    hints.add("self-help eviction");
    if (hasAweAcronym) hints.add("awe");
  }
  if (isAccommodationQuery(normalized)) {
    hints.add("reasonable accommodation");
    hints.add("service animal");
    hints.add("support animal");
    hints.add("emotional support animal");
    hints.add("accommodation request");
  }
  if (isSection8Query(normalized)) {
    hints.add("section 8");
    hints.add("hud");
    hints.add("housing choice voucher");
    hints.add("voucher");
  }
  if (isUnlawfulDetainerQuery(normalized)) {
    hints.add("unlawful detainer");
    hints.add("notice to quit");
    hints.add("three day notice");
    hints.add("detainer action");
    hints.add("eviction action");
    hints.add("eviction");
  }
  if (isSection8UnlawfulDetainerQuery(normalized)) {
    hints.add("section 8 eviction");
    hints.add("section 8 eviction action");
    hints.add("voucher eviction");
    hints.add("housing choice voucher eviction");
  }
  if (/\bcapital improvement\b/.test(normalized)) {
    hints.add("capital improvement");
    hints.add("passthrough");
  }

  const hintedCodes = issueQueryIndexCodeHints(query);
  if (hintedCodes.length > 0) {
    const syntheticFilters = { approvedOnly: false, indexCodes: hintedCodes } as SearchRequest["filters"];
    const issueContext = buildIndexCodeFilterContext(syntheticFilters, { includeGenericDhsFamilyAlias: false });
    for (const phrase of issueContext.searchPhrases) hints.add(phrase);
    for (const rulesCitation of issueContext.relatedRulesSections) hints.add(rulesCitation);
    for (const ordinanceCitation of issueContext.relatedOrdinanceSections) hints.add(ordinanceCitation);
  }

  return Array.from(hints).filter(Boolean).slice(0, 8);
}

function issueQueryReferenceHints(query: string): { rulesSections: string[]; ordinanceSections: string[] } {
  const normalized = normalize(query || "");
  if (!normalized) return { rulesSections: [], ordinanceSections: [] };

  const rulesSections = new Set<string>();
  const ordinanceSections = new Set<string>();

  if (hasExplicitOrdinance379Mention(query)) {
    ordinanceSections.add("37.9");
  }
  if (hasWrongfulEvictionPhrase(normalized) || containsWholeWord(normalized, "awe")) {
    ordinanceSections.add("37.9");
  }
  if (/\bharassment|retaliation\b/.test(normalized)) {
    ordinanceSections.add("37.10B");
    ordinanceSections.add("37.10b");
  }
  if (/\bcapital improvement\b/.test(normalized)) {
    rulesSections.add("VII-7.12");
  }

  return {
    rulesSections: Array.from(rulesSections),
    ordinanceSections: Array.from(ordinanceSections)
  };
}

function lockoutScopePhraseHints(query: string): string[] {
  if (!requiresLockoutSpecificity(query)) return [];
  const normalized = normalize(query || "");
  const hints = new Set<string>([
    "lockout",
    "locked out",
    "changed locks",
    "denied access",
    "self-help eviction",
    "shut off utilities",
    "utility shutoff"
  ]);
  if (normalized.includes("utility")) {
    hints.add("utility shutoff");
    hints.add("shut off utilities");
  }
  if (normalized.includes("repair")) {
    hints.add("complained about repairs");
    hints.add("requested repairs");
  }
  return Array.from(hints);
}

function requiresHabitabilitySpecificity(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  const conditionSignals = requiredHabitabilityPrimarySignals(query);
  if (conditionSignals.length === 0) return false;
  const hasReportingSignals = /\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(normalized);
  const hasRepairSignals = /\brepair|repairs|restore|restored|service|services\b/.test(normalized);
  return hasReportingSignals || hasRepairSignals;
}

function habitabilityScopePhraseHints(query: string): { conditionSignals: string[]; reportingHints: string[]; repairHints: string[] } {
  const conditionSignals = requiredHabitabilityPrimarySignals(query);
  if (!requiresHabitabilitySpecificity(query)) {
    return { conditionSignals, reportingHints: [], repairHints: [] };
  }
  const normalized = normalize(query || "");
  const reportingHints = [
    "reported",
    "complained",
    "notified",
    "notice",
    "repair request",
    "work order"
  ].filter((term) => normalized.includes(normalize(term)) || /report|complain|notified|notice|repair request|work order/.test(normalized));
  const repairHints = [
    "failed to repair",
    "did not repair",
    "refused to repair",
    "not repaired",
    "failed to restore",
    "restore service",
    "service restoration"
  ].filter((term) => normalized.includes(normalize(term)) || /repair|restore|service/.test(normalized));
  return {
    conditionSignals,
    reportingHints: uniq(reportingHints),
    repairHints: uniq(repairHints)
  };
}

async function fetchHabitabilityCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const { conditionSignals, reportingHints, repairHints } = habitabilityScopePhraseHints(query);
  if (conditionSignals.length === 0) return [];

  const conditionClause = conditionSignals.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ");
  const reportingClause = reportingHints.length > 0 ? reportingHints.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ") : "0";
  const repairClause = repairHints.length > 0 ? repairHints.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ") : "0";
  const conditionScoreExpr = conditionSignals.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const reportingScoreExpr = reportingHints.length > 0 ? reportingHints.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ") : "0";
  const repairScoreExpr = repairHints.length > 0 ? repairHints.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ") : "0";
  const sectionBoostExpr = `CASE
    WHEN lower(s.section_label) LIKE '%finding%' THEN 0.45
    WHEN lower(s.section_label) LIKE '%background%' OR lower(s.section_label) LIKE '%evidence%' OR lower(s.section_label) LIKE '%testimony%' THEN 0.35
    WHEN lower(s.section_label) LIKE '%conclusion%' THEN 0.18
    ELSE 0
  END`;

  const conditionBindings = conditionSignals.map((signal) => `%${normalize(signal)}%`);
  const reportingBindings = reportingHints.map((signal) => `%${normalize(signal)}%`);
  const repairBindings = repairHints.map((signal) => `%${normalize(signal)}%`);

  try {
    const rows = await env.DB.prepare(
      `SELECT
         scope.documentId
       FROM (
         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           s.section_label,
           s.chunk_text,
           (${conditionScoreExpr}) * 6 + (${reportingScoreExpr}) * 2.5 + (${repairScoreExpr}) * 3 + ${sectionBoostExpr} as scopeScore
         FROM (
           SELECT rs.document_id, rs.section_label, rs.chunk_text
           FROM retrieval_search_chunks rs
           WHERE rs.active = 1
           UNION ALL
           SELECT c.document_id, c.section_label, c.chunk_text
           FROM document_chunks c
         ) s
         JOIN documents d ON d.id = s.document_id
         ${where}
           AND (${conditionClause})
           AND ((${reportingClause}) OR (${repairClause}))
       ) scope
       GROUP BY scope.documentId
       ORDER BY
         MAX(scope.scopeScore) DESC,
         MAX(COALESCE(scope.searchableAt, '')) DESC,
         MAX(COALESCE(scope.decisionDate, '')) DESC,
         scope.documentId ASC
       LIMIT ?`
    )
      .bind(
        ...conditionBindings,
        ...reportingBindings,
        ...repairBindings,
        ...params,
        ...conditionBindings,
        ...reportingBindings,
        ...repairBindings,
        limit
      )
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

async function fetchLockoutCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const phraseHints = lockoutScopePhraseHints(query);
  if (phraseHints.length === 0) return [];

  const likeBindings = phraseHints.map((phrase) => `%${normalize(phrase)}%`);
  const retrievalTextHitClause = phraseHints.map(() => "lower(rs.chunk_text) LIKE ?").join(" OR ");
  const retrievalTextHitScoreExpr = phraseHints.map(() => "CASE WHEN lower(rs.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const retrievalSectionBoostExpr = `CASE
    WHEN lower(rs.section_label) LIKE '%conclusion%' THEN 0.55
    WHEN lower(rs.section_label) LIKE '%finding%' THEN 0.4
    WHEN lower(rs.section_label) LIKE '%background%' OR lower(rs.section_label) LIKE '%evidence%' OR lower(rs.section_label) LIKE '%testimony%' THEN 0.3
    ELSE 0
  END`;
  const documentTextHitClause = phraseHints.map(() => "lower(c.chunk_text) LIKE ?").join(" OR ");
  const documentTextHitScoreExpr = phraseHints.map(() => "CASE WHEN lower(c.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const documentSectionBoostExpr = `CASE
    WHEN lower(c.section_label) LIKE '%conclusion%' THEN 0.55
    WHEN lower(c.section_label) LIKE '%finding%' THEN 0.4
    WHEN lower(c.section_label) LIKE '%background%' OR lower(c.section_label) LIKE '%evidence%' OR lower(c.section_label) LIKE '%testimony%' THEN 0.3
    ELSE 0
  END`;

  try {
    const rows = await env.DB.prepare(
      `SELECT
         scope.documentId
       FROM (
         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           rs.section_label as section_label,
           rs.chunk_text as chunk_text,
           ${retrievalTextHitScoreExpr} + ${retrievalSectionBoostExpr} + 0.2 as scopeScore
         FROM retrieval_search_chunks rs
         JOIN documents d ON d.id = rs.document_id
         ${where}
           AND rs.active = 1
           AND (${retrievalTextHitClause})

         UNION ALL

         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           c.section_label as section_label,
           c.chunk_text as chunk_text,
           ${documentTextHitScoreExpr} + ${documentSectionBoostExpr} as scopeScore
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
           AND (${documentTextHitClause})
       ) scope
       GROUP BY scope.documentId
       ORDER BY
         MAX(scope.scopeScore) DESC,
         MAX(COALESCE(scope.searchableAt, '')) DESC,
         MAX(COALESCE(scope.decisionDate, '')) DESC,
         scope.documentId ASC
       LIMIT ?`
    )
      .bind(
        ...likeBindings,
        ...params,
        ...likeBindings,
        ...likeBindings,
        ...params,
        ...likeBindings,
        limit
      )
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

async function fetchIssueCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];

  const documentIds: string[] = [];
  const pushIds = (ids: string[]) => {
    for (const id of ids) {
      if (!id || documentIds.includes(id)) continue;
      documentIds.push(id);
      if (documentIds.length >= limit) break;
    }
  };

  const codeHints = issueQueryIndexCodeHints(query);
  const phraseHints = issueQueryPhraseHints(query);
  const manualReferenceHints = issueQueryReferenceHints(query);
  const ownerMoveInSearch = isOwnerMoveInIssueSearch(query);

  const hasReferenceDrivenHints =
    codeHints.length > 0 || manualReferenceHints.rulesSections.length > 0 || manualReferenceHints.ordinanceSections.length > 0;

  if (hasReferenceDrivenHints) {
    const filterContext = buildIndexCodeFilterContext(
      { approvedOnly: false, indexCodes: codeHints } as SearchRequest["filters"],
      { includeGenericDhsFamilyAlias: false }
    );
    const hintedRulesSections = uniq([...filterContext.relatedRulesSections, ...manualReferenceHints.rulesSections]).filter(Boolean);
    const hintedOrdinanceSections = uniq([...filterContext.relatedOrdinanceSections, ...manualReferenceHints.ordinanceSections]).filter(Boolean);

    const clauses: string[] = [];
    const bindings: Array<string | number> = [];
    const directCodes = uniq([...codeHints, ...filterContext.legacyCodeAliases]).filter(Boolean);

    if (directCodes.length > 0) {
      clauses.push(
        `(l.reference_type = 'index_code' AND (${directCodes
          .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))")
          .join(" OR ")}))`
      );
      for (const code of directCodes) bindings.push(normalizeFilterValue("index_code", code), code);
    }

    if (hintedRulesSections.length > 0) {
      clauses.push(
        `(l.reference_type = 'rules_section' AND (${hintedRulesSections
          .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?) OR lower(coalesce(l.canonical_value, '')) LIKE lower(?))")
          .join(" OR ")}))`
      );
      for (const rulesCitation of hintedRulesSections) {
        bindings.push(normalizeFilterValue("rules_section", rulesCitation), rulesCitation, `${rulesCitation}%`);
      }
    }

    if (hintedOrdinanceSections.length > 0) {
      clauses.push(
        `(l.reference_type = 'ordinance_section' AND (${hintedOrdinanceSections
          .map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?) OR lower(coalesce(l.canonical_value, '')) LIKE lower(?))")
          .join(" OR ")}))`
      );
      for (const ordinanceCitation of hintedOrdinanceSections) {
        bindings.push(normalizeFilterValue("ordinance_section", ordinanceCitation), ordinanceCitation, `${ordinanceCitation}%`);
      }
    }

    if (clauses.length > 0) {
      try {
        const rows = await env.DB.prepare(
          `SELECT DISTINCT d.id as documentId
           FROM document_reference_links l
           JOIN documents d ON d.id = l.document_id
           ${where}
             AND l.is_valid = 1
             AND (${clauses.join(" OR ")})
           ORDER BY COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
           LIMIT ?`
        )
          .bind(...params, ...bindings, limit)
          .all<{ documentId: string }>();
        pushIds((rows.results || []).map((row) => row.documentId));
      } catch (error) {
        if (!isRetryableSearchError(error)) throw error;
      }
    }
  }

  if (documentIds.length > 0 && hasReferenceDrivenHints) return documentIds.slice(0, limit);

  if (documentIds.length === 0 && ownerMoveInSearch) {
    try {
      const rows = await env.DB.prepare(
        `SELECT d.id as documentId
         FROM documents d
         ${where}
           AND lower(coalesce(d.ordinance_sections_json, '')) LIKE lower(?)
         ORDER BY COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
         LIMIT ?`
      )
        .bind(...params, "%37.9%", Math.max(limit - documentIds.length, 1))
        .all<{ documentId: string }>();
      pushIds((rows.results || []).map((row) => row.documentId));
    } catch (error) {
      if (!isRetryableSearchError(error)) throw error;
    }
  }

  if (documentIds.length === 0 && isVectorFirstIssueSearch(query)) return [];
  if (documentIds.length >= limit || phraseHints.length === 0) return documentIds.slice(0, limit);

  const phraseQuery = uniq([query, ...phraseHints]).filter(Boolean).join(" ");
  const match = buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", lexicalTerms(phraseQuery));
  const rank = buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", lexicalTerms(phraseQuery));

  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${rank.expr}) as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${where}
         AND rs.active = 1
         AND ${match.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...rank.params, ...params, ...match.params, Math.max(limit - documentIds.length, 1))
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  return documentIds.slice(0, limit);
}

async function fetchKeywordCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = []
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  if (scopedDocumentIds.length > maxKeywordCandidateDocumentBatchSize) {
    const out: string[] = [];
    for (let index = 0; index < scopedDocumentIds.length; index += maxKeywordCandidateDocumentBatchSize) {
      const batch = scopedDocumentIds.slice(index, index + maxKeywordCandidateDocumentBatchSize);
      const ids = await fetchKeywordCandidateDocumentIds(env, where, params, query, limit, batch);
      for (const id of ids) {
        if (!id || out.includes(id)) continue;
        out.push(id);
        if (out.length >= limit) return out;
      }
    }
    return out;
  }

  const terms = keywordCandidateTerms(query);
  if (terms.length === 0) return [];
  const wholeWordGuarded = keywordBoundaryGuardTerms(query).length > 0;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;

  const documentIds: string[] = [];
  const pushIds = (ids: string[]) => {
    for (const id of ids) {
      if (!id || documentIds.includes(id)) continue;
      documentIds.push(id);
      if (documentIds.length >= limit) break;
    }
  };

  const retrievalMatch = wholeWordGuarded
    ? buildWholeWordLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms)
    : buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const retrievalRank = wholeWordGuarded
    ? buildWholeWordLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms)
    : buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms);
  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${retrievalRank.expr}) as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${documentScopeClause}
         AND rs.active = 1
         AND ${retrievalMatch.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...retrievalRank.params, ...documentScopeParams, ...retrievalMatch.params, limit)
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  if (documentIds.length >= limit) return documentIds.slice(0, limit);

  const fallbackMatch = wholeWordGuarded
    ? buildWholeWordLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms)
    : buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const fallbackRank = wholeWordGuarded
    ? buildWholeWordLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms)
    : buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms);
  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${fallbackRank.expr}) as lexicalRank
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${documentScopeClause}
         AND ${fallbackMatch.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...fallbackRank.params, ...documentScopeParams, ...fallbackMatch.params, Math.max(limit - documentIds.length, 1))
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  return documentIds.slice(0, limit);
}

function buildAdaptiveRecallConfig(parsed: SearchRequest, pageWindow: number) {
  const activeKinds = activeStructuredFilterKinds(parsed.filters);
  const hasStructuredFilters = activeKinds.length > 0;
  const hasCombinedStructuredFilters = activeKinds.length >= 2;
  const issueGuidedSearch = isIssueGuidedSearch(parsed);
  const shortBroadIssueSearch = isShortBroadIssueSearch(parsed);
  const sentenceIssueSearch = issueGuidedSearch && !shortBroadIssueSearch;
  const shortBroadUnfilteredIssueSearch = shortBroadIssueSearch && !hasStructuredFilters;
  const shortBroadFilteredIssueSearch = shortBroadIssueSearch && hasStructuredFilters;
  const sentenceIssueUnfilteredSearch = sentenceIssueSearch && !hasStructuredFilters;
  const sentenceIssueFilteredSearch = sentenceIssueSearch && hasStructuredFilters;

  const lexicalSearchLimit = Math.min(
    shortBroadFilteredIssueSearch
      ? 48
      : shortBroadUnfilteredIssueSearch
        ? 80
        : sentenceIssueFilteredSearch
          ? 72
          : sentenceIssueUnfilteredSearch
            ? 96
        : 2400,
    shortBroadFilteredIssueSearch
      ? pageWindow * 2
      : shortBroadUnfilteredIssueSearch
        ? pageWindow * 4
        : sentenceIssueFilteredSearch
          ? pageWindow * 3
          : sentenceIssueUnfilteredSearch
            ? pageWindow * 4
        : hasCombinedStructuredFilters
          ? pageWindow * 20
          : hasStructuredFilters
            ? pageWindow * 15
            : pageWindow * 12
  );
  const vectorSearchLimit = Math.min(
    shortBroadIssueSearch ? 30 : 120,
    shortBroadIssueSearch
      ? Math.max(pageWindow, 10)
      : hasCombinedStructuredFilters
        ? pageWindow * 2
        : pageWindow
  );
  const decisionScopeDocumentLimit = parsed.filters.documentId
    ? 1
    : Math.min(
        shortBroadFilteredIssueSearch
          ? 10
          : shortBroadUnfilteredIssueSearch
            ? 12
            : sentenceIssueFilteredSearch
              ? 12
              : sentenceIssueUnfilteredSearch
                ? 14
            : 160,
        shortBroadFilteredIssueSearch
          ? Math.max(pageWindow, 8)
          : shortBroadUnfilteredIssueSearch
            ? Math.max(pageWindow, 10)
            : sentenceIssueFilteredSearch
              ? Math.max(pageWindow, 10)
              : sentenceIssueUnfilteredSearch
                ? Math.max(pageWindow, 12)
            : hasCombinedStructuredFilters
              ? Math.max(pageWindow * 8, 24)
              : hasStructuredFilters
                ? Math.max(pageWindow * 5, 16)
              : Math.max(pageWindow * 4, 12)
      );
  const fallbackDocumentLimit = parsed.filters.documentId
    ? 0
    : Math.min(
        issueGuidedSearch
          ? 0
          : 140,
        hasCombinedStructuredFilters
            ? Math.max(pageWindow * 10, 40)
            : hasStructuredFilters
              ? Math.max(pageWindow * 6, 24)
              : 0
      );
  const lexicalScopeDocumentLimit = parsed.filters.documentId
    ? 1
    : Math.min(
        shortBroadFilteredIssueSearch
          ? 12
          : shortBroadUnfilteredIssueSearch
            ? 0
            : sentenceIssueFilteredSearch
              ? 16
              : sentenceIssueUnfilteredSearch
                ? 18
            : 96,
        shortBroadFilteredIssueSearch
          ? Math.max(pageWindow, 8)
          : sentenceIssueSearch
            ? Math.max(decisionScopeDocumentLimit, pageWindow * 2)
          : Math.max(fallbackDocumentLimit, decisionScopeDocumentLimit, pageWindow * 10)
      );

  return {
    activeKinds,
    hasStructuredFilters,
    hasCombinedStructuredFilters,
    issueGuidedSearch,
    shortBroadIssueSearch,
    lexicalScopeDocumentLimit,
    lexicalSearchLimit,
    vectorSearchLimit,
    decisionScopeDocumentLimit,
    fallbackDocumentLimit
  };
}

async function hasAnyExactIndexCodeCoverage(env: Env, filters: SearchRequest["filters"]): Promise<boolean> {
  const normalizedCodes = uniq(
    requestedIndexCodeFilters(filters).map((value) => normalizeFilterValue("index_code", value)).filter(Boolean)
  );
  if (!normalizedCodes.length) return false;

  for (let index = 0; index < normalizedCodes.length; index += maxSqliteIdBatchSize) {
    const batch = normalizedCodes.slice(index, index + maxSqliteIdBatchSize);
    const codeClause = batch.map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))").join(" OR ");
    const bindings = batch.flatMap((code) => [code, code]);
    try {
      const rows = await env.DB.prepare(
        `SELECT 1
         FROM document_reference_links l
         JOIN documents d ON d.id = l.document_id
         WHERE d.rejected_at IS NULL
           AND d.file_type = 'decision_docx'
           AND l.reference_type = 'index_code'
           AND l.is_valid = 1
           AND (${codeClause})
         LIMIT 1`
      )
        .bind(...bindings)
        .all<{ "1": number }>();
      if ((rows.results || []).length > 0) return true;
    } catch (error) {
      if (isRetryableSearchError(error)) return false;
      throw error;
    }
  }

  return false;
}

async function lexicalSearch(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; termsOverride?: string[] }
): Promise<ChunkRow[]> {
  const terms = options?.termsOverride?.length ? options.termsOverride : lexicalTerms(query);
  if (!terms.length) return [];
  if (scopedDocumentIds.length > maxScopedLexicalDocumentBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < scopedDocumentIds.length; index += maxScopedLexicalDocumentBatchSize) {
      const batch = scopedDocumentIds.slice(index, index + maxScopedLexicalDocumentBatchSize);
      out.push(...(await lexicalSearch(env, where, params, query, limit, batch, options)));
    }
    return out
      .sort((a, b) => {
        const rankDiff = Number(b.lexicalRank || 0) - Number(a.lexicalRank || 0);
        if (rankDiff !== 0) return rankDiff;
        const searchableDiff = String(b.searchableAt || "").localeCompare(String(a.searchableAt || ""));
        if (searchableDiff !== 0) return searchableDiff;
        return Number(a.orderRank || 0) - Number(b.orderRank || 0);
      })
      .slice(0, limit);
  }
  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;
  const primaryMatch = buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const activatedMatch = buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const primaryRank = buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms);
  const activatedRank = buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms);
  try {
    const rows = await env.DB.prepare(
    `SELECT * FROM (
       SELECT
         c.id as chunkId,
         d.id as documentId,
         d.title,
         d.citation,
         d.author_name as authorName,
         d.decision_date as decisionDate,
         d.file_type as fileType,
         d.source_r2_key as sourceFileRef,
         d.source_link as sourceLink,
         d.index_codes_json as indexCodesJson,
         d.rules_sections_json as rulesSectionsJson,
         d.ordinance_sections_json as ordinanceSectionsJson,
         c.section_label as sectionLabel,
         c.paragraph_anchor as paragraphAnchor,
         c.citation_anchor as citationAnchor,
         c.chunk_text as chunkText,
         c.created_at as createdAt,
         CASE WHEN EXISTS (
           SELECT 1 FROM retrieval_search_chunks rs_active
           WHERE rs_active.document_id = d.id AND rs_active.active = 1
         ) THEN 1 ELSE 0 END as isTrustedTier,
         d.searchable_at as searchableAt,
         c.chunk_order as orderRank,
         ${primaryRank.expr} as lexicalRank
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${documentScopeClause}
       AND ${primaryActiveClause}
       AND ${primaryMatch.clause}

       UNION ALL

       SELECT
         rs.chunk_id as chunkId,
         d.id as documentId,
         d.title,
         d.citation,
         d.author_name as authorName,
         d.decision_date as decisionDate,
         d.file_type as fileType,
         d.source_r2_key as sourceFileRef,
         d.source_link as sourceLink,
         d.index_codes_json as indexCodesJson,
         d.rules_sections_json as rulesSectionsJson,
         d.ordinance_sections_json as ordinanceSectionsJson,
         rs.section_label as sectionLabel,
         rs.paragraph_anchor as paragraphAnchor,
         rs.citation_anchor as citationAnchor,
         rs.chunk_text as chunkText,
         rs.created_at as createdAt,
         1 as isTrustedTier,
         d.searchable_at as searchableAt,
         999999 as orderRank,
         ${activatedRank.expr} as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${documentScopeClause}
       AND rs.active = 1
       AND ${activatedMatch.clause}
     )
     ORDER BY lexicalRank DESC, searchableAt DESC, orderRank ASC
     LIMIT ?`
    )
      .bind(
        ...primaryRank.params,
        ...documentScopeParams,
        ...primaryMatch.params,
        ...activatedRank.params,
        ...documentScopeParams,
        ...activatedMatch.params,
        limit
      )
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    const fallbackMatch = buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms);
    const fallbackRank = buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms);
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${documentScopeClause}
         AND ${primaryActiveClause}
         AND ${fallbackMatch.clause}
         ORDER BY ${fallbackRank.expr} DESC, d.searchable_at DESC, c.chunk_order ASC
         LIMIT ?`
      )
        .bind(...documentScopeParams, ...fallbackMatch.params, ...fallbackRank.params, limit)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

async function lexicalSearchWholeWord(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; termsOverride?: string[] }
): Promise<ChunkRow[]> {
  const terms = options?.termsOverride?.length ? options.termsOverride : wholeWordLexicalTerms(query);
  if (!terms.length) return [];
  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;
  const primaryMatch = buildWholeWordLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const activatedMatch = buildWholeWordLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const primaryRank = buildWholeWordLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms);
  const activatedRank = buildWholeWordLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms);
  try {
    const rows = await env.DB.prepare(
      `SELECT * FROM (
         SELECT
           c.id as chunkId,
           d.id as documentId,
           d.title,
           d.citation,
           d.author_name as authorName,
         d.decision_date as decisionDate,
           d.file_type as fileType,
           d.source_r2_key as sourceFileRef,
           d.source_link as sourceLink,
           d.index_codes_json as indexCodesJson,
           d.rules_sections_json as rulesSectionsJson,
           d.ordinance_sections_json as ordinanceSectionsJson,
           c.section_label as sectionLabel,
           c.paragraph_anchor as paragraphAnchor,
           c.citation_anchor as citationAnchor,
           c.chunk_text as chunkText,
           c.created_at as createdAt,
           CASE WHEN EXISTS (
             SELECT 1 FROM retrieval_search_chunks rs_active
             WHERE rs_active.document_id = d.id AND rs_active.active = 1
           ) THEN 1 ELSE 0 END as isTrustedTier,
           d.searchable_at as searchableAt,
           c.chunk_order as orderRank,
           ${primaryRank.expr} as lexicalRank
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${documentScopeClause}
         AND ${primaryActiveClause}
         AND ${primaryMatch.clause}

         UNION ALL

         SELECT
           rs.chunk_id as chunkId,
           d.id as documentId,
           d.title,
           d.citation,
           d.author_name as authorName,
         d.decision_date as decisionDate,
           d.file_type as fileType,
           d.source_r2_key as sourceFileRef,
           d.source_link as sourceLink,
           d.index_codes_json as indexCodesJson,
           d.rules_sections_json as rulesSectionsJson,
           d.ordinance_sections_json as ordinanceSectionsJson,
           rs.section_label as sectionLabel,
           rs.paragraph_anchor as paragraphAnchor,
           rs.citation_anchor as citationAnchor,
           rs.chunk_text as chunkText,
           rs.created_at as createdAt,
           1 as isTrustedTier,
           d.searchable_at as searchableAt,
           999999 as orderRank,
           ${activatedRank.expr} as lexicalRank
         FROM retrieval_search_chunks rs
         JOIN documents d ON d.id = rs.document_id
         ${documentScopeClause}
         AND rs.active = 1
         AND ${activatedMatch.clause}
       )
       ORDER BY lexicalRank DESC, searchableAt DESC, orderRank ASC
       LIMIT ?`
    )
      .bind(
        ...primaryRank.params,
        ...documentScopeParams,
        ...primaryMatch.params,
        ...activatedRank.params,
        ...documentScopeParams,
        ...activatedMatch.params,
        limit
      )
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

async function vectorSearch(env: Env, queries: string[], limit: number): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  if (!env.AI) {
    return out;
  }
  const queryList = uniq(
    queries
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );
  if (!queryList.length) return out;

  for (const query of queryList) {
    let vector: number[] | null = null;
    try {
      vector = await embed(env, query);
    } catch (error) {
      if (isRetryableSearchError(error)) continue;
      throw error;
    }
    if (!vector) continue;

    try {
      const matches = await env.VECTOR_INDEX.query(vector, {
        topK: Math.min(25, Math.max(limit * 2, 10)),
        namespace: env.VECTOR_NAMESPACE,
        returnMetadata: true
      });

      for (const match of matches.matches) {
        const score = typeof match.score === "number" ? Math.max(0, Math.min(1, match.score)) : 0;
        const prior = out.get(match.id) ?? 0;
        if (score > prior) out.set(match.id, score);
      }
    } catch {
      try {
        const matches = await env.VECTOR_INDEX.query(vector, {
          topK: Math.min(25, Math.max(limit * 2, 10)),
          returnMetadata: true
        });

        for (const match of matches.matches) {
          const score = typeof match.score === "number" ? Math.max(0, Math.min(1, match.score)) : 0;
          const prior = out.get(match.id) ?? 0;
          if (score > prior) out.set(match.id, score);
        }
      } catch {
        // Vectorize query contract can vary across local/remote proxy modes.
      }
    }
  }

  return out;
}

async function vectorSearchWithDiagnostics(env: Env, queries: string[], limit: number): Promise<{
  scores: Map<string, number>;
  aiAvailable: boolean;
  vectorQueryAttempted: boolean;
  vectorMatchCount: number;
}> {
  const aiAvailable = Boolean(env.AI);
  if (!aiAvailable) {
    return {
      scores: new Map(),
      aiAvailable,
      vectorQueryAttempted: false,
      vectorMatchCount: 0
    };
  }

  let scores = new Map<string, number>();
  try {
    scores = await vectorSearch(env, queries, limit);
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }
  return {
    scores,
    aiAvailable,
    vectorQueryAttempted: true,
    vectorMatchCount: scores.size
  };
}

async function fetchScopedDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  try {
    const rows = await env.DB.prepare(
      `SELECT
        d.id as documentId
       FROM documents d
       ${where}
       ORDER BY
         CASE WHEN EXISTS (
           SELECT 1 FROM retrieval_search_chunks rs_active
           WHERE rs_active.document_id = d.id AND rs_active.active = 1
         ) THEN 1 ELSE 0 END DESC,
         COALESCE(d.decision_date, '') DESC,
         COALESCE(d.searchable_at, '') DESC,
         d.id ASC
       LIMIT ?`
    )
      .bind(...params, limit)
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

async function fetchChunksByIds(
  env: Env,
  chunkIds: string[],
  where: string,
  params: Array<string | number>
): Promise<ChunkRow[]> {
  if (chunkIds.length === 0) return [];
  if (chunkIds.length > maxSqliteIdBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < chunkIds.length; index += maxSqliteIdBatchSize) {
      const batch = chunkIds.slice(index, index + maxSqliteIdBatchSize);
      out.push(...(await fetchChunksByIds(env, batch, where, params)));
    }
    return out;
  }
  const placeholders = chunkIds.map(() => "?").join(",");
  try {
    const rows = await env.DB.prepare(
    `SELECT
      c.id as chunkId,
      d.id as documentId,
      d.title,
      d.citation,
      d.author_name as authorName,
         d.decision_date as decisionDate,
      d.file_type as fileType,
      d.source_r2_key as sourceFileRef,
      d.source_link as sourceLink,
      d.index_codes_json as indexCodesJson,
      d.rules_sections_json as rulesSectionsJson,
      d.ordinance_sections_json as ordinanceSectionsJson,
      c.section_label as sectionLabel,
      c.paragraph_anchor as paragraphAnchor,
      c.citation_anchor as citationAnchor,
      c.chunk_text as chunkText,
      c.created_at as createdAt,
      CASE WHEN EXISTS (
        SELECT 1 FROM retrieval_search_chunks rs_active
        WHERE rs_active.document_id = d.id AND rs_active.active = 1
      ) THEN 1 ELSE 0 END as isTrustedTier
     FROM document_chunks c
     JOIN documents d ON d.id = c.document_id
     ${where}
     AND c.id IN (${placeholders})

     UNION ALL

     SELECT
      rs.chunk_id as chunkId,
      d.id as documentId,
      d.title,
      d.citation,
      d.author_name as authorName,
         d.decision_date as decisionDate,
      d.file_type as fileType,
      d.source_r2_key as sourceFileRef,
      d.source_link as sourceLink,
      d.index_codes_json as indexCodesJson,
      d.rules_sections_json as rulesSectionsJson,
      d.ordinance_sections_json as ordinanceSectionsJson,
      rs.section_label as sectionLabel,
      rs.paragraph_anchor as paragraphAnchor,
      rs.citation_anchor as citationAnchor,
      rs.chunk_text as chunkText,
      rs.created_at as createdAt,
      1 as isTrustedTier
     FROM retrieval_search_chunks rs
     JOIN documents d ON d.id = rs.document_id
     ${where}
     AND rs.active = 1
     AND rs.chunk_id IN (${placeholders})`
    )
      .bind(...params, ...chunkIds, ...params, ...chunkIds)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
         AND c.id IN (${placeholders})`
      )
        .bind(...params, ...chunkIds)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

async function fetchChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  if (documentIds.length > maxSqliteIdBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < documentIds.length; index += maxSqliteIdBatchSize) {
      const batch = documentIds.slice(index, index + maxSqliteIdBatchSize);
      out.push(...(await fetchChunksByDocumentIds(env, batch, where, params)));
    }
    return out;
  }
  const placeholders = documentIds.map(() => "?").join(",");
  try {
    const rows = await env.DB.prepare(
      `SELECT
        c.id as chunkId,
        d.id as documentId,
        d.title,
        d.citation,
        d.author_name as authorName,
         d.decision_date as decisionDate,
        d.file_type as fileType,
        d.source_r2_key as sourceFileRef,
        d.source_link as sourceLink,
        d.index_codes_json as indexCodesJson,
        d.rules_sections_json as rulesSectionsJson,
        d.ordinance_sections_json as ordinanceSectionsJson,
        c.section_label as sectionLabel,
        c.paragraph_anchor as paragraphAnchor,
        c.citation_anchor as citationAnchor,
        c.chunk_text as chunkText,
        c.created_at as createdAt,
        CASE WHEN EXISTS (
          SELECT 1 FROM retrieval_search_chunks rs_active
          WHERE rs_active.document_id = d.id AND rs_active.active = 1
        ) THEN 1 ELSE 0 END as isTrustedTier
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${where}
       AND d.id IN (${placeholders})

       UNION ALL

       SELECT
        rs.chunk_id as chunkId,
        d.id as documentId,
        d.title,
        d.citation,
        d.author_name as authorName,
         d.decision_date as decisionDate,
        d.file_type as fileType,
        d.source_r2_key as sourceFileRef,
        d.source_link as sourceLink,
        d.index_codes_json as indexCodesJson,
        d.rules_sections_json as rulesSectionsJson,
        d.ordinance_sections_json as ordinanceSectionsJson,
        rs.section_label as sectionLabel,
        rs.paragraph_anchor as paragraphAnchor,
        rs.citation_anchor as citationAnchor,
        rs.chunk_text as chunkText,
        rs.created_at as createdAt,
        1 as isTrustedTier
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${where}
       AND rs.active = 1
       AND d.id IN (${placeholders})`
    )
      .bind(...params, ...documentIds, ...params, ...documentIds)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
         AND d.id IN (${placeholders})`
      )
        .bind(...params, ...documentIds)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

async function fetchSupportingFactChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>,
  context?: SearchContext
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  const allRows = await fetchChunksByDocumentIds(env, documentIds, where, params);
  const supportRows = allRows.filter((row) => isSupportingFactSectionLabel(row.sectionLabel || ""));
  if (!context || !hasHabitabilityServiceRestorationSignals(context.query)) {
    return supportRows;
  }

  const requiredConditionSignals = requiredHabitabilityPrimarySignals(context.query);
  const normalizedQuery = normalize(context.query || "");
  const wantsReportingSignals = /\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(normalizedQuery);
  const wantsRepairFailureSignals = /\brepair|repairs|restore|restored|service|services\b/.test(normalizedQuery);
  const reportingPatterns = [
    /\breport(?:ed|ing)?\b/g,
    /\bcomplain(?:ed|ing)?\b/g,
    /\bnotified\b/g,
    /\bnotice\b/g,
    /\brepair request\b/g,
    /\bwork order\b/g
  ];
  const repairFailurePatterns = [
    /\bfailed to repair\b/g,
    /\bdid not repair\b/g,
    /\brefused to repair\b/g,
    /\bnot repaired\b/g,
    /\bfailed to restore\b/g,
    /\bdid not restore\b/g,
    /\brestore service\b/g,
    /\bservice restoration\b/g,
    /\brestored service\b/g
  ];

  const grouped = new Map<string, ChunkRow[]>();
  for (const row of supportRows) {
    const current = grouped.get(row.documentId) || [];
    current.push(row);
    grouped.set(row.documentId, current);
  }

  const prioritized: ChunkRow[] = [];
  for (const rows of grouped.values()) {
    const scored = rows
      .map((row) => {
        const normalizedText = normalize(row.chunkText || "");
        const conditionHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
        const reportingHits = reportingPatterns.reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
        const repairFailureHits = repairFailurePatterns.reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
        let priorityScore = conditionHits * 6;
        if (wantsReportingSignals) priorityScore += Math.min(4, reportingHits * 2);
        if (wantsRepairFailureSignals) priorityScore += Math.min(6, repairFailureHits * 2);
        if (isFindingsLikeSectionLabel(row.sectionLabel || "")) priorityScore += 1.5;
        return { row, priorityScore };
      })
      .sort((a, b) => b.priorityScore - a.priorityScore);

    const targeted = scored.filter((item) => item.priorityScore > 0).map((item) => item.row);
    prioritized.push(...(targeted.length > 0 ? targeted : rows));
  }

  return prioritized;
}

async function fetchAuthorityChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  const allRows = await fetchChunksByDocumentIds(env, documentIds, where, params);
  return allRows.filter((row) => isConclusionsLikeSectionLabel(row.sectionLabel || ""));
}

function scoreRow(row: ChunkRow, vectorScore: number, context: SearchContext): RankingDiagnostics {
  const why: string[] = [];
  const searchableText = combinedSearchableText(row);
  const lexical = lexicalScore(searchableText, context.retrievalQuery);
  const loweredSnippet = normalize(searchableText);
  const loweredQuery = normalize(context.query);
  const queryIntent = inferQueryIntent(context);
  const issueTerms = inferIssueTerms(context.query);
  const hasIssueTerms = issueTerms.length > 0;
  const issueTermHits = issueTerms.filter((term) => loweredSnippet.includes(term)).length;
  const primarySignals = primaryIssueSignals(context.query);
  const primarySignalHits = primarySignals.filter((signal) => textContainsIssueSignal(loweredSnippet, signal)).length;
  const sentenceIssueAnchors = sentenceIssueAnchorTerms(context.query);
  const sentenceIssueAnchorHits = sentenceIssueAnchors.filter((term) => loweredSnippet.includes(normalize(term))).length;
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query);
  const sentenceSecondaryHits = sentenceSecondaryTokens.filter((term) => loweredSnippet.includes(normalize(term))).length;
  const sentenceFactualMetrics = sentenceFactualTokenMetrics(context.query, searchableText);
  const proceduralTerms = inferProceduralTerms(context.query);
  const hasProceduralTerms = proceduralTerms.length > 0;
  const proceduralTermHits = proceduralTerms.filter((term) => loweredSnippet.includes(term)).length;
  const normalizedChunkType = normalizeChunkTypeLabel(row.sectionLabel || "");
  const sentenceStyleReasoningQuery = isSentenceStyleReasoningQuery(context);
  const marketConditionReasoningQuery = isMarketConditionReasoningQuery(context);
  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(row.sectionLabel || "");
  const accommodationContext = hasAccommodationContext(searchableText);
  const section8Context = hasSection8Context(searchableText);
  const unlawfulDetainerContext = hasUnlawfulDetainerContext(searchableText);
  const section8UdQuery = isSection8UnlawfulDetainerQuery(context.query);
  const ownerMoveInContext = hasOwnerMoveInContext(searchableText);
  const ownerMoveInFollowThroughContext = hasOwnerMoveInFollowThroughContext(searchableText);
  const ownerMoveInFollowThroughRequired = requiresOwnerMoveInFollowThroughSpecificity(context.query);

  let exactPhraseBoost = 0;
  if (loweredSnippet.includes(loweredQuery) && context.queryType === "exact_phrase") {
    exactPhraseBoost = 0.35;
    why.push("exact_phrase_match");
  }

  let citationBoost = 0;
  const normCitation = normalize(row.citation);
  if (loweredQuery === normCitation || loweredQuery.includes(normCitation) || normCitation.includes(loweredQuery)) {
    citationBoost = 0.45;
    why.push("citation_exact_or_near");
  }

  const indexCodes = parseJsonList(row.indexCodesJson).map(normalize);
  const indexCodeFilterContext = buildIndexCodeFilterContext(context.filters);
  const explicitIndexCodeFilters = uniq([
    ...indexCodeFilterContext.normalizedCodes,
    ...indexCodeFilterContext.legacyCodeAliases.map((item) => normalizeFilterValue("index_code", item))
  ]).map(normalize);
  const ruleSections = parseJsonList(row.rulesSectionsJson).map(normalize);
  const ordinanceSections = parseJsonList(row.ordinanceSectionsJson).map(normalize);

  let metadataBoost = 0;
  if (
    explicitIndexCodeFilters.length > 0 &&
    explicitIndexCodeFilters.some((filterValue) => indexCodes.some((item) => item.includes(filterValue)))
  ) {
    metadataBoost += 0.22;
    why.push("index_code_overlap");
  }
  if (
    indexCodeFilterContext.relatedRulesSections.length > 0 &&
    indexCodeFilterContext.relatedRulesSections
      .map((item) => normalize(item))
      .some((filterValue) => ruleSections.some((item) => item.includes(filterValue)))
  ) {
    metadataBoost += 0.16;
    why.push("index_code_rules_compat_overlap");
  }
  if (
    indexCodeFilterContext.relatedOrdinanceSections.length > 0 &&
    indexCodeFilterContext.relatedOrdinanceSections
      .map((item) => normalize(item))
      .some((filterValue) => ordinanceSections.some((item) => item.includes(filterValue)))
  ) {
    metadataBoost += 0.16;
    why.push("index_code_ordinance_compat_overlap");
  }
  if (
    indexCodeFilterContext.searchPhrases.length > 0 &&
    indexCodeFilterContext.searchPhrases.map((item) => normalize(item)).some((phrase) => loweredSnippet.includes(phrase))
  ) {
    metadataBoost += 0.14;
    why.push("index_code_phrase_compat_overlap");
  }
  if (context.filters.rulesSection && ruleSections.some((item) => item.includes(normalize(context.filters.rulesSection || "")))) {
    metadataBoost += 0.18;
    why.push("rules_overlap");
  }
  if (
    context.filters.ordinanceSection &&
    ordinanceSections.some((item) => item.includes(normalize(context.filters.ordinanceSection || "")))
  ) {
    metadataBoost += 0.18;
    why.push("ordinance_overlap");
  }

  let sectionBoost = 0;
  const prioritizedSection = sectionPriorityBoost(row.sectionLabel || "");
  if (prioritizedSection.boost > 0) {
    sectionBoost += prioritizedSection.boost;
    if (prioritizedSection.why) why.push(prioritizedSection.why);
  }
  if (!hasIssueTerms && /conclusions? of law|reasoning|analysis/i.test(row.sectionLabel)) {
    sectionBoost += 0.11;
    why.push("reasoning_section_boost");
  }
  if (!hasIssueTerms && findingsLikeChunk) {
    sectionBoost += 0.05;
    why.push("findings_section_context_boost");
  }
  if (/rules?|ordinance/i.test(row.sectionLabel)) {
    sectionBoost += 0.1;
    why.push("legal_section_boost");
  }
  if (conclusionsLikeChunk && sentenceStyleReasoningQuery && lexical > 0.08) {
    sectionBoost += 0.14;
    why.push("conclusion_sentence_query_boost");
  }
  if (
    conclusionsLikeChunk &&
    (queryIntent === "authority" || queryIntent === "analysis" || queryIntent === "disposition") &&
    (lexical > 0.04 || vectorScore > 0.18)
  ) {
    sectionBoost += 0.08;
    why.push("conclusion_authority_query_boost");
  }
  if (conclusionsLikeChunk && issueTermHits > 0 && !hasProceduralTerms) {
    sectionBoost += 0.05;
    why.push("conclusion_issue_overlap_boost");
  }
  if (findingsLikeChunk && issueTermHits > 0 && !hasProceduralTerms) {
    sectionBoost += 0.035;
    why.push("findings_issue_overlap_boost");
  }
  if (conclusionsLikeChunk && (issueTermHits > 0 || primarySignalHits > 0)) {
    sectionBoost += sentenceStyleReasoningQuery
      ? sentenceFactualMetrics.matchedCount > 0 || sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0
        ? 0.08
        : 0.02
      : 0.08;
    why.push("conclusion_of_law_priority");
  }
  if (findingsLikeChunk && (issueTermHits > 0 || sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0)) {
    sectionBoost += sentenceStyleReasoningQuery ? 0.05 : 0.035;
    why.push("findings_of_fact_secondary_priority");
  }
  if (/^introduction$/i.test(row.sectionLabel)) {
    sectionBoost -= 0.08;
    why.push("intro_section_penalty");
  }
  if (/^body$/i.test(row.sectionLabel)) {
    sectionBoost -= 0.06;
    why.push("generic_body_penalty");
  }

  if (sentenceStyleReasoningQuery && conclusionsLikeChunk) {
    const phraseOverlapBoost = sentencePhraseOverlapScore(context.query, searchableText);
    if (phraseOverlapBoost > 0) {
      exactPhraseBoost += phraseOverlapBoost;
      why.push(`sentence_phrase_overlap_boost:${phraseOverlapBoost.toFixed(2)}`);
    }
  }
  const exactMultiWordBoost = exactMultiWordPhraseScore(context.query, searchableText);
  if (exactMultiWordBoost > 0) {
    exactPhraseBoost += exactMultiWordBoost;
    why.push(`multiword_phrase_match_boost:${exactMultiWordBoost.toFixed(2)}`);
  }
  if (marketConditionReasoningQuery && conclusionsLikeChunk) {
    const marketBoost = marketConditionReasoningScore(context.query, searchableText);
    if (marketBoost > 0) {
      exactPhraseBoost += marketBoost;
      why.push(`market_condition_reasoning_boost:${marketBoost.toFixed(2)}`);
    }
  }

  let partyNameBoost = 0;
  if (context.filters.partyName && normalize(row.title).includes(normalize(context.filters.partyName))) {
    partyNameBoost = 0.3;
    why.push("party_name_exactish");
  }

  const canonicalRowJudge = canonicalizeJudgeName(row.authorName);
  const explicitJudgeFilters = requestedJudgeFilters(context.filters);
  const referencedJudges = queryReferencesJudge(`${context.query} ${context.retrievalQuery}`);
  let judgeNameBoost = 0;
  if (
    explicitJudgeFilters.length > 0 &&
    canonicalRowJudge &&
    explicitJudgeFilters.some((judge) => normalizeJudgeLookupKey(canonicalRowJudge) === normalizeJudgeLookupKey(judge))
  ) {
    judgeNameBoost += 0.4;
    why.push("judge_name_filter_match");
  }
  if (
    referencedJudges.length > 0 &&
    canonicalRowJudge &&
    referencedJudges.some((judge) => normalizeJudgeLookupKey(judge) === normalizeJudgeLookupKey(canonicalRowJudge))
  ) {
    judgeNameBoost += explicitJudgeFilters.length > 0 ? 0.08 : 0.22;
    why.push("judge_name_query_match");
  }
  if (isJudgeDrivenQuery(context.query) && rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters)) {
    judgeNameBoost += 0.28;
    why.push("judge_only_author_match_boost");
  }
  if (isJudgeDrivenQuery(context.query) && !rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters)) {
    judgeNameBoost -= 0.35;
    why.push("judge_only_author_mismatch_penalty");
  }

  let trustTierBoost = 0;
  if (row.isTrustedTier === 1) {
    trustTierBoost = 0.09;
    why.push("trusted_tier_boost");
  } else if (vectorScore > 0 || lexical > 0.3) {
    trustTierBoost = 0.02;
    why.push("broad_chunked_doc_admission");
  }

  let rerank =
    lexical * 0.42 +
    vectorScore * 0.23 +
    exactPhraseBoost +
    citationBoost +
    metadataBoost +
    sectionBoost +
    partyNameBoost +
    judgeNameBoost +
    trustTierBoost;
  if (hasIssueTerms && issueTermHits > 0) {
    const issueBoost = Math.min(0.2, issueTermHits * 0.06);
    rerank += issueBoost;
    why.push(`issue_term_overlap:${issueTermHits}`);
  }
  if (sentenceStyleReasoningQuery && primarySignals.length > 0) {
    if (primarySignalHits >= Math.min(2, primarySignals.length)) {
      rerank += 0.08;
      why.push(`sentence_primary_issue_boost:${primarySignalHits}`);
    } else if (primarySignalHits === 1) {
      rerank += primarySignals.length === 1 ? 0.08 : 0.03;
      why.push("sentence_primary_issue_partial_boost");
    } else if (issueTermHits > 0) {
      rerank -= primarySignals.length === 1 ? 0.16 : 0.1;
      why.push("sentence_primary_issue_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchors.length > 0) {
    if (sentenceIssueAnchorHits >= Math.min(2, sentenceIssueAnchors.length)) {
      rerank += 0.13;
      why.push(`sentence_issue_anchor_boost:${sentenceIssueAnchorHits}`);
    } else if (sentenceIssueAnchorHits === 1) {
      rerank += 0.06;
      why.push("sentence_issue_anchor_partial_boost");
    } else if (primarySignalHits > 0) {
      rerank -= 0.11;
      why.push("sentence_issue_anchor_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchorHits > 0 && findingsLikeChunk) {
    rerank += 0.08;
    why.push("sentence_issue_anchor_findings_boost");
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchors.length > 0 && conclusionsLikeChunk && sentenceIssueAnchorHits === 0) {
    rerank -= 0.08;
    why.push("sentence_anchorless_conclusion_penalty");
  }
  if (sentenceStyleReasoningQuery && sentenceSecondaryTokens.length > 0) {
    if (sentenceSecondaryHits >= Math.min(2, sentenceSecondaryTokens.length)) {
      rerank += 0.09;
      why.push(`sentence_secondary_fact_boost:${sentenceSecondaryHits}`);
    } else if (sentenceSecondaryHits === 1) {
      rerank += 0.035;
      why.push("sentence_secondary_fact_partial_boost");
    } else if (primarySignalHits > 0) {
      rerank -= 0.09;
      why.push("sentence_secondary_fact_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceSecondaryHits > 0 && findingsLikeChunk) {
    rerank += 0.07;
    why.push("sentence_secondary_fact_findings_boost");
  }
  if (sentenceStyleReasoningQuery && sentenceFactualMetrics.matchedCount >= 2) {
    const factualCoverageBoost = Math.min(0.1, sentenceFactualMetrics.coverageRatio * 0.12);
    rerank += factualCoverageBoost + sentenceFactualMetrics.proximityBoost;
    why.push(`sentence_factual_coverage_boost:${sentenceFactualMetrics.coverageRatio.toFixed(2)}`);
    if (sentenceFactualMetrics.proximityBoost > 0) {
      why.push(`sentence_factual_proximity_boost:${sentenceFactualMetrics.proximityBoost.toFixed(2)}`);
    }
  }
  if (
    sentenceStyleReasoningQuery &&
    findingsLikeChunk &&
    primarySignalHits > 0 &&
    (sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0)
  ) {
    rerank += 0.12;
    why.push("sentence_factual_findings_preference");
  }
  if (
    sentenceStyleReasoningQuery &&
    primarySignals.length === 1 &&
    primarySignalHits === 0 &&
    isHousingServicesDefinitionBoilerplate(searchableText)
  ) {
    rerank -= 0.14;
    why.push("sentence_issue_boilerplate_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    hasOwnerMoveInPhrase(context.query) &&
    sentenceIssueAnchors.length > 0 &&
    sentenceIssueAnchorHits === 0 &&
    isOwnerMoveInLegalStandardBoilerplate(searchableText)
  ) {
    rerank -= 0.18;
    why.push("owner_move_in_legal_standard_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    sentenceSecondaryHits === 0 &&
    isHousingServicesDefinitionBoilerplate(searchableText)
  ) {
    rerank -= 0.08;
    why.push("sentence_secondary_boilerplate_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    conclusionsLikeChunk &&
    primarySignalHits > 0 &&
    sentenceIssueAnchorHits === 0 &&
    sentenceSecondaryHits === 0
  ) {
    rerank -= 0.16;
    why.push("sentence_generic_conclusion_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    conclusionsLikeChunk &&
    primarySignalHits > 0 &&
    sentenceFactualMetrics.totalCount >= 2 &&
    sentenceFactualMetrics.coverageRatio < 0.34
  ) {
    rerank -= 0.08;
    why.push("sentence_low_factual_coverage_conclusion_penalty");
  }
  if (hasIssueTerms && /findings? of fact|background|history|order/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("issue_section_boost");
  }
  if (hasIssueTerms && conclusionsLikeChunk) {
    rerank += 0.1;
    why.push("issue_conclusions_priority_boost");
  } else if (hasIssueTerms && findingsLikeChunk) {
    rerank += 0.06;
    why.push("issue_findings_priority_boost");
  }
  if (hasIssueTerms && isIssuePreferredChunkType(normalizedChunkType) && issueTermHits > 0) {
    rerank += 0.06;
    why.push("issue_preferred_chunk_type_boost");
  }
  if (hasIssueTerms && isIssueDisfavoredChunkType(normalizedChunkType) && issueTermHits === 0) {
    rerank -= 0.18;
    why.push("issue_disfavored_chunk_penalty");
  }
  if (hasProceduralTerms && proceduralTermHits > 0) {
    const proceduralBoost = Math.min(0.18, proceduralTermHits * 0.05);
    rerank += proceduralBoost;
    why.push(`procedural_term_overlap:${proceduralTermHits}`);
  }
  if (hasProceduralTerms && /order|conclusions? of law|procedural|history|background/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("procedural_section_boost");
  }
  if (hasProceduralTerms && isLowValueIssueIntentChunkType(normalizedChunkType) && proceduralTermHits === 0) {
    rerank -= 0.2;
    why.push("procedural_low_value_chunk_penalty");
  }
  if (hasIssueTerms && /conclusions? of law/i.test(row.sectionLabel) && isCapitalImprovementBoilerplate(row.chunkText) && issueTermHits === 0) {
    rerank -= 0.22;
    why.push("capital_improvement_boilerplate_penalty");
  }
  if (containsWholeWord(context.query, "mold") && hasMoldCollision(searchableText)) {
    rerank -= 0.3;
    why.push("mold_molding_collision_penalty");
  }
  if (containsWholeWord(context.query, "mildew")) {
    const normalizedMildewText = normalize(searchableText);
    const mildewAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    const mildewAnalysisOnly = /analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (/\bmildew\b/.test(normalizedMildewText)) {
      if (mildewAuthorityLike) {
        rerank += 0.08;
        why.push("mildew_authority_priority_boost");
      }
      if (mildewAnalysisOnly) {
        rerank -= 0.08;
        why.push("mildew_analysis_penalty");
      }
    } else if ((vectorScore > 0.16 || lexical > 0.12) && mildewAnalysisOnly) {
      rerank -= 0.16;
      why.push("mildew_context_missing_analysis_penalty");
    }
  }
  if (isCoolingIssueQuery(context.query) && issueTermHits === 0 && vectorScore > 0) {
    rerank -= 0.18;
    why.push("cooling_issue_evidence_penalty");
  }
  if (isCoolingIssueQuery(context.query) && issueTermHits > 0 && /findings? of fact|order/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("cooling_issue_evidence_boost");
  }
  if (isCoolingIssueQuery(context.query) && hasCoolingProxyDrift(searchableText)) {
    rerank -= 0.24;
    why.push("cooling_proxy_drift_penalty");
  }
  if (hasWrongContextForQuery(context.query, searchableText)) {
    rerank -= 0.24;
    why.push("family_wrong_context_penalty");
  }
  if (isAccommodationQuery(context.query)) {
    if (accommodationContext) {
      rerank += findingsLikeChunk ? 0.18 : 0.12;
      why.push("accommodation_context_boost");
      if (hasEmploymentAccommodationDrift(searchableText)) {
        rerank -= 0.34;
        why.push("accommodation_employment_drift_penalty");
      }
    } else if (hasEmploymentAccommodationDrift(searchableText)) {
      rerank -= 0.34;
      why.push("accommodation_employment_drift_penalty");
    } else if (vectorScore > 0.2 || lexical > 0.16) {
      rerank -= 0.22;
      why.push("accommodation_context_missing_penalty");
    }
  }
  if (isLockBoxQuery(context.query)) {
    const lockBoxContext = hasLockBoxContext(searchableText);
    const lockBoxAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (lockBoxContext) {
      rerank += lockBoxAuthorityLike ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("lock_box_context_boost");
      if (lockBoxAuthorityLike) {
        rerank += 0.08;
        why.push("lock_box_authority_priority_boost");
      }
    } else if (lockBoxAuthorityLike && issueTermHits === 0) {
      rerank -= 0.22;
      why.push("lock_box_generic_authority_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("lock_box_context_missing_penalty");
    }
  }
  if (isHomeownersExemptionQuery(context.query)) {
    if (hasHomeownersExemptionContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("homeowners_exemption_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("homeowners_exemption_context_missing_penalty");
    }
  }
  if (isCameraPrivacyQuery(context.query)) {
    if (hasCameraPrivacyContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("camera_privacy_context_boost");
    } else if ((/privacy/.test(normalize(searchableText)) && !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b/.test(normalize(searchableText))) && (vectorScore > 0.1 || lexical > 0.08)) {
      rerank -= 0.24;
      why.push("camera_privacy_missing_camera_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("camera_privacy_context_missing_penalty");
    }
  }
  if (isPackageSecurityQuery(context.query)) {
    const packageSecuritySensitiveDrift =
      /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
        normalize(searchableText)
      );
    const packageBoilerplateDrift =
      (/housing services are those services provided by the landlord|loss of any tenant housing services|housing services reasonably expected|planning code section 207|accessory dwelling unit|\badu\b/.test(
        normalize(searchableText)
      ) &&
        !/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
          normalize(searchableText)
        ));
    if (hasPackageDeliverySecurityContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.24 : findingsLikeChunk ? 0.16 : 0.12;
      why.push("package_security_delivery_context_boost");
      if (packageSecuritySensitiveDrift) {
        rerank -= 0.42;
        why.push("package_security_sensitive_drift_penalty");
      }
    } else if (hasPackageSecurityContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("package_security_context_boost");
      rerank -= 0.12;
      why.push("package_security_generic_context_penalty");
      if (packageSecuritySensitiveDrift) {
        rerank -= 0.42;
        why.push("package_security_sensitive_drift_penalty");
      }
      if (packageBoilerplateDrift) {
        rerank -= 0.34;
        why.push("package_security_boilerplate_drift_penalty");
      }
    } else if (packageSecuritySensitiveDrift) {
      rerank -= 0.32;
      why.push("package_security_security_deposit_penalty");
    } else if (packageBoilerplateDrift) {
      rerank -= 0.3;
      why.push("package_security_boilerplate_drift_penalty");
    } else if (/\bsecurity fee\b|\bsecurity fees\b|\bcharge for a security\b|\bunlawful charges? for security fees?\b/.test(normalize(searchableText))) {
      rerank -= 0.28;
      why.push("package_security_security_fee_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("package_security_context_missing_penalty");
    }
  }
  if (isDogQuery(context.query)) {
    if (hasDogPolicyContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.26 : findingsLikeChunk ? 0.18 : 0.14;
      why.push("dog_policy_context_boost");
    } else if (hasDogParkContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.14 : findingsLikeChunk ? 0.1 : 0.06;
      why.push("dog_park_context_boost");
    } else if (hasDogContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.18 : findingsLikeChunk ? 0.12 : 0.08;
      why.push("dog_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("dog_context_missing_penalty");
    }
  }
  if (isCollegeQuery(context.query)) {
    const collegeBondDrift =
      /\bcommunity college district\b|\bschool district\b|\bgeneral obligation bonds?\b|\bbond passthrough\b|\bpassthrough\b/.test(
        normalize(searchableText)
      ) && !hasCollegeContext(searchableText);
    if (hasCollegeContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("college_context_boost");
    } else if (collegeBondDrift) {
      rerank -= 0.28;
      why.push("college_bond_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("college_context_missing_penalty");
    }
  }
  if (isSelfEmployedQuery(context.query)) {
    if (hasSelfEmployedContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("self_employed_context_boost");
    } else if (/\b1099\b|\btax return\b|\btax returns\b/.test(normalize(searchableText))) {
      rerank += 0.06;
      why.push("self_employed_partial_evidence_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("self_employed_context_missing_penalty");
    }
  }
  if (isAdjudicatedQuery(context.query)) {
    if (hasAdjudicatedContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("adjudicated_context_boost");
    } else if (/\bdecid(?:ed|e)\b|\bstate court\b/.test(normalize(searchableText))) {
      rerank += 0.05;
      why.push("adjudicated_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("adjudicated_context_missing_penalty");
    }
  }
  if (isSocialMediaQuery(context.query)) {
    const socialSecurityDrift =
      /\bsocial security\b|\bsocial security number\b|\bsupplemental security income\b|\bssi\b/.test(normalize(searchableText));
    if (hasSocialMediaContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("social_media_context_boost");
      if (socialSecurityDrift) {
        rerank -= 0.32;
        why.push("social_media_social_security_drift_penalty");
      }
    } else if (/\bfacebook\b|\binstagram\b|\bnextdoor\b/.test(normalize(searchableText))) {
      rerank += 0.04;
      why.push("social_media_partial_platform_boost");
      if (socialSecurityDrift) {
        rerank -= 0.32;
        why.push("social_media_social_security_drift_penalty");
      }
    } else if (socialSecurityDrift) {
      rerank -= 0.32;
      why.push("social_media_social_security_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("social_media_context_missing_penalty");
    }
  }
  if (isCaregiverQuery(context.query)) {
    if (hasCaregiverContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("caregiver_context_boost");
    } else if (/\bcaregiver\b|\bcaregiving\b|\bcaretaker\b/.test(normalize(searchableText))) {
      rerank += 0.05;
      why.push("caregiver_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("caregiver_context_missing_penalty");
    }
  }
  if (isPoopQuery(context.query)) {
    const normalizedPoopText = normalize(searchableText);
    const poopAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    const poopAnalysisOnly = /analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (hasPoopContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("poop_context_boost");
      if (poopAuthorityLike) {
        rerank += 0.08;
        why.push("poop_authority_priority_boost");
      }
      if (poopAnalysisOnly) {
        rerank -= 0.08;
        why.push("poop_analysis_penalty");
      }
    } else if (/\bfeces\b|\bfaeces\b|\bdog waste\b|\banimal waste\b/.test(normalize(searchableText))) {
      rerank += 0.05;
      why.push("poop_partial_context_boost");
      if (poopAuthorityLike) {
        rerank += 0.06;
        why.push("poop_authority_priority_boost");
      }
      if (poopAnalysisOnly) {
        rerank -= 0.06;
        why.push("poop_analysis_penalty");
      }
      if (/\brat feces\b|\brodent urine\/feces\b/.test(normalizedPoopText) && !/\bdog waste\b|\banimal waste\b|\bhuman feces\b|\bsewage\b/.test(normalizedPoopText)) {
        rerank -= 0.04;
        why.push("poop_rodent_only_penalty");
      }
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("poop_context_missing_penalty");
    }
  }
  if (isMootQuery(context.query)) {
    if (hasMootContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("moot_context_boost");
    } else if (/\bnull and void\b|\brescinded\b|\bdismissed\b/.test(normalize(searchableText))) {
      rerank += 0.05;
      why.push("moot_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("moot_context_missing_penalty");
    }
  }
  if (isRemoteWorkQuery(context.query)) {
    if (hasRemoteWorkContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("remote_work_context_boost");
    } else if (/\bremote work\b|\bwork from home\b|\bworking from home\b/.test(normalize(searchableText))) {
      rerank += 0.06;
      why.push("remote_work_partial_phrase_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("remote_work_context_missing_penalty");
    }
  }
  if (isDivorceQuery(context.query)) {
    if (hasDivorceContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("divorce_context_boost");
    } else if (/\bspouse\b|\bhusband\b|\bwife\b/.test(normalize(searchableText))) {
      rerank -= 0.2;
      why.push("divorce_generic_spouse_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("divorce_context_missing_penalty");
    }
  }
  if (isIntercomQuery(context.query)) {
    if (hasIntercomContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("intercom_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("intercom_context_missing_penalty");
    }
  }
  if (isGarageSpaceQuery(context.query)) {
    if (hasGarageSpaceContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("garage_space_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("garage_space_context_missing_penalty");
    }
  }
  if (isCommonAreasQuery(context.query)) {
    if (hasCommonAreasContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("common_areas_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("common_areas_context_missing_penalty");
    }
  }
  if (isStairsQuery(context.query)) {
    if (hasStairsContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("stairs_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("stairs_context_missing_penalty");
    }
  }
  if (isPorchQuery(context.query)) {
    if (hasPorchContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("porch_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("porch_context_missing_penalty");
    }
  }
  if (isWindowsQuery(context.query)) {
    const windowsCapitalImprovementDrift =
      /\bcapital improvement\b|\bnew windows\b|\bcertified\b|\bpassthrough\b|\bamortiz/.test(normalize(searchableText)) &&
      !/\binoperable\b|\bbroken\b|\boperable\b|\bwindow latch\b|\bwindow sash\b|\bwould not open\b|\bwould not close\b|\bdraft\b|\bleak\b/.test(
        normalize(searchableText)
      );
    if (hasWindowsContext(searchableText)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("windows_context_boost");
    } else if (windowsCapitalImprovementDrift) {
      rerank -= 0.28;
      why.push("windows_capital_improvement_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("windows_context_missing_penalty");
    }
  }
  if (isCoLivingQuery(context.query)) {
    if (hasCoLivingContext(searchableText)) {
      rerank += findingsLikeChunk ? 0.18 : conclusionsLikeChunk ? 0.14 : 0.1;
      why.push("co_living_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("co_living_context_missing_penalty");
    }
  }
  if (isBuyoutPressureQuery(context.query)) {
    if (hasBuyoutPressureContext(searchableText)) {
      rerank += findingsLikeChunk ? 0.24 : 0.16;
      why.push("buyout_pressure_context_boost");
    } else if (hasBuyoutContext(searchableText)) {
      rerank -= 0.24;
      why.push("buyout_pressure_missing_pressure_penalty");
    } else if ((lexical > 0.12 || vectorScore > 0.16) && /settlement|claims|paid\s*\$|paying\s*\$|agreement/.test(normalize(searchableText))) {
      rerank -= 0.3;
      why.push("buyout_pressure_generic_settlement_penalty");
    }
  }
  if (isSection8Query(context.query) && section8Context) {
    rerank += conclusionsLikeChunk ? 0.12 : 0.08;
    why.push("section8_context_boost");
  }
  if (isUnlawfulDetainerQuery(context.query) && unlawfulDetainerContext) {
    rerank += conclusionsLikeChunk || findingsLikeChunk ? 0.16 : 0.1;
    why.push("unlawful_detainer_context_boost");
  }
  if (section8UdQuery) {
    if (section8Context && unlawfulDetainerContext) {
      rerank += conclusionsLikeChunk || findingsLikeChunk ? 0.24 : 0.16;
      why.push("section8_ud_joint_context_boost");
    } else if ((section8Context || unlawfulDetainerContext) && (lexical > 0.1 || vectorScore > 0.14)) {
      rerank -= 0.16;
      why.push("section8_ud_partial_context_penalty");
    }
    if (hasSection8RehabDrift(searchableText)) {
      rerank -= 0.34;
      why.push("section8_rehab_drift_penalty");
    }
    if (hasSection827RentIncreaseDrift(searchableText)) {
      rerank -= 0.42;
      why.push("section827_rent_increase_drift_penalty");
    }
  }
  if (ownerMoveInFollowThroughRequired) {
    if (ownerMoveInContext && ownerMoveInFollowThroughContext) {
      rerank += findingsLikeChunk ? 0.22 : 0.16;
      why.push("owner_move_in_follow_through_joint_context_boost");
    } else if (!ownerMoveInContext || !ownerMoveInFollowThroughContext) {
      rerank -= 0.3;
      why.push("owner_move_in_follow_through_missing_penalty");
    }
  }
  if ((hasIssueTerms || hasProceduralTerms) && isLowValueIssueIntentChunkType(normalizedChunkType) && lexical === 0 && vectorScore > 0) {
    rerank -= 0.26;
    why.push("low_value_issue_vector_penalty");
  }
  if (isEvictionProtectionQuery(context.query) && issueTermHits > 0 && /conclusions? of law|order|decision/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("eviction_protection_authority_boost");
  }
  if (requiresLockoutSpecificity(context.query)) {
    const hasLockoutContext = hasWrongfulEvictionLockoutContext(searchableText);
    if (hasLockoutContext) {
      rerank += /conclusions? of law|findings? of fact|order/i.test(row.sectionLabel) ? 0.24 : 0.14;
      why.push("wrongful_eviction_lockout_required_boost");
    } else if (
      isWrongfulEvictionIssueSearch(context.query) &&
      (hasWrongfulEvictionContext(searchableText) || hasHarassmentContext(searchableText) || hasRepairNoticeContext(searchableText))
    ) {
      rerank -= 0.42;
      why.push("wrongful_eviction_lockout_required_penalty");
    }
  }
  if (isWrongfulEvictionIssueSearch(context.query) && hasWrongfulEvictionLockoutContext(searchableText)) {
    rerank += /conclusions? of law|findings? of fact|order/i.test(row.sectionLabel) ? 0.14 : 0.08;
    why.push("wrongful_eviction_lockout_context_boost");
  }
  if (
    isWrongfulEvictionIssueSearch(context.query) &&
    sentenceStyleReasoningQuery &&
    !hasWrongfulEvictionLockoutContext(searchableText) &&
    (hasHarassmentContext(searchableText) || hasRepairNoticeContext(searchableText))
  ) {
    rerank -= 0.18;
    why.push("wrongful_eviction_missing_lockout_penalty");
  }
  if (isEvictionProtectionQuery(context.query) && issueTermHits === 0 && lexical > 0.5 && vectorScore === 0) {
    rerank -= 0.14;
    why.push("eviction_protection_lexical_only_penalty");
  }
  if (requiresStrongIssueEvidence(context.query) && !hasStrongIssueEvidence(context.query, row, issueTermHits, proceduralTermHits) && lexical > 0.2) {
    rerank -= 0.18;
    why.push("strong_issue_evidence_missing_penalty");
  }
  const intentChunkTypeBoost = intentBoostForChunkType(queryIntent, row.sectionLabel || "");
  rerank += intentChunkTypeBoost;
  if (intentChunkTypeBoost > 0) why.push(`intent_chunk_type_boost:${queryIntent}`);
  if (intentChunkTypeBoost < 0) why.push(`intent_chunk_type_penalty:${queryIntent}`);
  if (lexical > 0) why.push(vectorScore > 0 ? "lexical_and_vector_match" : "lexical_match");
  if (vectorScore > 0) why.push("vector_match");

  // Reduce lexical-only dominance for low-signal heading fragments unless query is direct citation lookup.
  const lexicalDominance = lexical > 0.25 && vectorScore === 0;
  if (context.queryType !== "citation_lookup" && lexicalDominance && isLowSignalStructuralChunkType(normalizedChunkType)) {
    rerank -= 0.12;
    why.push("lexical_low_signal_chunk_penalty");
  }

  const vectorDominance = vectorScore > 0.45 && lexical === 0;
  if (!isStructuralIntent(context) && context.queryType !== "citation_lookup" && vectorDominance && isLowSignalTabularChunkType(normalizedChunkType)) {
    rerank -= 0.28;
    why.push("vector_tabular_chunk_penalty");
  }
  if (!isStructuralIntent(context) && context.queryType !== "citation_lookup" && vectorDominance && hasMalformedDocxArtifact(row.chunkText)) {
    rerank -= 0.22;
    why.push("vector_docx_artifact_penalty");
  }
  if (!isStructuralIntent(context) && context.queryType !== "citation_lookup" && hasSevereExtractionArtifact(row.chunkText)) {
    rerank -= 0.3;
    why.push("severe_extraction_artifact_penalty");
  }

  if (context.queryType === "citation_lookup") {
    rerank += citationBoost * 0.35;
  }
  if (context.queryType === "party_name") {
    rerank += partyNameBoost * 0.35;
  }
  if (context.queryType === "rules_ordinance" || context.queryType === "index_code") {
    rerank += metadataBoost * 0.25;
  }

  // Monotonic compression keeps ranking order stable while reducing score over-separation.
  // This improves score-shape stability across mixed corpora without weakening retrieval gates.
  rerank = Math.pow(Math.max(0, rerank), 0.6);

  return {
    lexicalScore: Number(lexical.toFixed(6)),
    vectorScore: Number(vectorScore.toFixed(6)),
    exactPhraseBoost: Number(exactPhraseBoost.toFixed(6)),
    citationBoost: Number(citationBoost.toFixed(6)),
    metadataBoost: Number(metadataBoost.toFixed(6)),
    sectionBoost: Number(sectionBoost.toFixed(6)),
    partyNameBoost: Number(partyNameBoost.toFixed(6)),
    judgeNameBoost: Number(judgeNameBoost.toFixed(6)),
    trustTierBoost: Number(trustTierBoost.toFixed(6)),
    rerankScore: Number(rerank.toFixed(6)),
    why: uniq(why)
  };
}

function buildDocumentEvidenceSummary(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext
) {
  const issueTerms = inferIssueTerms(context.query);
  const proceduralTerms = inferProceduralTerms(context.query);
  const primarySignals = primaryIssueSignals(context.query);
  const sentenceAnchors = sentenceIssueAnchorTerms(context.query);
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query);
  const sentenceStyle = isSentenceStyleReasoningQuery(context);

  const aggregatedText = normalize(candidates.map((candidate) => combinedSearchableText(candidate.row)).join(" "));
  const uniqueIssueCoverage = issueTerms.filter((term) => aggregatedText.includes(normalize(term))).length;
  const uniqueProceduralCoverage = proceduralTerms.filter((term) => aggregatedText.includes(normalize(term))).length;
  const primaryCoverage = primarySignals.filter((signal) => textContainsIssueSignal(aggregatedText, signal)).length;
  const anchorCoverage = sentenceAnchors.filter((term) => aggregatedText.includes(normalize(term))).length;
  const secondaryCoverage = sentenceSecondaryTokens.filter((term) => aggregatedText.includes(normalize(term))).length;

  let docBoost = 0;
  const reasons: string[] = [];

  if (uniqueIssueCoverage > 1) {
    const boost = Math.min(0.08, uniqueIssueCoverage * 0.02);
    docBoost += boost;
    reasons.push(`document_issue_coverage_boost:${uniqueIssueCoverage}`);
  }
  if (uniqueProceduralCoverage > 1) {
    const boost = Math.min(0.06, uniqueProceduralCoverage * 0.02);
    docBoost += boost;
    reasons.push(`document_procedural_coverage_boost:${uniqueProceduralCoverage}`);
  }
  if (sentenceStyle) {
    if (primaryCoverage > 0) {
      const boost = Math.min(0.08, primaryCoverage * 0.03);
      docBoost += boost;
      reasons.push(`document_primary_signal_coverage:${primaryCoverage}`);
    }
    if (anchorCoverage > 0) {
      const boost = Math.min(0.12, anchorCoverage * 0.04);
      docBoost += boost;
      reasons.push(`document_anchor_coverage:${anchorCoverage}`);
    }
    if (secondaryCoverage > 0) {
      const boost = Math.min(0.08, secondaryCoverage * 0.03);
      docBoost += boost;
      reasons.push(`document_secondary_fact_coverage:${secondaryCoverage}`);
    }
  }

  let leadChunkId: string | null = null;
  let leadBoost = 0;
  let bestLeadScore = Number.NEGATIVE_INFINITY;
  let bestConclusionChunkId: string | null = null;
  let bestConclusionSupport = Number.NEGATIVE_INFINITY;
  let bestFindingsChunkId: string | null = null;
  let bestFindingsSupport = Number.NEGATIVE_INFINITY;
  for (const candidate of candidates) {
    const searchableText = combinedSearchableText(candidate.row);
    const normalizedText = normalize(searchableText);
    const issueHits = issueTerms.filter((term) => normalizedText.includes(normalize(term))).length;
    const primaryHits = primarySignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
    const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(normalize(term))).length;
    const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(normalize(term))).length;
    const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText);
    const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
    const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");

    let leadScore = 0;
    if (primaryHits > 0) leadScore += primaryHits * 0.08;
    if (anchorHits > 0) leadScore += anchorHits * 0.11;
    if (secondaryHits > 0) leadScore += secondaryHits * 0.08;
    if (factualMetrics.matchedCount >= 2) {
      leadScore += Math.min(0.16, factualMetrics.coverageRatio * 0.14) + factualMetrics.proximityBoost;
    }
    if (sentenceStyle && findingsLike && (anchorHits > 0 || secondaryHits > 0)) leadScore += 0.12;
    if (sentenceStyle && conclusionsLike && anchorHits === 0 && secondaryHits === 0 && primaryHits > 0) leadScore -= 0.08;
    if (sentenceStyle && conclusionsLike && factualMetrics.totalCount >= 2 && factualMetrics.coverageRatio < 0.34 && primaryHits > 0) {
      leadScore -= 0.08;
    }

    if (conclusionsLike) {
      const conclusionSupport =
        issueHits * 0.08 +
        primaryHits * 0.12 +
        (factualMetrics.matchedCount > 0 ? factualMetrics.coverageRatio * 0.08 : 0);
      if (conclusionSupport > bestConclusionSupport) {
        bestConclusionSupport = conclusionSupport;
        bestConclusionChunkId = candidate.row.chunkId;
      }
    }
    if (findingsLike) {
      const findingsSupport =
        anchorHits * 0.12 +
        secondaryHits * 0.1 +
        (factualMetrics.matchedCount > 0 ? factualMetrics.coverageRatio * 0.14 + factualMetrics.proximityBoost : 0);
      if (findingsSupport > bestFindingsSupport) {
        bestFindingsSupport = findingsSupport;
        bestFindingsChunkId = candidate.row.chunkId;
      }
    }

    if (leadScore > bestLeadScore) {
      bestLeadScore = leadScore;
      leadChunkId = candidate.row.chunkId;
    }
  }

  if (bestConclusionSupport >= 0.14 && bestFindingsSupport >= 0.18) {
    docBoost += 0.16;
    reasons.push("document_conclusion_findings_stitch");
    if (sentenceStyle && bestFindingsChunkId && bestFindingsSupport >= Math.max(0.18, bestConclusionSupport + 0.04)) {
      leadChunkId = bestFindingsChunkId;
      leadBoost = Math.max(leadBoost, 0.16);
      reasons.push("document_findings_led_stitch");
    } else if (bestConclusionChunkId) {
      leadChunkId = bestConclusionChunkId;
      leadBoost = Math.max(leadBoost, 0.14);
    }
  }

  if (sentenceStyle && bestLeadScore >= 0.16 && leadChunkId) {
    leadBoost = Math.max(leadBoost, Math.min(0.16, bestLeadScore * 0.6));
  }

  return {
    docBoost: Number(docBoost.toFixed(6)),
    reasons,
    leadChunkId,
    leadBoost: Number(leadBoost.toFixed(6))
  };
}

function representativeChunkDisplayScore(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext
): number {
  const searchableText = combinedSearchableText(candidate.row);
  const normalizedText = normalize(searchableText);
  const sentenceStyle = isSentenceStyleReasoningQuery(context);
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const primarySignals = primaryIssueSignals(context.query);
  const sentenceAnchors = sentenceIssueAnchorTerms(context.query);
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query);
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText);
  const issueTerms = inferIssueTerms(context.query);
  const proceduralTerms = inferProceduralTerms(context.query);

  const primaryHits = primarySignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
  const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(normalize(term))).length;
  const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(normalize(term))).length;
  const issueHits = issueTerms.filter((term) => normalizedText.includes(normalize(term))).length;
  const proceduralHits = proceduralTerms.filter((term) => normalizedText.includes(normalize(term))).length;

  let score = candidate.diagnostics.rerankScore * 0.12;
  if (primaryHits > 0) score += primaryHits * 0.2;
  if (anchorHits > 0) score += anchorHits * 0.24;
  if (secondaryHits > 0) score += secondaryHits * 0.18;
  if (factualMetrics.matchedCount >= 2) {
    score += Math.min(0.2, factualMetrics.coverageRatio * 0.22) + factualMetrics.proximityBoost;
  }
  if (issueHits > 0) score += Math.min(0.12, issueHits * 0.04);
  if (proceduralHits > 0) score += Math.min(0.1, proceduralHits * 0.04);

  if (sentenceStyle) {
    score += sentencePhraseOverlapScore(context.query, searchableText);
    score += exactMultiWordPhraseScore(context.query, searchableText);
    if (conclusionsLike && primaryHits > 0) {
      score += factualMetrics.matchedCount > 0 || anchorHits > 0 || secondaryHits > 0 ? 0.12 : 0.03;
    }
    if (findingsLike && (anchorHits > 0 || secondaryHits > 0)) score += 0.18;
    if (findingsLike && primaryHits > 0) score += 0.08;
    if (findingsLike && factualMetrics.matchedCount >= 2) score += 0.1;
    if (conclusionsLike && anchorHits === 0 && secondaryHits === 0 && primaryHits > 0) score -= 0.16;
    if (conclusionsLike && factualMetrics.totalCount >= 2 && factualMetrics.coverageRatio < 0.34 && primaryHits > 0) score -= 0.1;
  } else {
    if (conclusionsLike && issueHits > 0) score += 0.08;
    if (findingsLike && issueHits > 0) score += 0.06;
  }

  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.12;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.14;
  if (isHousingServicesDefinitionBoilerplate(searchableText) && secondaryHits === 0) score -= 0.12;
  if (hasOwnerMoveInPhrase(context.query) && isOwnerMoveInLegalStandardBoilerplate(searchableText) && anchorHits === 0) score -= 0.14;

  return Number(score.toFixed(6));
}

function toSearchResultPassage(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext,
  options?: { kind?: "authority" | "supporting_fact" }
): SearchResultPassage {
  const snippet =
    options?.kind === "supporting_fact"
      ? chooseSupportingFactSnippet(candidate.row.chunkText, context)
      : chooseSnippet(candidate.row.chunkText, context);
  return {
    chunkId: candidate.row.chunkId,
    snippet,
    sectionLabel: candidate.row.sectionLabel,
    sectionHeading: candidate.row.sectionLabel,
    citationAnchor: candidate.row.citationAnchor,
    paragraphAnchor: candidate.row.paragraphAnchor,
    chunkType: normalizeChunkTypeLabel(candidate.row.sectionLabel),
    score: candidate.diagnostics.rerankScore
  };
}

function authorityPassageScore(candidate: { row: ChunkRow; diagnostics: RankingDiagnostics }, context: SearchContext): number {
  const searchableText = combinedSearchableText(candidate.row);
  const normalizedText = normalize(searchableText);
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const primaryHits = primaryIssueSignals(context.query).filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
  const issueHits = inferIssueTerms(context.query).filter((term) => normalizedText.includes(normalize(term))).length;
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText);

  let score = candidate.diagnostics.rerankScore * 0.18;
  if (conclusionsLike) score += 0.34;
  if (findingsLike) score -= 0.04;
  if (primaryHits > 0) score += primaryHits * 0.14;
  if (issueHits > 0) score += Math.min(0.1, issueHits * 0.03);
  if (isSentenceStyleReasoningQuery(context)) {
    score += sentencePhraseOverlapScore(context.query, searchableText);
    score += exactMultiWordPhraseScore(context.query, searchableText);
    if (conclusionsLike && factualMetrics.matchedCount > 0) score += 0.08;
    if (conclusionsLike && factualMetrics.matchedCount === 0 && primaryHits > 0) score -= 0.06;
  }
  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.16;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.18;

  return Number(score.toFixed(6));
}

function hasHabitabilityServiceRestorationSignals(query: string): boolean {
  const normalized = normalize(query || "");
  if (!normalized) return false;
  return /\bmold|hot water|heat|heating|heater|boiler|radiator|rodent|cockroach|bed bug|ventilation|leak|water intrusion|plumbing|sewage|repair|repairs|restore service|service restoration\b/.test(
    normalized
  );
}

function requiredHabitabilityPrimarySignals(query: string): string[] {
  return primaryIssueSignals(query).filter((signal) =>
    ["mold", "heat", "hot water", "rodent", "cockroach", "bed bug"].includes(signal)
  );
}

function habitabilityCoverageSignals(text: string, query: string): {
  conditionSignalHits: number;
  reportingHits: number;
  repairFailureHits: number;
} {
  const normalizedText = normalize(text || "");
  const requiredConditionSignals = requiredHabitabilityPrimarySignals(query);
  const conditionSignalHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
  const reportingHits = [
    /\breport(?:ed|ing)?\b/g,
    /\bcomplain(?:ed|ing)?\b/g,
    /\bnotified\b/g,
    /\bnotice\b/g,
    /\brepair request\b/g,
    /\bwork order\b/g
  ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
  const repairFailureHits = [
    /\bfailed to repair\b/g,
    /\bdid not repair\b/g,
    /\brefused to repair\b/g,
    /\bnot repaired\b/g,
    /\bfailed to restore\b/g,
    /\bdid not restore\b/g,
    /\brestore service\b/g,
    /\bservice restoration\b/g,
    /\brestored service\b/g,
    /\brestore heat\b/g,
    /\brestore hot water\b/g
  ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
  return {
    conditionSignalHits,
    reportingHits,
    repairFailureHits
  };
}

function supportingFactAnchorDiagnostics(candidate: { row: ChunkRow; diagnostics: RankingDiagnostics }, context: SearchContext) {
  const searchableText = combinedSearchableText(candidate.row);
  const normalizedText = normalize(searchableText);
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const sentenceStyle = isSentenceStyleReasoningQuery(context);
  const primarySignals = primaryIssueSignals(context.query);
  const sentenceAnchors = sentenceIssueAnchorTerms(context.query);
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query);
  const primarySignalHits = primarySignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
  const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(normalize(term))).length;
  const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(normalize(term))).length;
  const issueHits = inferIssueTerms(context.query).filter((term) => normalizedText.includes(normalize(term))).length;
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText);
  const habitabilityServiceQuery = hasHabitabilityServiceRestorationSignals(context.query);

  let factualAnchorScore = 0;
  if (primarySignalHits > 0) factualAnchorScore += Math.min(0.18, primarySignalHits * 0.07);
  if (anchorHits > 0) factualAnchorScore += anchorHits * 0.15;
  if (secondaryHits > 0) factualAnchorScore += secondaryHits * 0.12;
  if (issueHits > 0) factualAnchorScore += Math.min(0.1, issueHits * 0.03);
  if (factualMetrics.matchedCount >= 2) {
    factualAnchorScore += Math.min(0.22, factualMetrics.coverageRatio * 0.24) + factualMetrics.proximityBoost;
  }
  if (hasOwnerMoveInPhrase(context.query)) {
    const occupancyHits = [
      /\boccup(?:y|ied|ancy)\b/g,
      /\bresid(?:e|ed|ency)\b/g,
      /\bmove(?:d)? in\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const failedFollowThroughHits = [
      /\bnever occupied\b/g,
      /\bdid not occupy\b/g,
      /\bfailed to occupy\b/g,
      /\bnever resided\b/g,
      /\bdid not reside\b/g,
      /\bnever moved in\b/g,
      /\bdid not move in\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.18, occupancyHits * 0.06);
    factualAnchorScore += Math.min(0.22, failedFollowThroughHits * 0.11);
    if (requiresOwnerMoveInFollowThroughSpecificity(context.query)) {
      if (failedFollowThroughHits > 0) {
        factualAnchorScore += 0.18;
      } else {
        factualAnchorScore -= 0.16;
      }
    }
  }
  if (/\bmold\b/.test(normalize(context.query || ""))) {
    const moldHits = normalizedText.includes("mold") ? 1 : 0;
    const reportingHits = [
      /\breport(?:ed|ing)?\b/g,
      /\bcomplain(?:ed|ing)?\b/g,
      /\bnotified\b/g,
      /\bnotice\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const repairFailureHits = [
      /\bfailed to repair\b/g,
      /\bdid not repair\b/g,
      /\brefused to repair\b/g,
      /\brepair request\b/g,
      /\bnot repaired\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    if (moldHits && reportingHits > 0) factualAnchorScore += 0.14;
    if (moldHits && repairFailureHits > 0) factualAnchorScore += 0.18;
    factualAnchorScore += Math.min(0.08, reportingHits * 0.03);
    factualAnchorScore += Math.min(0.12, repairFailureHits * 0.04);
    if (moldHits === 0) factualAnchorScore -= 0.18;
  }
  if (habitabilityServiceQuery) {
    const requiredConditionSignals = requiredHabitabilityPrimarySignals(context.query);
    const conditionSignalHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
    const conditionHits = [
      /\bmold\b/g,
      /\bhot water\b/g,
      /\bheat(?:ing)?\b/g,
      /\bheater\b/g,
      /\bboiler\b/g,
      /\bradiator\b/g,
      /\brodent\b/g,
      /\bcockroach\b/g,
      /\bbed bugs?\b/g,
      /\bventilation\b/g,
      /\bleak\b/g,
      /\bwater intrusion\b/g,
      /\bplumbing\b/g,
      /\bsewage\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const reportingNoticeHits = [
      /\breport(?:ed|ing)?\b/g,
      /\bcomplain(?:ed|ing)?\b/g,
      /\bnotified\b/g,
      /\bnotice\b/g,
      /\brepair request\b/g,
      /\bwork order\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const restorationHits = [
      /\bfailed to repair\b/g,
      /\bdid not repair\b/g,
      /\brefused to repair\b/g,
      /\bnot repaired\b/g,
      /\bfailed to restore\b/g,
      /\bdid not restore\b/g,
      /\brestore service\b/g,
      /\bservice restoration\b/g,
      /\brestored service\b/g,
      /\brestore heat\b/g,
      /\brestore hot water\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.12, conditionHits * 0.03);
    factualAnchorScore += Math.min(0.1, reportingNoticeHits * 0.03);
    factualAnchorScore += Math.min(0.16, restorationHits * 0.04);
    factualAnchorScore += Math.min(0.18, conditionSignalHits * 0.08);
    if (conditionHits > 0 && reportingNoticeHits > 0) factualAnchorScore += 0.14;
    if (conditionHits > 0 && restorationHits > 0) factualAnchorScore += 0.18;
    if (findingsLike && conditionHits > 0 && (reportingNoticeHits > 0 || restorationHits > 0)) factualAnchorScore += 0.12;
    if (requiredConditionSignals.length > 0 && conditionSignalHits === 0) factualAnchorScore -= 0.14;
  }
  if (/\bharassment|retaliation\b/.test(normalize(context.query || ""))) {
    const harassmentHits = [
      /\bharassment\b/g,
      /\bretaliation\b/g,
      /\btenant harassment\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const entryNoticeHits = [
      /\bnotice\b/g,
      /\bnotices\b/g,
      /\bentry\b/g,
      /\bentries\b/g,
      /\benter(?:ed|ing)?\b/g,
      /\baccess\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.16, harassmentHits * 0.05);
    factualAnchorScore += Math.min(0.12, entryNoticeHits * 0.03);
    if (harassmentHits > 0 && entryNoticeHits > 0) factualAnchorScore += 0.14;
    if (findingsLike && harassmentHits > 0) factualAnchorScore += 0.08;
  }
  if (hasWrongfulEvictionPhrase(context.query)) {
    const lockoutHits = [
      /\blockout\b/g,
      /\blocked out\b/g,
      /\bchanged locks?\b/g,
      /\bdenied access\b/g,
      /\bself-help eviction\b/g,
      /\bself help eviction\b/g,
      /\bshut off utilities\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.24, lockoutHits * 0.08);
  }

  let score = candidate.diagnostics.rerankScore * 0.1 + factualAnchorScore;
  if (findingsLike) score += 0.34;
  if (conclusionsLike) score -= 0.08;
  if (sentenceStyle && anchorHits === 0 && secondaryHits === 0 && factualMetrics.coverageRatio === 0 && factualAnchorScore < 0.08) {
    score -= 0.28;
  }
  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.16;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.18;

  return {
    score: Number(score.toFixed(6)),
    factualAnchorScore: Number(factualAnchorScore.toFixed(6)),
    anchorHits,
    secondaryHits,
    coverageRatio: Number(factualMetrics.coverageRatio.toFixed(4))
  };
}

function supportingFactPassageScore(candidate: { row: ChunkRow; diagnostics: RankingDiagnostics }, context: SearchContext): number {
  return supportingFactAnchorDiagnostics(candidate, context).score;
}

function pickPrimaryAuthorityCandidate(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext
) {
  const hasConclusionsLike = candidates.some((candidate) => isConclusionsLikeSectionLabel(candidate.row.sectionLabel || ""));
  const hasFindingsLike = candidates.some((candidate) => isFindingsLikeSectionLabel(candidate.row.sectionLabel || ""));
  return candidates
    .map((candidate) => {
      let score = authorityPassageScore(candidate, context);
      const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
      const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
      if (hasConclusionsLike && hasFindingsLike) {
        if (conclusionsLike) score += 0.18;
        if (findingsLike) score -= 0.12;
      }
      if (hasConclusionsLike && !conclusionsLike) score -= 0.04;
      return { candidate, score };
    })
    .sort((a, b) => b.score - a.score)[0]?.candidate;
}

function pickSupportingFactCandidate(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  authorityChunkId?: string,
  source: SupportingFactDebug["source"] = "matched_pool"
) {
  const sentenceStyle = isSentenceStyleReasoningQuery(context);
  const lockoutSpecificityRequired = requiresLockoutSpecificity(context.query);
  const ownerMoveInFollowThroughRequired = requiresOwnerMoveInFollowThroughSpecificity(context.query);
  const requiredConditionSignals = requiredHabitabilityPrimarySignals(context.query);
  const minimumAnchorScore = sentenceStyle ? 0.14 : 0.08;
  const scored = candidates
    .map((candidate) => ({ candidate, diagnostics: supportingFactAnchorDiagnostics(candidate, context) }))
    .sort((a, b) => b.diagnostics.score - a.diagnostics.score)
    .filter((item) => item.candidate.row.chunkId !== authorityChunkId);

  const acceptable = scored.find((item) => {
    const supportLikeSection = isSupportingFactSectionLabel(item.candidate.row.sectionLabel || "");
    const hasMeaningfulFactSignal =
      item.diagnostics.anchorHits > 0 ||
      item.diagnostics.secondaryHits > 0 ||
      item.diagnostics.coverageRatio >= 0.34 ||
      item.diagnostics.factualAnchorScore >= minimumAnchorScore;
    const candidateText = combinedSearchableText(item.candidate.row);
    const hasRequiredLockoutSignal = !lockoutSpecificityRequired || hasWrongfulEvictionLockoutContext(candidateText);
    const hasRequiredOwnerMoveInFollowThrough =
      !ownerMoveInFollowThroughRequired || hasOwnerMoveInFollowThroughContext(candidateText);
    const hasRequiredConditionSignal =
      requiredConditionSignals.length === 0 ||
      requiredConditionSignals.some((signal) => textContainsIssueSignal(normalize(candidateText), signal));

    if (!sentenceStyle) {
      return (supportLikeSection || hasMeaningfulFactSignal) && hasRequiredLockoutSignal && hasRequiredOwnerMoveInFollowThrough && hasRequiredConditionSignal;
    }

    if (source === "matched_pool") {
      return (
        supportLikeSection &&
        hasMeaningfulFactSignal &&
        hasRequiredLockoutSignal &&
        hasRequiredOwnerMoveInFollowThrough &&
        hasRequiredConditionSignal
      );
    }

    return (
      (supportLikeSection || hasMeaningfulFactSignal) &&
      hasRequiredLockoutSignal &&
      hasRequiredOwnerMoveInFollowThrough &&
      hasRequiredConditionSignal
    );
  });

  return acceptable;
}

function shouldRetrySupportingFactFallback(
  layers: {
    primaryAuthorityPassage?: SearchResultPassage;
    supportingFactPassage?: SearchResultPassage;
    supportingFactDebug?: SupportingFactDebug;
  },
  context: SearchContext
): boolean {
  if (!layers.supportingFactPassage) return true;
  if (!isSentenceStyleReasoningQuery(context)) return false;
  const debug = layers.supportingFactDebug;
  if (!debug || debug.source !== "matched_pool") return false;
  return debug.anchorHits === 0 && debug.secondaryHits === 0 && debug.coverageRatio === 0 && debug.factualAnchorScore < 0.08;
}

function buildDecisionDisplayLayers(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  supportingFactSource: SupportingFactDebug["source"] = "matched_pool"
): {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
} {
  if (candidates.length === 0) return {};

  const primaryAuthorityCandidate = pickPrimaryAuthorityCandidate(candidates, context);
  const supportingFactSelection = pickSupportingFactCandidate(candidates, context, primaryAuthorityCandidate?.row.chunkId, supportingFactSource);
  const supportingFactCandidate = supportingFactSelection?.candidate;

  return {
    primaryAuthorityPassage: primaryAuthorityCandidate ? toSearchResultPassage(primaryAuthorityCandidate, context, { kind: "authority" }) : undefined,
    supportingFactPassage: supportingFactCandidate ? toSearchResultPassage(supportingFactCandidate, context, { kind: "supporting_fact" }) : undefined,
    supportingFactDebug: supportingFactSelection
      ? {
          source: supportingFactSource,
          factualAnchorScore: supportingFactSelection.diagnostics.factualAnchorScore,
          anchorHits: supportingFactSelection.diagnostics.anchorHits,
          secondaryHits: supportingFactSelection.diagnostics.secondaryHits,
          coverageRatio: supportingFactSelection.diagnostics.coverageRatio
        }
      : undefined
  };
}

function decisionLayerSnippetText(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): string {
  return normalize(
    [
      layers?.primaryAuthorityPassage?.sectionLabel,
      layers?.primaryAuthorityPassage?.snippet,
      layers?.supportingFactPassage?.sectionLabel,
      layers?.supportingFactPassage?.snippet
    ]
      .filter(Boolean)
      .join(" ")
  );
}

function isGenericAweDecisionLayer(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): boolean {
  const text = decisionLayerSnippetText(layers);
  if (!text) return false;
  return (text.includes("report of alleged wrongful eviction") || containsWholeWord(text, "awe")) && !hasWrongfulEvictionLockoutContext(text);
}

function decisionLayerFingerprint(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): string {
  return decisionLayerSnippetText(layers)
    .replace(/\b(?:case\s+no\.?|no\.?)\s*[a-z]?\d+\b/g, " ")
    .replace(/\be\d+\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
}

function orderDecisionFirst(
  rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context?: SearchContext,
  decisionLayerMap?: Map<
    string,
    { primaryAuthorityPassage?: SearchResultPassage; supportingFactPassage?: SearchResultPassage; supportingFactDebug?: SupportingFactDebug }
  >
) {
  const grouped = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
  for (const candidate of rows) {
    const current = grouped.get(candidate.row.documentId) || [];
    current.push(candidate);
    grouped.set(candidate.row.documentId, current);
  }

  const groups = Array.from(grouped.entries())
    .map(([documentId, candidates]) => {
      const evidence = context ? buildDocumentEvidenceSummary(candidates, context) : null;
      const adjustedCandidates = candidates.map((candidate) => {
        const leadRowBoost = evidence && evidence.leadChunkId === candidate.row.chunkId ? evidence.leadBoost : 0;
        if (!leadRowBoost && !(evidence?.docBoost)) return candidate;
        return {
          row: candidate.row,
          diagnostics: {
            ...candidate.diagnostics,
            rerankScore: Number((candidate.diagnostics.rerankScore + leadRowBoost).toFixed(6)),
            why: uniq([
              ...candidate.diagnostics.why,
              leadRowBoost > 0 ? `document_factual_lead_boost:${leadRowBoost.toFixed(3)}` : "",
              ...(evidence?.docBoost ? evidence.reasons : [])
            ].filter(Boolean))
          }
        };
      });

      const sorted = adjustedCandidates.slice().sort((a, b) => {
        const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
        if (diff !== 0) return diff;
        return b.row.createdAt.localeCompare(a.row.createdAt);
      });
      const top = sorted[0];
      const supportScore = sorted.slice(1, 3).reduce((sum, item, index) => {
        const weight = index === 0 ? 0.18 : 0.08;
        return sum + item.diagnostics.rerankScore * weight;
      }, 0);
      const uniqueChunkTypes = uniq(sorted.map((item) => normalizeChunkTypeLabel(item.row.sectionLabel || ""))).filter(Boolean).length;
      const strongEvidenceCount = sorted.filter(
        (item) =>
          item.diagnostics.why.some((reason) => reason.startsWith("issue_term_overlap:") || reason.startsWith("procedural_term_overlap:")) ||
          item.diagnostics.why.includes("issue_preferred_chunk_type_boost") ||
          item.diagnostics.why.includes("eviction_protection_authority_boost")
      ).length;
      const docBoost = Math.min(0.14, Math.max(0, sorted.length - 1) * 0.025) + Math.min(0.05, uniqueChunkTypes * 0.015);
      const evidenceBoost = Math.min(0.08, strongEvidenceCount * 0.025);
      const trustBoost = top?.row.isTrustedTier === 1 ? 0.03 : 0;
      const layers = decisionLayerMap?.get(documentId);
      const layerText = decisionLayerSnippetText(layers);
      const authorityText = normalize(
        [layers?.primaryAuthorityPassage?.sectionLabel, layers?.primaryAuthorityPassage?.snippet].filter(Boolean).join(" ")
      );
      const supportText = normalize(
        [layers?.supportingFactPassage?.sectionLabel, layers?.supportingFactPassage?.snippet].filter(Boolean).join(" ")
      );
      const supportHasLockoutContext = hasWrongfulEvictionLockoutContext(supportText);
      let layerBoost = 0;
      const layerReasons: string[] = [];
      if (context && layers) {
        if (isConclusionsLikeSectionLabel(layers.primaryAuthorityPassage?.sectionLabel || "")) {
          layerBoost += 0.14;
          layerReasons.push("decision_layer_conclusions_authority_boost");
        } else if (isSentenceStyleReasoningQuery(context) && layers.primaryAuthorityPassage) {
          layerBoost -= 0.04;
          layerReasons.push("decision_layer_non_conclusions_authority_penalty");
        }
        if (layers.supportingFactPassage && isFindingsLikeSectionLabel(layers.supportingFactPassage.sectionLabel || "")) {
          layerBoost += 0.06;
          layerReasons.push("decision_layer_findings_support_boost");
        }
        if (layers.supportingFactDebug) {
          const debug = layers.supportingFactDebug;
          const habitabilitySupportWeight = hasHabitabilityServiceRestorationSignals(context.query) ? 1.35 : 1;
          const supportBoost = Math.min(
            hasHabitabilityServiceRestorationSignals(context.query) ? 0.36 : 0.26,
            (debug.factualAnchorScore * 0.18 + debug.anchorHits * 0.04 + debug.secondaryHits * 0.03 + debug.coverageRatio * 0.12) *
              habitabilitySupportWeight
          );
          if (supportBoost > 0) {
            layerBoost += supportBoost;
            layerReasons.push(`decision_layer_support_score_boost:${supportBoost.toFixed(3)}`);
          }
        }
        if (isSentenceStyleReasoningQuery(context) && layers.primaryAuthorityPassage && layers.supportingFactPassage) {
          layerBoost += 0.04;
          layerReasons.push("decision_layer_dual_snippet_boost");
        }
        if (isWrongfulEvictionIssueSearch(context.query) && hasWrongfulEvictionLockoutContext(layerText)) {
          layerBoost += 0.16;
          layerReasons.push("decision_layer_lockout_specific_boost");
        }
        if (requiresLockoutSpecificity(context.query)) {
          if (supportHasLockoutContext) {
            layerBoost += 0.22;
            layerReasons.push("decision_layer_support_lockout_minimum_boost");
          } else {
            layerBoost -= 0.42;
            layerReasons.push("decision_layer_missing_lockout_support_penalty");
          }
          if (!supportHasLockoutContext && (hasHarassmentContext(layerText) || hasRepairNoticeContext(layerText))) {
            layerBoost -= 0.16;
            layerReasons.push("decision_layer_generic_awe_overlap_penalty");
          }
        }
        if (hasHabitabilityServiceRestorationSignals(context.query)) {
          const authorityCoverage = habitabilityCoverageSignals(authorityText, context.query);
          const supportCoverage = habitabilityCoverageSignals(supportText, context.query);
          const combinedCoverage = habitabilityCoverageSignals(`${authorityText} ${supportText}`.trim(), context.query);

          if (supportCoverage.conditionSignalHits > 0) {
            layerBoost += 0.18;
            layerReasons.push("decision_layer_habitability_support_condition_boost");
          } else if (layers.supportingFactPassage) {
            layerBoost -= 0.12;
            layerReasons.push("decision_layer_habitability_missing_support_condition_penalty");
          }

          if (combinedCoverage.conditionSignalHits > 0 && combinedCoverage.reportingHits > 0) {
            layerBoost += 0.16;
            layerReasons.push("decision_layer_habitability_condition_notice_stitch");
          }
          if (combinedCoverage.conditionSignalHits > 0 && combinedCoverage.repairFailureHits > 0) {
            layerBoost += 0.18;
            layerReasons.push("decision_layer_habitability_condition_repair_stitch");
          }
          if (
            combinedCoverage.conditionSignalHits > 0 &&
            combinedCoverage.reportingHits > 0 &&
            combinedCoverage.repairFailureHits > 0
          ) {
            layerBoost += 0.22;
            layerReasons.push("decision_layer_habitability_full_fact_pattern_boost");
          }

          if (supportCoverage.reportingHits > 0 || supportCoverage.repairFailureHits > 0) {
            layerBoost += 0.08;
            layerReasons.push("decision_layer_habitability_support_fact_boost");
          }

          if (authorityCoverage.conditionSignalHits > 0 || authorityCoverage.repairFailureHits > 0) {
            layerBoost += 0.06;
            layerReasons.push("decision_layer_habitability_authority_alignment_boost");
          }
        }
        if (isPoopQuery(context.query)) {
          const strongPoopLayer =
            hasStrongPoopDecisionContext(`${authorityText} ${supportText}`.trim()) ||
            hasStrongPoopDecisionContext(layerText);
          const weakRodentPoopLayer =
            hasWeakRodentPoopContext(`${authorityText} ${supportText}`.trim()) || hasWeakRodentPoopContext(layerText);
          if (strongPoopLayer) {
            layerBoost += 0.42;
            layerReasons.push("decision_layer_poop_specificity_boost");
          } else if (weakRodentPoopLayer) {
            layerBoost -= 0.42;
            layerReasons.push("decision_layer_poop_rodent_only_penalty");
          }
        }
        if (isLockBoxQuery(context.query)) {
          const authorityHasLockBox = hasLockBoxContext(authorityText);
          const supportHasLockBox = hasLockBoxContext(supportText);
          if (authorityHasLockBox) {
            layerBoost += 0.28;
            layerReasons.push("decision_layer_lock_box_authority_boost");
          } else if (supportHasLockBox) {
            layerBoost -= 0.14;
            layerReasons.push("decision_layer_lock_box_support_only_penalty");
          }
        }
        if (isCameraPrivacyQuery(context.query)) {
          const authorityHasCameraPrivacy = hasCameraPrivacyContext(authorityText);
          const supportHasCameraPrivacy = hasCameraPrivacyContext(supportText);
          const authorityHasPrivacyOnly =
            /\bprivacy\b|\binvasion of privacy\b/.test(authorityText) &&
            !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b|\bvideo camera\b|\bvideo monitoring\b/.test(authorityText);
          if (authorityHasCameraPrivacy) {
            layerBoost += 0.3;
            layerReasons.push("decision_layer_camera_privacy_authority_boost");
          } else if (supportHasCameraPrivacy) {
            layerBoost -= 0.16;
            layerReasons.push("decision_layer_camera_privacy_support_only_penalty");
          } else if (authorityHasPrivacyOnly) {
            layerBoost -= 0.22;
            layerReasons.push("decision_layer_camera_privacy_generic_privacy_penalty");
          }
        }
      }

      let displayRows = sorted;
      if (context) {
        const displayScored = sorted
          .map((candidate) => ({
            candidate,
            displayScore: representativeChunkDisplayScore(candidate, context)
          }))
          .sort((a, b) => {
            const diff = b.displayScore - a.displayScore;
            if (diff !== 0) return diff;
            return b.candidate.diagnostics.rerankScore - a.candidate.diagnostics.rerankScore;
          });
        const displayLead = displayScored[0];
        if (displayLead) {
          const promotedCandidate =
            displayLead.candidate.row.chunkId === sorted[0]?.row.chunkId
              ? displayLead.candidate
              : {
                  row: displayLead.candidate.row,
                  diagnostics: {
                    ...displayLead.candidate.diagnostics,
                    why: uniq([
                      ...displayLead.candidate.diagnostics.why,
                      `representative_display_lead:${displayLead.displayScore.toFixed(3)}`
                    ])
                  }
                };
          displayRows = [
            promotedCandidate,
            ...sorted.filter((candidate) => candidate.row.chunkId !== displayLead.candidate.row.chunkId)
          ];
        }
      }

      return {
        documentId,
        docScore: Number(
          ((top?.diagnostics.rerankScore || 0) + supportScore + docBoost + evidenceBoost + trustBoost + (evidence?.docBoost || 0) + layerBoost).toFixed(6)
        ),
        rows: displayRows.map((candidate, index) =>
          index === 0 && layerReasons.length
            ? {
                row: candidate.row,
                diagnostics: {
                  ...candidate.diagnostics,
                  why: uniq([...candidate.diagnostics.why, ...layerReasons])
                }
              }
            : candidate
        ),
        hasStrongLockoutFacts:
          Boolean(
            layers?.supportingFactDebug &&
              supportHasLockoutContext &&
              layers.supportingFactDebug.factualAnchorScore >= 0.7
          ) || false,
        hasAnyLockoutFacts: supportHasLockoutContext || hasWrongfulEvictionLockoutContext(layerText),
        hasPackageDeliveryEvidence:
          hasPackageDeliverySecurityContext(layerText) ||
          /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
            layerText
          ),
        hasCameraPrivacyAuthorityEvidence: hasCameraPrivacyContext(authorityText),
        hasCameraPrivacySupportEvidence: hasCameraPrivacyContext(supportText),
        isCameraPrivacyGenericLike:
          isCameraPrivacyQuery(context?.query || "") &&
          (/\bprivacy\b|\binvasion of privacy\b/.test(layerText) &&
            !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b|\bvideo camera\b|\bvideo monitoring\b/.test(layerText)),
        isPackageSecurityGenericLike:
          isPackageSecurityQuery(context?.query || "") &&
          (/housing services are those services provided by the landlord|loss of any tenant housing services|housing services reasonably expected|planning code section 207|accessory dwelling unit|\badu\b/.test(
            layerText
          ) &&
            !(
              hasPackageDeliverySecurityContext(layerText) ||
              /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
                layerText
              )
            )),
        hasStrongPoopEvidence: hasStrongPoopDecisionContext(layerText),
        hasWeakRodentPoopEvidence: hasWeakRodentPoopContext(layerText),
        isGenericAweLike: isGenericAweDecisionLayer(layers),
        genericAweFingerprint: decisionLayerFingerprint(layers)
      };
    });

  if (context && isWrongfulEvictionIssueSearch(context.query)) {
    const hasStrongerLockoutDecision = groups.some((group) => group.hasStrongLockoutFacts);
    if (hasStrongerLockoutDecision) {
      for (const group of groups) {
        if (!group.hasAnyLockoutFacts) {
          group.docScore = Number((group.docScore - 0.22).toFixed(6));
        }
        if (group.isGenericAweLike && !group.hasStrongLockoutFacts) {
          group.docScore = Number((group.docScore - 0.28).toFixed(6));
        }
        if (group.hasAnyLockoutFacts && !group.hasStrongLockoutFacts) {
          group.docScore = Number((group.docScore - 0.08).toFixed(6));
        }
      }
      const preSorted = groups
        .slice()
        .sort((a, b) => {
          const diff = b.docScore - a.docScore;
          if (diff !== 0) return diff;
          return a.documentId.localeCompare(b.documentId);
        });
      const seenGenericAweFingerprints = new Set<string>();
      for (const group of preSorted) {
        if (!group.isGenericAweLike) continue;
        const fingerprint = group.genericAweFingerprint;
        if (!fingerprint) continue;
        if (seenGenericAweFingerprints.has(fingerprint)) {
          group.docScore = Number((group.docScore - 0.26).toFixed(6));
          continue;
        }
        seenGenericAweFingerprints.add(fingerprint);
      }
    }
  }

  if (context && isPackageSecurityQuery(context.query)) {
    const hasSpecificPackageDecision = groups.some((group) => group.hasPackageDeliveryEvidence);
    if (hasSpecificPackageDecision) {
      for (const group of groups) {
        if (group.hasPackageDeliveryEvidence) {
          group.docScore = Number((group.docScore + 0.18).toFixed(6));
        } else if (group.isPackageSecurityGenericLike) {
          group.docScore = Number((group.docScore - 0.42).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  if (context && isCameraPrivacyQuery(context.query)) {
    const hasSpecificCameraPrivacyDecision = groups.some((group) => group.hasCameraPrivacyAuthorityEvidence);
    if (hasSpecificCameraPrivacyDecision) {
      for (const group of groups) {
        if (group.hasCameraPrivacyAuthorityEvidence) {
          group.docScore = Number((group.docScore + 0.26).toFixed(6));
        } else if (group.hasCameraPrivacySupportEvidence) {
          group.docScore = Number((group.docScore - 0.16).toFixed(6));
        } else if (group.isCameraPrivacyGenericLike) {
          group.docScore = Number((group.docScore - 0.32).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  if (context && isPoopQuery(context.query)) {
    const hasStrongPoopDecision = groups.some((group) => group.hasStrongPoopEvidence);
    if (hasStrongPoopDecision) {
      for (const group of groups) {
        if (group.hasStrongPoopEvidence) {
          group.docScore = Number((group.docScore + 0.34).toFixed(6));
        } else if (group.hasWeakRodentPoopEvidence) {
          group.docScore = Number((group.docScore - 0.44).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  return groups
    .sort((a, b) => {
      const diff = b.docScore - a.docScore;
      if (diff !== 0) return diff;
      return a.documentId.localeCompare(b.documentId);
    })
    .flatMap((group) => group.rows);
}

function hasDecisionScopedSignal(diagnostics: RankingDiagnostics): boolean {
  return (
    diagnostics.lexicalScore > 0 ||
    diagnostics.vectorScore > 0 ||
    diagnostics.exactPhraseBoost > 0 ||
    diagnostics.citationBoost > 0 ||
    diagnostics.metadataBoost > 0 ||
    diagnostics.partyNameBoost > 0 ||
    diagnostics.judgeNameBoost > 0
  );
}

function hasRelaxedCombinedFilterRecoverySignal(
  row: ChunkRow,
  diagnostics: RankingDiagnostics,
  context: SearchContext
): boolean {
  if (hasDecisionScopedSignal(diagnostics)) return true;

  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(row.sectionLabel || "");
  const sectionPriorityChunk = conclusionsLikeChunk || findingsLikeChunk;

  if (!sectionPriorityChunk) return false;
  if (diagnostics.rerankScore >= 0.22) return true;
  if (chunkMatchesIssueTerms(row, context.query) || chunkMatchesProceduralTerms(row, context.query)) return true;
  if (diagnostics.metadataBoost > 0 || diagnostics.judgeNameBoost > 0) return true;
  return diagnostics.sectionBoost >= 0.14;
}

function applyCombinedFilterRecoveryBoost(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext
) {
  const structuredFilterKinds = activeStructuredFilterKinds(context.filters);
  if (structuredFilterKinds.length < 2) return candidate;

  let recoveryBoost = 0;
  const reasons: string[] = [];
  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");

  if (conclusionsLikeChunk) {
    recoveryBoost += 0.08;
    reasons.push("combined_filter_zero_hit_recovery_conclusions");
  } else if (findingsLikeChunk) {
    recoveryBoost += 0.04;
    reasons.push("combined_filter_zero_hit_recovery_findings");
  }
  if (candidate.diagnostics.metadataBoost > 0) {
    recoveryBoost += 0.04;
    reasons.push("combined_filter_zero_hit_recovery_metadata");
  }
  if (candidate.diagnostics.judgeNameBoost > 0) {
    recoveryBoost += 0.03;
    reasons.push("combined_filter_zero_hit_recovery_judge");
  }
  if (candidate.diagnostics.exactPhraseBoost > 0 || candidate.diagnostics.lexicalScore >= 0.2) {
    recoveryBoost += 0.03;
    reasons.push("combined_filter_zero_hit_recovery_text");
  }
  if (recoveryBoost <= 0) return candidate;

  return {
    row: candidate.row,
    diagnostics: {
      ...candidate.diagnostics,
      rerankScore: Number((candidate.diagnostics.rerankScore + recoveryBoost).toFixed(6)),
      why: uniq([...candidate.diagnostics.why, ...reasons])
    }
  };
}

function buildIssueFamilyFallbackCandidates(
  base: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  explicitJudgeFilters: string[],
  section8UdDocumentSupportIds: Set<string> = new Set()
) {
  const guarded = base
    .filter(({ row }) => !isJudgeDrivenQuery(context.query) || rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters))
    .filter(({ row }) => !(requiresStrongIssueEvidence(context.query) && hasWrongContextForQuery(context.query, combinedSearchableText(row))))
    .filter(
      ({ row, diagnostics }) =>
        !(
          !isStructuralIntent(context) &&
          context.queryType !== "citation_lookup" &&
          hasSevereExtractionArtifact(row.chunkText) &&
          diagnostics.lexicalScore < 0.6
        )
    )
    .filter(
      ({ row, diagnostics }) =>
        !(
          !isStructuralIntent(context) &&
          context.queryType !== "citation_lookup" &&
          diagnostics.lexicalScore === 0 &&
          diagnostics.vectorScore > 0 &&
          (isLowSignalVectorOnlyChunkType(row.sectionLabel || "") || hasMalformedDocxArtifact(row.chunkText))
        )
    );

  const familyMatches = guarded.filter(({ row, diagnostics }) => {
    const searchableText = combinedSearchableText(row);
    const normalizedText = normalize(searchableText);

    if (requiresOwnerMoveInFollowThroughSpecificity(context.query)) {
      const conclusionsOccupancyProxy =
        isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
        hasOwnerMoveInOccupancyStandardContext(searchableText) &&
        diagnostics.sectionBoost >= 0.14;
      return (
        (hasOwnerMoveInContext(searchableText) || conclusionsOccupancyProxy) &&
        (
          (
            (hasOwnerMoveInFollowThroughContext(searchableText) || normalizedText.includes("owner occupancy")) &&
            (diagnostics.lexicalScore >= 0.2 || diagnostics.vectorScore >= 0.55)
          ) ||
          conclusionsOccupancyProxy
        )
      );
    }

    if (isSection8UnlawfulDetainerQuery(context.query)) {
      return (
        (
          hasSection8Context(searchableText) &&
          (hasUnlawfulDetainerContext(searchableText) || /\beviction\b/.test(normalizedText)) &&
          (diagnostics.lexicalScore >= 0.25 || diagnostics.vectorScore >= 0.55)
        ) ||
        (
          chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds) &&
          (diagnostics.lexicalScore >= 0.1 || diagnostics.vectorScore >= 0.45)
        )
      );
    }

    if (isBuyoutPressureQuery(context.query)) {
      return (
        hasBuyoutContext(searchableText) &&
        (hasBuyoutPressureContext(searchableText) || /coerc|pressur|threat|harass/.test(normalizedText)) &&
        (diagnostics.lexicalScore >= 0.25 || diagnostics.vectorScore >= 0.55)
      );
    }

    return false;
  });

  return familyMatches.map((candidate) => ({
    row: candidate.row,
    diagnostics: {
      ...candidate.diagnostics,
      rerankScore: Number((candidate.diagnostics.rerankScore + 0.02).toFixed(6)),
      why: uniq([...candidate.diagnostics.why, "issue_family_zero_hit_fallback"])
    }
  }));
}

function buildDecisionScopedCandidates(
  rows: ChunkRow[],
  vectorScores: Map<string, number>,
  context: SearchContext,
  decisionScopeDocumentIds: string[],
  explicitJudgeFilters: string[],
  options?: { relaxedCombinedFilterRecovery?: boolean }
) {
  const relaxedCombinedFilterRecovery = Boolean(options?.relaxedCombinedFilterRecovery);
  const section8UdDocumentSupportIds = isSection8UnlawfulDetainerQuery(context.query)
    ? buildSection8UdDocumentSupportSet(rows)
    : new Set<string>();

  const base = rows
    .map((row) => {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      return { row, diagnostics };
    })
    .filter(({ row }) => decisionScopeDocumentIds.includes(row.documentId))
    .filter(({ row }) => rowMatchesQueryGuard(row, context.query))
    .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, context.filters.chunkType));

  if (!relaxedCombinedFilterRecovery) {
    const strict = base
      .filter(({ row }) => !isJudgeDrivenQuery(context.query) || rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters))
      .filter(
        ({ row, diagnostics }) =>
          !(
            isConditionIssueQuery(context.query) &&
            isIssueDisfavoredChunkType(row.sectionLabel || "") &&
            !chunkMatchesIssueTerms(row, context.query) &&
            diagnostics.lexicalScore < 0.2
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            (isConditionIssueQuery(context.query) || isNoticeProceduralQuery(context.query)) &&
            isLowValueIssueIntentChunkType(row.sectionLabel || "") &&
            !chunkMatchesIssueTerms(row, context.query) &&
            !chunkMatchesProceduralTerms(row, context.query) &&
            diagnostics.lexicalScore < 0.24
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            isCoolingIssueQuery(context.query) &&
            !chunkMatchesIssueTerms(row, context.query) &&
            ((diagnostics.lexicalScore === 0 && diagnostics.vectorScore > 0) || diagnostics.lexicalScore < 0.3) &&
            !/findings? of fact|order/i.test(row.sectionLabel || "")
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            isCoolingIssueQuery(context.query) &&
            !chunkMatchesIssueTerms(row, context.query) &&
            diagnostics.lexicalScore < 0.35
          )
      )
      .filter(
        ({ row }) =>
          !(
            requiresStrongIssueEvidence(context.query) &&
            hasWrongContextForQuery(context.query, combinedSearchableText(row))
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            requiresStrongIssueEvidence(context.query) &&
            !hasStrongIssueEvidence(
              context.query,
              row,
              inferIssueTerms(context.query).filter((term) => normalize(combinedSearchableText(row)).includes(term)).length,
              inferProceduralTerms(context.query).filter((term) => normalize(combinedSearchableText(row)).includes(term)).length
            ) &&
            !(isSection8UnlawfulDetainerQuery(context.query) && chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds)) &&
            diagnostics.lexicalScore < 0.7 &&
            diagnostics.vectorScore < 0.72
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(() => {
            if (!requiresOwnerMoveInFollowThroughSpecificity(context.query)) return false;
            const searchableText = combinedSearchableText(row);
            const conclusionsOccupancyProxy =
              isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
              hasOwnerMoveInOccupancyStandardContext(searchableText) &&
              diagnostics.sectionBoost >= 0.14;
            return (
              ((!hasOwnerMoveInContext(searchableText) && !conclusionsOccupancyProxy) ||
                (!hasOwnerMoveInFollowThroughContext(searchableText) && !conclusionsOccupancyProxy)) &&
              diagnostics.lexicalScore < 0.88 &&
              diagnostics.vectorScore < 0.82
            );
          })()
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            isSection8UnlawfulDetainerQuery(context.query) &&
            (!hasSection8Context(combinedSearchableText(row)) || !hasUnlawfulDetainerContext(combinedSearchableText(row))) &&
            !chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds) &&
            diagnostics.lexicalScore < 0.92 &&
            diagnostics.vectorScore < 0.84
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            !isStructuralIntent(context) &&
            context.queryType !== "citation_lookup" &&
            hasSevereExtractionArtifact(row.chunkText) &&
            diagnostics.lexicalScore < 0.6
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            !isStructuralIntent(context) &&
            context.queryType !== "citation_lookup" &&
            diagnostics.lexicalScore === 0 &&
            diagnostics.vectorScore > 0 &&
            (isLowSignalVectorOnlyChunkType(row.sectionLabel || "") || hasMalformedDocxArtifact(row.chunkText))
          )
      )
      .filter(({ diagnostics }) => hasDecisionScopedSignal(diagnostics));

    if (strict.length > 0) return strict;
    return buildIssueFamilyFallbackCandidates(base, context, explicitJudgeFilters, section8UdDocumentSupportIds);
  }

  return base
    .filter(
      ({ row, diagnostics }) =>
        !(
          !isStructuralIntent(context) &&
          context.queryType !== "citation_lookup" &&
          hasSevereExtractionArtifact(row.chunkText) &&
          diagnostics.lexicalScore < 0.4 &&
          diagnostics.sectionBoost < 0.1
        )
    )
    .filter(
      ({ row, diagnostics }) =>
        !(
          !isStructuralIntent(context) &&
          context.queryType !== "citation_lookup" &&
          diagnostics.lexicalScore === 0 &&
          diagnostics.vectorScore > 0 &&
          hasMalformedDocxArtifact(row.chunkText) &&
          !isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
          !isFindingsLikeSectionLabel(row.sectionLabel || "")
        )
    )
    .filter(({ row, diagnostics }) => hasRelaxedCombinedFilterRecoverySignal(row, diagnostics, context))
    .map((candidate) => applyCombinedFilterRecoveryBoost(candidate, context));
}

function diversify(rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>, context: SearchContext, limit: number) {
  const maxPerDocument =
    context.filters.documentId
      ? Math.max(3, limit)
      : context.queryType === "rules_ordinance" || context.queryType === "index_code" || context.queryType === "citation_lookup" || context.queryType === "keyword"
        ? 1
        : 2;
  const perDoc = new Map<string, number>();
  const seenAnchors = new Set<string>();
  const selectedAnchorOrdinalsByDoc = new Map<string, number[]>();
  const out: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];

  for (const candidate of rows) {
    if (seenAnchors.has(candidate.row.citationAnchor)) continue;
    const count = perDoc.get(candidate.row.documentId) ?? 0;
    if (count >= maxPerDocument) continue;
    if (context.queryType === "citation_lookup") {
      const candidateOrdinal = parseAnchorOrdinal(candidate.row.citationAnchor || "");
      const selectedOrdinals = selectedAnchorOrdinalsByDoc.get(candidate.row.documentId) || [];
      if (
        candidateOrdinal != null &&
        selectedOrdinals.some((ord) => Math.abs(Number(ord) - candidateOrdinal) <= 1)
      ) {
        continue;
      }
      candidate.diagnostics.why = uniq([...candidate.diagnostics.why, "citation_doc_cap=1", "citation_anchor_neighbor_guard=1"]);
    }

    seenAnchors.add(candidate.row.citationAnchor);
    perDoc.set(candidate.row.documentId, count + 1);
    const ordinal = parseAnchorOrdinal(candidate.row.citationAnchor || "");
    if (ordinal != null) {
      selectedAnchorOrdinalsByDoc.set(candidate.row.documentId, [...(selectedAnchorOrdinalsByDoc.get(candidate.row.documentId) || []), ordinal]);
    }
    out.push(candidate);

    if (out.length >= limit) break;
  }

  return out;
}

function applyLowSignalStructuralGuard(
  rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  limit: number
) {
  if (isStructuralIntent(context) || context.queryType === "citation_lookup") {
    return rows.slice(0, limit);
  }

  const maxLowSignalInTop = Math.max(1, Math.floor(limit / 5));
  let lowSignalCount = 0;
  const preferred: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];
  const overflow: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];

  for (const candidate of rows) {
    const lowSignal = isLowSignalStructuralChunkType(candidate.row.sectionLabel || "");
    if (!lowSignal) {
      preferred.push(candidate);
      continue;
    }
    if (lowSignalCount < maxLowSignalInTop) {
      preferred.push(candidate);
      lowSignalCount += 1;
      continue;
    }
    overflow.push(candidate);
  }

  return [...preferred, ...overflow].slice(0, limit);
}

async function runSearchInternal(env: Env, parsed: SearchRequest, queryType: SearchContext["queryType"], includeDiagnostics: boolean) {
  await ensureSearchRuntimeIndexes(env);
  const logStage = (stage: string, details: Record<string, unknown> = {}) => {
    if (!includeDiagnostics) return;
    try {
      console.info("[search-debug]", JSON.stringify({ query: parsed.query, stage, ...details }));
    } catch {
      console.info("[search-debug]", stage);
    }
  };
  const totalStartedAt = Date.now();
  let scopeBuildMs = 0;
  let lexicalScopeFetchMs = 0;
  let lexicalSearchMs = 0;
  let vectorSearchMs = 0;
  let vectorChunkFetchMs = 0;
  let initialScoringMs = 0;
  let decisionScopeFetchMs = 0;
  let decisionScopeBuildMs = 0;
  let finalizeResultsMs = 0;
  let lexicalScopeDocumentCount = 0;
  let lexicalRowCount = 0;
  let mergedChunkCount = 0;
  let scoredCount = 0;
  let rerankedCount = 0;
  let decisionScopeDocumentCount = 0;
  let decisionScopeChunkCount = 0;
  const pageWindow = parsed.offset + parsed.limit + parsed.limit;
  const effectiveQuery = enhanceQueryWithIndexCodeContext(parsed.query, parsed.filters);
  const retrievalQuery = expandQueryForRetrieval(effectiveQuery);
  const vectorQuery = chooseVectorQuery(effectiveQuery);
  const ownerMoveInIssueSearch = isOwnerMoveInIssueSearch(retrievalQuery);
  const wrongfulEvictionIssueSearch = isWrongfulEvictionIssueSearch(retrievalQuery);
  const infestationAliasIssueSearch = isInfestationAliasQuery(retrievalQuery);
  const keywordFamilyRecallQuery = queryType === "keyword" && isKeywordFamilyRecallQuery(effectiveQuery);
  const lockoutSpecificityRequired = requiresLockoutSpecificity(retrievalQuery);
  const habitabilitySpecificityRequired = requiresHabitabilitySpecificity(retrievalQuery);
  const directLexicalIssueSearch = ownerMoveInIssueSearch || wrongfulEvictionIssueSearch || infestationAliasIssueSearch;
  const bypassScopedKeywordRecall = keywordFamilyRecallQuery && requestedJudgeFilters(parsed.filters).length > 0;
  const requestedCodes = requestedIndexCodeFilters(parsed.filters);
  const exactIndexCodeCoverage = requestedCodes.length > 0 ? await hasAnyExactIndexCodeCoverage(env, parsed.filters) : false;
  const useSoftIndexCodeScope = requestedCodes.length > 0 && !exactIndexCodeCoverage;
  const scopeBuildStartedAt = Date.now();
  const { where, params } = buildSearchScope(parsed, parsed.corpusMode, { useSoftIndexCodeScope });
  const recallConfig = buildAdaptiveRecallConfig(parsed, pageWindow);
  scopeBuildMs = Date.now() - scopeBuildStartedAt;
  logStage("scope_build", {
    ms: scopeBuildMs,
    hasStructuredFilters: recallConfig.hasStructuredFilters,
    issueGuidedSearch: recallConfig.issueGuidedSearch,
    shortBroadIssueSearch: recallConfig.shortBroadIssueSearch,
    ownerMoveInIssueSearch,
    wrongfulEvictionIssueSearch,
    infestationAliasIssueSearch,
    keywordFamilyRecallQuery,
    bypassScopedKeywordRecall,
    lockoutSpecificityRequired,
    habitabilitySpecificityRequired,
    exactIndexCodeCoverage,
    useSoftIndexCodeScope
  });
  logStage("lexical_scope_fetch_start", {
    issueGuidedSearch: recallConfig.issueGuidedSearch,
    shortBroadIssueSearch: recallConfig.shortBroadIssueSearch
  });
  const lexicalScopeStartedAt = Date.now();
  const keywordScopedUniverseDocumentIds =
    bypassScopedKeywordRecall
      ? await fetchScopedDocumentIds(
          env,
          where,
          params,
          Math.max(recallConfig.lexicalScopeDocumentLimit * 8, recallConfig.decisionScopeDocumentLimit * 10, pageWindow * 20, 400)
        )
      : [];
  let lexicalScopeDocumentIds = bypassScopedKeywordRecall
    ? await fetchKeywordCandidateDocumentIds(
        env,
        where,
        params,
        retrievalQuery,
        Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 3, 60),
        keywordScopedUniverseDocumentIds
      )
    : lockoutSpecificityRequired
    ? await fetchLockoutCandidateDocumentIds(
        env,
        where,
        params,
        retrievalQuery,
        Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 3, pageWindow * 3, 18)
      )
    : habitabilitySpecificityRequired
      ? await fetchHabitabilityCandidateDocumentIds(
          env,
          where,
          params,
          retrievalQuery,
          Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 3, pageWindow * 3, 18)
        )
    : recallConfig.issueGuidedSearch && !directLexicalIssueSearch
      ? await fetchIssueCandidateDocumentIds(
          env,
          where,
          params,
          retrievalQuery,
          Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 2)
        )
      : [];
  if (bypassScopedKeywordRecall && lexicalScopeDocumentIds.length === 0 && keywordScopedUniverseDocumentIds.length > 0) {
    lexicalScopeDocumentIds = keywordScopedUniverseDocumentIds.slice(
      0,
      Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 3, 60)
    );
  }
  if (!bypassScopedKeywordRecall && lexicalScopeDocumentIds.length === 0 && recallConfig.hasStructuredFilters) {
    lexicalScopeDocumentIds = await fetchScopedDocumentIds(
      env,
      where,
      params,
      recallConfig.lexicalScopeDocumentLimit
    );
  }
  if (!bypassScopedKeywordRecall && lexicalScopeDocumentIds.length === 0 && recallConfig.issueGuidedSearch && !directLexicalIssueSearch) {
    if (!isVectorFirstIssueSearch(retrievalQuery)) {
      lexicalScopeDocumentIds = await fetchScopedDocumentIds(
        env,
        where,
        params,
        Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow * 2)
      );
    }
  }
  lexicalScopeDocumentCount = lexicalScopeDocumentIds.length;
  lexicalScopeFetchMs = Date.now() - lexicalScopeStartedAt;
  logStage("lexical_scope_fetch", { ms: lexicalScopeFetchMs, lexicalScopeDocumentCount });
  logStage("lexical_search_start", {
    skipLexicalForVectorFirstIssueSearch:
      recallConfig.issueGuidedSearch &&
      isVectorFirstIssueSearch(retrievalQuery) &&
      lexicalScopeDocumentIds.length === 0
  });
  const lexicalSearchStartedAt = Date.now();
  const skipLexicalForVectorFirstIssueSearch =
    recallConfig.issueGuidedSearch &&
    isVectorFirstIssueSearch(retrievalQuery) &&
    lexicalScopeDocumentIds.length === 0;
  const keywordTermsOverride = queryType === "keyword" ? keywordExecutionTerms(effectiveQuery) : undefined;
  const allowDocumentChunkLexicalSearch =
    recallConfig.issueGuidedSearch || (queryType === "keyword" && lexicalScopeDocumentIds.length > 0);
  let lexicalRows = skipLexicalForVectorFirstIssueSearch
    ? []
    : await lexicalSearch(
        env,
        where,
        params,
        retrievalQuery,
        recallConfig.lexicalSearchLimit,
        lexicalScopeDocumentIds,
        { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
      );
  const keywordProvisionalFallbackEligible =
    parsed.corpusMode === "trusted_only" &&
    queryType === "keyword" &&
    (isLiteralKeywordQuery(effectiveQuery) || isInfestationAliasQuery(retrievalQuery) || matchedCuratedKeywordFamilies(effectiveQuery).length > 0) &&
    lexicalRows.length < Math.min(Math.max(parsed.limit, 8), 18);
  if (keywordProvisionalFallbackEligible) {
    const { where: provisionalWhere, params: provisionalParams } = buildSearchScope(parsed, "trusted_plus_provisional", {
      useSoftIndexCodeScope
    });
    const provisionalLexicalRows = await lexicalSearch(
      env,
      provisionalWhere,
      provisionalParams,
      retrievalQuery,
      recallConfig.lexicalSearchLimit,
      lexicalScopeDocumentIds,
      { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
    );
    const mergedLexicalRows = new Map<string, ChunkRow>();
    for (const row of lexicalRows) mergedLexicalRows.set(row.chunkId, row);
    for (const row of provisionalLexicalRows) {
      if (!mergedLexicalRows.has(row.chunkId)) mergedLexicalRows.set(row.chunkId, row);
    }
    lexicalRows = Array.from(mergedLexicalRows.values())
      .sort((a, b) => {
        const rankDiff = Number(b.lexicalRank || 0) - Number(a.lexicalRank || 0);
        if (rankDiff !== 0) return rankDiff;
        const trustDiff = Number(b.isTrustedTier || 0) - Number(a.isTrustedTier || 0);
        if (trustDiff !== 0) return trustDiff;
        const searchableDiff = String(b.searchableAt || "").localeCompare(String(a.searchableAt || ""));
        if (searchableDiff !== 0) return searchableDiff;
        return Number(a.orderRank || 0) - Number(b.orderRank || 0);
      })
      .slice(0, recallConfig.lexicalSearchLimit);
    logStage("keyword_provisional_lexical_fallback", {
      baseLexicalRowCount: lexicalRowCount,
      provisionalLexicalRowCount: provisionalLexicalRows.length,
      mergedLexicalRowCount: lexicalRows.length
    });
  }
  lexicalRowCount = lexicalRows.length;
  lexicalSearchMs = Date.now() - lexicalSearchStartedAt;
  logStage("lexical_search", { ms: lexicalSearchMs, lexicalRowCount, skipLexicalForVectorFirstIssueSearch });
  logStage("vector_search_start");
  const vectorSearchStartedAt = Date.now();
  const vectorRuntime = shouldSkipVectorSearch(effectiveQuery, parsed.filters, queryType)
    ? {
        scores: new Map<string, number>(),
        aiAvailable: Boolean(env.AI),
        vectorQueryAttempted: false,
        vectorMatchCount: 0
      }
    : await vectorSearchWithDiagnostics(env, [vectorQuery, retrievalQuery], recallConfig.vectorSearchLimit);
  vectorSearchMs = Date.now() - vectorSearchStartedAt;
  logStage("vector_search", { ms: vectorSearchMs, vectorQueryAttempted: vectorRuntime.vectorQueryAttempted, vectorMatchCount: vectorRuntime.vectorMatchCount });
  const vectorScores = vectorRuntime.scores;

  const merged = new Map<string, ChunkRow>();
  for (const row of lexicalRows) merged.set(row.chunkId, row);

  const vectorChunkFetchStartedAt = Date.now();
  const extraVectorIds = Array.from(vectorScores.keys()).filter((id) => !merged.has(id));
  logStage("vector_chunk_fetch_start", { extraVectorIdCount: extraVectorIds.length });
  const issueSpecificScopeRequired = lockoutSpecificityRequired || habitabilitySpecificityRequired;
  const vectorRows = (await fetchChunksByIds(env, extraVectorIds, where, params)).filter((row) =>
    !issueSpecificScopeRequired || lexicalScopeDocumentIds.length === 0 || lexicalScopeDocumentIds.includes(row.documentId)
  );
  vectorChunkFetchMs = Date.now() - vectorChunkFetchStartedAt;
  for (const row of vectorRows) merged.set(row.chunkId, row);
  if (merged.size === 0 && recallConfig.issueGuidedSearch && lexicalScopeDocumentIds.length > 0) {
    const recoveryDocLimit = recallConfig.shortBroadIssueSearch
      ? recallConfig.hasStructuredFilters
        ? 4
        : 6
      : recallConfig.hasStructuredFilters
        ? 6
        : 8;
    const recoveryDocumentIds = lexicalScopeDocumentIds.slice(
      0,
      Math.min(
        recoveryDocLimit,
        Math.max(1, Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow))
      )
    );
    const recoveryRows = await fetchChunksByDocumentIds(env, recoveryDocumentIds, where, params);
    for (const row of recoveryRows) merged.set(row.chunkId, row);
    logStage("short_broad_zero_hit_recovery", {
      recoveryDocumentCount: recoveryDocumentIds.length,
      recoveryRowCount: recoveryRows.length
    });
  }
  mergedChunkCount = merged.size;
  logStage("vector_chunk_fetch", { ms: vectorChunkFetchMs, mergedChunkCount });

  const context: SearchContext = {
    query: effectiveQuery,
    retrievalQuery,
    vectorQuery,
    queryType,
    filters: parsed.filters,
    snippetMaxLength: parsed.snippetMaxLength
  };
  const explicitJudgeFilters = requestedJudgeFilters(parsed.filters);

  const initialScoringStartedAt = Date.now();
  logStage("initial_scoring_start", { mergedChunkCount: merged.size });
  const scoreMergedRows = (rows: ChunkRow[]) =>
    rows
      .map((row) => {
        const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
        return { row, diagnostics };
      })
      .filter(({ row }) => rowMatchesQueryGuard(row, effectiveQuery))
      .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, parsed.filters.chunkType));

  let scored = scoreMergedRows(Array.from(merged.values()));
  const wholeWordKeywordRescueEligible =
    queryType === "keyword" &&
    scored.length === 0 &&
    lexicalRows.length > 0 &&
    (isLiteralKeywordQuery(effectiveQuery) || matchedCuratedKeywordFamilies(effectiveQuery).length > 0);
  if (wholeWordKeywordRescueEligible) {
    const { where: provisionalWhere, params: provisionalParams } = buildSearchScope(parsed, "trusted_plus_provisional", {
      useSoftIndexCodeScope
    });
    const wholeWordRows = await lexicalSearchWholeWord(
      env,
      provisionalWhere,
      provisionalParams,
      effectiveQuery,
      recallConfig.lexicalSearchLimit,
      lexicalScopeDocumentIds,
      { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
    );
    merged.clear();
    for (const row of wholeWordRows) merged.set(row.chunkId, row);
    mergedChunkCount = merged.size;
    scored = wholeWordRows
      .map((row) => {
        const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
        return { row, diagnostics };
      })
      .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, parsed.filters.chunkType));
    logStage("whole_word_keyword_rescue", {
      wholeWordRowCount: wholeWordRows.length,
      mergedChunkCount,
      rescuedScoredCount: scored.length
    });
  }
  scoredCount = scored.length;
  initialScoringMs = Date.now() - initialScoringStartedAt;
  logStage("initial_scoring", { ms: initialScoringMs, scoredCount: scored.length });

  // Index-code filters should admit decisions at the document scope, then let the query rank
  // the best passages inside those decisions. A hard chunk-level compatibility filter causes
  // good decisions to disappear when the right passage does not itself mention the selected code.
  const scoredWithIndexCompatibility = scored;

  const docHitCounts = scoredWithIndexCompatibility.reduce<Map<string, number>>((acc, candidate) => {
    acc.set(candidate.row.documentId, (acc.get(candidate.row.documentId) ?? 0) + 1);
    return acc;
  }, new Map());

  const docAwareScored = scoredWithIndexCompatibility.map((candidate) => {
    const docHitCount = docHitCounts.get(candidate.row.documentId) ?? 1;
    const docCoverageBoost = Math.min(0.12, Math.max(0, docHitCount - 1) * 0.025);
    if (docCoverageBoost <= 0) return candidate;
    return {
      row: candidate.row,
      diagnostics: {
        ...candidate.diagnostics,
        rerankScore: Number((candidate.diagnostics.rerankScore + docCoverageBoost).toFixed(6)),
        why: uniq([...candidate.diagnostics.why, `document_multi_match_boost:${docHitCount}`])
      }
    };
  });

  const rerankedBase = queryType === "citation_lookup"
    ? (() => {
        const byFamily = countBy(docAwareScored.map(({ row }) => buildCitationFamilySignature(row)));
        return docAwareScored.map((candidate) => {
          const familyKey = buildCitationFamilySignature(candidate.row);
          const familyCount = Number(byFamily[familyKey] || 0);
          const familyPenalty = familyCount > 1 ? Number(Math.min(0.24, (familyCount - 1) * 0.03).toFixed(6)) : 0;
          const adjusted = Number(Math.max(0, candidate.diagnostics.rerankScore - familyPenalty).toFixed(6));
          return {
            row: candidate.row,
            diagnostics: {
              ...candidate.diagnostics,
              rerankScore: adjusted,
              why: uniq([
                ...candidate.diagnostics.why,
                familyPenalty > 0 ? `citation_family_repeat_penalty:${familyKey}:${familyPenalty.toFixed(3)}` : ""
              ].filter(Boolean))
            }
          };
        });
      })()
    : docAwareScored;

  const reranked = rerankedBase
    .sort((a, b) => {
      const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
      if (diff !== 0) return diff;
      return b.row.createdAt.localeCompare(a.row.createdAt);
    });
  rerankedCount = reranked.length;

  const ownerMoveInFollowThroughDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const ownerMoveInFollowThroughSyntheticSeedIds =
    requiresOwnerMoveInFollowThroughSpecificity(context.query)
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "owner occupancy principal residence",
          ownerMoveInFollowThroughDecisionScopeLimit
        )
      : [];

  const buyoutPressureDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const buyoutPressureSyntheticSeedIds =
    isBuyoutPressureQuery(context.query)
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "buyout payment vacate threats intimidation coercion",
          buyoutPressureDecisionScopeLimit
        )
      : [];

  const section8UnlawfulDetainerDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const section8UnlawfulDetainerSyntheticSeedIds =
    isSection8UnlawfulDetainerQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "section 8 eviction action",
            section8UnlawfulDetainerDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "housing choice voucher unlawful detainer complaint",
            section8UnlawfulDetainerDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "section 8 unlawful detainer complaint",
            section8UnlawfulDetainerDecisionScopeLimit
          ))
        ]).slice(0, section8UnlawfulDetainerDecisionScopeLimit)
      : [];

  const homeownersExemptionDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const homeownersExemptionSyntheticSeedIds =
    isHomeownersExemptionQuery(context.query)
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "homeowner property tax exemption principal residence",
          homeownersExemptionDecisionScopeLimit
        )
      : [];

  const coLivingDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const coLivingSyntheticSeedIds =
    isCoLivingQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separate tenancy individual room",
            coLivingDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separate rental agreements common areas",
            coLivingDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separately rented rooms common areas",
            coLivingDecisionScopeLimit
          ))
        ]).slice(0, coLivingDecisionScopeLimit)
      : [];

  const collegeDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const collegeSyntheticSeedIds =
    isCollegeQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "temporarily living in student housing attend school",
            collegeDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "school breaks intends to return to live full-time in the unit",
            collegeDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "attend college temporary absence permanent residence",
            collegeDecisionScopeLimit
          ))
        ]).slice(0, collegeDecisionScopeLimit)
      : [];

  const selfEmployedDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const selfEmployedSyntheticSeedIds =
    isSelfEmployedQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "self employed 1099 tax returns",
            selfEmployedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "self-employed files tax returns using the subject unit as address",
            selfEmployedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "1099 reporting income principal residence address",
            selfEmployedDecisionScopeLimit
          ))
        ]).slice(0, selfEmployedDecisionScopeLimit)
      : [];

  const adjudicatedDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const adjudicatedSyntheticSeedIds =
    isAdjudicatedQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "already decided precluded",
            adjudicatedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "properly adjudicated in state court",
            adjudicatedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "previously decided final decision",
            adjudicatedDecisionScopeLimit
          ))
        ]).slice(0, adjudicatedDecisionScopeLimit)
      : [];

  const socialMediaDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const socialMediaSyntheticSeedIds =
    isSocialMediaQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "social media principal residence",
            socialMediaDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "facebook roommate search",
            socialMediaDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "instagram occupancy principal residence",
            socialMediaDecisionScopeLimit
          ))
        ]).slice(0, socialMediaDecisionScopeLimit)
      : [];

  const caregiverDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const caregiverSyntheticSeedIds =
    isCaregiverQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "primary caregiver principal residence",
            caregiverDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "caregiver return to live in the unit",
            caregiverDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "caregiving principal residence family schedule",
            caregiverDecisionScopeLimit
          ))
        ]).slice(0, caregiverDecisionScopeLimit)
      : [];

  const poopDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const poopSyntheticSeedIds =
    isPoopQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "dog waste backyard",
            poopDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "human feces hallway sewage",
            poopDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "animal waste common areas",
            poopDecisionScopeLimit
          ))
        ]).slice(0, poopDecisionScopeLimit)
      : [];

  const mootDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const mootSyntheticSeedIds =
    isMootQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "rendered moot null and void",
            mootDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "rescinded administratively dismissed",
            mootDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "failure to repair claim is moot",
            mootDecisionScopeLimit
          ))
        ]).slice(0, mootDecisionScopeLimit)
      : [];

  const remoteWorkDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const remoteWorkSyntheticSeedIds =
    isRemoteWorkQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "work from home construction noise",
            remoteWorkDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "unable to work from home quiet enjoyment",
            remoteWorkDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "power was turned off unable to work from home",
            remoteWorkDecisionScopeLimit
          ))
        ]).slice(0, remoteWorkDecisionScopeLimit)
      : [];

  const divorceDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const divorceSyntheticSeedIds =
    isDivorceQuery(context.query)
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "divorced separated spouse moved out",
            divorceDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "marital issues live separately",
            divorceDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "divorce separation residence occupancy",
            divorceDecisionScopeLimit
          ))
        ]).slice(0, divorceDecisionScopeLimit)
      : [];

  const legacyPestIssueDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const requestedLegacyPestCodes = requestedIndexCodeFilters(parsed.filters);
  const legacyPestSeedQuery =
    /cockroach|cockroaches|roach|roaches/.test(normalize(context.query)) && requestedLegacyPestCodes.includes("G44")
      ? "cockroach infestation"
      : /rodent|rodents|rat|rats|mouse|mice/.test(normalize(context.query)) && requestedLegacyPestCodes.includes("G76")
        ? "rodent infestation"
        : "";
  const relaxedLegacyPestParsed = legacyPestSeedQuery
    ? { ...parsed, filters: { ...parsed.filters, indexCodes: [] } }
    : null;
  const relaxedLegacyPestScope = relaxedLegacyPestParsed
    ? buildSearchScope(relaxedLegacyPestParsed, parsed.corpusMode, { useSoftIndexCodeScope: false })
    : null;
  const legacyPestSyntheticSeedIds =
    legacyPestSeedQuery && relaxedLegacyPestScope
      ? await fetchKeywordCandidateDocumentIds(
          env,
          relaxedLegacyPestScope.where,
          relaxedLegacyPestScope.params,
          legacyPestSeedQuery,
          legacyPestIssueDecisionScopeLimit
        )
      : [];

  const issueFamilyDecisionScopeSeedIds =
    requiresOwnerMoveInFollowThroughSpecificity(context.query)
      ? uniq([
          ...reranked
            .filter(({ row, diagnostics }) => {
              const searchableText = combinedSearchableText(row);
              const conclusionsOccupancyProxy =
                isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
                hasOwnerMoveInOccupancyStandardContext(searchableText);
              return (
                (hasOwnerMoveInContext(searchableText) || conclusionsOccupancyProxy) &&
                (hasOwnerMoveInFollowThroughContext(searchableText) || conclusionsOccupancyProxy) &&
                (diagnostics.vectorScore > 0 || diagnostics.sectionBoost >= 0.14 || diagnostics.lexicalScore >= 0.1)
              );
            })
            .map((candidate) => candidate.row.documentId),
          ...ownerMoveInFollowThroughSyntheticSeedIds
        ]).slice(0, ownerMoveInFollowThroughDecisionScopeLimit)
      : isAccommodationQuery(context.query)
        ? uniq(
            reranked
              .filter(({ row, diagnostics }) => {
                const searchableText = combinedSearchableText(row);
                const normalizedText = normalize(searchableText);
                return (
                  hasAccommodationContext(searchableText) &&
                  (diagnostics.vectorScore > 0 ||
                    diagnostics.lexicalScore >= 0.12 ||
                    diagnostics.sectionBoost >= 0.1 ||
                    /\breasonable accommodation|service animal|support animal|emotional support animal|assistance animal\b/.test(
                      normalizedText
                    ))
                );
              })
              .map((candidate) => candidate.row.documentId)
          ).slice(0, Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit)))
      : isBuyoutPressureQuery(context.query)
        ? uniq([
            ...reranked
              .filter(({ row, diagnostics }) => {
                const searchableText = combinedSearchableText(row);
                return (
                  hasBuyoutContext(searchableText) &&
                  (hasBuyoutPressureContext(searchableText) || diagnostics.vectorScore > 0) &&
                  (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.15 || diagnostics.sectionBoost >= 0.12)
                );
              })
              .map((candidate) => candidate.row.documentId),
            ...buyoutPressureSyntheticSeedIds
          ]).slice(0, buyoutPressureDecisionScopeLimit)
        : isSection8UnlawfulDetainerQuery(context.query)
          ? uniq([
              ...reranked
                .filter(({ row, diagnostics }) => {
                  const searchableText = combinedSearchableText(row);
                  const normalizedText = normalize(searchableText);
                  return (
                    hasSection8Context(searchableText) &&
                    (hasUnlawfulDetainerContext(searchableText) || /\beviction\b/.test(normalizedText) || diagnostics.vectorScore > 0) &&
                    (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.15 || diagnostics.sectionBoost >= 0.12)
                  );
                })
                .map((candidate) => candidate.row.documentId),
              ...section8UnlawfulDetainerSyntheticSeedIds
            ]).slice(0, section8UnlawfulDetainerDecisionScopeLimit)
        : isHomeownersExemptionQuery(context.query)
          ? uniq([
              ...reranked
                .filter(({ row, diagnostics }) => {
                  const searchableText = combinedSearchableText(row);
                  return (
                    hasHomeownersExemptionContext(searchableText) &&
                    (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                  );
                })
                .map((candidate) => candidate.row.documentId),
              ...homeownersExemptionSyntheticSeedIds
            ]).slice(0, homeownersExemptionDecisionScopeLimit)
          : isDivorceQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasDivorceContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...divorceSyntheticSeedIds
              ]).slice(0, divorceDecisionScopeLimit)
          : isAdjudicatedQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasAdjudicatedContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...adjudicatedSyntheticSeedIds
              ]).slice(0, adjudicatedDecisionScopeLimit)
          : isSocialMediaQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasSocialMediaContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...socialMediaSyntheticSeedIds
              ]).slice(0, socialMediaDecisionScopeLimit)
          : isCaregiverQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasCaregiverContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...caregiverSyntheticSeedIds
              ]).slice(0, caregiverDecisionScopeLimit)
          : isPoopQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasPoopContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...poopSyntheticSeedIds
              ]).slice(0, poopDecisionScopeLimit)
          : isMootQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasMootContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...mootSyntheticSeedIds
              ]).slice(0, mootDecisionScopeLimit)
          : isSelfEmployedQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasSelfEmployedContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...selfEmployedSyntheticSeedIds
              ]).slice(0, selfEmployedDecisionScopeLimit)
          : isRemoteWorkQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasRemoteWorkContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...remoteWorkSyntheticSeedIds
              ]).slice(0, remoteWorkDecisionScopeLimit)
          : isCollegeQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasCollegeContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...collegeSyntheticSeedIds
              ]).slice(0, collegeDecisionScopeLimit)
          : isCoLivingQuery(context.query)
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = combinedSearchableText(row);
                    return (
                      hasCoLivingContext(searchableText) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...coLivingSyntheticSeedIds
              ]).slice(0, coLivingDecisionScopeLimit)
          : legacyPestSyntheticSeedIds.slice(0, legacyPestIssueDecisionScopeLimit);
  const topDecisionIds = uniq(orderDecisionFirst(reranked, context).map((candidate) => candidate.row.documentId)).slice(
    0,
    recallConfig.decisionScopeDocumentLimit
  );
  const issueSpecificSeedDecisionIds =
    issueSpecificScopeRequired && lexicalScopeDocumentIds.length > 0
      ? lexicalScopeDocumentIds.slice(0, Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow * 2))
      : [];
  let decisionScopeDocumentIds = uniq([...issueFamilyDecisionScopeSeedIds, ...issueSpecificSeedDecisionIds, ...topDecisionIds]);
  if (!bypassScopedKeywordRecall && recallConfig.fallbackDocumentLimit > 0) {
    const fallbackDocumentIds = await fetchScopedDocumentIds(env, where, params, recallConfig.fallbackDocumentLimit);
    for (const documentId of fallbackDocumentIds) {
      if (!decisionScopeDocumentIds.includes(documentId)) {
        decisionScopeDocumentIds.push(documentId);
      }
    }
  }
  decisionScopeDocumentCount = decisionScopeDocumentIds.length;
  const decisionScopeFetchStartedAt = Date.now();
  logStage("decision_scope_fetch_start", { decisionScopeDocumentCount });
  const useMergedOnlyDecisionScope = recallConfig.issueGuidedSearch || queryType === "keyword";
  let decisionScopeRows = useMergedOnlyDecisionScope
    ? Array.from(merged.values()).filter((row) => decisionScopeDocumentIds.includes(row.documentId))
    : await fetchChunksByDocumentIds(env, decisionScopeDocumentIds, where, params);
  const supplementalIssueSeedDocumentIds = uniq([
    ...ownerMoveInFollowThroughSyntheticSeedIds,
    ...buyoutPressureSyntheticSeedIds,
    ...section8UnlawfulDetainerSyntheticSeedIds,
    ...homeownersExemptionSyntheticSeedIds,
    ...divorceSyntheticSeedIds,
    ...adjudicatedSyntheticSeedIds,
    ...socialMediaSyntheticSeedIds,
    ...caregiverSyntheticSeedIds,
    ...poopSyntheticSeedIds,
    ...mootSyntheticSeedIds,
    ...selfEmployedSyntheticSeedIds,
    ...remoteWorkSyntheticSeedIds,
    ...collegeSyntheticSeedIds,
    ...coLivingSyntheticSeedIds
  ]);
  const relaxedSupplementalIssueSeedDocumentIds = uniq(legacyPestSyntheticSeedIds);
  if (useMergedOnlyDecisionScope && (supplementalIssueSeedDocumentIds.length > 0 || relaxedSupplementalIssueSeedDocumentIds.length > 0)) {
    const presentDecisionScopeDocumentIds = new Set(decisionScopeRows.map((row) => row.documentId));
    const seenDecisionScopeChunkIds = new Set(decisionScopeRows.map((row) => row.chunkId));
    const missingSupplementalIssueSeedDocumentIds = supplementalIssueSeedDocumentIds.filter(
      (documentId) => decisionScopeDocumentIds.includes(documentId) && !presentDecisionScopeDocumentIds.has(documentId)
    );
    if (missingSupplementalIssueSeedDocumentIds.length > 0) {
      const supplementalIssueRows = await fetchChunksByDocumentIds(env, missingSupplementalIssueSeedDocumentIds, where, params);
      for (const row of supplementalIssueRows) {
        if (seenDecisionScopeChunkIds.has(row.chunkId)) continue;
        decisionScopeRows.push(row);
        seenDecisionScopeChunkIds.add(row.chunkId);
      }
      logStage("synthetic_issue_seed_scope_fetch", {
        missingSyntheticIssueSeedDocumentCount: missingSupplementalIssueSeedDocumentIds.length,
        supplementalRowCount: supplementalIssueRows.length
      });
    }
    const missingRelaxedSupplementalIssueSeedDocumentIds = relaxedSupplementalIssueSeedDocumentIds.filter(
      (documentId) => decisionScopeDocumentIds.includes(documentId) && !presentDecisionScopeDocumentIds.has(documentId)
    );
    if (missingRelaxedSupplementalIssueSeedDocumentIds.length > 0 && relaxedLegacyPestScope) {
      const relaxedSupplementalIssueRows = await fetchChunksByDocumentIds(
        env,
        missingRelaxedSupplementalIssueSeedDocumentIds,
        relaxedLegacyPestScope.where,
        relaxedLegacyPestScope.params
      );
      for (const row of relaxedSupplementalIssueRows) {
        if (seenDecisionScopeChunkIds.has(row.chunkId)) continue;
        decisionScopeRows.push(row);
        seenDecisionScopeChunkIds.add(row.chunkId);
      }
      logStage("legacy_issue_seed_scope_fetch", {
        missingLegacyIssueSeedDocumentCount: missingRelaxedSupplementalIssueSeedDocumentIds.length,
        supplementalRowCount: relaxedSupplementalIssueRows.length
      });
    }
  }
  decisionScopeFetchMs = Date.now() - decisionScopeFetchStartedAt;
  logStage("decision_scope_fetch", {
    ms: decisionScopeFetchMs,
    decisionScopeDocumentCount,
    decisionScopeRowCount: decisionScopeRows.length,
    useMergedOnlyDecisionScope
  });
  const decisionScopeMerged = new Map<string, ChunkRow>();
  for (const row of decisionScopeRows) {
    decisionScopeMerged.set(row.chunkId, row);
  }
  for (const row of merged.values()) {
    if (decisionScopeDocumentIds.includes(row.documentId)) {
      decisionScopeMerged.set(row.chunkId, row);
    }
  }
  decisionScopeChunkCount = decisionScopeMerged.size;

  const decisionScopeBuildStartedAt = Date.now();
  logStage("decision_scope_build_start", { decisionScopeChunkCount });
  let decisionScoped = buildDecisionScopedCandidates(
    Array.from(decisionScopeMerged.values()),
    vectorScores,
    context,
    decisionScopeDocumentIds,
    explicitJudgeFilters
  );
  const usedCombinedFilterZeroHitRecovery = decisionScoped.length === 0 && recallConfig.hasCombinedStructuredFilters;
  if (usedCombinedFilterZeroHitRecovery) {
    decisionScoped = buildDecisionScopedCandidates(
      Array.from(decisionScopeMerged.values()),
      vectorScores,
      context,
      decisionScopeDocumentIds,
      explicitJudgeFilters,
      { relaxedCombinedFilterRecovery: true }
    );
  }

  const section8UdDecisionScopedDocumentSupportIds = isSection8UnlawfulDetainerQuery(context.query)
    ? buildSection8UdDocumentSupportSet(Array.from(decisionScopeMerged.values()))
    : new Set<string>();

  const scopedDocHitCounts = decisionScoped.reduce<Map<string, number>>((acc, candidate) => {
    acc.set(candidate.row.documentId, (acc.get(candidate.row.documentId) ?? 0) + 1);
    return acc;
  }, new Map());

  const decisionScopedDocAware = decisionScoped.map((candidate) => {
    const docHitCount = scopedDocHitCounts.get(candidate.row.documentId) ?? 1;
    const docCoverageBoost = Math.min(0.12, Math.max(0, docHitCount - 1) * 0.025);
    const section8UdDocumentBoost =
      isSection8UnlawfulDetainerQuery(context.query) &&
      chunkQualifiesForSection8UdDocumentSupport(candidate.row, candidate.diagnostics, section8UdDecisionScopedDocumentSupportIds)
        ? isFindingsLikeSectionLabel(candidate.row.sectionLabel || "")
          ? 0.18
          : 0.1
        : 0;
    if (docCoverageBoost <= 0 && section8UdDocumentBoost <= 0) return candidate;
    return {
      row: candidate.row,
      diagnostics: {
        ...candidate.diagnostics,
        rerankScore: Number((candidate.diagnostics.rerankScore + docCoverageBoost + section8UdDocumentBoost).toFixed(6)),
        why: uniq([
          ...candidate.diagnostics.why,
          ...(docCoverageBoost > 0 ? [`document_multi_match_boost:${docHitCount}`] : []),
          ...(section8UdDocumentBoost > 0
            ? [isFindingsLikeSectionLabel(candidate.row.sectionLabel || "") ? "section8_ud_document_findings_boost" : "section8_ud_document_support_boost"]
            : [])
        ])
      }
    };
  });
  decisionScopeBuildMs = Date.now() - decisionScopeBuildStartedAt;
  logStage("decision_scope_build", { ms: decisionScopeBuildMs, decisionScopeChunkCount, decisionScopedCount: decisionScoped.length });

  const finalizeResultsStartedAt = Date.now();
  const decisionFirst = orderDecisionFirst(
    decisionScopedDocAware.sort((a, b) => {
      const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
      if (diff !== 0) return diff;
      return b.row.createdAt.localeCompare(a.row.createdAt);
    }),
    context
  );
  const decisionLayerMap = new Map<
    string,
    { primaryAuthorityPassage?: SearchResultPassage; supportingFactPassage?: SearchResultPassage; supportingFactDebug?: SupportingFactDebug }
  >();
  const decisionFirstByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
  for (const candidate of decisionFirst) {
    const current = decisionFirstByDecision.get(candidate.row.documentId) || [];
    current.push(candidate);
    decisionFirstByDecision.set(candidate.row.documentId, current);
  }
  for (const [documentId, candidates] of decisionFirstByDecision.entries()) {
    decisionLayerMap.set(documentId, buildDecisionDisplayLayers(candidates, context));
  }
  const authorityFallbackDocumentIds = Array.from(decisionLayerMap.entries())
    .filter(([, layers]) => !isConclusionsLikeSectionLabel(layers.primaryAuthorityPassage?.sectionLabel || ""))
    .map(([documentId]) => documentId)
    .slice(0, parsed.limit + parsed.offset + 10);
  if (authorityFallbackDocumentIds.length > 0) {
    const authorityFallbackRows = await fetchAuthorityChunksByDocumentIds(env, authorityFallbackDocumentIds, where, params);
    const fallbackByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
    for (const row of authorityFallbackRows) {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      const current = fallbackByDecision.get(row.documentId) || [];
      current.push({ row, diagnostics });
      fallbackByDecision.set(row.documentId, current);
    }
    for (const [documentId, fallbackCandidates] of fallbackByDecision.entries()) {
      const existing = decisionLayerMap.get(documentId) || {};
      const authorityCandidate = pickPrimaryAuthorityCandidate(fallbackCandidates, context);
      if (!authorityCandidate) continue;
      const primaryAuthorityPassage = toSearchResultPassage(authorityCandidate, context, { kind: "authority" });
      let supportingFactPassage = existing.supportingFactPassage;
      let supportingFactDebug = existing.supportingFactDebug;
      if (supportingFactPassage?.chunkId === primaryAuthorityPassage.chunkId) {
        supportingFactPassage = undefined;
        supportingFactDebug = undefined;
      }
      decisionLayerMap.set(documentId, {
        primaryAuthorityPassage,
        supportingFactPassage,
        supportingFactDebug
      });
    }
  }
  const missingSupportingFactDocumentIds = Array.from(decisionLayerMap.entries())
    .filter(([, layers]) => shouldRetrySupportingFactFallback(layers, context))
    .map(([documentId]) => documentId)
    .slice(0, parsed.limit + parsed.offset + 10);
  if (missingSupportingFactDocumentIds.length > 0) {
    const supportingFactFallbackRows = await fetchSupportingFactChunksByDocumentIds(
      env,
      missingSupportingFactDocumentIds,
      where,
      params,
      context
    );
    const fallbackByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
    for (const row of supportingFactFallbackRows) {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      const current = fallbackByDecision.get(row.documentId) || [];
      current.push({ row, diagnostics });
      fallbackByDecision.set(row.documentId, current);
    }
    for (const [documentId, fallbackCandidates] of fallbackByDecision.entries()) {
      const existing = decisionLayerMap.get(documentId) || {};
      const supportingFactSelection = pickSupportingFactCandidate(
        fallbackCandidates,
        context,
        existing.primaryAuthorityPassage?.chunkId,
        "fallback_findings_background_pool"
      );
      const supportingFactCandidate = supportingFactSelection?.candidate;
      if (supportingFactCandidate && supportingFactSelection) {
        decisionLayerMap.set(documentId, {
          primaryAuthorityPassage: existing.primaryAuthorityPassage,
          supportingFactPassage: toSearchResultPassage(supportingFactCandidate, context, { kind: "supporting_fact" }),
          supportingFactDebug: {
            source: "fallback_findings_background_pool",
            factualAnchorScore: supportingFactSelection.diagnostics.factualAnchorScore,
            anchorHits: supportingFactSelection.diagnostics.anchorHits,
            secondaryHits: supportingFactSelection.diagnostics.secondaryHits,
            coverageRatio: supportingFactSelection.diagnostics.coverageRatio
          }
        });
      }
    }
  }
  const decisionFirstLayerAware = orderDecisionFirst(decisionFirst, context, decisionLayerMap);
  const diversified = diversify(decisionFirstLayerAware, context, pageWindow * 2);
  const guarded = applyLowSignalStructuralGuard(diversified, context, pageWindow);
  const inferredJudgeNamesByDocument = inferDocumentJudgeNames(decisionScopeRows);

  const allResultRows = guarded.map(({ row, diagnostics }) => {
    const layers = decisionLayerMap.get(row.documentId);
    const primaryAuthorityPassage = layers?.primaryAuthorityPassage;
    return {
      documentId: row.documentId,
      chunkId: primaryAuthorityPassage?.chunkId ?? row.chunkId,
      title: row.title,
      citation: row.citation,
      authorName: sanitizeDisplayJudgeName(row.authorName) || inferredJudgeNamesByDocument.get(row.documentId) || null,
      decisionDate: row.decisionDate || null,
      fileType: row.fileType,
      snippet: buildLayeredResultSnippet(
        context,
        primaryAuthorityPassage,
        layers?.supportingFactPassage,
        layers?.supportingFactDebug,
        row.chunkText
      ),
      sectionLabel: primaryAuthorityPassage?.sectionLabel || row.sectionLabel,
      sourceFileRef: row.sourceFileRef,
      sourceLink: effectiveSourceLink(env, row.documentId, row.sourceLink),
      citationAnchor: primaryAuthorityPassage?.citationAnchor || row.citationAnchor,
      sectionHeading: primaryAuthorityPassage?.sectionHeading || row.sectionLabel,
      paragraphAnchor: primaryAuthorityPassage?.paragraphAnchor || row.paragraphAnchor,
      corpusTier: row.isTrustedTier === 1 ? "trusted" : "provisional",
      chunkType: primaryAuthorityPassage?.chunkType || normalizeChunkTypeLabel(row.sectionLabel),
      retrievalReason: diagnostics.why.slice(0, 4),
      matchedPassage: toSearchResultPassage({ row, diagnostics }, context),
      ...layers,
      score: diagnostics.rerankScore,
      lexicalScore: diagnostics.lexicalScore,
      vectorScore: diagnostics.vectorScore,
      ...(includeDiagnostics ? { diagnostics } : {})
    };
  });
  const pagedRows = allResultRows.slice(parsed.offset, parsed.offset + parsed.limit);
  const hasMore = allResultRows.length > parsed.offset + parsed.limit;
  finalizeResultsMs = Date.now() - finalizeResultsStartedAt;
  const totalMs = Date.now() - totalStartedAt;
  logStage("finalize_results", { ms: finalizeResultsMs, totalMs, resultCount: allResultRows.length });

  if (includeDiagnostics) {
    const tierCounts = pagedRows.reduce(
      (acc, row) => {
        if (row.corpusTier === "trusted") acc.trusted += 1;
        else acc.provisional += 1;
        return acc;
      },
      { trusted: 0, provisional: 0 }
    );
    return searchDebugResponseSchema.parse({
      query: parsed.query,
      queryType,
      corpusMode: parsed.corpusMode,
      offset: parsed.offset,
      limit: parsed.limit,
      hasMore,
      combinedFilterZeroHitRecoveryUsed: usedCombinedFilterZeroHitRecovery,
      tierCounts,
      filters: parsed.filters,
      runtimeDiagnostics: {
        aiAvailable: vectorRuntime.aiAvailable,
        vectorQueryAttempted: vectorRuntime.vectorQueryAttempted,
        vectorMatchCount: vectorRuntime.vectorMatchCount,
        vectorNamespace: env.VECTOR_NAMESPACE,
        lexicalScopeDocumentCount,
        lexicalRowCount,
        mergedChunkCount,
        scoredCount,
        rerankedCount,
        decisionScopeDocumentCount,
        decisionScopeChunkCount,
        stageTimingsMs: {
          scopeBuild: scopeBuildMs,
          lexicalScopeFetch: lexicalScopeFetchMs,
          lexicalSearch: lexicalSearchMs,
          vectorSearch: vectorSearchMs,
          vectorChunkFetch: vectorChunkFetchMs,
          initialScoring: initialScoringMs,
          decisionScopeFetch: decisionScopeFetchMs,
          decisionScopeBuild: decisionScopeBuildMs,
          finalizeResults: finalizeResultsMs,
          total: totalMs
        }
      },
      total: allResultRows.length,
      results: pagedRows
    });
  }

  const tierCounts = pagedRows.reduce(
    (acc, row) => {
      if (row.corpusTier === "trusted") acc.trusted += 1;
      else acc.provisional += 1;
      return acc;
    },
    { trusted: 0, provisional: 0 }
  );
  return searchResponseSchema.parse({
    query: parsed.query,
    corpusMode: parsed.corpusMode,
    offset: parsed.offset,
    limit: parsed.limit,
    hasMore,
    tierCounts,
    total: allResultRows.length,
    results: pagedRows
  });
}

export async function search(env: Env, input: unknown) {
  const parsed = searchRequestSchema.parse(input);
  try {
    return await runSearchInternal(env, parsed, "keyword", false);
  } catch (error) {
    if (isRetryableSearchError(error)) {
      return runSearchInternal(env, parsed, "keyword", false);
    }
    throw error;
  }
}

export async function searchDebug(env: Env, input: unknown) {
  const parsed = searchDebugRequestSchema.parse(input);
  try {
    return await runSearchInternal(env, parsed, parsed.queryType, true);
  } catch (error) {
    if (isRetryableSearchError(error)) {
      return runSearchInternal(env, parsed, parsed.queryType, true);
    }
    throw error;
  }
}
