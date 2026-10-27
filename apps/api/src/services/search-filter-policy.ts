// Pure publication/filter SQL planning; parameter order and reference semantics are part of the contract.
import { searchPublicationSql } from "./publication-policy";
import { canonicalizeJudgeName } from "./judges";
import { normalizeFilterValue } from "./legal-references";
import { buildIndexCodeSelectionClause } from "./search-index-code-filter";
import { normalizeWhitespace, uniq } from "./search-text";
import { SearchRequest, canonicalIndexCodeOptions } from "@beedle/shared";
import type { DocumentReferenceSectionFacet, IndexCodeFilterContext, IndexCodeFilterContextOptions, SearchScopeOptions } from "./search-types";



// D1 rejects a prepared statement with more than ~100 bound parameters. The lexical recall queries bind
// several parameters per query term, so term expansions must be capped to keep each statement under it.
const D1_MAX_BOUND_PARAMS = 100;


// Cap a lexical query's term expansion so the whole statement stays under D1's bound-parameter limit. A
// statement binds `perTerm` parameters per term plus `fixedParams` non-term parameters (scope ids bound
// once or twice, filter binds, the LIMIT). Trimming to the largest term count that still fits is a no-op
// for any query already under the limit — it only trims queries that would otherwise overflow — so it is
// safe everywhere. Highest-priority terms come first, so slicing keeps the strongest signals.
export function boundLexicalTermsForD1(terms: string[], perTerm: number, fixedParams: number): string[] {
  const maxTerms = Math.max(1, Math.floor((D1_MAX_BOUND_PARAMS - fixedParams) / perTerm));
  return terms.length > maxTerms ? terms.slice(0, maxTerms) : terms;
}


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


export function requestedJudgeFilters(filters: SearchRequest["filters"]): string[] {
  const raw = uniq([...(filters.judgeNames || []), filters.judgeName || ""].filter(Boolean));
  return raw
    .map((value) => canonicalizeJudgeName(value))
    .filter((value): value is string => Boolean(value));
}


export function requestedIndexCodeFilters(filters: SearchRequest["filters"]): string[] {
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


export function buildIndexCodeFilterContext(
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


export function directIndexCodeMatchValuesForRequestedCode(code: string, options: IndexCodeFilterContextOptions = {}): string[] {
  const includeGenericDhsFamilyAlias = options.includeGenericDhsFamilyAlias !== false;
  const normalizedCode = normalizeFilterValue("index_code", code);
  const option = canonicalIndexCodeByNormalized.get(normalizedCode);
  const manualAliases = MANUAL_INDEX_CODE_ALIAS_MAP[normalizedCode];
  const values = new Set<string>([code]);
  for (const legacyCode of manualAliases?.legacyCodes || []) values.add(String(legacyCode).trim());
  if (includeGenericDhsFamilyAlias && isDhsFamilyIndexCodeOption(option)) values.add("13");
  return Array.from(values).filter(Boolean);
}


export function bindIndexCodeMatchValues(params: Array<string | number>, values: string[]) {
  for (const value of values) {
    params.push(normalizeFilterValue("index_code", value), value);
  }
  for (const value of values) {
    params.push(normalizeFilterValue("index_code", value), value);
  }
}


export function buildDirectIndexCodeCompatibilityClause(values: string[]): string {
  const facetClauses = values.map(() => "(dic.normalized_code = ? OR lower(dic.code) = lower(?))").join(" OR ");
  const referenceClauses = values.map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))").join(" OR ");
  return `(
    EXISTS (
      SELECT 1 FROM document_index_codes dic
      WHERE dic.document_id = d.id
        AND (${facetClauses})
    )
    OR EXISTS (
      SELECT 1 FROM document_reference_links l
      WHERE l.document_id = d.id
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
        AND (${referenceClauses})
    )
  )`;
}


export function bindReferenceSectionMatchValues(
  params: Array<string | number>,
  referenceType: DocumentReferenceSectionFacet,
  values: string[],
  options: { includePrefixMatch?: boolean } = {}
) {
  for (const value of values) {
    const normalizedValue = normalizeFilterValue(referenceType, value);
    params.push(normalizedValue, value);
    if (options.includePrefixMatch) params.push(`${normalizedValue}%`, `${value}%`);
  }
  for (const value of values) {
    const normalizedValue = normalizeFilterValue(referenceType, value);
    params.push(normalizedValue, value);
    if (options.includePrefixMatch) params.push(`${normalizedValue}%`, `${value}%`);
  }
}


export function buildReferenceSectionCompatibilityClause(
  referenceType: DocumentReferenceSectionFacet,
  values: string[],
  options: { includePrefixMatch?: boolean } = {}
): string {
  const isRules = referenceType === "rules_section";
  const table = isRules ? "document_rules_sections" : "document_ordinance_sections";
  const alias = isRules ? "drs" : "dos";
  const facetClauses = values
    .map(() =>
      options.includePrefixMatch
        ? `(${alias}.normalized_section = ? OR lower(${alias}.section) = lower(?) OR ${alias}.normalized_section LIKE ? OR lower(${alias}.section) LIKE lower(?))`
        : `(${alias}.normalized_section = ? OR lower(${alias}.section) = lower(?))`
    )
    .join(" OR ");
  const referenceClauses = values
    .map(() =>
      options.includePrefixMatch
        ? "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?) OR l.normalized_value LIKE ? OR lower(coalesce(l.canonical_value, '')) LIKE lower(?))"
        : "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))"
    )
    .join(" OR ");
  return `(
    EXISTS (
      SELECT 1 FROM ${table} ${alias}
      WHERE ${alias}.document_id = d.id
        AND (${facetClauses})
    )
    OR EXISTS (
      SELECT 1 FROM document_reference_links l
      WHERE l.document_id = d.id
        AND l.reference_type = '${referenceType}'
        AND l.is_valid = 1
        AND (${referenceClauses})
    )
  )`;
}


function buildRequestedIndexCodeClause(
  requestedCodes: string[],
  params: Array<string | number>,
  operator: "and" | "or"
): string {
  const codeGroups = requestedCodes.map((code) =>
    directIndexCodeMatchValuesForRequestedCode(code, { includeGenericDhsFamilyAlias: false }).map((value) => ({
      code: value,
      normalizedCode: normalizeFilterValue("index_code", value)
    }))
  );
  return buildIndexCodeSelectionClause(codeGroups, operator, params);
}


export function buildSearchScope(
  parsed: SearchRequest,
  corpusMode: SearchRequest["corpusMode"],
  options: SearchScopeOptions = {}
) {
  const clauses: string[] = [searchPublicationSql({ ...parsed, corpusMode })];
  const params: Array<string | number> = [];

  if (parsed.filters.jurisdiction) {
    clauses.push("d.jurisdiction = ?");
    params.push(parsed.filters.jurisdiction);
  }

  if (parsed.filters.documentId) {
    clauses.push("d.id = ?");
    params.push(parsed.filters.documentId);
  }

  if(parsed.filters.documentIds?.length){clauses.push("d.id IN (SELECT value FROM json_each(?))");params.push(JSON.stringify(parsed.filters.documentIds));}

  if (parsed.filters.fileType) {
    clauses.push("d.file_type = ?");
    params.push(parsed.filters.fileType);
  } else {
    // Product search is decision-first by default; other file types remain available via explicit filter.
    clauses.push("d.file_type = 'decision_docx'");
  }

  const indexCodeFilterContext = buildIndexCodeFilterContext(parsed.filters, { includeGenericDhsFamilyAlias: false });
  // An explicit Boolean selection must always constrain results, including codes
  // with no corpus coverage. Preserve legacy soft matching for older callers.
  const useSoftIndexCodeScope = Boolean(options.useSoftIndexCodeScope) && !parsed.filters.indexCodeOperator;
  if (indexCodeFilterContext.requestedCodes.length > 0 && !useSoftIndexCodeScope) {
    const compatibilityClauses: string[] = [];

    if (parsed.filters.indexCodeOperator || indexCodeFilterContext.requestedCodes.length > 1) {
      compatibilityClauses.push(
        buildRequestedIndexCodeClause(indexCodeFilterContext.requestedCodes, params, parsed.filters.indexCodeOperator ?? "and")
      );
    } else {
      const directIndexCodeValues = uniq([...indexCodeFilterContext.requestedCodes, ...indexCodeFilterContext.legacyCodeAliases]).filter(Boolean);

      if (directIndexCodeValues.length > 0) {
        compatibilityClauses.push(buildDirectIndexCodeCompatibilityClause(directIndexCodeValues));
        bindIndexCodeMatchValues(params, directIndexCodeValues);
      }

      if (indexCodeFilterContext.relatedRulesSections.length > 0) {
        compatibilityClauses.push(buildReferenceSectionCompatibilityClause("rules_section", indexCodeFilterContext.relatedRulesSections));
        bindReferenceSectionMatchValues(params, "rules_section", indexCodeFilterContext.relatedRulesSections);
      }

      if (indexCodeFilterContext.relatedOrdinanceSections.length > 0) {
        compatibilityClauses.push(buildReferenceSectionCompatibilityClause("ordinance_section", indexCodeFilterContext.relatedOrdinanceSections));
        bindReferenceSectionMatchValues(params, "ordinance_section", indexCodeFilterContext.relatedOrdinanceSections);
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
    clauses.push(buildReferenceSectionCompatibilityClause("rules_section", [parsed.filters.rulesSection]));
    bindReferenceSectionMatchValues(params, "rules_section", [parsed.filters.rulesSection]);
  }

  if (parsed.filters.ordinanceSection) {
    clauses.push(buildReferenceSectionCompatibilityClause("ordinance_section", [parsed.filters.ordinanceSection]));
    bindReferenceSectionMatchValues(params, "ordinance_section", [parsed.filters.ordinanceSection]);
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

  if (parsed.filters.decisionSeries) {
    // Citation is the canonical visible identifier. A small legacy set has a malformed citation,
    // so fall back to case_number and then the persisted title only when neither prior value has
    // a recognized T/L series prefix. This keeps filtering deterministic without hiding recoverable
    // legacy decisions or relying on result text in the browser.
    clauses.push(`(
      CASE
        WHEN upper(substr(trim(coalesce(d.citation, '')), 1, 1)) IN ('T', 'L')
          THEN upper(substr(trim(d.citation), 1, 1))
        WHEN upper(substr(trim(coalesce(d.case_number, '')), 1, 1)) IN ('T', 'L')
          THEN upper(substr(trim(d.case_number), 1, 1))
        ELSE upper(substr(trim(coalesce(d.title, '')), 1, 1))
      END
    ) = ?`);
    params.push(parsed.filters.decisionSeries);
  }

  if (parsed.filters.fromDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.valid_decision_date >= ?)");
    params.push(parsed.filters.fromDate);
  }

  if (parsed.filters.toDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.valid_decision_date <= ?)");
    params.push(parsed.filters.toDate);
  }

  return { where: `WHERE ${clauses.join(" AND ")}`, params };
}


export function activeStructuredFilterKinds(
  filters: SearchRequest["filters"],
  precomputed?: { requestedJudgeFilters?: string[]; requestedIndexCodeFilters?: string[] }
): string[] {
  const kinds: string[] = [];
  if ((precomputed?.requestedJudgeFilters ?? requestedJudgeFilters(filters)).length > 0) kinds.push("judge");
  if ((precomputed?.requestedIndexCodeFilters ?? requestedIndexCodeFilters(filters)).length > 0) kinds.push("index_code");
  if (filters.decisionSeries) kinds.push("decision_series");
  if (filters.rulesSection) kinds.push("rules_section");
  if (filters.ordinanceSection) kinds.push("ordinance_section");
  if (filters.partyName) kinds.push("party_name");
  if (filters.fromDate || filters.toDate) kinds.push("date_range");
  return kinds;
}
