// Pure query expansion and recall-channel policy; no bindings or I/O.
import { phraseConceptGroups } from "./search-concepts";
import { buildIndexCodeFilterContext, inferIssueTerms, keywordCandidateTerms, requestedIndexCodeFilters } from "./search-query-analysis";
import { hasOwnerMoveInPhrase, hasWrongfulEvictionPhrase, isAdjudicatedQuery, isAntInfestationQuery, isBuyoutPressureQuery, isCameraPrivacyQuery, isCaregiverQuery, isCoLivingQuery, isCollegeQuery, isCommonAreasQuery, isDivorceQuery, isDogQuery, isGarageSpaceQuery, isHomeownersExemptionQuery, isInfestationAliasQuery, isIntercomQuery, isMootQuery, isPackageSecurityQuery, isPoopQuery, isPorchQuery, isRemoteWorkQuery, isSection8UnlawfulDetainerQuery, isSelfEmployedQuery, isSocialMediaQuery, isStairsQuery, isWindowsQuery, requiresOwnerMoveInFollowThroughSpecificity } from "./search-query-classification";
import { containsWholeWord, meaningfulLexicalTokens, normalize, normalizeWhitespace, tokenize, uniq } from "./search-text";
import { SearchRequest } from "@beedle/shared";
import type { SearchContext } from "./search-types";
import { VECTOR_FIRST_ISSUE_TERMS, VECTOR_SKIP_BROAD_ISSUE_TERMS } from './search-evidence-policy';



// Upper bound on the keyword-family recall universe. fetchKeywordCandidateDocumentIds re-ranks this
// pre-ranked pool 12 docs at a time — one small lexical query per batch — so an unbounded pool fires
// hundreds of queries per request. The top of the (scope-ranked) pool holds the answers, so cap it to
// keep the bypass path fast without changing top-N quality.
export const KEYWORD_RECALL_UNIVERSE_MAX = 200;


const GENERIC_DECISION_QUERY_TERMS = new Set(["decision", "decisions", "document", "documents", "case", "cases", "search"]);


function isGenericDecisionQuery(query: string): boolean {
  return GENERIC_DECISION_QUERY_TERMS.has(normalize(query));
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


const NORMALIZED_VECTOR_SKIP_BROAD_ISSUE_TERMS = VECTOR_SKIP_BROAD_ISSUE_TERMS.map((term) => normalize(term));


const NORMALIZED_VECTOR_FIRST_ISSUE_TERMS = VECTOR_FIRST_ISSUE_TERMS.map((term) => normalize(term));


export function shouldSkipVectorSearch(
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

  const normalizedQueryContext = { normalizedQuery };
  if (
    requiresOwnerMoveInFollowThroughSpecificity(query, normalizedQueryContext) ||
    isBuyoutPressureQuery(query, normalizedQueryContext) ||
    isSection8UnlawfulDetainerQuery(query, normalizedQueryContext) ||
    isCameraPrivacyQuery(query, normalizedQueryContext) ||
    isPackageSecurityQuery(query, normalizedQueryContext) ||
    isDogQuery(query, normalizedQueryContext) ||
    isIntercomQuery(query, normalizedQueryContext) ||
    isGarageSpaceQuery(query, normalizedQueryContext) ||
    isCommonAreasQuery(query, normalizedQueryContext) ||
    isStairsQuery(query, normalizedQueryContext) ||
    isCoLivingQuery(query, normalizedQueryContext) ||
    isHomeownersExemptionQuery(query, normalizedQueryContext) ||
    isCollegeQuery(query, normalizedQueryContext) ||
    isDivorceQuery(query, normalizedQueryContext)
  ) return false;
  // NS-10: the vector-first check must precede the <=2-token skip — otherwise bare "harassment"/
  // "buyout" (the topics DESIGNED to lean on semantic recall) skipped the vector channel entirely
  // and, with lexical also skipped for the vector-first class, got neither.
  if (tokenCount <= 3 && NORMALIZED_VECTOR_FIRST_ISSUE_TERMS.some((term) => normalizedQuery.includes(term))) return false;
  if (tokenCount <= 2) return true;
  if (inferIssueTerms(query, normalizedQueryContext).length > 0 && tokenCount <= 12) return true;
  if (tokenCount <= 3 && NORMALIZED_VECTOR_SKIP_BROAD_ISSUE_TERMS.some((term) => normalizedQuery.includes(term))) return true;
  if (tokenCount <= 3 && shouldUseSoftIndexCodeScope(query, filters)) return true;
  return false;
}


export function enhanceQueryWithIndexCodeContext(query: string, filters: SearchRequest["filters"]): string {
  const trimmedQuery = String(query || "").trim();
  const indexContext = buildIndexCodeFilterContext(filters);
  if (!trimmedQuery || !indexContext.requestedCodes.length) return trimmedQuery;

  const contextualPhrases = uniq(indexContext.searchPhrases).filter(Boolean);
  const referenceHints = uniq([...indexContext.relatedRulesSections, ...indexContext.relatedOrdinanceSections]).filter(Boolean);

  if (isGenericDecisionQuery(trimmedQuery)) {
    const extras = uniq([...indexContext.requestedCodes, ...referenceHints, ...contextualPhrases]).slice(0, 8);
    return uniq([trimmedQuery, ...extras].filter(Boolean)).join(" ");
  }

  // A content-bearing query stays THE USER'S QUERY. The index-code filter already constrains the
  // document scope through the WHERE clause; injecting the code's catalog description into the query
  // double-counts the filter and drowns the user's terms — measured: "restrictions" + K12 became a
  // 12-token Costa-Hawkins word soup whose results ranked on the DESCRIPTION language, returning 1
  // of the 18 restriction-bearing K12 documents (and not a dense one). Description injection is
  // reserved for generic decision queries above, where the user supplied no content signal at all.
  return trimmedQuery;
}


export function keywordExecutionTerms(query: string, precomputed?: { normalizedQuery?: string; normalizedGroups?: string[][] }): string[] {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedGroups = precomputed?.normalizedGroups ?? phraseConceptGroups(normalizedQuery);
  if (normalizedGroups.length >= 2) {
    const normalized = normalizeWhitespace(normalizedQuery);
    const tokens = meaningfulLexicalTokens(normalizedQuery).slice(0, 4);
    return uniq([normalized, ...tokens].filter(Boolean)).slice(0, 5);
  }
  return keywordCandidateTerms(query, { normalizedQuery }).slice(0, 5);
}


export function expandQueryForRetrieval(query: string): string {
  const q = normalize(query);
  if (!q) return query;

  const additions: string[] = [];
  const add = (...values: string[]) => additions.push(...values);
  const hasOmiAcronym = containsWholeWord(q, "omi");
  const hasAweAcronym = containsWholeWord(q, "awe");

  if (/\b(?:heat|heating|heater|boiler|radiator\b)/.test(q)) {
    add("heat", "heating", "heater", "boiler", "radiator", "hot water");
  }
  if (/\b(?:cool|cooling|ventilation|air\b)/.test(q)) {
    add("cooling", "ventilation", "air flow", "air circulation", "overheating", "temperature control");
  }
  if (/\b(?:notice|service|served|mail\b)/.test(q)) {
    add("notice", "service", "served", "mailing", "posting", "repair request", "work order", "written notice");
  }
  if (/\b(?:repair|maintenance|condition|habitability\b)/.test(q)) {
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
  if (/\b(?:harassment|retaliation\b)/.test(q)) {
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


export function chooseVectorQuery(originalQuery: string): string {
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
