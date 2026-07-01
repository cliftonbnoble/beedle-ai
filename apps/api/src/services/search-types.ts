// Shared search types extracted from search.ts (SEARCH-02c module split, step 5a).
//
// The core row / context / result interfaces used across the search service. Extracting them into a
// leaf module lets the DB (fts/scope) and scoring modules share these types without importing back from
// search.ts (which would be circular). Type-only relocation: zero runtime behavior change.

import type { SearchRequest, SearchDebugRequest } from "@beedle/shared";
export interface ChunkRow {
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

export interface RankingDiagnostics {
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

export interface SearchContext {
  query: string;
  retrievalQuery: string;
  vectorQuery: string;
  queryType: SearchDebugRequest["queryType"];
  filters: SearchRequest["filters"];
  snippetMaxLength: number;
  derived?: QueryDerivedContext;
  rowSearchableTextCache?: Map<string, string>;
  normalizedRowSearchableTextCache?: Map<string, string>;
  normalizedRowChunkTextCache?: Map<string, string>;
  rowMetadataCache?: Map<string, RowMetadata>;
}

export interface RowMetadata {
  normalizedIndexCodes: string[];
  normalizedRulesSections: string[];
  normalizedOrdinanceSections: string[];
  normalizedTitle: string;
  normalizedCitation: string;
}

export interface QueryDerivedContext {
  normalizedQuery: string;
  normalizedRetrievalQuery: string;
  queryIntent: QueryIntent;
  issueTerms: string[];
  normalizedIssueTerms: string[];
  proceduralTerms: string[];
  normalizedProceduralTerms: string[];
  longQueryTokens: string[];
  retrievalLexicalTokens: string[];
  normalizedRetrievalPhraseConceptGroups: string[][];
  primarySignals: string[];
  normalizedPrimarySignals: string[];
  sentenceIssueAnchors: string[];
  normalizedSentenceIssueAnchors: string[];
  sentenceSecondaryTokens: string[];
  normalizedSentenceSecondaryTokens: string[];
  normalizedSentenceFactualTokens: string[];
  phraseTokens: string[];
  sentencePhraseOverlapTokens: string[];
  normalizedPhraseConceptGroups: string[][];
  structuralIntent: boolean;
  sentenceStyleReasoningQuery: boolean;
  marketConditionReasoningQuery: boolean;
  phraseEvidenceQuery: boolean;
  antInfestationQuery: boolean;
  retrievalInfestationAliasQuery: boolean;
  retrievalOwnerMoveInIssueQuery: boolean;
  retrievalWrongfulEvictionIssueQuery: boolean;
  retrievalLockoutSpecificityRequired: boolean;
  retrievalHabitabilitySpecificityRequired: boolean;
  vectorFirstIssueQuery: boolean;
  keywordFamilyRecallQuery: boolean;
  curatedKeywordFamilyQuery: boolean;
  literalKeywordQuery: boolean;
  literalKeywordTokens: string[];
  keywordBoundaryGuardTerms: string[];
  leakWindowQuery: boolean;
  section8UdQuery: boolean;
  ownerMoveInQuery: boolean;
  ownerMoveInFollowThroughRequired: boolean;
  habitabilityServiceQuery: boolean;
  requiredHabitabilitySignals: string[];
  lockoutSpecificityRequired: boolean;
  lockBoxQuery: boolean;
  harassmentRetaliationQuery: boolean;
  wrongfulEvictionQuery: boolean;
  wrongfulEvictionIssueQuery: boolean;
  coolingIssueQuery: boolean;
  conditionIssueQuery: boolean;
  noticeProceduralQuery: boolean;
  strongIssueEvidenceRequired: boolean;
  accommodationQuery: boolean;
  homeownersExemptionQuery: boolean;
  selfEmployedQuery: boolean;
  adjudicatedQuery: boolean;
  socialMediaQuery: boolean;
  caregiverQuery: boolean;
  mootQuery: boolean;
  divorceQuery: boolean;
  remoteWorkQuery: boolean;
  collegeQuery: boolean;
  coLivingQuery: boolean;
  buyoutQuery: boolean;
  buyoutPressureQuery: boolean;
  rentReductionQuery: boolean;
  nuisanceQuery: boolean;
  evictionProtectionQuery: boolean;
  packageSecurityQuery: boolean;
  cameraPrivacyQuery: boolean;
  poopQuery: boolean;
  dogQuery: boolean;
  intercomQuery: boolean;
  garageSpaceQuery: boolean;
  commonAreasQuery: boolean;
  stairsQuery: boolean;
  porchQuery: boolean;
  windowsQuery: boolean;
  section8Query: boolean;
  unlawfulDetainerQuery: boolean;
  roomHeatQuery: boolean;
  judgeDrivenQuery: boolean;
  referencedJudges: string[];
  queryMentionsMold: boolean;
  queryMentionsMildew: boolean;
  indexCodeFilterContext: IndexCodeFilterContext;
  explicitIndexCodeFilters: string[];
  normalizedIndexCodeRelatedRulesSections: string[];
  normalizedIndexCodeRelatedOrdinanceSections: string[];
  normalizedIndexCodeSearchPhrases: string[];
  normalizedRulesSectionFilter: string;
  normalizedOrdinanceSectionFilter: string;
  normalizedPartyNameFilter: string;
  activeStructuredFilterKinds: string[];
  explicitJudgeFilters: string[];
  explicitJudgeLookupKeys: string[];
  referencedJudgeLookupKeys: string[];
}

export type SearchResultPassage = {
  chunkId: string;
  snippet: string;
  sectionLabel: string;
  sectionHeading: string;
  citationAnchor: string;
  paragraphAnchor: string;
  chunkType: string;
  score: number;
};

export type SupportingFactDebug = {
  source: "matched_pool" | "fallback_findings_background_pool";
  factualAnchorScore: number;
  anchorHits: number;
  secondaryHits: number;
  coverageRatio: number;
};

export type IndexCodeFilterContext = {
  requestedCodes: string[];
  normalizedCodes: string[];
  legacyCodeAliases: string[];
  relatedRulesSections: string[];
  relatedOrdinanceSections: string[];
  searchPhrases: string[];
};

export type IndexCodeFilterContextOptions = {
  includeGenericDhsFamilyAlias?: boolean;
};

export type DocumentReferenceSectionFacet = "rules_section" | "ordinance_section";

export type CuratedKeywordFamily = {
  triggers: string[];
  expansions: string[];
};

export type QueryIntent =
  | "authority"
  | "findings"
  | "procedural"
  | "analysis"
  | "disposition"
  | "citation"
  | "comparative"
  | "unknown";

export type SearchScopeOptions = {
  useSoftIndexCodeScope?: boolean;
};
