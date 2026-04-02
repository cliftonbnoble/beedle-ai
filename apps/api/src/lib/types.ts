export interface Env {
  DB: D1Database;
  SOURCE_BUCKET: R2Bucket;
  VECTOR_INDEX: VectorizeIndex;
  AI?: Ai;
  AI_EMBEDDING_MODEL: string;
  VECTOR_NAMESPACE: string;
  CORS_ALLOWED_ORIGINS?: string;
  R2_PUBLIC_BASE_URL: string;
  SOURCE_PROXY_BASE_URL?: string;
  LLM_API_KEY?: string;
  LLM_BASE_URL?: string;
  LLM_MODEL?: string;
}

export interface AuthoredSection {
  canonicalKey: string;
  heading: string;
  order: number;
  paragraphs: Array<{
    anchor: string;
    order: number;
    text: string;
  }>;
}

export interface ParsedDocument {
  sections: AuthoredSection[];
  qcFlags: {
    hasIndexCodes: boolean;
    hasRulesSection: boolean;
    hasOrdinanceSection: boolean;
  };
  plainText: string;
  extractedMetadata: {
    indexCodes: string[];
    rulesSections: string[];
    ordinanceSections: string[];
    caseNumber: string | null;
    decisionDate: string | null;
    author: string | null;
    outcomeLabel: "grant" | "deny" | "partial" | "unclear";
    extractionConfidence: number;
  };
  warnings: string[];
}
