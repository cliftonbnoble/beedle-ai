export interface Env {
  APP_ENV: "local" | "preview" | "production";
  DEPLOYMENT_READY: string;
  DB: D1Database;
  SOURCE_BUCKET: R2Bucket;
  VECTOR_INDEX?: VectorizeIndex;
  SEARCH_RATE_LIMIT: RateLimit;
  INGEST_RATE_LIMIT: RateLimit;
  LLM_RATE_LIMIT: RateLimit;
  ADMIN_WRITE_RATE_LIMIT: RateLimit;
  REVIEW_READ_RATE_LIMIT: RateLimit;
  EXPORT_RATE_LIMIT: RateLimit;
  COMPUTE_BUDGETS_JSON?: string;
  VECTOR_JOBS_QUEUE: Queue<import("../services/vector-jobs").VectorJobMessage>;
  AI?: Ai;
  AI_EMBEDDING_MODEL: string;
  AI_EMBEDDING_DIMENSIONS?: string;
  VECTOR_LIFECYCLE_READY?: string;
  VECTOR_FILTERS_READY?: string;
  VECTOR_NAMESPACE: string;
  CORS_ALLOWED_ORIGINS?: string;
  R2_PUBLIC_BASE_URL: string;
  SOURCE_PROXY_BASE_URL?: string;
  LLM_API_KEY?: string;
  LLM_BASE_URL?: string;
  LLM_MODEL?: string;
  AI_CHAT_MODEL?: string;
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
  citations: import("../services/legal-citations").ParsedCitation[];
  extraction: import("../services/text-extraction").ExtractionDiagnostics;
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
