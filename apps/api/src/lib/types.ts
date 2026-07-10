export interface Env {
  DB: D1Database;
  SOURCE_BUCKET: R2Bucket;
  VECTOR_INDEX: VectorizeIndex;
  SEARCH_RATE_LIMIT: RateLimit;
  INGEST_RATE_LIMIT: RateLimit;
  LLM_RATE_LIMIT: RateLimit;
  ADMIN_WRITE_RATE_LIMIT: RateLimit;
  AUTH_RATE_LIMIT: RateLimit;
  VECTOR_JOBS_QUEUE: Queue<import("../services/vector-jobs").VectorJobMessage>;
  AI?: Ai;
  AI_EMBEDDING_MODEL: string;
  VECTOR_NAMESPACE: string;
  CORS_ALLOWED_ORIGINS?: string;
  AUTH_USERNAME?: string;
  AUTH_PASSWORD_HASH?: string;
  AUTH_SESSION_SECRET?: string;
  AUTH_SESSION_TTL_SECONDS?: string;
  AUTH_COOKIE_NAME?: string;
  AUTH_COOKIE_SECURE?: string;
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
