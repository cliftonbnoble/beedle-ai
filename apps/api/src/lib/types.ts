// Runtime bindings come from the hermetic Wrangler generator. Only request-owned
// state, deployment-injected values and the Queue message contract are layered here.
export interface Env extends Omit<WorkerBindings, "APP_ENV" | "VECTOR_JOBS_QUEUE"> {
  APP_ENV: "local" | "preview" | "production";
  VECTOR_JOBS_QUEUE: Queue<import("../services/vector-jobs").VectorJobMessage>;
  TELEMETRY?: import("./telemetry").RequestTelemetry;
  SEARCH_CAPABILITIES?: Readonly<{schemaVersion:number;fts:boolean;vocabulary:boolean}>;
  SEARCH_WORK_BUDGET?: import("../services/search-work-budget").SearchWorkBudget;
  RELEASE_REVISION?: string;
  RELEASE_MAINTENANCE_ONLY?: string;
  RELEASE_CONFIG_SHA256?: string;
  LLM_API_KEY?: string;
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
    unverifiedDecisionDate?:string;
    author: string | null;
    outcomeLabel: "grant" | "deny" | "partial" | "unclear";
    extractionConfidence: number;
  };
  warnings: string[];
}

// SQL maintenance deliberately has no provider, source-bucket or public-route capability.
export type DatabaseEnv = Pick<Env, "DB">;
