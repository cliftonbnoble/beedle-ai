import { resolveReferenceEdition, editionProvisions, type EditionProvision } from "./reference-editions";
import { catalogState, assertCatalogState, stageLegalCatalog, publishLegalCatalog, type CatalogRows } from "./legal-catalog";
import { sha256Bytes } from "../lib/digests";
import { RequestValidationError, RequestConflictError, ServiceUnavailableError } from "../lib/http";
import { citationKey, citationIdentity, parseCitation } from "./legal-citations";
import { legalCitationVerifyRequestSchema, legalReferenceInspectResponseSchema, legalReferenceRebuildRequestSchema, referenceValidationInputSchema, knownCalendarDate } from "@beedle/shared";
import type { Env } from "../lib/types";
import { CRITICAL_EXCEPTION_CITATIONS } from "./qc-shared";

function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID()}`;
}

function normalizeToken(input: string): string {
  return input.toLowerCase().replace(/[\s_]+/g, "").replace(/[^a-z0-9.()\-]/g, "");
}

function normalizeIndexCode(input: string): string {
  return normalizeToken(String(input || "").trim().replace(/^ic(?:[\s_-]+|(?=\d))/i, "")).replace(/^[-]+/, "");
}

function normalizeCitation(input: string): string {
  return citationIdentity(input);
}

function normalizeOrdinanceCitationForLookup(input: string): string {
  return citationIdentity(input, "ordinance");
}

function normalizeBareRulesCitation(input: string): string {
  return citationIdentity(input, "rule");
}

function citationMatch(normalizedQuery: string, normalizedCandidate: string): boolean {
  if (!normalizedQuery || !normalizedCandidate) return false;
  if (normalizedQuery === normalizedCandidate) return true;
  // Directional match: a base section may resolve to a subsection, but a subsection does not resolve to its parent.
  if (normalizedCandidate.startsWith(`${normalizedQuery}(`)) return true;
  return false;
}

function normalizedBaseCitation(value: string): string {
  const raw = normalizeCitation(value);
  const idx = raw.indexOf("(");
  const baseRaw = idx >= 0 ? raw.slice(0, idx) : raw;
  return baseRaw;
}

function parseJsonArray(value: string | null | undefined): string[] {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function unique(values: string[]): string[] {
  return Array.from(new Set(values));
}

const DEFAULT_CRITICAL_CITATIONS = ["37.2(g)", "37.3(a)(1)", "37.15", "1.11", "6.13", "10.10(c)(3)", "13.14"];
const KNOWN_CRITICAL_EXCEPTION_CITATIONS = new Set<string>(CRITICAL_EXCEPTION_CITATIONS);

const REFERENCE_BATCH_SIZE = 50;
export async function executeReferenceStatementBatches(env: Env, statements: D1PreparedStatement[]) {
  for (let i = 0; i < statements.length; i += REFERENCE_BATCH_SIZE) await env.DB.batch(statements.slice(i, i + REFERENCE_BATCH_SIZE));
}

function compactWhitespace(input: string | null | undefined): string {
  return String(input ?? "")
    .replace(/\r/g, "")
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+/g, " ")
    .trim();
}

function canonicalHeading(input: string | null | undefined): string | null {
  const value = compactWhitespace(input);
  return value.length ? value : null;
}

function rowScore(row: { heading?: string | null; body_text: string; source_page_anchor?: string | null; subsection_path?: string | null }): number {
  const headingLen = Math.min(compactWhitespace(row.heading).length, 240);
  const bodyLen = Math.min(compactWhitespace(row.body_text).length, 4000);
  const anchorBonus = row.source_page_anchor ? 25 : 0;
  const subsectionBonus = row.subsection_path ? 20 : 0;
  return bodyLen + headingLen + anchorBonus + subsectionBonus;
}

function mergeBodies(primary: string, secondary: string): { body: string; merged: boolean } {
  const a = compactWhitespace(primary);
  const b = compactWhitespace(secondary);
  if (!b.length) return { body: a, merged: false };
  if (!a.length) return { body: b, merged: false };
  if (a.includes(b)) return { body: a, merged: false };
  if (b.includes(a)) return { body: b, merged: false };
  const merged = `${a}\n\n${b}`;
  return { body: merged, merged: true };
}

function chooseCanonical<T extends { heading?: string | null; body_text: string; source_page_anchor?: string | null; subsection_path?: string | null; citation: string }>(
  left: T,
  right: T
): { canonical: T; alternate: T } {
  const leftScore = rowScore(left);
  const rightScore = rowScore(right);
  if (leftScore > rightScore) return { canonical: left, alternate: right };
  if (rightScore > leftScore) return { canonical: right, alternate: left };
  if (compactWhitespace(left.citation).localeCompare(compactWhitespace(right.citation)) <= 0) {
    return { canonical: left, alternate: right };
  }
  return { canonical: right, alternate: left };
}

function dedupeIndexCodes(
  rows: Array<{
    code_identifier: string;
    family?: string | null;
    label?: string | null;
    description?: string | null;
    reserved: boolean;
    legacy_pre_1002: boolean;
    linked_ordinance_sections: string[];
    linked_rules_sections: string[];
    source_page_anchor?: string | null;
  }>
) {
  const byNormalized = new Map<
    string,
    {
      code_identifier: string;
      family?: string | null;
      label?: string | null;
      description?: string | null;
      reserved: boolean;
      legacy_pre_1002: boolean;
      linked_ordinance_sections: string[];
      linked_rules_sections: string[];
      source_page_anchor?: string | null;
    }
  >();
  let duplicatesDropped = 0;
  for (const row of rows) {
    const codeIdentifier = compactWhitespace(row.code_identifier);
    if (!codeIdentifier) continue;
    const key = normalizeIndexCode(codeIdentifier);
    const normalizedRow = {
      ...row,
      code_identifier: codeIdentifier,
      family: canonicalHeading(row.family),
      label: canonicalHeading(row.label),
      description: canonicalHeading(row.description),
      source_page_anchor: canonicalHeading(row.source_page_anchor),
      linked_ordinance_sections: unique((row.linked_ordinance_sections ?? []).map((item) => compactWhitespace(item)).filter(Boolean)),
      linked_rules_sections: unique((row.linked_rules_sections ?? []).map((item) => compactWhitespace(item)).filter(Boolean))
    };
    const existing = byNormalized.get(key);
    if (!existing) {
      byNormalized.set(key, normalizedRow);
      continue;
    }
    duplicatesDropped += 1;
    const existingScore =
      compactWhitespace(existing.label).length + compactWhitespace(existing.description).length + existing.linked_ordinance_sections.length + existing.linked_rules_sections.length;
    const currentScore =
      compactWhitespace(normalizedRow.label).length +
      compactWhitespace(normalizedRow.description).length +
      normalizedRow.linked_ordinance_sections.length +
      normalizedRow.linked_rules_sections.length;
    if (currentScore > existingScore) {
      byNormalized.set(key, normalizedRow);
    } else {
      byNormalized.set(key, {
        ...existing,
        linked_ordinance_sections: unique([...existing.linked_ordinance_sections, ...normalizedRow.linked_ordinance_sections]),
        linked_rules_sections: unique([...existing.linked_rules_sections, ...normalizedRow.linked_rules_sections])
      });
    }
  }
  return { rows: Array.from(byNormalized.values()), duplicatesDropped };
}

function dedupeOrdinanceSections(
  rows: Array<{ section_number: string; subsection_path?: string | null; heading?: string | null; body_text: string; page_anchor?: string | null }>
) {
  type CanonicalRow = {
    section_number: string;
    subsection_path: string | null;
    citation: string;
    normalized_citation: string;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
  };
  const byNormalized = new Map<string, CanonicalRow>();
  const byCitation = new Map<string, CanonicalRow>();
  let duplicateNormalizedCitationsEncountered = 0;
  let duplicatesMerged = 0;
  let duplicatesDropped = 0;

  for (const row of rows) {
    const section = compactWhitespace(row.section_number);
    if (!section) continue;
    const subsection = canonicalHeading(row.subsection_path);
    const citation = subsection ? `${section}${subsection}` : section;
    const normalized = normalizeCitation(citation);
    if (!normalized) continue;

    const candidate: CanonicalRow = {
      section_number: section,
      subsection_path: subsection,
      citation,
      normalized_citation: normalized,
      heading: canonicalHeading(row.heading),
      body_text: compactWhitespace(row.body_text),
      source_page_anchor: canonicalHeading(row.page_anchor)
    };
    if (!candidate.body_text) continue;

    const existingByNormalized = byNormalized.get(normalized);
    if (!existingByNormalized) {
      byNormalized.set(normalized, candidate);
      continue;
    }
    duplicateNormalizedCitationsEncountered += 1;
    duplicatesDropped += 1;
    const { canonical, alternate } = chooseCanonical(existingByNormalized, candidate);
    const merged = mergeBodies(canonical.body_text, alternate.body_text);
    if (merged.merged) duplicatesMerged += 1;
    byNormalized.set(normalized, { ...canonical, body_text: merged.body });
  }

  for (const row of byNormalized.values()) {
    const citationKey = compactWhitespace(row.citation).toLowerCase();
    const existing = byCitation.get(citationKey);
    if (!existing) {
      byCitation.set(citationKey, row);
      continue;
    }
    duplicatesDropped += 1;
    const { canonical, alternate } = chooseCanonical(existing, row);
    const merged = mergeBodies(canonical.body_text, alternate.body_text);
    if (merged.merged) duplicatesMerged += 1;
    byCitation.set(citationKey, { ...canonical, body_text: merged.body });
  }

  return {
    rows: Array.from(byCitation.values()),
    diagnostics: {
      duplicate_normalized_citations_encountered: duplicateNormalizedCitationsEncountered,
      duplicates_merged: duplicatesMerged,
      duplicates_dropped: duplicatesDropped
    }
  };
}

function dedupeRulesSections(
  rows: Array<{ part?: string | null; section_number: string; heading?: string | null; body_text: string; page_anchor?: string | null }>
) {
  type CanonicalRow = {
    part: string | null;
    section_number: string;
    citation: string;
    normalized_citation: string;
    canonical_bare_citation: string;
    normalized_bare_citation: string;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
  };
  const byNormalized = new Map<string, CanonicalRow>();
  const byCitation = new Map<string, CanonicalRow>();
  let duplicateNormalizedCitationsEncountered = 0;
  let duplicatesMerged = 0;
  let duplicatesDropped = 0;

  for (const row of rows) {
    const sectionNumber = compactWhitespace(row.section_number);
    if (!sectionNumber) continue;
    const part = canonicalHeading(row.part);
    const citation = part ? `Part ${part.replace(/^part\s+/i, "")}-${sectionNumber}` : sectionNumber;
    const normalized = normalizeCitation(citation);
    if (!normalized) continue;

    const candidate: CanonicalRow = {
      part,
      section_number: sectionNumber,
      citation,
      normalized_citation: normalized,
      canonical_bare_citation: sectionNumber,
      normalized_bare_citation: normalizeBareRulesCitation(sectionNumber),
      heading: canonicalHeading(row.heading),
      body_text: compactWhitespace(row.body_text),
      source_page_anchor: canonicalHeading(row.page_anchor)
    };
    if (!candidate.body_text) continue;

    const existingByNormalized = byNormalized.get(normalized);
    if (!existingByNormalized) {
      byNormalized.set(normalized, candidate);
      continue;
    }
    duplicateNormalizedCitationsEncountered += 1;
    duplicatesDropped += 1;
    const { canonical, alternate } = chooseCanonical(existingByNormalized, candidate);
    const merged = mergeBodies(canonical.body_text, alternate.body_text);
    if (merged.merged) duplicatesMerged += 1;
    byNormalized.set(normalized, { ...canonical, body_text: merged.body });
  }

  for (const row of byNormalized.values()) {
    const citationKey = compactWhitespace(row.citation).toLowerCase();
    const existing = byCitation.get(citationKey);
    if (!existing) {
      byCitation.set(citationKey, row);
      continue;
    }
    duplicatesDropped += 1;
    const { canonical, alternate } = chooseCanonical(existing, row);
    const merged = mergeBodies(canonical.body_text, alternate.body_text);
    if (merged.merged) duplicatesMerged += 1;
    byCitation.set(citationKey, { ...canonical, body_text: merged.body });
  }

  return {
    rows: Array.from(byCitation.values()),
    diagnostics: {
      duplicate_normalized_citations_encountered: duplicateNormalizedCitationsEncountered,
      duplicates_merged: duplicatesMerged,
      duplicates_dropped: duplicatesDropped
    }
  };
}

export async function rebuildLegalReferences(env: Env, payload: unknown) {
  const parsed = legalReferenceRebuildRequestSchema.parse(payload);
  const now = new Date().toISOString();
  const base = await catalogState(env);
  const dedupedIndex = dedupeIndexCodes(parsed.index_codes);
  const dedupedOrdinance = dedupeOrdinanceSections(parsed.ordinance_sections);
  const dedupedRules = dedupeRulesSections(parsed.rules_sections);
  const normalizedOrdinanceSet = new Set(dedupedOrdinance.rows.map((row) => row.normalized_citation));
  const normalizedRulesSet = new Set(dedupedRules.rows.map((row) => row.normalized_citation));
  const normalizedRulesBareSet = new Set(dedupedRules.rows.map((row) => row.normalized_bare_citation));
  const normalizedIndexSet = new Set(dedupedIndex.rows.map((row) => normalizeIndexCode(row.code_identifier)));
  const crosswalk = parsed.crosswalk
    .map((row) => ({
      ...row,
      index_code: row.index_code ? compactWhitespace(row.index_code) : undefined,
      ordinance_section: row.ordinance_section ? compactWhitespace(row.ordinance_section) : undefined,
      rules_section: row.rules_section ? compactWhitespace(row.rules_section) : undefined,
      source: compactWhitespace(row.source || "normalized_import") || "normalized_import"
    }))
    .filter((row) => row.index_code || row.ordinance_section || row.rules_section)
    .map((row) => {
      const hasKnownIndex = row.index_code ? normalizedIndexSet.has(normalizeIndexCode(row.index_code)) : true;
      const hasResolvedOrdinance = row.ordinance_section
        ? Array.from(normalizedOrdinanceSet).some((candidate) => citationMatch(normalizeCitation(row.ordinance_section || ""), candidate))
        : true;
      const hasResolvedRules = row.rules_section
        ? Array.from(normalizedRulesSet).some((candidate) => citationMatch(normalizeCitation(row.rules_section || ""), candidate)) ||
          Array.from(normalizedRulesBareSet).some((candidate) => citationMatch(normalizeBareRulesCitation(row.rules_section || ""), candidate))
        : true;
      if (hasKnownIndex && hasResolvedOrdinance && hasResolvedRules) return row;
      return { ...row, source: `${row.source}_unresolved` };
    });
  const coverageReport = {
    ...(parsed.coverage_report ?? {}),
    ordinance: parsed.coverage_report?.ordinance
      ? {
          ...parsed.coverage_report.ordinance,
          duplicate_normalized_citations_encountered: dedupedOrdinance.diagnostics.duplicate_normalized_citations_encountered,
          duplicates_merged: dedupedOrdinance.diagnostics.duplicates_merged,
          duplicates_dropped: dedupedOrdinance.diagnostics.duplicates_dropped,
          committed_section_count: dedupedOrdinance.rows.length
        }
      : undefined,
    rules: parsed.coverage_report?.rules
      ? {
          ...parsed.coverage_report.rules,
          duplicate_normalized_citations_encountered: dedupedRules.diagnostics.duplicate_normalized_citations_encountered,
          duplicates_merged: dedupedRules.diagnostics.duplicates_merged,
          duplicates_dropped: dedupedRules.diagnostics.duplicates_dropped,
          committed_section_count: dedupedRules.rows.length
        }
      : undefined
  };
  if (!dedupedIndex.rows.length || !dedupedOrdinance.rows.length || !dedupedRules.rows.length ||
    Object.values(parsed.source_trace).some(value => !value.trim())) {
    throw new RequestValidationError("A replacement catalog requires nonempty index, ordinance and rules sets with source descriptions.");
  }
  for (const [kind, count] of [["ordinance", dedupedOrdinance.rows.length], ["rules", dedupedRules.rows.length]] as const) {
    const declared = parsed.coverage_report?.[kind];
    if (declared && declared.expected_section_count > count) throw new RequestValidationError(`${kind} coverage is below the declared expected count; review the import before publication.`);
  }
  const rows: CatalogRows = {
    legal_reference_sources: [
      ...Object.entries(parsed.source_trace).map(([source_key,source_path]) => ({source_key,source_path,updated_at:now})),
      {source_key:"coverage_report_json",source_path:JSON.stringify(coverageReport),updated_at:now}
    ],
    legal_index_codes: dedupedIndex.rows.map(row => ({id:id("idx"),code_identifier:row.code_identifier.trim(),normalized_code:normalizeIndexCode(row.code_identifier),
      family:row.family??null,label:row.label??null,description:row.description??null,is_reserved:row.reserved?1:0,is_legacy_pre_1002:row.legacy_pre_1002?1:0,
      linked_ordinance_sections_json:JSON.stringify(unique(row.linked_ordinance_sections.map(item=>item.trim()).filter(Boolean))),
      linked_rules_sections_json:JSON.stringify(unique(row.linked_rules_sections.map(item=>item.trim()).filter(Boolean))),
      source_page_anchor:row.source_page_anchor??null,active:1,created_at:now,updated_at:now})),
    legal_ordinance_sections: dedupedOrdinance.rows.map(row=>({id:id("ord"),...row,active:1,created_at:now,updated_at:now})),
    legal_rules_sections: dedupedRules.rows.map(row=>({id:id("rul"),...row,active:1,created_at:now,updated_at:now})),
    legal_reference_crosswalk: crosswalk.map(row=>({id:id("xw"),index_code_id:row.index_code??null,ordinance_citation:row.ordinance_section??null,rules_citation:row.rules_section??null,source:row.source,created_at:now}))
  };
  const payloadDigest = await sha256Bytes(new TextEncoder().encode(JSON.stringify(parsed)));
  const existing=await env.DB.prepare("SELECT payload_sha256 FROM legal_catalog_versions WHERE id=?").bind(base.version).first<{payload_sha256:string|null}>();
  await assertCatalogState(env,base);
  const publication=existing?.payload_sha256===payloadDigest ? {...base,replayed:true} :
    await publishLegalCatalog(env,await stageLegalCatalog(env,rows,payloadDigest,base,parsed.edition),base);

  return {
    ok: true,
    publication,
    source_trace: parsed.source_trace,
    counts: {
      index_codes: dedupedIndex.rows.length,
      ordinance_sections: dedupedOrdinance.rows.length,
      rules_sections: dedupedRules.rows.length,
      crosswalk: crosswalk.length
    },
    collision_diagnostics: {
      index_codes_duplicates_dropped: dedupedIndex.duplicatesDropped,
      ordinance: dedupedOrdinance.diagnostics,
      rules: dedupedRules.diagnostics
    }
  };
}

export async function validateReferencesAgainstNormalized(env: Env, values: {indexCodes:string[];rulesSections:string[];ordinanceSections:string[]}, context?:{jurisdiction:string;as_of_date:string|null}, prepared?:ReferenceValidationPlan) {
  values=referenceValidationInputSchema.parse(values);
  const plan=prepared??await prepareReferenceValidation(env,values,context);
  assertReferencePlan(plan,values,context);
  const {refs,editionValidation}=plan;
  return {
    unknownIndexCodes: unique(values.indexCodes).filter(value=>!refs.indexByNormalized.has(normalizeIndexCode(value))),
    unknownRules: unique(values.rulesSections).filter(value=>editionValidation.edition.status==='matched'?!editionValidation.rules.has(citationIdentity(value,'rule')):!resolvedRule(refs,value)),
    unknownOrdinance: unique(values.ordinanceSections).filter(value=>editionValidation.edition.status==='matched'?!editionValidation.ordinances.has(citationIdentity(value,'ordinance')):!refs.ordinanceByNormalized.has(normalizeOrdinanceCitationForLookup(value))),
    catalogVersion: refs.catalogVersion,
    ...(context?{editionId:editionValidation.edition.edition_id,applicabilityStatus:editionValidation.edition.status}:{})
  };
}

async function inferIndexCodesFromReferencesSnapshot(
  env: Env,
  input: { rulesSections: string[]; ordinanceSections: string[] }
) {
  const normalizedRules = unique(input.rulesSections.map((item) => item.trim()).filter(Boolean).map((item) => normalizeCitation(item)));
  const normalizedRulesBare = unique(
    input.rulesSections.map((item) => item.trim()).filter(Boolean).map((item) => normalizeBareRulesCitation(item))
  );
  const normalizedOrdinance = unique(
    input.ordinanceSections.map((item) => item.trim()).filter(Boolean).map((item) => normalizeOrdinanceCitationForLookup(item))
  );
  if (normalizedRules.length === 0 && normalizedOrdinance.length === 0) {
    return { inferredIndexCodes: [], evidence: [] as string[] };
  }

  const indexRows = await env.DB.prepare(
    `SELECT code_identifier as codeIdentifier, normalized_code as normalizedCode,
            linked_ordinance_sections_json as linkedOrdinanceSectionsJson,
            linked_rules_sections_json as linkedRulesSectionsJson,
            is_reserved as isReserved
     FROM legal_index_codes
     WHERE active = 1`
  ).all<{
    codeIdentifier: string;
    normalizedCode: string;
    linkedOrdinanceSectionsJson: string;
    linkedRulesSectionsJson: string;
    isReserved: number;
  }>();
  const byNormalized = new Map(
    (indexRows.results ?? [])
      .filter((row) => !row.isReserved)
      .map((row) => [row.normalizedCode, row.codeIdentifier] as const)
  );
  const inferred = new Set<string>();
  const evidence = new Set<string>();

  for (const row of indexRows.results ?? []) {
    if (row.isReserved) continue;
    const linkedOrd = parseJsonArray(row.linkedOrdinanceSectionsJson).map((item) => normalizeCitation(item));
    const linkedRules = parseJsonArray(row.linkedRulesSectionsJson).map((item) => normalizeCitation(item));
    const linkedRulesBare = parseJsonArray(row.linkedRulesSectionsJson).map((item) => normalizeBareRulesCitation(item));
    const ordMatch =
      normalizedOrdinance.length > 0 &&
      linkedOrd.some((candidate) => normalizedOrdinance.some((query) => citationMatch(query, candidate)));
    const rulesMatch =
      normalizedRules.length > 0 &&
      (linkedRules.some((candidate) => normalizedRules.some((query) => citationMatch(query, candidate))) ||
        linkedRulesBare.some((candidate) => normalizedRulesBare.some((query) => citationMatch(query, candidate))));
    if (ordMatch || rulesMatch) {
      inferred.add(row.codeIdentifier);
      evidence.add(`linked_sections:${row.codeIdentifier}`);
    }
  }

  const crosswalkRows = await env.DB.prepare(
    `SELECT index_code_id as indexCodeId, ordinance_citation as ordinanceCitation, rules_citation as rulesCitation
     FROM legal_reference_crosswalk`
  ).all<{ indexCodeId: string | null; ordinanceCitation: string | null; rulesCitation: string | null }>();
  for (const row of crosswalkRows.results ?? []) {
    if (!row.indexCodeId) continue;
    const normalizedCode = normalizeIndexCode(row.indexCodeId);
    const canonical = byNormalized.get(normalizedCode);
    if (!canonical) continue;
    const ordMatch =
      row.ordinanceCitation &&
      normalizedOrdinance.some((query) => citationMatch(query, normalizeOrdinanceCitationForLookup(row.ordinanceCitation || "")));
    const rulesMatch =
      row.rulesCitation &&
      (normalizedRules.some((query) => citationMatch(query, normalizeCitation(row.rulesCitation || ""))) ||
        normalizedRulesBare.some((query) => citationMatch(query, normalizeBareRulesCitation(row.rulesCitation || ""))));
    if (ordMatch || rulesMatch) {
      inferred.add(canonical);
      evidence.add(`crosswalk:${canonical}`);
    }
  }

  return {
    inferredIndexCodes: Array.from(inferred).sort(),
    evidence: Array.from(evidence).sort()
  };
}

export interface ReferenceLookups {
  catalogEpoch:number;
  catalogVersion: string;
  indexByNormalized: Map<string, { canonicalValue: string; isReserved: number }>;
  rulesByNormalized: Map<string, string>;
  rulesByBare: Map<string, string>;
  ordinanceByNormalized: Map<string, string>;
}

function ruleCatalogLabel(row: {canonicalValue:string;part?:string|null;sectionNumber?:string|null}) {
  return row.part && row.sectionNumber ? `Part ${row.part.replace(/^part\s+/i, "")}-${row.sectionNumber}` : row.canonicalValue;
}

function resolvedRule(refs: ReferenceLookups, value: string) {
  const key = normalizeBareRulesCitation(value);
  // A bare section is not an exact choice when several Parts contain it.
  return parseCitation(value,"rule").part ? refs.rulesByNormalized.get(key) : refs.rulesByBare.get(key);
}

// Loads the three normalized reference sets once (the same load-whole pattern verifyCitations uses in
// production — these are bounded lookup tables, not corpus tables). Map insertion keeps the first row
// per key, mirroring the previous per-value `LIMIT 1` lookups.
export async function loadReferenceLookups(env: Env): Promise<ReferenceLookups> {
  const state = await catalogState(env);
  const capacity=await env.DB.prepare(`SELECT COUNT(*) AS rows,COALESCE(SUM(
    COALESCE(length(CAST(json_extract(payload_json,'$.normalized_code') AS BLOB)),0)+COALESCE(length(CAST(json_extract(payload_json,'$.normalized_citation') AS BLOB)),0)+
    COALESCE(length(CAST(json_extract(payload_json,'$.code_identifier') AS BLOB)),0)+COALESCE(length(CAST(json_extract(payload_json,'$.citation') AS BLOB)),0)+
    COALESCE(length(CAST(json_extract(payload_json,'$.part') AS BLOB)),0)+COALESCE(length(CAST(json_extract(payload_json,'$.section_number') AS BLOB)),0)),0) AS bytes
    FROM legal_catalog_rows WHERE catalog_id=? AND table_name IN ('legal_index_codes','legal_rules_sections','legal_ordinance_sections') AND json_extract(payload_json,'$.active')=1`)
    .bind(state.version).first<{rows:number;bytes:number}>();
  if(!capacity||capacity.rows>30000||capacity.bytes>4*1024*1024)throw new ServiceUnavailableError('Reference catalog exceeds bounded lookup capacity');
  const rows=(await env.DB.prepare(`SELECT table_name AS kind,
    COALESCE(json_extract(payload_json,'$.normalized_code'),json_extract(payload_json,'$.normalized_citation')) AS normalized,
    COALESCE(json_extract(payload_json,'$.code_identifier'),json_extract(payload_json,'$.citation')) AS canonicalValue,
    json_extract(payload_json,'$.normalized_bare_citation') AS bare,json_extract(payload_json,'$.part') AS part,
    json_extract(payload_json,'$.section_number') AS sectionNumber,COALESCE(json_extract(payload_json,'$.is_reserved'),0) AS isReserved
    FROM legal_catalog_rows WHERE catalog_id=? AND table_name IN ('legal_index_codes','legal_rules_sections','legal_ordinance_sections')
      AND json_extract(payload_json,'$.active')=1 ORDER BY row_id LIMIT 30001`).bind(state.version)
    .all<{kind:string;normalized:string;canonicalValue:string;bare:string|null;part:string|null;sectionNumber:string|null;isReserved:number}>()).results||[];
  if(rows.length>30000)throw new ServiceUnavailableError('Reference catalog exceeds bounded lookup capacity');
  const indexRows={results:rows.filter(row=>row.kind==='legal_index_codes')};
  const rulesRows={results:rows.filter(row=>row.kind==='legal_rules_sections')};
  const ordinanceRows={results:rows.filter(row=>row.kind==='legal_ordinance_sections')};
  const indexByNormalized = new Map<string, { canonicalValue: string; isReserved: number }>();
  for (const row of indexRows.results ?? []) {
    if (!indexByNormalized.has(row.normalized)) {
      indexByNormalized.set(row.normalized, { canonicalValue: row.canonicalValue, isReserved: row.isReserved });
    }
  }
  const rulesByNormalized = new Map<string, string>();
  const rulesByBare = new Map<string, string>();
  const normalizedSeen = new Set<string>();
  const bareSeen = new Set<string>();
  const uniqueRule = (map:Map<string,string>,seen:Set<string>,key:string,value:string) => {
    if (!key) return;
    if (seen.has(key)) map.delete(key);
    else { seen.add(key); map.set(key,value); }
  };
  for (const row of rulesRows.results ?? []) {
    // Recompute from retained citation identity, including catalogs captured
    // before the Part-preserving normalizer. Never pick a first ambiguous row.
    const label=ruleCatalogLabel(row);
    uniqueRule(rulesByNormalized,normalizedSeen,citationIdentity(label,"rule"),row.canonicalValue);
    uniqueRule(rulesByBare,bareSeen,citationKey(label,"rule"),row.canonicalValue);
  }
  const ordinanceByNormalized = new Map<string, string>();
  const ordinanceSeen = new Set<string>();
  for (const row of ordinanceRows.results ?? []) {
    uniqueRule(ordinanceByNormalized,ordinanceSeen,citationIdentity(row.canonicalValue,"ordinance"),row.canonicalValue);
  }

  return { catalogVersion:state.version, catalogEpoch:state.epoch, indexByNormalized, rulesByNormalized, rulesByBare, ordinanceByNormalized };
}

async function loadReferenceEditionValidation(env:Env,refs:ReferenceLookups,input:{rulesSections:string[];ordinanceSections:string[]},context?:{jurisdiction:string;as_of_date:string|null}|null){
  context=referenceValidationContext(context);
  const edition=await resolveReferenceEdition(env,context?.jurisdiction&&context.as_of_date?{jurisdiction:context.jurisdiction,as_of_date:context.as_of_date}:undefined,{registryEpoch:refs.catalogEpoch});
  edition.date_basis=context?.as_of_date?"decision_date_proxy":"unknown";
  if(!edition.jurisdiction)edition.jurisdiction=context?.jurisdiction||null;
  if(!edition.as_of_date)edition.as_of_date=context?.as_of_date||null;
  const rules=edition.status==='matched'&&edition.edition_id?await editionProvisions(env,edition.edition_id,'rule',input.rulesSections.filter(value=>citationIdentity(value,'rule'))):new Map<string,EditionProvision>();
  const ordinances=edition.status==='matched'&&edition.edition_id?await editionProvisions(env,edition.edition_id,'ordinance',input.ordinanceSections.filter(value=>citationIdentity(value,'ordinance'))):new Map<string,EditionProvision>();
  return {edition,rules,ordinances};
}

type ReferenceValues={indexCodes:string[];rulesSections:string[];ordinanceSections:string[]};
type ReferenceContext={jurisdiction:string;as_of_date:string|null};
function referenceValidationContext(context?:ReferenceContext|null):ReferenceContext|null {
 if(!context)return null;
 return {jurisdiction:context.jurisdiction.length<=200?context.jurisdiction:'',as_of_date:knownCalendarDate(context.as_of_date)};
}
export interface ReferenceValidationPlan {
 refs:ReferenceLookups;context:ReferenceContext|null;
 editionValidation:Awaited<ReturnType<typeof loadReferenceEditionValidation>>;
 ruleKeys:Set<string>;ordinanceKeys:Set<string>;
}
export async function prepareReferenceValidation(env:Env,values:ReferenceValues,context?:ReferenceContext|null,lookups?:ReferenceLookups):Promise<ReferenceValidationPlan>{
 const input=referenceValidationInputSchema.parse(values);const refs=lookups??await loadReferenceLookups(env);const captured=referenceValidationContext(context);
 const editionValidation=await loadReferenceEditionValidation(env,refs,input,captured);
 return {refs,context:captured,editionValidation,ruleKeys:new Set(input.rulesSections.map(value=>citationIdentity(value,'rule'))),ordinanceKeys:new Set(input.ordinanceSections.map(value=>citationIdentity(value,'ordinance')))};
}
function assertReferencePlan(plan:ReferenceValidationPlan,values:ReferenceValues,context?:ReferenceContext|null){
 if(JSON.stringify(plan.context)!==JSON.stringify(referenceValidationContext(context))||values.rulesSections.some(value=>!plan.ruleKeys.has(citationIdentity(value,'rule')))||values.ordinanceSections.some(value=>!plan.ordinanceKeys.has(citationIdentity(value,'ordinance'))))throw new RequestConflictError('Reference inputs changed; prepare a fresh revision-scoped validation plan');
}

export async function buildDocumentReferenceValidationStatements(
  env: Env,
  documentId: string,
  input: { indexCodes: string[]; rulesSections: string[]; ordinanceSections: string[] },
  lookups?: ReferenceLookups,
  generationId?: string,
  editionContext?:{jurisdiction:string;as_of_date:string|null},
  prepared?:ReferenceValidationPlan
) {
  input=referenceValidationInputSchema.parse(input);
  const now = new Date().toISOString();
  const generation = generationId ? [generationId] : [];
  const linksTable = generationId ? "staged_document_reference_links" : "document_reference_links";
  const issuesTable = generationId ? "staged_document_reference_issues" : "document_reference_issues";
  const resetStatements: D1PreparedStatement[] = [
    env.DB.prepare(`DELETE FROM ${linksTable} WHERE document_id = ? ${generationId ? "AND generation_id = ?" : ""}`).bind(documentId, ...generation),
    env.DB.prepare(`DELETE FROM ${issuesTable} WHERE document_id = ? ${generationId ? "AND generation_id = ?" : ""}`).bind(documentId, ...generation)
  ];
  const validationStatements: D1PreparedStatement[] = [];

  const context=editionContext || await env.DB.prepare('SELECT jurisdiction,decision_date AS as_of_date FROM documents WHERE id=?').bind(documentId).first<ReferenceContext>();
  const plan=prepared??await prepareReferenceValidation(env,input,context,lookups);
  assertReferencePlan(plan,input,context);
  const refs=plan.refs;
  const {edition,rules,ordinances}=plan.editionValidation;
  const editionValues=(kind:'index_code'|'rule'|'ordinance',value:string)=>{
    const provision=kind==='rule'?rules.get(citationIdentity(value,'rule')):kind==='ordinance'?ordinances.get(citationIdentity(value,'ordinance')):null;
    const status=kind==='index_code'?'not_applicable':edition.status==='matched'&&!provision?'unavailable':edition.status;
    return [edition.edition_id,provision?.id||null,provision?.bodySha256||null,status];
  };

  for (const value of unique(input.indexCodes.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeIndexCode(value);
    const row = refs.indexByNormalized.get(normalized) ?? null;

    validationStatements.push(
      env.DB.prepare(
        `INSERT INTO ${linksTable} (${generationId ? "generation_id," : ""}
          id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at, catalog_version,edition_id,edition_provision_id,edition_body_sha256,applicability_status
        ) VALUES (${generationId ? "?," : ""} ?, ?, 'index_code', ?, ?, ?, ?, ?, ?,?,?,?,?)`
      ).bind(...generation, id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now, refs.catalogVersion,...editionValues('index_code',value))
    );

    if (!row || row.isReserved) {
      const message = !row
        ? "Index code not found in normalized reference set"
        : "Index code exists but is marked reserved; verify before approval";
      validationStatements.push(
        env.DB.prepare(
          `INSERT INTO ${issuesTable} (${generationId ? "generation_id," : ""}
            id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at, catalog_version
          ) VALUES (${generationId ? "?," : ""} ?, ?, 'index_code', ?, ?, ?, ?, ?, ?)`
        ).bind(...generation, id("dri"), documentId, value, normalized, message, row ? "warning" : "error", now, refs.catalogVersion)
      );
    }
  }

  for (const value of unique(input.rulesSections.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeBareRulesCitation(value);
    // Mirrors the previous `normalized_citation = ? OR normalized_bare_citation = ?` lookup, with a
    // deterministic preference for the exact normalized match.
    const canonicalValue = edition.status==='matched'?(rules.get(citationIdentity(value,'rule'))?.citation??null):(resolvedRule(refs,value)??null);
    const row = canonicalValue === null ? null : { canonicalValue };

    validationStatements.push(
      env.DB.prepare(
        `INSERT INTO ${linksTable} (${generationId ? "generation_id," : ""}
          id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at, catalog_version,edition_id,edition_provision_id,edition_body_sha256,applicability_status
        ) VALUES (${generationId ? "?," : ""} ?, ?, 'rules_section', ?, ?, ?, ?, ?, ?,?,?,?,?)`
      ).bind(...generation, id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now, edition.status==='matched'?edition.edition_id:refs.catalogVersion,...editionValues('rule',value))
    );

    if (!row) {
      validationStatements.push(
        env.DB.prepare(
          `INSERT INTO ${issuesTable} (${generationId ? "generation_id," : ""}
            id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at, catalog_version
          ) VALUES (${generationId ? "?," : ""} ?, ?, 'rules_section', ?, ?, ?, 'error', ?, ?)`
        ).bind(...generation, id("dri"), documentId, value, normalized, edition.status==='matched'?"Rules reference not found in selected reference edition":"Rules reference not found in normalized reference set", now, edition.status==='matched'?edition.edition_id:refs.catalogVersion)
      );
    }
  }

  for (const value of unique(input.ordinanceSections.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeOrdinanceCitationForLookup(value);
    const ordinanceCanonical = edition.status==='matched'?(ordinances.get(citationIdentity(value,'ordinance'))?.citation??null):(refs.ordinanceByNormalized.get(normalized)??null);
    const row = ordinanceCanonical === null ? null : { canonicalValue: ordinanceCanonical };

    validationStatements.push(
      env.DB.prepare(
        `INSERT INTO ${linksTable} (${generationId ? "generation_id," : ""}
          id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at, catalog_version,edition_id,edition_provision_id,edition_body_sha256,applicability_status
        ) VALUES (${generationId ? "?," : ""} ?, ?, 'ordinance_section', ?, ?, ?, ?, ?, ?,?,?,?,?)`
      ).bind(...generation, id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now, edition.status==='matched'?edition.edition_id:refs.catalogVersion,...editionValues('ordinance',value))
    );

    if (!row) {
      validationStatements.push(
        env.DB.prepare(
          `INSERT INTO ${issuesTable} (${generationId ? "generation_id," : ""}
            id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at, catalog_version
          ) VALUES (${generationId ? "?," : ""} ?, ?, 'ordinance_section', ?, ?, ?, 'error', ?, ?)`
        ).bind(...generation, id("dri"), documentId, value, normalized, edition.status==='matched'?"Ordinance reference not found in selected reference edition":"Ordinance reference not found in normalized reference set", now, edition.status==='matched'?edition.edition_id:refs.catalogVersion)
      );
    }
  }
  if (generationId) {
    return [...resetStatements,...validationStatements,env.DB.prepare("UPDATE document_generations SET snapshot_json=json_set(snapshot_json,'$.reference_catalog_version',?,'$.reference_edition_context_json',?) WHERE id=? AND document_id=?")
      .bind(refs.catalogVersion,JSON.stringify(edition),generationId,documentId)];
  }
  const guard=crypto.randomUUID();
  return [env.DB.prepare("INSERT INTO reference_validation_guards(token,document_id,expected_catalog,expected_epoch) VALUES (?,?,?,?)").bind(guard,documentId,refs.catalogVersion,refs.catalogEpoch),
    env.DB.prepare(`INSERT INTO document_reference_validation_history(id,document_id,document_revision,catalog_version,links_json,issues_json,created_at,edition_context_json)
      SELECT ?,id,document_revision,reference_catalog_version,
        (SELECT COALESCE(json_group_array(json_object('type',reference_type,'raw',raw_value,'normalized',normalized_value,'canonical',canonical_value,'valid',is_valid,'catalog',catalog_version,'edition',edition_id,'editionProvision',edition_provision_id,'editionBodySha256',edition_body_sha256,'applicability',applicability_status)),'[]') FROM document_reference_links WHERE document_id=documents.id),
        (SELECT COALESCE(json_group_array(json_object('type',reference_type,'raw',raw_value,'normalized',normalized_value,'message',message,'severity',severity,'catalog',catalog_version)),'[]') FROM document_reference_issues WHERE document_id=documents.id),?,reference_edition_context_json
      FROM documents WHERE id=? AND ingestion_state!='preparing'`).bind(crypto.randomUUID(),now,documentId),
    ...resetStatements,...validationStatements,
    env.DB.prepare("UPDATE documents SET reference_catalog_version=?,reference_edition_context_json=? WHERE id=?").bind(refs.catalogVersion,JSON.stringify(edition),documentId),
    env.DB.prepare("DELETE FROM reference_validation_guards WHERE token=?").bind(guard)];
}

async function inspectLegalReferencesSnapshot(env: Env) {
  const counts = await Promise.all([
    env.DB.prepare(`SELECT COUNT(*) as count FROM legal_index_codes`).first<{ count: number }>(),
    env.DB.prepare(`SELECT COUNT(*) as count FROM legal_ordinance_sections`).first<{ count: number }>(),
    env.DB.prepare(`SELECT COUNT(*) as count FROM legal_rules_sections`).first<{ count: number }>(),
    env.DB.prepare(`SELECT COUNT(*) as count FROM legal_reference_crosswalk`).first<{ count: number }>(),
    env.DB.prepare(`SELECT COUNT(*) as count FROM document_reference_issues`).first<{ count: number }>()
  ]);

  const sources = await env.DB.prepare(`SELECT source_key as sourceKey, source_path as sourcePath FROM legal_reference_sources`).all<{
    sourceKey: string;
    sourcePath: string;
  }>();
  const sourceTrace = {
    index_codes: sources.results?.find((row) => row.sourceKey === "index_codes")?.sourcePath,
    ordinance: sources.results?.find((row) => row.sourceKey === "ordinance")?.sourcePath,
    rules: sources.results?.find((row) => row.sourceKey === "rules")?.sourcePath
  };
  const coverageRaw = sources.results?.find((row) => row.sourceKey === "coverage_report_json")?.sourcePath;
  let coverageReport: unknown = undefined;
  if (coverageRaw) {
    try {
      coverageReport = JSON.parse(coverageRaw);
    } catch {
      coverageReport = undefined;
    }
  }
  const indexCodes = await env.DB.prepare(
    `SELECT code_identifier, normalized_code, family, label, description, is_reserved as reserved,
      is_legacy_pre_1002 as legacy_pre_1002, linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor
     FROM legal_index_codes
     ORDER BY code_identifier ASC
     LIMIT 30`
  ).all<{
    code_identifier: string;
    normalized_code: string;
    family: string | null;
    label: string | null;
    description: string | null;
    reserved: number;
    legacy_pre_1002: number;
    linked_ordinance_sections_json: string;
    linked_rules_sections_json: string;
    source_page_anchor: string | null;
  }>();

  const ordinance = await env.DB.prepare(
    `SELECT section_number, subsection_path, citation, normalized_citation, heading, body_text, source_page_anchor
     FROM legal_ordinance_sections
     ORDER BY citation ASC
     LIMIT 30`
  ).all<{
    section_number: string;
    subsection_path: string | null;
    citation: string;
    normalized_citation: string;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
  }>();

  const rules = await env.DB.prepare(
    `SELECT part, section_number, citation, normalized_citation, canonical_bare_citation, normalized_bare_citation, heading, body_text, source_page_anchor
     FROM legal_rules_sections
     ORDER BY citation ASC
     LIMIT 30`
  ).all<{
    part: string | null;
    section_number: string;
    citation: string;
    normalized_citation: string;
    canonical_bare_citation: string | null;
    normalized_bare_citation: string | null;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
  }>();

  const issues = await env.DB.prepare(
    `SELECT document_id, reference_type, raw_value, normalized_value, message, created_at
     FROM document_reference_issues
     ORDER BY created_at DESC
     LIMIT 60`
  ).all<{
    document_id: string;
    reference_type: string;
    raw_value: string;
    normalized_value: string;
    message: string;
    created_at: string;
  }>();

  const crosswalkRows = await env.DB.prepare(
    `SELECT index_code_id as indexCode, ordinance_citation as ordinanceCitation, rules_citation as rulesCitation, source
     FROM legal_reference_crosswalk
     LIMIT 2000`
  ).all<{
    indexCode: string | null;
    ordinanceCitation: string | null;
    rulesCitation: string | null;
    source: string;
  }>();
  const ordinanceNormRows = await env.DB.prepare(`SELECT normalized_citation as normalizedCitation FROM legal_ordinance_sections WHERE active = 1`).all<{
    normalizedCitation: string;
  }>();
  const rulesNormRows = await env.DB.prepare(
    `SELECT normalized_citation as normalizedCitation, normalized_bare_citation as normalizedBareCitation
     FROM legal_rules_sections
     WHERE active = 1`
  ).all<{
    normalizedCitation: string;
    normalizedBareCitation: string | null;
  }>();
  const ordinanceSet = new Set((ordinanceNormRows.results ?? []).map((row) => row.normalizedCitation));
  const rulesSet = new Set((rulesNormRows.results ?? []).map((row) => row.normalizedCitation).filter(Boolean));
  const rulesBareSet = new Set((rulesNormRows.results ?? []).map((row) => row.normalizedBareCitation || "").filter(Boolean));
  const unresolvedCrosswalks = (crosswalkRows.results ?? [])
    .map((row) => {
      const ordinanceCitation = row.ordinanceCitation ?? undefined;
      const rulesCitation = row.rulesCitation ?? undefined;
      if (
        ordinanceCitation &&
        !Array.from(ordinanceSet).some((candidate) => citationMatch(normalizeOrdinanceCitationForLookup(ordinanceCitation), candidate))
      ) {
        return {
          index_code: row.indexCode ?? undefined,
          ordinance_citation: ordinanceCitation,
          rules_citation: rulesCitation,
          source: row.source,
          reason: "ordinance_unresolved"
        };
      }
      if (
        rulesCitation &&
        !Array.from(rulesSet).some((candidate) => citationMatch(normalizeCitation(rulesCitation), candidate)) &&
        !Array.from(rulesBareSet).some((candidate) => citationMatch(normalizeBareRulesCitation(rulesCitation), candidate))
      ) {
        return {
          index_code: row.indexCode ?? undefined,
          ordinance_citation: ordinanceCitation,
          rules_citation: rulesCitation,
          source: row.source,
          reason: "rules_unresolved"
        };
      }
      return null;
    })
    .filter((row): row is NonNullable<typeof row> => Boolean(row))
    .slice(0, 120);
  const criticalCitationChecks = await verifyCitations(env, { citations: DEFAULT_CRITICAL_CITATIONS });
  const criticalCitationExceptions = criticalCitationChecks.checks.map((check) => {
    if (check.status === "resolved") {
      return {
        citation: check.citation,
        status: check.status,
        classification: "exact_resolved" as const,
        recommendation: "Exact match present in committed normalized references."
      };
    }
    if (check.status === "ambiguous" && check.ordinance_matches.length > 0 && check.rules_matches.length > 0) {
      return {
        citation: check.citation,
        status: check.status,
        classification: "cross_context_ambiguity" as const,
        recommendation: "Citation appears in both ordinance and rules contexts; keep ambiguous and require contextual reviewer confirmation."
      };
    }
    if (check.diagnostic === "multiple_exact") {
      return {
        citation: check.citation,
        status: check.status,
        classification: "multiple_exact_matches" as const,
        recommendation: "Multiple exact matches found; keep ambiguous and require explicit source selection."
      };
    }
    if (check.diagnostic === "parent_or_related_only") {
      return {
        citation: check.citation,
        status: check.status,
        classification: "parent_or_related_only" as const,
        recommendation: "Only parent/related references found; keep unresolved until exact subsection is present in normalized source."
      };
    }
    if (check.diagnostic === "not_found") {
      return {
        citation: check.citation,
        status: check.status,
        classification: "not_found_in_committed_set" as const,
        recommendation: "No committed exact citation found; treat as source-limited or parser miss pending manual review."
      };
    }
    return {
      citation: check.citation,
      status: check.status,
      classification: "other" as const,
      recommendation: "Unresolved critical citation needs manual review."
    };
  });
  const ordinanceCount = counts[1]?.count ?? 0;
  const rulesCount = counts[2]?.count ?? 0;
  const unresolvedCrosswalkCount = unresolvedCrosswalks.length;
  const totalCrosswalkCandidates =
    typeof (coverageReport as { crosswalk?: { total_candidates?: number } } | undefined)?.crosswalk?.total_candidates === "number"
      ? (coverageReport as { crosswalk?: { total_candidates: number } }).crosswalk?.total_candidates ?? 0
      : counts[3]?.count ?? 0;
  const criticalResolved = criticalCitationChecks.checks.every((item) => item.status === "resolved");
  const coverageOrdinanceCount =
    typeof (coverageReport as { ordinance?: { parsed_section_count?: number } } | undefined)?.ordinance?.parsed_section_count === "number"
      ? (coverageReport as { ordinance?: { parsed_section_count: number } }).ordinance?.parsed_section_count ?? ordinanceCount
      : ordinanceCount;
  const coverageRulesCount =
    typeof (coverageReport as { rules?: { parsed_section_count?: number } } | undefined)?.rules?.parsed_section_count === "number"
      ? (coverageReport as { rules?: { parsed_section_count: number } }).rules?.parsed_section_count ?? rulesCount
      : rulesCount;
  const ordinanceDropped =
    typeof (coverageReport as { ordinance?: { duplicates_dropped?: number } } | undefined)?.ordinance?.duplicates_dropped === "number"
      ? (coverageReport as { ordinance?: { duplicates_dropped: number } }).ordinance?.duplicates_dropped ?? 0
      : 0;
  const rulesDropped =
    typeof (coverageReport as { rules?: { duplicates_dropped?: number } } | undefined)?.rules?.duplicates_dropped === "number"
      ? (coverageReport as { rules?: { duplicates_dropped: number } }).rules?.duplicates_dropped ?? 0
      : 0;
  const resolvedCrosswalkCount = Math.max(0, totalCrosswalkCandidates - unresolvedCrosswalkCount);
  const unresolvedOrAmbiguousCritical = criticalCitationExceptions.filter((item) => item.status !== "resolved");
  const unresolvedKnownExceptionsOnly =
    unresolvedOrAmbiguousCritical.length > 0 && unresolvedOrAmbiguousCritical.every((item) => KNOWN_CRITICAL_EXCEPTION_CITATIONS.has(item.citation));
  const readinessRecommendation: "blocked" | "safe_for_limited_pilot_import" | "safe_for_broader_import" =
    criticalResolved &&
    totalCrosswalkCandidates > 0 &&
    resolvedCrosswalkCount > 0 &&
    unresolvedCrosswalkCount === 0 &&
    ordinanceCount >= 15 &&
    rulesCount >= 10
      ? "safe_for_broader_import"
      : unresolvedKnownExceptionsOnly && ordinanceCount >= 15 && rulesCount >= 10 && totalCrosswalkCandidates > 0 && resolvedCrosswalkCount > 0
        ? "safe_for_limited_pilot_import"
        : "blocked";
  const readinessStatus = {
    ordinance_coverage_ok: ordinanceCount >= 15,
    rules_coverage_ok: rulesCount >= 10,
    crosswalk_resolvable: totalCrosswalkCandidates > 0 && resolvedCrosswalkCount > 0 && unresolvedCrosswalkCount === 0,
    critical_citations_ok: criticalResolved,
    crosswalk_candidates_meaningful: totalCrosswalkCandidates > 0,
    counts_consistent:
      coverageOrdinanceCount >= ordinanceCount &&
      coverageRulesCount >= rulesCount &&
      coverageOrdinanceCount - ordinanceCount <= Math.max(ordinanceDropped, 0) &&
      coverageRulesCount - rulesCount <= Math.max(rulesDropped, 0),
    readiness_recommendation: readinessRecommendation
  };

  return legalReferenceInspectResponseSchema.parse({
    source_trace: sourceTrace,
    summary: {
      index_code_count: counts[0]?.count ?? 0,
      ordinance_section_count: counts[1]?.count ?? 0,
      rules_section_count: counts[2]?.count ?? 0,
      crosswalk_count: counts[3]?.count ?? 0,
      unmatched_reference_issue_count: counts[4]?.count ?? 0
    },
    coverage_report: coverageReport as Record<string, unknown> | undefined,
    readiness_status: readinessStatus,
    samples: {
      index_codes: (indexCodes.results ?? []).map((row) => ({
        ...row,
        reserved: Boolean(row.reserved),
        legacy_pre_1002: Boolean(row.legacy_pre_1002),
        linked_ordinance_sections: parseJsonArray(row.linked_ordinance_sections_json),
        linked_rules_sections: parseJsonArray(row.linked_rules_sections_json)
      })),
      ordinance_sections: (ordinance.results ?? []).map((row) => ({
        ...row,
        body_text: row.body_text.slice(0, 500)
      })),
      rules_sections: (rules.results ?? []).map((row) => ({
        ...row,
        body_text: row.body_text.slice(0, 500)
      }))
    },
    unmatched_reference_issues: issues.results ?? [],
    unresolved_crosswalks: unresolvedCrosswalks,
    critical_citation_checks: criticalCitationChecks.checks,
    critical_citation_exceptions: criticalCitationExceptions
  });
}

export function normalizeFilterValue(type: "index_code" | "rules_section" | "ordinance_section", value: string) {
  if (type === "index_code") return normalizeIndexCode(value);
  if (type === "ordinance_section") return normalizeOrdinanceCitationForLookup(value);
  return normalizeCitation(value);
}

export async function backfillReferenceValidation(env: Env, limit = 500, offset = 0) {
  const { createBackfillJob } = await import("./maintenance-jobs");
  return createBackfillJob(env,"reference_backfill",{limit,offset});
}

async function verifyCitationsSnapshot(env: Env, payload: unknown) {
  const parsed = legalCitationVerifyRequestSchema.parse(payload);
  const ordinanceRows = await env.DB.prepare(
    `SELECT citation, heading, normalized_citation as normalizedCitation
     FROM legal_ordinance_sections
     WHERE active = 1`
  ).all<{ citation: string; heading: string | null; normalizedCitation: string }>();
  const rulesRows = await env.DB.prepare(
    `SELECT citation, part, section_number AS sectionNumber, heading, normalized_citation as normalizedCitation, normalized_bare_citation as normalizedBareCitation
     FROM legal_rules_sections
     WHERE active = 1`
  ).all<{ citation: string; part?:string|null; sectionNumber?:string|null; heading: string | null; normalizedCitation: string; normalizedBareCitation: string | null }>();
  const ordinanceDataset = (ordinanceRows.results ?? []).map(row=>({...row,normalizedCitation:citationIdentity(row.citation,"ordinance")}));
  const rulesDataset = (rulesRows.results ?? []).map(row => {
    const label=ruleCatalogLabel({...row,canonicalValue:row.citation});
    return {...row,normalizedCitation:citationIdentity(label,"rule"),normalizedBareCitation:citationKey(label,"rule")};
  });
  const checks: Array<{
    citation: string;
    normalized: string;
    status: "resolved" | "unresolved" | "ambiguous";
    diagnostic?: "exact_match" | "parent_or_related_only" | "not_found" | "multiple_exact";
    ordinance_matches: Array<{ citation: string; heading: string | null }>;
    rules_matches: Array<{ citation: string; heading: string | null }>;
  }> = [];

  for (const citation of parsed.citations) {
    const context = parseCitation(citation).context;
    const ordDataset = context === "rule" ? [] : ordinanceDataset;
    const rulDataset = context === "ordinance" ? [] : rulesDataset;
    const normalized = normalizeCitation(citation);
    const normalizedBare = normalizeBareRulesCitation(citation);
    const normalizedBase = normalizedBaseCitation(citation);
    const ordExact = ordDataset.filter((row) => row.normalizedCitation === normalized).map((row) => ({ citation: row.citation, heading: row.heading }));
    const rulExact = rulDataset
      .filter((row) => row.normalizedCitation === normalized || row.normalizedBareCitation === normalizedBare)
      .map((row) => ({ citation: row.citation, heading: row.heading }));
    const exactTotal = ordExact.length + rulExact.length;

    const ordRelated =
      exactTotal > 0
        ? ordExact
        : ordDataset
            .filter((row) => {
              if (!row.normalizedCitation) return false;
              if (row.normalizedCitation === normalizedBase) return true;
              return citationMatch(normalized, row.normalizedCitation);
            })
            .map((row) => ({ citation: row.citation, heading: row.heading }));
    const rulRelated =
      exactTotal > 0
        ? rulExact
        : rulDataset
            .filter((row) => {
              if (!row.normalizedCitation) return false;
              if (row.normalizedCitation === normalizedBase || row.normalizedBareCitation === normalizedBase) return true;
              return citationMatch(normalized, row.normalizedCitation) || citationMatch(normalizedBare, row.normalizedBareCitation || "");
            })
            .map((row) => ({ citation: row.citation, heading: row.heading }));

    const ord = Array.from(new Map(ordRelated.map((row) => [row.citation, row])).values());
    const rul = Array.from(new Map(rulRelated.map((row) => [row.citation, row])).values());
    const totalRelated = ord.length + rul.length;
    const status: "resolved" | "unresolved" | "ambiguous" = exactTotal === 0 ? "unresolved" : exactTotal === 1 ? "resolved" : "ambiguous";
    const diagnostic: "exact_match" | "parent_or_related_only" | "not_found" | "multiple_exact" =
      exactTotal > 1 ? "multiple_exact" : exactTotal === 1 ? "exact_match" : totalRelated > 0 ? "parent_or_related_only" : "not_found";

    checks.push({
      citation,
      normalized,
      status,
      diagnostic,
      ordinance_matches: ord,
      rules_matches: rul
    });
  }

  return { checks };
}

async function listRulesCitationInventorySnapshot(
  env: Env,
  options?: { citation?: string; normalized?: string; bare?: string; prefix?: string; limit?: number }
) {
  const rows = await env.DB.prepare(
    `SELECT citation, normalized_citation as normalizedCitation, canonical_bare_citation as canonicalBareCitation,
            normalized_bare_citation as normalizedBareCitation, heading
     FROM legal_rules_sections
     WHERE active = 1
     ORDER BY citation ASC`
  ).all<{
    citation: string;
    normalizedCitation: string;
    canonicalBareCitation: string | null;
    normalizedBareCitation: string | null;
    heading: string | null;
  }>();
  const citation = options?.citation ? normalizeCitation(options.citation) : "";
  const normalized = options?.normalized ? normalizeCitation(options.normalized) : "";
  const bare = options?.bare ? normalizeBareRulesCitation(options.bare) : "";
  const prefix = options?.prefix ? normalizeCitation(options.prefix) : "";
  const limit = Math.max(1, Math.min(options?.limit ?? 100, 500));

  const filtered = (rows.results ?? []).filter((row) => {
    if (citation && row.normalizedCitation !== citation && row.normalizedBareCitation !== citation) return false;
    if (normalized && row.normalizedCitation !== normalized) return false;
    if (bare && row.normalizedBareCitation !== bare) return false;
    if (prefix && !row.normalizedCitation.startsWith(prefix) && !(row.normalizedBareCitation || "").startsWith(prefix)) return false;
    return true;
  });

  return {
    total: filtered.length,
    rows: filtered.slice(0, limit).map((row) => ({
      display_citation: row.citation,
      canonical_bare_citation: row.canonicalBareCitation,
      normalized_citation: row.normalizedCitation,
      normalized_bare_citation: row.normalizedBareCitation,
      heading: row.heading
    }))
  };
}

export async function inferIndexCodesFromReferences(...args: Parameters<typeof inferIndexCodesFromReferencesSnapshot>) {
  const state = await catalogState(args[0]);
  const result = await inferIndexCodesFromReferencesSnapshot(...args);
  await assertCatalogState(args[0], state);
  return {...result,catalogVersion:state.version,catalogEpoch:state.epoch};
}

export async function inspectLegalReferences(...args: Parameters<typeof inspectLegalReferencesSnapshot>) {
  const state = await catalogState(args[0]);
  const result = await inspectLegalReferencesSnapshot(...args);
  const referenceEdition=await resolveReferenceEdition(args[0],undefined,{fallbackToCurrent:true,registryEpoch:state.epoch});
  await assertCatalogState(args[0], state);
  return {...result,referenceEdition,catalogVersion:state.version,catalogEpoch:state.epoch};
}

export async function verifyCitations(...args: Parameters<typeof verifyCitationsSnapshot>) {
  const state = await catalogState(args[0]);
  const result = await verifyCitationsSnapshot(...args);
  await assertCatalogState(args[0], state);
  return {...result,catalogVersion:state.version,catalogEpoch:state.epoch};
}

export async function listRulesCitationInventory(...args: Parameters<typeof listRulesCitationInventorySnapshot>) {
  const state = await catalogState(args[0]);
  const result = await listRulesCitationInventorySnapshot(...args);
  await assertCatalogState(args[0], state);
  return {...result,catalogVersion:state.version,catalogEpoch:state.epoch};
}
