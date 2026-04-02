import { legalCitationVerifyRequestSchema, legalReferenceInspectResponseSchema, legalReferenceRebuildRequestSchema } from "@beedle/shared";
import type { Env } from "../lib/types";

function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID()}`;
}

function normalizeToken(input: string): string {
  return input.toLowerCase().replace(/[\s_]+/g, "").replace(/[^a-z0-9.()\-]/g, "");
}

function normalizeIndexCode(input: string): string {
  return normalizeToken(input).replace(/^ic/, "").replace(/^[-]+/, "");
}

function normalizeCitation(input: string): string {
  return normalizeToken(input).replace(/^section/, "").replace(/^sec/, "").replace(/^rule/, "").replace(/^part[0-9a-z.\-]+\-/, "");
}

const SAFE_37X_ORDINANCE_PREFIX_BASES = new Set(["37.1", "37.2", "37.8"]);

export function normalizeOrdinanceCitationForLookup(input: string): string {
  const normalized = normalizeCitation(input);
  if (!normalized.startsWith("ordinance37.")) return normalized;
  const withoutPrefix = normalized.replace(/^ordinance/, "");
  const base = withoutPrefix.replace(/\([a-z0-9]+\)/g, "");
  if (SAFE_37X_ORDINANCE_PREFIX_BASES.has(base)) {
    return withoutPrefix;
  }
  return normalized;
}

function normalizeBareRulesCitation(input: string): string {
  return normalizeToken(input)
    .replace(/^section/, "")
    .replace(/^sec/, "")
    .replace(/^rule/, "")
    .replace(/^[ivxlcdm]+\-/i, "")
    .replace(/^part[0-9a-z.\-]+\-/, "");
}

function citationMatch(normalizedQuery: string, normalizedCandidate: string): boolean {
  if (!normalizedQuery || !normalizedCandidate) return false;
  if (normalizedQuery === normalizedCandidate) return true;
  // Directional match: a base section may resolve to a subsection, but a subsection does not resolve to its parent.
  if (normalizedCandidate.startsWith(`${normalizedQuery}(`)) return true;
  return false;
}

function normalizedBaseCitation(value: string): string {
  const raw = String(value)
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/^rule/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "");
  const idx = raw.indexOf("(");
  const baseRaw = idx >= 0 ? raw.slice(0, idx) : raw;
  return normalizeToken(baseRaw);
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
const KNOWN_CRITICAL_EXCEPTION_CITATIONS = new Set(["37.2(g)", "10.10(c)(3)", "37.15"]);

type ReferenceSnapshot = {
  sources: Array<{ source_key: string; source_path: string; updated_at: string }>;
  indexCodes: Array<{
    id: string;
    code_identifier: string;
    normalized_code: string;
    family: string | null;
    label: string | null;
    description: string | null;
    is_reserved: number;
    is_legacy_pre_1002: number;
    linked_ordinance_sections_json: string;
    linked_rules_sections_json: string;
    source_page_anchor: string | null;
    active: number;
    created_at: string;
    updated_at: string;
  }>;
  ordinanceSections: Array<{
    id: string;
    section_number: string;
    subsection_path: string | null;
    citation: string;
    normalized_citation: string;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
    active: number;
    created_at: string;
    updated_at: string;
  }>;
  rulesSections: Array<{
    id: string;
    part: string | null;
    section_number: string;
    citation: string;
    normalized_citation: string;
    canonical_bare_citation: string | null;
    normalized_bare_citation: string | null;
    heading: string | null;
    body_text: string;
    source_page_anchor: string | null;
    active: number;
    created_at: string;
    updated_at: string;
  }>;
  crosswalk: Array<{
    id: string;
    index_code_id: string | null;
    ordinance_citation: string | null;
    rules_citation: string | null;
    source: string;
    created_at: string;
  }>;
};

async function takeReferenceSnapshot(env: Env): Promise<ReferenceSnapshot> {
  const [sources, indexCodes, ordinanceSections, rulesSections, crosswalk] = await Promise.all([
    env.DB.prepare(
      `SELECT source_key, source_path, updated_at
       FROM legal_reference_sources`
    ).all<{ source_key: string; source_path: string; updated_at: string }>(),
    env.DB.prepare(
      `SELECT id, code_identifier, normalized_code, family, label, description, is_reserved, is_legacy_pre_1002,
              linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor, active, created_at, updated_at
       FROM legal_index_codes`
    ).all<ReferenceSnapshot["indexCodes"][number]>(),
    env.DB.prepare(
      `SELECT id, section_number, subsection_path, citation, normalized_citation, heading, body_text, source_page_anchor, active, created_at, updated_at
       FROM legal_ordinance_sections`
    ).all<ReferenceSnapshot["ordinanceSections"][number]>(),
    env.DB.prepare(
      `SELECT id, part, section_number, citation, normalized_citation, canonical_bare_citation, normalized_bare_citation,
              heading, body_text, source_page_anchor, active, created_at, updated_at
       FROM legal_rules_sections`
    ).all<ReferenceSnapshot["rulesSections"][number]>(),
    env.DB.prepare(
      `SELECT id, index_code_id, ordinance_citation, rules_citation, source, created_at
       FROM legal_reference_crosswalk`
    ).all<ReferenceSnapshot["crosswalk"][number]>()
  ]);
  return {
    sources: sources.results ?? [],
    indexCodes: indexCodes.results ?? [],
    ordinanceSections: ordinanceSections.results ?? [],
    rulesSections: rulesSections.results ?? [],
    crosswalk: crosswalk.results ?? []
  };
}

async function clearReferenceTables(env: Env) {
  await env.DB.prepare("DELETE FROM legal_reference_crosswalk").run();
  await env.DB.prepare("DELETE FROM legal_reference_sources").run();
  await env.DB.prepare("DELETE FROM legal_index_codes").run();
  await env.DB.prepare("DELETE FROM legal_ordinance_sections").run();
  await env.DB.prepare("DELETE FROM legal_rules_sections").run();
}

async function restoreReferenceSnapshot(env: Env, snapshot: ReferenceSnapshot) {
  await clearReferenceTables(env);
  for (const row of snapshot.sources) {
    await env.DB.prepare(
      `INSERT INTO legal_reference_sources (source_key, source_path, updated_at)
       VALUES (?, ?, ?)`
    )
      .bind(row.source_key, row.source_path, row.updated_at)
      .run();
  }
  for (const row of snapshot.indexCodes) {
    await env.DB.prepare(
      `INSERT INTO legal_index_codes (
        id, code_identifier, normalized_code, family, label, description, is_reserved, is_legacy_pre_1002,
        linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        row.id,
        row.code_identifier,
        row.normalized_code,
        row.family,
        row.label,
        row.description,
        row.is_reserved,
        row.is_legacy_pre_1002,
        row.linked_ordinance_sections_json,
        row.linked_rules_sections_json,
        row.source_page_anchor,
        row.active,
        row.created_at,
        row.updated_at
      )
      .run();
  }
  for (const row of snapshot.ordinanceSections) {
    await env.DB.prepare(
      `INSERT INTO legal_ordinance_sections (
        id, section_number, subsection_path, citation, normalized_citation, heading, body_text, source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        row.id,
        row.section_number,
        row.subsection_path,
        row.citation,
        row.normalized_citation,
        row.heading,
        row.body_text,
        row.source_page_anchor,
        row.active,
        row.created_at,
        row.updated_at
      )
      .run();
  }
  for (const row of snapshot.rulesSections) {
    await env.DB.prepare(
      `INSERT INTO legal_rules_sections (
        id, part, section_number, citation, normalized_citation, canonical_bare_citation, normalized_bare_citation,
        heading, body_text, source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        row.id,
        row.part,
        row.section_number,
        row.citation,
        row.normalized_citation,
        row.canonical_bare_citation,
        row.normalized_bare_citation,
        row.heading,
        row.body_text,
        row.source_page_anchor,
        row.active,
        row.created_at,
        row.updated_at
      )
      .run();
  }
  for (const row of snapshot.crosswalk) {
    await env.DB.prepare(
      `INSERT INTO legal_reference_crosswalk (
        id, index_code_id, ordinance_citation, rules_citation, source, created_at
      ) VALUES (?, ?, ?, ?, ?, ?)`
    )
      .bind(row.id, row.index_code_id, row.ordinance_citation, row.rules_citation, row.source, row.created_at)
      .run();
  }
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
    const citation = part ? `${part}-${sectionNumber}` : sectionNumber;
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
  const snapshot = await takeReferenceSnapshot(env);
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
  try {
    await clearReferenceTables(env);
    await env.DB.prepare(
      `INSERT INTO legal_reference_sources (source_key, source_path, updated_at) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)`
    )
      .bind(
        "index_codes",
        parsed.source_trace.index_codes,
        now,
        "ordinance",
        parsed.source_trace.ordinance,
        now,
        "rules",
        parsed.source_trace.rules,
        now,
        "coverage_report_json",
        JSON.stringify(coverageReport),
        now
      )
      .run();

    for (const row of dedupedIndex.rows) {
    const code = row.code_identifier.trim();
    await env.DB.prepare(
      `INSERT INTO legal_index_codes (
        id, code_identifier, normalized_code, family, label, description,
        is_reserved, is_legacy_pre_1002, linked_ordinance_sections_json, linked_rules_sections_json,
        source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
    )
      .bind(
        id("idx"),
        code,
        normalizeIndexCode(code),
        row.family ?? null,
        row.label ?? null,
        row.description ?? null,
        row.reserved ? 1 : 0,
        row.legacy_pre_1002 ? 1 : 0,
        JSON.stringify(unique(row.linked_ordinance_sections.map((item) => item.trim()).filter(Boolean))),
        JSON.stringify(unique(row.linked_rules_sections.map((item) => item.trim()).filter(Boolean))),
        row.source_page_anchor ?? null,
        now,
        now
      )
      .run();
    }

    for (const row of dedupedOrdinance.rows) {
    const citation = row.citation;
    await env.DB.prepare(
      `INSERT INTO legal_ordinance_sections (
        id, section_number, subsection_path, citation, normalized_citation, heading, body_text,
        source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
    )
      .bind(
        id("ord"),
        row.section_number.trim(),
        row.subsection_path ?? null,
        citation.trim(),
        row.normalized_citation,
        row.heading ?? null,
        row.body_text,
        row.source_page_anchor ?? null,
        now,
        now
      )
      .run();
    }

    for (const row of dedupedRules.rows) {
    const citation = row.citation;
    await env.DB.prepare(
      `INSERT INTO legal_rules_sections (
        id, part, section_number, citation, normalized_citation, canonical_bare_citation, normalized_bare_citation,
        heading, body_text, source_page_anchor, active, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
    )
      .bind(
        id("rul"),
        row.part ?? null,
        row.section_number.trim(),
        citation.trim(),
        row.normalized_citation,
        row.canonical_bare_citation,
        row.normalized_bare_citation,
        row.heading ?? null,
        row.body_text,
        row.source_page_anchor ?? null,
        now,
        now
      )
      .run();
    }

    for (const row of crosswalk) {
    await env.DB.prepare(
      `INSERT INTO legal_reference_crosswalk (
        id, index_code_id, ordinance_citation, rules_citation, source, created_at
      ) VALUES (?, ?, ?, ?, ?, ?)`
    )
      .bind(
        id("xw"),
        row.index_code ?? null,
        row.ordinance_section ?? null,
        row.rules_section ?? null,
        row.source,
        now
      )
      .run();
    }
  } catch (error) {
    try {
      await restoreReferenceSnapshot(env, snapshot);
    } catch (restoreError) {
      const primaryMessage = error instanceof Error ? error.message : String(error);
      const restoreMessage = restoreError instanceof Error ? restoreError.message : String(restoreError);
      throw new Error(`Reference rebuild failed and rollback restore also failed: ${primaryMessage}; restore error: ${restoreMessage}`);
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Reference rebuild failed without replacing committed data: ${message}`);
  }

  return {
    ok: true,
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

export async function validateReferencesAgainstNormalized(
  env: Env,
  values: {
    indexCodes: string[];
    rulesSections: string[];
    ordinanceSections: string[];
  }
) {
  const unknownIndexCodes: string[] = [];
  const unknownRules: string[] = [];
  const unknownOrdinance: string[] = [];

  const normalizedIndexCodes = unique(values.indexCodes.map((item) => item.trim()).filter(Boolean));
  const normalizedRules = unique(values.rulesSections.map((item) => item.trim()).filter(Boolean));
  const normalizedOrdinance = unique(values.ordinanceSections.map((item) => item.trim()).filter(Boolean));

  for (const code of normalizedIndexCodes) {
    const row = await env.DB.prepare(
      `SELECT code_identifier as canonicalValue, is_reserved as isReserved
       FROM legal_index_codes
       WHERE normalized_code = ? AND active = 1
       LIMIT 1`
    )
      .bind(normalizeIndexCode(code))
      .first<{ canonicalValue: string; isReserved: number }>();
    if (!row) unknownIndexCodes.push(code);
  }

  for (const ref of normalizedRules) {
    const row = await env.DB.prepare(
      `SELECT citation as canonicalValue
       FROM legal_rules_sections
       WHERE (normalized_citation = ? OR normalized_bare_citation = ?) AND active = 1
       LIMIT 1`
    )
      .bind(normalizeCitation(ref), normalizeBareRulesCitation(ref))
      .first<{ canonicalValue: string }>();
    if (!row) unknownRules.push(ref);
  }

  for (const ref of normalizedOrdinance) {
    const row = await env.DB.prepare(
      `SELECT citation as canonicalValue
       FROM legal_ordinance_sections
       WHERE normalized_citation = ? AND active = 1
       LIMIT 1`
    )
      .bind(normalizeOrdinanceCitationForLookup(ref))
      .first<{ canonicalValue: string }>();
    if (!row) unknownOrdinance.push(ref);
  }

  return {
    unknownIndexCodes,
    unknownRules,
    unknownOrdinance
  };
}

export async function inferIndexCodesFromReferences(
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

export async function refreshDocumentReferenceValidation(
  env: Env,
  documentId: string,
  input: { indexCodes: string[]; rulesSections: string[]; ordinanceSections: string[] }
) {
  const now = new Date().toISOString();
  await env.DB.prepare(`DELETE FROM document_reference_links WHERE document_id = ?`).bind(documentId).run();
  await env.DB.prepare(`DELETE FROM document_reference_issues WHERE document_id = ?`).bind(documentId).run();

  for (const value of unique(input.indexCodes.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeIndexCode(value);
    const row = await env.DB.prepare(
      `SELECT code_identifier as canonicalValue, is_reserved as isReserved
       FROM legal_index_codes
       WHERE normalized_code = ? AND active = 1
       LIMIT 1`
    )
      .bind(normalized)
      .first<{ canonicalValue: string; isReserved: number }>();

    await env.DB.prepare(
      `INSERT INTO document_reference_links (
        id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
      ) VALUES (?, ?, 'index_code', ?, ?, ?, ?, ?)`
    )
      .bind(id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now)
      .run();

    if (!row || row.isReserved) {
      const message = !row
        ? "Index code not found in normalized reference set"
        : "Index code exists but is marked reserved; verify before approval";
      await env.DB.prepare(
        `INSERT INTO document_reference_issues (
          id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at
        ) VALUES (?, ?, 'index_code', ?, ?, ?, ?, ?)`
      )
        .bind(id("dri"), documentId, value, normalized, message, row ? "warning" : "error", now)
        .run();
    }
  }

  for (const value of unique(input.rulesSections.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeCitation(value);
    const row = await env.DB.prepare(
      `SELECT citation as canonicalValue
       FROM legal_rules_sections
       WHERE (normalized_citation = ? OR normalized_bare_citation = ?) AND active = 1
       LIMIT 1`
    )
      .bind(normalized, normalizeBareRulesCitation(value))
      .first<{ canonicalValue: string }>();

    await env.DB.prepare(
      `INSERT INTO document_reference_links (
        id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
      ) VALUES (?, ?, 'rules_section', ?, ?, ?, ?, ?)`
    )
      .bind(id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now)
      .run();

    if (!row) {
      await env.DB.prepare(
        `INSERT INTO document_reference_issues (
          id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at
        ) VALUES (?, ?, 'rules_section', ?, ?, ?, 'error', ?)`
      )
        .bind(id("dri"), documentId, value, normalized, "Rules reference not found in normalized reference set", now)
        .run();
    }
  }

  for (const value of unique(input.ordinanceSections.map((item) => item.trim()).filter(Boolean))) {
    const normalized = normalizeOrdinanceCitationForLookup(value);
    const row = await env.DB.prepare(
      `SELECT citation as canonicalValue
       FROM legal_ordinance_sections
       WHERE normalized_citation = ? AND active = 1
       LIMIT 1`
    )
      .bind(normalized)
      .first<{ canonicalValue: string }>();

    await env.DB.prepare(
      `INSERT INTO document_reference_links (
        id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
      ) VALUES (?, ?, 'ordinance_section', ?, ?, ?, ?, ?)`
    )
      .bind(id("drl"), documentId, value, normalized, row?.canonicalValue ?? null, row ? 1 : 0, now)
      .run();

    if (!row) {
      await env.DB.prepare(
        `INSERT INTO document_reference_issues (
          id, document_id, reference_type, raw_value, normalized_value, message, severity, created_at
        ) VALUES (?, ?, 'ordinance_section', ?, ?, ?, 'error', ?)`
      )
        .bind(id("dri"), documentId, value, normalized, "Ordinance reference not found in normalized reference set", now)
        .run();
    }
  }
}

export async function inspectLegalReferences(env: Env) {
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

export async function backfillReferenceValidation(env: Env, limit = 500) {
  const rows = await env.DB.prepare(
    `SELECT id, index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson
     FROM documents
     ORDER BY created_at DESC
     LIMIT ?`
  )
    .bind(Math.max(1, Math.min(limit, 5000)))
    .all<{
      id: string;
      indexCodesJson: string;
      rulesSectionsJson: string;
      ordinanceSectionsJson: string;
    }>();

  let processed = 0;
  for (const row of rows.results ?? []) {
    await refreshDocumentReferenceValidation(env, row.id, {
      indexCodes: parseJsonArray(row.indexCodesJson),
      rulesSections: parseJsonArray(row.rulesSectionsJson),
      ordinanceSections: parseJsonArray(row.ordinanceSectionsJson)
    });
    processed += 1;
  }

  return { processed };
}

export async function verifyCitations(env: Env, payload: unknown) {
  const parsed = legalCitationVerifyRequestSchema.parse(payload);
  const ordinanceRows = await env.DB.prepare(
    `SELECT citation, heading, normalized_citation as normalizedCitation
     FROM legal_ordinance_sections
     WHERE active = 1`
  ).all<{ citation: string; heading: string | null; normalizedCitation: string }>();
  const rulesRows = await env.DB.prepare(
    `SELECT citation, heading, normalized_citation as normalizedCitation, normalized_bare_citation as normalizedBareCitation
     FROM legal_rules_sections
     WHERE active = 1`
  ).all<{ citation: string; heading: string | null; normalizedCitation: string; normalizedBareCitation: string | null }>();
  const ordinanceDataset = ordinanceRows.results ?? [];
  const rulesDataset = rulesRows.results ?? [];
  const checks: Array<{
    citation: string;
    normalized: string;
    status: "resolved" | "unresolved" | "ambiguous";
    diagnostic?: "exact_match" | "parent_or_related_only" | "not_found" | "multiple_exact";
    ordinance_matches: Array<{ citation: string; heading: string | null }>;
    rules_matches: Array<{ citation: string; heading: string | null }>;
  }> = [];

  for (const citation of parsed.citations) {
    const normalized = normalizeCitation(citation);
    const normalizedBare = normalizeBareRulesCitation(citation);
    const normalizedBase = normalizedBaseCitation(citation);
    const ordExact = ordinanceDataset.filter((row) => row.normalizedCitation === normalized).map((row) => ({ citation: row.citation, heading: row.heading }));
    const rulExact = rulesDataset
      .filter((row) => row.normalizedCitation === normalized || row.normalizedBareCitation === normalizedBare)
      .map((row) => ({ citation: row.citation, heading: row.heading }));
    const exactTotal = ordExact.length + rulExact.length;

    const ordRelated =
      exactTotal > 0
        ? ordExact
        : ordinanceDataset
            .filter((row) => {
              if (!row.normalizedCitation) return false;
              if (row.normalizedCitation === normalizedBase) return true;
              return citationMatch(normalized, row.normalizedCitation);
            })
            .map((row) => ({ citation: row.citation, heading: row.heading }));
    const rulRelated =
      exactTotal > 0
        ? rulExact
        : rulesDataset
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

export async function listRulesCitationInventory(
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
