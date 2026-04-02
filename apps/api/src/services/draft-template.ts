import {
  draftTemplateRequestSchema,
  draftTemplateResponseSchema,
  type CaseAssistantResponse,
  type DraftTemplateRequest,
  type DraftTemplateResponse
} from "@beedle/shared";
import type { Env } from "../lib/types";
import { runCaseAssistant } from "./case-assistant";
import { getActiveTaxonomyConfig, resolveCaseTypeTemplate } from "./template-config";

type Authority = CaseAssistantResponse["similar_cases"][number];

function compact(input: string, max = 180): string {
  return input.length <= max ? input : `${input.slice(0, max - 3)}...`;
}

function contextualSuffix(params: { findingsText?: string; lawText?: string; mode: DraftTemplateRequest["template_mode"] }): string {
  if (params.mode !== "lightly_contextualized") return "";
  const snippets: string[] = [];
  if (params.findingsText && params.findingsText.trim().length > 0) {
    snippets.push(`Context hint from findings: ${compact(params.findingsText.trim(), 140)}`);
  }
  if (params.lawText && params.lawText.trim().length > 0) {
    snippets.push(`Context hint from law text: ${compact(params.lawText.trim(), 140)}`);
  }
  if (snippets.length === 0) return "";
  return `\n${snippets.join("\n")}`;
}

async function maybeRetrieveAuthorities(env: Env, parsed: DraftTemplateRequest): Promise<CaseAssistantResponse | null> {
  const findings = parsed.findings_text?.trim() ?? "";
  const law = parsed.law_text?.trim() ?? "";
  if (!findings || !law) return null;
  return runCaseAssistant(env, {
    findings_text: findings,
    law_text: law,
    index_codes: parsed.index_codes,
    rules_sections: parsed.rules_sections,
    ordinance_sections: parsed.ordinance_sections,
    issue_tags: parsed.issue_tags
  });
}

function modeSpecificPlaceholder(base: string, mode: DraftTemplateRequest["template_mode"], sectionName: string): string {
  if (mode === "blank_scaffold") return base;
  if (mode === "guided_scaffold") {
    return `${base}\n[Guidance: keep this section concise, numbered where appropriate, and tied to record evidence.]`;
  }
  if (sectionName === "Conclusions of Law") {
    return `${base}\n[Light context reminder: include only legal conclusions supported by provided findings/law.]`;
  }
  return `${base}\n[Light context reminder: adapt this scaffold to provided inputs without asserting unverified facts.]`;
}

function confidenceNote(params: {
  mode: DraftTemplateRequest["template_mode"];
  authorities: Authority[];
  hasFindings: boolean;
  hasLaw: boolean;
}): string {
  if (params.mode === "blank_scaffold") {
    return "Completeness note: blank scaffold generated. This is structure-only and intentionally non-case-specific.";
  }
  if (!params.hasFindings || !params.hasLaw) {
    return "Completeness note: guided scaffold generated without full contextual inputs; add findings and law text for stronger contextual prompts.";
  }
  if (params.authorities.length < 2) {
    return "Completeness note: lightly contextualized scaffold generated with limited supporting authority retrieval.";
  }
  return "Completeness note: lightly contextualized scaffold generated with supporting authority references for reviewer orientation.";
}

export async function runDraftTemplate(env: Env, input: unknown): Promise<DraftTemplateResponse> {
  const parsed = draftTemplateRequestSchema.parse(input);
  const taxonomy = getActiveTaxonomyConfig();
  const { caseType: definition, resolution } = resolveCaseTypeTemplate(parsed.case_type);
  const caseAssistant = await maybeRetrieveAuthorities(env, parsed);
  const authorities = caseAssistant ? [...caseAssistant.similar_cases, ...caseAssistant.relevant_law] : [];

  const templateSections = taxonomy.canonical_sections.map((section) => {
    const sectionName = section.name;
    const hints = definition.template_section_hints[sectionName] ?? [];
    const prompts = parsed.template_mode === "blank_scaffold" ? [] : [...section.base_prompts, ...hints];
    const citationIds =
      parsed.template_mode === "blank_scaffold"
        ? []
        : authorities
            .filter((item) => sectionName === "Conclusions of Law" ? /conclusions? of law|reasoning|analysis/i.test(item.section_label) : true)
            .slice(0, 2)
            .map((item) => item.citation_id);

    const placeholder = modeSpecificPlaceholder(section.placeholder, parsed.template_mode, sectionName);
    return {
      section_name: sectionName,
      section_purpose: section.purpose,
      placeholder_text: `${placeholder}${contextualSuffix({
        findingsText: parsed.findings_text,
        lawText: parsed.law_text,
        mode: parsed.template_mode
      })}`,
      drafting_prompts: prompts,
      example_structure: parsed.template_mode === "blank_scaffold" ? undefined : section.example_structure,
      citation_ids: citationIds
    };
  });

  const suggestedPrompts = [
    ...definition.focus_prompts,
    ...definition.default_issue_tags.map((tag) => `Consider issue tag: ${tag}`),
    ...(definition.index_code_mappings.length > 0 ? [`Configured Index Code mappings: ${definition.index_code_mappings.join(", ")}`] : []),
    ...(definition.rules_hints.length > 0 ? [`Configured Rules hints: ${definition.rules_hints.join(", ")}`] : []),
    ...(definition.ordinance_hints.length > 0 ? [`Configured Ordinance hints: ${definition.ordinance_hints.join(", ")}`] : []),
    ...(parsed.index_codes.length > 0 ? [`Address Index Codes: ${parsed.index_codes.join(", ")}`] : []),
    ...(parsed.rules_sections.length > 0 ? [`Address Rules sections: ${parsed.rules_sections.join(", ")}`] : []),
    ...(parsed.ordinance_sections.length > 0 ? [`Address Ordinance sections: ${parsed.ordinance_sections.join(", ")}`] : [])
  ];

  const response = {
    case_type: definition.id,
    template_title: `${definition.label} Decision Template`,
    template_mode: parsed.template_mode,
    template_sections: templateSections,
    suggested_prompts: suggestedPrompts.slice(0, 10),
    guidance_notes: [
      definition.description,
      ...taxonomy.template_defaults.guardrails,
      ...resolution.warnings,
      ...(parsed.template_mode === "lightly_contextualized"
        ? ["Context was applied in scaffold form only; this output is not final adjudicative text."]
        : [])
    ].slice(0, 8),
    supporting_authorities: parsed.template_mode === "blank_scaffold" ? [] : authorities.slice(0, 8),
    confidence_or_completeness_note: confidenceNote({
      mode: parsed.template_mode,
      authorities,
      hasFindings: Boolean(parsed.findings_text?.trim()),
      hasLaw: Boolean(parsed.law_text?.trim())
    }),
    citations: parsed.template_mode === "blank_scaffold" || !caseAssistant ? [] : caseAssistant.citations.slice(0, 8)
  };

  return draftTemplateResponseSchema.parse(response);
}
