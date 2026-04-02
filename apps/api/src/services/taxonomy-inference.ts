import type { AuthoredSection, ParsedDocument } from "../lib/types";
import { getActiveTaxonomyConfig } from "./template-config";

export interface TaxonomySuggestion {
  caseTypeId: string;
  caseTypeLabel: string;
  confidence: number;
  fallback: boolean;
  signals: string[];
}

function lower(value: string | null | undefined): string {
  return (value || "").toLowerCase();
}

function countHits(text: string, items: string[]): number {
  const haystack = lower(text);
  return items.reduce((sum, item) => (item && haystack.includes(lower(item)) ? sum + 1 : sum), 0);
}

function sectionSignalText(sections: AuthoredSection[]): string {
  return sections.map((section) => `${section.heading}\n${section.paragraphs.map((p) => p.text).join("\n")}`).join("\n\n");
}

export function inferTaxonomySuggestion(params: {
  title: string;
  citation: string;
  sections: AuthoredSection[];
  metadata: ParsedDocument["extractedMetadata"];
}): TaxonomySuggestion {
  const taxonomy = getActiveTaxonomyConfig();
  if (taxonomy.case_types.length === 0) {
    return {
      caseTypeId: "general",
      caseTypeLabel: "General",
      confidence: 0.2,
      fallback: true,
      signals: ["missing_case_type_config"]
    };
  }
  const corpusText = `${params.title}\n${params.citation}\n${sectionSignalText(params.sections)}`;

  let best = taxonomy.case_types[0]!;
  let bestScore = -1;
  let bestSignals: string[] = [];

  for (const entry of taxonomy.case_types) {
    let score = 0;
    const signals: string[] = [];

    const aliasHits = countHits(corpusText, [entry.label, ...entry.aliases]);
    if (aliasHits > 0) {
      score += Math.min(0.35, aliasHits * 0.12);
      signals.push(`alias_hits:${aliasHits}`);
    }

    const indexOverlap = entry.index_code_mappings.filter((item) => params.metadata.indexCodes.includes(item)).length;
    if (indexOverlap > 0) {
      score += Math.min(0.3, indexOverlap * 0.18);
      signals.push(`index_overlap:${indexOverlap}`);
    }

    const ruleOverlap = entry.rules_hints.filter((item) => params.metadata.rulesSections.includes(item)).length;
    if (ruleOverlap > 0) {
      score += Math.min(0.2, ruleOverlap * 0.12);
      signals.push(`rules_overlap:${ruleOverlap}`);
    }

    const ordOverlap = entry.ordinance_hints.filter((item) => params.metadata.ordinanceSections.includes(item)).length;
    if (ordOverlap > 0) {
      score += Math.min(0.2, ordOverlap * 0.12);
      signals.push(`ordinance_overlap:${ordOverlap}`);
    }

    const promptHits = countHits(corpusText, entry.focus_prompts);
    if (promptHits > 0) {
      score += Math.min(0.15, promptHits * 0.03);
      signals.push(`focus_prompt_hits:${promptHits}`);
    }

    if (score > bestScore) {
      best = entry;
      bestScore = score;
      bestSignals = signals;
    }
  }

  const fallback = best.id === taxonomy.default_case_type_id && bestScore < 0.45;
  const confidence = Number(Math.max(0.2, Math.min(0.95, bestScore)).toFixed(2));

  return {
    caseTypeId: best.id,
    caseTypeLabel: best.label,
    confidence,
    fallback,
    signals: bestSignals.length > 0 ? bestSignals : ["weak_signal_fallback"]
  };
}
