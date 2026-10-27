import type { AuthoredSection, ParsedDocument } from '../lib/types';
import type { TaxonomyConfig } from '@beedle/shared';
import { getActiveTaxonomyConfig } from './template-config';
export const TAXONOMY_INFERENCE_VERSION = 'taxonomy-evidence-2';
export interface TaxonomySuggestion {
    caseTypeId: string;
    caseTypeLabel: string;
    confidence: number;
    fallback: boolean;
    signals: string[];
    inferenceVersion: string;
    configVersion: string;
    reviewStatus: 'unreviewed' | 'reviewed_for_inference';
    confidenceKind: 'uncalibrated_evidence_score';
    previousSuggestion?: {
        caseTypeId: string | null;
        caseTypeLabel: string | null;
        confidence: number | null;
        fallback: boolean | null;
    };
}
type Input = {
    jurisdiction?:string;
    title: string;
    citation: string;
    sections: AuthoredSection[];
    metadata: ParsedDocument['extractedMetadata'];
};
const normalize = (value: string) => value.normalize('NFKC').toLowerCase().replace(/[_\s]+/g, ' ').trim();
const tokens = (value: string) => normalize(value).match(/[\p{L}\p{N}]+/gu) ?? [];
function fallback(taxonomy: TaxonomyConfig, reason: string): TaxonomySuggestion {
    const entry = taxonomy.case_types.find(row => row.id === taxonomy.default_case_type_id && ['general', 'unknown'].includes(row.id));
    return { caseTypeId: entry?.id ?? 'unknown', caseTypeLabel: entry?.label ?? 'Unknown', confidence: 0, fallback: true, signals: [reason],
        inferenceVersion: TAXONOMY_INFERENCE_VERSION, configVersion: taxonomy.version, reviewStatus: taxonomy.inference_policy.review_status, confidenceKind: 'uncalibrated_evidence_score' };
}
const phraseText = (value: string) => ' ' + value.split(/[.!?;\n]+/).map(part => tokens(part).join(' ')).join(' | ') + ' ';
function phraseSignal(text: string, phrase: string) {
    const words = tokens(phrase);
    if (!words.length || words.length > 24)
        return { positive: false, negated: false, multi: false };
    const needle = words.join(' '), haystack = text;
    let from = 0, negated = false;
    while (from < haystack.length) {
        const at = haystack.indexOf(' ' + needle + ' ', from);
        if (at < 0)
            break;
        const prefix = haystack.slice(Math.max(0, at - 100), at);
        const before = prefix.slice(prefix.lastIndexOf('|') + 1).trim().split(' ').slice(-6);
        // Conservative lexical abstention, not a legal determination. A denial of
        // relief AFTER the type phrase is still evidence of that type of proceeding.
        if (before.some(word => ['not', 'no', 'without', 'neither', 'unrelated', 'excluding'].includes(word)))
            negated = true;
        else
            return { positive: true, negated, multi: words.length > 1 };
        from = at + needle.length + 1;
    }
    return { positive: false, negated, multi: words.length > 1 };
}
const reference = (value: string) => normalize(value).replace(/^(?:rules?|ordinance)\s+/, '').replace(/\s+/g, '');
const indexCode = (value: string) => normalize(value).replace(/^ic[-\s]*/, '').replace(/\s+/g, '');
export function inferTaxonomySuggestion(params: Input, taxonomy: TaxonomyConfig = getActiveTaxonomyConfig()): TaxonomySuggestion {
    if (!taxonomy.case_types.length)
        return fallback(taxonomy, 'missing_case_type_config');
    if (!taxonomy.case_types.some(row => row.id === taxonomy.default_case_type_id && ['general', 'unknown'].includes(row.id)))
        return fallback(taxonomy, 'invalid_unknown_fallback');
    if (taxonomy.inference_policy.review_status !== 'reviewed_for_inference')
        return fallback(taxonomy, 'taxonomy_not_reviewed_for_corpus');
    if(!params.jurisdiction||!taxonomy.inference_policy.jurisdictions.some(value=>normalize(value)===normalize(params.jurisdiction!)))return fallback(taxonomy,'jurisdiction_outside_reviewed_taxonomy');
    const parts = [params.title, params.citation];
    let length = parts.join('\n').length;
    if(length>240_000)return fallback(taxonomy,'taxonomy_text_limit');
    if([params.metadata.indexCodes,params.metadata.rulesSections,params.metadata.ordinanceSections].some(values=>values.length>200))return fallback(taxonomy,'taxonomy_reference_limit');
    for (const section of params.sections) {
        for (const text of [section.heading, ...section.paragraphs.map(row => row.text)]) {
            length += text.length + 1;
            if (length > 240000)
                return fallback(taxonomy, 'taxonomy_text_limit');
            parts.push(text);
        }
    }
    const body = phraseText(parts.join('\n')), titleText = phraseText(params.title), codes = new Set(params.metadata.indexCodes.slice(0, 200).map(indexCode));
    const rules = new Set(params.metadata.rulesSections.slice(0, 200).map(reference)), ordinance = new Set(params.metadata.ordinanceSections.slice(0, 200).map(reference));
    const candidates = taxonomy.case_types.filter(entry => entry.id !== taxonomy.default_case_type_id).map(entry => {
        let aliasScore = 0, negated = false;
        for (const alias of new Set([entry.label, ...entry.aliases].map(normalize))) {
            const title = phraseSignal(titleText, alias), text = phraseSignal(body, alias);
            negated ||= title.negated || text.negated;
            if (title.positive)
                aliasScore = Math.max(aliasScore, title.multi ? 0.65 : 0.35);
            else if (text.positive)
                aliasScore = Math.max(aliasScore, text.multi ? 0.55 : 0.2);
        }
        const index = entry.index_code_mappings.some(value => codes.has(indexCode(value)));
        const rule = entry.rules_hints.some(value => rules.has(reference(value))), ord = entry.ordinance_hints.some(value => ordinance.has(reference(value)));
        const score = Number(Math.min(0.95, Math.max(aliasScore, index ? 0.65 : 0) + (rule ? 0.1 : 0) + (ord ? 0.1 : 0)).toFixed(2));
        return { entry, score, index, signals: [...(aliasScore ? ['positive_type_phrase'] : []), ...(index ? ['exact_index_mapping'] : []), ...(rule ? ['exact_rule_hint'] : []), ...(ord ? ['exact_ordinance_hint'] : []), ...(negated ? ['negated_type_phrase_ignored'] : [])] };
    }).sort((a, b) => b.score - a.score || a.entry.id.localeCompare(b.entry.id));
    if (candidates.filter(row => row.index).length > 1)
        return fallback(taxonomy, 'conflicting_index_mappings');
    const best = candidates[0], second = candidates[1];
    if (best?.index && best.signals.includes('negated_type_phrase_ignored') && !best.signals.includes('positive_type_phrase'))
        return fallback(taxonomy, 'negated_structured_conflict');
    if (!best || best.score < taxonomy.inference_policy.min_score)
        return fallback(taxonomy, candidates.some(row => row.signals.includes('negated_type_phrase_ignored')) ? 'negated_or_weak_signal' : 'insufficient_evidence');
    if (second && best.score - second.score < taxonomy.inference_policy.min_margin - 1e-9)
        return fallback(taxonomy, 'ambiguous_evidence');
    return { ...fallback(taxonomy, 'unused'), caseTypeId: best.entry.id, caseTypeLabel: best.entry.label, confidence: best.score, fallback: false, signals: best.signals };
}
/** Present old suggestions for comparison without silently rewriting metadata,
 * legal approval or provenance. Unknown/corrupt/old policy is an abstention. */
export function storedTaxonomySuggestion(value: Record<string, unknown>, taxonomy: TaxonomyConfig = getActiveTaxonomyConfig(), jurisdiction?:string): TaxonomySuggestion {
    const previous = { caseTypeId: typeof value.caseTypeId === 'string' ? value.caseTypeId.slice(0, 200) : null,
        caseTypeLabel: typeof value.caseTypeLabel === 'string' ? value.caseTypeLabel.slice(0, 200) : null,
        confidence: typeof value.confidence === 'number' && Number.isFinite(value.confidence) ? value.confidence : null,
        fallback: typeof value.fallback === 'boolean' ? value.fallback : value.fallback === 1 ? true : value.fallback === 0 ? false : null };
    const current = value.inferenceVersion === TAXONOMY_INFERENCE_VERSION && value.configVersion === taxonomy.version;
    if (current && previous.fallback === true && previous.confidence === 0 && previous.caseTypeId === taxonomy.default_case_type_id)
        return fallback(taxonomy, 'unknown_or_weak_stored_suggestion');
    if (current && jurisdiction && taxonomy.inference_policy.jurisdictions.some(value=>normalize(value)===normalize(jurisdiction)) && taxonomy.inference_policy.review_status === 'reviewed_for_inference' && previous.fallback === false && value.reviewStatus === 'reviewed_for_inference' && previous.confidence !== null && previous.confidence >= taxonomy.inference_policy.min_score && previous.confidence <= 0.95) {
        const entry = taxonomy.case_types.find(row => row.id === previous.caseTypeId && row.id !== taxonomy.default_case_type_id);
        if (entry)
            return { ...fallback(taxonomy, 'unused'), caseTypeId: entry.id, caseTypeLabel: entry.label, confidence: previous.confidence, fallback: false, reviewStatus: 'reviewed_for_inference', signals: ['retained_versioned_suggestion'] };
    }
    const result = fallback(taxonomy, current ? 'unknown_or_weak_stored_suggestion' : 'legacy_or_changed_taxonomy_policy');
    return { ...result, previousSuggestion: previous };
}
