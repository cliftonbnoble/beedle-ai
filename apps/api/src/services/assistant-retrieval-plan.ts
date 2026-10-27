import type { AssistantChatRequest, AssistantChatResponse, SearchRequest } from '@beedle/shared';
import { boundAssistantRetrievalQuery, assistantQuestionError } from '@beedle/shared';
import { canonicalizeJudgeName } from './judges';
import { normalizeFilterValue } from './legal-references';
import { RequestValidationError } from '../lib/http';
import { sha256Bytes } from '../lib/digests';
type Filters = SearchRequest['filters'];
type Previous = NonNullable<AssistantChatResponse['referenceContext']>;
export type AssistantRetrievalPlan = {
    query: string;
    queryTruncated: boolean;
    subjectQuery: string;
    filters: Filters;
    resolution: NonNullable<AssistantChatResponse['retrievalContext']>['resolution'];
    scopeChanged: boolean;
    clarification: string | null;
    preserveReferences: boolean;
};
const asciiLower = (value: string) => value.replace(/[A-Z]/g, char => char.toLowerCase());
const unique = (values: string[]) => [...new Set(values)].sort();
const same = (a: string[], b: string[], normalize: (value: string) => string) => JSON.stringify(unique(a.map(normalize))) === JSON.stringify(unique(b.map(normalize)));
export function prepareAssistantFilters(input: AssistantChatRequest): Filters {
    const chosen = { ...input.filters };
    const scopeCodes = [...(chosen.indexCodes || []), ...(chosen.indexCode ? [chosen.indexCode] : [])];
    const scopeJudges = [...(chosen.judgeNames || []), ...(chosen.judgeName ? [chosen.judgeName] : [])];
    const canonicalJudge = (name: string) => { const normalized = canonicalizeJudgeName(name); if (!normalized)
        throw new RequestValidationError('Judge filters must contain a name.'); return normalized; };
    if (input.indexCodes.length && scopeCodes.length && !same(input.indexCodes, scopeCodes, value => normalizeFilterValue('index_code', value)))
        throw new RequestValidationError('Conflicting index-code filters; supply one consistent scope.');
    if (input.judgeNames.length && scopeJudges.length && !same(input.judgeNames, scopeJudges, value => asciiLower(canonicalJudge(value))))
        throw new RequestValidationError('Conflicting judge filters; supply one consistent scope.');
    const codes = scopeCodes.length ? scopeCodes : input.indexCodes;
    const judges = scopeJudges.length ? scopeJudges : input.judgeNames;
    const documentIds = unique(chosen.documentIds || []);
    if (chosen.documentId && documentIds.length && !documentIds.includes(chosen.documentId))
        throw new RequestValidationError('The selected document conflicts with the selected document set.');
    if (chosen.documentId) {
        documentIds.splice(0, documentIds.length, chosen.documentId);
    }
    delete chosen.documentId;
    delete chosen.indexCode;
    delete chosen.judgeName;
    if (!codes.length && chosen.indexCodeOperator)
        throw new RequestValidationError('Choose index codes before choosing a code operator.');
    const filters: Filters = { ...chosen, approvedOnly: true, fileType: 'decision_docx' };
    if (codes.length) {
        filters.indexCodes = unique(codes);
        filters.indexCodeOperator = chosen.indexCodeOperator || 'and';
    }
    else
        delete filters.indexCodes;
    if (judges.length)
        filters.judgeNames = unique(judges.map(canonicalJudge));
    else
        delete filters.judgeNames;
    if (documentIds.length)
        filters.documentIds = documentIds;
    else
        delete filters.documentIds;
    if (filters.fromDate && filters.toDate && filters.fromDate > filters.toDate)
        throw new RequestValidationError('The start date must not follow the end date.');
    return filters;
}
export async function assistantScopeKey(filters: Filters) {
    const normalized = { ...filters, indexCodes: filters.indexCodes?.map(code => normalizeFilterValue('index_code', code)).sort(),
        judgeNames: filters.judgeNames?.map(asciiLower).sort() };
    const entries = Object.entries(normalized).filter(([, value]) => value !== undefined).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
    return sha256Bytes(new TextEncoder().encode(JSON.stringify({ version: 'assistant-scope-1', filters: Object.fromEntries(entries) })));
}
export function assistantSubjectQuery(question: string) {
    const compact = question.replace(/\s+/g, ' ').trim();
    const match = compact.match(/\b(?:have we(?: ever)?|did we|do we|has the board|has an alj)\s+(?:granted|denied|granted or denied|found|addressed|considered)?\s*(?:petitions?|cases?|decisions?)?\s*(?:involving|about|for|on|where|with)\s+(.+?)[?.!]*$/i);
    const bounded = boundAssistantRetrievalQuery(match?.[1] || compact);
    if (assistantQuestionError(bounded.query))
        throw new RequestValidationError('Add a specific issue with at least three letters or numbers for retrieval.');
    return bounded;
}
export function resolveAssistantRetrieval(question: string, filters: Filters, scopeKey: string, previous?: Previous, previousQuestion?: string): AssistantRetrievalPlan {
    const selected = filters.documentIds || [];
    const scopeChanged = Boolean(previous && previous.scopeKey !== scopeKey);
    const refs = previous?.referenceDocumentIds || [];
    let historySubject = '';
    if (previousQuestion) {
        try {
            historySubject = assistantSubjectQuery(previousQuestion).query;
        }
        catch { /* Unusable older questions cannot supply an antecedent. */ }
    }
    const subject = previous?.subjectQuery || historySubject;
    const base = { subjectQuery: subject, filters, scopeChanged, preserveReferences: false };
    const clarify = (message: string): AssistantRetrievalPlan => ({ ...base, query: '', queryTruncated: false, resolution: 'clarification', clarification: message });
    const ordinal = question.match(/\b(first|second|third|fourth|fifth|sixth|seventh|eighth|last|[1-8](?:st|nd|rd|th)?)\s+(?:cases?|decisions?|examples?|one)\b/i)
        || question.match(/\b(?:case|decision|example)\s*(?:number\s*)?#?\s*([1-8])\b/i)
        || question.match(/^(?:(?:what|how)\s+about\s+)?(?:the\s+)?(first|second|third|fourth|fifth|sixth|seventh|eighth|last|[1-8](?:st|nd|rd|th)?)\s*[?.!]*$/i);
    const ordinalWords = question.match(/\b(?:first|second|third|fourth|fifth|sixth|seventh|eighth|last|[1-8](?:st|nd|rd|th))\b/gi) || [];
    const caseNumbers = [...question.matchAll(/\b(?:case|decision|example)\s*(?:number\s*)?#?\s*([1-8])\b/gi)].map(match => match[1]);
    const ordinalNames = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth'];
    const ordinalNumber = (word: string) => word.toLowerCase() === 'last' ? refs.length : /^[1-8]/.test(word) ? Number(word[0]) : ordinalNames.indexOf(word.toLowerCase()) + 1;
    if (ordinal && new Set([...ordinalWords.map(ordinalNumber), ...caseNumbers.map(Number)]).size > 1)
        return clarify('Select the intended decisions explicitly to compare multiple numbered cases.');
    const singular = /\b(?:this case|that case|the case|this decision|that decision)\b/i.test(question)
        || (/\b(?:it|its)\b/i.test(question) && question.split(/\s+/).length <= 14);
    const plural = /\b(?:them|both cases|both decisions|these cases|those cases|these decisions|those decisions)\b/i.test(question);
    const generic = /^(?:why|how so|tell me more|more detail|continue|what happened next)[?.!\s]*$/i.test(question.trim());
    const more = /\b(?:more|other|additional|another)\s+(?:cases?|decisions?|examples?)\b/i.test(question);
    let targets: string[] = [];
    let resolution: AssistantRetrievalPlan['resolution'] = 'standalone';
    if (ordinal) {
        const key = ordinal[1]!.toLowerCase();
        const index = ordinalNumber(key) - 1;
        const target = refs[index];
        if (!target)
            return clarify('Select a decision or restate the issue; that numbered case is not available in the previous reference list.');
        if (selected.length && !selected.includes(target))
            return clarify('That numbered case conflicts with the focused decisions. Clear or change the focus first.');
        if (scopeChanged && !selected.includes(target))
            return clarify('The filters changed. Select the case again or restate the question under the current filters.');
        targets = [target];
        resolution = 'ordinal';
    }
    else if (more) {
        if (selected.length)
            return clarify('Clear the focused decisions before searching for other cases.');
        if (!subject)
            return clarify('Restate the issue to search for other decisions.');
        resolution = 'more_cases';
    }
    else if (singular || generic || plural) {
        if (!previous && !selected.length && /^is it (?:legal|possible|permissible|necessary|true)\b/i.test(question)) {
            const bounded = assistantSubjectQuery(question);
            return { ...base, query: bounded.query, queryTruncated: bounded.truncated, subjectQuery: bounded.query, filters, resolution: 'standalone', clarification: null };
        }
        if (!subject)
            return clarify('Restate the issue or choose a decision so the follow-up has a clear subject.');
        if (selected.length) {
            if ((singular || generic) && selected.length !== 1)
                return clarify('Choose one focused decision or name its reference number.');
            if (plural && selected.length < 2)
                return clarify('Only one decision is focused; select the intended cases or restate the question.');
            targets = selected;
            resolution = 'selected_case';
        }
        else if (previous && !scopeChanged) {
            if (plural)
                targets = previous.activeDocumentIds.length > 1 ? previous.activeDocumentIds : refs;
            else if (previous.activeDocumentIds.length === 1)
                targets = previous.activeDocumentIds;
            else
                return clarify('Choose a decision or name its reference number; the follow-up could refer to more than one case.');
            if (!targets.length)
                return clarify('No earlier decision is available to resolve that reference. Restate the issue.');
            if (/\bboth\b/i.test(question) && targets.length !== 2)
                return clarify('Select the two intended decisions; the preceding reference list does not identify a pair.');
            resolution = plural ? 'previous_issue' : 'pronoun';
        }
        else
            return clarify(scopeChanged ? 'The filters changed. Select a case or restate the issue under the current filters.' : 'Select a case or restate the question; earlier answer text is not a reliable case identifier.');
    }
    if (resolution === 'standalone') {
        const bounded = assistantSubjectQuery(question);
        return { ...base, query: bounded.query, queryTruncated: bounded.truncated, subjectQuery: bounded.query, filters, resolution, clarification: null };
    }
    if (!subject)
        return clarify('Restate the issue for this case.');
    const residual = question.replace(/\b(?:first|second|third|fourth|fifth|sixth|seventh|eighth|last|[1-8](?:st|nd|rd|th)?|case|cases|decision|decisions|example|examples|one|it|its|this|that|these|those|them|both|what|how|why|about|the|did|does|was|is|and|also|tell|me|more|continue)\b/gi, ' ').replace(/\s+/g, ' ').trim();
    const bounded = boundAssistantRetrievalQuery(`${residual} ${subject}`);
    const effective = { ...filters, ...(targets.length ? { documentIds: unique(targets) } : {}) };
    return { ...base, query: bounded.query, queryTruncated: bounded.truncated, filters: effective, resolution, clarification: null,
        preserveReferences: Boolean(previous && !scopeChanged && targets.length && targets.every(id => refs.includes(id))) };
}
