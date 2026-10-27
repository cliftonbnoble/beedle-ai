import type { Env } from '../lib/types';
import type { SearchRequest } from '@beedle/shared';
import type { AssistantDecision } from './assistant-evidence';
import { buildSearchScope, keywordCandidateTerms, normalizeChunkTypeLabel } from './search-query-analysis';
import { indexedLexicalSearch } from './search-indexed-lexical';
import { meaningfulLexicalTokens } from './search-text';
import { chooseSnippetForTargets } from './search-scoring';
export async function eligibleAssistantDocuments(env: Env, request: SearchRequest, ids: string[]) {
    if (!ids.length)
        return [];
    const { where, params } = buildSearchScope(request, 'trusted_only');
    return (await env.DB.prepare(`SELECT d.id FROM json_each(?) selected CROSS JOIN documents d ${where} AND d.id=selected.value ORDER BY d.id`)
        .bind(JSON.stringify(ids), ...params).all<{
        id: string;
    }>()).results.map(row => row.id);
}
export async function addAssistantPassages(env: Env, request: SearchRequest, question: string, decisions: AssistantDecision[]) {
    if (!decisions.length)
        return decisions;
    const { where, params } = buildSearchScope(request, 'trusted_only');
    const focus = meaningfulLexicalTokens(question).filter(term => Array.from(term).length >= 3).slice(0, 8);
    const terms = focus.length ? focus : keywordCandidateTerms(request.query).filter(term => Array.from(term).length >= 3);
    const rows = await indexedLexicalSearch(env, where, params, terms.length ? terms : [request.query], 96, decisions.map(row => row.documentId));
    return decisions.map(decision => {
        const known = new Set([decision.primaryAuthorityPassage?.chunkId, decision.supportingFactPassage?.chunkId, decision.matchedPassage?.chunkId]);
        const candidates = rows.filter(row => row.documentId === decision.documentId && !known.has(row.chunkId)).map(row => {
            env.SEARCH_WORK_BUDGET?.consume('scoringOperations');
            const lower = row.chunkText.toLowerCase();
            return { row, rank: (row.lexicalRank || 0) + focus.filter(term => lower.includes(term.toLowerCase())).length * 4 };
        }).sort((a, b) => b.rank - a.rank || a.row.chunkId.localeCompare(b.row.chunkId));
        const selected: typeof candidates = [];
        const sections = new Set<string>();
        for (const candidate of candidates) {
            if (!sections.has(candidate.row.sectionLabel)) {
                sections.add(candidate.row.sectionLabel);
                selected.push(candidate);
            }
            if (selected.length === 2)
                break;
        }
        for (const candidate of candidates) {
            if (selected.length === 2)
                break;
            if (!selected.includes(candidate))
                selected.push(candidate);
        }
        return { ...decision, additionalPassages: selected.map(({ row }) => ({ chunkId: row.chunkId,
                snippet: chooseSnippetForTargets(row.chunkText, terms.length ? terms : [request.query], 360).slice(0, 360), sectionLabel: row.sectionLabel,
                sectionHeading: row.sectionLabel, citationAnchor: row.citationAnchor, paragraphAnchor: row.paragraphAnchor,
                chunkType: normalizeChunkTypeLabel(row.sectionLabel), score: row.lexicalRank || 0 })) };
    });
}
