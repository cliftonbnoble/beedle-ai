import assert from 'node:assert/strict';
import { catalogPayload } from '../helpers/catalog-fixture.mjs';
export async function readyIndexes(fixture) { let state = await fixture.request('/admin/search-index'); for (let n = 0; n < 40 && !state.readiness.ready; n++)
    state = await fixture.request('/admin/search-index/step', {}); assert.equal(state.readiness.ready, true, 'Search builders did not reach readiness'); }
export async function readyReviewer(fixture) { let state = await fixture.request('/admin/reviewer-index'); for (let n = 0; n < 40 && !state.ready; n++)
    state = await fixture.request('/admin/reviewer-index/step', {}); assert.equal(state.ready, true, 'Reviewer index did not reach readiness'); }
export async function ingestCase(fixture, label, { vectors = false, status = 201 } = {}) {
    const text = '# Index Codes\n\n100\n\n# Rules\n\nRule 1.2\n\n# Ordinance\n\nOrdinance 37.8\n\n# Findings of Fact\n\nThe tenant gave the landlord written repair notice about a roof leak. The landlord received the notice and disputed when repairs were completed.\n\n# Conclusions of Law\n\nThe Board reviewed the repair notice and the complete evidentiary record under Rule 1.2 and Ordinance 37.8.';
    return fixture.request('/ingest/decision', { fileType: 'decision_docx', jurisdiction: 'Test', title: `Rent Board case ${label}`, citation: 'T-' + label.toUpperCase(), performVectorUpsert: vectors, sourceFile: { filename: `received-${label.toLowerCase()}.md`, mimeType: 'text/markdown', bytesBase64: Buffer.from(text).toString('base64') } }, { status });
}
export async function reviewedCase(fixture, label, { approve = true, taxonomy = { fallback: false, confidence: 0.2 } } = {}) {
    const doc = await ingestCase(fixture, label);
    const path = '/admin/ingestion/documents/' + doc.documentId;
    await fixture.request('/__test/taxonomy', { documentId: doc.documentId, ...taxonomy });
    let detail = await fixture.request(path);
    detail = await fixture.request(path + '/metadata', { expectedRevision: detail.documentRevision, mutationId: crypto.randomUUID(), index_codes: ['100'], rules_sections: ['1.2'], ordinance_sections: ['37.8'], author_name: 'Erin E. Katayama', case_number: 'T-' + label.toUpperCase(), confirm_required_metadata: true });
    if (approve)
        await fixture.request(path + '/approve', { expectedRevision: detail.documentRevision });
    return doc.documentId;
}
export async function seedFixture(fixture) {
    await readyIndexes(fixture);
    await fixture.request('/admin/references/rebuild', catalogPayload('Synthetic integration'));
    const alpha = await reviewedCase(fixture, 'Alpha', { taxonomy: { fallback: true, confidence: 0.9 } }), beta = await reviewedCase(fixture, 'Beta'), staged = await reviewedCase(fixture, 'Gamma', { approve: false, taxonomy: { fallback: true, confidence: 0.9 } });
    await readyReviewer(fixture);
    const seedIds = { alpha, beta, staged };
    await fixture.request('/__test/control', { seedIds });
    return seedIds;
}
