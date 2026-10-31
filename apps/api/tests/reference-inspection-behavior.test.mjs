import test from 'node:test';
import assert from 'node:assert/strict';
import { memoryD1 } from './helpers/d1-memory.mjs';
import { loadTs } from './helpers/load-ts.mjs';
import { catalogPayload } from './helpers/catalog-fixture.mjs';

const { rebuildLegalReferences } = loadTs('src/services/legal-references.ts');
const { handleInspectReferences } = loadTs('src/routes/admin-references.ts');
const { legalReferenceInspectResponseSchema } = loadTs('../../packages/shared/src/index.ts');

for (const anchor of ['reviewed-source#page-2', null]) {
  test(`reference inspection returns strict public samples without writes (${anchor ? 'anchored' : 'no anchor'})`, async () => {
    const { DB, sqlite } = memoryD1();
    const env = { DB };
    try {
      const payload = catalogPayload('Inspection fixture');
      payload.index_codes[0].source_page_anchor = anchor;
      payload.rules_sections[0].page_anchor = anchor;
      payload.ordinance_sections[0].page_anchor = anchor;
      const ruleBody = 'Synthetic rule text for bounded inspection. '.repeat(30);
      const ordinanceBody = 'Synthetic ordinance text for bounded inspection. '.repeat(30);
      payload.rules_sections[0].body_text = ruleBody;
      payload.ordinance_sections[0].body_text = ordinanceBody;
      await rebuildLegalReferences(env, payload);

      const changesBefore = sqlite.prepare('SELECT total_changes() AS count').get().count;
      const stateBefore = sqlite.prepare('SELECT * FROM legal_catalog_state').get();
      const storedRuleBefore = sqlite.prepare('SELECT body_text FROM legal_rules_sections').get().body_text;
      const storedOrdinanceBefore = sqlite.prepare('SELECT body_text FROM legal_ordinance_sections').get().body_text;
      sqlite.exec('PRAGMA query_only = ON');
      const response = await handleInspectReferences(new Request('https://preview.example/admin/references'), env);
      const body = await response.json();
      assert.equal(response.status, 200, body.error);
      const result = legalReferenceInspectResponseSchema.parse(body);
      assert.equal(result.summary.index_code_count, 1);
      assert.equal(result.summary.ordinance_section_count, 1);
      assert.equal(result.summary.rules_section_count, 1);
      const index = result.samples.index_codes[0];
      assert.equal(index.code_identifier, '100');
      assert.equal(index.source_page_anchor, anchor);
      assert.deepEqual(index.linked_rules_sections, ['1.2']);
      assert.deepEqual(index.linked_ordinance_sections, ['37.8']);
      assert.equal(typeof index.reserved, 'boolean');
      assert.equal(typeof index.legacy_pre_1002, 'boolean');
      assert.equal('linked_rules_sections_json' in body.samples.index_codes[0], false);
      assert.equal('linked_ordinance_sections_json' in body.samples.index_codes[0], false);
      assert.equal(result.samples.rules_sections[0].page_anchor, anchor);
      assert.equal(result.samples.ordinance_sections[0].page_anchor, anchor);
      assert.equal(result.samples.rules_sections[0].body_text, storedRuleBefore.slice(0, 500));
      assert.equal(result.samples.ordinance_sections[0].body_text, storedOrdinanceBefore.slice(0, 500));
      assert.equal('source_page_anchor' in body.samples.rules_sections[0], false);
      assert.equal('source_page_anchor' in body.samples.ordinance_sections[0], false);
      assert.equal(sqlite.prepare('SELECT total_changes() AS count').get().count, changesBefore);
      assert.deepEqual(sqlite.prepare('SELECT * FROM legal_catalog_state').get(), stateBefore);
      assert.equal(sqlite.prepare('SELECT body_text FROM legal_rules_sections').get().body_text, storedRuleBefore);
      assert.equal(sqlite.prepare('SELECT body_text FROM legal_ordinance_sections').get().body_text, storedOrdinanceBefore);
      const internalColumnLeak = structuredClone(result);
      internalColumnLeak.samples.index_codes[0].linked_rules_sections_json = '[]';
      assert.equal(legalReferenceInspectResponseSchema.safeParse(internalColumnLeak).success, false);
    } finally {
      sqlite.close();
    }
  });
}
