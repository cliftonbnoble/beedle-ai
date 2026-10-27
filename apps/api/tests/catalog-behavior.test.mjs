import test from 'node:test';
import assert from 'node:assert/strict';
import { memoryD1 } from './helpers/d1-memory.mjs';
import { loadTs } from './helpers/load-ts.mjs';
import { catalogPayload } from './helpers/catalog-fixture.mjs';
const { rebuildLegalReferences, loadReferenceLookups, buildDocumentReferenceValidationStatements, backfillReferenceValidation, verifyCitations } = loadTs('src/services/legal-references.ts');
const { catalogState, restoreLegalCatalog } = loadTs('src/services/legal-catalog.ts');
const { withDocumentRevision } = loadTs('src/services/document-revision.ts');
const { ingestDocumentBytes, approveDecision } = loadTs('src/services/ingest.ts');
const { updateIngestionMetadata, getIngestionDocumentDetail } = loadTs('src/services/admin-ingestion.ts');
const { trustedDocumentSql } = loadTs('src/services/publication-policy.ts');
async function setup() {
  const { DB, sqlite } = memoryD1();
  const objects = new Map();
  const env = {
    DB, R2_PUBLIC_BASE_URL: 'https://example.invalid', SOURCE_PROXY_BASE_URL: 'https://api.example', VECTOR_NAMESPACE: 'catalog-test',
    SOURCE_BUCKET: { async put(key, bytes) { objects.set(key, new Uint8Array(bytes)); }, async get(key) { const bytes = objects.get(key); return bytes ? { size: bytes.length, arrayBuffer: async () => bytes.slice().buffer } : null; }, async delete(key) { objects.delete(key); } }
  };
  const first = await rebuildLegalReferences(env, catalogPayload('First'));
  const visible = () => JSON.stringify({ codes: sqlite.prepare('SELECT * FROM legal_index_codes ORDER BY id').all(), rules: sqlite.prepare('SELECT * FROM legal_rules_sections ORDER BY id').all(), ordinance: sqlite.prepare('SELECT * FROM legal_ordinance_sections ORDER BY id').all(), state: sqlite.prepare('SELECT * FROM legal_catalog_state').get() });
  const doc = async () => {
    const text = '# Index Codes\n\n100\n\n# Rules\n\nRule 1.2\n\n# Ordinance\n\nOrdinance 37.8\n\n# Findings of Fact\n\nThe tenant gave written notice of needed repairs and the landlord received it. The parties disputed when repairs had been completed.\n\n# Conclusions of Law\n\nThe remedy requires review of the evidence under Rule 1.2 and Ordinance 37.8.';
    const result = await ingestDocumentBytes(env, { title: 'Catalog validation appeal', citation: 'C-16', jurisdiction: 'Test', fileType: 'decision_docx', performVectorUpsert: false, sourceFile: { filename: 'appeal.md', mimeType: 'text/markdown' } }, new TextEncoder().encode(text));
    const detail = await updateIngestionMetadata(env, result.documentId, {expectedRevision:sqlite.prepare("SELECT document_revision FROM documents WHERE id=?").get(result.documentId).document_revision,mutationId:crypto.randomUUID(), index_codes: ['100'], rules_sections: ['1.2'], ordinance_sections: ['37.8'], confirm_required_metadata: true });
    return { id: result.documentId, detail };
  };
  return { env, sqlite, first, visible, doc };
}
test('catalog staging and publication failures preserve the complete previous projection at every batch boundary', async () => {
  for (const stop of [1, 2, 3, 4]) {
    const { env, sqlite, visible } = await setup();
    try {
      const before = visible();
      const batch = env.DB.batch.bind(env.DB);
      let calls = 0;
      env.DB.batch = async (statements) => {
        calls++; assert.equal(visible(), before); if (calls === stop)
          return batch([...statements, env.DB.prepare('INSERT INTO missing_table VALUES (1)')]); return batch(statements);
      };
      const payload = catalogPayload('Replacement');
      payload.index_codes = Array.from({ length: 121 }, (_, i) => ({ code_identifier: String(100 + i) }));
      await assert.rejects(rebuildLegalReferences(env, payload));
      assert.equal(visible(), before);
      assert.ok(sqlite.prepare("SELECT COUNT(*) AS n FROM legal_catalog_versions WHERE state IN ('failed','ready')").get().n >= 1);
    }
    finally {
      sqlite.close();
    }
  }
});
test('concurrent catalog publishers have one winner; retained versions restore with monotonic epochs', async () => {
  const { env, sqlite, first } = await setup();
  try {
    const outcomes = await Promise.allSettled([rebuildLegalReferences(env, catalogPayload('B')), rebuildLegalReferences(env, catalogPayload('C'))]);
    assert.equal(outcomes.filter(row => row.status === 'fulfilled').length, 1);
    assert.equal((await catalogState(env)).epoch, 2);
    await assert.rejects(restoreLegalCatalog(env, { versionId: first.publication.version, expectedEpoch: 1 }), /changed/);
    const restored = await restoreLegalCatalog(env, { versionId: first.publication.version, expectedEpoch: 2 });
    assert.equal(restored.epoch, 3);
    assert.match(sqlite.prepare('SELECT body_text FROM legal_rules_sections').get().body_text, /First/);
    assert.throws(() => sqlite.exec('DELETE FROM legal_rules_sections'), /read_only/);
    assert.throws(() => sqlite.exec("UPDATE legal_catalog_rows SET payload_json='{}'"), /immutable/);
  }
  finally {
    sqlite.close();
  }
});
test('lost publication acknowledgment preserves the committed catalog and identical rebuild is a no-op', async () => {
  const { env, sqlite } = await setup();
  try {
    const batch = env.DB.batch.bind(env.DB);
    let lost = false;
    env.DB.batch = async (statements) => {
      const result = await batch(statements); if (!lost && statements.some(row => row.sql.includes('INSERT INTO legal_catalog_guards'))) {
        lost = true;
        throw new Error('lost acknowledgment');
      } return result;
    };
    const payload = catalogPayload('New');
    await assert.rejects(rebuildLegalReferences(env, payload), /lost acknowledgment/);
    assert.equal((await catalogState(env)).epoch, 2);
    const replay = await rebuildLegalReferences(env, payload);
    assert.equal(replay.publication.replayed, true);
    assert.equal(replay.publication.epoch, 2);
    assert.match(sqlite.prepare('SELECT body_text FROM legal_rules_sections').get().body_text, /New/);
  }
  finally {
    sqlite.close();
  }
});
test('a catalog switch holds old approval until deliberate reference refresh and preserves prior validation', async () => {
  const { env, sqlite, first, doc } = await setup();
  try {
    const { id, detail } = await doc();
    assert.equal((await approveDecision(env, id, detail.documentRevision)).approved, true);
    assert.ok(sqlite.prepare(`SELECT d.id FROM documents d WHERE ${trustedDocumentSql()}`).get());
    await rebuildLegalReferences(env, catalogPayload('Replacement'));
    assert.equal(sqlite.prepare(`SELECT d.id FROM documents d WHERE ${trustedDocumentSql()}`).get(), undefined);
    assert.ok((await getIngestionDocumentDetail(env, id)).approvalReadiness.blockers.includes('reference_catalog_changed'));
    const accepted = await backfillReferenceValidation(env);
    const {stepMaintenanceJob}=loadTs('src/services/maintenance-jobs.ts');
    const refreshed = await stepMaintenanceJob(env,accepted.jobId);
    assert.equal(refreshed.counts.completed, 1);
    assert.equal(sqlite.prepare('SELECT approved_at FROM documents').get().approved_at, null);
    assert.ok(sqlite.prepare('SELECT * FROM document_reference_validation_history WHERE catalog_version=?').get(first.publication.version));
    const checked = await updateIngestionMetadata(env, id, {expectedRevision:sqlite.prepare("SELECT document_revision FROM documents WHERE id=?").get(id).document_revision,mutationId:crypto.randomUUID(), confirm_required_metadata: true });
    assert.equal((await approveDecision(env, id, checked.documentRevision)).approved, true);
  }
  finally {
    sqlite.close();
  }
});
test('pinned validation and multi-query readers cannot silently mix catalog versions', async () => {
  const { env, sqlite, doc } = await setup();
  try {
    const { id, detail } = await doc();
    const lookups = await loadReferenceLookups(env);
    await rebuildLegalReferences(env, catalogPayload('Replacement'));
    const before = sqlite.prepare('SELECT * FROM documents').get();
    const statements = await buildDocumentReferenceValidationStatements(env, id, { indexCodes: ['100'], rulesSections: ['1.2'], ordinanceSections: ['37.8'] }, lookups);
    await assert.rejects(withDocumentRevision(env, id, detail.documentRevision, statements), /catalog changed/);
    assert.deepEqual(sqlite.prepare('SELECT * FROM documents').get(), before);
    const prepare = env.DB.prepare;
    let switched = false;
    env.DB.prepare = sql => {
      const statement = prepare(sql); if (sql.includes('FROM legal_ordinance_sections')) {
        const all = statement.all;
        statement.all = async function() {
          const result = await all.call(this); if (!switched) {
            switched = true;
            await rebuildLegalReferences(env, catalogPayload('Third'));
          } return result;
        };
      } return statement;
    };
    await assert.rejects(verifyCitations(env, { citations: ['1.2', '37.8'] }), /catalog changed/);
  }
  finally {
    sqlite.close();
  }
});
test('empty or declared-incomplete catalogs fail before changing the active version', async () => {
  const { env, sqlite, visible } = await setup();
  try {
    const before = visible();
    const empty = catalogPayload();
    empty.rules_sections = [];
    await assert.rejects(rebuildLegalReferences(env, empty), /nonempty/);
    const incomplete = catalogPayload();
    incomplete.coverage_report = { rules: { parser_used: 'true_text', expected_section_count: 2, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 } };
    await assert.rejects(rebuildLegalReferences(env, incomplete), /coverage/);
    assert.equal(visible(), before);
  }
  finally {
    sqlite.close();
  }
});
test('approval is fenced when the catalog changes after readiness was inspected', async () => {
  const { env, sqlite, doc } = await setup();
  try {
    const { id, detail } = await doc();
    const prepare = env.DB.prepare;
    let switched = false;
    env.DB.prepare = sql => {
      const statement = prepare(sql); if (sql.includes('UPDATE documents SET approved_at')) {
        const bind = statement.bind;
        statement.bind = function(...args) {
          const bound = bind.apply(this, args); const run = bound.first; bound.first = async function() {
            if (!switched) {
              switched = true;
              await rebuildLegalReferences(env, catalogPayload('Changed'));
            } return run.call(this);
          }; return bound;
        };
      } return statement;
    };
    const result = await approveDecision(env, id, detail.documentRevision);
    assert.equal(result.approved, false);
    assert.equal(result.conflict, true);
    assert.equal(sqlite.prepare('SELECT approved_at FROM documents').get().approved_at, null);
  }
  finally {
    sqlite.close();
  }
});
test('reference refresh transaction failure keeps old validation and its document revision intact', async () => {
  const { env, sqlite, doc } = await setup();
  try {
    await doc();
    await rebuildLegalReferences(env, catalogPayload('Replacement'));
    const before = JSON.stringify({ doc: sqlite.prepare('SELECT * FROM documents').get(), refs: sqlite.prepare('SELECT * FROM document_reference_links ORDER BY id').all(), history: sqlite.prepare('SELECT * FROM document_reference_validation_history').all() });
    const accepted=await backfillReferenceValidation(env);
    const {stepMaintenanceJob}=loadTs('src/services/maintenance-jobs.ts');
    const batch = env.DB.batch.bind(env.DB);
    env.DB.batch = statements => batch([...statements, env.DB.prepare('INSERT INTO missing_table VALUES (1)')]);
    assert.equal((await stepMaintenanceJob(env,accepted.jobId)).state,'retryable');
    assert.equal(JSON.stringify({ doc: sqlite.prepare('SELECT * FROM documents').get(), refs: sqlite.prepare('SELECT * FROM document_reference_links ORDER BY id').all(), history: sqlite.prepare('SELECT * FROM document_reference_validation_history').all() }), before);
  }
  finally {
    sqlite.close();
  }
});
test('pre-catalog document generation manifests remain restorable with explicitly unverified old validation', async () => {
  const { env, sqlite, doc } = await setup();
  try {
    const { id } = await doc();
    const { reprocessIngestionDocument } = loadTs('src/services/admin-ingestion.ts');
    const { restoreDocumentGeneration } = loadTs('src/services/document-generations.ts');
    const { generationTables } = loadTs('src/services/generation-tables.ts');
    const { sha256Bytes } = loadTs('src/lib/digests.ts');
    const current = await reprocessIngestionDocument(env, id);
    const retained = current.reprocessArtifacts.previousGenerationId;
    const old = {};
    for (const [table, columns] of Object.entries(generationTables)) {
      const rows = sqlite.prepare(`SELECT ${columns.filter(column => !['catalog_version','edition_id','edition_provision_id','edition_body_sha256','applicability_status'].includes(column)).join(',')} FROM staged_${table} WHERE generation_id=? ORDER BY id`).all(retained);
      old[table] = { count: rows.length, sha256: await sha256Bytes(new TextEncoder().encode(JSON.stringify(rows))) };
    }
    sqlite.prepare("UPDATE document_generations SET manifest_version=1,manifest_json=?,snapshot_json=json_remove(snapshot_json,'$.reference_catalog_version','$.reference_edition_context_json') WHERE id=?").run(JSON.stringify(old), retained);
    for (const table of ['staged_document_reference_links', 'staged_document_reference_issues'])
      sqlite.prepare(`UPDATE ${table} SET catalog_version='unverified' WHERE generation_id=?`).run(retained);
    sqlite.prepare("UPDATE staged_document_reference_links SET edition_id=NULL,edition_provision_id=NULL,edition_body_sha256=NULL,applicability_status='unknown' WHERE generation_id=?").run(retained);
    await restoreDocumentGeneration(env, id, retained, current.documentRevision);
    const restored = await getIngestionDocumentDetail(env, id);
    assert.equal(restored.approvedAt, null);
    assert.ok(restored.approvalReadiness.blockers.includes('reference_catalog_changed'));
    assert.ok(restored.chunks.length);
  }
  finally {
    sqlite.close();
  }
});

test('Part-distinct rules remain separate and bare validation cannot choose the first Part', async () => {
  const {env,sqlite}=await setup();
  try {
    const payload=catalogPayload();payload.rules_sections=[
      {part:'IV',section_number:'4.5(a)',body_text:'Part IV requires notice and an evidentiary hearing.'},
      {part:'V',section_number:'4.5(a)',body_text:'Part V concerns a different filing procedure.'},
      {part:'VI',section_number:'4.5(a)',body_text:'Part VI must not revive a previously ambiguous bare key.'}
    ];
    await rebuildLegalReferences(env,payload);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM legal_rules_sections').get().n,3);
    const lookups=await loadReferenceLookups(env);
    assert.equal(lookups.rulesByBare.has('4.5(a)'),false);
    const {validateReferencesAgainstNormalized}=loadTs('src/services/legal-references.ts');
    const checked=await validateReferencesAgainstNormalized(env,{indexCodes:[],rulesSections:['Rule IV-4.5(a)','Rule VII-4.5(a)','4.5(a)'],ordinanceSections:['Ordinance Part IV - 37.8']});
    assert.deepEqual(checked.unknownRules,['Rule VII-4.5(a)','4.5(a)']);
    assert.deepEqual(checked.unknownOrdinance,['Ordinance Part IV - 37.8']);
    const statements=await buildDocumentReferenceValidationStatements(env,'unused',{indexCodes:[],rulesSections:['4.5(a)'],ordinanceSections:[]},lookups);
    assert.equal(statements.find(s=>s.sql.includes('INSERT INTO document_reference_links')).args[5],0);
  }finally{sqlite.close();}
});
