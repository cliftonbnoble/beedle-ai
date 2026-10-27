import {readyReviewerQueue} from "./helpers/reviewer-queue.mjs";
import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { approveDecision } = loadTs('src/services/ingest.ts');
const { getIngestionDocumentDetail, listIngestionDocuments, updateIngestionMetadata, rejectIngestionDocument } = loadTs('src/services/admin-ingestion.ts');
const { handleDecisionApproval } = loadTs('src/routes/ingest.ts');
const { handleApproveIngestionDocument } = loadTs('src/routes/admin-ingestion.ts');
const { trustedDocumentSql } = loadTs('src/services/publication-policy.ts');
const { withDocumentRevision } = loadTs('src/services/document-revision.ts');

function setup() {
  const { DB, sqlite } = memoryD1();
  sqlite.exec(`INSERT INTO documents (id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,
    ingestion_state,ingestion_completed_at,expected_chunk_count,qc_has_index_codes,qc_has_rules_section,qc_has_ordinance_section,
    qc_passed,qc_required_confirmed,extraction_confidence,extraction_warnings_json,index_codes_json,rules_sections_json,ordinance_sections_json,reference_catalog_version)
    VALUES ('doc','decision_docx','Test','Decision','T1','source','https://example.invalid','now','now','ready','now',1,1,1,1,1,1,0.8,'[]','["100"]','["1.2"]','["37.8"]','legacy');`);
  const env = { DB, SOURCE_PROXY_BASE_URL: 'https://api.example' };
  const revision = () => sqlite.prepare('SELECT document_revision FROM documents WHERE id = ?').get('doc').document_revision;
  return { env, sqlite, revision };
}
function confirm(sqlite) { sqlite.exec('UPDATE documents SET qc_required_confirmed = 1'); }

test('every reviewer blocker prevents approval through both HTTP aliases on the reviewed revision', async () => {
  const cases = [
    ['reference_catalog_changed', "UPDATE documents SET reference_catalog_version='unverified'"],
    ['ingestion_not_ready', "UPDATE documents SET ingestion_state='legacy'"],
    ['not_decision_docx', "UPDATE documents SET file_type='law_pdf'"],
    ['rejected', "UPDATE documents SET rejected_at='now'"],
    ['already_approved', "UPDATE documents SET approved_at='now'"],
    ['qc_gate_not_passed', 'UPDATE documents SET qc_passed=0'],
    ['metadata_not_confirmed', 'UPDATE documents SET qc_required_confirmed=0'],
    ['critical_reference_exception_present', `UPDATE documents SET rules_sections_json='["Rule 10.10(c)(3)"]'`],
    ['unresolved_references_above_threshold', Array.from({length: 6}, (_,i) => `INSERT INTO document_reference_issues (id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at) VALUES ('i${i}','doc','rules_section','bad','bad','Unknown','error','now')`).join(';')],
    ['warnings_above_threshold', `UPDATE documents SET extraction_warnings_json='${JSON.stringify(Array(9).fill('warning'))}'`],
    ['extraction_confidence_below_threshold', 'UPDATE documents SET extraction_confidence=0.2'],
    ['missing_rules_detection', 'UPDATE documents SET qc_has_rules_section=0'],
    ['missing_ordinance_detection', 'UPDATE documents SET qc_has_ordinance_section=0']
  ];
  for (const [blocker, mutation] of cases) {
    const { env, sqlite, revision } = setup();
    try {
      sqlite.exec(mutation); if (blocker !== 'metadata_not_confirmed' && blocker !== 'already_approved') confirm(sqlite);
      const detail = await getIngestionDocumentDetail(env, 'doc');
      assert.ok(detail.approvalReadiness.blockers.includes(blocker), blocker);
      await readyReviewerQueue(env);
      const list = await listIngestionDocuments(env, { status: "all" });
      assert.ok(list.documents.find(row => row.id === "doc").approvalReadiness.blockers.includes(blocker), `list ${blocker}`);
      for (const handler of [handleDecisionApproval, handleApproveIngestionDocument]) {
        const request = new Request('https://api.example/approve', { method: 'POST', headers: {'content-type':'application/json'}, body: JSON.stringify({expectedRevision: revision()}) });
        const response = await handler(request, env, 'doc');
        assert.equal(response.status, 422, blocker);
        assert.ok((await response.json()).readiness.blockers.includes(blocker), blocker);
      }
    } finally { sqlite.close(); }
  }
});

test('approval records its policy and exact revision; missing or stale revision fails closed', async () => {
  const { env, sqlite, revision } = setup();
  try {
    for (const handler of [handleDecisionApproval, handleApproveIngestionDocument]) {
      const response = await handler(new Request('https://api.example/approve', {method:'POST',headers:{'content-type':'application/json'},body:'{}'}), env, 'doc');
      assert.equal(response.status, 400);
    }
    assert.equal((await approveDecision(env, 'doc', revision() + 1)).conflict, true);
    const expected = revision();
    assert.equal((await approveDecision(env, 'doc', expected)).approved, true);
    const row = sqlite.prepare('SELECT * FROM documents WHERE id=?').get('doc');
    assert.equal(row.approved_revision, expected); assert.equal(row.published_revision, expected);
    assert.equal(row.approval_policy_version, 'reviewer-policy-1');
    assert.ok(sqlite.prepare(`SELECT d.id FROM documents d WHERE ${trustedDocumentSql()}`).get());
    sqlite.exec("UPDATE documents SET approval_policy_version='retired-policy'");
    assert.equal(sqlite.prepare(`SELECT d.id FROM documents d WHERE ${trustedDocumentSql()}`).get(), undefined);
    await rejectIngestionDocument(env, 'doc', {expectedRevision:sqlite.prepare("SELECT document_revision FROM documents WHERE id=?").get('doc').document_revision,mutationId:crypto.randomUUID(),reason:'Reviewer rejected this evidence'});
    const rejected = sqlite.prepare('SELECT * FROM documents WHERE id=?').get('doc');
    assert.equal(rejected.approved_at, null); assert.equal(rejected.approved_revision, null); assert.equal(rejected.qc_required_confirmed, 0);
  } finally { sqlite.close(); }
});

test('intervening rejection, metadata edit, confirmation revocation, and new reference issue fence approval', async () => {
  for (const mutation of ["UPDATE documents SET rejected_at='now'", "UPDATE documents SET author_name='Changed'", 'UPDATE documents SET qc_required_confirmed=0', "INSERT INTO document_reference_issues (id,document_id,reference_type,raw_value,normalized_value,message,severity,created_at) VALUES ('i','doc','rules_section','bad','bad','Unknown','error','now')"]) {
    const { env, sqlite, revision } = setup();
    try {
      const prepare = env.DB.prepare.bind(env.DB); let injected = false;
      env.DB.prepare = sql => {
        if (!injected && sql.includes('UPDATE documents SET approved_at')) { injected = true; sqlite.exec(mutation); }
        return prepare(sql);
      };
      const result = await approveDecision(env, 'doc', revision());
      assert.equal(result.approved, false); assert.equal(result.conflict, true);
      assert.equal(sqlite.prepare('SELECT approved_at FROM documents').get().approved_at, null);
    } finally { sqlite.close(); }
  }
});

test('metadata and reference replacement is atomic, revokes approval, and confirms only the committed evidence', async () => {
  const { env, sqlite, revision } = setup();
  try {
    await approveDecision(env, 'doc', revision()); const old = revision();
    const result = await updateIngestionMetadata(env, 'doc', {expectedRevision:sqlite.prepare("SELECT document_revision FROM documents WHERE id=?").get('doc').document_revision,mutationId:crypto.randomUUID(),index_codes:['100'],rules_sections:['1.2'],ordinance_sections:['37.8'], author_name:'Reviewed author',confirm_required_metadata:true});
    assert.ok(result.documentRevision > old); assert.equal(result.approvedAt, null); assert.equal(result.qcRequiredConfirmed, 1);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_revision_guards').get().n, 0);
    const current = revision();
    await assert.rejects(withDocumentRevision(env, 'doc', old, [env.DB.prepare("UPDATE documents SET author_name='stale'")]), /Document changed/);
    assert.equal(revision(), current); assert.equal(sqlite.prepare('SELECT author_name FROM documents').get().author_name, 'Reviewed author');
    const batch = env.DB.batch.bind(env.DB);
    env.DB.batch = statements => batch([...statements, env.DB.prepare('INSERT INTO missing_table VALUES (1)')]);
    await assert.rejects(updateIngestionMetadata(env, 'doc', {expectedRevision:sqlite.prepare("SELECT document_revision FROM documents WHERE id=?").get('doc').document_revision,mutationId:crypto.randomUUID(),author_name:'Failed write',confirm_required_metadata:true}));
    assert.equal(revision(), current); assert.equal(sqlite.prepare('SELECT author_name FROM documents').get().author_name, 'Reviewed author');
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_revision_guards').get().n, 0);
  } finally { sqlite.close(); }
});
