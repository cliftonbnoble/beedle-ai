import test from "node:test";
import assert from "node:assert/strict";
import { loadTs } from "./helpers/load-ts.mjs";
import { memoryD1 } from "./helpers/d1-memory.mjs";
import { pdfFixture } from "./helpers/pdf-fixture.mjs";

const { publishedDocumentSql, trustedDocumentSql, trustLabelSql } = loadTs("src/services/publication-policy.ts");
const { buildSearchScope } = loadTs("src/services/search-query-analysis.ts");
const { searchRequestSchema } = loadTs("../../packages/shared/src/index.ts");
const { handleGetSource } = loadTs("src/routes/source.ts");
const { getDecisionRetrievalPreview } = loadTs("src/services/retrieval-foundation.ts");
const { handleDecisionRetrievalChunks } = loadTs("src/routes/retrieval-debug.ts");
const { fetchUploadedDocResults } = loadTs("src/services/case-assistant.ts");
const { ingestDocumentBytes, buildDocumentTextArtifactStatements } = loadTs("src/services/ingest.ts");
const { search } = loadTs("src/services/search.ts");

function environment(DB) {
  const objects = new Map();
  return { DB, R2_PUBLIC_BASE_URL: "https://example.invalid", SOURCE_PROXY_BASE_URL: "https://api.example",
    SOURCE_BUCKET: { async put(key, bytes, options) { objects.set(key, { body: bytes, ...options }); }, async get(key) { return objects.get(key) || { body: "Original" }; }, async delete(key) { objects.delete(key); } },
    VECTOR_NAMESPACE: "test", AI_EMBEDDING_MODEL: "test"
  };
}

async function addDocument(env, id, { state = "ready", published = true, approved = true, rejected = false, revision = 1, publishedRevision = revision, approvedRevision = revision } = {}) {
  await env.DB.prepare(`INSERT INTO documents (id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,
    ingestion_state,ingestion_completed_at,expected_chunk_count,document_revision,published_revision,approved_revision,searchable_at,approved_at,rejected_at,qc_required_confirmed,approval_policy_version,reference_catalog_version)
    VALUES (?,'decision_docx','Test',? ,?,'source','https://example.invalid','now','now',?,'now',1,?,?,?,?,?,?,1,'reviewer-policy-1','legacy')`)
    .bind(id, id, id, "preparing", revision, published ? publishedRevision : null, approved ? approvedRevision : null, published ? "now" : null, approved ? "now" : null, rejected ? "now" : null).run();
  const artifacts = buildDocumentTextArtifactStatements(env, { documentId: id, citation: id, sections: [{ canonicalKey: "conclusions_of_law", heading: "Conclusions of Law", order: 0, paragraphs: [{ anchor: "p1", order: 0, text: "The tenant retained possession of the dwelling after a disputed notice." }] }] });
  await env.DB.batch(artifacts.statements);
  await env.DB.prepare("UPDATE documents SET ingestion_state = ? WHERE id = ?").bind(state, id).run();
}

test("lifecycle matrix agrees across search scopes, trust labels, source downloads, previews, and uploaded research", async () => {
  const { DB, sqlite } = memoryD1(); const env = environment(DB);
  try {
    const states = [
      ["approved-unactivated", {}, true, true], ["published-provisional", { approved: false }, true, false],
      ["activated-unapproved", { approved: false }, true, false],
      ["staged", { published: false }, false, false], ["partial", { state: "preparing" }, false, false],
      ["needs-review", { state: "needs_review" }, false, false], ["legacy", { state: "legacy" }, false, false],
      ["rejected", { rejected: true }, false, false], ["stale-publication", { revision: 2, publishedRevision: 1 }, false, false],
      ["stale-approval", { revision: 2, approvedRevision: 1 }, true, false]
    ];
    for (const [id, options] of states) await addDocument(env, id, options);
    sqlite.exec(`INSERT INTO retrieval_activation_batches VALUES ('batch','now',1,1,'digest','rollback');
      INSERT INTO retrieval_search_chunks (chunk_id,batch_id,document_id,title,citation,source_file_ref,source_link,section_label,paragraph_anchor,citation_anchor,chunk_text,chunk_type,retrieval_priority,active,created_at)
      SELECT 'activated-chunk','batch',d.id,d.title,d.citation,'source',d.source_link,c.section_label,c.paragraph_anchor,c.citation_anchor,c.chunk_text,'conclusions_of_law','high',1,'now'
      FROM documents d JOIN document_chunks c ON c.document_id=d.id WHERE d.id='activated-unapproved';`);
    for (const [id, , published, trusted] of states) {
      const publicRow = sqlite.prepare(`SELECT d.id FROM documents d WHERE id=? AND ${publishedDocumentSql()}`).get(id);
      const trustRow = sqlite.prepare(`SELECT d.id FROM documents d WHERE id=? AND ${trustedDocumentSql()}`).get(id);
      assert.equal(Boolean(publicRow), published, `${id} publication`);
      assert.equal(Boolean(trustRow), trusted, `${id} trust`);
      assert.equal(sqlite.prepare(`SELECT ${trustLabelSql()} AS value FROM documents d WHERE id=?`).get(id).value, Number(trusted));
      const request = Object.assign(new Request(`https://api.example/source/${id}`), { params: { documentId: id } });
      assert.equal((await handleGetSource(request, env)).status, published ? 200 : 404, `${id} source`);
      const preview = await getDecisionRetrievalPreview(env, id);
      assert.equal(Boolean(preview), published, `${id} preview`);
      if (preview) assert.equal(preview.document.corpusTier, trusted ? "trusted" : "provisional", `${id} reader review status`);
      assert.equal((await fetchUploadedDocResults(env, [id])).length > 0, trusted, `${id} uploaded research`);
    }
    for (const corpusMode of ["trusted_only", "trusted_plus_provisional"]) {
      const parsed = searchRequestSchema.parse({ query: "possession", corpusMode, filters: { approvedOnly: false } });
      const scope = buildSearchScope(parsed, corpusMode);
      const actual = sqlite.prepare(`SELECT d.id FROM documents d ${scope.where}`).all(...scope.params).map(row => row.id).sort();
      const expected = states.filter(row => corpusMode === "trusted_only" ? row[3] : row[2]).map(row => row[0]).sort();
      assert.deepEqual(actual, expected);
      const response = await search(env, parsed);
      assert.ok(response.results.every(row => expected.includes(row.documentId)));
      assert.deepEqual([...new Set(response.results.map(row => row.documentId))].sort(), expected);
      assert.equal(response.corpusMode, corpusMode);
      if (corpusMode === "trusted_only") assert.ok(response.results.every(row => row.corpusTier === "trusted"));
    }
    const inspection = Object.assign(new Request("https://api.example/admin/ingestion/legacy/source"), { params: { documentId: "legacy" } });
    assert.equal((await handleGetSource(inspection, env, true)).status, 200);
    assert.equal((await getDecisionRetrievalPreview(env, "legacy", { inspection: true })).document.corpusTier, "held");
  } finally { sqlite.close(); }
});

test("actual ingest becomes ready only after the source and artifact count commit; law is provisional", async () => {
  const { DB, sqlite } = memoryD1(); const env = environment(DB);
  try {
    const input = { title: "Law", citation: "Law", jurisdiction: "Test", fileType: "law_pdf", performVectorUpsert: false, sourceFile: { filename: "law.pdf", mimeType: "application/pdf" } };
    const result = await ingestDocumentBytes(env, input, pdfFixture());
    const row = sqlite.prepare("SELECT * FROM documents WHERE id=?").get(result.documentId);
    assert.equal(row.ingestion_state, "ready"); assert.equal(row.published_revision, row.document_revision);
    assert.equal(row.expected_chunk_count, sqlite.prepare("SELECT COUNT(*) AS n FROM document_chunks WHERE document_id=?").get(row.id).n);
    assert.equal(row.approved_revision, null); assert.equal(row.qc_required_confirmed, 0);
    env.SOURCE_BUCKET.put = async () => { throw new Error("simulated R2 failure"); };
    await assert.rejects(ingestDocumentBytes(env, input, pdfFixture()), /simulated R2 failure/);
    assert.equal(sqlite.prepare("SELECT COUNT(*) AS n FROM documents").get().n, 1);
  } finally { sqlite.close(); }
});

test('source and preview readers recheck publication after asynchronous reads', async () => {
  for (const reader of ['source','fallback','preview','preview-republished']) {
    const {DB,sqlite}=memoryD1(); const env=environment(DB);
    try {
      await addDocument(env,'race');
      const revoke=()=>sqlite.prepare('UPDATE documents SET searchable_at=NULL WHERE id=?').run('race');
      if(reader==='source'||reader==='fallback')env.SOURCE_BUCKET.get=async()=>{revoke();return reader==='source'?{body:'Old source'}:null;};
      else if(reader==='preview-republished') { const prepare=DB.prepare;DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes('WHERE id=? AND document_revision=?') && sql.includes('AS corpusTier')){const first=statement.first;statement.first=async function(){sqlite.prepare("UPDATE documents SET title='Newly published revision' WHERE id='race'").run();sqlite.prepare("UPDATE documents SET published_revision=document_revision WHERE id='race'").run();return first.call(this);};}return statement;};}
      else {const prepare=DB.prepare;DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes('FROM document_sections s')){const all=statement.all;statement.all=async function(){const result=await all.call(this);revoke();return result;};}return statement;};}
      if(reader.startsWith('preview'))assert.equal(await getDecisionRetrievalPreview(env,'race'),null);
      else {
        const request=Object.assign(new Request('https://api.example/source/race'),{params:{documentId:'race'}});
        assert.equal((await handleGetSource(request,env)).status,404,reader);
      }
    }finally{sqlite.close();}
  }
});

test('reader review status rejects an approval revoked during a read without a document revision change', async () => {
  const {DB,sqlite}=memoryD1();const env=environment(DB);
  try {
    await addDocument(env,'trust-race');
    const revision=sqlite.prepare("SELECT document_revision FROM documents WHERE id='trust-race'").get().document_revision;
    const prepare=DB.prepare;
    DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes('FROM document_sections s')){const all=statement.all;statement.all=async function(){const result=await all.call(this);sqlite.prepare("UPDATE documents SET approved_at=NULL,approved_revision=NULL WHERE id='trust-race'").run();return result;};}return statement;};
    await assert.rejects(getDecisionRetrievalPreview(env,'trust-race'),/review status changed/);
    assert.equal(sqlite.prepare("SELECT document_revision FROM documents WHERE id='trust-race'").get().document_revision,revision);
  } finally {sqlite.close();}
});

test('inspection fallback cannot combine old source metadata with new stored text', async () => {
  const {DB,sqlite}=memoryD1();const env=environment(DB);
  try {
    await addDocument(env,'inspection-race');
    env.SOURCE_BUCKET.get=async()=>{sqlite.prepare("UPDATE documents SET title='Changed title' WHERE id='inspection-race'").run();return null;};
    const request=Object.assign(new Request('https://api.example/admin/ingestion/inspection-race/source'),{params:{documentId:'inspection-race'}});
    assert.equal((await handleGetSource(request,env,true)).status,409);
  }finally{sqlite.close();}
});

test('reader previews identify a stable corpus revision and reject a mid-read corpus change', async () => {
  const {DB,sqlite}=memoryD1(); const env=environment(DB);
  try {
    await addDocument(env,'reader-revision');
    const request=new Request('https://api.example/decisions/reader-revision/chunks');
    const response=await handleDecisionRetrievalChunks(request,env,'reader-revision');
    assert.equal(response.status,200);
    assert.equal((await response.json()).corpusRevision,String(sqlite.prepare('SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1').get().revision));
    assert.equal(response.headers.get('cache-control'),'private, no-store');
    const prepare=DB.prepare;
    DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes('FROM document_sections s')){const all=statement.all;statement.all=async function(){const result=await all.call(this);sqlite.exec('UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1');return result;};}return statement;};
    assert.equal((await handleDecisionRetrievalChunks(request,env,'reader-revision')).status,409);
    assert.equal((await handleDecisionRetrievalChunks(request,env,'missing')).status,404);
  }finally{sqlite.close();}
});
