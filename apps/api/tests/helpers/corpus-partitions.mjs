import {memoryD1} from './d1-memory.mjs';
import {loadTs} from './load-ts.mjs';
const {corpusPartitionBinding,corpusPartitionIndex}=loadTs('src/services/corpus-partitions.ts');
const {buildDocumentTextArtifactStatements}=loadTs('src/services/ingest.ts');
export async function createPartitionFixture(){
  const stores=Array.from({length:32},()=>memoryD1());const ids=Array(32).fill(null);
  for(let n=0;ids.some(id=>!id);n++){const id='synthetic-partition-'+n;const index=await corpusPartitionIndex(id);if(!ids[index])ids[index]=id;}
  for(let index=0;index<32;index++){
    const {DB,sqlite}=stores[index],id=ids[index];
    sqlite.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,
      ingestion_state,ingestion_completed_at,expected_chunk_count,document_revision,published_revision,searchable_at)
      VALUES(?,'decision_docx','Test',?,?,'source','https://example.invalid','now','now','preparing','now',1,1,1,'now')`).run(id,'Synthetic possession '+index,'Test-'+index);
    const artifacts=buildDocumentTextArtifactStatements({DB},{documentId:id,citation:'Test-'+index,sections:[{canonicalKey:'conclusions_of_law',heading:'Conclusions of Law',order:0,
      paragraphs:[{anchor:'p1',order:0,text:'The tenant retained possession of the dwelling after a disputed notice. The possession claim was considered.'}]}]});
    await DB.batch(artifacts.statements);sqlite.prepare("UPDATE documents SET ingestion_state='ready' WHERE id=?").run(id);
  }
  const manifest={version:1,partitionCount:32,readOnly:true,restoreOwnerId:'11111111-1111-4111-8111-111111111111',restorePlanSha256:'c'.repeat(64),sourceManifestSha256:'a'.repeat(64),catalogVersion:'legacy',catalogSha256:'b'.repeat(64),documents:32,publishedDocuments:32,
    partitions:stores.map(({sqlite},index)=>({index,binding:corpusPartitionBinding(index),documents:1,publishedDocuments:1,revision:sqlite.prepare('SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1').get().revision}))};
  const env={DB:stores[0].DB,CORPUS_PARTITIONS_JSON:JSON.stringify(manifest),SOURCE_PROXY_BASE_URL:'https://api.example',VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'test',VECTOR_LIFECYCLE_READY:'false',VECTOR_FILTERS_READY:'false'};
  for(let i=1;i<32;i++)env[corpusPartitionBinding(i)]=stores[i].DB;
  return{stores,env,ids,close(){for(const store of stores)store.sqlite.close();}};
}
