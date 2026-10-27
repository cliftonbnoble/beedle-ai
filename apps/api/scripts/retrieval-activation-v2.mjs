import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
export async function convertLegacySelection(legacy, prepare, batchId, performVectorUpsert) {
  const rows = legacy.embeddingPayload?.rows ?? legacy.rows;
  if (!Array.isArray(rows) || !rows.length || rows.some(row => !row.documentId || !row.chunkId || !row.sourceText)) {
    throw new Error('Conversion requires a legacy embedding payload with complete document IDs, source chunk IDs, and text. Regenerate missing evidence before conversion.');
  }
  const key = row => JSON.stringify([row.documentId, row.sourceChunkId ?? row.chunkId]);
  const old = new Map(rows.map(row => [key(row), row]));
  if (old.size !== rows.length)
    throw new Error('Legacy payload contains duplicate chunks.');
  const plan = await prepare({
    schemaVersion: 2, batchId, performVectorUpsert,
    documentIds: [...new Set(rows.map(row => row.documentId))],
    selectedChunks: rows.map(row => ({ documentId: row.documentId, sourceChunkId: row.chunkId }))
  });
  if (plan.chunks.length !== rows.length)
    throw new Error('Converted selection differs; regenerate and review a fresh plan.');
  for (const chunk of plan.chunks) {
    const previous = old.get(key(chunk));
    const fields = ['sourceText', 'chunkType', 'retrievalPriority', 'citationAnchorStart', 'citationAnchorEnd', 'hasCanonicalReferenceAlignment'];
    if (!previous || fields.some(field => previous[field] !== chunk[field])) {
      throw new Error('Legacy evidence differs from current persisted evidence. Regenerate and review a fresh plan; nothing was published.');
    }
  }
  return plan;
}
export async function main(args = process.argv.slice(2)) {
  const { values, positionals } = parseArgs({
    args, allowPositionals: true, options: {
      batch: { type: 'string' }, documents: { type: 'string' }, file: { type: 'string' }, legacy: { type: 'string' },
      'database-only': { type: 'boolean', default: false }, 'dry-run': { type: 'boolean', default: false }
    }
  });
  const command = positionals[0];
  if (!['prepare', 'convert', 'write', 'rollback'].includes(command) || !values.file) {
    throw new Error('Usage: node scripts/retrieval-activation-v2.mjs prepare --batch NEW_ID --documents DOC_ID[,DOC_ID] --file NEW_PLAN.json [--database-only]\n  convert --batch NEW_ID --legacy OLD_EMBEDDING_PAYLOAD.json --file NEW_PLAN.json [--database-only]\n  write|rollback --file PLAN.json [--dry-run]\nAPI_BASE_URL defaults to http://127.0.0.1:8787. Prepare/convert never publish.');
  }
  const base = new URL(process.env.API_BASE_URL || 'http://127.0.0.1:8787');
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password)
    throw new Error('Invalid API_BASE_URL.');
  async function post(action, payload) {
    const response = await fetch(new URL(`/admin/retrieval/activation/${action}`, base), {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(300000)
    });
    const body = await response.json();
    if (!response.ok)
      throw new Error(`${action} failed (${response.status}): ${JSON.stringify(body)}`);
    return body;
  }
  if (command === 'prepare' || command === 'convert') {
    if (!values.batch)
      throw new Error('Use a fresh --batch ID for the reviewed plan.');
    const prepare = async (input) => (await post('prepare', input)).plan;
    const plan = command === 'convert'
      ? await convertLegacySelection(JSON.parse(await fs.readFile(values.legacy || '', 'utf8')), prepare, values.batch, !values['database-only'])
      : await prepare({ schemaVersion: 2, batchId: values.batch, documentIds: (values.documents || '').split(',').filter(Boolean), performVectorUpsert: !values['database-only'] });
    await fs.writeFile(values.file, `${JSON.stringify(plan, null, 2)}\n`, { flag: 'wx' });
    console.log(`Prepared ${values.file}. Review its source revisions, chunks, vector mode, and rollback targets before write. Existing files were preserved.`);
    return;
  }
  const plan = JSON.parse(await fs.readFile(values.file, 'utf8'));
  if (plan.schemaVersion !== 2)
    throw new Error('This command requires a reviewed v2 plan. Convert legacy evidence explicitly.');
  const result = await post(command, command === 'write' ? { plan, dryRun: values['dry-run'] } : {
    schemaVersion: 2, batchId: plan.batchId, digest: plan.digest, rollbackDigest: plan.rollbackDigest, dryRun: values['dry-run']
  });
  console.log(JSON.stringify(result, null, 2));
  if (result.state === 'retryable')
    console.log('Vectors are still pending. Retry the identical file; do not change its batch ID or contents.');
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
