import {z} from 'zod';
import {sha256Bytes} from '../lib/digests';
import {observeVerifiedRestorationSource} from './restoration-source';
import type {RestorationIdentity} from './restoration-job-control';

const hash = z.string().regex(/^[a-f0-9]{64}$/);
const opaque = z.string().min(1).max(1024);
const key = opaque.refine(value => new TextEncoder().encode(value).length <= 1024 &&
  !/[\u0000-\u001f\u007f]/.test(value) && new TextDecoder().decode(new TextEncoder().encode(value)) === value);
export const restorationByteAuditRecordSchema = z.object({
  documentId:z.string().min(1).max(200), claimToken:z.string().uuid(),
  sourceSha256:hash, sourceBytes:z.number().int().min(1).max(1_000_000),
  originalKey:key, originalVersion:opaque, copyKey:key, copyVersion:opaque, copyEtag:opaque,
  receiptGenerationId:z.string().min(1).max(200).regex(/^[A-Za-z0-9_.:-]+$/), receiptDocumentRevision:z.number().int().positive(),
  receiptSha256:hash, currentKey:key, currentVersion:opaque, currentEtag:opaque
}).strict();
export type RestorationByteAuditRecord = z.infer<typeof restorationByteAuditRecordSchema>;
// A held original has no parsed text generation. Its copy remains the current
// original pointer; the audit must not require or invent a processed-text key.
export const restorationHeldByteAuditRecordSchema = restorationByteAuditRecordSchema
  .omit({currentKey:true,currentVersion:true,currentEtag:true})
  .extend({receiptGenerationId:z.literal('initial')}).strict();
export type RestorationHeldByteAuditRecord = z.infer<typeof restorationHeldByteAuditRecordSchema>;
export const restorationByteAuditSourceSchema = z.object({ownerId:z.string().uuid(), resourceId:z.string().uuid(),
  planSha256:hash, resourcePlanSha256:hash, sourceManifestSha256:hash, catalogSha256:hash,
  partitionIndex:z.number().int().min(0).max(31)}).strict();
const receiptSchema = z.object({
  version:z.number().int().min(1).max(3), ownerId:z.string().uuid(), planSha256:hash,
  partitionIndex:z.number().int().min(0).max(31), resourceId:z.string().uuid(),
  documentId:z.string(), claimToken:z.string().uuid(), sourceSha256:hash,
  sourceBytes:z.number().int().positive(), generationId:z.string(), documentRevision:z.number().int().positive(),
  humanApproval:z.literal(false), copy:z.object({
    ownerId:z.string().uuid(), planSha256:hash, sourceSha256:hash, sourceBytes:z.number().int().positive(),
    originalVersion:opaque, targetKey:key, targetVersion:opaque, targetEtag:opaque,
    disposition:z.enum(['created_verified','existing_verified','observed_verified'])
  }).strict()
}).passthrough();

async function boundedRead(bucket:Pick<R2Bucket,'get'>, objectKey:string, maximum:number) {
  try {
    const object = await bucket.get(objectKey);
    if (!object || !Number.isSafeInteger(object.size) || object.size < 1 || object.size > maximum ||
      !object.version || !object.etag) throw Error('source_byte_audit_object_identity');
    const bytes = new Uint8Array(await object.arrayBuffer());
    if (bytes.byteLength !== object.size || bytes.byteLength > maximum) throw Error('source_byte_audit_object_size');
    return {object, bytes};
  } catch {
    // R2 errors can include private object keys. The Workflow's immutable claim
    // must remain unknown/held; this read helper never retries or emits the key.
    throw Error('source_byte_audit_object_read_unconfirmed');
  }
}

/** Byte evidence only. The caller must independently pin and reconcile D1
 * owner/schema/journals/jobs/receipts/documents and corpus revision around it.
 * This capability has no database and exposes GET only for every bucket.
 */
export async function auditRestorationSourceBytes(input:{
  source:RestorationIdentity; record:RestorationByteAuditRecord;
  originals:Pick<R2Bucket,'get'>; target:Pick<R2Bucket,'get'>; proofs:Pick<R2Bucket,'get'>;
}) {
  const parsed = restorationByteAuditRecordSchema.safeParse(input.record);
  if (!parsed.success || input.originals === input.target || input.originals === input.proofs || input.target === input.proofs)
    throw Error('source_byte_audit_record_or_binding_alias');
  const row = parsed.data, source = input.source;
  if (!restorationByteAuditSourceSchema.safeParse(source).success)
    throw Error('source_byte_audit_source_identity');
  const documentSha256 = await sha256Bytes(new TextEncoder().encode(row.documentId));
  const proofKey = `restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${documentSha256}/${row.receiptGenerationId}.json`;
  const stored = await boundedRead(input.proofs, proofKey, 131072);
  if (await sha256Bytes(stored.bytes) !== row.receiptSha256 || stored.object.customMetadata?.ownerid !== source.ownerId ||
    stored.object.customMetadata.plansha256 !== source.planSha256 || stored.object.customMetadata.proofsha256 !== row.receiptSha256)
    throw Error('source_byte_audit_receipt_bytes_or_owner');
  let value:unknown;
  try { value = JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(stored.bytes)); }
  catch { throw Error('source_byte_audit_receipt_encoding'); }
  const receipt = receiptSchema.safeParse(value);
  if (!receipt.success) throw Error('source_byte_audit_receipt_shape');
  const proof = receipt.data, copy = proof.copy;
  if (proof.ownerId !== source.ownerId || proof.planSha256 !== source.planSha256 || proof.resourceId !== source.resourceId ||
    proof.partitionIndex !== source.partitionIndex || proof.documentId !== row.documentId || proof.claimToken !== row.claimToken ||
    proof.generationId !== row.receiptGenerationId || proof.documentRevision !== row.receiptDocumentRevision ||
    proof.sourceSha256 !== row.sourceSha256 || proof.sourceBytes !== row.sourceBytes ||
    copy.ownerId !== source.ownerId || copy.planSha256 !== source.planSha256 || copy.sourceSha256 !== row.sourceSha256 ||
    copy.sourceBytes !== row.sourceBytes || copy.originalVersion !== row.originalVersion || copy.targetKey !== row.copyKey ||
    copy.targetVersion !== row.copyVersion || copy.targetEtag !== row.copyEtag)
    throw Error('source_byte_audit_receipt_identity');
  const observed = await observeVerifiedRestorationSource(input.originals, input.target, {
    ownerId:source.ownerId, planSha256:source.planSha256, partitionIndex:source.partitionIndex,
    originalKey:row.originalKey, originalVersion:row.originalVersion, sourceSha256:row.sourceSha256, sourceBytes:row.sourceBytes
  });
  if (observed.targetKey !== row.copyKey || observed.targetVersion !== row.copyVersion || observed.targetEtag !== row.copyEtag)
    throw Error('source_byte_audit_copy_changed');
  const current = await boundedRead(input.target, row.currentKey, 1_000_000);
  if (current.object.version !== row.currentVersion || current.object.etag !== row.currentEtag ||
    current.bytes.byteLength !== row.sourceBytes || await sha256Bytes(current.bytes) !== row.sourceSha256)
    throw Error('source_byte_audit_current_changed');
  return {state:'original_copy_current_bytes_verified' as const, documentSha256, sourceSha256:row.sourceSha256,
    sourceBytes:row.sourceBytes, originalVersion:row.originalVersion, copyVersion:row.copyVersion,
    currentVersion:row.currentVersion, receiptSha256:row.receiptSha256,
    databaseBookendsVerified:false as const, fullIntegrityOrPublicationApproval:false as const};
}

/** Original/copy evidence for a separately declared held-unreadable member.
 * Membership and unchanged D1 holds are independently checked by the caller.
 * No source bytes are decoded and no generated text or publication is credited.
 */
export async function auditHeldRestorationSourceBytes(input:{
  source:RestorationIdentity; record:RestorationHeldByteAuditRecord;
  originals:Pick<R2Bucket,'get'>; target:Pick<R2Bucket,'get'>; proofs:Pick<R2Bucket,'get'>;
}) {
  const parsed=restorationHeldByteAuditRecordSchema.safeParse(input.record);
  if(!parsed.success||!restorationByteAuditSourceSchema.safeParse(input.source).success||
    input.originals===input.target||input.originals===input.proofs||input.target===input.proofs)
    throw Error('held_byte_audit_record_or_binding_identity');
  const row=parsed.data,source=input.source,documentSha256=await sha256Bytes(new TextEncoder().encode(row.documentId));
  const proofKey=`restoration/${source.ownerId}/${source.planSha256}/${source.partitionIndex}/${documentSha256}/initial.json`;
  const stored=await boundedRead(input.proofs,proofKey,131072);
  if(await sha256Bytes(stored.bytes)!==row.receiptSha256||stored.object.customMetadata?.ownerid!==source.ownerId||
    stored.object.customMetadata.plansha256!==source.planSha256||stored.object.customMetadata.proofsha256!==row.receiptSha256)
    throw Error('held_byte_audit_receipt_bytes_or_owner');
  let value:unknown;
  try{value=JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(stored.bytes));}
  catch{throw Error('held_byte_audit_receipt_encoding');}
  const parsedReceipt=receiptSchema.extend({generationId:z.literal('initial'),
    disposition:z.literal('held_unreadable_original_preserved'),currentSourceKey:key,
    currentSourceVersion:opaque,currentSourceEtag:opaque}).safeParse(value);
  if(!parsedReceipt.success)throw Error('held_byte_audit_receipt_shape');
  const proof=parsedReceipt.data,copy=proof.copy;
  if(proof.ownerId!==source.ownerId||proof.planSha256!==source.planSha256||proof.resourceId!==source.resourceId||
    proof.partitionIndex!==source.partitionIndex||proof.documentId!==row.documentId||proof.claimToken!==row.claimToken||
    proof.documentRevision!==row.receiptDocumentRevision||proof.sourceSha256!==row.sourceSha256||proof.sourceBytes!==row.sourceBytes||
    copy.ownerId!==source.ownerId||copy.planSha256!==source.planSha256||copy.sourceSha256!==row.sourceSha256||
    copy.sourceBytes!==row.sourceBytes||copy.originalVersion!==row.originalVersion||copy.targetKey!==row.copyKey||
    copy.targetVersion!==row.copyVersion||copy.targetEtag!==row.copyEtag||proof.currentSourceKey!==row.copyKey||
    proof.currentSourceVersion!==row.copyVersion||proof.currentSourceEtag!==row.copyEtag)
    throw Error('held_byte_audit_receipt_identity');
  try{
    const observed=await observeVerifiedRestorationSource(input.originals,input.target,{
      ownerId:source.ownerId,planSha256:source.planSha256,partitionIndex:source.partitionIndex,
      originalKey:row.originalKey,originalVersion:row.originalVersion,sourceSha256:row.sourceSha256,sourceBytes:row.sourceBytes});
    if(observed.targetKey!==row.copyKey||observed.targetVersion!==row.copyVersion||observed.targetEtag!==row.copyEtag)
      throw Error('held_byte_audit_copy_changed');
  }catch{throw Error('held_byte_audit_original_or_copy_unconfirmed');}
  return{state:'held_original_copy_bytes_verified' as const,documentSha256,sourceSha256:row.sourceSha256,
    sourceBytes:row.sourceBytes,originalVersion:row.originalVersion,copyVersion:row.copyVersion,receiptSha256:row.receiptSha256,
    parsedTextVerified:false as const,searchPublication:false as const,humanApproval:false as const,
    databaseBookendsVerified:false as const,fullIntegrityOrPublicationApproval:false as const};
}
