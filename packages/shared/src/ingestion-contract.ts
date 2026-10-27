import { z } from 'zod';
// V1 describes the receipt payload already persisted by M22. It adds no new wire fields.
const id = z.string().min(1).max(200);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
export const ingestionReceiptResultSchemaV1 = z.object({
    ingestionRequestId: digest.optional(),
    sourceIdentity: z.object({
        sha256: digest, recordId: id, sourceRevision: z.number().int().safe().positive(), predecessorDocumentId: id.nullable(), identicalSourceCount: z.number().int().safe().nonnegative(), identicalSourceDocumentIds: z.array(id).max(10000)
    }).strict().optional(),
    documentId: id, qc: z.object({ hasIndexCodes: z.boolean(), hasRulesSection: z.boolean(), hasOrdinanceSection: z.boolean() }).strict(),
    sourceLink: z.string().min(1).max(4096), chunkCount: z.number().int().safe().nonnegative(), searchable: z.boolean(),
    warnings: z.array(z.string().max(20000)).max(10000), extractionConfidence: z.number().finite().min(0).max(1),
    extractionStatus: z.enum(['extracted', 'needs_review']), vectorJobStatus: z.enum(['queued', 'not_requested', 'queue_unavailable'])
}).strict();
export type IngestionReceiptResultV1 = z.infer<typeof ingestionReceiptResultSchemaV1>;
