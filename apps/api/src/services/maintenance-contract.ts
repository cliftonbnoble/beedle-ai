import { z } from 'zod';
import { activationWriteSchema } from './activation-contract';
import { RequestConflictError } from '../lib/http';
import { validatedStoredJson as stored } from '../lib/stored-json';
export { storedJson as storedMaintenanceJson } from '../lib/stored-json';
export const maintenanceIdentifier = z.string().min(1).max(200).regex(/^[A-Za-z0-9_.:-]+$/);
export const maintenanceRequestSchema = z.object({
    jobId: maintenanceIdentifier.optional(), limit: z.number().int().min(1).max(5000),
    offset: z.literal(0).default(0), after: z.string().max(256).default(''), batchSize: z.number().int().min(1).max(100).optional(),
    includeDocumentChunks: z.boolean().default(true), includeTrustedChunks: z.boolean().default(true), dryRun: z.boolean().default(false)
}).strict();
const backfillPlanSchema = maintenanceRequestSchema.extend({
    jobId: maintenanceIdentifier, kind: z.enum(['vector_backfill', 'reference_backfill']),
    model: z.string().min(1).max(200).nullable(), namespace: z.string().min(1).max(64).nullable(),
    profileId: z.string().regex(/^[a-f0-9]{64}$/).nullable(), catalogVersion: z.string().min(1).max(200).nullable()
}).strict();
const vectorItemSchema = z.object({
    chunkId: maintenanceIdentifier, sourceText: z.string().max(20000), sectionLabel: z.string().max(1000),
    paragraphAnchor: z.string().max(500), citationAnchor: z.string().max(1000),
    generation: maintenanceIdentifier.optional(), batchId: maintenanceIdentifier.optional()
}).strict();
const references = z.array(z.string().min(1).max(200)).max(50);
const referenceItemSchema = z.object({ indexCodes: references, rulesSections: references, ordinanceSections: references }).strict();
const progressSchema = z.object({ pendingVectorCount: z.number().int().min(0).max(128).optional() }).passthrough();
export function readActivationPayload(raw: string) {
    return stored(activationWriteSchema, raw);
}
export function readBackfillPayload(raw: string, kind: 'vector_backfill' | 'reference_backfill') {
    const value = stored(backfillPlanSchema, raw);
    if (value.kind !== kind || (kind === 'vector_backfill' && !value.profileId) || (kind === 'reference_backfill' && !value.catalogVersion))
        throw new RequestConflictError('Stored maintenance plan kind or version is inconsistent');
    return value;
}
export function readVectorItem(raw: string, kind: string) {
    const value = stored(vectorItemSchema, raw);
    if (kind !== 'document_chunk' && kind !== 'trusted_chunk')
        throw new RequestConflictError('Stored vector item kind is invalid');
    const generation = kind === 'document_chunk' ? value.generation : value.batchId;
    if (!generation || (kind === 'document_chunk' ? value.batchId !== undefined : value.generation !== undefined))
        throw new RequestConflictError('Stored vector generation is inconsistent');
    return { ...value, generationId: generation };
}
export function readReferenceItem(raw: string) {
    return stored(referenceItemSchema, raw);
}
export function readActivationProgress(raw: string) {
    return stored(progressSchema, raw);
}
