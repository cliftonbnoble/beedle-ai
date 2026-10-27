import { stageReferenceEdition, verifyReferenceEdition } from "./reference-editions";
import type { LegalEditionDescriptor } from "@beedle/shared";
import { z } from "zod";
import type { Env } from "../lib/types";
import { RequestConflictError, RequestValidationError } from "../lib/http";
import { sha256Bytes } from "../lib/digests";
import { catalogTables, type CatalogTable } from "./catalog-tables";
export type CatalogRows = Record<CatalogTable, Array<Record<string, string | number | null>>>;
type Manifest = Record<CatalogTable, {
  count: number;
  sha256: string;
}>;
export type CatalogState = {
  version: string;
  epoch: number;
};
const tables = Object.keys(catalogTables) as CatalogTable[];
const now = () => new Date().toISOString();
export async function catalogState(env: Env): Promise<CatalogState> {
  const row = await env.DB.prepare('SELECT active_version AS version,epoch FROM legal_catalog_state WHERE singleton=1').first<CatalogState>();
  if (!row)
    throw new RequestValidationError('Legal catalog migration is required');
  return row;
}
export async function assertCatalogState(env: Env, expected: CatalogState) {
  const current = await catalogState(env);
  if (current.epoch !== expected.epoch || current.version !== expected.version)
    throw new RequestConflictError('Legal catalog changed during this operation; reload and retry.');
}
export async function readCatalogRows(env: Env, version: string): Promise<CatalogRows> {
  const results = (await env.DB.prepare('SELECT table_name,payload_json FROM legal_catalog_rows WHERE catalog_id=? ORDER BY table_name,row_id').bind(version)
    .all<{
      table_name: CatalogTable;
      payload_json: string;
    }>()).results || [];
  const rows = Object.fromEntries(tables.map(table => [table, []])) as unknown as CatalogRows;
  for (const row of results) {
    if (!tables.includes(row.table_name))
      throw new RequestValidationError('Unknown retained catalog table');
    rows[row.table_name].push(JSON.parse(row.payload_json));
  }
  return rows;
}
async function manifest(rows: CatalogRows): Promise<Manifest> {
  const result = {} as Manifest;
  for (const table of tables) {
    const columns = catalogTables[table];
    const ordered = [...rows[table]].sort((a, b) => (String(a[columns[0]]) < String(b[columns[0]]) ? -1 : String(a[columns[0]]) > String(b[columns[0]]) ? 1 : 0));
    const text = JSON.stringify(ordered.map(row => columns.map(column => row[column])));
    if (ordered.length > 10000 || new TextEncoder().encode(text).length > 8 * 1024 * 1024)
      throw new RequestValidationError('Catalog exceeds bounded publication limits');
    result[table] = { count: ordered.length, sha256: await sha256Bytes(new TextEncoder().encode(text)) };
  }
  return result;
}
async function verifyVersion(env: Env, id: string) {
  const version = await env.DB.prepare('SELECT id,state,base_epoch,parent_id,manifest_json FROM legal_catalog_versions WHERE id=?').bind(id).first<{
    id: string;
    state: string;
    base_epoch: number;
    parent_id: string | null;
    manifest_json: string | null;
  }>();
  if (!version)
    throw new RequestValidationError('Catalog version not found');
  await verifyReferenceEdition(env,id);
  const rows = await readCatalogRows(env, id);
  const measured = await manifest(rows);
  if (version.manifest_json && JSON.stringify(measured) !== version.manifest_json)
    throw new RequestValidationError('Retained catalog manifest mismatch');
  // The migration captured immutable legacy rows before enabling write guards.
  // Seal that observed snapshot honestly; this is not original-file provenance.
  if (id === 'legacy' && !version.manifest_json) {
    await env.DB.prepare('UPDATE legal_catalog_versions SET manifest_json=? WHERE id=? AND manifest_json IS NULL').bind(JSON.stringify(measured), id).run();
  }
  else if (!version.manifest_json)
    throw new RequestValidationError('Catalog version has no verified manifest');
  return { version, rows, manifest: measured };
}
export async function stageLegalCatalog(env: Env, rows: CatalogRows, payloadDigest: string, base: CatalogState, edition?:LegalEditionDescriptor) {
  const expected = await manifest(rows);
  const version = crypto.randomUUID();
  await env.DB.prepare("INSERT INTO legal_catalog_versions(id,parent_id,base_epoch,state,payload_sha256,created_at) VALUES (?,?,?,'staging',?,?)")
    .bind(version, base.version, base.epoch, payloadDigest, now()).run();
  try {
    await stageReferenceEdition(env,version,edition);
    const statements = tables.flatMap(table => rows[table].map(row => env.DB.prepare('INSERT INTO legal_catalog_rows(catalog_id,table_name,row_id,payload_json) VALUES (?,?,?,?)')
      .bind(version, table, String(row[catalogTables[table][0]]), JSON.stringify(row))));
    for (let at = 0;at < statements.length;at += 50)
      await env.DB.batch(statements.slice(at, at + 50));
    const actual = await manifest(await readCatalogRows(env, version));
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new RequestValidationError('Staged catalog count or digest mismatch');
    await env.DB.prepare("UPDATE legal_catalog_versions SET state='ready',manifest_json=? WHERE id=? AND state='staging'").bind(JSON.stringify(expected), version).run();
    return version;
  }
  catch (error) {
    await env.DB.prepare("UPDATE legal_catalog_versions SET state='failed',last_error='Catalog staging failed; live catalog retained' WHERE id=? AND state='staging'").bind(version).run().catch(() => { });
    throw error;
  }
}
export async function publishLegalCatalog(env: Env, version: string, expected: CatalogState) {
  const target = await verifyVersion(env, version);
  if (!['ready', 'retained', 'published'].includes(target.version.state))
    throw new RequestValidationError('Catalog version is not publishable');
  if (target.version.state === 'ready' && (target.version.base_epoch !== expected.epoch || target.version.parent_id !== expected.version))
    throw new RequestConflictError('Staged catalog was prepared against a different active version');
  await verifyVersion(env, expected.version);
  if (version === expected.version) {
    await assertCatalogState(env, expected);
    return { ...expected, replayed: true };
  }
  const token = crypto.randomUUID();
  const timestamp = now();
  const statements = [env.DB.prepare('INSERT INTO legal_catalog_guards(token,version_id,expected_epoch) VALUES (?,?,?)').bind(token, version, expected.epoch)];
  for (const table of tables)
    statements.push(env.DB.prepare(`DELETE FROM ${table}`));
  for (const table of tables) {
    const columns = catalogTables[table];
    statements.push(env.DB.prepare(`INSERT INTO ${table}(${columns.join(',')})
    SELECT ${columns.map(column => `json_extract(payload_json,'$.${column}')`).join(',')} FROM legal_catalog_rows WHERE catalog_id=? AND table_name=?`).bind(version, table));
  }
  statements.push(env.DB.prepare("UPDATE legal_catalog_versions SET state='retained' WHERE state='published'"), env.DB.prepare("UPDATE legal_catalog_versions SET state='published',published_at=? WHERE id=?").bind(timestamp, version), env.DB.prepare('UPDATE legal_catalog_state SET active_version=?,epoch=epoch+1 WHERE singleton=1').bind(version), env.DB.prepare('INSERT INTO legal_catalog_events(id,version_id,previous_version,epoch,created_at) VALUES (?,?,?,?,?)').bind(crypto.randomUUID(), version, expected.version, expected.epoch + 1, timestamp), env.DB.prepare('DELETE FROM legal_catalog_guards WHERE token=?').bind(token));
  try {
    await env.DB.batch(statements);
  }
  catch (error) {
    if (String(error).includes('catalog_revision_conflict'))
      throw new RequestConflictError('Another catalog publication won; inspect the retained version before retrying.');
    throw error;
  }
  return { version, epoch: expected.epoch + 1, previousVersion: expected.version, replayed: false };
}
export async function listLegalCatalogVersions(env: Env) {
  const current = await catalogState(env);
  const versions = (await env.DB.prepare('SELECT id,parent_id AS parentId,base_epoch AS baseEpoch,state,payload_sha256 AS payloadSha256,manifest_json AS manifestJson,(SELECT descriptor_json FROM legal_reference_editions WHERE catalog_id=legal_catalog_versions.id) AS editionJson,created_at AS createdAt,published_at AS publishedAt,last_error AS lastError FROM legal_catalog_versions ORDER BY created_at DESC LIMIT 100').all()).results || [];
  return { ...current, versions };
}
export async function restoreLegalCatalog(env: Env, raw: unknown) {
  const input = z.object({ versionId: z.string().min(1).max(128).regex(/^[A-Za-z0-9-]+$/), expectedEpoch: z.number().int().nonnegative() }).strict().parse(raw);
  const current = await catalogState(env);
  if (current.epoch !== input.expectedEpoch)
    throw new RequestConflictError('Catalog changed; inspect its current version before restoring.');
  const target = await env.DB.prepare('SELECT state FROM legal_catalog_versions WHERE id=?').bind(input.versionId).first<{
    state: string;
  }>();
  if (!target || !['retained', 'published'].includes(target.state))
    throw new RequestValidationError('Only a previously published catalog can be restored');
  return publishLegalCatalog(env, input.versionId, current);
}
