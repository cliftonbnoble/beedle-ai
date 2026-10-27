import { accessSync, constants, readSync, closeSync, openSync, realpathSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const apiRoot = fileURLToPath(new URL('../../', import.meta.url));
export const repositoryRoot = fileURLToPath(new URL('../../../../', import.meta.url));

// Never guess among Wrangler's local databases, or let sqlite3 create a typo path.
// This establishes an explicit local file, not permission to mutate a live corpus.
export function requireExplicitDatabasePath(value = process.env.D1_DB_PATH) {
  if (typeof value !== 'string' || !value || !path.isAbsolute(value) || value.includes('\0'))
    throw new Error('D1_DB_PATH must explicitly name an absolute path to an existing SQLite snapshot. No database is selected automatically.');
  let resolved;
  try {
    resolved = realpathSync(value);
    if (!statSync(resolved).isFile()) throw new Error('not a regular file');
    accessSync(resolved, constants.R_OK);
    const fd = openSync(resolved, 'r');
    try {
      const header = Buffer.alloc(16);
      if (readSync(fd, header, 0, 16, 0) !== 16 || header.toString('ascii') !== 'SQLite format 3\0') throw new Error('not SQLite');
    } finally { closeSync(fd); }
  } catch {
    throw new Error('D1_DB_PATH must name a readable, existing SQLite file. Use a reviewed snapshot; do not guess a Wrangler state file.');
  }
  return resolved;
}
