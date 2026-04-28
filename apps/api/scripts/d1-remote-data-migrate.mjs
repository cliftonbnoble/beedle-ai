import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';

const sqlitePath =
  process.env.D1_DB_PATH ||
  '/Users/cliftonnoble/Code/beedle-ai/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/c1163e5c7f431b6e93caee023c4de17a42d58b1f179732141bd39cd6138e1bac.sqlite';
const cwd = process.cwd();
const reportsDir = path.resolve(cwd, 'reports');
const jobsDir = path.join(reportsDir, 'd1-remote-migration-jobs');
const statePath = path.join(jobsDir, 'd1-remote-data-migration-state.json');
const tmpSqlPath = path.join(jobsDir, 'd1-remote-data-migration-current.sql');
const databaseName = process.env.D1_REMOTE_DATABASE || 'beedle';
const batchSize = Math.max(1, Number.parseInt(process.env.D1_REMOTE_MIGRATION_BATCH_SIZE || '100', 10));
const maxBatches = Math.max(0, Number.parseInt(process.env.D1_REMOTE_MIGRATION_MAX_BATCHES || '0', 10));
const resetState = process.env.D1_REMOTE_MIGRATION_RESET === '1';
const importRetries = Math.max(0, Number.parseInt(process.env.D1_REMOTE_MIGRATION_IMPORT_RETRIES || '5', 10));
const retryDelayMs = Math.max(1000, Number.parseInt(process.env.D1_REMOTE_MIGRATION_RETRY_DELAY_MS || '30000', 10));

const tables = [
  'legal_reference_sources',
  'legal_index_codes',
  'legal_ordinance_sections',
  'legal_rules_sections',
  'legal_reference_crosswalk',
  'documents',
  'document_sections',
  'section_paragraphs',
  'document_chunks',
  'document_reference_links',
  'document_reference_issues',
  'retrieval_activation_batches',
  'retrieval_activation_documents',
  'retrieval_activation_chunks',
  'retrieval_embedding_rows',
  'retrieval_search_rows',
  'retrieval_search_chunks'
];

function execFile(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
      ...options
    });
    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${command} ${args.join(' ')} failed with ${code}\n${stdout}\n${stderr}`));
    });
  });
}

function quoteIdent(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function parseJsonRows(stdout) {
  return JSON.parse(stdout || '[]');
}

async function runSqlJson(sql) {
  const sqliteUri = `file:${sqlitePath}?mode=ro&immutable=1`;
  const { stdout } = await execFile('sqlite3', ['-json', sqliteUri, sql], {
    maxBuffer: 100 * 1024 * 1024
  });
  return parseJsonRows(stdout);
}

async function runSqlLines(sql) {
  const sqliteUri = `file:${sqlitePath}?mode=ro&immutable=1`;
  const { stdout } = await execFile('sqlite3', ['-batch', sqliteUri, sql], {
    maxBuffer: 250 * 1024 * 1024
  });
  return stdout;
}

async function loadState() {
  if (resetState) return { generatedAt: new Date().toISOString(), tables: {} };
  try {
    return JSON.parse(await fs.readFile(statePath, 'utf8'));
  } catch {
    return { generatedAt: new Date().toISOString(), tables: {} };
  }
}

async function saveState(state) {
  state.updatedAt = new Date().toISOString();
  await fs.writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`);
}

async function getColumns(table) {
  const rows = await runSqlJson(`PRAGMA table_info(${quoteIdent(table)});`);
  return rows.map((row) => String(row.name));
}

function cleanRowWhere(table, lastRowid) {
  const clauses = [`rowid > ${Number(lastRowid) || 0}`];
  if (table === 'retrieval_embedding_rows') {
    clauses.push('rowid IN (SELECT MIN(rowid) FROM retrieval_embedding_rows GROUP BY chunk_id)');
  }
  return clauses.join(' AND ');
}

async function getNextRowids(table, lastRowid) {
  const rows = await runSqlJson(`
    SELECT rowid
    FROM ${quoteIdent(table)}
    WHERE ${cleanRowWhere(table, lastRowid)}
    ORDER BY rowid
    LIMIT ${batchSize};
  `);
  return rows.map((row) => Number(row.rowid)).filter(Number.isFinite);
}

function buildInsertExpression(table, columns) {
  const columnList = columns.map(quoteIdent).join(', ');
  const valuesExpr = columns.map((column) => `quote(${quoteIdent(column)})`).join(` || ', ' || `);
  return `'INSERT OR IGNORE INTO ${quoteIdent(table)} (${columnList}) VALUES(' || ${valuesExpr} || ');'`;
}

async function writeBatchSql(table, rowids, columns) {
  const rowidList = rowids.join(', ');
  const insertExpr = buildInsertExpression(table, columns);
  const insertLines = await runSqlLines(`
    SELECT ${insertExpr}
    FROM ${quoteIdent(table)}
    WHERE rowid IN (${rowidList})
    ORDER BY rowid;
  `);
  // Remote D1 imports reject explicit BEGIN/COMMIT via wrangler execute.
  // Wrangler still rolls the uploaded file back if execution fails.
  const sql = [insertLines.trim(), ''].join('\n');
  await fs.writeFile(tmpSqlPath, sql);
}

async function importBatch() {
  const { stdout, stderr } = await execFile('npx', [
    'wrangler',
    'd1',
    'execute',
    databaseName,
    '--remote',
    '--yes',
    '--file',
    tmpSqlPath
  ], {
    maxBuffer: 100 * 1024 * 1024
  });
  return `${stdout}${stderr}`.trim();
}

function sleep(ms) {
  return promisify(setTimeout)(ms);
}

async function importBatchWithRetry(table, rowCount) {
  let lastError = null;
  for (let attempt = 0; attempt <= importRetries; attempt += 1) {
    try {
      if (attempt > 0) {
        console.log(`${table}: retrying failed remote import attempt ${attempt}/${importRetries} for ${rowCount} rows`);
      }
      return await importBatch();
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      console.error(`${table}: remote import failed on attempt ${attempt + 1}/${importRetries + 1}: ${message}`);
      if (attempt >= importRetries) break;
      await sleep(retryDelayMs);
    }
  }
  throw lastError;
}

async function tableCount(table) {
  const [{ count = 0 } = {}] = await runSqlJson(`SELECT COUNT(*) AS count FROM ${quoteIdent(table)};`);
  return Number(count || 0);
}

async function cleanTableCount(table) {
  if (table !== 'retrieval_embedding_rows') return tableCount(table);
  const [{ count = 0 } = {}] = await runSqlJson(`
    SELECT COUNT(*) AS count
    FROM retrieval_embedding_rows
    WHERE rowid IN (SELECT MIN(rowid) FROM retrieval_embedding_rows GROUP BY chunk_id);
  `);
  return Number(count || 0);
}

async function main() {
  await fs.mkdir(jobsDir, { recursive: true });
  const state = await loadState();
  let batchesRun = 0;

  console.log(JSON.stringify({ sqlitePath, databaseName, batchSize, maxBatches, statePath }, null, 2));

  for (const table of tables) {
    const columns = await getColumns(table);
    const totalCleanRows = await cleanTableCount(table);
    const tableState = state.tables[table] || { lastRowid: 0, importedRows: 0, complete: false };
    state.tables[table] = tableState;
    tableState.totalCleanRows = totalCleanRows;

    if (tableState.complete) {
      console.log(`${table}: already complete (${tableState.importedRows}/${totalCleanRows})`);
      continue;
    }

    console.log(`${table}: starting at rowid>${tableState.lastRowid}; target clean rows=${totalCleanRows}`);

    while (true) {
      if (maxBatches && batchesRun >= maxBatches) {
        await saveState(state);
        console.log(`Reached max batch limit (${maxBatches}); state saved.`);
        return;
      }

      const rowids = await getNextRowids(table, tableState.lastRowid);
      if (rowids.length === 0) {
        tableState.complete = true;
        tableState.completedAt = new Date().toISOString();
        await saveState(state);
        console.log(`${table}: complete (${tableState.importedRows}/${totalCleanRows})`);
        break;
      }

      await writeBatchSql(table, rowids, columns);
      const importOutput = await importBatchWithRetry(table, rowids.length);
      tableState.lastRowid = rowids[rowids.length - 1];
      tableState.importedRows += rowids.length;
      tableState.lastImportedAt = new Date().toISOString();
      tableState.lastImportOutput = importOutput.split(/\r?\n/).slice(-8).join('\n');
      batchesRun += 1;
      await saveState(state);

      console.log(
        `${table}: imported batch rows=${rowids.length}; progress=${tableState.importedRows}/${totalCleanRows}; lastRowid=${tableState.lastRowid}`
      );
    }
  }

  console.log(`Remote D1 data migration complete. State: ${statePath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
