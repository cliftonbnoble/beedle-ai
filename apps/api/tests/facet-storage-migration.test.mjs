import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const migrationPath = path.resolve(process.cwd(), "migrations/0009_document_facets.sql");

async function sqliteAvailable() {
  try {
    await execFileAsync("sqlite3", ["--version"]);
    return true;
  } catch {
    return false;
  }
}

async function execSql(dbPath, sql) {
  await execFileAsync("sqlite3", [dbPath, sql], { maxBuffer: 10 * 1024 * 1024 });
}

async function queryJson(dbPath, sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], { maxBuffer: 10 * 1024 * 1024 });
  return JSON.parse(stdout || "[]");
}

test("document facet migration backfills and syncs normalized join tables", async (t) => {
  if (!(await sqliteAvailable())) {
    t.skip("sqlite3 is not available");
    return;
  }

  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "beedle-facet-migration-"));
  const dbPath = path.join(dir, "facet.sqlite");

  try {
    await execSql(
      dbPath,
      `
        PRAGMA foreign_keys = ON;
        CREATE TABLE documents (
          id TEXT PRIMARY KEY,
          index_codes_json TEXT NOT NULL DEFAULT '[]',
          rules_sections_json TEXT NOT NULL DEFAULT '[]',
          ordinance_sections_json TEXT NOT NULL DEFAULT '[]'
        );
        INSERT INTO documents (
          id, index_codes_json, rules_sections_json, ordinance_sections_json
        ) VALUES (
          'doc-1',
          '["IC-104", "104", " G-28 "]',
          '["Rule 1.11", "1.11"]',
          '["Ordinance 37.3(a)(1)"]'
        );
        INSERT INTO documents (
          id, index_codes_json, rules_sections_json, ordinance_sections_json
        ) VALUES (
          'doc-bad-json',
          'not json',
          'not json',
          'not json'
        );
      `
    );

    await execSql(dbPath, await fs.readFile(migrationPath, "utf8"));

    assert.deepEqual(await queryJson(dbPath, "SELECT code, normalized_code FROM document_index_codes ORDER BY normalized_code;"), [
      { code: "IC-104", normalized_code: "104" },
      { code: "G-28", normalized_code: "g-28" }
    ]);
    assert.deepEqual(await queryJson(dbPath, "SELECT section, normalized_section FROM document_rules_sections ORDER BY normalized_section;"), [
      { section: "Rule 1.11", normalized_section: "1.11" }
    ]);
    assert.deepEqual(await queryJson(dbPath, "SELECT section, normalized_section FROM document_ordinance_sections;"), [
      { section: "Ordinance 37.3(a)(1)", normalized_section: "37.3(a)(1)" }
    ]);

    await execSql(
      dbPath,
      `
        UPDATE documents
        SET index_codes_json = '["IC-105"]',
            rules_sections_json = '[]',
            ordinance_sections_json = '["Ordinance 37.15"]'
        WHERE id = 'doc-1';
      `
    );

    assert.deepEqual(await queryJson(dbPath, "SELECT code, normalized_code FROM document_index_codes;"), [
      { code: "IC-105", normalized_code: "105" }
    ]);
    assert.deepEqual(await queryJson(dbPath, "SELECT COUNT(*) AS count FROM document_rules_sections;"), [{ count: 0 }]);
    assert.deepEqual(await queryJson(dbPath, "SELECT section, normalized_section FROM document_ordinance_sections;"), [
      { section: "Ordinance 37.15", normalized_section: "37.15" }
    ]);

    await execSql(dbPath, "DELETE FROM documents WHERE id = 'doc-1';");

    assert.deepEqual(await queryJson(dbPath, "SELECT COUNT(*) AS count FROM document_index_codes;"), [{ count: 0 }]);
    assert.deepEqual(await queryJson(dbPath, "SELECT COUNT(*) AS count FROM document_rules_sections;"), [{ count: 0 }]);
    assert.deepEqual(await queryJson(dbPath, "SELECT COUNT(*) AS count FROM document_ordinance_sections;"), [{ count: 0 }]);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});
