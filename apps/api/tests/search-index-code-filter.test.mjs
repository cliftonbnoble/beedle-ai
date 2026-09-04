import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { buildIndexCodeSelectionClause } from "../src/services/search-index-code-filter.ts";

function fixture() {
  const db = new DatabaseSync(":memory:");
  db.exec(`
    CREATE TABLE documents (id TEXT PRIMARY KEY, author_name TEXT);
    CREATE TABLE document_index_codes (document_id TEXT, code TEXT, normalized_code TEXT);
    CREATE INDEX index_codes_document ON document_index_codes(document_id);
    CREATE TABLE document_reference_links (
      document_id TEXT, reference_type TEXT, is_valid INTEGER, canonical_value TEXT, normalized_value TEXT
    );
    INSERT INTO documents VALUES ('a', 'Judge A'), ('b', 'Judge A'), ('both', 'Judge A'),
      ('linked', 'Judge A'), ('invalid', 'Judge A'), ('neither', 'Judge A'), ('other-judge', 'Judge B');
    INSERT INTO document_index_codes VALUES
      ('a', 'A', 'a'), ('b', 'B', 'b'), ('both', 'A', 'a'), ('both', 'B', 'b'),
      ('linked', 'A', 'a'), ('invalid', 'A', 'a'), ('neither', 'C', 'c'),
      ('other-judge', 'A', 'a'), ('other-judge', 'B', 'b');
    INSERT INTO document_reference_links VALUES
      ('linked', 'index_code', 1, 'B', 'b'), ('invalid', 'index_code', 0, 'B', 'b');
  `);
  return db;
}

function match(db, codes, operator, judge) {
  const params = [];
  const clause = buildIndexCodeSelectionClause(codes, operator, params);
  const judgeClause = judge ? " AND d.author_name = ?" : "";
  if (judge) params.push(judge);
  return db.prepare(`SELECT d.id FROM documents d WHERE (${clause})${judgeClause} ORDER BY d.id`)
    .all(...params).map((row) => row.id);
}

const group = (...codes) => codes.map((code) => ({ code, normalizedCode: code.toLowerCase() }));

test("OR returns either code while AND requires both on the same decision", () => {
  const db = fixture();
  try {
    assert.deepEqual(match(db, [group("A"), group("B")], "or", "Judge A"), ["a", "b", "both", "invalid", "linked"]);
    assert.deepEqual(match(db, [group("A"), group("B")], "and", "Judge A"), ["both", "linked"]);
  } finally { db.close(); }
});

test("unknown codes cannot silently remove the filter", () => {
  const db = fixture();
  try {
    assert.deepEqual(match(db, [group("A"), group("missing")], "and"), []);
    assert.deepEqual(match(db, [group("A"), group("missing")], "or"), ["a", "both", "invalid", "linked", "other-judge"]);
    assert.deepEqual(match(db, [group("missing")], "or"), []);
  } finally { db.close(); }
});

test("aliases are alternatives within each code, including under AND", () => {
  const db = fixture();
  try {
    assert.deepEqual(match(db, [group("renamed-a", "A"), group("renamed-b", "B")], "and", "Judge A"), ["both", "linked"]);
    assert.deepEqual(match(db, [group("a")], "and"), match(db, [group("A")], "or"));
    assert.deepEqual(match(db, [group("A"), group("A")], "and"), match(db, [group("A")], "and"));
  } finally { db.close(); }
});

test("a full catalog selection stays within the parameter budget and matches safely", () => {
  const db = fixture();
  const codes = [group("A"), ...Array.from({ length: 621 }, (_, i) => group(`missing-${i}`))];
  const params = [];
  buildIndexCodeSelectionClause(codes, "or", params);
  assert.ok(params.length < 10, "leave room for the other filters and lexical query parameters");
  try {
    assert.deepEqual(match(db, codes, "or"), match(db, [group("A")], "or"));
    assert.deepEqual(match(db, codes, "and"), []);
    assert.deepEqual(match(db, [group("A') OR 1=1 --")], "or"), []);
  } finally { db.close(); }
});
