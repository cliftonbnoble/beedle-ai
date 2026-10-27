import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { loadTs } from "./load-ts.mjs";

export function memoryD1({migrationThrough=Infinity,finalizeEmptyPhraseIndex=true}={}) {
  const sqlite = new DatabaseSync(":memory:");
  const directory = new URL("../../migrations/", import.meta.url);
  for (const filename of readdirSync(directory).filter(name => name.endsWith(".sql") && Number(name.split("_")[0])<=migrationThrough).sort()) sqlite.exec(readFileSync(new URL(filename, directory), "utf8"));
  // Most service fixtures start with a ready empty schema. Complete the empty
  // shadow cutover using the production DDL; migration/replay tests opt out and
  // exercise the leased builder itself.
  if(finalizeEmptyPhraseIndex && sqlite.prepare("SELECT 1 FROM sqlite_master WHERE name='search_phrase_build'").get()) {
    sqlite.exec('BEGIN');
    try {for(const sql of loadTs('src/services/search-phrase-cutover.ts').phraseCutoverSql)sqlite.exec(sql);sqlite.exec('COMMIT');}
    catch(error){sqlite.exec('ROLLBACK');throw error;}
  }
  const DB = {
    prepare(sql) {
      // D1 accepts numbered positional placeholders. node:sqlite exposes those
      // as named bindings, so preserve the actual SQL and adapt only binding.
      const execute=(method,args)=> /\?\d+/.test(sql)
        ? sqlite.prepare(sql)[method](Object.fromEntries(args.map((value,index)=>[`?${index+1}`,value])))
        : sqlite.prepare(sql)[method](...args);
      return {
        sql, args: [],
        bind(...args) { return { ...this, args }; },
        async all() { return { results: execute('all',this.args), success: true, meta: {} }; },
        async first(column) { const row = execute('get',this.args); return column ? row?.[column] ?? null : row ?? null; },
        _run() {
          const before=sqlite.prepare('SELECT total_changes() AS n').get().n;
          const results=/\bRETURNING\b|^\s*(?:SELECT|PRAGMA|EXPLAIN|WITH)\b/i.test(sql)?execute('all',this.args):(execute('run',this.args),[]);
          const after=sqlite.prepare('SELECT total_changes() AS n').get().n;
          return {success:true,results,meta:{changes:Number(after-before)}};
        },
        async run() { return this._run(); }
      };
    },
    async batch(statements) {
      sqlite.exec("BEGIN");
      try { const result = statements.map(statement => statement._run()); sqlite.exec("COMMIT"); return result; }
      catch (error) { sqlite.exec("ROLLBACK"); throw error; }
    }
  };
  return { DB, sqlite };
}
