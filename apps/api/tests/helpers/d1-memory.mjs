import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync } from "node:fs";

export function memoryD1() {
  const sqlite = new DatabaseSync(":memory:");
  const directory = new URL("../../migrations/", import.meta.url);
  for (const filename of readdirSync(directory).filter(name => name.endsWith(".sql")).sort()) sqlite.exec(readFileSync(new URL(filename, directory), "utf8"));
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
        _run() { const result = execute('run',this.args); return { success: true, results: [], meta: { changes: Number(result.changes) } }; },
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
