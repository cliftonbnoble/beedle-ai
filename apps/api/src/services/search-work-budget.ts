import type { Env } from "../lib/types";
import { SearchWorkBudgetError } from "../lib/http";
import { ProviderOperationError, withinProviderDeadline } from "../lib/provider-operation";
import { corpusPartitions } from "./corpus-partitions";

export const searchWorkPolicy={milliseconds:25_000,statements:96,returnedRows:12_000,rowsPerQuery:2500,returnedBytes:16*1024*1024,reportedRowsRead:2_000_000,vectorQueries:16,embeddingTexts:8,scoringOperations:30_000,pipelinePasses:8} as const;
// A partitioned request still has one deadline and one aggregate budget. The
// statement limit reserves room below D1's paid 1,000-query invocation ceiling.
export const partitionedSearchWorkPolicy={...searchWorkPolicy,statements:768,returnedRows:24_000,returnedBytes:32*1024*1024,scoringOperations:60_000,pipelinePasses:64} as const;
type Limits={ [K in keyof typeof searchWorkPolicy]:number };
type Counter=Exclude<keyof Limits,'milliseconds'|'rowsPerQuery'>;
type Database=Pick<D1Database,'prepare'|'batch'>;
export class SearchWorkBudget {
  readonly deadlineAt:number;
  readonly limits:Limits;
  private readonly counters:Record<Counter,number>={statements:0,returnedRows:0,returnedBytes:0,reportedRowsRead:0,vectorQueries:0,embeddingTexts:0,scoringOperations:0,pipelinePasses:0};
  private failure:SearchWorkBudgetError|null=null;
  private corrections=0;
  private readonly wrapped=new WeakMap<Database,Database>();
  private readonly underlying=new WeakMap<Database,Database>();
  constructor(readonly signal?:AbortSignal,limits:Partial<Limits>={},private readonly parent?:SearchWorkBudget) {
    this.limits={...searchWorkPolicy,...limits};this.deadlineAt=Math.min(Date.now()+this.limits.milliseconds,parent?.deadlineAt??Infinity);
  }
  fork(){return new SearchWorkBudget(this.signal,{},this);}
  forkDatabase<T extends Database>(database:T){const budget=this.fork();return{budget,database:budget.wrap((this.underlying.get(database)||database) as T)};}
  snapshot(){return {...this.counters,millisecondsRemaining:Math.max(0,this.deadlineAt-Date.now())};}
  private stop(reason:string):never {this.failure ||= new SearchWorkBudgetError(reason,this.snapshot());if(this.parent)this.parent.stop(reason);throw this.failure;}
  assertAvailable(){this.parent?.assertAvailable();if(this.failure)throw this.failure;if(this.signal?.aborted)this.stop('cancelled');if(Date.now()>=this.deadlineAt)this.stop('deadline');}
  consume(key:Counter,units=1){this.assertAvailable();this.parent?.consume(key,units);this.counters[key]+=units;if(this.counters[key]>this.limits[key])this.stop(key);}
  claimCorrection(){this.assertAvailable();if(this.corrections)return false;this.corrections++;return true;}
  async bounded<T>(operation:()=>Promise<T>,timeoutMs=this.limits.milliseconds):Promise<T> {
    this.assertAvailable();
    try{return await withinProviderDeadline({signal:this.signal,deadlineAt:this.deadlineAt},timeoutMs,operation);}
    catch(error){if(error instanceof ProviderOperationError&&(error.reason==='timeout'||error.reason==='cancelled'))this.stop(error.reason==='cancelled'?'cancelled':'deadline');throw error;}
  }
  wrap<T extends Database>(database:T):T {
    const existing=this.wrapped.get(database);if(existing)return existing as T;
    const budget=this;
    const descriptions=new WeakMap<D1PreparedStatement,{sql:string;bindings:unknown[]}>();
    function limitedSql(sql:string,first=false) {
      if(!/^\s*(SELECT|WITH)\b/i.test(sql))return sql;
      const remaining=Math.min(budget.limits.rowsPerQuery,budget.limits.returnedRows-budget.counters.returnedRows);
      if(remaining<=0)budget.stop('returnedRows');
      return `SELECT * FROM (${sql.trim().replace(/;\s*$/,'')}) LIMIT ${first?1:remaining+1}`;
    }
    function observe(result:D1Result<Record<string,unknown>>) {
      budget.consume('returnedRows',result.results.length);
      if(result.results.length>budget.limits.rowsPerQuery)budget.stop('rowsPerQuery');
      budget.consume('returnedBytes',new TextEncoder().encode(JSON.stringify(result.results)).byteLength);
      budget.consume('reportedRowsRead',Number(result.meta.rows_read||0));
      return result;
    }
    function prepare(sql:string,bindings:unknown[]=[]):D1PreparedStatement {
      const statement=database.prepare(sql);
      const proxy=new Proxy(statement,{get(target,key){
        if(key==='bind')return (...args:unknown[])=>prepare(sql,args);
        if(key==='all'||key==='run'||key==='first'||key==='raw')return async(...args:unknown[])=>{
          budget.consume('statements');
          const prepared=database.prepare(limitedSql(sql,key==='first')).bind(...bindings);
          const result=observe(await budget.bounded(()=>key==='run'?prepared.run<Record<string,unknown>>():prepared.all<Record<string,unknown>>()));
          if(key==='first')return args[0]?result.results[0]?.[String(args[0])]??null:result.results[0]??null;
          if(key==='raw'){
            const columns=Object.keys(result.results[0]||{});const rows=result.results.map(row=>columns.map(column=>row[column]));
            return args[0]&&typeof args[0]==='object'&&'columnNames' in args[0]&&args[0].columnNames?[columns,...rows]:rows;
          }
          return result;
        };
        const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
      }});
      descriptions.set(proxy,{sql,bindings});return proxy;
    }
    const proxy=new Proxy(database,{get(target,key){
      if(key==='prepare')return prepare;
      if(key==='exec')return ()=>budget.stop('untracked_exec');
      if(key==='withSession')return (...args:unknown[])=>budget.wrap((Reflect.get(target,key) as Function).apply(target,args));
      if(key==='batch')return async(statements:D1PreparedStatement[])=>{
        if(statements.some(statement=>!descriptions.has(statement)))budget.stop('untracked_batch');
        if(statements.some(statement=>/^\s*(SELECT|WITH)\b/i.test(descriptions.get(statement)?.sql||'')))budget.stop('read_batch');
        budget.consume('statements',statements.length);
        const originals=statements.map(statement=>{const description=descriptions.get(statement);return description?database.prepare(limitedSql(description.sql)).bind(...description.bindings):statement;});
        const results=await budget.bounded(()=>database.batch<Record<string,unknown>>(originals));for(const result of results)observe(result);return results;
      };
      const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
    }});
    this.wrapped.set(database,proxy);this.wrapped.set(proxy,proxy);this.underlying.set(proxy,database);return proxy;
  }
}
export function newSearchWorkBudget(env:Env,signal?:AbortSignal){return new SearchWorkBudget(signal,corpusPartitions(env)?partitionedSearchWorkPolicy:{});}
export function searchBudgetEnvironment(env:Env,signal?:AbortSignal) {
  const layout=corpusPartitions(env);
  const budget=env.SEARCH_WORK_BUDGET||newSearchWorkBudget(env,signal);
  const wrapped={...env,SEARCH_WORK_BUDGET:budget,DB:budget.wrap(env.DB)};
  if(layout){
    for(const part of layout.partitions)Reflect.set(wrapped,part.binding,budget.wrap(part.database));
    if(env.CORPUS_COORDINATOR_DB)wrapped.CORPUS_COORDINATOR_DB=budget.wrap(env.CORPUS_COORDINATOR_DB);
  }
  return wrapped;
}
