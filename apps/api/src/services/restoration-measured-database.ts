import {type RestorationCapacityPhase,restorationCapacityObserver} from './restoration-capacity-observer';

// first() exposes no D1 metadata. Execute that same bounded statement once with
// all(), observe its acknowledged metadata, then preserve first-row semantics.
// No second SQL request, inferred size, SQL payload or automatic retry is added.
export function restorationMeasuredDatabase(database:D1Database,
  observer:Pick<ReturnType<typeof restorationCapacityObserver>,'observe'>,phase:RestorationCapacityPhase):D1Database{
  const descriptions=new WeakMap<D1PreparedStatement,{sql:string;values:unknown[]}>();
  function prepare(sql:string,values:unknown[]=[]):D1PreparedStatement{
    const statement=database.prepare(sql).bind(...values),proxy=new Proxy(statement,{get(target,key){
      if(key==='bind')return(...args:unknown[])=>prepare(sql,args);
      if(key==='all')return async()=>observer.observe(await statement.all(),phase);
      if(key==='run')return async()=>observer.observe(await statement.run(),phase);
      if(key==='first')return async(column?:string)=>{
        const result=observer.observe(await statement.all<Record<string,unknown>>(),phase),first=result.results[0];
        if(!first)return null;if(column===undefined)return first;
        if(!Object.prototype.hasOwnProperty.call(first,column))throw Error('restoration_measured_column_missing');
        return first[column];
      };
      if(key==='raw')return()=>{throw Error('restoration_unmeasured_raw_access');};
      const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
    }});descriptions.set(proxy,{sql,values});return proxy;
  }
  return new Proxy(database,{get(target,key){
    if(key==='prepare')return prepare;
    if(key==='batch')return async(statements:D1PreparedStatement[])=>{
      const originals=statements.map(statement=>{const description=descriptions.get(statement);if(!description)throw Error('restoration_unmeasured_batch');return database.prepare(description.sql).bind(...description.values);});
      return observer.observe(await database.batch(originals),phase);
    };
    if(key==='exec'||key==='dump'||key==='withSession')return()=>{throw Error('restoration_unmeasured_database_access');};
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
}
