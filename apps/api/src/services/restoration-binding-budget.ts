import {RestorationControlError} from './restoration-job-control';
// D1 batches count as one binding request. Preserve each actual prepared
// statement and count the binding call without changing SQL, values or results.
export function restorationBindingBudget(database:D1Database,maximum=700){
  if(!Number.isSafeInteger(maximum)||maximum<1||maximum>900)throw new RestorationControlError('invalid_restoration_binding_budget');
  let calls=0;const descriptions=new WeakMap<D1PreparedStatement,{sql:string;values:unknown[]}>();
  function consume(){if(calls>=maximum)throw new RestorationControlError('restoration_binding_budget_exhausted');calls++;}
  function prepare(sql:string,values:unknown[]=[]):D1PreparedStatement{
    const statement=database.prepare(sql).bind(...values);
    const proxy=new Proxy(statement,{get(target,key){
      if(key==='bind')return(...args:unknown[])=>prepare(sql,args);
      if(key==='first')return(column?:string)=>{consume();return column===undefined?statement.first():statement.first(column);};
      if(key==='all')return()=>{consume();return statement.all();};
      if(key==='run')return()=>{consume();return statement.run();};
      if(key==='raw')return(options?:{columnNames?:boolean})=>{consume();return options?.columnNames===true?statement.raw({columnNames:true}):statement.raw();};
      const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
    }});descriptions.set(proxy,{sql,values});return proxy;
  }
  const db=new Proxy(database,{get(target,key){
    if(key==='prepare')return prepare;
    if(key==='exec'||key==='dump'||key==='withSession')return()=>{throw new RestorationControlError('untracked_restoration_database_access');};
    if(key==='batch')return async(statements:D1PreparedStatement[])=>{
      const originals=statements.map(statement=>{const description=descriptions.get(statement);if(!description)throw new RestorationControlError('untracked_restoration_batch');return database.prepare(description.sql).bind(...description.values);});
      consume();return database.batch(originals);
    };
    const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  return{db,get calls(){return calls;},get remaining(){return maximum-calls;}};
}
