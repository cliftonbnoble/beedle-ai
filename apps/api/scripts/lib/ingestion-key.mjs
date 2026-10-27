import {createHash} from 'node:crypto';
// Stable across process restarts. The target and operation are part of the key;
// a distinct legal record/revision requires its own explicit operation input.
export function ingestionImportKey(target,operation,payload){
 const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
 const source=payload.sourceFile,bytes=source.bytes??Buffer.from(source.bytesBase64,'base64');
 const hash=createHash('sha256').update(bytes).digest('hex');
 const hex=createHash('sha256').update(JSON.stringify([1,new URL(target).origin,operation,canonical({...payload,idempotencyKey:undefined,sourceFile:{filename:source.filename,mimeType:source.mimeType,sha256:hash}})])).digest('hex');
 return `${hex.slice(0,8)}-${hex.slice(8,12)}-5${hex.slice(13,16)}-a${hex.slice(17,20)}-${hex.slice(20,32)}`;
}
