export function resolveCorpusPolicy(value:string|undefined){
  if(value===undefined||value==='')return{readOnly:false,includeProvisionalByDefault:false};
  if(value!=='unreviewed-provisional-v1')throw new Error('Unknown corpus deployment policy');
  return{readOnly:true,includeProvisionalByDefault:true};
}
export const corpusPolicy=resolveCorpusPolicy(process.env.NEXT_PUBLIC_CORPUS_POLICY);
export function initialIncludeProvisional(mode:string|null){
  if(mode==='trusted_only')return false;
  if(mode==='trusted_plus_provisional')return true;
  return corpusPolicy.includeProvisionalByDefault;
}
