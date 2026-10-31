export interface CorpusPartitionManifest {
 version:1;partitionCount:32;readOnly:true;sourceManifestSha256:string;catalogVersion:string;catalogSha256:string;
 restoreOwnerId:string;restorePlanSha256:string;documents:number;publishedDocuments:number;
 partitions:Array<{index:number;binding:string;documents:number;publishedDocuments:number;revision:number}>;
}
export const partitionIntegrityCommands:Record<'metadata'|'body'|'phrase'|'physical'|'foreign_keys'|'source_sha',{command:string;sha256:string}>;
export const corpusPartitionPolicy:{id:string;partitions:number;readOnly:boolean;lexicalOnly:boolean;routing:string;candidateDocuments:number;fanout:number;rankingVersion:string;integrityCommands:Record<string,string>};
export function corpusPartitionBinding(index:number):string;
export function parseCorpusPartitionManifest(text:string):CorpusPartitionManifest;
export function partitionResourceBindings(target:{d1_databases:Array<{binding:string;database_name:string;database_id:string}>},manifest:CorpusPartitionManifest):Array<{binding:string;database_name:string;database_id:string}>;
export const partitionProofQuery:string;
export function partitionProofProblems(manifest:CorpusPartitionManifest,index:number,row:Record<string,unknown>|null):string[];
export const corpusReviewCacheProofQuery:string;
export function manifestTextFingerprint(text:string):Promise<string>;
export function corpusReviewCacheProblems(manifest:CorpusPartitionManifest,manifestSha:string,row:Record<string,unknown>|null,policy:string):string[];
