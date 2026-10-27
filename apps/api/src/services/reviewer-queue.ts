import { z } from "zod";
import type { Env } from "../lib/types";
import type { ListIngestionDocumentsOptions } from "./reviewer-contracts";
import { RequestConflictError, RequestValidationError, ReviewerQueueUnavailableError } from "../lib/http";
import { inspectReviewerQueue, REVIEWER_POLICY_VERSION, type ReviewerDocument } from "./reviewer-queue-index";
import { sha256Bytes } from "../lib/digests";

const sorts={
 createdAtDesc:[["q.created_at","DESC"]],createdAtAsc:[["q.created_at","ASC"]],
 confidenceDesc:[["q.confidence","DESC"],["q.created_at","DESC"]],confidenceAsc:[["q.confidence","ASC"],["q.created_at","DESC"]],
 titleAsc:[["q.title","ASC"],["q.created_at","DESC"]],titleDesc:[["q.title","DESC"],["q.created_at","DESC"]],warningCountDesc:[["q.warning_count","DESC"],["q.unresolved_count","DESC"],["q.created_at","DESC"]],
 unresolvedReferenceDesc:[["q.unresolved_count","DESC"],["q.warning_count","DESC"],["q.created_at","DESC"]],criticalExceptionDesc:[["q.critical_count","DESC"],["q.warning_count","DESC"],["q.created_at","DESC"]],
 approvalReadinessDesc:[["q.approval_score","DESC"],["q.created_at","DESC"]],
 reviewerReadinessDesc:[["q.reviewer_ready","DESC"],["q.risk_rank","DESC"],["q.approval_score","DESC"],["q.created_at","DESC"]],
 reviewerEffortAsc:[["q.effort_rank","ASC"],["g.member_count","DESC"],["q.approval_score","DESC"],["q.unresolved_count","ASC"],["q.created_at","DESC"]],
 batchabilityDesc:[["g.member_count","DESC"],["q.unresolved_count","DESC"],["q.approval_score","DESC"],["q.created_at","DESC"]],
 unresolvedLeverageDesc:[["-q.effort_rank","ASC"],["g.member_count","DESC"],["q.unresolved_count","ASC"],["q.approval_score","DESC"],["q.created_at","DESC"]],
 blocked37xBatchKeyAsc:[["q.batch_key","ASC"],["q.batch_safe","DESC"],["q.unresolved_count","DESC"],["json_extract(q.payload_json,'$.unresolvedUnsafe37xCount')","DESC"],["q.created_at","DESC"]]
} as const;
const booleanOption=z.boolean().optional();
const stringOption=z.string().min(1).max(200).optional();
const optionsSchema=z.object({
 status:z.enum(['all','staged','searchable','approved','rejected','pending']).default('all'),fileType:z.enum(['decision_docx','law_pdf']).optional(),
 hasWarnings:booleanOption,missingRequired:booleanOption,unresolvedReferencesOnly:booleanOption,criticalExceptionsOnly:booleanOption,
 filteredNoiseOnly:booleanOption,lowConfidenceTaxonomyOnly:booleanOption,missingRulesOnly:booleanOption,missingOrdinanceOnly:booleanOption,
 approvalReadyOnly:booleanOption,reviewerReadyOnly:booleanOption,unresolvedTriageBucket:stringOption,recurringCitationFamily:stringOption,
 blocked37xOnly:booleanOption,blocked37xFamily:stringOption,blocked37xBatchKey:stringOption,safeToBatchReviewOnly:booleanOption,
 estimatedReviewerEffort:z.enum(['low','medium','high']).optional(),reviewerRiskLevel:z.enum(['low','medium','high']).optional(),
 blocker:stringOption,runtimeManualCandidatesOnly:booleanOption,realOnly:booleanOption,taxonomyCaseTypeId:stringOption,query:stringOption,
 sort:z.enum(Object.keys(sorts) as [keyof typeof sorts,...(keyof typeof sorts)[]]).default('createdAtDesc'),
 limit:z.number().int().min(1).max(2000).default(50).transform(value=>Math.min(value,100)),cursor:z.string().min(1).max(8192).optional(),
 excludeDocumentId:z.string().min(1).max(200).optional(),
 triageGroupKey:z.string().regex(/^[a-f0-9]{64}$/).optional()
}).strict();
const cursorSchema=z.object({v:z.literal(1),epoch:z.number().int().positive().safe(),digest:z.string().length(64),
 keys:z.array(z.union([z.string().max(2048),z.number().finite()])).max(6)}).strict();
function encode(value:unknown){return btoa(String.fromCharCode(...new TextEncoder().encode(JSON.stringify(value)))).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');}
function decode(value:string){try{return cursorSchema.parse(JSON.parse(new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(Uint8Array.from(atob(value.replaceAll('-','+').replaceAll('_','/')),char=>char.charCodeAt(0)))));}catch{throw new RequestValidationError('Invalid reviewer cursor. Restart the queue.');}}
const field=(path:string)=>`json_extract(q.payload_json,'$.${path}')`;
const keyType=(column:string)=>column==='q.created_at'||column==='q.title'||column==='q.batch_key'||column==='q.id'?'string':'number';

const summaryFields={
 approved:`${field('approvedAt')} IS NOT NULL`,rejected:`${field('rejectedAt')} IS NOT NULL`,searchable:`${field('searchableAt')} IS NOT NULL`,staged:`${field('searchableAt')} IS NULL`,
 missingRequired:`${field('fileType')}='decision_docx' AND (${field('qcPassed')}=0 OR ${field('qcRequiredConfirmed')}=0)`,
 withWarnings:'q.warning_count>0',withUnresolvedReferences:'q.unresolved_count>0',withCriticalExceptions:'q.critical_count>0',
 withFilteredNoise:`${field('filteredNoiseCount')}>0`,withLowConfidenceTaxonomy:`${field('lowConfidenceTaxonomy')}=1`,
 withMissingRulesDetection:`${field('missingRulesDetection')}=1`,withMissingOrdinanceDetection:`${field('missingOrdinanceDetection')}=1`,
 approvalReady:`${field('approvalReadiness.eligible')}=1`,reviewerReady:'q.reviewer_ready=1',likelyFixtures:`${field('isLikelyFixture')}=1`,realDocs:`${field('isLikelyFixture')}=0`,
 realApprovalReady:`${field('isLikelyFixture')}=0 AND ${field('approvalReadiness.eligible')}=1`,realReviewerReady:`${field('isLikelyFixture')}=0 AND q.reviewer_ready=1`,
 realApproved:`${field('isLikelyFixture')}=0 AND ${field('approvedAt')} IS NOT NULL`,realSearchable:`${field('isLikelyFixture')}=0 AND ${field('searchableAt')} IS NOT NULL`,
 surfacedRuntimeManualCandidates:`${field('runtimeSurfaceForManualReview')}=1`,surfacedRuntimeManualRealCandidates:`${field('runtimeSurfaceForManualReview')}=1 AND ${field('isLikelyFixture')}=0`,
 surfacedRuntimeManualFixtureCandidates:`${field('runtimeSurfaceForManualReview')}=1 AND ${field('isLikelyFixture')}=1`,
 unsafeRuntimeManualSurfacedViolations:`${field('runtimeSurfaceForManualReview')}=1 AND json_array_length(q.payload_json,'$.blocked37xReferences')>0`,
 unsafeRuntimeManualSuppressedCount:`${field('runtimeSurfaceForManualReview')}=0 AND json_array_length(q.payload_json,'$.blocked37xReferences')>0`
};
type QueueSummary={total:number;[key:string]:unknown};

export async function listIngestionDocuments(env:Env,input:ListIngestionDocumentsOptions={}) {
 const options=optionsSchema.parse(input);
 const state=await inspectReviewerQueue(env);
 if(!state.ready)throw new ReviewerQueueUnavailableError('Reviewer queue refresh is pending. Complete the bounded reviewer-index steps, then retry.');
 const {cursor,limit,...scope}=options;
 const digest=await sha256Bytes(new TextEncoder().encode(JSON.stringify({policy:REVIEWER_POLICY_VERSION,scope})));
 const order:Array<readonly [string,string]>=[...sorts[options.sort],['q.id','ASC']];
 const position=cursor?decode(cursor):null;
 if(position && (position.epoch!==state.epoch || position.digest!==digest))throw new RequestConflictError('Reviewer queue or filters changed. Restart from the first page.');
 if(position && (position.keys.length!==order.length || position.keys.some((value,index)=>typeof value!==keyType(order[index]![0]))))throw new RequestValidationError('Invalid reviewer cursor position.');
 const predicates:string[]=[];const binds:unknown[]=[];
 const add=(sql:string,...values:unknown[])=>{predicates.push(`(${sql})`);binds.push(...values);};
 const equal=(path:string,value:unknown)=>add(`${field(path)}=?`,value);
 const member=(path:string,value:unknown)=>add(`EXISTS(SELECT 1 FROM json_each(q.payload_json,'$.${path}') WHERE value=?)`,value);
 switch(options.status){
  case 'staged':add(`${field('searchableAt')} IS NULL`);break;case 'searchable':add(`${field('searchableAt')} IS NOT NULL`);break;
  case 'approved':add(`${field('approvedAt')} IS NOT NULL`);break;case 'rejected':add(`${field('rejectedAt')} IS NOT NULL`);break;
  case 'pending':add(`${field('approvedAt')} IS NULL AND ${field('rejectedAt')} IS NULL`);break;
 }
 if(options.fileType)equal('fileType',options.fileType);
 if(options.hasWarnings)add('q.warning_count>0');
 if(options.missingRequired)add(`${field('fileType')}='decision_docx' AND (${field('qcHasIndexCodes')}=0 OR ${field('qcHasRulesSection')}=0 OR ${field('qcHasOrdinanceSection')}=0)`);
 if(options.missingRulesOnly)add(`${field('fileType')}='decision_docx' AND ${field('qcHasRulesSection')}=0`);
 if(options.missingOrdinanceOnly)add(`${field('fileType')}='decision_docx' AND ${field('qcHasOrdinanceSection')}=0`);
 if(options.unresolvedReferencesOnly)add('q.unresolved_count>0');
 if(options.criticalExceptionsOnly)add('q.critical_count>0');
 if(options.filteredNoiseOnly)add(`${field('filteredNoiseCount')}>0`);
 if(options.lowConfidenceTaxonomyOnly)equal('lowConfidenceTaxonomy',1);
 if(options.realOnly)equal('isLikelyFixture',0);
 if(options.runtimeManualCandidatesOnly)equal('runtimeSurfaceForManualReview',1);
 if(options.approvalReadyOnly)equal('approvalReadiness.eligible',1);
 if(options.reviewerReadyOnly)add('q.reviewer_ready=1');
 if(options.reviewerRiskLevel)equal('reviewerRiskLevel',options.reviewerRiskLevel);
 if(options.estimatedReviewerEffort)equal('estimatedReviewerEffort',options.estimatedReviewerEffort);
 if(options.unresolvedTriageBucket)member('unresolvedBuckets',options.unresolvedTriageBucket);
 if(options.recurringCitationFamily)member('recurringCitationFamilies',options.recurringCitationFamily);
 if(options.blocker)member('approvalReadiness.blockers',options.blocker);
 if(options.blocked37xOnly)add("json_array_length(q.payload_json,'$.blocked37xReferences')>0");
 if(options.blocked37xFamily)add("EXISTS(SELECT 1 FROM json_each(q.payload_json,'$.blocked37xReferences') WHERE json_extract(value,'$.family')=?)",options.blocked37xFamily);
 if(options.blocked37xBatchKey)add('q.batch_key=?',options.blocked37xBatchKey);
 if(options.safeToBatchReviewOnly)add('q.batch_safe=1');
 if(options.taxonomyCaseTypeId)equal('taxonomySuggestion.caseTypeId',options.taxonomyCaseTypeId);
 if(options.triageGroupKey)add('q.group_key=?',options.triageGroupKey);
 if(options.excludeDocumentId)add('q.id<>?',options.excludeDocumentId);
 if(options.query?.trim())add(`(q.title LIKE ? OR ${field('citation')} LIKE ? OR ${field('caseNumber')} LIKE ?)`,...Array(3).fill(`%${options.query.trim()}%`));
 const where=predicates.length?'WHERE '+predicates.join(' AND '):'';
 const from='FROM reviewer_queue_state q JOIN reviewer_queue_groups g ON g.group_key=q.group_key';
 const summary=await env.DB.prepare(`SELECT COUNT(*) AS total,${Object.entries(summaryFields).map(([key,sql])=>`COALESCE(SUM(CASE WHEN ${sql} THEN 1 ELSE 0 END),0) AS ${key}`).join(',')} ${from} ${where}`)
  .bind(...binds).first<QueueSummary>();
 const blockerBreakdown=(await env.DB.prepare(`SELECT b.value AS blocker,COUNT(*) AS count ${from},json_each(q.payload_json,'$.approvalReadiness.blockers') b ${where} GROUP BY b.value ORDER BY count DESC,b.value`)
  .bind(...binds).all<{blocker:string;count:number}>()).results;
 const pagePredicates=predicates.slice();const pageBinds=binds.slice();
 if(position){
  const terms=order.map(([column,direction],index)=>{
   const prefix=order.slice(0,index).map(([previous],i)=>{pageBinds.push(position.keys[i]);return `${previous}=?`;});
   pageBinds.push(position.keys[index]);return `(${[...prefix,`${column}${direction==='ASC'?'>':'<'}?`].join(' AND ')})`;
  });pagePredicates.push(`(${terms.join(' OR ')})`);
 }
 const pageWhere=pagePredicates.length?'WHERE '+pagePredicates.join(' AND '):'';
 const rows=(await env.DB.prepare(`SELECT q.payload_json AS payload,g.member_count-1 AS peerCount,
  ${order.map(([column],index)=>`${column} AS k${index}`).join(',')} ${from} ${pageWhere}
  ORDER BY ${order.map(([column,direction])=>`${column} ${direction}`).join(',')} LIMIT ?`).bind(...pageBinds,limit+1)
  .all<{payload:string;peerCount:number;[key:string]:unknown}>()).results;
 const pageRows=rows.slice(0,limit);
 if(pageRows.some(row=>order.some(([column],index)=>{
   const value=row[`k${index}`];
   return typeof value!==keyType(column)||(typeof value==='number'?!Number.isFinite(value):typeof value==='string'&&value.length>2048);
 })))throw new ReviewerQueueUnavailableError('Reviewer queue contains invalid sort values. Repair the source and refresh the queue.');
 const documents=pageRows.map(row=>({...JSON.parse(row.payload),peerCount:row.peerCount}) as ReviewerDocument);
 const latest=await inspectReviewerQueue(env);
 if(!latest.ready || latest.epoch!==state.epoch)throw new RequestConflictError('Reviewer queue changed while loading. Restart from the first page.');
 const last=pageRows.at(-1);
 const nextCursor=rows.length>limit&&last?encode({v:1,epoch:state.epoch,digest,keys:order.map((_,index)=>last[`k${index}`])}):null;
 return {documents,summary:{...summary,total:summary?.total??0,scope:'matching_queue',countKind:'exact',blockerBreakdown},
  pageSummary:{total:documents.length},nextCursor,page:{limit,count:documents.length,epoch:state.epoch,policyVersion:REVIEWER_POLICY_VERSION,complete:!nextCursor}};
}
