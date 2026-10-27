import { z } from "zod";
import { extractCitations } from "./legal-citations";
import type { AssistantChatResponse, SearchResponse } from "@beedle/shared";

export type AssistantDecision = Pick<SearchResponse['results'][number],
 'documentId'|'title'|'citation'|'authorName'|'sourceLink'|'primaryAuthorityPassage'|'supportingFactPassage'|'matchedPassage'|'snippet'|'score'> & {additionalPassages?:Array<NonNullable<SearchResponse['results'][number]['matchedPassage']>>};
export type AssistantEvidence = {id:string;documentId:string;chunkId:string;sectionLabel:string;citationAnchor:string;paragraphAnchor:string;text:string;kind:string};
const compact=(value:string)=>value.replace(/\s+/g,' ').trim();
const modelSchema=z.object({claims:z.array(z.object({text:z.string().trim().min(1).max(1200),
 evidence:z.array(z.object({sourceId:z.string().min(1).max(40),quote:z.string().trim().min(20).max(1200)}).strict()).min(1).max(3)}).strict()).min(1).max(8)}).strict();
export class AssistantEvidenceError extends Error {constructor(readonly reason:string){super(reason);}}

export function assistantEvidence(decisions:AssistantDecision[]):AssistantEvidence[] {
 const evidence:AssistantEvidence[]=[];const seen=new Set<string>();
 for(const decision of decisions) {
  for(const [kind,passage] of [['authority_candidate',decision.primaryAuthorityPassage],['fact_candidate',decision.supportingFactPassage],['retrieved_match',decision.matchedPassage],...(decision.additionalPassages||[]).map(passage=>['additional_match',passage] as const)] as const) {
   if(!passage?.snippet)continue;
   const key=JSON.stringify([decision.documentId,passage.chunkId]);if(seen.has(key))continue;seen.add(key);
   evidence.push({id:`S${evidence.length+1}`,documentId:decision.documentId,chunkId:passage.chunkId,
    sectionLabel:passage.sectionLabel,citationAnchor:passage.citationAnchor,paragraphAnchor:passage.paragraphAnchor,
    text:compact(passage.snippet),kind});
  }
 }
 return evidence;
}
const stopWords=new Set(['the','and','for','that','this','with','from','was','were','are','has','had','have','which','their','there','these','those']);
const words=(text:string)=>new Set((text.toLowerCase().match(/[a-z]{3,}/g)||[]).filter(word=>!stopWords.has(word)));
const negative=(text:string)=>/\b(no|not|never|without|cannot|denied|deny|failed|fails)\b|n['’]t\b/i.test(text);

export function parseAssistantClaims(content:string,evidence:AssistantEvidence[],decisions:AssistantDecision[]):AssistantChatResponse['claims'] {
 let parsed:z.infer<typeof modelSchema>;
 try {parsed=modelSchema.parse(JSON.parse(content));}catch{throw new AssistantEvidenceError('invalid_claim_structure');}
 const known=new Map(evidence.map(source=>[source.id,source]));
 return parsed.claims.map((claim,index)=>{
  const spans=claim.evidence.map(item=>{
   const source=known.get(item.sourceId);if(!source)throw new AssistantEvidenceError('unknown_evidence_id');
   const quote=compact(item.quote);const start=source.text.indexOf(quote);
   if(start<0)throw new AssistantEvidenceError('quote_not_in_evidence');
   return {evidenceId:source.id,documentId:source.documentId,chunkId:source.chunkId,sectionLabel:source.sectionLabel,
    citationAnchor:source.citationAnchor,paragraphAnchor:source.paragraphAnchor,quote,excerpt:source.text,start,end:start+quote.length,
    offsetBasis:'normalized_retrieved_excerpt' as const,occurrenceVerified:true as const};
  }).filter((span,index,all)=>all.findIndex(other=>other.evidenceId===span.evidenceId && other.start===span.start && other.end===span.end)===index);
  const quotes=spans.map(span=>span.quote).join(' ');
  for(const match of claim.text.matchAll(/\[([^\]]+)\]/g)) {
   if(quotes.includes(match[0]))continue; // An original footnote is excerpt text, not a new source ID.
   if(match[1]!.split(',').some(id=>!spans.some(span=>span.evidenceId===id.trim())))throw new AssistantEvidenceError('unknown_evidence_id');
  }
  const claimWords=words(claim.text),sourceWords=words(quotes);
  const common=[...claimWords].filter(word=>sourceWords.has(word));
  if(common.length<Math.min(3,claimWords.size)||common.length/Math.max(claimWords.size,1)<0.35)throw new AssistantEvidenceError('claim_not_related_to_quotes');
  for(const sentence of claim.text.split(/(?<=[.!?])\s+(?=[A-Z“"0-9])/)) {
   const sentenceWords=words(sentence),overlap=[...sentenceWords].filter(word=>sourceWords.has(word)).length;
   if(overlap<Math.min(3,sentenceWords.size)||overlap/Math.max(sentenceWords.size,1)<0.35)throw new AssistantEvidenceError('claim_not_related_to_quotes');
  }
  if(negative(claim.text)!==negative(quotes))throw new AssistantEvidenceError('possible_contradiction');
  const provisions=extractCitations(claim.text).filter(citation=>citation.canonical);
  const sourceProvisions=extractCitations(quotes);
  if(provisions.some(citation=>!sourceProvisions.some(source=>source.context===citation.context && source.canonical===citation.canonical && source.part===citation.part))) {
   throw new AssistantEvidenceError('provision_not_in_quotes');
  }
  const metadata=decisions.filter(decision=>spans.some(span=>span.documentId===decision.documentId)).map(decision=>`${decision.title} ${decision.citation}`).join(' ');
  const numberPattern=/\b\d+(?:\.\d+)*(?:\([a-z0-9]+\))*/gi;
  const numbers=claim.text.match(numberPattern)||[];const sourceNumbers=new Set((`${quotes} ${metadata}`.match(numberPattern)||[]).map(value=>value.toLowerCase()));
  if(numbers.some(number=>!sourceNumbers.has(number.toLowerCase())))throw new AssistantEvidenceError('unmatched_number_or_provision');
  return {id:`C${index+1}`,text:claim.text,status:'unverified' as const,flags:['semantic_support_unverified'],evidence:spans};
 });
}

export function extractiveAssistantClaims(evidence:AssistantEvidence[]):AssistantChatResponse['claims'] {
 const seen=new Set<string>();
 return evidence.filter(source=>{if(seen.has(source.documentId))return false;seen.add(source.documentId);return true;}).slice(0,5).map((source,index)=>({
  id:`C${index+1}`,text:source.text,status:'source_excerpt' as const,flags:['relevance_requires_review'],evidence:[{
   evidenceId:source.id,documentId:source.documentId,chunkId:source.chunkId,sectionLabel:source.sectionLabel,citationAnchor:source.citationAnchor,
   paragraphAnchor:source.paragraphAnchor,quote:source.text,excerpt:source.text,start:0,end:source.text.length,
   offsetBasis:'normalized_retrieved_excerpt' as const,occurrenceVerified:true as const
  }]
 }));
}
export function renderAssistantClaims(claims:AssistantChatResponse['claims'],extractive:boolean,scopeLabel:string) {
 if(!claims.length)return `No source excerpts were available for a supported answer. Scope searched: ${scopeLabel}. Try adjusting the question or filters.`;
 return [extractive?'Retrieved excerpts for review. Their relevance to your question has not been verified.':'Generated research notes — unverified. Check each claim against its quoted passage and the original decision.',
  ...claims.map(claim=>`${claim.text} [${[...new Set(claim.evidence.map(span=>span.evidenceId))].join(', ')}]`)].join('\n\n');
}
