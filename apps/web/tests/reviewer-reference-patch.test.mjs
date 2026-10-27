import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from '../../api/tests/helpers/load-ts.mjs';
const {reviewerReferencePatch}=loadTs('../web/src/lib/reviewer-reference-patch.ts');
const {adminIngestionMetadataUpdateSchema}=loadTs('../../packages/shared/src/index.ts');
const baseline={indexCodes:Array.from({length:20},(_,i)=>'IC-'+i).join(', '),rulesSections:Array.from({length:30},(_,i)=>'Rule '+i).join(', '),ordinanceSections:'37.3, 37.9'};
const validate=patch=>adminIngestionMetadataUpdateSchema.parse({expectedRevision:1,mutationId:crypto.randomUUID(),...patch});
test('confirmation and scalar edits preserve unchanged large legacy reference arrays',()=>{
 const patch=reviewerReferencePatch({...baseline},baseline);
 assert.deepEqual(patch,{});
 assert.equal(validate({...patch,confirm_required_metadata:true}).confirm_required_metadata,true);
 assert.equal(validate({...patch,case_number:'corrected case'}).case_number,'corrected case');
});
test('changing or clearing a short array does not resubmit unrelated large references',()=>{
 assert.deepEqual(validate(reviewerReferencePatch({...baseline,ordinanceSections:'37.3, 37.8'},baseline)).ordinance_sections,['37.3','37.8']);
 assert.deepEqual(reviewerReferencePatch({...baseline,ordinanceSections:''},baseline),{ordinance_sections:[]});
});
test('formatting-only edits preserve the existing array, including its order',()=>{
 assert.deepEqual(reviewerReferencePatch({...baseline,ordinanceSections:' 37.3 ,37.9, '},baseline),{});
 assert.deepEqual(reviewerReferencePatch({...baseline,ordinanceSections:'37.9,37.3'},baseline),{ordinance_sections:['37.9','37.3']});
});
test('editing a large legacy array remains bounded and never silently truncates citations',()=>{
 const patch=reviewerReferencePatch({...baseline,rulesSections:baseline.rulesSections+', Rule 31'},baseline);
 assert.equal(patch.rules_sections.length,31);
 assert.throws(()=>validate(patch));
 assert.equal(baseline.rulesSections.split(',').length,30);
});
