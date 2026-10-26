// Run with playwright-cli run-code --filename apps/web/tests/browser/reviewer-races.js.
// Requires the local web dev server on localhost:5555. Every API request is mocked.
async (page) => {
  const check = (condition, message) => { if (!condition) throw new Error(message); };
  const requests = [];
  const holds = new Map();
  const docs = Object.fromEntries(['A', 'B'].map(id => [id, {
    id, documentRevision: 7, title: `Review ${id}`, citation: `CASE-${id}`, fileType: 'decision_docx',
    sourceFileRef: `local-${id}`, indexCodes: ['G10'], rulesSections: ['1.10'], ordinanceSections: ['37.9'],
    caseNumber: `CASE-${id}`, authorName: `AUTHOR-${id}`, outcomeLabel: 'unclear', qcRequiredConfirmed: 1,
    approvalReadiness: {eligible:true,score:100,blockers:[],cautions:[]},
    taxonomySuggestion: {caseTypeId:'test', confidence:1, fallback:false},
    sections:[], chunks:[], referenceIssues:[], extractionWarnings:[], blocked37xReferences:[],
    reviewerRiskLevel:'low', estimatedReviewerEffort:'low', unresolvedReferenceCount:0
  }]));
  function hold(key, status = 200) {
    let release, started;
    const gate = new Promise(resolve => { release = resolve; });
    const ready = new Promise(resolve => { started = resolve; });
    holds.set(key, {gate, started, status});
    return {release, ready};
  }
  const headers = {'access-control-allow-origin':'http://localhost:5555','access-control-allow-credentials':'true','access-control-allow-headers':'content-type','access-control-allow-methods':'GET,POST,OPTIONS'};
  await page.unroute('http://127.0.0.1:8787/**');
  await page.route('http://127.0.0.1:8787/**', async route => {
    const request = route.request(); const path = request.url().replace('http://127.0.0.1:8787','').split('?')[0];
    if(request.method()==='OPTIONS')return route.fulfill({status:204,headers});
    const match = path.match(/\/admin\/ingestion\/documents\/([AB])(?:\/(metadata|approve|reject))?$/);
    if(!match)return route.fulfill({status:200,headers,json:{documents:Object.values(docs),summary:null}});
    const [,id,action] = match; const key = `${request.method()} ${id}${action?' '+action:''}`;
    const body = action ? request.postDataJSON() : null;
    requests.push({key,body}); const snapshot = JSON.parse(JSON.stringify(docs[id]));
    const delayed=holds.get(key); if(delayed){holds.delete(key);delayed.started();await delayed.gate;}
    if(delayed?.status >= 400)return route.fulfill({status:delayed.status,headers,json:{error:`Fixture failure for ${id}`}});
    if(action){
      if(action==='metadata') { docs[id].caseNumber=body.case_number;docs[id].authorName=body.author_name; }
      docs[id].documentRevision++;
    }
    return route.fulfill({status:200,headers,json:action?{ok:true}:snapshot});
  });
  const select = id => page.getByRole('button',{name:`Review ${id}`,exact:true}).click();
  const loaded = id => page.getByRole('heading',{name:`Review ${id}`,exact:true}).waitFor();
  const caseInput = page.getByPlaceholder('Case Number',{exact:true});
  const save = page.getByRole('button',{name:'Save Metadata',exact:true});
  const reload = page.getByRole('button',{name:'Reload selected document',exact:true});
  const waitEnabled = async locator => { await locator.waitFor(); await page.waitForFunction(element=>!element.disabled && !element.closest('fieldset:disabled'),await locator.elementHandle()); };
  await page.goto('http://localhost:5555/admin/ingestion');
  await select('A'); await loaded('A');

  // A's fields disappear immediately while B is loading. A cannot be submitted as B.
  let held=hold('GET B'); await select('B'); await held.ready;
  check(await caseInput.count()===0,'Old form remained visible during selection');
  check(await save.count()===0,'Old save action remained available');
  held.release(); await loaded('B'); check(await caseInput.inputValue()==='CASE-B','B received A values');

  // Late A success cannot beat B; a failed B load never exposes A.
  held=hold('GET A'); await select('A');await held.ready;await select('B');await loaded('B');held.release();
  await page.getByText('Loading detail...', {exact:true}).waitFor({state:'hidden'});
  check(await caseInput.inputValue()==='CASE-B','Late A load replaced B');
  await select('A');await loaded('A');held=hold('GET B',503);await select('B');await held.ready;held.release();
  await page.getByText('The service hit a temporary problem. Please try again.',{exact:true}).waitFor();
  check(await save.count()===0 && await caseInput.count()===0,'Failed B load retained actionable A');
  await reload.click();await loaded('B');

  // Unsaved changes can cancel navigation without a second bubbled dialog.
  await select('A');await loaded('A');await caseInput.fill('A-unsaved');
  let dialogs=0;const dismiss=async dialog=>{dialogs++;await dialog.dismiss();};page.on('dialog',dismiss);
  await select('B');page.off('dialog',dismiss);
  check(dialogs===1,'Cancel triggered duplicate discard dialogs');
  check(await caseInput.inputValue()==='A-unsaved','Cancel discarded edits');
  page.once('dialog',dialog=>dialog.accept());await select('B');await loaded('B');

  // Save A, switch to B, then release A. Repeated click is synchronously fenced.
  await select('A');await loaded('A');await caseInput.fill('A-saved');
  held=hold('POST A metadata');const before=requests.filter(x=>x.key==='POST A metadata').length;
  await save.evaluate(button=>{button.click();button.click();button.click();});await held.ready;
  check(await save.isDisabled(),'Save remained enabled in flight');
  await select('B');await loaded('B');const beforeReload=requests.filter(x=>x.key==='GET A').length;
  held.release();await page.getByText('Review A (A): metadata saved.',{exact:true}).waitFor();await waitEnabled(save);
  check(requests.filter(x=>x.key==='POST A metadata').length===before+1,'Repeated submission escaped the fence');
  check(requests.filter(x=>x.key==='GET A').length===beforeReload,'A completion requested a stale detail reload');
  check(await caseInput.inputValue()==='CASE-B','A save completion overwrote B form');
  const saved=requests.filter(x=>x.key==='POST A metadata').at(-1);check(saved.body.case_number==='A-saved','Snapshot values were not saved to A');

  // Approval and rejection also retain their original document identity.
  for(const action of ['approve','reject']){
    await select('A');await loaded('A');
    if(action==='reject')await page.getByPlaceholder('Reject reason',{exact:true}).fill('Review fixture rejection');
    held=hold(`POST A ${action}`);await page.getByRole('button',{name:action==='approve'?'Approve for Search':'Reject',exact:true}).click();await held.ready;
    await select('B');await loaded('B');held.release();
    await page.getByText(`Review A (A): ${action==='approve'?'approved':'rejected'}.`,{exact:true}).waitFor();await waitEnabled(save);
    check(await caseInput.inputValue()==='CASE-B',`${action} completion replaced B`);
    if(action==='approve')check(requests.filter(x=>x.key==='POST A approve').at(-1).body.expectedRevision===docs.A.documentRevision-1,'Approval lost captured revision');
  }

  // Returning to A is a new session: completion must not overwrite it; require explicit reload.
  await select('A');await loaded('A');await caseInput.fill('A-returned');held=hold('POST A metadata');await save.click();await held.ready;
  await select('B');await loaded('B');await select('A');await loaded('A');held.release();
  await page.getByText('This document changed in an earlier review session. Reload the selected document before making another change.',{exact:true}).waitFor();
  check(await save.isDisabled(),'Returned session could write stale values');await reload.click();await loaded('A');
  check(await caseInput.inputValue()==='A-returned','Explicit reload did not fetch committed A');

  // A failed mutation preserves edits, identifies A, and never contaminates B.
  await caseInput.fill('A-retry');held=hold('POST A metadata',503);await save.click();await held.ready;held.release();
  await page.getByText(/Review A \(A\): The service hit a temporary problem/).waitFor();await waitEnabled(save);
  check(await caseInput.inputValue()==='A-retry','Failed save lost unsaved edits');
  check(requests.filter(x=>x.key.startsWith('POST B')).length===0,'An A action wrote B');
  // A's completed write can leave a slow detail refresh. B can start its own
  // write, and finishing that old A read must not release B's mutation lock.
  page.once('dialog',dialog=>dialog.accept());await reload.click();await loaded('A');await caseInput.fill('A-followup');
  held=hold('POST A metadata');await save.click();await held.ready;
  const slowRead=hold('GET A');held.release();await slowRead.ready;
  await select('B');await loaded('B');await waitEnabled(save);
  await caseInput.fill('B-own-edit');const bWrite=hold('POST B metadata');await save.click();await bWrite.ready;
  const oldResponse=page.waitForResponse(response=>response.url().endsWith('/documents/A')&&response.request().method()==='GET');
  slowRead.release();await (await oldResponse).finished();
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>resolve())));
  check(await save.isDisabled(),'Old A refresh released B mutation lock');
  check(await caseInput.inputValue()==='B-own-edit','Old A refresh replaced B values');
  bWrite.release();await page.getByText('Review B (B): metadata saved.',{exact:true}).waitFor();await waitEnabled(save);
  check(requests.filter(x=>x.key==='POST B metadata').at(-1).body.case_number==='B-own-edit','B write lost its own snapshot');
  await page.screenshot({path:'output/playwright/h18-reviewer-races.png',fullPage:true});
  await page.unroute('http://127.0.0.1:8787/**');
  const result = {passed:true, scenarios:10, mutations:requests.filter(x=>x.key.startsWith('POST')).map(x=>x.key)};
  await page.evaluate(result => { window.__reviewerRaceResult = result; }, result);
  return result;
}
