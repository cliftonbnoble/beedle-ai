import { test, expect, type Page } from '@playwright/test';
const api = 'http://127.0.0.1:8819';
let readerIds: { alpha: string; beta: string; race: string; staged: string; provisional: string };
test.beforeAll(async ({ request }) => {
  const seeded = await (await request.get(api + '/__test/seeded')).json();
  expect(seeded.fixture).toBe('beedle-hermetic-v1');
  const source = '# Index Codes\n\n100\n\n# Rules\n\nRule 1.2\n\n# Ordinance\n\nOrdinance 37.8\n\n# Findings of Fact\n\nThe tenant gave the landlord written repair notice about a roof leak. The landlord received the notice and disputed when repairs were completed.\n\n# Conclusions of Law\n\nThe Board reviewed the repair notice and the complete evidentiary record under Rule 1.2 and Ordinance 37.8.';
  const ids: string[] = [];
  for (const label of ['ReaderAlpha', 'ReaderBeta', 'ReaderRace', 'ReaderProvisional']) {
    const ingest = await request.post(api + '/ingest/decision', { data: { idempotencyKey: crypto.randomUUID(), fileType: 'decision_docx', jurisdiction: 'Test', title: 'Rent Board case ' + label, citation: 'T-' + label.toUpperCase(), performVectorUpsert: false,
      sourceFile: { filename: label + '.md', mimeType: 'text/markdown', bytesBase64: Buffer.from(source).toString('base64') } } });
    expect(ingest.status()).toBe(201); const { documentId } = await ingest.json(); ids.push(documentId);
    if (label === 'ReaderProvisional') {
      const published = await request.post(api + '/__test/publish-provisional', { data: { documentId } });
      expect(published.status()).toBe(200);
      expect((await (await request.get(api + '/decisions/' + documentId + '/chunks')).json()).document.corpusTier).toBe('provisional');
      continue;
    }
    expect((await request.post(api + '/__test/taxonomy', { data: { documentId, fallback: false, confidence: 0.2 } })).ok()).toBe(true);
    const url = api + '/admin/ingestion/documents/' + documentId, detail = await (await request.get(url)).json();
    const saved = await request.post(url + '/metadata', { data: { expectedRevision: detail.documentRevision, mutationId: crypto.randomUUID(), index_codes: ['100'], rules_sections: ['1.2'], ordinance_sections: ['37.8'], author_name: 'Erin E. Katayama', case_number: 'T-' + label.toUpperCase(), confirm_required_metadata: true } });
    expect(saved.ok()).toBe(true);
    expect((await request.post(url + '/approve', { data: { expectedRevision: (await saved.json()).documentRevision } })).ok()).toBe(true);
  }
  readerIds = { alpha: ids[0]!, beta: ids[1]!, race: ids[2]!, provisional: ids[3]!, staged: seeded.seedIds.staged };
});
function latch() { let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve; }); return { promise, release }; }
const path = (id: string, query = '') => '/search/decision/' + encodeURIComponent(id) + (query ? '?query=' + encodeURIComponent(query) : '');
async function changeQuery(page: Page, query: string) {
  await page.evaluate(query => { const url = new URL(location.href); if (query) url.searchParams.set('query', query); else url.searchParams.delete('query'); history.pushState(null, '', url); }, query);
}
test.beforeEach(async ({ context, request }) => {
  await context.route('**/*', route => ['127.0.0.1', 'localhost'].includes(new URL(route.request().url()).hostname) ? route.continue() : route.abort());
  await context.addInitScript(() => {
    const original = window.fetch.bind(window); (window as any).__readerAborts = [];
    window.fetch = (input, init) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      init?.signal?.addEventListener('abort', () => (window as any).__readerAborts.push(url), { once: true });
      return original(input, init);
    };
  });
  expect((await (await request.get(api + '/__test/seeded')).json()).fixture).toBe('beedle-hermetic-v1');
});

test('direct reader links retain the server review status even when URL filters claim trusted-only', async ({ page }) => {
  await page.goto(path(readerIds.provisional) + '?corpusMode=trusted_only&approvedOnly=1');
  await expect(page.getByRole('note', { name: 'Review status' })).toContainText('Provisional — not human approved');
  await page.goto(path(readerIds.alpha));
  await expect(page.getByRole('note', { name: 'Review status' })).toContainText('Human-approved current revision.');
});

test('an ID-only bookmark reads the published decision without retrieval, and an unpublished ID stays blocked', async ({ page }) => {
  const seedIds = readerIds; let searches = 0;
  page.on('request', request => { if (request.url() === api + '/search') searches++; });
  await page.goto(path(seedIds.alpha));
  await expect(page.getByRole('heading', { name: 'Rent Board case ReaderAlpha', exact: true })).toBeVisible();
  await expect(page.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
  await expect(page.getByText('Reading the published decision without search context.', { exact: true })).toBeVisible();
  expect(searches).toBe(0);
  await page.goto(path(seedIds.staged, 'repair notice'));
  await expect(page.getByRole('alert').filter({ hasText: 'Decision detail failed:' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Full decision', exact: true })).toHaveCount(0);
  await expect(page.locator('.decision-reader')).toHaveCount(0); expect(searches).toBe(0);
});

test('failed search matches do not hide the document; retry reuses its preview and restores passage anchors', async ({ page }) => {
  const seedIds = readerIds; let searches = 0, previews = 0;
  page.on('request', request => { if (request.url().includes('/decisions/' + seedIds.alpha + '/chunks')) previews++; });
  await page.route(api + '/search', route => ++searches === 1 ? route.fulfill({ status: 503, contentType: 'application/json', body: '{"error":"synthetic outage"}' }) : route.continue());
  await page.goto(path(seedIds.alpha, 'repair notice'));
  await expect(page.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
  await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches are unavailable. You can still read the full decision.');
  await page.getByRole('button', { name: 'Retry search matches', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches loaded');
  expect(previews).toBe(1); expect(searches).toBe(2);
  await page.evaluate(() => { const url = new URL(location.href); url.searchParams.set('limit', '25'); history.replaceState(null, '', url); });
  await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches loaded');
  expect(previews).toBe(1); expect(searches).toBe(2);
  const jump = page.getByRole('navigation', { name: 'Matched passages' }).getByRole('link').first();
  const href = await jump.getAttribute('href'); await jump.click();
  expect(await page.evaluate(fragment => Boolean(document.getElementById(decodeURIComponent(fragment!.slice(1)))), href)).toBe(true);
  await page.reload(); await expect(page.locator('.decision-reader')).toBeVisible();
  expect(await page.evaluate(() => Boolean(document.getElementById(decodeURIComponent(location.hash.slice(1)))))).toBe(true);
});

test('a slow optional search leaves text readable; changing the query aborts it without refetching the document', async ({ page }) => {
  const seedIds = readerIds; const held = latch(), entered = latch();
  let previews = 0;
  page.on('request', request => { if (request.url().includes('/decisions/' + seedIds.alpha + '/chunks')) previews++; });
  await page.route(api + '/search', async route => {
    if (route.request().postDataJSON().query !== 'repair notice') return route.continue();
    const response = await route.fetch(); entered.release(); await held.promise;
    try { await route.fulfill({ response, json: { ...(await response.json()), results: [], total: 0 } }); } catch { /* Aborted navigation is expected. */ }
  });
  try {
    await page.goto(path(seedIds.alpha, 'repair notice')); await entered.promise;
    await expect(page.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
    await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Loading search matches');
    await changeQuery(page, 'written repair');
    await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches loaded');
    await expect.poll(() => page.evaluate(() => (window as any).__readerAborts.filter((url: string) => url.endsWith('/search')).length)).toBeGreaterThan(0);
    held.release(); await expect(page.locator('.decision-reader__paragraph-frame.is-matched').first()).toBeVisible();
    await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches loaded');
    expect(previews).toBe(1);
    await changeQuery(page, 'x'); // Stale/invalid query validation is optional-context failure.
    await expect(page.getByRole('region', { name: 'Search context' })).toContainText('Search matches are unavailable');
    await expect(page.getByRole('heading', { name: 'Full decision', exact: true })).toBeVisible();
    await changeQuery(page, '');
    await expect(page.getByRole('region', { name: 'Search context' })).toHaveCount(0); expect(previews).toBe(1);
  } finally { held.release(); }
});

test('leaving a delayed document cancels its request and its late response cannot replace the next decision', async ({ page }) => {
  const seedIds = readerIds; const held = latch(), entered = latch();
  await page.route(api + '/decisions/' + seedIds.alpha + '/chunks?includeText=1', async route => {
    const response = await route.fetch(); entered.release(); await held.promise;
    try { await route.fulfill({ response }); } catch { /* The old page request was cancelled. */ }
  });
  try {
    await page.goto(path(seedIds.alpha)); await entered.promise;
    await page.getByRole('link', { name: 'Search', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'Open Search', exact: true })).toBeVisible();
    await expect.poll(() => page.evaluate(id => (window as any).__readerAborts.some((url: string) => url.includes('/decisions/' + id + '/chunks')), seedIds.alpha)).toBe(true);
    await page.goto(path(seedIds.beta)); await expect(page.getByRole('heading', { name: 'Rent Board case ReaderBeta', exact: true })).toBeVisible();
    held.release();
    await expect(page.getByRole('heading', { name: 'Rent Board case ReaderAlpha', exact: true })).toHaveCount(0);
    await expect(page.getByRole('heading', { name: 'Full decision', exact: true })).toBeVisible();
  } finally { held.release(); }
});

test('a failed document retries independently and a mismatched document response is rejected', async ({ page }) => {
  const seedIds = readerIds; let calls = 0, searches = 0;
  page.on('request', request => { if (request.url() === api + '/search') searches++; });
  const previewUrl = api + '/decisions/' + seedIds.alpha + '/chunks?includeText=1';
  await page.route(previewUrl, async route => {
    if (++calls === 1) return route.fulfill({ status: 503, contentType: 'application/json', body: '{"error":"synthetic outage"}' });
    if (calls === 2) return route.continue();
    const response = await route.fetch(), json = await response.json();
    if (calls === 3) json.document.documentId = seedIds.beta;
    else json.chunks[0].documentId = seedIds.beta;
    await route.fulfill({ response, json });
  });
  await page.goto(path(seedIds.alpha, 'repair notice'));
  await expect(page.getByRole('alert').filter({ hasText: 'Decision detail failed:' })).toBeVisible(); expect(searches).toBe(0);
  await page.getByRole('button', { name: 'Retry decision', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Rent Board case ReaderAlpha', exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByRole('alert').filter({ hasText: 'Decision detail failed:' })).toContainText('Received a different decision');
  await expect(page.locator('.decision-reader')).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole('alert').filter({ hasText: 'Decision detail failed:' })).toContainText('Received a different decision');
  await expect(page.locator('.decision-reader')).toHaveCount(0);
});

test('a corpus change between document and search cannot attach newer matches to the captured decision', async ({ page, request }) => {
  const held=latch(), entered=latch();
  await page.route(api+'/search',async route=>{entered.release();await held.promise;await route.continue();});
  try {
    await page.goto(path(readerIds.race,'repair notice'));await entered.promise;
    await expect(page.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
    const url=api+'/admin/ingestion/documents/'+readerIds.race;
    const detail=await (await request.get(url)).json();
    const edit=await request.post(url+'/metadata',{data:{expectedRevision:detail.documentRevision,mutationId:crypto.randomUUID(),case_number:'T-READER-RACE-CHANGED'}});
    expect(edit.ok(),await edit.text()).toBe(true);
    held.release();
    await expect(page.getByRole('region',{name:'Search context'})).toContainText('The decision or corpus changed. Reload the decision');
    await expect(page.getByRole('navigation',{name:'Matched passages'})).toHaveCount(0);
    await expect(page.locator('.decision-reader__paragraph-frame.is-matched')).toHaveCount(0);
    await expect(page.locator('.decision-reader')).toBeVisible();
    await page.getByRole('button',{name:'Reload decision',exact:true}).click();
    await expect(page.getByRole('alert').filter({hasText:'Decision detail failed:'})).toBeVisible();
    await expect(page.locator('.decision-reader')).toHaveCount(0);
  }finally{held.release();}
});

for (const missing of ['preview', 'search']) test(`missing ${missing} revision evidence retains text without attaching passage matches`, async ({ page }) => {
  const endpoint=missing==='preview'?api+'/decisions/'+readerIds.alpha+'/chunks?includeText=1':api+'/search';
  await page.route(endpoint,async route=>{const response=await route.fetch(),json=await response.json();delete json.corpusRevision;await route.fulfill({response,json});});
  await page.goto(path(readerIds.alpha,'repair notice'));
  await expect(page.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
  await expect(page.getByRole('button',{name:'Reload decision',exact:true})).toBeVisible();
  await expect(page.locator('.decision-reader__paragraph-frame.is-matched')).toHaveCount(0);
});
