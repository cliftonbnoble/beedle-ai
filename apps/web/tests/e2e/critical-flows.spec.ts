import { test, expect, type Page, type BrowserContext } from '@playwright/test';
const api = 'http://127.0.0.1:8819';
const composer = (page: Page) => page.getByPlaceholder('Ask about prior decisions…');
function latch() { let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve; }); return { promise, release }; }
async function records(page: Page) { return page.evaluate(() => new Promise<any[]>((resolve, reject) => { const request = indexedDB.open('beedle-assistant-v2', 1); request.onerror = () => reject(request.error); request.onsuccess = () => { const db = request.result, tx = db.transaction('threads', 'readonly'), rows = tx.objectStore('threads').getAll(); tx.oncomplete = () => { db.close(); resolve(rows.result); }; tx.onerror = () => reject(tx.error); }; })); }
async function freshChat(page: Page) { await page.goto('/case-assistant?new=1'); await expect(page).toHaveURL(/thread=/); await expect(composer(page)).toBeVisible(); await expect(page.getByText('Chats saved on this browser', { exact: true })).toBeVisible(); }
async function send(page: Page, question: string) { await composer(page).fill(question); await page.getByRole('button', { name: 'Send', exact: true }).click(); }
async function localOnly(context: BrowserContext) { await context.route('**/*', route => { const url = new URL(route.request().url()); return ['127.0.0.1', 'localhost'].includes(url.hostname) ? route.continue() : route.abort('blockedbyclient'); }); }
test.beforeEach(async ({ context, request }) => { await localOnly(context); const response = await request.get(api + '/__test/seeded'); expect(response.ok()).toBe(true); expect((await response.json()).fixture).toBe('beedle-hermetic-v1'); });
test('search submits the real scope and renders published cases without the staged case', async ({ page }) => {
    await page.goto('/search');
    await page.getByRole('textbox', { name: 'Search', exact: true }).fill('repair notice');
    const response = page.waitForResponse(response => response.url() === api + '/search' && response.request().method() === 'POST');
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    const result = await response;
    expect(result.ok()).toBe(true);
    const payload = result.request().postDataJSON();
    expect(payload.filters.approvedOnly).toBe(true);
    await expect(page.getByText('Rent Board case Alpha', { exact: true }).first()).toBeVisible();
    await expect(page.getByText('Rent Board case Beta', { exact: true }).first()).toBeVisible();
    await expect(page.getByText('Rent Board case Gamma', { exact: true })).toHaveCount(0);
    const opened=page.waitForEvent('popup');
    await page.getByRole('link',{name:'Open Decision',exact:true}).first().click();
    const reader=await opened;
    await expect(reader.getByRole('heading',{name:'Full decision',exact:true})).toBeVisible();
    await expect(reader.locator('.decision-reader')).toContainText('The tenant gave the landlord written repair notice');
    await expect(reader).toHaveURL(/\/search\/decision\/doc_/);
    await reader.reload();
    await expect(reader.getByRole('heading',{name:'Full decision',exact:true})).toBeVisible();
    await reader.close();
});
test('a delayed metadata response for Alpha cannot overwrite the selected Beta form', async ({ page, request }) => {
    const { seedIds } = await (await request.get(api + '/__test/seeded')).json();
    await page.goto('/admin/ingestion');
    await page.getByRole('button', { name: 'Rent Board case Alpha', exact: true }).click();
    await expect(page.getByPlaceholder('Case Number')).toHaveValue('T-ALPHA');
    const held = latch(), entered = latch();
    await page.route(api + '/admin/ingestion/documents/' + seedIds.alpha + '/metadata', async (route) => { const response = await route.fetch(); entered.release(); await held.promise; await route.fulfill({ response }); });
    try {
        await page.getByPlaceholder('Case Number').fill('T-ALPHA-EDITED');
        await page.getByRole('button', { name: 'Save Metadata', exact: true }).click();
        await entered.promise;
        await page.getByRole('button', { name: 'Rent Board case Beta', exact: true }).click();
        await expect(page.getByPlaceholder('Case Number')).toHaveValue('T-BETA');
        held.release();
        await expect.poll(async () => { const response = await request.get(api + '/admin/ingestion/documents/' + seedIds.alpha); return (await response.json()).caseNumber; }).toBe('T-ALPHA-EDITED');
        await expect(page.getByPlaceholder('Case Number')).toHaveValue('T-BETA');
    }
    finally {
        held.release();
    }
});
test('a late detail response cannot move the reviewer back to an earlier selection', async ({ page, request }) => {
    const { seedIds } = await (await request.get(api + '/__test/seeded')).json();
    await page.goto('/admin/ingestion');
    const held = latch(), entered = latch();
    await page.route(api + '/admin/ingestion/documents/' + seedIds.alpha, async (route) => { const response = await route.fetch(); entered.release(); await held.promise; try {
        await route.fulfill({ response });
    }
    catch { /* The client may abort the obsolete request. */ } });
    try {
        await page.getByRole('button', { name: 'Rent Board case Alpha', exact: true }).click();
        await entered.promise;
        await page.getByRole('button', { name: 'Rent Board case Beta', exact: true }).click();
        await expect(page.getByPlaceholder('Case Number')).toHaveValue('T-BETA');
        held.release();
        await expect(page.getByRole('group', { name: 'Document review' })).toBeEnabled();
        await expect(page.getByPlaceholder('Case Number')).toHaveValue('T-BETA');
    }
    finally {
        held.release();
    }
});
test('saving while a real chat response is pending keeps it live and reload records interruption', async ({ page }) => {
    await freshChat(page);
    const held = latch(), entered = latch();
    await page.route(api + '/api/assistant/chat', async (route) => { const response = await route.fetch(); entered.release(); await held.promise; try {
        await route.fulfill({ response });
    }
    catch { /* Reload closes the original request. */ } });
    try {
        await send(page, 'repair notice');
        await entered.promise;
        await expect(page.getByRole('button', { name: 'Thinking', exact: true })).toBeDisabled();
        await expect.poll(async () => { const rows = await records(page); return rows.some(row => row.thread?.interrupted && row.thread.messages.some((message: any) => message.content === 'repair notice')); }).toBe(true);
        // The durable projection strips pending state; its own save notification must
        // never hydrate that projection over the active request.
        await expect(page.getByRole('button', { name: 'Thinking', exact: true })).toBeDisabled();
        await expect(page.getByText('Chats saved on this browser', { exact: true })).toBeVisible();
        await page.reload();
        await expect(page.getByText(/A response was pending when this chat was saved/)).toBeVisible();
        await composer(page).fill('repair notice again');
        await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeEnabled();
    }
    finally {
        held.release();
    }
});
async function injectQuota(page: Page) { await page.evaluate(() => { const original = IDBDatabase.prototype.transaction; (window as any).__failWrites = true; IDBDatabase.prototype.transaction = function (...args: any[]) { if ((window as any).__failWrites && args[1] === 'readwrite')
    throw new DOMException('Synthetic quota boundary', 'QuotaExceededError'); return original.apply(this, args as any); }; }); }
test('quota failures preserve the completed response in memory and retry saves it durably', async ({ page }) => {
    await freshChat(page);
    await injectQuota(page);
    await send(page, 'repair notice');
    await expect(page.getByRole('button', { name: 'Retry saving', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Thinking', exact: true })).toHaveCount(0);
    await expect.poll(async () => page.locator('article').filter({ has: page.locator('strong', { hasText: 'ASSISTANT' }) }).first().innerText()).toContain('The Board reviewed the repair notice');
    await page.evaluate(() => { (window as any).__failWrites = false; });
    await page.getByRole('button', { name: 'Retry saving', exact: true }).click();
    await expect(page.getByText('Chats saved on this browser', { exact: true })).toBeVisible();
    await expect.poll(async () => { const rows = await records(page); return rows.some(row => row.thread?.messages.some((message: any) => message.role === 'assistant' && message.content.includes('repair notice'))); }).toBe(true);
    await page.reload();
    await expect.poll(async () => page.locator('article').filter({ has: page.locator('strong', { hasText: 'ASSISTANT' }) }).first().innerText()).toContain('The Board reviewed the repair notice');
});
test('conflicting transcripts in two real tabs fork and each pending response finishes in its own transcript', async ({ page, context }) => {
    await freshChat(page);
    const url = page.url(), second = await context.newPage();
    await second.goto(url);
    await expect(composer(second)).toBeVisible();
    await expect(second.getByText('Chats saved on this browser', { exact: true })).toBeVisible();
    await injectQuota(page);
    await injectQuota(second);
    const held = latch(), entered = latch();
    let count = 0;
    await context.route(api + '/api/assistant/chat', async (route) => { const response = await route.fetch(); if (++count === 2)
        entered.release(); await held.promise; await route.fulfill({ response }); });
    try {
        await send(page, 'repair notice Alpha question');
        await send(second, 'repair notice Beta question');
        await entered.promise;
        await expect(page.getByRole('button', { name: 'Retry saving', exact: true })).toBeVisible();
        await expect(second.getByRole('button', { name: 'Retry saving', exact: true })).toBeVisible();
        await page.evaluate(() => { (window as any).__failWrites = false; });
        await page.getByRole('button', { name: 'Retry saving', exact: true }).click();
        await expect(page.getByText('Chats saved on this browser', { exact: true })).toBeVisible();
        await second.evaluate(() => { (window as any).__failWrites = false; });
        await second.getByRole('button', { name: 'Retry saving', exact: true }).click();
        held.release();
        await expect.poll(async () => { const rows = await records(page); const active = rows.filter(row => row.thread?.messages.some((m: any) => m.role === 'user')); return active.length === 2 && active.every(row => row.thread.messages.some((m: any) => m.role === 'assistant' && m.query === row.thread.messages.find((m: any) => m.role === 'user')?.content)) && active.every(row => row.thread.messages.filter((m: any) => m.role === 'user').length === 1); }).toBe(true);
        const rows = await records(page);
        const questions = rows.flatMap(row => row.thread?.messages.filter((m: any) => m.role === 'user').map((m: any) => m.content) || []);
        expect(questions.sort()).toEqual(['repair notice Alpha question', 'repair notice Beta question']);
    }
    finally {
        held.release();
        await second.close();
    }
});

test('provider rate-limit fallback reports its actual category and survives reload',async({page,request})=>{
 await request.post(api+'/__test/control',{data:{mode:'rate_limited'}});
 try{await freshChat(page);await send(page,'repair notice');await expect(page.getByText('The model provider is rate limited; showing retrieved excerpts.',{exact:true})).toBeVisible();
 await expect.poll(async()=>{const rows=await records(page);return rows.some(row=>row.thread?.messages.some((message:any)=>message.fallbackReason==='provider_rate_limited'));}).toBe(true);
 await page.reload();await expect(page.getByText('The model provider is rate limited; showing retrieved excerpts.',{exact:true})).toBeVisible();}
 finally{await request.post(api+'/__test/control',{data:{mode:'valid'}});}
});
