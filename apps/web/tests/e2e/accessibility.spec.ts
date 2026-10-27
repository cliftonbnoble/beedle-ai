import { test, expect, type Page, type Locator } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const api = 'http://127.0.0.1:8819';
async function tabTo(page: Page, target: Locator) {
  for (let n = 0; n < 160; n++) {
    if (await target.evaluate(el => el === document.activeElement)) return;
    await page.keyboard.press('Tab');
  }
  await expect(target).toBeFocused();
}
async function audit(page: Page, selector: string) {
  const result = await new AxeBuilder({ page }).include(selector)
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze();
  expect(result.violations).toEqual([]);
}
test.beforeEach(async ({ context }) => {
  await context.route('**/*', route => ['127.0.0.1', 'localhost'].includes(new URL(route.request().url()).hostname) ? route.continue() : route.abort());
});

test('chat deletion contains focus, cancels safely and returns focus after deletion', async ({ page }) => {
  await page.goto('/case-assistant?new=1');
  await expect(page).toHaveURL(/thread=/);
  const composer = page.getByPlaceholder('Ask about prior decisions…');
  await tabTo(page, composer); await page.keyboard.type('repair notice keyboard dialog');
  await tabTo(page, page.getByRole('button', { name: 'Send', exact: true })); await page.keyboard.press('Enter');
  const trigger = page.getByRole('button', { name: /^Delete conversation / }).first();
  await expect(trigger).toBeVisible();
  await tabTo(page, trigger); await page.keyboard.press('Enter');
  const dialog = page.getByRole('dialog', { name: 'Remove this chat?' });
  const cancel = dialog.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeFocused(); await audit(page, 'dialog');
  await page.screenshot({ path: '../../output/playwright/l04-dialog.png', fullPage: true });
  await composer.evaluate(el => el.focus()); await expect(cancel).toBeFocused();
  for (const key of ['Tab', 'Tab', 'Shift+Tab', 'Shift+Tab']) {
    await page.keyboard.press(key);
    expect(await dialog.evaluate(el => el.contains(document.activeElement))).toBe(true);
  }
  await page.keyboard.press('Escape'); await expect(dialog).toHaveCount(0); await expect(trigger).toBeFocused();
  await page.keyboard.press('Enter'); await expect(cancel).toBeFocused();
  await page.keyboard.press('Enter'); await expect(dialog).toHaveCount(0); await expect(trigger).toBeFocused();
  await page.keyboard.press('Enter'); await expect(cancel).toBeFocused();
  await page.mouse.click(1, 1); await expect(dialog).toHaveCount(0); await expect(trigger).toBeFocused();
  await page.keyboard.press('Enter'); await expect(cancel).toBeFocused();
  await page.keyboard.press('Tab'); await expect(dialog.getByRole('button', { name: 'Delete', exact: true })).toBeFocused();
  await page.keyboard.press('Enter'); await expect(dialog).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'New chat', exact: true })).toBeFocused();
  await expect(page.getByRole('status').filter({ hasText: /removed from active chat history/ })).toBeVisible();
});

test('reviewer selects, edits, saves and rejects with persistent labels and keyboard controls', async ({ page, request }) => {
  for (let n = 0; n < 20; n++) {
    const state = await (await request.get(api + '/admin/reviewer-index')).json();
    if (state.ready) break;
    await request.post(api + '/admin/reviewer-index/step', { data: {} });
  }
  await page.goto('/admin/ingestion');
  const gamma = page.getByRole('button', { name: 'Rent Board case Gamma', exact: true });
  await expect(gamma).toBeVisible(); await tabTo(page, gamma); await page.keyboard.press('Enter');
  await expect(gamma).toHaveAttribute('aria-pressed', 'true');
  const group = page.getByRole('group', { name: 'Document review' }); await expect(group).toBeEnabled();
  await audit(page, 'main');
  const caseNumber = page.getByRole('textbox', { name: 'Case Number', exact: true });
  await tabTo(page, caseNumber); await page.keyboard.press('ControlOrMeta+A'); await page.keyboard.type('T-KEYBOARD');
  const save = page.getByRole('button', { name: 'Save Metadata', exact: true });
  await tabTo(page, save); await page.keyboard.press('Enter');
  await expect(page.getByRole('status').filter({ hasText: /metadata saved/ })).toBeVisible();
  await expect(caseNumber).toHaveValue('T-KEYBOARD');
  const reason = page.getByRole('textbox', { name: 'Reject reason', exact: true });
  await tabTo(page, reason); await page.keyboard.type('Keyboard regression fixture only');
  await tabTo(page, page.getByRole('button', { name: 'Reject', exact: true })); await page.keyboard.press('Enter');
  await expect(page.getByRole('status').filter({ hasText: /rejected\./ })).toBeVisible();
  for (const width of [640, 390]) {
    await page.setViewportSize({ width, height: 900 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
    await expect(caseNumber).toBeVisible();
    await page.screenshot({ path: `../../output/playwright/l04-reviewer-${width}.png`, fullPage: true });
  }
});

test('removing the conversation in another tab dismisses its stale dialog safely', async ({ page, context }) => {
  await page.goto('/case-assistant?new=1'); await expect(page).toHaveURL(/thread=/);
  await page.getByPlaceholder('Ask about prior decisions…').fill('repair notice remote deletion');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Thinking', exact: true })).toHaveCount(0);
  await expect(page.getByText('Chats saved on this browser', { exact: true })).toBeVisible();
  const threadId = new URL(page.url()).searchParams.get('thread')!;
  const trigger = page.getByRole('button', { name: /^Delete conversation / }).first();
  await expect(trigger).toBeVisible();
  const second = await context.newPage();
  try {
    await second.goto(page.url());
    await expect(second).toHaveURL(new RegExp(threadId));
    await expect(second.getByRole('button', { name: 'Delete conversation repair notice remote deletion', exact: true })).toBeVisible();
    await trigger.click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await second.getByRole('button', { name: 'Delete conversation repair notice remote deletion', exact: true }).click();
    await second.getByRole('dialog').getByRole('button', { name: 'Delete', exact: true }).click();
    await expect.poll(() => second.evaluate(id => new Promise(resolve => {
      const open = indexedDB.open('beedle-assistant-v2', 1);
      open.onsuccess = () => { const db = open.result; const row = db.transaction('threads').objectStore('threads').get(id); row.onsuccess = () => { resolve(row.result?.thread === null); db.close(); }; };
    }), threadId)).toBe(true);
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'New chat', exact: true })).toBeFocused();
  } finally { await second.close(); }
});
