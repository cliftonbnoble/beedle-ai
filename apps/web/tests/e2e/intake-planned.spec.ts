import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test('planned intake makes no upload promise, accepts no file and offers the existing review route', async ({ page, context }) => {
  await context.route('**/*', route => ['127.0.0.1', 'localhost'].includes(new URL(route.request().url()).hostname) ? route.continue() : route.abort());
  const writes: string[] = [];
  let chooserOpened = false;
  page.on('request', request => { if (new URL(request.url()).port === '8819' && request.method() !== 'GET') writes.push(request.url()); });
  page.on('filechooser', () => { chooserOpened = true; });
  await page.goto('/add-decision');
  await expect(page.getByRole('heading', { name: 'Browser intake is not available', exact: true })).toBeVisible();
  await expect(page.getByText('Browser uploads are planned and are not available yet.', { exact: true })).toBeVisible();
  const main = page.getByRole('main');
  await expect(main.locator('input[type="file"], [role="progressbar"], .is-active')).toHaveCount(0);
  await expect(main.getByRole('button')).toHaveCount(0);
  await expect(main).not.toContainText(/Encrypted intake|extraction ready|Live Analysis|Click to select/);
  await main.click();
  await main.evaluate(el => { const dataTransfer = new DataTransfer(); dataTransfer.items.add(new File(['synthetic fixture'], 'fixture.docx')); el.dispatchEvent(new DragEvent('drop', { dataTransfer, bubbles: true, cancelable: true })); });
  await expect(main).toContainText('No document is being scanned, processed or added to search here.');
  expect(chooserOpened).toBe(false); expect(writes).toEqual([]);
  const result = await new AxeBuilder({ page }).include('main').withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze();
  expect(result.violations).toEqual([]);
  await main.getByRole('link', { name: 'review documents already ingested' }).click();
  await expect(page).toHaveURL(/\/admin\/ingestion$/);
  await expect(page.getByRole('heading', { name: 'Pilot Documents', exact: true })).toBeVisible();
});
