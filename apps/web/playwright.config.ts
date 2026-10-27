import { defineConfig } from '@playwright/test';
// Fresh local resources are mandatory. Never reuse a developer or production API.
export default defineConfig({
    testDir: './tests/e2e', fullyParallel: false, workers: 1, forbidOnly: Boolean(process.env.CI),
    retries: 0, timeout: 45000, expect: { timeout: 15000 },
    outputDir: '../../output/playwright/results',
    reporter: [['list'], ['html', { outputFolder: '../../output/playwright/report', open: 'never' }]],
    use: { baseURL: 'http://127.0.0.1:5819', browserName: 'chromium',
        ...(process.env.BEEDLE_TEST_CHROME_CHANNEL ? { channel: process.env.BEEDLE_TEST_CHROME_CHANNEL } : {}),
        trace: 'retain-on-failure', screenshot: 'only-on-failure' },
    webServer: [
        { command: 'pnpm --filter @beedle/api test:serve', url: 'http://127.0.0.1:8819/__test/seeded', timeout: 90000, reuseExistingServer: false, gracefulShutdown: { signal: 'SIGTERM', timeout: 10000 } },
        { command: process.env.BEEDLE_E2E_ADAPTER === 'pages' ? 'pnpm exec wrangler pages dev .vercel/output/static --ip 127.0.0.1 --port 5819 --compatibility-date 2026-03-01 --compatibility-flag nodejs_compat' : 'pnpm exec next dev -H 127.0.0.1 -p 5819', url: 'http://127.0.0.1:5819', timeout: 90000, reuseExistingServer: false, gracefulShutdown: { signal: 'SIGTERM', timeout: 10000 },
            env: { NEXT_PUBLIC_APP_ENV: 'local', NEXT_PUBLIC_API_BASE_URL: 'http://127.0.0.1:8819' } }
    ]
});
