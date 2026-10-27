import { createWorkerFixture } from './fixture.mjs';
import { seedFixture } from './seeds.mjs';
const port = Number(process.env.FIXTURE_API_PORT || 8819), webPort = Number(process.env.FIXTURE_WEB_PORT || 5819);
if (!Number.isInteger(port) || port < 1024 || port > 65535 || !Number.isInteger(webPort) || webPort < 1024 || webPort > 65535)
    throw new Error('Invalid isolated fixture ports');
const fixture = await createWorkerFixture({ port, webPort });
let stopping = false;
async function stop() { if (stopping)
    return; stopping = true; await fixture.stop(); process.exit(0); }
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
try {
    const seedIds = await seedFixture(fixture);
    console.log(JSON.stringify({ fixture: 'beedle-hermetic-v1', url: fixture.url, seedIds }));
}
catch (error) {
    await fixture.stop();
    throw error;
}
await new Promise(() => { });
