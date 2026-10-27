// Run only in the protected release workflow; does not contact Cloudflare.
const id = process.env.PREVIEW_RUN_ID;
if (!/^[1-9][0-9]{0,15}$/.test(id || ''))
    throw new Error('A successful preview workflow run ID is required');
const response = await fetch(`${process.env.GITHUB_API_URL}/repos/${process.env.GITHUB_REPOSITORY}/actions/runs/${id}`, { headers: { authorization: `Bearer ${process.env.GITHUB_TOKEN}`, accept: 'application/vnd.github+json' }, signal: AbortSignal.timeout(15000), redirect: 'error' });
if (!response.ok)
    throw new Error('Could not verify preview workflow provenance');
const run = await response.json();
if (run.head_sha !== process.env.GITHUB_SHA || run.head_branch !== 'main' || run.event !== 'workflow_dispatch' || run.conclusion !== 'success' || run.repository?.full_name !== process.env.GITHUB_REPOSITORY || run.path?.split('@')[0] !== '.github/workflows/deploy-api.yml')
    throw new Error('Preview evidence must come from the successful protected main-branch release workflow at this exact revision');
console.log('Preview workflow provenance verified');
