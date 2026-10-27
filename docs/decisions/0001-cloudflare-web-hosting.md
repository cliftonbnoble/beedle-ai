# ADR 0001: gated current adapter; evaluate a maintained Workers path separately

Date: 2026-10-26. Status: current-path containment accepted locally; replacement prototype and deployed-preview acceptance required before platform migration.

Beedle currently uses Next 15.5.28 / React 19.1.10 and `@cloudflare/next-on-pages` 1.13.16. The adapter's declared Next support stops at 15.5.2. A successful build and local runtime establish observed compatibility for this commit, not upstream support. Keep the patched framework; do not downgrade to silence the adapter peer warning.

Cloudflare's current [Pages Next.js guide](https://developers.cloudflare.com/pages/framework-guides/nextjs/) recommends vinext on Workers for full-stack applications and static export for Pages. Its [Workers guide](https://developers.cloudflare.com/workers/framework-guides/web-apps/nextjs/) describes vinext for existing Next 16 projects and retains OpenNext for existing deployments with compatibility gaps. These are deployment choices to evaluate, not justification for changing frameworks in this remediation.

Decision: gate the current output with a frozen root install, actual adapter build and browser tests through the generated Pages Worker/static assets. Require an isolated deployed preview and explicit support-risk acceptance before releasing that temporary path. Keep API bindings/corpus on the separate Worker. Plan a separate isolated vinext/Workers prototype after validating the Next 16 transition, rather than mechanically replacing the adapter in this finding.

The prototype must prove dynamic decision URLs/direct reloads, query/filter preservation, search/editor races, browser IndexedDB recovery, public build variable isolation, RSC/static assets, source download headers/links, CORS/perimeter, failure responses, limits and rollback. Compare bundle size, startup/latency and operational ownership with this measured baseline. Run M19's required suites against its real output. Do not share production resources, routes, secrets, user browser origins or private corpus during the experiment.

A static Pages export could eventually fit the mostly client-rendered UI, but the dynamic decision path currently uses an edge route. Static export needs an explicit arbitrary-ID routing/reload design and parity tests. OpenNext adds another adapter/runtime contract and is not selected without a demonstrated compatibility need. A plain client SPA also requires changes to Next navigation/rendering conventions; those costs are not hidden in a routine hosting upgrade.

Production activation, route migration, browser-origin/storage continuity and secret/perimeter changes require a separate concrete rollout. Local M20 checks do not constitute approval or completion of that hosting migration.
