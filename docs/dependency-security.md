# Dependency security maintenance

The repository maintainer owns dependency updates. Review upstream security
advisories when Dependabot opens a PR; prioritize critical/high fixes before
feature releases. Dependabot proposes updates weekly and does not auto-merge.
Keep Next, React/React DOM, the deployment adapter and native image dependencies
compatible as one reviewed change. Review every new major/minor line explicitly.

H02 baseline, assessed 2026-10-26: Next 15.5.28, React/React DOM 19.1.10, and
Sharp 0.35.5. Next 15 remains listed in the upstream support table as Maintenance
LTS at this assessment; recheck the policy for every upgrade. H02 retains the
existing Pages deployment while M20 owns the adapter/platform migration.

Sources:

- [October 14 Next.js security update](https://nextjs.org/blog/nextjs-security-update-october-14-2026)
- [Image dependency advisory](https://github.com/vercel/next.js/security/advisories/GHSA-2464-fhr4-w9xw)
- [Next.js support policy](https://nextjs.org/support-policy)

`pnpm --filter @beedle/web check:framework-security` verifies the **resolved**
Next/React and Next's resolved Sharp versions against reviewed floors, including
React DOM parity. It prevents known regressions; it is not a complete advisory
scanner and intentionally rejects unreviewed release lines. Update its floors
and tests after checking primary advisories. The root Sharp override prevents
an older lockfile-compatible native dependency from surviving a framework bump.

Required update checks: frozen install, framework guard, workspace typechecks,
web tests, Next build, actual hosting build, and local runtime smoke for static
and dynamic routes. Before production release, verify an isolated preview and
the actual API flows. No deployment is performed by the security guard.

Rollback by reverting the dependency commit and rebuilding only if the previous
release is safe; never redeploy a known vulnerable version merely to restore
adapter compatibility. Prefer a patched forward fix. Major hosting changes need
their own migration/rollback plan.
