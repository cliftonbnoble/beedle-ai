# Reviewer form identity and asynchronous completion (H18)

The ingestion review pane now keeps a form snapshot containing document ID, inspected revision, values and a baseline for unsaved-change detection. A synchronous session fence tracks the selection epoch, detail-request sequence and mutation ticket. React state controls rendering; the fence prevents duplicate clicks or an old event handler from bypassing an update before React rerenders.

Selecting a different document immediately removes the previous form. Only a matching response with a valid document ID/revision may populate it. Failed loads leave no actionable old form and provide an explicit reload control. Metadata, approval and rejection take their document ID from the captured form; approval also sends that form's inspected revision under H11's existing server contract.

A single mutation may run at a time in this review page. Fields and mutation buttons remain disabled while it runs, but another document can still be selected and read. Completion refreshes the list using the current filters. It reloads detail only when the original selection session and inspected revision still match. Returning A → B → A creates a different session: a completed earlier mutation requires explicit reload, and any old pending detail response is invalidated. Success/error notices always identify the affected document, even when another document is selected.

Unsaved metadata prevents approval/rejection until saved. Switching documents or reloading asks before discarding unsaved edits; cancelling preserves the form and selection. Failed saves retain the typed values. There is no automatic mutation retry. Browser navigation warns about unsaved edits when no submitted mutation is pending. A submitted request may still finish after navigating away.

This finding prevents client selection/form races. It does not implement M12's broader optimistic concurrency/idempotency contract for metadata and rejection across separate reviewers, tabs, scripts or unknown network outcomes. That Medium finding remains open by scope. H01 authentication also remains deferred.

## Verification

`pnpm --filter @beedle/web test:web` executes the session state-machine cases alongside existing web guards. Cases include superseded responses, failed loads, forged IDs, invalid revisions, immediate duplicate submissions, late completions in different selection states, and mutation ownership.

The reproducible browser regression is `apps/web/tests/browser/reviewer-races.js`. Start `pnpm dev:web`, open a clean browser with Playwright CLI, then run:

```sh
playwright-cli run-code --filename apps/web/tests/browser/reviewer-races.js
playwright-cli eval 'window.__reviewerRaceResult'
```

The script intercepts all loopback API calls; it does not connect to production or require a real database. It deliberately holds and releases responses while interacting with the actual React page. Its ten scenarios cover loading B after A, late A reads, failed B loads, unsaved navigation cancellation/acceptance, repeated saves, save/approve/reject completion after switching to B, returning to A, failed-save value retention, and a stalled A detail refresh overlapping B's own save. The write lock ends with the write; a later old read cannot release a newer write's lock. Expect `passed: true` only A targets for A actions, and a separate B target for B's deliberately overlapping save. The screenshot is written to ignored `output/playwright/h18-reviewer-races.png`. Playwright CLI can yield when the intentional confirmation dialog appears; the final browser result marker confirms the entire script completed.

No schema migration or production data change is required for H18. Roll back the UI commit independently if necessary, but doing so reintroduces the original selection race.
