# Keyboard and dialog behavior

The conversation deletion confirmation uses a native modal `dialog` opened with `showModal()`. The background is inert, the dialog has a name and description, and Cancel receives initial focus. Explicit Tab/Shift+Tab boundary wrapping avoids leaving the action group for browser chrome. Escape, Cancel and a backdrop click dismiss without deleting. A pointer drag starting inside the dialog cannot accidentally dismiss it outside.

On dismissal, focus returns to the original opener. If the conversation/opener was removed (including deletion in another tab), focus returns to New chat. The body scroll setting is restored on cleanup. Conversation removal is announced through a polite status region; existing local-store tombstone/recovery semantics remain unchanged.

The reviewer queue's existing button and row pointer handler both call `selectDocument`, preserving the H18 identity/unsaved-change checks. The button now exposes selected state. All queue selectors and metadata/rejection fields have visible associated labels, and loading, export, mutation and error messages expose status/alert semantics. A visible focus ring remains on interactive controls. Below 1100px the panels stack; the wide table scrolls in its panel rather than collapsing text or expanding the page.

## Acceptance

`apps/web/tests/e2e/accessibility.spec.ts` runs axe WCAG A/AA checks on the rendered dialog and reviewer main region, plus actual keyboard selection/edit/save/rejection and dialog traversal, Escape, Cancel, backdrop dismissal, inert background, opener restoration and deletion fallback. A separate two-tab flow waits for durable conversation completion, deletes that exact conversation in the second tab and verifies dismissal/focus in the first. Existing delayed-response tests continue to exercise reviewer identity fences.

This is scoped accessibility acceptance, not a claim of full application WCAG conformance. Chrome/macOS automated checks and visual inspection do not replace supported-device and screen-reader acceptance (VoiceOver/NVDA). Verify readable announcements and high zoom on those platforms before release.

No data migration or authentication change. Revert L04 to roll back; doing so restores the previous accessibility gaps. Native dialog support is required in the supported browsers.

References: [W3C native modal dialog technique](https://www.w3.org/WAI/WCAG22/Techniques/html/H102), [MDN dialog behavior](https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/dialog).
