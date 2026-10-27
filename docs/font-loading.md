# UI font loading

The UI and decision reader use `--font-ui`: `ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif`. Existing 400/500/600/700/800 weights remain. The operating system supplies fonts; no font file or external stylesheet is shipped. This removes network-dependent font discovery and swapping. Appearance and synthesized weights can vary by OS.

## Local adapter comparison (2026-10-27)

Measured `/search` through Wrangler Pages, an isolated seeded API, Chrome 156 on macOS, 1440×1000, Slow 4G, 4× CPU slowdown, reduced motion, HTTP cache bypass. One synthetic trace per version, before L03 and after L03; no field data or INP claim. MCP returned trace summaries inline; a raw trace export was unavailable in its configured filesystem.

| Observation | Before | After |
| --- | --- | --- |
| LCP | 2728 ms | 2801 ms |
| CLS | 0.0094 | 0.00 |
| Font discovery chain | HTML → local CSS → Google CSS → WOFF2 | HTML → local CSS |
| External font CSS | ~614 ms; ~1100 transferred bytes | No request |
| WOFF2 | 48,432 bytes (Content-Length) | No request |

LCP did not improve in this sample; do not turn these timings into a speedup claim. Chrome identified the font as a possible layout-shift cause before the change. After the change, `document.fonts.size` was zero and the only stylesheet/font network entry was the local stylesheet.

Automated checks run against the actual Pages build and count attempted requests (including blocked requests), loaded font faces, computed family and horizontal overflow at 1440, 820 and 390 px. Screenshots under ignored `output/playwright/l03-native-*.png` were inspected at all three widths. Existing search, reviewer and conversation flows passed. A source guard protects reader line height and measure as well as shared typography.

## Release and rollback

This is a code-only change. Verify supported Windows/Linux/browser combinations for text wrapping, weight contrast and zoom before release; macOS screenshots cannot prove cross-platform font parity. Reverting L03 restores prior typography but also restores the external dependency. No database, binding or authentication change is involved.

Background: [web.dev font-loading guidance](https://web.dev/articles/optimize-webfont-loading) and [Chrome performance tooling](https://developer.chrome.com/docs/devtools/performance/overview).
