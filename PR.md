# PR: Theme system — 8 presets, animations, sounds, easter eggs

## Summary

Adds a runtime theme system that retints the entire Texera frontend (dashboard chrome, sidebar, workspace canvas, operator boxes, links, modals, forms, Monaco editor) and a small "delight" layer (confetti on workflow success, animated edge/operator entrance, opt-in chimes, Konami easter egg). 8 built-in themes — Light, Dark, Sepia, Solarized Dark, Gruvbox, Synthwave, Forest, Cyberpunk — picked from a submenu in the user-icon dropdown. Theme preference persists per-user via the existing `/api/user/config` endpoint (with localStorage fallback for logged-out / pre-auth paint).

Also includes a small fix to `bin/texera`: `stop` now sweeps known ports (1234/4200/3001/8080–9096) to kill orphaned children whose process group was lost — previously a leaked y-websocket on port 1234 from a prior `start` would block the next `start`'s frontend with `EADDRINUSE`.

## Motivation

Texera looks the same to every user, and it's been an opaque "operate this tool" experience rather than something users feel ownership over. Three goals:

1. **Accessibility** — dark-mode-OS users see a flash of light theme and have no way to fix it. Sepia / Solarized address eye-strain for long workflow-authoring sessions.
2. **Personality** — a research/data-science tool people spend hours in should be customizable enough to feel "theirs." Synthwave / Cyberpunk / Forest / Gruvbox each give the same workspace a completely different visual identity without breaking any chrome.
3. **Hackathon: dkNet-AI Apache Texera Agent Hackathon** — built as a "make using Texera enjoyable instead of it being just a tool" submission.

## What's in the box

### Theme contract (`frontend/src/styles/_tokens.scss`)

~45 CSS custom properties on `:root`, semantically named so themes can re-interpret freely:

- **Surfaces** — `--tx-bg-base`, `--tx-bg-surface`, `--tx-bg-elevated`, `--tx-bg-canvas`, `--tx-bg-hover`, `--tx-bg-active`, `--tx-bg-overlay`
- **Text** — `--tx-text-primary` / `secondary` / `tertiary` / `disabled` / `inverse`
- **Borders** — `--tx-border-subtle` / `default` / `strong`
- **Primary** — `--tx-primary` / `hover` / `active` / `soft` / `fg`
- **Semantic** — `--tx-success`, `--tx-warning`, `--tx-danger`, `--tx-info` (+ `*-soft` variants)
- **Shadows** — `--tx-shadow-sm` / `md` / `lg`
- **Typography & layout** — `--tx-font-ui`, `--tx-font-mono`, `--tx-radius-{sm,md,lg}`
- **Motion** — `--tx-motion-{fast,default,slow}`, `--tx-motion-ease`
- **Workspace canvas** — `--tx-canvas-bg`, `--tx-canvas-grid`, `--tx-op-body-bg`, `--tx-op-body-stroke`, `--tx-op-label`, `--tx-op-sublabel`, `--tx-op-port`, `--tx-link-stroke`, `--tx-link-handle`, `--tx-minimap-border`, `--tx-monaco-theme`

A universal CSS transition on `background-color / border-color / color / fill / stroke / box-shadow` gives every theme switch a ~200ms crossfade instead of a hard cut. `prefers-reduced-motion` users get instant switches.

### Theme service (`frontend/src/app/common/service/theme/theme.service.ts`)

- Applies tokens to `:root` via `style.setProperty`
- Sets `color-scheme` so the browser picks correct defaults for scrollbars, form controls, etc.
- Exposes a `data-theme` attribute on `:root` so non-token CSS can branch
- First paint: `prefers-color-scheme: dark` → Dark, otherwise Light
- Subsequent: loads persisted theme from user-config (logged in) or localStorage (anonymous)
- Re-resolves on login/logout
- Calls `monaco.editor.setTheme()` when Monaco is loaded, so the code editor flips with everything else

### 8 built-in themes (`frontend/src/app/common/service/theme/themes.ts`)

Each theme is a flat `Record<string, string>` of token overrides — anything unspecified inherits from the defaults. Includes canvas-specific overrides so the workspace fits each theme's palette (Synthwave: pink links on cyan ports; Cyberpunk: red ports on cyan links; Solarized uses Ethan Schoonover's official base-0X codes; Gruvbox uses Pavel Pertsev's palette).

### ng-zorro chrome retint (`frontend/src/styles.scss`, +~280 lines)

ng-zorro ships as a pre-built minified stylesheet — we can't customize its `.less` variables without forking the build. Instead, the high-visibility classes are overridden using token variables:

- Layouts, sider, top header, footer
- Menus (top nav, sidebar, dropdowns, submenu popups in CDK overlays, `.ant-menu-light` for sider's `nzTheme="light"`)
- Modals (content, header, footer, mask, close)
- Cards & tables (headers, hover states)
- Inputs, selects, picker, textareas, checkboxes, radios
- Buttons (default / primary / link / dashed / text — including **disabled states**, which previously hardcoded `#f5f5f5` and looked pale-white on any dark theme)
- Tabs (used by result panel + property editor)
- Tooltips, popovers, collapse panels

### Workspace canvas

JointJS doesn't render `background.color` or grid dots through CSS — both are baked in as inline styles / canvas-rendered images at paper construction. So `WorkflowEditorComponent` now:

- Injects `ThemeService` and subscribes to the active-theme stream
- On every theme change reads `--tx-canvas-bg` and `--tx-canvas-grid` from `:root` via `getComputedStyle`
- Calls `paper.drawBackground({color})` + `paper.drawGrid({name:"fixedDot", args:{color, ...}})` to redraw

Operator boxes, port circles, link strokes, and operator labels are themed via CSS overrides on JointJS-generated SVG (`.operator-element rect.body`, `.joint-link path.connection`, etc.). SVG attribute fills lose to CSS rules at the same level, so no changes are needed in `JointUIService`.

### Monaco editor

`CodeEditorComponent` now injects `ThemeService` and:
- At init, sets `workbench.colorTheme` to `"Default Light Modern"` or `"Default Dark Modern"` based on the active theme's `mode`
- `ThemeService.applyToDom` calls global `monaco.editor.setTheme("vs"/"vs-dark")` so already-open editors flip too

### Motion + sound layer (`frontend/src/app/common/service/motion/motion.service.ts`)

- Confetti on workflow execution `Completed` — canvas-based, no library dep. Particles sample `--tx-primary` / `--tx-success` / `--tx-warning` / `--tx-info` / `--tx-danger` so the burst matches the active theme.
- Shake on `Failed`
- WebAudio-synthesized chimes (success: ascending C major triad; fail: descending tritone)
- Link draw-in animation (stroke-dashoffset 0→length)
- Operator settle animation (scale 0.6→1.08→1.0 with overshoot)
- 800ms "arm" timer so existing operators/links of a workflow opened from persistence don't all animate at once
- `prefers-reduced-motion` default for motion; sound is **off by default**

### Easter egg

Type `↑ ↑ ↓ ↓ ← → ← → B A` anywhere → locks to Synthwave, three confetti volleys over 4.5s, plays the success chord, then restores your previous theme.

### Preferences UI

Two new submenus under the user-icon avatar:
- **Theme** — all 8 presets, checkmark on active
- **Preferences** — Animations on/off, Sound effects on/off

Both pieces persist per-user the same way the theme does.

## Files changed (high-level)

### New
- `frontend/src/styles/_tokens.scss` — token contract + crossfade transitions
- `frontend/src/app/common/service/theme/theme.service.ts` — apply, persist, monaco sync
- `frontend/src/app/common/service/theme/themes.ts` — 8 presets
- `frontend/src/app/common/service/motion/motion.service.ts` — confetti, chimes, Konami

### Modified
- `frontend/src/styles.scss` — ng-zorro retint + workspace JointJS CSS overrides + disabled-state retint
- `frontend/src/app/app.component.ts` — eager-instantiate `ThemeService` and `MotionService`
- `frontend/src/app/dashboard/component/user/user-icon/user-icon.component.{ts,html}` — Theme & Preferences submenus
- `frontend/src/app/dashboard/component/user/list-item/list-item.component.scss` — workflow/dataset/project list rows tokenized
- `frontend/src/app/dashboard/component/dashboard.component.scss`, `button-style.scss` — top nav, primary action button
- `frontend/src/app/workspace/component/workspace.component.scss` — top menu + canvas wrapper
- `frontend/src/app/workspace/component/left-panel/left-panel.component.scss` — operator picker
- `frontend/src/app/workspace/component/property-editor/property-editor.component.scss` — right panel
- `frontend/src/app/workspace/component/result-panel/result-panel.component.scss` — bottom panel
- `frontend/src/app/workspace/component/workflow-editor/mini-map/mini-map.component.scss` — minimap nav border
- `frontend/src/app/workspace/component/workflow-editor/workflow-editor.component.ts` — paper bg/grid redraw on theme change, motion hooks
- `frontend/src/app/workspace/component/code-editor-dialog/code-editor.component.ts` — initial `colorTheme` matches theme mode
- `bin/texera` — `stop` now port-sweeps known ports as a safety net against orphaned children (fixes a leaked y-websocket lockout from the previous PR)

## Test plan

- [ ] Cold start: `prefers-color-scheme: dark` user sees Dark on first paint (no flash of Light)
- [ ] Switch through all 8 themes from the user-icon menu → dashboard chrome (top nav, sider, list-item rows, modals) flips with crossfade
- [ ] Open a workflow → workspace top menu, left panel, right panel, result panel, minimap border, canvas background, grid dots, operator boxes, link strokes, port circles, labels all match the active theme
- [ ] Open the code editor → Monaco theme follows (Default Dark/Light Modern at init; flips live on theme change)
- [ ] Run a workflow → confetti from center + chord (with sound enabled)
- [ ] Drop a new operator → it scales in with overshoot
- [ ] Draw a new link → it draws itself in over ~380ms
- [ ] Toggle Animations off → entrance animations + confetti suppressed; theme transitions still crossfade
- [ ] Toggle Sound off → no chimes
- [ ] Theme persists across reload (logged out: localStorage; logged in: user-config)
- [ ] Log in / log out → theme follows the account preference correctly
- [ ] Konami code from anywhere → Synthwave + confetti volleys + chord; reverts after ~4.5s
- [ ] Disabled buttons (e.g. greyed-out run/stop/pause in workspace) recede into the theme on every preset, not pale white
- [ ] `texera stop && texera start full` after an interrupted run no longer dies on `EADDRINUSE: ::1:1234`

## Out of scope / future

- **Theme builder** — explicitly deferred; the 8 presets cover the common cases
- **Hub sharing** — no builder = nothing user-made to share
- **Time-of-day auto theme switching** — deliberately not shipped (annoying for demos; user can pick manually)
- **Operator node skin variants** (flat / glossy / rounded / sticker), **edge router variants**, **canvas pattern toggles** — these were part of the original phase 2 plan but tied to the theme builder; bringing them back is mechanical once a builder exists
