# AGENTS.md — frontend

Scoped agent rules for `frontend/`. Loaded automatically on top of the repo-root [`AGENTS.md`](../AGENTS.md). For recipes, the mental model behind these rules, and troubleshooting, read [`TESTING.md`](TESTING.md) — it is the canonical reference for both humans and agents and should be consulted before writing or fixing any component spec.

## Stack

Angular (standalone components) · Vitest · `@angular/build:unit-test` builder · jsdom by default; Playwright Chromium via `gui:test-browser` for specs that need real DOM/SVG geometry · v8 coverage. Test setup file `src/test-zone-setup.ts` wraps `it`/`test` in a ProxyZone so Angular's `fakeAsync` works under Vitest.

## Golden rules

1. **Always call `fixture.detectChanges()` at least once.** Without it the component constructor runs but the template never does — the template is AOT-emitted code that only executes during change detection, and v8 coverage attributes template hits back to the `.html` file via source maps. No `detectChanges()` ⇒ `.component.html` stays at 0 % even when the spec passes green.
2. **Standalone components go in `imports:`, not `declarations:`.** Angular errors at compile time if a standalone component appears in `declarations:`. To strip a standalone child for shallow rendering use `TestBed.overrideComponent(Parent, { set: { imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA] } })` before `compileComponents()`.
3. **`beforeEach` is `async () => { ... }`, not `waitForAsync(() => …)`.** `test-zone-setup.ts` wraps `it`/`test` in a ProxyZone but not `beforeEach`; `waitForAsync` inside `beforeEach` throws "Expected to be running in 'ProxyZone'".

## Minimum viable spec template

```ts
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { MyComponent } from "./my.component";

describe("MyComponent", () => {
  let fixture: ComponentFixture<MyComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MyComponent, HttpClientTestingModule],
      providers: [...commonTestProviders],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MyComponent);
    fixture.detectChanges(); // ⇐ this line is the HTML-coverage switch
  });

  it("should create", () => {
    expect(fixture.componentInstance).toBeTruthy();
  });
});
```

Anything more complex (event handlers, `*ngIf` branches, async data) — read [`TESTING.md`](TESTING.md).

## Shared test infrastructure

- **Common providers**: `commonTestProviders` from `common/testing/test-utils.ts`. Always spread these; do not redeclare `GuiConfigService` etc. per spec.
- **Service stubs**: reuse the existing `Stub…Service` siblings (for example `StubOperatorMetadataService`). When a service has no stub yet, add a `*.stub.ts` or `*.mock.ts` next to the real implementation rather than inlining a `vi.fn()` chain inside each spec.
- **Mocks**: `vi.fn()` + `.mockReturnValue(...)`. RxJS-returning methods use `.mockReturnValue(of(...))` or `EMPTY`. For partial method replacement, `vi.spyOn(obj, "method").mockReturnValue(...)`.

## Anti-patterns (review will flag these)

| Pattern                                                                | Why it's wrong                                                                                                                                                                          |
| ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spec body entirely commented out, only the license header survives     | The file runs to "0 tests" and reports green; the template never renders. If a spec is dead, delete it; don't leave `//` as a graveyard.                                                |
| `NO_ERRORS_SCHEMA` on a spec that asserts about children               | Children silently fail to render; branches inside `*ngIf="child.ready"` stay uncovered and broken templates pass. Use `MockComponent` or `overrideComponent({ set: { imports: [] } })`. |
| `TestBed.overrideComponent(C, { set: { template: '' } })`              | Destroys the very thing under test; HTML coverage will be permanently 0 %.                                                                                                              |
| `declarations: [StandaloneComponent]`                                  | Angular throws at compile time — standalone components can't be declared in an NgModule. Use `imports:`.                                                                                |
| `beforeEach(waitForAsync(() => …))`                                    | Throws "Expected to be running in 'ProxyZone'" under Vitest. Use `async () => { … }`.                                                                                                   |
| Spec depends on a real HTTP / WebSocket call                           | Use `HttpClientTestingModule` and stub WS observables with `Subject` / `of(...)`.                                                                                                       |
| Inventing a one-off provider mock when a `Stub…Service` already exists | Drift: the next spec invents another mock. Reuse and extend the existing stub.                                                                                                          |

## When to use browser mode (`gui:test-browser`)

The default `yarn test` path runs every spec under jsdom — fast, no browser boot. Switch a spec to browser mode **only** when it depends on real DOM or SVG behavior jsdom can't fake:

- `SVGSVGElement.getScreenCTM()` / `getBoundingClientRect()` returning real layout coordinates (jointjs paper).
- Pointer-event hit testing — `elementFromPoint`, drag-and-drop with real geometry.
- CSS-layout-dependent assertions (offsetWidth / offsetHeight, scroll positions).

Per-spec routing lives in `angular.json` (`gui:test` excludes, `gui:test-browser` runs only what's routed there). Do not duplicate the list in `vitest.config.ts` — see the comment in that file.

## Pointers

- Recipes, decision trees, coverage troubleshooting: [`TESTING.md`](TESTING.md).
- Minimum-viable reference spec (jsdom): `src/app/workspace/component/workflow-editor/mini-map/mini-map.component.spec.ts`.
- Browser-mode reference spec (real DOM): `src/app/workspace/component/workflow-editor/workflow-editor.component.spec.ts`.
- Two-target jsdom / browser-mode setup rationale: [#5017](https://github.com/apache/texera/pull/5017).
