# Frontend testing guide

Canonical reference for writing, running, and maintaining unit tests in `frontend/`. Written for both human contributors and AI agents — read it on demand when [`AGENTS.md`](AGENTS.md)'s rules need a deeper recipe, the mental model behind a constraint, or troubleshooting steps.

For repo-wide testing philosophy (TDD, characterization tests, "every test must cover a specific failure mode") see [`../AGENTS.md`](../AGENTS.md) "Tests come first".

## Contents

1. [The stack](#the-stack)
2. [Running tests](#running-tests)
3. [Why `detectChanges()` is the coverage switch](#why-detectchanges-is-the-coverage-switch)
4. [Recipes](#recipes)
5. [Standalone components](#standalone-components)
6. [jsdom vs browser mode](#jsdom-vs-browser-mode)
7. [Mocking](#mocking)
8. [Anti-patterns](#anti-patterns)
9. [Coverage troubleshooting](#coverage-troubleshooting)
10. [References](#references)

## The stack

| Layer                    | Choice                                                                                                                                 |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| Test framework           | Vitest                                                                                                                                 |
| Angular test integration | `@angular/build:unit-test` builder (two targets in `angular.json`: `gui:test` and `gui:test-browser`)                                  |
| Default DOM              | jsdom                                                                                                                                  |
| Real-browser DOM         | `@vitest/browser` + Playwright (Chromium, headless)                                                                                    |
| Coverage                 | `@vitest/coverage-v8`                                                                                                                  |
| Test setup               | `src/test-zone-setup.ts` wraps `it`/`test` in an Angular ProxyZone (Vitest does not provide one and Angular's `fakeAsync` requires it) |
| Globals                  | `globals: true` in `vitest.config.ts`, so `describe / it / expect / vi / beforeEach` come from the runtime — no per-file imports       |

`src/main.test.ts` is intentionally a near-empty `export {}`. The `unit-test` builder uses `buildTarget`'s `main` to seed the bundle graph; if it pointed at the real `main.ts`, every component declared in `AppModule` would be type-checked for every spec, surfacing template errors for components no active spec touches. Keeping `main.test.ts` empty narrows the graph to what each spec actually imports.

## Running tests

```bash
# default — jsdom, watch off
yarn test

# the same, with coverage in lcov form (CI shape)
yarn test:ci

# only the specs routed to real browser DOM (Playwright Chromium)
ng run gui:test-browser

# coverage report you can open in a browser
yarn test -- --coverage --coverage.reporter=html
# then open coverage/index.html
```

Single-file and watch loops use Vitest's own filtering:

```bash
ng test --test-file src/app/workspace/component/workflow-editor/mini-map/mini-map.component.spec.ts
```

## Why `detectChanges()` is the coverage switch

Angular's Ivy compiler turns each component template into a TypeScript function:

```ts
function MiniMapComponent_Template(rf, ctx) {
  if (rf & 1) {
    // creation pass
    ɵɵelementStart(0, "div");
    ɵɵlistener("click", () => ctx.onClick());
    ɵɵtext(1);
    ɵɵelementEnd();
  }
  if (rf & 2) {
    // update pass
    ɵɵadvance(1);
    ɵɵtextInterpolate(ctx.label);
  }
}
```

This function is **not** invoked by the component constructor; it runs only during change detection. The Vite build emits source maps that map each `ɵɵ…` call back to the `.html` line that produced it, and v8 coverage records hits against that source-mapped location.

Consequences:

- `TestBed.createComponent(C)` alone covers the constructor but leaves the template at 0 %.
- A single `fixture.detectChanges()` runs the creation pass and the first update pass; most ordinary `.component.html` files jump to 70 – 90 % from this one call.
- Branches gated by `*ngIf="cond"` need a second `detectChanges()` with `cond` toggled to cover the other side. The same applies to `*ngFor` over an empty vs non-empty array, and to `[ngSwitch]` cases.

If `.component.html` shows 0 % after your spec runs, you almost certainly hit one of the [anti-patterns](#anti-patterns) — most often the constructor compiled and "should create" passed but `detectChanges` was never reached.

## Recipes

### A. Minimum viable spec

Use this as the starting point for any new component. It already covers the template's creation pass.

```ts
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { MyComponent } from "./my.component";

describe("MyComponent", () => {
  let fixture: ComponentFixture<MyComponent>;
  let component: MyComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MyComponent, HttpClientTestingModule],
      providers: [...commonTestProviders],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
```

### B. `*ngIf` branches

Each `*ngIf` is a separate sub-template. To cover both sides, run `detectChanges()` once per branch:

```ts
it("hides the run button while running", () => {
  component.isRunning = false;
  fixture.detectChanges();
  expect(fixture.nativeElement.querySelector(".run-btn")).toBeTruthy();

  component.isRunning = true;
  fixture.detectChanges();
  expect(fixture.nativeElement.querySelector(".run-btn")).toBeNull();
  expect(fixture.nativeElement.querySelector(".pause-btn")).toBeTruthy();
});
```

### C. Event handler

```ts
import { By } from "@angular/platform-browser";

it("calls onRun() when the run button is clicked", () => {
  const spy = vi.spyOn(component, "onRun");
  fixture.detectChanges();
  fixture.debugElement.query(By.css(".run-btn")).triggerEventHandler("click", null);
  expect(spy).toHaveBeenCalledOnce();
});
```

Prefer `triggerEventHandler("click", null)` over `nativeElement.click()` when the binding goes through Angular event syntax — it goes through the Angular renderer and survives renderer-2 vs DOM-renderer differences.

### D. Component talking to a stubbed service

Reuse existing `Stub…Service` doubles where they exist. Where they don't, spread a fresh `vi.fn()` mock through `providers`:

```ts
import { OperatorMetadataService } from "../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../service/operator-metadata/stub-operator-metadata.service";
import { of } from "rxjs";

beforeEach(async () => {
  await TestBed.configureTestingModule({
    imports: [MyComponent, HttpClientTestingModule],
    providers: [
      { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      {
        provide: WorkflowPersistService,
        useValue: {
          persistWorkflow: vi.fn().mockReturnValue(of(stubWorkflow)),
        },
      },
      ...commonTestProviders,
    ],
  }).compileComponents();
});
```

### E. Component with async data (HTTP, RxJS)

For HTTP-driven flows, `HttpClientTestingModule` is enough; `await fixture.whenStable()` flushes pending microtasks:

```ts
it("loads the workflow on init", async () => {
  fixture.detectChanges();
  await fixture.whenStable();
  expect(component.workflow).toEqual(stubWorkflow);
});
```

For timers (`debounceTime`, `setTimeout`, `interval`), use `fakeAsync` + `tick`:

```ts
import { fakeAsync, tick } from "@angular/core/testing";

it("debounces the query before firing", fakeAsync(() => {
  component.query$.next("foo");
  tick(199);
  expect(search).not.toHaveBeenCalled();
  tick(1);
  expect(search).toHaveBeenCalledWith("foo");
}));
```

`fakeAsync` works because `test-zone-setup.ts` installs a ProxyZone around `it`/`test`. It does **not** install one around `beforeEach`, so do not write `beforeEach(fakeAsync(...))` — set component state and call `tick()` from inside the `it` body instead.

## Standalone components

Newly-generated components are standalone. The component itself goes into `imports:`, never `declarations:`:

```ts
TestBed.configureTestingModule({
  imports: [MyStandaloneComponent, HttpClientTestingModule],
  providers: [...commonTestProviders],
});
```

To replace heavy or hard-to-instantiate child components with stubs while keeping the parent template under test, use the `set / remove / add` form of `overrideComponent` **before** `compileComponents()`:

```ts
TestBed.overrideComponent(WorkspaceComponent, {
  set: { imports: [], providers: [], schemas: [CUSTOM_ELEMENTS_SCHEMA] },
});
```

Working reference: `src/app/workspace/component/workspace.component.spec.ts`.

This drops the children's transitive dependency tree but leaves the parent template rendering — `<ng-template>` tags, `@ViewChild` queries, and event bindings still work because the template itself is unchanged. Do **not** use `set: { template: "" }` — that erases the template and guarantees 0 % HTML coverage.

## jsdom vs browser mode

Default path (`gui:test`, `vitest.config.ts`) runs every spec under jsdom. Fast, no browser boot, works for the overwhelming majority of component logic.

Browser path (`gui:test-browser`, `vitest.browser.config.ts`) runs in real Chromium via Playwright. Use it **only** for the cases jsdom can't fake:

| Spec needs                                                            | jsdom verdict           | Action       |
| --------------------------------------------------------------------- | ----------------------- | ------------ |
| `getBoundingClientRect()` / `offsetWidth` with real layout            | Returns zeros           | Browser mode |
| `SVGGraphicsElement.getScreenCTM()` for SVG coordinate math (jointjs) | Returns identity matrix | Browser mode |
| Pointer-event hit testing (`elementFromPoint`, drag-and-drop)         | Misses elements         | Browser mode |
| CSS-driven visibility / scroll measurements                           | Unreliable              | Browser mode |
| Plain DOM tree assertions, classes, attributes, text content          | Fine                    | jsdom        |
| Event handlers, observables, services, routing                        | Fine                    | jsdom        |

Routing rules: per-spec inclusion / exclusion is set in `angular.json`, not in `vitest.config.ts`. The comment in `vitest.config.ts` explains why — duplicating the list there triggers a Vite warning and the builder-side filter wins anyway.

Working reference (browser mode): `src/app/workspace/component/workflow-editor/workflow-editor.component.spec.ts`. See [#5017](https://github.com/apache/texera/pull/5017) for the rationale behind the two-target split.

## Mocking

### Functions / spies

```ts
const onClick = vi.fn(); // bare mock
const persist = vi.fn().mockReturnValue(of(stubWorkflow)); // returns Observable
const search = vi.fn().mockResolvedValue([1, 2]); // returns Promise
const spy = vi.spyOn(component, "onRun"); // spy without replacing
```

### Service doubles

Prefer a sibling `Stub…Service` class (the `StubOperatorMetadataService` pattern is the model) when the service is used by more than one spec. They live next to the real service and are imported by name; they keep the spec setup terse and the mock surface consistent across the codebase.

For one-shot mocks, an inline `useValue` with `vi.fn()` is fine, but if you find yourself copying the same `useValue` block across specs, lift it to a `*.stub.ts` next to the service.

### RxJS

Replace methods that return `Observable<T>` with `vi.fn().mockReturnValue(of(value))` or `mockReturnValue(EMPTY)` / `mockReturnValue(throwError(() => err))`. For multi-emission streams expose a `Subject` you control and call `subject.next(...)` from inside the test.

## Anti-patterns

The patterns listed below are the ones that have actually appeared in this codebase. Each ships a real fix.

### 1. Fully commented-out spec

The license header is the only live code; the file reports "0 tests, 0 failures" and Vitest counts it as a pass. The corresponding `.component.html` stays at 0 %.

**Fix**: delete the commented code outright (git history keeps it). Either replace it with the minimum-viable spec from Recipe A, or remove the spec file entirely.

### 2. `NO_ERRORS_SCHEMA` everywhere

A spec adds `schemas: [NO_ERRORS_SCHEMA]` to silence "unknown element" errors from un-imported children, but then asserts something about the parent template that depends on the child rendering. Branches inside `*ngIf="child.ready"` are dead because `child.ready` never fires.

**Fix**: import the real child, or use `overrideComponent({ set: { imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA] } })` to drop the child's transitive imports while letting the unknown element render as an inert tag. `CUSTOM_ELEMENTS_SCHEMA` says "I know what I'm doing", `NO_ERRORS_SCHEMA` says "swallow all template errors" — the second is almost never what you want.

### 3. `overrideComponent({ set: { template: "" } })`

Used historically to bypass a child template that wouldn't compile. Side effect: the parent's template is wiped, so HTML coverage is permanently 0 %.

**Fix**: see Anti-pattern 2 — override `imports` and `schemas` instead, keep the template intact.

### 4. `declarations: [StandaloneComponent]`

Angular errors out at compile time with "Component is standalone and can't be declared in any NgModule".

**Fix**: move the component to `imports:`.

### 5. `beforeEach(waitForAsync(() => …))`

`test-zone-setup.ts` wraps `it`/`test` in a ProxyZone but **not** `beforeEach`. `waitForAsync` throws "Expected to be running in 'ProxyZone'" the moment it tries to detect the zone.

**Fix**: `beforeEach(async () => { await TestBed.configureTestingModule(...).compileComponents(); })`.

### 6. Real HTTP / WebSocket calls

A spec that requires the dev server to be running, or that opens a real WebSocket, is not a unit test. It will fail in CI for unrelated reasons and slow the whole suite down.

**Fix**: `HttpClientTestingModule` for HTTP; a `Subject` you `.next()` into for WebSocket-shaped observables.

### 7. One-off `useValue` mock when a `Stub…Service` exists

Drift: spec A invents a partial mock for `OperatorMetadataService`, spec B invents a different partial mock, none of them agrees with the real interface, and a refactor breaks all three differently.

**Fix**: use `StubOperatorMetadataService`. When you find yourself wanting a new method on the stub, add it to the stub class, not to the spec.

## Coverage troubleshooting

`yarn test:ci` produces `coverage/lcov.info`; the GitHub-side dashboard is at [app.codecov.io/gh/apache/texera](https://app.codecov.io/gh/apache/texera). If a `.component.html` shows 0 % even though the spec passes, walk this list top-to-bottom:

1. **Did `compileComponents()` actually resolve?** A missing provider makes `TestBed.configureTestingModule(...).compileComponents()` reject; Vitest reports the spec as failed but coverage still says 0 %. Run the spec locally, read the actual error in the output, and add the missing provider (commonly `HttpClient`, `Router`, `ActivatedRoute`, or a service that pulls in `GuiConfigService`).
2. **Is `fixture.detectChanges()` actually called?** Step through `beforeEach`; an early-throw or a typo means the line is dead code.
3. **Has the template been overridden to `""`**? See Anti-pattern 3.
4. **Does the spec use `NO_ERRORS_SCHEMA` to silence missing children, and then assert nothing about the rendered children**? Coverage will reflect that the children's branches were never reached. See Anti-pattern 2.
5. **Is the build source-map-enabled?** `angular.json` `gui:test` inherits from `build.test`. Confirm `sourceMap: true` (the default). A custom builder swap can silently turn it off; without source maps, v8 hits land on the compiled JS, not on the `.html`.
6. **Is the file on the exclusion list?** Check `angular.json` for `unit-test` `polyfills` / `include` / `exclude` patterns. Some files in `common/formly/*.type.ts` are intentionally excluded (their license header file mentions TypeFox).
7. **Is the spec routed to a different builder than expected?** If a file is in `gui:test-browser`'s include list, `yarn test` won't run it. Inspect both targets' `include` arrays.
8. **Is the spec file fully commented out?** See Anti-pattern 1. If Vitest reports "0 tests" for the file, this is the cause.

## References

- [Angular testing guide — components](https://angular.dev/guide/testing/components-basics)
- [Angular testing scenarios](https://angular.dev/guide/testing/components-scenarios)
- [Angular component harnesses overview](https://angular.dev/guide/testing/component-harnesses-overview)
- [Vitest docs — browser mode](https://vitest.dev/guide/browser/)
- [`@angular/build:unit-test` builder](https://angular.dev/tools/cli/build-system-migration)
- [#5017 — Vitest browser-mode setup](https://github.com/apache/texera/pull/5017)
