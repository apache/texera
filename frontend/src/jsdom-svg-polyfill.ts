/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Test-env polyfills loaded via `setupFiles` in `angular.json`. Most of
// the gaps below were introduced by the monaco-languageclient v10 upgrade,
// which pulls in `@codingame/monaco-vscode-*` v25 — that stack calls into
// browser APIs jsdom doesn't ship (Constructable Stylesheets, `CSS.escape`,
// `matchMedia`, `requestIdleCallback`, …). jointjs's SVG geometry stubs
// are the only block that predates the upgrade.

// CSS-import shim: the unit-test builder pre-bundles specs with
// `externalPackages: true`, so transitive `.css` imports from the codingame
// stack reach Node's native ESM loader and crash with
// `Unknown file extension ".css"`. Register a hook that resolves any `.css`
// specifier to a `CSSStyleSheet`-shaped no-op module. Source is inlined as
// a `data:` URL (no `.mjs` sidecar). Gated by a `globalThis` flag because
// `setupFiles` re-runs per spec file and `module.register` chains.
import { register as registerLoader } from "node:module";

const CSS_HOOK_FLAG = Symbol.for("texera.cssLoaderHookRegistered");
const PROCESS_HANDLERS_FLAG = Symbol.for("texera.processErrorHandlersInstalled");
const flagHolder = globalThis as Record<symbol, boolean | undefined>;
if (!flagHolder[CSS_HOOK_FLAG]) {
  // The hook's default export is a CSSStyleSheet-shaped stub rather than a
  // bare `{}` — a few transitive consumers (codingame v25's
  // `css-style-sheet` export form) read `.replaceSync` / `.cssRules` off
  // the imported value, and a plain empty object would crash them.
  const stubModule =
    "const s = { cssRules: [] };" +
    "s.replaceSync = () => {};" +
    "s.replace = () => Promise.resolve();" +
    "s.insertRule = () => 0;" +
    "s.deleteRule = () => {};" +
    "export default s;";
  const stubUrl = `data:text/javascript;charset=utf-8,${encodeURIComponent(stubModule)}`;
  const cssLoaderHookSource = `
const STUB_URL = ${JSON.stringify(stubUrl)};
export function resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".css") || /\\.css(\\?|$)/.test(specifier)) {
    return { url: STUB_URL, shortCircuit: true, format: "module" };
  }
  return nextResolve(specifier, context);
}
`;
  registerLoader(`data:text/javascript;charset=utf-8,${encodeURIComponent(cssLoaderHookSource)}`);
  flagHolder[CSS_HOOK_FLAG] = true;
}

type AnyFn = (...args: unknown[]) => unknown;
// Loose `globalThis` accessor — jsdom installs DOM globals here, but TS's
// `lib.dom.d.ts` types many of them as never-undefined. Cast through `any`
// so the `?.prototype` short-circuits below stay typed-light.
const G = globalThis as Record<string, any>;
const installIfMissing = (proto: Record<string, AnyFn> | undefined, fns: Record<string, AnyFn>) => {
  if (!proto) return;
  for (const [name, impl] of Object.entries(fns)) {
    if (typeof proto[name] !== "function") proto[name] = impl;
  }
};

// SVG geometry APIs (`SVGSVGElement#createSVGMatrix`, `createSVGPoint`,
// `createSVGTransform`, `getScreenCTM`, `getCTM`, `getBBox`). jsdom doesn't
// implement these and jointjs reaches into them during graph layout, so the
// spec build crashes with `TypeError: svgDocument.createSVGMatrix is not a
// function`. Stubs return identity-ish geometry — enough for jointjs to
// instantiate. Specs needing accurate geometry should run under Vitest
// browser mode rather than jsdom (tracked in #4861).
// Method names are space-separated to keep the prettier-formatted source on
// one line each; both stubs only need their named methods to exist as
// callables, so the contents don't matter beyond returning the right shape.
const MATRIX_METHODS =
  "multiply inverse translate scale scaleNonUniform rotate rotateFromVector flipX flipY skewX skewY";
const TRANSFORM_METHODS = "setMatrix setTranslate setScale setRotate setSkewX setSkewY";
function fakeMatrix(): Record<string, unknown> {
  const m: Record<string, unknown> = { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 };
  for (const fn of MATRIX_METHODS.split(" ")) m[fn] = () => fakeMatrix();
  return m;
}
function fakeTransform(): Record<string, unknown> {
  const t: Record<string, unknown> = { type: 0, matrix: fakeMatrix(), angle: 0 };
  for (const fn of TRANSFORM_METHODS.split(" ")) t[fn] = () => undefined;
  return t;
}
const fakePoint = (): Record<string, unknown> => ({ x: 0, y: 0, matrixTransform: () => fakePoint() });
const fakeRect = () => ({ x: 0, y: 0, width: 0, height: 0 });
installIfMissing(G.SVGSVGElement?.prototype, {
  createSVGMatrix: fakeMatrix as AnyFn,
  createSVGPoint: fakePoint as AnyFn,
  createSVGTransform: fakeTransform as AnyFn,
  createSVGTransformFromMatrix: fakeTransform as AnyFn,
});
installIfMissing(G.SVGGraphicsElement?.prototype, {
  getScreenCTM: fakeMatrix as AnyFn,
  getCTM: fakeMatrix as AnyFn,
  getBBox: fakeRect as AnyFn,
});

// Constructable Stylesheets API (`new CSSStyleSheet().replaceSync(...)`) —
// jsdom doesn't ship it, but @codingame/monaco-vscode-api v25 calls it at
// module load. Stub with an inert constructor; specs don't visually render
// anything, so swallowing CSS is safe.
if (!G.CSSStyleSheet) {
  G.CSSStyleSheet = class {
    cssRules: unknown[] = [];
    replaceSync = () => undefined;
    replace = () => Promise.resolve();
    insertRule = () => 0;
    deleteRule = () => undefined;
  };
} else {
  installIfMissing(G.CSSStyleSheet.prototype, {
    replaceSync: (() => undefined) as AnyFn,
    replace: (() => Promise.resolve()) as AnyFn,
  });
}

// `Document.prototype` shims — jsdom is missing `adoptedStyleSheets` (used by
// the codingame runtime to push Constructable Stylesheets at it) and the
// legacy `queryCommandSupported` (probed by monaco-editor on init).
const docProto = G.Document?.prototype as Record<string, unknown> | undefined;
if (docProto && !("adoptedStyleSheets" in docProto)) {
  Object.defineProperty(docProto, "adoptedStyleSheets", {
    configurable: true,
    get() {
      return (this as { __adoptedStyleSheets?: unknown[] }).__adoptedStyleSheets ?? [];
    },
    set(v: unknown[]) {
      (this as { __adoptedStyleSheets?: unknown[] }).__adoptedStyleSheets = v;
    },
  });
}
installIfMissing(docProto as Record<string, AnyFn> | undefined, { queryCommandSupported: (() => false) as AnyFn });

// `CSS` global namespace (`CSS.escape`, `CSS.supports`) — jsdom doesn't
// ship it; the codingame v25 theme service calls `CSS.escape(...)` from an
// idle-callback runner and crashes without the stub. The escape impl mirrors
// the spec (https://drafts.csswg.org/cssom/#serialize-an-identifier) just
// enough that `value === out` for the common case — otherwise a noisy
// `console.warn` fires every paint.
G.CSS ??= {};
installIfMissing(G.CSS, {
  escape: ((v: string) => String(v).replace(/[!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~]/g, "\\$&")) as AnyFn,
  supports: (() => false) as AnyFn,
});

// `window.matchMedia` — jsdom doesn't implement it; the codingame v25 theme
// service calls it in a deferred idle callback to detect dark/light preference.
// Stub returns an inert MediaQueryList that always reports no match.
const matchMediaStub: AnyFn = ((query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: () => undefined,
  removeListener: () => undefined,
  addEventListener: () => undefined,
  removeEventListener: () => undefined,
  dispatchEvent: () => false,
})) as AnyFn;
if (typeof G.matchMedia !== "function") G.matchMedia = matchMediaStub;
if (G.window && typeof G.window.matchMedia !== "function") G.window.matchMedia = matchMediaStub;

// `requestIdleCallback` / `cancelIdleCallback` — Chrome-only APIs jsdom
// doesn't ship; monaco-related modules crash at construction without them.
// Approximate with `setTimeout`; the deadline arg is a coarse stub for
// callers that only read `didTimeout`.
G.requestIdleCallback ??= (cb: (d: { didTimeout: boolean; timeRemaining: () => number }) => void) =>
  setTimeout(() => cb({ didTimeout: false, timeRemaining: () => 50 }), 0);
G.cancelIdleCallback ??= (id: number) => clearTimeout(id);

// `ResizeObserver` — jsdom doesn't implement it; components that watch their
// own size (e.g. markdown-description) construct one on render. An inert stub
// is enough: jsdom has no layout, so there is never a resize to report.
G.ResizeObserver ??= class {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
};

// `WebSocket` — y-websocket schedules a reconnect timer the moment a
// collaborative-editing service is constructed. When that timer fires AFTER
// vitest has begun tearing down the jsdom window, jsdom's WebSocket
// implementation crashes during construction (`Cannot read properties of null
// (reading '_cookieJar')` → `Invalid value used as weak map key`) and vitest
// fails the run even though every test passed. Stub with an inert no-op so
// the timer can fire without touching jsdom; the only specs that genuinely
// exercise WebSocket behaviour are already excluded from the suite. Real
// WebSocket testing belongs under Vitest browser mode.
class InertWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSING = 2;
  readonly CLOSED = 3;
  readyState = 3;
  bufferedAmount = 0;
  binaryType: "blob" | "arraybuffer" = "blob";
  url = "";
  protocol = "";
  extensions = "";
  onopen: AnyFn | null = null;
  onerror: AnyFn | null = null;
  onmessage: AnyFn | null = null;
  onclose: AnyFn | null = null;
  send = () => undefined;
  close = () => undefined;
  addEventListener = () => undefined;
  removeEventListener = () => undefined;
  dispatchEvent = () => false;
  constructor(_url?: string, _protocols?: string | string[]) {}
}
G.WebSocket = InertWebSocket;

// Process-level error suppression for benign ngZorro icon / codingame
// extension fetches. NzIconService fetches icon SVGs from `/assets/...` when
// the icon isn't pre-registered; jsdom's XHR rejects with `AggregateError`
// and the lookup re-throws as `IconNotFoundError`. Vitest catches both as
// unhandled errors and CI treats that as a hard failure. Stubbing every spec
// with `NzIconModule.forChild([...])` is impractical — dozens of icons.
// Suppress just these patterns at the process level.
function isBenignIconError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  const stack = err instanceof Error ? err.stack ?? "" : "";
  return (
    msg.includes("[@ant-design/icons-angular]") ||
    (err instanceof Error && err.name === "AggregateError" && /xhr-utils/.test(stack)) ||
    // codingame v25 default extensions try to fetch their bundled themes /
    // language configs over `extension-file://` URIs at activation time.
    // jsdom can't resolve the scheme so the fetch rejects, but it's cosmetic
    // — the spec body never depends on the theme/grammar being applied.
    msg.includes("extension-file://") ||
    /workbenchThemeService|monaco-vscode-theme|monaco-vscode-.*-default-extension/.test(stack)
  );
}
// Same gating pattern as the CSS loader hook above — vitest re-evaluates
// this setup file once per spec file, and attaching fresh `process.on(...)`
// handlers each time grows the listener chain (`MaxListenersExceededWarning`
// after ~11 specs) and reruns the benign-error filter on every captured
// rejection.
if (!flagHolder[PROCESS_HANDLERS_FLAG]) {
  process.on("uncaughtException", err => {
    if (isBenignIconError(err)) return;
    // Re-throwing inside `uncaughtException` aborts the Node process, which
    // crashes the Vitest worker mid-run and leaves the runner hanging.
    console.error(err);
  });
  process.on("unhandledRejection", reason => {
    if (isBenignIconError(reason)) return;
    console.error(reason);
  });
  flagHolder[PROCESS_HANDLERS_FLAG] = true;
}
