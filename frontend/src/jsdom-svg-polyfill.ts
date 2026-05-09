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

// Test-environment polyfills + setup hooks for jsdom + the Angular
// `@angular/build:unit-test` builder. Pulled in via `setupFiles` in
// `angular.json`. Each block below patches one specific gap that surfaces
// when the codingame monaco-vscode-* v25 stack or jointjs runs under jsdom.

// ───────────────────────────────────────────────────────────────────────────
// Node ESM loader hook so every transitive `.css` import resolves to an empty
// module. The unit-test builder pre-bundles spec files with `externalPackages:
// true`, so imports like `monaco-languageclient` reach Node's native ESM
// loader instead of Vite's transform pipeline — without the hook, every spec
// that transitively loads the codingame v25 stack crashes with
// `Unknown file extension ".css"`. The hook source lives inline as a `data:`
// URL so we don't carry a sidecar `.mjs`. Must run before any spec body
// imports the affected packages; `module.register` needs Node 20.6+ (the
// project pins Node ≥ 24).
import { register as registerLoader } from "node:module";

const cssLoaderHookSource = `
export function resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".css") || /\\.css(\\?|$)/.test(specifier)) {
    return {
      url: "data:text/javascript,export%20default%20%7B%7D%3B",
      shortCircuit: true,
      format: "module",
    };
  }
  return nextResolve(specifier, context);
}
`;
registerLoader(`data:text/javascript;charset=utf-8,${encodeURIComponent(cssLoaderHookSource)}`);

type AnyFn = (...args: unknown[]) => unknown;

// ───────────────────────────────────────────────────────────────────────────
// SVG geometry APIs (`SVGSVGElement#createSVGMatrix`, `createSVGPoint`,
// `createSVGTransform`, `getScreenCTM`, `getCTM`, `getBBox`). jsdom doesn't
// implement these and jointjs reaches into them during graph layout, so the
// spec build crashes with `TypeError: svgDocument.createSVGMatrix is not a
// function`. Stubs below return identity-ish geometry — enough for jointjs
// construction code not to throw. Specs needing accurate geometry should
// run under Vitest browser mode rather than jsdom (tracked in #4861).
function fakeMatrix() {
  // Minimal SVGMatrix shape — just the methods jointjs touches.
  const m: Record<string, unknown> = { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 };
  m.multiply = () => fakeMatrix();
  m.inverse = () => fakeMatrix();
  m.translate = () => fakeMatrix();
  m.scale = () => fakeMatrix();
  m.scaleNonUniform = () => fakeMatrix();
  m.rotate = () => fakeMatrix();
  m.rotateFromVector = () => fakeMatrix();
  m.flipX = () => fakeMatrix();
  m.flipY = () => fakeMatrix();
  m.skewX = () => fakeMatrix();
  m.skewY = () => fakeMatrix();
  return m;
}

function fakePoint() {
  const p: Record<string, unknown> = { x: 0, y: 0 };
  p.matrixTransform = () => fakePoint();
  return p;
}

function fakeTransform() {
  return {
    type: 0,
    matrix: fakeMatrix(),
    angle: 0,
    setMatrix: () => undefined,
    setTranslate: () => undefined,
    setScale: () => undefined,
    setRotate: () => undefined,
    setSkewX: () => undefined,
    setSkewY: () => undefined,
  };
}

function fakeRect() {
  return { x: 0, y: 0, width: 0, height: 0 };
}

const SVG_GLOBAL = (globalThis as unknown as { SVGSVGElement?: { prototype: Record<string, AnyFn> } }).SVGSVGElement;
const SVG_ELEMENT_GLOBAL = (globalThis as unknown as { SVGGraphicsElement?: { prototype: Record<string, AnyFn> } })
  .SVGGraphicsElement;

if (SVG_GLOBAL?.prototype) {
  const proto = SVG_GLOBAL.prototype;
  if (typeof proto.createSVGMatrix !== "function") proto.createSVGMatrix = fakeMatrix as AnyFn;
  if (typeof proto.createSVGPoint !== "function") proto.createSVGPoint = fakePoint as AnyFn;
  if (typeof proto.createSVGTransform !== "function") proto.createSVGTransform = fakeTransform as AnyFn;
  if (typeof proto.createSVGTransformFromMatrix !== "function")
    proto.createSVGTransformFromMatrix = fakeTransform as AnyFn;
}

if (SVG_ELEMENT_GLOBAL?.prototype) {
  const proto = SVG_ELEMENT_GLOBAL.prototype;
  if (typeof proto.getScreenCTM !== "function") proto.getScreenCTM = fakeMatrix as AnyFn;
  if (typeof proto.getCTM !== "function") proto.getCTM = fakeMatrix as AnyFn;
  if (typeof proto.getBBox !== "function") proto.getBBox = fakeRect as AnyFn;
}

// Constructable Stylesheets API (`new CSSStyleSheet().replaceSync(...)`) —
// jsdom doesn't ship it, but @codingame/monaco-vscode-api v25 calls it at
// module load. Stub with an inert constructor; specs don't visually render
// anything, so swallowing CSS is safe.
const CSS_GLOBAL = (globalThis as unknown as { CSSStyleSheet?: { prototype: Record<string, AnyFn> } }).CSSStyleSheet;
if (!CSS_GLOBAL) {
  class InertCSSStyleSheet {
    cssRules: unknown[] = [];
    replaceSync(): void {}
    replace(): Promise<void> {
      return Promise.resolve();
    }
    insertRule(): number {
      return 0;
    }
    deleteRule(): void {}
  }
  (globalThis as unknown as { CSSStyleSheet: typeof InertCSSStyleSheet }).CSSStyleSheet = InertCSSStyleSheet;
} else if (typeof CSS_GLOBAL.prototype.replaceSync !== "function") {
  CSS_GLOBAL.prototype.replaceSync = (() => undefined) as AnyFn;
  if (typeof CSS_GLOBAL.prototype.replace !== "function") {
    CSS_GLOBAL.prototype.replace = (() => Promise.resolve()) as AnyFn;
  }
}

// `Document.prototype` shims — jsdom is missing `adoptedStyleSheets` (used by
// the codingame runtime to push Constructable Stylesheets at it) and the
// legacy `queryCommandSupported` (probed by monaco-editor on init).
const docProto = (globalThis as unknown as { Document?: { prototype: Record<string, unknown> } }).Document?.prototype;
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
if (docProto && typeof docProto.queryCommandSupported !== "function") {
  (docProto as Record<string, unknown>).queryCommandSupported = (() => false) as AnyFn;
}

// `CSS` global namespace (`CSS.escape`, `CSS.supports`) — jsdom doesn't
// ship it; the codingame v25 theme service calls `CSS.escape(...)` from an
// idle-callback runner and crashes without the stub. The escape impl
// mirrors the spec (https://drafts.csswg.org/cssom/#serialize-an-identifier)
// just enough that `value === out` for the common case — otherwise a noisy
// `console.warn` fires every paint.
const cssGlobal = globalThis as unknown as { CSS?: { escape?: (value: string) => string; supports?: AnyFn } };
if (!cssGlobal.CSS) {
  cssGlobal.CSS = {};
}
if (typeof cssGlobal.CSS.escape !== "function") {
  cssGlobal.CSS.escape = (value: string) => String(value).replace(/[!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~]/g, "\\$&");
}
if (typeof cssGlobal.CSS.supports !== "function") {
  cssGlobal.CSS.supports = (() => false) as AnyFn;
}

// `window.matchMedia` — jsdom doesn't implement it; the codingame v25 theme
// service calls it in a deferred idle callback to detect dark/light preference.
// Stub returns an inert MediaQueryList that always reports no match.
const winForMatchMedia = globalThis as unknown as {
  matchMedia?: AnyFn;
  window?: { matchMedia?: AnyFn };
};
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
if (typeof winForMatchMedia.matchMedia !== "function") {
  winForMatchMedia.matchMedia = matchMediaStub;
}
if (winForMatchMedia.window && typeof winForMatchMedia.window.matchMedia !== "function") {
  winForMatchMedia.window.matchMedia = matchMediaStub;
}

// `requestIdleCallback` / `cancelIdleCallback` — Chrome-only APIs jsdom
// doesn't ship; monaco-related modules crash at construction without them.
// Approximate with `setTimeout`; the deadline arg is a coarse stub for
// callers that only read `didTimeout`.
const idleGlobal = globalThis as unknown as Record<string, AnyFn | undefined>;
if (typeof idleGlobal.requestIdleCallback !== "function") {
  idleGlobal.requestIdleCallback = ((cb: (d: { didTimeout: boolean; timeRemaining: () => number }) => void) =>
    setTimeout(() => cb({ didTimeout: false, timeRemaining: () => 50 }), 0)) as AnyFn;
}
if (typeof idleGlobal.cancelIdleCallback !== "function") {
  idleGlobal.cancelIdleCallback = ((id: number) => clearTimeout(id)) as AnyFn;
}

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
  send(): void {}
  close(): void {}
  addEventListener(): void {}
  removeEventListener(): void {}
  dispatchEvent(): boolean {
    return false;
  }
  constructor(_url?: string, _protocols?: string | string[]) {}
}
(globalThis as unknown as { WebSocket: typeof InertWebSocket }).WebSocket = InertWebSocket;

// Process-level error suppression for benign ngZorro icon / codingame
// extension fetches. NzIconService fetches icon SVGs from `/assets/...` when
// the icon isn't pre-registered; jsdom's XHR rejects with `AggregateError`
// and the lookup re-throws as `IconNotFoundError`. Vitest catches both as
// unhandled errors and CI treats that as a hard failure. Stubbing every
// spec with `NzIconModule.forChild([...])` is impractical — there are
// dozens of icons. Suppress just these two patterns at the process level.
function isBenignIconError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  const stack = err instanceof Error ? err.stack ?? "" : "";
  return (
    msg.includes("[@ant-design/icons-angular]") ||
    (err instanceof Error && err.name === "AggregateError" && /xhr-utils/.test(stack)) ||
    // codingame v25 default extensions try to fetch their bundled themes /
    // language configs over `extension-file://` URIs at activation time. jsdom
    // can't resolve that scheme so the fetch rejects, but it's purely cosmetic
    // — the spec body never depends on the theme/grammar being applied.
    msg.includes("extension-file://") ||
    /workbenchThemeService|monaco-vscode-theme|monaco-vscode-.*-default-extension/.test(stack)
  );
}
process.on("uncaughtException", err => {
  if (!isBenignIconError(err)) throw err;
});
process.on("unhandledRejection", reason => {
  if (!isBenignIconError(reason)) throw reason;
});
