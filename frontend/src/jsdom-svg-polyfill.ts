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

/**
 * jsdom doesn't implement the SVG geometry APIs (`SVGSVGElement#createSVGMatrix`,
 * `createSVGPoint`, `createSVGTransform`, `getScreenCTM`, `getCTM`,
 * `getBBox`). jointjs reaches into these during graph layout and crashes
 * the spec build with `TypeError: svgDocument.createSVGMatrix is not a
 * function` etc.
 *
 * The stubs below return identity-ish geometry: matrices/points behave like
 * the identity, bounding boxes report zero dimensions. That's enough for
 * jointjs construction code to not throw; specs that actually depend on
 * accurate geometry should run under Vitest browser mode rather than
 * jsdom (tracked in #4861), but the bulk of the texera specs only need
 * jointjs to instantiate cleanly.
 */

type AnyFn = (...args: unknown[]) => unknown;

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

/**
 * jsdom doesn't implement the legacy `document.queryCommandSupported`,
 * which monaco-editor probes during initialization. Without it the
 * editor's setup throws even when no spec actually exercises monaco.
 */
const docProto = (globalThis as unknown as { Document?: { prototype: Record<string, AnyFn> } }).Document?.prototype;
if (docProto && typeof docProto.queryCommandSupported !== "function") {
  docProto.queryCommandSupported = (() => false) as AnyFn;
}

/**
 * jsdom doesn't implement `requestIdleCallback` / `cancelIdleCallback`
 * (a Chrome-only API). Specs that pull in monaco-related modules
 * crash at construction with `ReferenceError: requestIdleCallback is
 * not defined`.
 *
 * Approximate with `setTimeout` so callbacks still fire. The deadline
 * argument is a coarse stub — enough for callers that only read
 * `didTimeout`.
 */
const idleGlobal = globalThis as unknown as Record<string, AnyFn | undefined>;
if (typeof idleGlobal.requestIdleCallback !== "function") {
  idleGlobal.requestIdleCallback = ((cb: (d: { didTimeout: boolean; timeRemaining: () => number }) => void) =>
    setTimeout(() => cb({ didTimeout: false, timeRemaining: () => 50 }), 0)) as AnyFn;
}
if (typeof idleGlobal.cancelIdleCallback !== "function") {
  idleGlobal.cancelIdleCallback = ((id: number) => clearTimeout(id)) as AnyFn;
}

/**
 * jsdom doesn't implement `ResizeObserver`; components that watch their own
 * size (e.g. markdown-description) construct one on render. An inert stub is
 * enough: jsdom has no layout, so there is never a resize to report.
 */
const observerGlobal = globalThis as unknown as { ResizeObserver?: unknown };
observerGlobal.ResizeObserver ??= class {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
};

<<<<<<< HEAD
/**
 * y-websocket schedules a reconnect timer the moment a service that uses
 * collaborative editing is constructed. When that timer fires AFTER vitest
 * has begun tearing down the jsdom window, jsdom's WebSocket implementation
 * crashes during construction (`Cannot read properties of null (reading
 * '_cookieJar')` → `Invalid value used as weak map key`). Vitest catches
 * this as an unhandled error and fails the run even though every test
 * passed.
 *
 * Stub WebSocket with an inert no-op so the timer can fire without
 * touching jsdom. The collaborative-editing specs that actually exercise
 * WebSocket behaviour are excluded from the test suite (component specs +
 * the workflow-action suite is the only collaboration-touching active
 * spec). Real WebSocket testing belongs under Vitest browser mode.
 */
=======
// `window.getComputedStyle(elt, pseudoElt)` and `window.scrollTo` — jsdom
// implements neither. Both route through its `notImplemented` helper, which
// emits a `jsdomError` on the virtual console; vitest's jsdom environment
// forwards that to `console.error`, one full stack trace per call. html2canvas
// calls both on every render — a `:before` and an `:after` lookup for each
// cloned node, plus one `scrollTo` per document clone — so the
// report-generation spec, which drives the real renderer on purpose (`vi.mock`
// can't reach it under the builder's `isolate: false`), buries the run in
// traces while all of its tests pass.
// Dropping `pseudoElt` changes no behaviour for the arguments jsdom only
// complains about: it ignores them and returns the element's own declaration
// either way. The exception is a shadow-DOM pseudo-element (`::part(…)` /
// `::slotted(…)`), which jsdom rejects with a `TypeError` before it reaches
// `notImplemented` — those are forwarded so the rejection still happens.
// `scrollTo` has nothing to move — jsdom has no layout.
// Both patches wrap a global that this setup file is re-evaluated against once
// per spec file, so each marks what it installed and does nothing when it finds
// its own mark — otherwise the wrappers nest one layer deeper per spec file.
// The mark rides on the installed function rather than on a `globalThis` flag
// so that a jsdom window replaced mid-run still gets patched.
const NOISE_PATCH_MARK = Symbol.for("texera.jsdomNotImplementedNoisePatched");
const alreadyPatched = (fn: unknown): boolean => typeof fn === "function" && NOISE_PATCH_MARK in (fn as object);
const markPatched = (fn: AnyFn): AnyFn => Object.assign(fn, { [NOISE_PATCH_MARK]: true });

const SHADOW_DOM_PSEUDO = /^::(?:part|slotted)\(/i;
const jsdomGetComputedStyle = G.getComputedStyle as ((elt: Element, pseudoElt?: string | null) => unknown) | undefined;
if (typeof jsdomGetComputedStyle === "function" && !alreadyPatched(jsdomGetComputedStyle)) {
  const withoutPseudoElement = markPatched(((elt: Element, pseudoElt?: string | null) =>
    pseudoElt !== undefined && pseudoElt !== null && SHADOW_DOM_PSEUDO.test(String(pseudoElt))
      ? jsdomGetComputedStyle(elt, pseudoElt)
      : jsdomGetComputedStyle(elt)) as AnyFn);
  G.getComputedStyle = withoutPseudoElement;
  if (G.window) G.window.getComputedStyle = withoutPseudoElement;
}
// The only `scrollTo` html2canvas aims at this window is guarded by a
// scroll-offset check that cannot fire under jsdom — there is no layout, so
// both offsets are 0 and the call is skipped. The one it does make belongs to
// the throwaway iframe it clones the page into, and jsdom installs the method
// on each window instance rather than on a shared prototype, so it has to be
// neutered as each `contentWindow` is handed out.
const inertScrollTo: AnyFn = () => undefined;
const iframeProto = G.HTMLIFrameElement?.prototype;
const contentWindow = iframeProto && Object.getOwnPropertyDescriptor(iframeProto, "contentWindow");
if (contentWindow?.get && !alreadyPatched(contentWindow.get)) {
  const getContentWindow = contentWindow.get;
  Object.defineProperty(iframeProto, "contentWindow", {
    ...contentWindow,
    get: markPatched(function (this: unknown): unknown {
      const frameWindow = getContentWindow.call(this) as Record<string, unknown> | null;
      if (frameWindow) frameWindow.scrollTo = inertScrollTo;
      return frameWindow;
    } as AnyFn),
  });
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
>>>>>>> 646e46083 (fix(test, frontend): bound the workflow-snapshot render's cost under jsdom (#7999))
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

/**
 * NgZorro's NzIconService dynamically fetches icon SVGs over HTTP from
 * `/assets/...` when the icon isn't pre-registered. jsdom's XHR
 * implementation rejects those requests with an `AggregateError`, and
 * downstream the icon lookup re-throws as `IconNotFoundError`. Vitest
 * catches both as unhandled errors, which CI treats as a hard failure
 * (locally Vitest only reports them as non-fatal warnings).
 *
 * Stubbing every spec with `NzIconModule.forChild([...])` for every
 * icon its template uses is impractical — there are dozens. Instead,
 * suppress the two specific error patterns at the process level: they
 * originate inside ngZorro's icon plumbing and don't affect the
 * assertions specs actually make.
 */
function isBenignIconError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return (
    msg.includes("[@ant-design/icons-angular]") ||
    (err instanceof Error && err.name === "AggregateError" && /xhr-utils/.test(err.stack ?? ""))
  );
}
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
