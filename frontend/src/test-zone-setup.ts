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
 * Vitest+Angular doesn't install the ProxyZone wrapper around each test
 * that Karma+Jasmine implicitly provided. Without a ProxyZone in the
 * call chain, Angular's `fakeAsync` throws
 * `Expected to be running in 'ProxyZone'`.
 *
 * Wrap Vitest's `it` so each test body runs inside a ProxyZone. The proxy
 * is forked ONCE from the ROOT zone and reused for every test, with its
 * delegate reset between tests — the same shape zone.js uses for its own
 * jasmine/jest integrations and its shared-proxy helper.
 *
 * Forking from `Zone.root` (rather than `Zone.current`, as before) is what
 * keeps the proxy exactly one level deep. When an async spec resolves, its
 * continuation can leave the forked ProxyZone as `Zone.current`; the next
 * `Zone.current.fork(...)` then nested a proxy inside that one, and across
 * the many spec files a Vitest worker runs the chain grew without bound.
 * Every `zone.run()` recurses through the whole
 * `ProxyZoneSpec.onInvoke -> _ZoneDelegate.invoke` delegate chain, so once
 * it is deep enough the stack overflows with `RangeError: Maximum call
 * stack size exceeded` in whichever unrelated spec happens to be running.
 * See apache/texera#6593.
 *
 * This is a setupFile (referenced from `vitest.config.ts`), so it executes
 * once per test file before any spec body runs.
 */
import "zone.js/testing";

type ProxyZone = { run: <T>(fn: () => T) => T };
type ProxyZoneSpecInstance = { resetDelegate: () => void };

type ZoneType = {
  root: { fork: (spec: object) => ProxyZone };
  ProxyZoneSpec: new () => ProxyZoneSpecInstance;
};

declare const Zone: ZoneType;

const ProxyZoneSpec = Zone.ProxyZoneSpec;

// Fork a single ProxyZone from the root zone and reuse it for every test.
let sharedProxyZoneSpec: ProxyZoneSpecInstance | null = null;
let sharedProxyZone: ProxyZone | null = null;

function getProxyZone(): ProxyZone {
  let spec = sharedProxyZoneSpec;
  let zone = sharedProxyZone;
  if (!spec || !zone) {
    spec = new ProxyZoneSpec();
    zone = Zone.root.fork(spec);
    sharedProxyZoneSpec = spec;
    sharedProxyZone = zone;
  }
  // Clear any delegate a prior test (e.g. one that threw inside fakeAsync)
  // may have left set, so each test starts from a clean proxy state.
  spec.resetDelegate();
  return zone;
}

type ItFn = (name: string, fn?: (...args: unknown[]) => unknown, timeout?: number) => unknown;

function wrapInProxyZone<T extends ItFn>(target: T): T {
  const wrapped = ((name: string, fn?: (...args: unknown[]) => unknown, timeout?: number) => {
    if (!fn) return target(name);
    return target(
      name,
      function wrapper(this: unknown, ...args: unknown[]) {
        return new Promise<void>((resolve, reject) => {
          const zone = getProxyZone();
          zone.run(() => {
            try {
              const result = fn.apply(this, args);
              if (result && typeof (result as Promise<unknown>).then === "function") {
                (result as Promise<unknown>).then(() => resolve(), reject);
              } else {
                resolve();
              }
            } catch (e) {
              reject(e);
            }
          });
        });
      },
      timeout
    );
  }) as T;
  return wrapped;
}

function patchTestRunner(name: "it" | "test"): void {
  const g = globalThis as unknown as Record<string, unknown>;
  const original = g[name];
  if (typeof original !== "function") return;
  const wrapped = wrapInProxyZone(original as ItFn);
  // Forward all enumerable AND non-enumerable properties (.skip, .only,
  // .todo, .each, .skipIf, .runIf, ...) so callers like `it.todo(...)`
  // still resolve. Wrap .skip / .only with the same ProxyZone behaviour;
  // .todo / .each / others pass through unchanged.
  for (const key of Reflect.ownKeys(original)) {
    if (key === "length" || key === "name" || key === "prototype") continue;
    const value = (original as unknown as Record<string | symbol, unknown>)[key as string];
    const transformed =
      (key === "skip" || key === "only") && typeof value === "function" ? wrapInProxyZone(value as ItFn) : value;
    Object.defineProperty(wrapped, key, {
      value: transformed,
      writable: true,
      configurable: true,
      enumerable: true,
    });
  }
  g[name] = wrapped;
}

patchTestRunner("it");
patchTestRunner("test");
