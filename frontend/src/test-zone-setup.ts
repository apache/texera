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
 * Wrap Vitest's `it` so each test body runs inside a freshly-forked
 * ProxyZone. This is a setupFile (referenced from `vitest.config.ts`),
 * so it executes once per test file before any spec body runs.
 */
import "zone.js/testing";

type ZoneType = {
  current: { fork: (spec: object) => { run: <T>(fn: () => T) => T } };
  ProxyZoneSpec: new () => object;
};

declare const Zone: ZoneType;

const ProxyZoneSpec = (Zone as unknown as { ProxyZoneSpec: new () => object }).ProxyZoneSpec;

type ItFn = (name: string, fn?: (...args: unknown[]) => unknown, timeout?: number) => unknown;

function wrapInProxyZone<T extends ItFn>(target: T): T {
  const wrapped = ((name: string, fn?: (...args: unknown[]) => unknown, timeout?: number) => {
    if (!fn) return target(name);
    return target(
      name,
      function wrapper(this: unknown, ...args: unknown[]) {
        return new Promise<void>((resolve, reject) => {
          const zone = Zone.current.fork(new ProxyZoneSpec());
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
