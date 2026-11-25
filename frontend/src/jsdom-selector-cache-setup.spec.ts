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

import { Component } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";

/**
 * Guards the selector-cache reset that `jsdom-selector-cache-setup.ts` runs before each test.
 *
 * The reset finds nwsapi's caches through private jsdom fields and does nothing when they are missing,
 * so a jsdom upgrade that moved them would quietly bring the memory growth back. These tests read the
 * same fields and fail instead. The reset runs between tests, so each test that fills the cache is
 * followed by one that checks what the reset left behind; the tests rely on running in order.
 */

type NwsapiCaches = { selectResolvers: Record<string, unknown>; matchResolvers: Record<string, unknown> };
type DocumentImpl = { _nwsapi?: NwsapiCaches };

function documentImpl(): DocumentImpl {
  const implSymbol = Object.getOwnPropertySymbols(document).find(symbol => symbol.description === "impl");
  if (!implSymbol) throw new Error("jsdom no longer keeps the document's implementation under its `impl` symbol");
  return (document as unknown as Record<symbol, DocumentImpl>)[implSymbol];
}

function selectorEngine(): NwsapiCaches {
  const engine = documentImpl()._nwsapi;
  if (!engine) throw new Error("jsdom no longer keeps the document's selector engine at `_nwsapi`");
  return engine;
}

@Component({ selector: "texera-selector-cache-probe", template: '<span class="probe">probe</span>', standalone: true })
class ProbeComponent {}

describe("jsdom-selector-cache-setup", () => {
  // The objects the engine cached into while a fixture was alive.
  let filledCaches: NwsapiCaches;

  it("finds the cache that a TestBed fixture fills", () => {
    const fixture = TestBed.createComponent(ProbeComponent);
    fixture.detectChanges();
    // `By.css` goes through `Element.matches`, which caches in `matchResolvers`.
    expect(fixture.nativeElement.querySelector(".probe")).not.toBeNull();
    expect(fixture.debugElement.query(By.css(".probe"))).not.toBeNull();

    const { selectResolvers, matchResolvers } = selectorEngine();
    filledCaches = { selectResolvers, matchResolvers };
    // TestBed found the fixture's host element by this id, so the cache now holds the fixture.
    expect(Object.keys(selectResolvers)).toEqual(expect.arrayContaining([`#${fixture.nativeElement.id}`, ".probe"]));
    expect(Object.keys(matchResolvers)).toContain(".probe");
  });

  it("empties that cache in place after the fixture is torn down", () => {
    const engine = selectorEngine();
    // The engine keeps reading the objects it exported, so they have to be emptied, not replaced.
    expect(engine.selectResolvers).toBe(filledCaches.selectResolvers);
    expect(engine.matchResolvers).toBe(filledCaches.matchResolvers);
    // Fully empty, so the queries TestBed's teardown ran after the test are gone too.
    expect(Object.keys(engine.selectResolvers)).toEqual([]);
    expect(Object.keys(engine.matchResolvers)).toEqual([]);
  });

  it("keeps answering queries after the cache is emptied", () => {
    // TestBed finds the new fixture's host element with a query, so creating the fixture is the first check.
    const fixture = TestBed.createComponent(ProbeComponent);
    fixture.detectChanges();
    const probe = fixture.nativeElement.querySelector(".probe");
    expect(probe).not.toBeNull();
    expect(fixture.debugElement.query(By.css(".probe")).nativeElement).toBe(probe);

    // The engine caches its answers in the same objects again.
    expect(Object.keys(filledCaches.selectResolvers)).toContain(".probe");
    expect(Object.keys(filledCaches.matchResolvers)).toContain(".probe");
  });

  describe("before jsdom starts the document's selector engine", () => {
    let engine: NwsapiCaches | undefined;

    // jsdom starts the engine on a document's first selector query. Detach it to get back to that
    // state, then put it back so the rest of the worker keeps the same engine.
    beforeAll(() => {
      engine = documentImpl()._nwsapi;
      documentImpl()._nwsapi = undefined;
    });

    afterAll(() => {
      documentImpl()._nwsapi = engine;
    });

    it("skips the reset without starting the engine", () => {
      // The reset already ran before this test, so reaching this line means it did not throw.
      expect(documentImpl()._nwsapi).toBeUndefined();
    });
  });
});
