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
import { macroGraph } from "../../../../../../common/workflow-core/src/test/typescript/macro-test-graph";
import { TestBed } from "@angular/core/testing";
import { provideHttpClient } from "@angular/common/http";
import { HttpTestingController, provideHttpClientTesting } from "@angular/common/http/testing";
import { MacroExpansionService } from "./macro-expansion.service";
import { testWorkflow } from "../../../common/testing/workflow-graph";

describe("backend macro expansion", () => {
  let service: MacroExpansionService;
  let http: HttpTestingController;
  const graph = testWorkflow();
  const expanded = { content: graph, paths: {}, ports: {} };
  const expansionRequest = () => http.expectOne(request => request.url.endsWith("/compile/expand"));
  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [provideHttpClient(), provideHttpClientTesting()] });
    service = TestBed.inject(MacroExpansionService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  it("shares an in-flight request and uses the backend response without changing the snapshot", async () => {
    const original = JSON.stringify(graph);
    const first = service.expand(graph),
      second = service.expand(graph);
    const request = expansionRequest();
    expect(request.request.body).toEqual(graph);
    request.flush(expanded);
    expect(await first).toEqual(expanded);
    expect(await second).toEqual(expanded);
    expect(service.get(graph)).toEqual(expanded);
    expect(JSON.stringify(graph)).toBe(original);
    expect(await service.expand(graph)).toEqual(expanded);
    http.expectNone(request => request.url.endsWith("/compile/expand"));
  });

  it("does not cache a failure and retries the same snapshot", async () => {
    const failed = expect(service.expand(graph)).rejects.toThrow("Recursive macro reference");
    expansionRequest().flush({ message: "Recursive macro reference" }, { status: 400, statusText: "Bad Request" });
    await failed;
    expect(service.get(graph)).toBeUndefined();
    const retry = service.expand(graph);
    expansionRequest().flush(expanded);
    await retry;
  });

  it("invalidates exposed-property and interface changes while reusing presentation-only changes", () => {
    service.remember(graph, expanded);
    expect(service.get({ ...graph, operatorPositions: { moved: { x: 30, y: 20 } } })).toBe(expanded);
    expect(service.get({ ...graph, formBinding: { fields: [] } })).toBeUndefined();
    expect(
      service.get(macroGraph({ ...graph, macroInterface: { id: "new", inputs: [], outputs: [] } }))
    ).toBeUndefined();
  });
  it("retains an actively inspected execution while evicting unused previews", () => {
    service.remember(graph, expanded);
    const previews = Array.from({ length: 25 }, (_, i) => ({
      ...graph,
      formBinding: { fields: [], instruction: { body: String(i) } },
    }));
    for (const preview of previews) {
      expect(service.get(graph)).toBe(expanded);
      service.remember(preview, { ...expanded, content: preview });
    }
    expect(service.get(graph)).toBe(expanded);
    expect(service.get(previews[0])).toBeUndefined();
    expect(service.get(previews.at(-1)!)).toBeDefined();
  });
});
