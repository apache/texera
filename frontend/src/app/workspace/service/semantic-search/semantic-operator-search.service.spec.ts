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

import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { INDEX_URL, SemanticOperatorSearchService } from "./semantic-operator-search.service";

// The real library downloads tens of megabytes of model on first use, so the
// service's dynamic import is answered with an extractor that always returns
// the same query vector. Ranking is then decided entirely by the index below.
// vi.hoisted, because vi.mock factories run before the rest of the module.
const { pipelineMock } = vi.hoisted(() => ({
  pipelineMock: vi.fn(async () => async () => ({ data: [1, 0] })),
}));
vi.mock("@xenova/transformers", () => ({
  env: { allowLocalModels: true },
  pipeline: pipelineMock,
}));

/** The vector the stubbed extractor always returns. */
const QUERY_VECTOR = [1, 0];

/** Unit vectors, so the dot product against QUERY_VECTOR is the similarity. */
const index = {
  model: "test-model",
  dims: 2,
  operators: [
    { operatorType: "Aligned", name: "Aligned", group: "G", description: "", hinted: false, vector: [1, 0] },
    { operatorType: "Half", name: "Half", group: "G", description: "", hinted: false, vector: [0.6, 0.8] },
    { operatorType: "Orthogonal", name: "Orthogonal", group: "G", description: "", hinted: false, vector: [0, 1] },
  ],
};

function schema(operatorType: string): OperatorSchema {
  return {
    operatorType,
    operatorVersion: "1",
    jsonSchema: {},
    additionalMetadata: {
      userFriendlyName: operatorType,
      operatorGroupName: "G",
      inputPorts: [],
      outputPorts: [],
    },
  } as unknown as OperatorSchema;
}

const available = [schema("Aligned"), schema("Half"), schema("Orthogonal")];

describe("SemanticOperatorSearchService", () => {
  let service: SemanticOperatorSearchService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    pipelineMock.mockClear();
    TestBed.configureTestingModule({ imports: [HttpClientTestingModule] });
    service = TestBed.inject(SemanticOperatorSearchService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  /** Lets the pending promises settle before the next expectation. */
  const settle = () => new Promise(resolve => setTimeout(resolve));

  it("ranks the operators on offer by similarity to the query", async () => {
    const hits = service.search("anything", available);
    httpMock.expectOne(INDEX_URL).flush(index);

    expect((await hits).map(hit => hit.schema.operatorType)).toEqual(["Aligned", "Half", "Orthogonal"]);
  });

  it("leaves out operators the palette is not offering", async () => {
    const hits = service.search("anything", [schema("Orthogonal")]);
    httpMock.expectOne(INDEX_URL).flush(index);

    expect((await hits).map(hit => hit.schema.operatorType)).toEqual(["Orthogonal"]);
  });

  it("loads once for searches that arrive together", async () => {
    const first = service.search("anything", available);
    const second = service.search("anything else", available);

    // expectOne fails if the second search started a second download.
    httpMock.expectOne(INDEX_URL).flush(index);
    await Promise.all([first, second]);

    expect(pipelineMock).toHaveBeenCalledTimes(1);
  });

  it("tries again after a failed load instead of staying disabled", async () => {
    // Without clearing the cached attempt, the rejected promise would be reused
    // for every later search and the ranker would stay down until a reload.
    const failed = service.search("anything", available);
    httpMock.expectOne(INDEX_URL).error(new ProgressEvent("network error"));
    await expect(failed).rejects.toBeDefined();
    await settle();

    const retried = service.search("anything", available);
    httpMock.expectOne(INDEX_URL).flush(index);

    expect((await retried).map(hit => hit.schema.operatorType)).toEqual(["Aligned", "Half", "Orthogonal"]);
    expect(service.isReady()).toBe(true);
  });
});
