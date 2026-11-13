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

import { SearchResultItem, searchResultPayload } from "./search-result";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { DashboardModel } from "./dashboard-model.interface";

describe("searchResultPayload", () => {
  const workflow = { isOwner: true, workflow: { wid: 1, name: "wf" } } as unknown as DashboardWorkflow;
  const dataset = { isOwner: true, dataset: { did: 2, name: "ds" } } as unknown as DashboardDataset;
  const model = { isOwner: true, model: { mid: 3, name: "m" } } as unknown as DashboardModel;

  it("returns a workflow row's payload", () => {
    expect(searchResultPayload({ resourceType: "workflow", workflow })).toBe(workflow);
  });

  it("returns a dataset row's payload", () => {
    expect(searchResultPayload({ resourceType: "dataset", dataset })).toBe(dataset);
  });

  it("returns a model row's payload", () => {
    expect(searchResultPayload({ resourceType: "model", model })).toBe(model);
  });

  it("returns undefined for a row of a kind this client does not model", () => {
    expect(searchResultPayload({ resourceType: "computing-unit" })).toBeUndefined();
    const project = { resourceType: "project", project: { pid: 4, name: "p" } } as unknown as SearchResultItem;
    expect(searchResultPayload(project)).toBeUndefined();
  });

  it("returns undefined, not null, for a row whose payload field is null", () => {
    const orphan = { resourceType: "model", model: null } as unknown as SearchResultItem;
    expect(searchResultPayload(orphan)).toBeUndefined();
  });
});
