/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { buildLogicalPlan } from "../../app/workspace/service/execute-workflow/logical-plan";
import { WorkflowGraph } from "../../app/workspace/service/workflow-graph/model/workflow-graph";
import { SharedModel } from "../../app/workspace/service/workflow-graph/model/shared-model";
import {
  mockScanPredicate,
  mockResultPredicate,
  mockScanResultLink,
} from "../../app/workspace/service/workflow-graph/model/mock-workflow-data";

const shared = new SharedModel(1, undefined, undefined, { connect: false });
const graph = new WorkflowGraph([mockScanPredicate, mockResultPredicate], [mockScanResultLink], [], shared);
afterAll(() => {
  shared.wsProvider.destroy();
  shared.destroy();
  vi.restoreAllMocks();
});
describe("existing logical-plan wire contract", () => {
  it("keeps flattened operator fields and external ordinal port identities", () => {
    const plan = buildLogicalPlan(graph);
    expect(plan.operators).toEqual(
      [mockResultPredicate, mockScanPredicate].map(op => ({
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType,
        inputPorts: op.inputPorts,
        outputPorts: op.outputPorts,
      }))
    );
    expect(plan.links).toEqual([
      {
        fromOpId: mockScanPredicate.operatorID,
        fromPortId: { id: 0, internal: false },
        toOpId: mockResultPredicate.operatorID,
        toPortId: { id: 0, internal: false },
      },
    ]);
    expect(plan.opsToViewResult).toEqual([]);
    expect(plan.opsToReuseResult).toEqual([]);
  });
  it("filters view/reuse flags to the selected target subgraph", () => {
    graph.setOperatorProperty(mockScanPredicate.operatorID, { operatorID: "forged", text: "日本語" });
    graph.getSharedOperatorType(mockScanPredicate.operatorID).set("markedForReuse", true);
    graph.getSharedOperatorType(mockResultPredicate.operatorID).set("viewResult", true);
    const plan = buildLogicalPlan(graph, mockScanPredicate.operatorID);
    expect(plan.operators).toHaveLength(1);
    expect(plan.operators[0].operatorID).toBe(mockScanPredicate.operatorID);
    expect(plan.opsToViewResult).toEqual([]);
    expect(plan.opsToReuseResult).toEqual([mockScanPredicate.operatorID]);
  });
  it("keeps a missing target as a specific graph error", () => {
    expect(() => buildLogicalPlan(graph, "missing")).toThrow();
  });
});
