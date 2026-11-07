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

import { afterEach, describe, expect, it } from "vitest";
import { validateWorkflowForRun } from "./workflow-validation";
import { WorkflowGraph } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { mockOperatorMetaData } from "../../core/workspace/service/operator-metadata/mock-operator-metadata.data";
import {
  mockScanPredicate,
  mockResultPredicate,
  mockScanResultLink,
  mockScalaExecutorPredicate,
  mockLoopStartPredicate,
  mockLoopEndPredicate,
  mockLoopStartScalaExecutorLink,
  mockScalaExecutorLoopEndLink,
} from "../../core/workspace/service/workflow-graph/model/mock-workflow-data";
import type { OperatorPredicate, OperatorLink } from "../../core/workspace/types/workflow-common.interface";
const models: SharedModel[] = [];
afterEach(() => models.splice(0).forEach(model => model.destroy()));
function graph(operators: OperatorPredicate[] = [], links: OperatorLink[] = []) {
  const shared = new SharedModel(undefined, undefined, undefined, { connect: false });
  models.push(shared);
  return new WorkflowGraph(operators, links, [], shared);
}
const source = { ...mockScanPredicate, operatorProperties: { tableName: "test-table" } };
const validate = (graph: WorkflowGraph) => validateWorkflowForRun({ graph, metadata: mockOperatorMetaData });
describe("workflow run validation", () => {
  it("accepts a connected workflow with complete required values", () => {
    const g = graph([source, mockResultPredicate], [mockScanResultLink]);
    const before = g.getAllOperators();
    expect(validate(g)).toEqual({ isValid: true, workflowEmpty: false, errors: {} });
    expect(g.getAllOperators()).toEqual(before);
  });
  it("keeps AJV keyword messages, validates all required inputs and leaves saved values untouched", () => {
    const g = graph([mockScanPredicate, mockResultPredicate]);
    const result = validate(g);
    expect(result.isValid).toBe(false);
    expect(result.errors[source.operatorID].messages.required).toBe("must have required property 'tableName'");
    expect(result.errors[mockResultPredicate.operatorID].messages.inputs).toBe(" requires at least 1 inputs, has 0");
    expect(g.getOperator(source.operatorID).operatorProperties).toEqual(mockScanPredicate.operatorProperties);
  });
  it("skips disabled nodes and ignores links touching them, while detecting empty enabled workflows", () => {
    const g = graph([source, mockResultPredicate], [mockScanResultLink]);
    g.disableOperator(source.operatorID);
    expect(validate(g).errors[source.operatorID]).toBeUndefined();
    expect(validate(g).errors[mockResultPredicate.operatorID].messages.inputs).toContain("has 0");
    g.disableOperator(mockResultPredicate.operatorID);
    expect(validate(g)).toEqual({ isValid: false, workflowEmpty: true, errors: {} });
    expect(validate(graph())).toEqual({ isValid: false, workflowEmpty: true, errors: {} });
  });
  it("enforces exactly one incoming link when the port disallows multiple links", () => {
    const sink = { ...mockResultPredicate, inputPorts: [{ portID: "input-0", disallowMultiInputs: true }] },
      second = { ...source, operatorID: "second" };
    const g = graph(
      [source, second, sink],
      [
        mockScanResultLink,
        { linkID: "second", source: { operatorID: "second", portID: "output-0" }, target: mockScanResultLink.target },
      ]
    );
    expect(validate(g).errors[sink.operatorID].messages.inputs).toBe(" requires 1 input, has 2");
    g.disableOperator(second.operatorID);
    expect(validate(g).isValid).toBe(true);
  });
  it("retains the legacy primitive-only loop-reference waiver and keeps malformed/array values invalid", () => {
    const body = (properties: Record<string, unknown>) => ({
      ...mockScalaExecutorPredicate,
      operatorProperties: properties,
    });
    const block = (properties: Record<string, unknown>) =>
      graph(
        [mockLoopStartPredicate, body(properties), mockLoopEndPredicate],
        [mockLoopStartScalaExecutorLink, mockScalaExecutorLoopEndLink]
      );
    expect(
      validate(block({ limit: "$K", fraction: "$unknown", caseSensitive: "$flag" })).errors[
        mockScalaExecutorPredicate.operatorID
      ]
    ).toBeUndefined();
    expect(validate(block({ limit: "$1st" })).errors[mockScalaExecutorPredicate.operatorID].messages.type).toBe(
      "must be integer"
    );
    expect(
      validate(block({ limit: 1, columns: "$K" })).errors[mockScalaExecutorPredicate.operatorID].messages.type
    ).toBe("must be array");
    const outside = graph([body({ limit: "$K" })]);
    expect(validate(outside).errors[mockScalaExecutorPredicate.operatorID].messages.type).toBe("must be integer");
  });
  it("reports missing and malformed metadata as errors rather than accepting the operator", () => {
    const g = graph([source]);
    expect(
      validateWorkflowForRun({ graph: g, metadata: { operators: [], groups: [] } }).errors[source.operatorID].messages
        .schema
    ).toContain("metadata");
    expect(
      validateWorkflowForRun({
        graph: g,
        metadata: mockOperatorMetaData,
        schemaForOperator: () => ({ type: "invalid" }) as never,
      }).errors[source.operatorID].messages.schema
    ).toContain("schema");
  });
  it("rejects conflicting input schemas before run and keeps known-empty different from unknown", () => {
    const second = { ...source, operatorID: "second" };
    const g = graph(
      [source, second, mockResultPredicate],
      [
        mockScanResultLink,
        { linkID: "second", source: { operatorID: "second", portID: "output-0" }, target: mockScanResultLink.target },
      ]
    );
    const result = validateWorkflowForRun({
      graph: g,
      metadata: mockOperatorMetaData,
      outputSchemas: { [source.operatorID]: { "0_false": [] }, second: {} },
    });
    expect(result.errors[mockResultPredicate.operatorID].messages.inputSchema).toContain(
      "Multiple links with different schemas"
    );
  });
});
