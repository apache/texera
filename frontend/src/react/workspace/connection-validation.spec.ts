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

import { describe, expect, it } from "vitest";
import { canConnect } from "./connection-validation";
import type { WorkflowGraphReadonly } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
const op = (id: string) => ({
  operatorID: id,
  operatorType: "Test",
  operatorVersion: "1",
  operatorProperties: {},
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
});
const operators = [op("a"), op("b"), op("c")];
function graph(links = []) {
  return {
    hasOperator: (id: string) => operators.some(op => op.operatorID === id),
    getOperator: (id: string) => operators.find(op => op.operatorID === id)!,
    getAllLinks: () => links,
  } as unknown as WorkflowGraphReadonly;
}
const metadata = {
  operators: [
    {
      operatorType: "Test",
      jsonSchema: {},
      operatorVersion: "1",
      additionalMetadata: {
        userFriendlyName: "Test",
        operatorGroupName: "Test",
        inputPorts: [{ disallowMultiLinks: true }],
        outputPorts: [{}],
      },
    },
  ],
  groups: [],
} satisfies OperatorMetadata;
describe("canvas port connection rules", () => {
  it("accepts output to input and rejects missing ports/operators, reversed ports and self links", () => {
    expect(canConnect(graph(), metadata, "a", "output-0", "b", "input-0")).toBe(true);
    for (const [source, sp, target, tp] of [
      ["a", null, "b", "input-0"],
      ["a", "input-0", "b", "input-0"],
      ["a", "output-0", "b", "output-0"],
      ["missing", "output-0", "b", "input-0"],
      ["a", "output-0", "a", "input-0"],
    ])
      expect(canConnect(graph(), metadata, source!, sp, target!, tp)).toBe(false);
  });
  it("rejects duplicate links and occupied single-link inputs, while allowing multi-link metadata", () => {
    const links = [
      { linkID: "ab", source: { operatorID: "a", portID: "output-0" }, target: { operatorID: "b", portID: "input-0" } },
    ];
    expect(canConnect(graph(links), metadata, "a", "output-0", "b", "input-0")).toBe(false);
    expect(canConnect(graph(links), metadata, "c", "output-0", "b", "input-0")).toBe(false);
    const multi = {
      ...metadata,
      operators: [
        {
          ...metadata.operators[0],
          additionalMetadata: {
            ...metadata.operators[0].additionalMetadata,
            inputPorts: [{ disallowMultiLinks: false }],
          },
        },
      ],
    };
    expect(canConnect(graph(links), multi, "c", "output-0", "b", "input-0")).toBe(true);
    expect(canConnect(graph(links), metadata, "a", "output-0", "b", "input-0", "ab")).toBe(true);
  });
});
