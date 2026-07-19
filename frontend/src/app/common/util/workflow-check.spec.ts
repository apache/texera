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

import { checkIfWorkflowBroken } from "./workflow-check";

type WorkflowInput = Parameters<typeof checkIfWorkflowBroken>[0];

function createWorkflow(
  operatorIDs: ReadonlyArray<string>,
  links: ReadonlyArray<{
    sourceOperatorID: string;
    targetOperatorID: string;
  }>
): WorkflowInput {
  return {
    content: {
      operators: operatorIDs.map(operatorID => ({ operatorID })),
      links: links.map(({ sourceOperatorID, targetOperatorID }) => ({
        source: { operatorID: sourceOperatorID },
        target: { operatorID: targetOperatorID },
      })),
    },
  } as unknown as WorkflowInput;
}

describe("checkIfWorkflowBroken", () => {
  it("should return false when every link references existing operators", () => {
    const workflow = createWorkflow(
      ["operator-1", "operator-2"],
      [{ sourceOperatorID: "operator-1", targetOperatorID: "operator-2" }]
    );

    expect(checkIfWorkflowBroken(workflow)).toBe(false);
  });

  it("should return true when a link source references a missing operator", () => {
    const workflow = createWorkflow(
      ["operator-2"],
      [{ sourceOperatorID: "missing-operator", targetOperatorID: "operator-2" }]
    );

    expect(checkIfWorkflowBroken(workflow)).toBe(true);
  });

  it("should return true when a link target references a missing operator", () => {
    const workflow = createWorkflow(
      ["operator-1"],
      [{ sourceOperatorID: "operator-1", targetOperatorID: "missing-operator" }]
    );

    expect(checkIfWorkflowBroken(workflow)).toBe(true);
  });

  it("should return false when the workflow has no links", () => {
    const workflow = createWorkflow(["operator-1"], []);

    expect(checkIfWorkflowBroken(workflow)).toBe(false);
  });
});
