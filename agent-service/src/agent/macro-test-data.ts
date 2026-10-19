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

// Three levels with repeated IDs and dependency bundles stored inside legacy definition bodies.
export function macroWorkflow() {
  const port = { portID: "output-0" };
  const leaf = {
    operatorID: "leaf",
    operatorType: "TestOp",
    operatorVersion: "1",
    operatorProperties: { value: "original" },
    inputPorts: [],
    outputPorts: [port],
    showAdvanced: false,
  };
  const instance = (id: string, definition: string, value: string) => ({
    ...leaf,
    operatorID: id,
    operatorType: `WorkflowMacro:${definition}:1`,
    operatorProperties: { value },
  });
  const body = (id: string, operator: typeof leaf) => ({
    operators: [operator],
    links: [],
    operatorPositions: { [operator.operatorID]: { x: 0, y: 0 } },
    commentBoxes: [],
    settings: { dataTransferBatchSize: 10 },
    macroInterface: {
      id,
      inputs: [],
      outputs: [{ ...port, targets: [{ operatorID: operator.operatorID, portID: port.portID }] }],
    },
    formBinding: {
      fields: [{ id: "value", operatorID: operator.operatorID, propertyKey: "value", displayName: "Value" }],
    },
  });
  const inner = { id: "inner", version: 1, name: "Inner", content: body("inner", leaf) };
  const middle = {
    id: "middle",
    version: 1,
    name: "Middle",
    content: { ...body("middle", instance("child", "inner", "middle")), macroDependencies: [inner] },
  };
  const outer = {
    id: "outer",
    version: 1,
    name: "Outer",
    content: { ...body("outer", instance("child", "middle", "outer")), macroDependencies: [middle] },
  };
  return {
    ...body("workflow", leaf),
    macroInterface: undefined,
    operators: [instance("one", "outer", "café 東京"), instance("two", "outer", "other")],
    macroDependencies: [outer],
  };
}
