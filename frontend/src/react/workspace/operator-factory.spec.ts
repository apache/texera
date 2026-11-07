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
import { createOperator, createOperatorFactory } from "./operator-factory";
import type { OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
const schema: OperatorSchema = {
  operatorType: "PythonUDFV2",
  operatorVersion: "1.2",
  jsonSchema: {
    type: "object",
    properties: {
      workers: { type: "integer", default: 1 },
      config: { type: "object", default: { labels: ["日本語"] } },
    },
  },
  additionalMetadata: {
    userFriendlyName: "Python UDF",
    operatorGroupName: "UDF",
    inputPorts: [{ displayName: "Table", disallowMultiLinks: true, dependencies: [{ id: 2, internal: true }] }],
    outputPorts: [{}],
    dynamicInputPorts: true,
    dynamicOutputPorts: true,
  },
};
describe("schema-backed React operator factory", () => {
  it("preserves live type/version, default properties and both port contracts", () => {
    const operator = createOperator(schema, { uuid: () => "id" });
    expect(operator).toEqual({
      operatorID: "PythonUDFV2-operator-id",
      operatorType: "PythonUDFV2",
      operatorVersion: "1.2",
      operatorProperties: { workers: 1, config: { labels: ["日本語"] } },
      inputPorts: [
        {
          portID: "input-0",
          displayName: "Table",
          disallowMultiInputs: true,
          isDynamicPort: false,
          dependencies: [{ id: 2, internal: true }],
        },
      ],
      outputPorts: [{ portID: "output-0", displayName: "", disallowMultiInputs: false, isDynamicPort: false }],
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: "Python UDF",
      dynamicInputPorts: true,
      dynamicOutputPorts: true,
    });
  });
  it("deep-clones defaults and accepts an explicit empty display name without mutating metadata", () => {
    const first = createOperator(schema, { customDisplayName: "" });
    const second = createOperator(schema);
    (first.operatorProperties.config.labels as string[]).push("edited");
    expect(second.operatorProperties.config.labels).toEqual(["日本語"]);
    expect(first.customDisplayName).toBe("");
    expect(schema.jsonSchema.properties?.config).toMatchObject({ default: { labels: ["日本語"] } });
  });
  it("uses the same factory interface as migration LLM and refuses missing operator schemas", () => {
    const factory = createOperatorFactory({ operators: [schema], groups: [] }, { uuid: () => "test" });
    expect(factory.getNewOperatorPredicate("PythonUDFV2", "Step").customDisplayName).toBe("Step");
    expect(() => factory.getNewOperatorPredicate("Unknown")).toThrow("doesn't exist in operator metadata");
    expect(() => createOperatorFactory([]).getNewOperatorPredicate("PythonUDFV2")).toThrow();
  });
  it("keeps empty/dynamic ports empty and exposes malformed schema failures specifically", () => {
    const empty = createOperator({
      ...schema,
      jsonSchema: { type: "object" },
      additionalMetadata: {
        ...schema.additionalMetadata,
        inputPorts: [],
        outputPorts: [],
        dynamicInputPorts: undefined,
        dynamicOutputPorts: undefined,
      },
    });
    expect(empty.inputPorts).toEqual([]);
    expect(empty.outputPorts).toEqual([]);
    expect(empty.dynamicInputPorts).toBe(false);
    expect(() => createOperator({ ...schema, jsonSchema: { type: "invalid" } as never })).toThrow();
  });
});
