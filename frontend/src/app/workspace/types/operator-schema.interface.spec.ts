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

import { OperatorSchema, areOperatorSchemasEqual } from "./operator-schema.interface";

function schema(
  overrides: {
    operatorType?: string;
    operatorVersion?: string;
    jsonSchema?: object;
    userFriendlyName?: string;
    operatorGroupName?: string;
    operatorDescription?: string;
    supportReconfiguration?: boolean;
    allowPortCustomization?: boolean;
    inputPorts?: { displayName?: string; disallowMultiLinks?: boolean }[];
    outputPorts?: { displayName?: string }[];
  } = {}
): OperatorSchema {
  return {
    operatorType: overrides.operatorType ?? "ScanSource",
    operatorVersion: overrides.operatorVersion ?? "1.0",
    jsonSchema: (overrides.jsonSchema ?? {
      type: "object",
      properties: { a: { type: "string" }, b: { type: "number" } },
    }) as unknown as OperatorSchema["jsonSchema"],
    additionalMetadata: {
      userFriendlyName: overrides.userFriendlyName ?? "Scan",
      operatorGroupName: overrides.operatorGroupName ?? "Source",
      operatorDescription: overrides.operatorDescription ?? "reads data",
      inputPorts: overrides.inputPorts ?? [{ displayName: "in0", disallowMultiLinks: false }],
      outputPorts: overrides.outputPorts ?? [{ displayName: "out0" }],
      supportReconfiguration: overrides.supportReconfiguration ?? false,
      allowPortCustomization: overrides.allowPortCustomization ?? false,
    },
  };
}

describe("areOperatorSchemasEqual", () => {
  it("returns true for two identically-shaped schemas", () => {
    expect(areOperatorSchemasEqual(schema(), schema())).toBe(true);
  });

  it("returns false when operatorType differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ operatorType: "CSVFileScan" }))).toBe(false);
  });

  it("returns false when operatorVersion differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ operatorVersion: "2.0" }))).toBe(false);
  });

  it("returns false when a nested jsonSchema field differs", () => {
    const other = schema({ jsonSchema: { type: "object", properties: { a: { type: "integer" } } } });
    expect(areOperatorSchemasEqual(schema(), other)).toBe(false);
  });

  it("returns false when jsonSchema arrays differ only in order", () => {
    const a = schema({ jsonSchema: { type: "object", required: ["a", "b"] } });
    const b = schema({ jsonSchema: { type: "object", required: ["b", "a"] } });
    expect(areOperatorSchemasEqual(a, b)).toBe(false);
  });

  it("returns false when additionalMetadata.userFriendlyName differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ userFriendlyName: "Scanner" }))).toBe(false);
  });

  it("returns false when additionalMetadata.operatorGroupName differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ operatorGroupName: "Analysis" }))).toBe(false);
  });

  it("returns false when a boolean metadata flag differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ supportReconfiguration: true }))).toBe(false);
  });

  it("returns false when additionalMetadata.operatorDescription differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ operatorDescription: "different" }))).toBe(false);
  });

  it("returns false when additionalMetadata.allowPortCustomization differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ allowPortCustomization: true }))).toBe(false);
  });

  it("returns false when the number of input ports differs", () => {
    const other = schema({ inputPorts: [{ displayName: "in0" }, { displayName: "in1" }] });
    expect(areOperatorSchemasEqual(schema(), other)).toBe(false);
  });

  it("returns false when an input port's displayName differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ inputPorts: [{ displayName: "renamed" }] }))).toBe(false);
  });

  it("returns false when an input port's disallowMultiLinks differs", () => {
    const other = schema({ inputPorts: [{ displayName: "in0", disallowMultiLinks: true }] });
    expect(areOperatorSchemasEqual(schema(), other)).toBe(false);
  });

  it("returns false when input ports match as a set but differ in order", () => {
    const a = schema({ inputPorts: [{ displayName: "in0" }, { displayName: "in1" }] });
    const b = schema({ inputPorts: [{ displayName: "in1" }, { displayName: "in0" }] });
    expect(areOperatorSchemasEqual(a, b)).toBe(false);
  });

  it("returns false when the number of output ports differs", () => {
    const other = schema({ outputPorts: [{ displayName: "out0" }, { displayName: "out1" }] });
    expect(areOperatorSchemasEqual(schema(), other)).toBe(false);
  });

  it("returns false when an output port's displayName differs", () => {
    expect(areOperatorSchemasEqual(schema(), schema({ outputPorts: [{ displayName: "renamed" }] }))).toBe(false);
  });

  it("returns false when output ports match as a set but differ in order", () => {
    const a = schema({ outputPorts: [{ displayName: "out0" }, { displayName: "out1" }] });
    const b = schema({ outputPorts: [{ displayName: "out1" }, { displayName: "out0" }] });
    expect(areOperatorSchemasEqual(a, b)).toBe(false);
  });
});
