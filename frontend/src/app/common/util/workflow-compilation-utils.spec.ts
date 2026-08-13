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

import { addCompilationError, areAllPortSchemasEqual } from "./workflow-compilation-utils";
import {
  CompilationState,
  CompilationStateInfo,
  OperatorPortSchemaMap,
  PortSchema,
} from "../../workspace/types/workflow-compiling.interface";

type FailedCompilationState = Extract<CompilationStateInfo, { state: CompilationState.Failed }>;

type SucceededCompilationState = Extract<CompilationStateInfo, { state: CompilationState.Succeeded }>;

function requireFailed(state: CompilationStateInfo): FailedCompilationState {
  expect(state.state).toBe(CompilationState.Failed);
  if (state.state !== CompilationState.Failed) {
    throw new Error("Expected a failed compilation state");
  }
  return state;
}

const integerSchema: PortSchema = [
  {
    attributeName: "id",
    attributeType: "integer",
  },
];

const equalIntegerSchema: PortSchema = [
  {
    attributeName: "id",
    attributeType: "integer",
  },
];

const stringSchema: PortSchema = [
  {
    attributeName: "id",
    attributeType: "string",
  },
];

describe("areAllPortSchemasEqual", () => {
  it("should return true for an empty schema array", () => {
    expect(areAllPortSchemasEqual([])).toBe(true);
  });

  it("should return true for a single schema", () => {
    expect(areAllPortSchemasEqual([integerSchema])).toBe(true);
  });

  it("should return true for deeply equal schemas", () => {
    expect(equalIntegerSchema).not.toBe(integerSchema);
    expect(areAllPortSchemasEqual([integerSchema, equalIntegerSchema])).toBe(true);
  });

  it("should return true when every schema is undefined", () => {
    expect(areAllPortSchemasEqual([undefined, undefined])).toBe(true);
  });

  it("should return false when defined schemas differ", () => {
    expect(areAllPortSchemasEqual([integerSchema, stringSchema])).toBe(false);
  });

  it("should return false when defined and undefined schemas are mixed", () => {
    expect(areAllPortSchemasEqual([integerSchema, undefined])).toBe(false);
  });
});

describe("addCompilationError", () => {
  const uninitializedState: CompilationStateInfo = {
    state: CompilationState.Uninitialized,
  };

  it("should create a failed state with the new operator error", () => {
    const result = requireFailed(
      addCompilationError(uninitializedState, "operator-1", "Compilation failed", "Failure details")
    );

    const error = result.operatorErrors["operator-1"];
    expect(error.message).toBe("Compilation failed");
    expect(error.details).toBe("Failure details");
    expect(error.operatorId).toBe("operator-1");
    expect(error.workerId).toBe("");
    expect(error.type).toEqual({ name: "COMPILATION_ERROR" });
  });

  it("should default omitted error details to an empty string", () => {
    const result = requireFailed(addCompilationError(uninitializedState, "operator-1", "Compilation failed"));

    expect(result.operatorErrors["operator-1"].details).toBe("");
  });

  it("should initialize the output-schema map for an uninitialized state", () => {
    const result = requireFailed(addCompilationError(uninitializedState, "operator-1", "Compilation failed"));

    expect(result.operatorOutputPortSchemaMap).toEqual({});
  });

  it("should preserve earlier errors when the input state is already failed", () => {
    const firstResult = requireFailed(addCompilationError(uninitializedState, "operator-1", "First error"));

    const outputSchemaMap: Readonly<Record<string, OperatorPortSchemaMap>> = {
      "operator-1": {
        "output-0": integerSchema,
      },
    };

    const failedState: FailedCompilationState = {
      state: CompilationState.Failed,
      operatorOutputPortSchemaMap: outputSchemaMap,
      operatorErrors: firstResult.operatorErrors,
    };

    const result = requireFailed(addCompilationError(failedState, "operator-2", "Second error"));

    expect(Object.keys(result.operatorErrors).sort()).toEqual(["operator-1", "operator-2"]);
    expect(result.operatorErrors["operator-1"]).toEqual(firstResult.operatorErrors["operator-1"]);
    expect(result.operatorOutputPortSchemaMap).toEqual(outputSchemaMap);
  });

  it("should carry over the output-schema map from a succeeded state", () => {
    const outputSchemaMap: Readonly<Record<string, OperatorPortSchemaMap>> = {
      "operator-1": {
        "output-0": integerSchema,
      },
    };

    const succeededState: SucceededCompilationState = {
      state: CompilationState.Succeeded,
      physicalPlan: {} as SucceededCompilationState["physicalPlan"],
      operatorOutputPortSchemaMap: outputSchemaMap,
    };

    const result = requireFailed(addCompilationError(succeededState, "operator-2", "Compilation failed"));

    expect(result.operatorOutputPortSchemaMap).toEqual(outputSchemaMap);
    expect(Object.keys(result.operatorErrors)).toEqual(["operator-2"]);
  });
});
