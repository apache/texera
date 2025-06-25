/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CompilationState, CompilationStateInfo, PortSchema } from "../../workspace/types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../workspace/types/workflow-websocket.interface";

/**
 * Checks if all PortSchemas in an array are equal to each other.
 * Requires either all schemas to be undefined, or all to be defined and equal.
 *
 * @param schemas Array of PortSchemas to compare (can contain undefined values)
 * @returns true if all schemas are equal, false otherwise
 */
export function areAllPortSchemasEqual(schemas: (PortSchema | undefined)[]): boolean {
  if (schemas.length <= 1) {
    return true;
  }
  return schemas.every(schema => schema === schemas[0]);
}

/**
 * Creates a new CompilationStateInfo with a failed state and adds an error for the specified operator.
 *
 * @param currentState The current compilation state info
 * @param operatorId The ID of the operator that caused the error
 * @param errorMessage The error message to display
 * @param errorDetails Additional details about the error
 * @returns A new CompilationStateInfo with the error added
 */
export function addCompilationError(
  currentState: CompilationStateInfo,
  operatorId: string,
  errorMessage: string,
  errorDetails?: string
): CompilationStateInfo {
  const existingOutputSchemas =
    currentState.state === CompilationState.Uninitialized ? {} : currentState.operatorOutputPortSchemaMap;

  const existingErrors = currentState.state === CompilationState.Failed ? currentState.operatorErrors : {};

  const newError: WorkflowFatalError = {
    message: errorMessage,
    details: errorDetails || "",
    operatorId: operatorId,
    workerId: "",
    type: { name: "COMPILATION_ERROR" },
    timestamp: {
      seconds: Math.floor(Date.now() / 1000),
      nanos: (Date.now() % 1000) * 1000000,
    },
  };

  return {
    state: CompilationState.Failed,
    operatorOutputPortSchemaMap: existingOutputSchemas,
    operatorErrors: {
      ...existingErrors,
      [operatorId]: newError,
    },
  };
}
