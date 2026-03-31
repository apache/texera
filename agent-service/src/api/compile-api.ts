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

/**
 * Compilation API client for Texera Agent Service.
 * Provides functions to compile workflow logical plans.
 */

import { from, Observable, EMPTY } from "rxjs";
import { catchError } from "rxjs/operators";
import { getBackendConfig } from "./backend-api";
import type { LogicalPlan, OperatorPortSchemaMap } from "../types/workflow";

// ============================================================================
// Types
// ============================================================================

/**
 * Schema attribute with name and type
 */
export interface SchemaAttribute {
  attributeName: string;
  attributeType: "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary";
}

/**
 * Port schema is an array of schema attributes
 */
export type PortSchema = ReadonlyArray<SchemaAttribute>;

/**
 * Workflow fatal error from compilation
 */
export interface WorkflowFatalError {
  type: string;
  message: string;
  operatorId?: string;
}

/**
 * Response from the workflow compilation API
 */
export interface WorkflowCompilationResponse {
  /** Physical plan (only present if compilation succeeded) */
  physicalPlan?: any;
  /** Output schemas per operator */
  operatorOutputSchemas: Record<string, OperatorPortSchemaMap>;
  /** Errors per operator (only present if compilation failed) */
  operatorErrors: Record<string, WorkflowFatalError>;
}

// ============================================================================
// Compilation API
// ============================================================================

/**
 * Compile a workflow logical plan.
 * Returns an Observable following the RxJS pattern used in the frontend.
 *
 * @param logicalPlan - The logical plan to compile
 * @returns Observable with compilation response
 */
export function compileWorkflow(logicalPlan: LogicalPlan): Observable<WorkflowCompilationResponse> {
  const config = getBackendConfig();
  const url = `${config.compileEndpoint}/api/compile`;

  // Create a simplified request body (matching frontend pattern)
  const body = {
    operators: logicalPlan.operators,
    links: logicalPlan.links,
    opsToReuseResult: [],
    opsToViewResult: [],
  };

  // Use from() to convert Promise to Observable
  return from(
    fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    }).then(async response => {
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Compilation failed: ${response.status} ${response.statusText} - ${errorText}`);
      }
      return response.json() as Promise<WorkflowCompilationResponse>;
    })
  ).pipe(
    catchError((err: unknown) => {
      console.warn("[CompileAPI] Compile workflow API returns error", err);
      return EMPTY;
    })
  );
}

/**
 * Compile a workflow logical plan (Promise-based version).
 * Useful when you need to await the result.
 *
 * @param logicalPlan - The logical plan to compile
 * @returns Promise with compilation response or null on error
 */
export async function compileWorkflowAsync(logicalPlan: LogicalPlan): Promise<WorkflowCompilationResponse | null> {
  const config = getBackendConfig();
  const url = `${config.compileEndpoint}/api/compile`;

  const body = {
    operators: logicalPlan.operators,
    links: logicalPlan.links,
    opsToReuseResult: [],
    opsToViewResult: [],
  };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.warn(`[CompileAPI] Compilation failed: ${response.status} ${response.statusText} - ${errorText}`);
      return null;
    }

    return (await response.json()) as WorkflowCompilationResponse;
  } catch (error) {
    console.warn("[CompileAPI] Compile workflow API error:", error);
    return null;
  }
}
