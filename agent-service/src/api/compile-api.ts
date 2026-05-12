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

import { getBackendConfig } from "./backend-api";
import type { LogicalPlan, OperatorPortSchemaMap } from "../types/workflow";
import { createLogger } from "../logger";

const log = createLogger("CompileAPI");

export interface SchemaAttribute {
  attributeName: string;
  attributeType: "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary";
}

export type PortSchema = ReadonlyArray<SchemaAttribute>;

export interface WorkflowFatalError {
  type: string;
  message: string;
  operatorId?: string;
}

export interface WorkflowCompilationResponse {
  physicalPlan?: any;
  operatorOutputSchemas: Record<string, OperatorPortSchemaMap>;
  operatorErrors: Record<string, WorkflowFatalError>;
}

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
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text();
      log.warn({ status: response.status, statusText: response.statusText, body: errorText }, "compilation failed");
      return null;
    }

    return (await response.json()) as WorkflowCompilationResponse;
  } catch (error) {
    log.warn({ err: error }, "compile workflow API error");
    return null;
  }
}
