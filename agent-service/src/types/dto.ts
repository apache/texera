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

// DTOs: request/response bodies exchanged with backend services. Distinct from
// domain types (workflow.ts, execution.ts, agent.ts) which model in-memory
// state, and from ws.ts which carries this service's own WebSocket frames.

import type { WorkflowContent, OperatorPortSchemaMap } from "./workflow";

// --- Dashboard Service: workflow persistence ---

/**
 * Parsed, in-memory workflow returned by the workflow client functions. The
 * backend serializes `content` as a JSON string; the client decodes it into a
 * WorkflowContent before returning, so this is distinct from the raw wire
 * shapes below.
 */
export interface Workflow {
  wid: number;
  name: string;
  description?: string;
  content: WorkflowContent;
  creationTime?: number;
  lastModifiedTime?: number;
}

/**
 * Raw JOOQ Workflow POJO returned by `POST /workflow/persist`. `content` is a
 * JSON string and the published flag is `isPublic` (the column name).
 */
export interface WorkflowPojo {
  wid: number;
  name: string;
  description?: string;
  content: string;
  creationTime?: number;
  lastModifiedTime?: number;
  isPublic?: boolean;
}

/**
 * `GET /workflow/{wid}` response wrapper. `content` is a JSON string and the
 * published flag is renamed to `isPublished`; it also adds `readonly`.
 */
export interface WorkflowWithPrivilege {
  wid: number;
  name: string;
  description?: string;
  content: string;
  creationTime?: number;
  lastModifiedTime?: number;
  isPublished?: boolean;
  readonly?: boolean;
}

export interface WorkflowPersistRequest {
  wid?: number;
  name: string;
  description?: string;
  content: string;
  isPublic?: boolean;
}

// --- Workflow Compiling Service ---

export interface WorkflowFatalError {
  // FatalErrorType enum name, e.g. "COMPILATION_ERROR" | "EXECUTION_FAILURE".
  type: string;
  message: string;
  details?: string;
  operatorId?: string;
  workerId?: string;
  timestamp?: { seconds: number; nanos: number };
}

// `POST /api/compile` returns a Jackson polymorphic type discriminated by
// `type`: a success carries the physical plan, a failure carries per-operator
// errors. Both carry the output schemas computed so far.
export interface WorkflowCompilationSuccess {
  type: "success";
  physicalPlan: unknown;
  operatorOutputSchemas: Record<string, OperatorPortSchemaMap>;
}

export interface WorkflowCompilationFailure {
  type: "failure";
  operatorErrors: Record<string, WorkflowFatalError>;
  operatorOutputSchemas: Record<string, OperatorPortSchemaMap>;
}

export type WorkflowCompilationResponse = WorkflowCompilationSuccess | WorkflowCompilationFailure;

// --- Shared HTTP envelopes ---

/** Error body returned by the agent-service REST routes. */
export interface ErrorResponse {
  error: string;
}
