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

// Wire DTOs: request/response bodies exchanged with backend services and the
// WebSocket frames this service sends to its own clients. Distinct from domain
// types (workflow.ts, execution.ts, agent.ts) which model in-memory state.

import type { WorkflowContent, OperatorPortSchemaMap } from "./workflow";
import type { ReActStep } from "./agent";

// --- Dashboard Service: workflow persistence ---

export interface Workflow {
  wid: number;
  name: string;
  description?: string;
  content: WorkflowContent;
  creationTime?: number;
  lastModifiedTime?: number;
  isPublished?: boolean;
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
  message: string;
  details: string;
  operatorId: string;
  workerId: string;
  type: { name: string };
  timestamp: { nanos: number; seconds: number };
}

export interface WorkflowCompilationResponse {
  physicalPlan?: any;
  operatorOutputSchemas: Record<string, OperatorPortSchemaMap>;
  operatorErrors: Record<string, WorkflowFatalError>;
}

// --- This service's WebSocket protocol (/agents/:id/react) ---

export interface WsMessage {
  type: "message" | "stop";
  content?: string;
  messageSource?: "chat" | "feedback";
}

export interface OperatorResultSummaryWs {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: { portIndex: number; rows: number; columns: number }[];
  outputColumns?: number;
  error?: string;
  warnings?: string[];
  consoleLogCount?: number;
  totalRowCount?: number;
  sampleRecords?: Record<string, any>[];
  resultStatistics?: Record<string, string>;
}

export interface WsOutgoingMessage {
  type: "step" | "state" | "error" | "complete" | "init" | "headChange";
  step?: ReActStep;
  state?: string;
  error?: string;
  steps?: ReActStep[];
  headId?: string;
  operatorResults?: Record<string, OperatorResultSummaryWs>;
  workflowContent?: any;
}
