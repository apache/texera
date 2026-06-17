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

import type { LanguageModel, ModelMessage } from "ai";
import type { WorkflowContent } from "./workflow";

export enum AgentState {
  UNAVAILABLE = "UNAVAILABLE",
  AVAILABLE = "AVAILABLE",
  GENERATING = "GENERATING",
  STOPPING = "STOPPING",
}

export interface TokenUsage {
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  cachedInputTokens?: number;
}

export const INITIAL_STEP_ID = "step-initial";

export interface ReActStep {
  id: string;
  parentId?: string;
  messageId: string;
  stepId: number;
  timestamp: number;
  role: "user" | "agent";
  content: string;
  isBegin: boolean;
  isEnd: boolean;
  toolCalls?: Array<{
    toolName: string;
    toolCallId: string;
    input: any;
  }>;
  toolResults?: Array<{
    toolCallId: string;
    output: any;
    isError?: boolean;
  }>;
  usage?: TokenUsage;
  inputMessages?: any[];
  messageSource?: "chat" | "feedback";
  beforeWorkflowContent?: WorkflowContent;
  afterWorkflowContent?: WorkflowContent;
}

export enum OperatorResultSerializationMode {
  TSV = "tsv",
}

export interface AgentSettings {
  systemPrompt: string;
  disabledTools: Set<string>;
  maxOperatorResultCharLimit: number;
  maxOperatorResultCellCharLimit: number;
  operatorResultSerializationMode: OperatorResultSerializationMode;
  toolTimeoutMs: number;
  executionTimeoutMs: number;
  maxSteps: number;
  allowedOperatorTypes: string[];
}

export const DEFAULT_AGENT_SETTINGS: Omit<AgentSettings, "systemPrompt"> = {
  disabledTools: new Set(),
  maxOperatorResultCharLimit: 2000,
  maxOperatorResultCellCharLimit: 2000,
  operatorResultSerializationMode: OperatorResultSerializationMode.TSV,
  toolTimeoutMs: 240000,
  executionTimeoutMs: 240000,
  maxSteps: 100,
  allowedOperatorTypes: [
    "CSVFileScan",
    "Filter",
    "Projection",
    "TypeCasting",
    "Sort",
    "Limit",
    "Distinct",
    "Union",
    "KeywordSearch",
    "HashJoin",
    "Aggregate",
    "LineChart",
    "BarChart",
    "PieChart",
    "Histogram",
    "Scatterplot",
    "WordCloud",
    "PythonUDFV2",
  ],
};

export interface UserInfo {
  uid: number;
  name: string;
  email: string;
  role: string;
}

/**
 * Runtime delegation binding for an agent: the user it acts on behalf of and
 * the workflow / computing unit it is bound to. `workflowId` is required
 * because delegation is only established once a workflow has been resolved.
 * This is in-memory state, not "configuration".
 */
export interface AgentDelegation {
  userToken: string;
  userInfo?: UserInfo;
  workflowId: number;
  workflowName?: string;
  computingUnitId?: number;
}

/**
 * Wire shape of a delegation at the HTTP boundary: `workflowId` is optional
 * (resolved server-side) and `userToken` is masked in outbound responses.
 */
export interface AgentDelegationDto {
  userToken: string;
  userInfo?: UserInfo;
  workflowId?: number;
  workflowName?: string;
  computingUnitId?: number;
}

/** Tunable agent settings in their wire form (units in seconds/minutes). */
export interface AgentSettingsDto {
  maxOperatorResultCharLimit?: number;
  maxOperatorResultCellCharLimit?: number;
  operatorResultSerializationMode?: "tsv";
  toolTimeoutSeconds?: number;
  executionTimeoutMinutes?: number;
  disabledTools?: string[];
  maxSteps?: number;
  allowedOperatorTypes?: string[];
}

/** Body of `PATCH /agents/:id/settings`; identical to the settings DTO. */
export type UpdateAgentSettingsRequest = AgentSettingsDto;

export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  state: AgentState;
  createdAt: Date;
  delegate?: AgentDelegationDto;
  settings?: AgentSettingsDto;
}

export interface CreateAgentRequest {
  modelType: string;
  name?: string;
  workflowId?: number;
  computingUnitId?: number;
  settings?: AgentSettingsDto;
}

/** Options for constructing a `TexeraAgent`. */
export interface TexeraAgentOptions {
  model: LanguageModel;
  modelType: string;
  agentId: string;
  agentName?: string;
  systemPrompt?: string;
}

/** Outcome of a single `sendMessage` generation run. */
export interface AgentMessageResult {
  response: string;
  messages: ModelMessage[];
  usage: TokenUsage;
  stopped: boolean;
  error?: string;
}

export type ReActStepCallback = (step: ReActStep) => void;

/** Parameters for a single workflow-execution request. */
export interface ExecutionRequestParams {
  userToken: string;
  workflowId: number;
  computingUnitId?: number;
  maxOperatorResultCharLimit?: number;
  maxOperatorResultCellCharLimit?: number;
  executionTimeoutMs?: number;
}
