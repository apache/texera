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

import type { ModelMessage } from "ai";

// Re-export ModelMessage for use in other modules
export type { ModelMessage };

/**
 * Operator access information for a tool call.
 * Tracks which operators were viewed, added, or modified.
 */
export interface ToolOperatorAccess {
  viewedOperatorIds: string[];
  addedOperatorIds: string[];
  modifiedOperatorIds: string[];
}

/**
 * Agent lifecycle state.
 */
export enum AgentState {
  UNAVAILABLE = "Unavailable",
  AVAILABLE = "Available",
  GENERATING = "Generating",
  STOPPING = "Stopping",
}

/**
 * ReActStep - Represents a single reasoning and acting step in the agent's response.
 * Each step contains the agent's reasoning text, tool calls, results, and metadata.
 */
export interface ReActStep {
  messageId: string;
  stepId: number;
  timestamp: Date;
  role: "user" | "agent";
  content: string;
  isBegin: boolean;
  isEnd: boolean;
  toolCalls?: any[];
  toolResults?: any[];
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    cachedInputTokens?: number;
  };
  /** Messages array sent to the LLM for this step (only when context optimization is active) */
  inputMessages?: any[];
  // Map from tool call index to operator access information
  operatorAccess?: Map<number, ToolOperatorAccess>;

  // Versioning fields:
  /** Unique step ID string for tree references */
  id: string;
  /** Parent step ID — forms the version tree */
  parentId?: string;
  /** Source of the user message: "chat" or "feedback" */
  messageSource?: string;
  /** Workflow state before this step executed */
  beforeWorkflowContent?: any;
  /** Workflow state after this step executed */
  afterWorkflowContent?: any;
}
