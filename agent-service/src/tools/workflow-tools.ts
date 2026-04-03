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
 * Common workflow tools for Texera Agent Service.
 * These tools are shared across both CODE and GENERAL agent modes.
 */

import { z } from "zod";
import { tool } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import {
  createToolResult,
  createErrorResult,
} from "./tools-utility";
import type { OperatorMetadataStore } from "./metadata-tools";
import type { ParallelCallCoordinator } from "./parallel-call-coordinator";

// ============================================================================
// Types for tool context
// ============================================================================

export interface ToolContext {
  metadataStore?: OperatorMetadataStore;
  settings?: {
    maxOperatorResultCharLimit?: number;
    toolTimeoutMs?: number;
    executionTimeoutMs?: number;
  };
  /** Coordinates parallel tool calls with inter-operator dependencies */
  parallelCoordinator?: ParallelCallCoordinator;
}

// ============================================================================
// Tool Name Constants
// ============================================================================

export const TOOL_NAME_DELETE_OPERATOR = "deleteOperator";

// ============================================================================
// Delete Operator Tool
// ============================================================================

export function createDeleteOperatorTool(workflowState: WorkflowState, context?: ToolContext) {
  return tool({
    description: "Delete an operator from the workflow. This also deletes all connected links.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const deleted = workflowState.deleteOperator(args.operatorId);
        if (!deleted) {
          return createErrorResult(`Operator ${args.operatorId} not found`);
        }
        return createToolResult(`Deleted operator: ${args.operatorId}`);
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}
