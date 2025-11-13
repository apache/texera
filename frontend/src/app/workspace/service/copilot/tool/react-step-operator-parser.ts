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

import {
  TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS,
  TOOL_NAME_GET_CURRENT_OPERATOR,
  TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW,
  TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW,
  TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW,
  TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW,
} from "./current-workflow-editing-observing-tools";
import {
  TOOL_NAME_GET_CURRENT_OPERATOR_RESULT,
  TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO,
  TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT,
} from "./current-workflow-execution-tools";
import { TOOL_NAME_VALIDATE_CURRENT_OPERATOR } from "./current-workflow-validation-tools";

/**
 * Central parser module to extract relevant operator IDs from tool calls and results.
 * This module provides a unified way to parse operator IDs based on tool types.
 */

/**
 * Operator access information indicating which operators are read from or written to.
 */
export interface ToolOperatorAccess {
  READ: string[];
  WRITE: string[];
}

/**
 * Parse operator access (READ/WRITE) from a tool call and its result.
 * Different tools store operator IDs in different locations (input args vs. output result).
 *
 * @param toolCall - The tool call object containing toolName and args
 * @param toolResult - The tool result object containing the output/result
 * @returns ToolOperatorAccess object with READ and WRITE operator IDs
 */
export function parseOperatorAccessFromToolCall(toolCall: any, toolResult?: any): ToolOperatorAccess {
  const access: ToolOperatorAccess = { READ: [], WRITE: [] };
  if (!toolCall || !toolCall.toolName) {
    return access;
  }

  const toolName = toolCall.toolName;

  try {
    // Parse based on tool type and classify as READ or WRITE
    switch (toolName) {
      // Tools that READ operators from the result (context switching)
      case TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS:
        if (toolResult && toolResult.output) {
          if (toolResult.output.success && Array.isArray(toolResult.output.operatorIds)) {
            access.READ.push(...toolResult.output.operatorIds);
          }
        }
        break;

      // Tools that READ operator information
      case TOOL_NAME_GET_CURRENT_OPERATOR:
      case TOOL_NAME_VALIDATE_CURRENT_OPERATOR:
      case TOOL_NAME_GET_CURRENT_OPERATOR_RESULT:
      case TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO:
      case TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT:
        if (toolCall.args && toolCall.args.operatorId) {
          access.READ.push(toolCall.args.operatorId);
        }
        break;

      // Tools that WRITE to operators (modify properties)
      case TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW:
      case TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW:
        if (toolCall.args && toolCall.args.operatorId) {
          access.WRITE.push(toolCall.args.operatorId);
        }
        break;

      // Tools that WRITE (delete operator)
      case TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW:
        if (toolCall.args && toolCall.args.operatorId) {
          access.WRITE.push(toolCall.args.operatorId);
        }
        break;

      // Tools that WRITE (create new operator)
      case TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW:
        if (toolResult && toolResult.output) {
          if (toolResult.output.success && toolResult.output.operatorId) {
            access.WRITE.push(toolResult.output.operatorId);
          }
        }
        break;

      // Add more tool parsers here as needed
      default:
        // For unknown tools, try to extract operatorId from args and classify as READ
        if (toolCall.args && toolCall.args.operatorId) {
          access.READ.push(toolCall.args.operatorId);
        }
        break;
    }
  } catch (error) {
    console.error(`Error parsing operator access from tool ${toolName}:`, error);
  }

  // Remove duplicates and filter out invalid IDs
  access.READ = [...new Set(access.READ.filter(id => id && typeof id === "string"))];
  access.WRITE = [...new Set(access.WRITE.filter(id => id && typeof id === "string"))];

  return access;
}

/**
 * Parse operator access for all tool calls in a step.
 *
 * @param toolCalls - Array of tool call objects
 * @param toolResults - Array of corresponding tool result objects
 * @returns Map from tool call index to ToolOperatorAccess
 */
export function parseOperatorAccessFromStep(toolCalls: any[], toolResults?: any[]): Map<number, ToolOperatorAccess> {
  const accessMap = new Map<number, ToolOperatorAccess>();

  if (!toolCalls || toolCalls.length === 0) {
    return accessMap;
  }

  for (let i = 0; i < toolCalls.length; i++) {
    const toolCall = toolCalls[i];
    const toolResult = toolResults && toolResults[i] ? toolResults[i] : undefined;
    const access = parseOperatorAccessFromToolCall(toolCall, toolResult);

    // Only add to map if there are any READ or WRITE operations
    if (access.READ.length > 0 || access.WRITE.length > 0) {
      accessMap.set(i, access);
    }
  }

  return accessMap;
}

/**
 * Extract all READ operator IDs from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs that were read
 */
export function getAllReadOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  if (!step.operatorAccess) {
    return [];
  }

  const allReadIds: string[] = [];
  for (const access of step.operatorAccess.values()) {
    allReadIds.push(...access.READ);
  }

  // Return unique operator IDs
  return [...new Set(allReadIds)];
}

/**
 * Extract all WRITE operator IDs from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs that were written
 */
export function getAllWriteOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  if (!step.operatorAccess) {
    return [];
  }

  const allWriteIds: string[] = [];
  for (const access of step.operatorAccess.values()) {
    allWriteIds.push(...access.WRITE);
  }

  // Return unique operator IDs
  return [...new Set(allWriteIds)];
}

/**
 * Extract all operator IDs (both READ and WRITE) from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs involved in this step
 */
export function getAllOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  const readIds = getAllReadOperatorIds(step);
  const writeIds = getAllWriteOperatorIds(step);

  // Combine and return unique IDs
  return [...new Set([...readIds, ...writeIds])];
}
