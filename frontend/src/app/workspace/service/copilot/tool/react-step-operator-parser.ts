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
 * Central parser module to extract operator access information from tool results.
 * Tools should populate viewedOperatorIds and modifiedOperatorIds in their results.
 */

/**
 * Operator access information indicating which operators were viewed or modified.
 * Tools should populate these fields in their results to indicate operator interaction.
 */
export interface ToolOperatorAccess {
  viewedOperatorIds: string[];
  modifiedOperatorIds: string[];
}

/**
 * Parse operator access from a tool call's result.
 * Tools should populate viewedOperatorIds and modifiedOperatorIds in their results.
 *
 * @param toolCall - The tool call object containing toolName and args
 * @param toolResult - The tool result object containing the output/result with operator IDs
 * @returns ToolOperatorAccess object with viewedOperatorIds and modifiedOperatorIds
 */
export function parseOperatorAccessFromToolCall(toolCall: any, toolResult?: any): ToolOperatorAccess {
  const access: ToolOperatorAccess = { viewedOperatorIds: [], modifiedOperatorIds: [] };

  if (!toolResult || !toolResult.output) {
    return access;
  }

  try {
    const output = toolResult.output;

    // Extract viewedOperatorIds from tool result
    if (Array.isArray(output.viewedOperatorIds)) {
      access.viewedOperatorIds = output.viewedOperatorIds.filter((id: any) => id && typeof id === "string");
    }

    // Extract modifiedOperatorIds from tool result
    if (Array.isArray(output.modifiedOperatorIds)) {
      access.modifiedOperatorIds = output.modifiedOperatorIds.filter((id: any) => id && typeof id === "string");
    }

    // Remove duplicates
    access.viewedOperatorIds = [...new Set(access.viewedOperatorIds)];
    access.modifiedOperatorIds = [...new Set(access.modifiedOperatorIds)];
  } catch (error) {
    console.error("Error parsing operator access from tool result:", error);
  }

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

    // Only add to map if there are any viewed or modified operations
    if (access.viewedOperatorIds.length > 0 || access.modifiedOperatorIds.length > 0) {
      accessMap.set(i, access);
    }
  }

  return accessMap;
}

/**
 * Extract all viewed operator IDs from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs that were viewed
 */
export function getAllViewedOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  if (!step.operatorAccess) {
    return [];
  }

  const allViewedIds: string[] = [];
  for (const access of step.operatorAccess.values()) {
    allViewedIds.push(...access.viewedOperatorIds);
  }

  // Return unique operator IDs
  return [...new Set(allViewedIds)];
}

/**
 * Extract all modified operator IDs from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs that were modified
 */
export function getAllModifiedOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  if (!step.operatorAccess) {
    return [];
  }

  const allModifiedIds: string[] = [];
  for (const access of step.operatorAccess.values()) {
    allModifiedIds.push(...access.modifiedOperatorIds);
  }

  // Return unique operator IDs
  return [...new Set(allModifiedIds)];
}

/**
 * Extract all operator IDs (both viewed and modified) from a ReActStep.
 *
 * @param step - The ReActStep to extract from
 * @returns Array of unique operator IDs involved in this step
 */
export function getAllOperatorIds(step: { operatorAccess?: Map<number, ToolOperatorAccess> }): string[] {
  const viewedIds = getAllViewedOperatorIds(step);
  const modifiedIds = getAllModifiedOperatorIds(step);

  // Combine and return unique IDs
  return [...new Set([...viewedIds, ...modifiedIds])];
}
