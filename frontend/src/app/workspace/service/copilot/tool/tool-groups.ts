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
 * Tool groups for categorizing copilot tools in the timeline visualization.
 * Groups: Observe, Execute, Modify
 *
 * These tool names must match the tool names defined in agent-service.
 */

export enum ToolGroup {
  OBSERVE = "Observe",
  EXECUTE = "Execute",
  MODIFY = "Modify",
}

export interface ToolGroupConfig {
  group: ToolGroup;
  color: string;
  icon: string;
  description: string;
}

// Color scheme inspired by Git visualization
export const TOOL_GROUP_CONFIGS: Record<ToolGroup, ToolGroupConfig> = {
  [ToolGroup.OBSERVE]: {
    group: ToolGroup.OBSERVE,
    color: "#52c41a", // Green - for read/observe operations
    icon: "eye",
    description: "Tools that observe and inspect workflow state",
  },
  [ToolGroup.EXECUTE]: {
    group: ToolGroup.EXECUTE,
    color: "#1890ff", // Blue - for execution operations
    icon: "play-circle",
    description: "Tools that execute workflows and retrieve results",
  },
  [ToolGroup.MODIFY]: {
    group: ToolGroup.MODIFY,
    color: "#fa8c16", // Orange - for modification operations (agent actions)
    icon: "edit",
    description: "Tools that modify workflow structure",
  },
};

// Tool name constants (must match agent-service tool names)
const TOOL_NAME_ADD_OPERATOR = "addOperator";
const TOOL_NAME_MODIFY_OPERATOR = "modifyOperator";
const TOOL_NAME_DELETE_OPERATOR = "deleteOperator";
const TOOL_NAME_CREATE_OR_MODIFY_OPERATOR = "createOrModifyOperator";
const TOOL_NAME_EXECUTE_OPERATOR = "executeOperator";

// Mapping of tool names to their groups
export const TOOL_NAME_TO_GROUP: Record<string, ToolGroup> = {
  // Execute group
  [TOOL_NAME_EXECUTE_OPERATOR]: ToolGroup.EXECUTE,

  // Modify group - tools that create agent actions
  [TOOL_NAME_ADD_OPERATOR]: ToolGroup.MODIFY,
  [TOOL_NAME_MODIFY_OPERATOR]: ToolGroup.MODIFY,
  [TOOL_NAME_DELETE_OPERATOR]: ToolGroup.MODIFY,
  [TOOL_NAME_CREATE_OR_MODIFY_OPERATOR]: ToolGroup.MODIFY,
};

/**
 * Get the group for a given tool name.
 * Returns OBSERVE as default if tool is not found.
 */
export function getToolGroup(toolName: string): ToolGroup {
  return TOOL_NAME_TO_GROUP[toolName] || ToolGroup.OBSERVE;
}

/**
 * Get the configuration for a tool group.
 */
export function getToolGroupConfig(group: ToolGroup): ToolGroupConfig {
  return TOOL_GROUP_CONFIGS[group];
}

/**
 * Get the color for a given tool name.
 */
export function getToolColor(toolName: string): string {
  const group = getToolGroup(toolName);
  return TOOL_GROUP_CONFIGS[group].color;
}
