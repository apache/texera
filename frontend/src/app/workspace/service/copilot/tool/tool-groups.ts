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
 * Groups: Observe, Execute, Modify, Record
 */

// Import tool name constants from each tool file
import * as workflowMetadataTools from "./workflow-metadata-tools";
import * as baselineTools from "./baseline-tools";
import * as currentWorkflowEditingObservingTools from "./current-workflow-editing-observing-tools";
import * as currentWorkflowExecutionTools from "./current-workflow-execution-tools";
import * as actionPlanTools from "./action-plan-tools";
import * as operatorTools from "./operator-tools";

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
    color: "#fa8c16", // Orange - for modification operations (action plans)
    icon: "edit",
    description: "Tools that modify workflow structure",
  },
};

// Mapping of tool names to their groups
export const TOOL_NAME_TO_GROUP: Record<string, ToolGroup> = {
  // Observe group - metadata tools
  [workflowMetadataTools.TOOL_NAME_LIST_ALL_OPERATOR_TYPES_AND_SCHEMAS]: ToolGroup.OBSERVE,
  [workflowMetadataTools.TOOL_NAME_GET_OPERATOR_PORTS_INFO]: ToolGroup.OBSERVE,
  [workflowMetadataTools.TOOL_NAME_GET_OPERATOR_METADATA]: ToolGroup.OBSERVE,

  // Baseline mode tools
  [baselineTools.TOOL_NAME_CREATE_PYTHON_UDF]: ToolGroup.MODIFY,
  [baselineTools.TOOL_NAME_EXECUTE_TO_OPERATOR]: ToolGroup.EXECUTE,

  // Observe group - workflow inspection tools
  [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_WORKFLOW]: ToolGroup.OBSERVE,
  [currentWorkflowEditingObservingTools.TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS]: ToolGroup.OBSERVE,
  [currentWorkflowEditingObservingTools.TOOL_NAME_LIST_CURRENT_LINKS]: ToolGroup.OBSERVE,
  [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_OPERATOR]: ToolGroup.OBSERVE,
  [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_WORKFLOW_COMPILATION_STATE]: ToolGroup.OBSERVE,
  [currentWorkflowEditingObservingTools.TOOL_NAME_LIST_OPERATORS_IN_CURRENT_WORKFLOW]: ToolGroup.OBSERVE,

  // Execute group - workflow execution tools
  [currentWorkflowExecutionTools.TOOL_NAME_EXECUTE_CURRENT_WORKFLOW]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_GET_EXISTING_WORKFLOW_EXECUTION_RESULT]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_EXECUTION_STATE]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_KILL_CURRENT_WORKFLOW]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO]: ToolGroup.EXECUTE,
  [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS]: ToolGroup.EXECUTE,

  // Modify group - action plan tools (workflow modifications)
  [actionPlanTools.TOOL_NAME_ADD_TO_WORKFLOW]: ToolGroup.MODIFY,
  [actionPlanTools.TOOL_NAME_MODIFY_IN_WORKFLOW]: ToolGroup.MODIFY,
  [actionPlanTools.TOOL_NAME_GET_ACTION_PLAN]: ToolGroup.MODIFY,
  [actionPlanTools.TOOL_NAME_LIST_ACTION_PLANS]: ToolGroup.MODIFY,
  [actionPlanTools.TOOL_NAME_DELETE_ACTION_PLAN]: ToolGroup.MODIFY,
  [actionPlanTools.TOOL_NAME_UPDATE_ACTION_PLAN]: ToolGroup.MODIFY,

  // Modify group - operator-specific tools
  [operatorTools.TOOL_NAME_ADD_PYTHON_UDF_V2]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_AGGREGATE]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_PROJECTION]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_HASH_JOIN]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_SORT]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_UNION]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_INTERSECT]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_CARTESIAN_PRODUCT]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_CSV_FILE_SCAN]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_ADD_LINK]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_MODIFY_OPERATOR]: ToolGroup.MODIFY,
  [operatorTools.TOOL_NAME_DELETE_FROM_WORKFLOW]: ToolGroup.MODIFY,

  // Modify group - direct workflow editing tools (currently commented out in copilot)
  [currentWorkflowEditingObservingTools.TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
  [currentWorkflowEditingObservingTools.TOOL_NAME_ADD_LINK_TO_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
  [currentWorkflowEditingObservingTools.TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
  [currentWorkflowEditingObservingTools.TOOL_NAME_DELETE_LINK_IN_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
  [currentWorkflowEditingObservingTools.TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
  [currentWorkflowEditingObservingTools.TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW]: ToolGroup.MODIFY,
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
