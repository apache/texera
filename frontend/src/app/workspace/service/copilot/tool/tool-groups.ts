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

export enum ToolGroup {
  OBSERVE = "Observe",
  EXECUTE = "Execute",
  MODIFY = "Modify",
  RECORD = "Record",
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
  [ToolGroup.RECORD]: {
    group: ToolGroup.RECORD,
    color: "#722ed1", // Purple - for data check recording operations
    icon: "file-text",
    description: "Tools that record and manage data checks",
  },
};

// Mapping of tool names to their groups
export const TOOL_NAME_TO_GROUP: Record<string, ToolGroup> = {
  // Observe group - metadata tools
  listAllOperatorTypes: ToolGroup.OBSERVE,
  getOperatorPropertiesSchema: ToolGroup.OBSERVE,
  getOperatorPortsInfo: ToolGroup.OBSERVE,
  getOperatorMetadata: ToolGroup.OBSERVE,

  // Baseline mode tools
  createPythonUDF: ToolGroup.MODIFY,

  // Observe group - workflow inspection tools
  listCurrentRelevantOperatorIds: ToolGroup.OBSERVE,
  listCurrentLinks: ToolGroup.OBSERVE,
  getCurrentOperator: ToolGroup.OBSERVE,
  getCurrentWorkflowCompilationState: ToolGroup.OBSERVE,
  listOperatorsInCurrentWorkflow: ToolGroup.OBSERVE,

  // Observe group - validation tools
  getCurrentWorkflowValidationInfo: ToolGroup.OBSERVE,
  validateCurrentOperator: ToolGroup.OBSERVE,

  // Execute group - workflow execution tools
  executeCurrentWorkflow: ToolGroup.EXECUTE,
  getCurrentExecutionState: ToolGroup.EXECUTE,
  killCurrentWorkflow: ToolGroup.EXECUTE,
  hasCurrentOperatorResult: ToolGroup.EXECUTE,
  getCurrentOperatorResult: ToolGroup.EXECUTE,
  getCurrentOperatorResultInfo: ToolGroup.EXECUTE,
  getCurrentComputingUnitStatus: ToolGroup.EXECUTE,

  // Modify group - action plan tools (workflow modifications)
  addToWorkflow: ToolGroup.MODIFY,
  modifyInWorkflow: ToolGroup.MODIFY,
  deleteFromWorkflow: ToolGroup.MODIFY,
  getActionPlan: ToolGroup.MODIFY,
  listActionPlans: ToolGroup.MODIFY,
  deleteActionPlan: ToolGroup.MODIFY,
  updateActionPlan: ToolGroup.MODIFY,

  // Modify group - direct workflow editing tools (currently commented out in copilot)
  addOperatorToCurrentWorkflow: ToolGroup.MODIFY,
  addLinkToCurrentWorkflow: ToolGroup.MODIFY,
  deleteOperatorInCurrentWorkflow: ToolGroup.MODIFY,
  deleteLinkInCurrentWorkflow: ToolGroup.MODIFY,
  setOperatorPropertyInCurrentWorkflow: ToolGroup.MODIFY,
  setPortPropertyInCurrentWorkflow: ToolGroup.MODIFY,

  // Record group - data check tools
  addDataCheck: ToolGroup.RECORD,
  listDataChecks: ToolGroup.RECORD,
  updateDataCheck: ToolGroup.RECORD,
  deleteDataCheck: ToolGroup.RECORD,
  clearDataChecks: ToolGroup.RECORD,
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
