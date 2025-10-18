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

import { z } from "zod";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorLink } from "../../types/workflow-common.interface";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";

// Tool interface compatible with AI SDK
interface AITool {
  description: string;
  parameters: z.ZodTypeAny;
  execute: (args: any) => Promise<any>;
}

/**
 * Create workflow manipulation tools for Vercel AI SDK
 */
export function createWorkflowTools(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService
): Record<string, AITool> {

  // Tool: Add Operator
  const addOperator: AITool = {
    description: "Add a new operator to the workflow",
    parameters: z.object({
      operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(args.operatorType)) {
          return {
            success: false,
            error: `Unknown operator type: ${args.operatorType}. Use listOperatorTypes tool to see available types.`,
          };
        }

        // Get a new operator predicate with default settings
        const operator = workflowUtilService.getNewOperatorPredicate(args.operatorType);

        // Calculate a default position (can be adjusted by auto-layout later)
        const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const defaultX = 100 + (existingOperators.length % 5) * 200;
        const defaultY = 100 + Math.floor(existingOperators.length / 5) * 150;
        const position = { x: defaultX, y: defaultY };

        // Add the operator to the workflow
        workflowActionService.addOperator(operator, position);

        return {
          success: true,
          operatorId: operator.operatorID,
          message: `Added ${args.operatorType} operator to workflow`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  };

  // Tool: Add Link
  const addLink: AITool = {
    description: "Connect two operators with a link",
    parameters: z.object({
      sourceOperatorId: z.string().describe("ID of the source operator"),
      sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
      targetOperatorId: z.string().describe("ID of the target operator"),
      targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
    }),
    execute: async (args: { sourceOperatorId: string; sourcePortId?: string; targetOperatorId: string; targetPortId?: string }) => {
      try {
        // Default port IDs if not specified
        const sourcePId = args.sourcePortId || "output-0";
        const targetPId = args.targetPortId || "input-0";

        const link: OperatorLink = {
          linkID: `link_${Date.now()}`,
          source: {
            operatorID: args.sourceOperatorId,
            portID: sourcePId,
          },
          target: {
            operatorID: args.targetOperatorId,
            portID: targetPId,
          },
        };

        workflowActionService.addLink(link);

        return {
          success: true,
          linkId: link.linkID,
          message: `Connected ${args.sourceOperatorId}:${sourcePId} to ${args.targetOperatorId}:${targetPId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  };

  // Tool: List Operators
  const listOperators: AITool = {
    description: "Get all operators in the current workflow",
    parameters: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        return {
          success: true,
          operators: operators,
          count: operators.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  };

  // Tool: List Links
  const listLinks: AITool = {
    description: "Get all links in the current workflow",
    parameters: z.object({}),
    execute: async () => {
      try {
        const links = workflowActionService.getTexeraGraph().getAllLinks();
        return {
          success: true,
          links: links,
          count: links.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  };

  // Tool: List Operator Types
  const listOperatorTypes: AITool = {
    description: "Get all available operator types in the system",
    parameters: z.object({}),
    execute: async () => {
      try {
        const operatorTypes = workflowUtilService.getOperatorTypeList();
        return {
          success: true,
          operatorTypes: operatorTypes,
          count: operatorTypes.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  };

  // Return the tools
  return {
    addOperator,
    addLink,
    listOperators,
    listLinks,
    listOperatorTypes,
  };
}
