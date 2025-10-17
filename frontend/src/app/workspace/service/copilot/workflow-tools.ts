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

import { createTool } from "@mastra/core/tools";
import { z } from "zod";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";

/**
 * Create workflow manipulation tools that work with WorkflowActionService
 */
export function createWorkflowTools(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService
) {
  // Tool: Add Operator
  const addOperator = createTool({
    id: "addOperator",
    description: "Add a new operator to the workflow",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
    }),
    outputSchema: z.object({
      success: z.boolean(),
      operatorId: z.string().optional(),
      message: z.string().optional(),
      error: z.string().optional(),
    }),
    execute: async ({ context }) => {
      const { operatorType } = context;

      try {
        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(operatorType)) {
          return {
            success: false,
            error: `Unknown operator type: ${operatorType}. Use listOperatorTypes tool to see available types.`,
          };
        }

        // Get a new operator predicate with default settings
        const operator = workflowUtilService.getNewOperatorPredicate(operatorType);

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
          message: `Added ${operatorType} operator to workflow`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });

  // Tool: Add Link
  const addLink = createTool({
    id: "addLink",
    description: "Connect two operators with a link",
    inputSchema: z.object({
      sourceOperatorId: z.string().describe("ID of the source operator"),
      sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
      targetOperatorId: z.string().describe("ID of the target operator"),
      targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
    }),
    outputSchema: z.object({
      success: z.boolean(),
      linkId: z.string().optional(),
      message: z.string().optional(),
      error: z.string().optional(),
    }),
    execute: async ({ context }) => {
      const { sourceOperatorId, sourcePortId, targetOperatorId, targetPortId } = context;

      try {
        // Default port IDs if not specified
        const sourcePId = sourcePortId || "output-0";
        const targetPId = targetPortId || "input-0";

        const link: OperatorLink = {
          linkID: `link_${Date.now()}`,
          source: {
            operatorID: sourceOperatorId,
            portID: sourcePId,
          },
          target: {
            operatorID: targetOperatorId,
            portID: targetPId,
          },
        };

        workflowActionService.addLink(link);

        return {
          success: true,
          linkId: link.linkID,
          message: `Connected ${sourceOperatorId}:${sourcePId} to ${targetOperatorId}:${targetPId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });

  // Return the tools
  return {
    addOperator,
    addLink,
  };
}
