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
import { tool } from "ai";
import { DataInconsistencyService } from "../../data-inconsistency/data-inconsistency.service";

// Tool name constants
export const TOOL_NAME_ADD_INCONSISTENCY = "addInconsistency";
export const TOOL_NAME_LIST_INCONSISTENCIES = "listInconsistencies";
export const TOOL_NAME_UPDATE_INCONSISTENCY = "updateInconsistency";
export const TOOL_NAME_DELETE_INCONSISTENCY = "deleteInconsistency";
export const TOOL_NAME_CLEAR_INCONSISTENCIES = "clearInconsistencies";

/**
 * Tool to add a data inconsistency to the list
 */
export function createAddInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: TOOL_NAME_ADD_INCONSISTENCY,
    description:
      "Add a data inconsistency finding to the inconsistency list. Use this when you find data errors or anomalies in the workflow results.",
    inputSchema: z.object({
      name: z.string().describe("Short name for the inconsistency (e.g., 'Negative Prices', 'Missing Values')"),
      description: z.string().describe("Detailed description of the inconsistency found"),
      operatorId: z.string().describe("ID of the operator that revealed this inconsistency"),
    }),
    execute: async (args: { name: string; description: string; operatorId: string }) => {
      try {
        const inconsistency = service.addInconsistency(args.name, args.description, args.operatorId);
        return {
          success: true,
          message: `Added inconsistency: ${args.name}`,
          inconsistency,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}

/**
 * Tool to list all data inconsistencies
 */
export function createListInconsistenciesTool(service: DataInconsistencyService) {
  return tool({
    name: TOOL_NAME_LIST_INCONSISTENCIES,
    description: "Get all data inconsistencies found so far",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        const inconsistencies = service.getAllInconsistencies();
        return {
          success: true,
          count: inconsistencies.length,
          inconsistencies,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}

/**
 * Tool to update an existing data inconsistency
 */
export function createUpdateInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: TOOL_NAME_UPDATE_INCONSISTENCY,
    description: "Update an existing data inconsistency",
    inputSchema: z.object({
      id: z.string().describe("ID of the inconsistency to update"),
      name: z.string().optional().describe("New name for the inconsistency"),
      description: z.string().optional().describe("New description"),
      operatorId: z.string().optional().describe("New operator ID"),
    }),
    execute: async (args: { id: string; name?: string; description?: string; operatorId?: string }) => {
      try {
        const updates: any = {};
        if (args.name !== undefined) updates.name = args.name;
        if (args.description !== undefined) updates.description = args.description;
        if (args.operatorId !== undefined) updates.operatorId = args.operatorId;

        const updated = service.updateInconsistency(args.id, updates);
        if (!updated) {
          return {
            success: false,
            error: `Inconsistency not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Updated inconsistency: ${args.id}`,
          inconsistency: updated,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}

/**
 * Tool to delete a data inconsistency
 */
export function createDeleteInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: TOOL_NAME_DELETE_INCONSISTENCY,
    description: "Delete a data inconsistency from the list",
    inputSchema: z.object({
      id: z.string().describe("ID of the inconsistency to delete"),
    }),
    execute: async (args: { id: string }) => {
      try {
        const deleted = service.deleteInconsistency(args.id);
        if (!deleted) {
          return {
            success: false,
            error: `Inconsistency not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Deleted inconsistency: ${args.id}`,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}

/**
 * Tool to clear all data inconsistencies
 */
export function createClearInconsistenciesTool(service: DataInconsistencyService) {
  return tool({
    name: TOOL_NAME_CLEAR_INCONSISTENCIES,
    description: "Clear all data inconsistencies from the list",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        service.clearAll();
        return {
          success: true,
          message: "Cleared all inconsistencies",
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}
