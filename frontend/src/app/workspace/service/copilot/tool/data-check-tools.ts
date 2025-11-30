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
import { DataCheckService } from "../../data-check/data-check.service";

// Tool name constants
export const TOOL_NAME_ADD_DATA_CHECK = "addDataCheck";
export const TOOL_NAME_LIST_DATA_CHECKS = "listDataChecks";
export const TOOL_NAME_UPDATE_DATA_CHECK = "updateDataCheck";
export const TOOL_NAME_DELETE_DATA_CHECK = "deleteDataCheck";
export const TOOL_NAME_CLEAR_DATA_CHECKS = "clearDataChecks";

/**
 * Tool to add a data check to the list
 */
export function createAddDataCheckTool(service: DataCheckService) {
  return tool({
    name: TOOL_NAME_ADD_DATA_CHECK,
    description:
      "Add a data check finding to the data check list. Use this when you find data errors or anomalies in the workflow results.",
    inputSchema: z.object({
      name: z.string().describe("Short name for the data check (e.g., 'Negative Prices', 'Missing Values')"),
      description: z.string().describe("Detailed description of the data check found"),
      operatorId: z.string().describe("ID of the operator that revealed this data check"),
    }),
    execute: async (args: { name: string; description: string; operatorId: string }) => {
      try {
        const dataCheck = service.addDataCheck(args.name, args.description, args.operatorId);
        return {
          success: true,
          message: `Added data check: ${args.name}`,
          dataCheck,
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
 * Tool to list all data checks
 */
export function createListDataChecksTool(service: DataCheckService) {
  return tool({
    name: TOOL_NAME_LIST_DATA_CHECKS,
    description: "Get all data checks found so far",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        const dataChecks = service.getAllDataChecks();
        return {
          success: true,
          count: dataChecks.length,
          dataChecks,
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
 * Tool to update an existing data check
 */
export function createUpdateDataCheckTool(service: DataCheckService) {
  return tool({
    name: TOOL_NAME_UPDATE_DATA_CHECK,
    description: "Update an existing data check",
    inputSchema: z.object({
      id: z.string().describe("ID of the data check to update"),
      name: z.string().optional().describe("New name for the data check"),
      description: z.string().optional().describe("New description"),
      operatorId: z.string().optional().describe("New operator ID"),
    }),
    execute: async (args: { id: string; name?: string; description?: string; operatorId?: string }) => {
      try {
        const updates: any = {};
        if (args.name !== undefined) updates.name = args.name;
        if (args.description !== undefined) updates.description = args.description;
        if (args.operatorId !== undefined) updates.operatorId = args.operatorId;

        const updated = service.updateDataCheck(args.id, updates);
        if (!updated) {
          return {
            success: false,
            error: `Data check not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Updated data check: ${args.id}`,
          dataCheck: updated,
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
 * Tool to delete a data check
 */
export function createDeleteDataCheckTool(service: DataCheckService) {
  return tool({
    name: TOOL_NAME_DELETE_DATA_CHECK,
    description: "Delete a data check from the list",
    inputSchema: z.object({
      id: z.string().describe("ID of the data check to delete"),
    }),
    execute: async (args: { id: string }) => {
      try {
        const deleted = service.deleteDataCheck(args.id);
        if (!deleted) {
          return {
            success: false,
            error: `Data check not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Deleted data check: ${args.id}`,
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
 * Tool to clear all data checks
 */
export function createClearDataChecksTool(service: DataCheckService) {
  return tool({
    name: TOOL_NAME_CLEAR_DATA_CHECKS,
    description: "Clear all data checks from the list",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        service.clearAll();
        return {
          success: true,
          message: "Cleared all data checks",
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
