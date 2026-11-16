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

// How the agent learns what it can build with. The system prompt holds no operator catalog:
// `list_operator_types` names every operator type and what it does, and `get_operator_schema` gives
// the properties of the types the agent picks, several in one call.

import { tool } from "ai";
import { z } from "zod";
import type { WorkflowSystemMetadata } from "../util/workflow-system-metadata";

export const TOOL_NAME_LIST_OPERATOR_TYPES = "list_operator_types";
export const TOOL_NAME_GET_OPERATOR_SCHEMA = "get_operator_schema";

export function createListOperatorTypesTool(metadataStore: WorkflowSystemMetadata) {
  return tool({
    description:
      "List every operator type, one per line, with what it does. Call it once, before you first choose " +
      "operators in this conversation.",
    inputSchema: z.object({}),
    execute: async () => {
      const types = Object.entries(metadataStore.getAllOperatorTypes());
      if (types.length === 0) throw new Error("No operator types are loaded.");
      return types
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([type, description]) => `${type}: ${description.replace(/\s+/g, " ").trim()}`)
        .join("\n");
    },
  });
}

export function createGetOperatorSchemaTool(metadataStore: WorkflowSystemMetadata) {
  return tool({
    description:
      "Get the properties of one or more operator types: which are required, and each one's type, allowed " +
      "values and default. Call it, once for all the types you need, before you add operators of types you " +
      "have not used in this conversation.",
    inputSchema: z.object({
      operatorTypes: z
        .array(z.string())
        .min(1)
        .describe(`The operator types, as ${TOOL_NAME_LIST_OPERATOR_TYPES} names them, such as Aggregate`),
    }),
    execute: async ({ operatorTypes }) => {
      const unknown = operatorTypes.filter(type => metadataStore.getSchema(type) === undefined);
      if (unknown.length > 0) {
        throw new Error(
          `No such operator types: ${unknown.join(", ")}. ${TOOL_NAME_LIST_OPERATOR_TYPES} names the types that exist.`
        );
      }
      const schemas = Object.fromEntries(
        operatorTypes.map(type => [
          type,
          { description: metadataStore.getDescription(type), ...metadataStore.getCompactSchema(type) },
        ])
      );
      return JSON.stringify(schemas);
    },
  });
}
