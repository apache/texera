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
 * Operator metadata tools for Texera Agent Service.
 */

import { z } from "zod";
import { tool } from "ai";
import Ajv from "ajv";
import { createToolResult, createErrorResult } from "./tools-utility";
import { fetchOperatorMetadata, type OperatorSchema, type OperatorMetadata } from "../api/backend-api";
import type { ValidationError, Validation } from "../types/workflow";

// Re-export validation types for backwards compatibility
export type { ValidationError, Validation } from "../types/workflow";

// ============================================================================
// Tool Name Constants
// ============================================================================

export const TOOL_NAME_LIST_ALL_AVAILABLE_OPERATOR_TYPES = "listAllAvailableOperatorTypes";
export const TOOL_NAME_GET_OPERATOR_SCHEMA = "getOperatorSchema";

// ============================================================================
// Operator Schema Types
// ============================================================================

/**
 * Operator schema info structure
 */
export interface OperatorSchemaInfo {
  properties: any;
  required: any;
  definitions: any;
}

/**
 * Compact operator schema with inlined definitions
 */
export interface CompactOperatorSchema {
  properties: Record<string, any>;
  required: string[];
}

// ============================================================================
// Whitelist of Allowed Operator Types
// ============================================================================

export const ALLOWED_OPERATOR_TYPES = [
  "CSVFileScan",
  "Sort",
  "HashJoin",
  "Limit",
  "Projection",
  "TableLimit",
  "LineChart",
  "BarChart",
  "PythonUDFV2",
] as const;

// ============================================================================
// Schema Processing Utilities
// ============================================================================

// Keys to filter out from properties
const FILTERED_PROPERTY_KEYS = ["dummyPropertyList"];

// Keys to filter out from definitions
const FILTERED_DEFINITION_KEYS = [
  "DummyProperties",
  "PortDescription",
  "HashPartition",
  "RangePartition",
  "SinglePartition",
  "BroadcastPartition",
  "UnknownPartition",
];

// Keys to exclude from compact schema
const COMPACT_SCHEMA_EXCLUDED_KEYS = ["propertyOrder", "autofill", "autofillAttributeOnPort", "attributeTypeRules"];

/**
 * Filter an object by excluding specified keys.
 */
function filterObjectKeys(obj: any, keysToExclude: string[]): any {
  if (!obj || typeof obj !== "object") {
    return obj;
  }
  const filtered: any = {};
  for (const key of Object.keys(obj)) {
    if (!keysToExclude.includes(key)) {
      filtered[key] = obj[key];
    }
  }
  return filtered;
}

/**
 * Recursively inline $ref references and clean up schema.
 */
function inlineRefs(schema: any, definitions: Record<string, any>): any {
  if (!schema || typeof schema !== "object") {
    return schema;
  }

  if (schema.$ref && typeof schema.$ref === "string") {
    const refPath = schema.$ref.replace("#/definitions/", "");
    const refDef = definitions[refPath];
    if (refDef) {
      return inlineRefs(refDef, definitions);
    }
    return schema;
  }

  if (Array.isArray(schema)) {
    return schema.map(item => inlineRefs(item, definitions));
  }

  const result: any = {};
  for (const [key, value] of Object.entries(schema)) {
    if (COMPACT_SCHEMA_EXCLUDED_KEYS.includes(key)) {
      continue;
    }
    if (typeof value === "object" && value !== null) {
      result[key] = inlineRefs(value, definitions);
    } else {
      result[key] = value;
    }
  }
  return result;
}

/**
 * Get compact schema with inlined definitions.
 */
export function getCompactSchema(jsonSchema: any): CompactOperatorSchema | null {
  try {
    const properties = filterObjectKeys(jsonSchema.properties, FILTERED_PROPERTY_KEYS);
    const definitions = filterObjectKeys(jsonSchema.definitions, FILTERED_DEFINITION_KEYS) || {};

    const compactProperties: Record<string, any> = {};
    for (const [propName, propSchema] of Object.entries(properties || {})) {
      compactProperties[propName] = inlineRefs(propSchema, definitions);
    }

    return {
      properties: compactProperties,
      required: jsonSchema.required || [],
    };
  } catch {
    return null;
  }
}

// ============================================================================
// Operator Metadata Store (Singleton)
// ============================================================================

// Shared Ajv instance - same configuration as frontend ValidationWorkflowService
const ajv = new Ajv({ allErrors: true, strict: false });

/**
 * In-memory store for operator schemas.
 * Can be populated from backend API or manually registered.
 * Uses singleton pattern - initialized once at server startup.
 */
export class OperatorMetadataStore {
  /** Singleton instance */
  private static instance: OperatorMetadataStore | null = null;

  /**
   * Get the singleton instance.
   */
  static getInstance(): OperatorMetadataStore {
    if (!OperatorMetadataStore.instance) {
      OperatorMetadataStore.instance = new OperatorMetadataStore();
    }
    return OperatorMetadataStore.instance;
  }

  /**
   * Initialize the global singleton from backend.
   * Should be called once at server startup.
   */
  static async initializeGlobal(): Promise<OperatorMetadataStore> {
    const store = OperatorMetadataStore.getInstance();
    if (!store.isInitialized()) {
      await store.initializeFromBackend();
    }
    return store;
  }

  private schemas: Map<string, any> = new Map();
  private descriptions: Map<string, string> = new Map();
  private additionalMetadata: Map<string, any> = new Map();
  private initialized = false;

  /**
   * Initialize the store by fetching operator metadata from the backend.
   * This is the preferred way to populate the store.
   */
  async initializeFromBackend(): Promise<void> {
    try {
      const metadata = await fetchOperatorMetadata();
      this.loadFromMetadata(metadata);
      this.initialized = true;
      console.log(`[OperatorMetadataStore] Loaded ${this.schemas.size} operators from backend`);
    } catch (error) {
      console.warn("[OperatorMetadataStore] Failed to fetch from backend:", error);
      throw error;
    }
  }

  /**
   * Load operator metadata from a pre-fetched metadata object.
   */
  loadFromMetadata(metadata: OperatorMetadata): void {
    for (const op of metadata.operators) {
      this.schemas.set(op.operatorType, op.jsonSchema);
      this.descriptions.set(
        op.operatorType,
        op.additionalMetadata.operatorDescription || op.additionalMetadata.userFriendlyName
      );
      this.additionalMetadata.set(op.operatorType, op.additionalMetadata);
    }
  }

  /**
   * Check if the store is initialized.
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Register an operator schema manually (for testing or custom operators).
   */
  registerOperator(operatorType: string, jsonSchema: any, description: string = ""): void {
    this.schemas.set(operatorType, jsonSchema);
    this.descriptions.set(operatorType, description);
  }

  /**
   * Get schema for an operator type.
   */
  getSchema(operatorType: string): any | undefined {
    return this.schemas.get(operatorType);
  }

  /**
   * Get description for an operator type.
   */
  getDescription(operatorType: string): string {
    return this.descriptions.get(operatorType) || "";
  }

  /**
   * Get additional metadata for an operator type.
   */
  getAdditionalMetadata(operatorType: string): any | undefined {
    return this.additionalMetadata.get(operatorType);
  }

  /**
   * Get all operator types with descriptions.
   */
  getAllOperatorTypes(): Record<string, string> {
    const result: Record<string, string> = {};
    for (const [type, desc] of this.descriptions) {
      result[type] = desc;
    }
    return result;
  }

  /**
   * Get compact schema for an operator type.
   */
  getCompactSchema(operatorType: string): CompactOperatorSchema | null {
    const schema = this.schemas.get(operatorType);
    if (!schema) return null;
    return getCompactSchema(schema);
  }

  /**
   * Get all schemas as JSON (for system prompt embedding).
   */
  getAllSchemasAsJson(): string {
    const result: Record<string, OperatorSchemaInfo> = {};
    for (const [type, schema] of this.schemas) {
      result[type] = {
        properties: filterObjectKeys(schema.properties, FILTERED_PROPERTY_KEYS),
        required: schema.required,
        definitions: filterObjectKeys(schema.definitions, FILTERED_DEFINITION_KEYS),
      };
    }
    return JSON.stringify(result, null, 2);
  }

  /**
   * Get the number of registered operators.
   */
  getOperatorCount(): number {
    return this.schemas.size;
  }

  /**
   * Check if an operator type exists.
   */
  operatorTypeExists(operatorType: string): boolean {
    return this.schemas.has(operatorType);
  }

  /**
   * Validate operator properties against its schema using Ajv.
   * Returns the same Validation type as frontend ValidationWorkflowService for consistency.
   */
  validateOperatorProperties(operatorType: string, properties: Record<string, any>): Validation {
    const schema = this.schemas.get(operatorType);
    if (!schema) {
      return { isValid: false, messages: { error: `Unknown operator type: ${operatorType}` } };
    }

    try {
      const isValid = ajv.validate(schema, properties);

      if (isValid) {
        return { isValid: true };
      }

      // Convert Ajv errors to messages format (same as frontend ValidationWorkflowService)
      const messages: Record<string, string> = {};
      if (ajv.errors) {
        for (const error of ajv.errors) {
          const key = error.instancePath
            ? error.instancePath.replace(/^\//, "").replace(/\//g, ".")
            : (error.params as any)?.missingProperty || error.keyword;
          messages[key] = error.message || "Validation failed";
        }
      }
      return { isValid: false, messages };
    } catch (e) {
      return { isValid: false, messages: { error: `Validation error: ${e}` } };
    }
  }
}

/**
 * Format validation result into a readable message for the agent.
 */
export function formatValidationErrors(validation: Validation): string {
  if (validation.isValid) return "";
  const errorMessages = Object.entries(validation.messages).map(([key, msg]) => `${key}: ${msg}`);
  return errorMessages.join("; ");
}

/**
 * Format a compact schema summary showing only required properties.
 * Returns a single-line string suitable for error messages.
 */
export function formatCompactSchemaForError(compactSchema: CompactOperatorSchema): string {
  const requiredProps: Record<string, any> = {};
  for (const key of compactSchema.required) {
    if (compactSchema.properties[key]) {
      requiredProps[key] = compactSchema.properties[key];
    }
  }
  return `required: [${compactSchema.required.join(", ")}], properties: ${JSON.stringify(requiredProps)}`;
}

// ============================================================================
// Tool Creators
// ============================================================================

/**
 * Create tool to list all available operator types with descriptions.
 * @param metadataStore - The operator metadata store
 * @param onlyUseRelationalOperators - If true, only return operators from ALLOWED_OPERATOR_TYPES list
 */
export function createListAllAvailableOperatorTypesTool(
  metadataStore: OperatorMetadataStore,
  onlyUseRelationalOperators: boolean = false
) {
  return tool({
    description:
      "List all available operator types in Texera with their descriptions. " +
      "Use this to discover what operators are available before adding them to a workflow.",
    inputSchema: z.object({}),
    execute: async () => {
      let operators = metadataStore.getAllOperatorTypes();

      // Filter to only allowed relational operators if setting is enabled
      if (onlyUseRelationalOperators) {
        const allowedSet = new Set<string>(ALLOWED_OPERATOR_TYPES);
        operators = Object.fromEntries(Object.entries(operators).filter(([type]) => allowedSet.has(type)));
      }

      const count = Object.keys(operators).length;
      if (count === 0) {
        return createErrorResult("No operator types registered.");
      }

      const lines = [`Found ${count} available operator type(s):`];
      for (const [type, description] of Object.entries(operators)) {
        lines.push(`  - ${type}: ${description}`);
      }

      return createToolResult(lines.join("\n"));
    },
  });
}

/**
 * Create tool to get the schema of a specific operator type.
 */
export function createGetOperatorSchemaTool(metadataStore: OperatorMetadataStore) {
  return tool({
    description:
      "Get the JSON schema for a specific operator type. " +
      "Returns a compact schema with inlined definitions. " +
      "Use this to understand what properties an operator requires.",
    inputSchema: z.object({
      operatorType: z.string().describe("The operator type to get the schema for"),
    }),
    execute: async (args: { operatorType: string }) => {
      const compactSchema = metadataStore.getCompactSchema(args.operatorType);
      if (!compactSchema) {
        return createErrorResult(`Operator type "${args.operatorType}" not found.`);
      }

      const lines = [
        `Schema for operator type "${args.operatorType}":`,
        `required: [${compactSchema.required.join(", ")}]`,
        `properties:`,
        JSON.stringify(compactSchema.properties, null, 2),
      ];

      return createToolResult(lines.join("\n"));
    },
  });
}
