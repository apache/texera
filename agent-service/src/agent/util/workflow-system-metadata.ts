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

import Ajv from "ajv";
import { fetchOperatorMetadata, type OperatorSchema, type OperatorMetadata } from "../../api/backend-api";
import type { ValidationError, Validation } from "../../types/workflow";
import { createLogger } from "../../logger";

const log = createLogger("WorkflowSystemMetadata");

export type { ValidationError, Validation } from "../../types/workflow";

interface OperatorSchemaInfo {
  properties: any;
  required: any;
  definitions: any;
}

interface CompactOperatorSchema {
  properties: Record<string, any>;
  required: string[];
}

const FILTERED_PROPERTY_KEYS = ["dummyPropertyList"];

const FILTERED_DEFINITION_KEYS = [
  "DummyProperties",
  "PortDescription",
  "HashPartition",
  "RangePartition",
  "SinglePartition",
  "BroadcastPartition",
  "UnknownPartition",
];

const COMPACT_SCHEMA_EXCLUDED_KEYS = ["propertyOrder", "autofill", "autofillAttributeOnPort", "attributeTypeRules"];

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

function getCompactSchema(jsonSchema: any): CompactOperatorSchema | null {
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

// Matches the frontend ValidationWorkflowService Ajv configuration.
const ajv = new Ajv({ allErrors: true, strict: false });

/**
 * Process-wide singleton cache of operator metadata fetched from the backend.
 *
 * Holds each operator type's JSON schema, description, and additional
 * metadata, plus a compact schema variant used in system prompts and error
 * messages. Exposes Ajv-backed property validation that matches the
 * frontend's `ValidationWorkflowService` configuration.
 */
export class WorkflowSystemMetadata {
  private static instance: WorkflowSystemMetadata | null = null;

  static getInstance(): WorkflowSystemMetadata {
    if (!WorkflowSystemMetadata.instance) {
      WorkflowSystemMetadata.instance = new WorkflowSystemMetadata();
    }
    return WorkflowSystemMetadata.instance;
  }

  static async initializeGlobal(): Promise<WorkflowSystemMetadata> {
    const instance = WorkflowSystemMetadata.getInstance();
    if (!instance.isInitialized()) {
      await instance.initializeFromBackend();
    }
    return instance;
  }

  private schemas: Map<string, any> = new Map();
  private descriptions: Map<string, string> = new Map();
  private additionalMetadata: Map<string, any> = new Map();
  private initialized = false;

  async initializeFromBackend(): Promise<void> {
    try {
      const metadata = await fetchOperatorMetadata();
      this.loadFromMetadata(metadata);
      this.initialized = true;
      log.info({ operatorCount: this.schemas.size }, "loaded operators from backend");
    } catch (error) {
      log.warn({ err: error }, "failed to fetch from backend");
      throw error;
    }
  }

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

  isInitialized(): boolean {
    return this.initialized;
  }

  getSchema(operatorType: string): any | undefined {
    return this.schemas.get(operatorType);
  }

  getDescription(operatorType: string): string {
    return this.descriptions.get(operatorType) || "";
  }

  getAdditionalMetadata(operatorType: string): any | undefined {
    return this.additionalMetadata.get(operatorType);
  }

  getAllOperatorTypes(): Record<string, string> {
    const result: Record<string, string> = {};
    for (const [type, desc] of this.descriptions) {
      result[type] = desc;
    }
    return result;
  }

  getCompactSchema(operatorType: string): CompactOperatorSchema | null {
    const schema = this.schemas.get(operatorType);
    if (!schema) return null;
    return getCompactSchema(schema);
  }

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

  getOperatorCount(): number {
    return this.schemas.size;
  }

  operatorTypeExists(operatorType: string): boolean {
    return this.schemas.has(operatorType);
  }

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

export function formatValidationErrors(validation: Validation): string {
  if (validation.isValid) return "";
  const errorMessages = Object.entries(validation.messages).map(([key, msg]) => `${key}: ${msg}`);
  return errorMessages.join("; ");
}

export function formatCompactSchemaForError(compactSchema: CompactOperatorSchema): string {
  const requiredProps: Record<string, any> = {};
  for (const key of compactSchema.required) {
    if (compactSchema.properties[key]) {
      requiredProps[key] = compactSchema.properties[key];
    }
  }
  return `required: [${compactSchema.required.join(", ")}], properties: ${JSON.stringify(requiredProps)}`;
}
