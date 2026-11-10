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

import type { JSONSchema7, JSONSchema7Definition } from "json-schema";
import { cloneDeep } from "lodash";
import type { CustomJSONSchema7 } from "../../core/workspace/types/custom-json-schema.interface";
import type { OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
import type { OperatorPortSchemaMap, PortSchema } from "../../core/workspace/types/workflow-compiling.interface";
import { parseLogicalOperatorPortID } from "../../core/common/util/logical-operator-port-serde";
import { serializePortIdentity } from "../../core/common/util/port-identity-serde";
import { areAllPortSchemasEqual } from "../../core/common/util/workflow-compilation-utils";

export interface InputSchemaLink {
  readonly source: { readonly operatorID: string; readonly portID: string };
  readonly target: { readonly operatorID: string; readonly portID: string };
}

export interface InputSchemaConflict {
  operatorId: string;
  portIndex: number;
  message: string;
  details: string;
}

/** Resolves input choices from every incoming link, retaining unknown and known-empty schemas. */
export function inputSchemasForOperator(options: {
  operatorId: string;
  graph: { getInputLinksByOperatorId(operatorId: string): readonly InputSchemaLink[] };
  dynamicSchema: { additionalMetadata: { inputPorts: readonly unknown[] } } | undefined;
  outputSchemas: Readonly<Record<string, OperatorPortSchemaMap>>;
}): { inputSchemas: OperatorPortSchemaMap | undefined; errors: InputSchemaConflict[] } {
  const { operatorId, graph, dynamicSchema, outputSchemas } = options;
  const inputLinks = graph.getInputLinksByOperatorId(operatorId);
  const errors: InputSchemaConflict[] = [];
  if (!inputLinks.length || !dynamicSchema) return { inputSchemas: undefined, errors };
  const inputSchemas: Record<string, PortSchema | undefined> = {};
  dynamicSchema.additionalMetadata.inputPorts.forEach((_port, portIndex) => {
    const portId = serializePortIdentity({ id: portIndex, internal: false });
    inputSchemas[portId] = undefined;
    const links = inputLinks.filter(link => parseLogicalOperatorPortID(link.target.portID)?.portNumber === portIndex);
    const schemas = links.map(link => {
      const upstream = outputSchemas[link.source.operatorID];
      const sourcePort = parseLogicalOperatorPortID(link.source.portID);
      return upstream && sourcePort
        ? upstream[serializePortIdentity({ id: sourcePort.portNumber, internal: false })]
        : undefined;
    });
    if (schemas.length > 1 && !areAllPortSchemasEqual(schemas)) {
      errors.push({
        operatorId,
        portIndex,
        message: `Multiple links with different schemas connected to the same input port ${portIndex}`,
        details: `Port ${portIndex} received ${schemas.length} different schemas (some may be undefined)`,
      });
    } else if (schemas.length) inputSchemas[portId] = schemas[0];
  });
  return { inputSchemas: Object.keys(inputSchemas).length ? inputSchemas : undefined, errors };
}

// The existing pure compilation transforms are shared without loading Angular services.
export function mutateSchemaProperties(
  jsonSchemaToChange: CustomJSONSchema7,
  matchFunc: (propertyName: string, propertyValue: CustomJSONSchema7) => boolean,
  mutationFunc: (propertyName: string, propertyValue: CustomJSONSchema7) => CustomJSONSchema7
): CustomJSONSchema7 {
  // recursively walks the JSON schema property tree to find the property name
  const mutatePropertyRecurse = (jsonSchema: JSONSchema7) => {
    const schemaProperties = jsonSchema.properties;
    const schemaDefinitions = jsonSchema.definitions;
    const schemaItems = jsonSchema.items;

    // nested JSON schema property can have 2 types: object or array
    const mutateObjectProperty = (objectProperty: { [key: string]: JSONSchema7Definition }) => {
      Object.entries(objectProperty).forEach(([propertyName, propertyValue]) => {
        if (typeof propertyValue === "boolean") {
          return;
        }
        if (matchFunc(propertyName, propertyValue as CustomJSONSchema7)) {
          objectProperty[propertyName] = mutationFunc(propertyName, propertyValue as CustomJSONSchema7);
        } else {
          mutatePropertyRecurse(propertyValue);
        }
      });
    };
    const mutateArrayProperty = (arrayProperty: JSONSchema7Definition[]) => {
      arrayProperty.forEach(item => {
        if (typeof item !== "boolean") {
          mutatePropertyRecurse(item);
        }
      });
    };

    if (schemaProperties) {
      mutateObjectProperty(schemaProperties);
    }
    if (schemaDefinitions) {
      mutateObjectProperty(schemaDefinitions);
    }
    if (schemaItems && typeof schemaItems !== "boolean") {
      if (Array.isArray(schemaItems)) {
        mutateArrayProperty(schemaItems);
      } else {
        mutatePropertyRecurse(schemaItems);
      }
    }
  };

  // deep copy the schema first to avoid changing the original schema object
  const jsonSchemaCopy = cloneDeep(jsonSchemaToChange);
  mutatePropertyRecurse(jsonSchemaCopy);

  return jsonSchemaCopy;
}

export function setOperatorInputAttrs(
  operatorSchema: OperatorSchema,
  inputPortSchemaMap: OperatorPortSchemaMap | undefined
): OperatorSchema {
  // If the inputSchema is empty, just return the original operator metadata.
  if (!inputPortSchemaMap || Object.keys(inputPortSchemaMap).length === 0) {
    return operatorSchema;
  }

  let newJsonSchema = operatorSchema.jsonSchema;

  const getAttrNames = (attrName: string, v: CustomJSONSchema7): string[] | undefined => {
    const i = v.autofillAttributeOnPort;
    if (i === undefined || i === null || !Number.isInteger(i)) {
      return undefined;
    }

    // Use serializePortIdentity to get the correct key for the input port
    const portId = serializePortIdentity({ id: i, internal: false });
    const inputAttrAtPort = inputPortSchemaMap[portId];
    if (!inputAttrAtPort) {
      return undefined;
    }
    const attrNames: string[] = inputAttrAtPort.map(attr => attr.attributeName);
    if (v.additionalEnumValue) {
      attrNames.push(v.additionalEnumValue);
    }

    // ajv does not support null values, so it converts all the nulls to empty strings.
    // https://github.com/ajv-validator/ajv/issues/1471
    // the null -> "" change is done by Ajv.validate() with useDefault set to true.
    // It is converted during the property editor form initialization and workflow validation, instead of during schema propagation.
    if (!operatorSchema.jsonSchema.required?.includes(attrName)) {
      if (v.default) {
        if (typeof v.default !== "string") {
          throw new Error("default value must be a string");
        }
        // We are adding the default value or "" into
        // the enum list to pass the frontend check for optional properties.
        attrNames.push(v.default);
      } else {
        attrNames.push("");
      }
    }
    return attrNames;
  };

  newJsonSchema = mutateSchemaProperties(
    newJsonSchema,
    (k, v) => v.autofill === "attributeName",
    (attrName, old) => ({
      ...old,
      type: "string",
      enum: getAttrNames(attrName, old),
      uniqueItems: true,
    })
  );

  newJsonSchema = mutateSchemaProperties(
    newJsonSchema,
    (k, v) => v.autofill === "attributeNameList",
    (attrName, old) => ({
      ...old,
      type: "array",
      uniqueItems: true,
      items: {
        ...(old.items as CustomJSONSchema7),
        type: "string",
        enum: getAttrNames(attrName, old),
      },
    })
  );

  return {
    ...operatorSchema,
    jsonSchema: newJsonSchema,
  };
}

/**
 * Walks an operator's property values with its json schema and drops any
 * value that references an input attribute which is no longer valid.
 *
 * Only properties marked with an `autofill` annotation are affected, and only when the schema carries an
 * `enum` of valid attribute names (i.e. the input schema is known). Two cases are handled:
 *  - `attributeName`: a single column name. Reset to "" if it's not in the enum.
 *  - `attributeNameList`: a list of column names. Filter out entries that aren't in the enum.
 *
 * Returns the (possibly new) properties object and whether anything changed.
 */
export function dropInvalidAttributeValues(
  schema: JSONSchema7Definition | undefined,
  value: unknown
): { value: unknown; changed: boolean } {
  if (typeof schema !== "object" || schema === null || value === undefined || value === null) {
    return { value, changed: false };
  }
  const s = schema as CustomJSONSchema7;

  if (s.autofill === "attributeNameList") {
    const itemEnum = (s.items as CustomJSONSchema7 | undefined)?.enum;
    if (Array.isArray(value) && Array.isArray(itemEnum)) {
      const filtered = value.filter(v => itemEnum.includes(v));
      return { value: filtered, changed: filtered.length !== value.length };
    }
    return { value, changed: false };
  }

  if (s.autofill === "attributeName") {
    if (Array.isArray(s.enum) && typeof value === "string" && !s.enum.includes(value)) {
      return { value: "", changed: true };
    }
    return { value, changed: false };
  }

  // recurse into object properties
  if (s.properties && typeof value === "object" && !Array.isArray(value)) {
    let changed = false;
    const newValue: Record<string, unknown> = { ...(value as Record<string, unknown>) };
    Object.entries(s.properties).forEach(([key, propSchema]) => {
      if (key in newValue) {
        const res = dropInvalidAttributeValues(propSchema, newValue[key]);
        if (res.changed) {
          newValue[key] = res.value;
          changed = true;
        }
      }
    });
    return { value: changed ? newValue : value, changed };
  }

  // recurse into array items (only when items is a single schema, not a tuple schema)
  if (s.items && !Array.isArray(s.items) && Array.isArray(value)) {
    let changed = false;
    const newArr = value.map(item => {
      const res = dropInvalidAttributeValues(s.items as JSONSchema7Definition, item);
      if (res.changed) {
        changed = true;
        return res.value;
      }
      return item;
    });
    return { value: changed ? newArr : value, changed };
  }

  return { value, changed: false };
}

export function restoreOperatorInputAttrs(operatorSchema: OperatorSchema): OperatorSchema {
  let newJsonSchema = operatorSchema.jsonSchema;

  newJsonSchema = mutateSchemaProperties(
    newJsonSchema,
    (k, v) => v.autofill === "attributeName",
    (attrName, old) => ({
      ...old,
      type: "string",
      enum: undefined,
      uniqueItems: undefined,
    })
  );

  newJsonSchema = mutateSchemaProperties(
    newJsonSchema,
    (k, v) => v.autofill === "attributeNameList",
    (attrName, old) => ({
      ...old,
      type: "array",
      uniqueItems: undefined,
      items: {
        ...(old.items as CustomJSONSchema7),
        type: "string",
        enum: undefined,
      },
    })
  );

  return {
    ...operatorSchema,
    jsonSchema: newJsonSchema,
  };
}
