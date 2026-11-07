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

import Ajv, { type ErrorObject, type ValidateFunction } from "ajv";
import type { JSONSchema7, JSONSchema7Definition } from "json-schema";

export type PropertyPath = readonly (string | number)[];
export type PropertySchemaDefinition = OperatorPropertySchema | boolean;

export interface ValueRule {
  enum?: readonly string[];
  type?: "integer" | "number";
  minimum?: number;
  exclusiveMinimum?: number;
  pattern?: string;
  examples?: readonly string[];
}

/** JSON Schema plus the field behavior emitted by Texera operator descriptors. */
export interface OperatorPropertySchema extends JSONSchema7 {
  properties?: Record<string, PropertySchemaDefinition>;
  items?: PropertySchemaDefinition | PropertySchemaDefinition[];
  propertyOrder?: number;
  toggleHidden?: string[];
  hideTarget?: string;
  hideType?: "regex" | "equals";
  hideExpectedValue?: string;
  hideOnNull?: boolean;
  uniqueAmongRows?: boolean;
  noLoopVariable?: boolean;
  "enable-presets"?: boolean;
  "x-widget"?: string;
  valueRules?: {
    allOf: readonly {
      if: Readonly<Record<string, { valEnum?: readonly string[] }>>;
      then: ValueRule;
    }[];
  };
}

export interface PropertyIssue {
  path: PropertyPath;
  keyword: string;
  message: string;
}

const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false });
const validators = new WeakMap<OperatorPropertySchema, ValidateFunction>();
const unsafeKeys = new Set(["__proto__", "prototype", "constructor"]);

export function schemaType(schema: OperatorPropertySchema): string | undefined {
  return Array.isArray(schema.type)
    ? schema.type.find(type => type !== "null") ?? (schema.type.includes("null") ? "null" : undefined)
    : schema.type;
}

export function asSchema(schema: JSONSchema7Definition | undefined): OperatorPropertySchema {
  return typeof schema === "object" && schema !== null ? schema : {};
}

function clone<T>(value: T): T {
  return value === undefined ? value : structuredClone(value);
}

/** Defaults are values for missing members; existing null and falsy values are preserved. */
export function applySchemaDefaults(schema: OperatorPropertySchema, value: unknown, root = schema): unknown {
  schema = resolveSchemaReference(schema, root);
  let result = clone(value === undefined && schema.default !== undefined ? schema.default : value);
  if (result !== null && typeof result === "object" && !Array.isArray(result)) {
    const object = result as Record<string, unknown>;
    for (const [key, property] of Object.entries(schema.properties ?? {})) {
      if (unsafeKeys.has(key) || typeof property === "boolean") continue;
      const current = Object.hasOwn(object, key) ? object[key] : undefined;
      const next = applySchemaDefaults(property, current, root);
      if (next !== undefined) object[key] = next;
    }
  } else if (Array.isArray(result)) {
    result = result.map((item, index) => applySchemaDefaults(itemSchema(schema, index), item, root));
  }
  return result;
}

export function newSchemaValue(schema: OperatorPropertySchema): unknown {
  if (schema.default !== undefined) return applySchemaDefaults(schema, schema.default);
  if (schema.const !== undefined) return clone(schema.const);
  if (schema.enum?.length) return clone(schema.enum[0]);
  switch (schemaType(schema)) {
    case "object":
      return applySchemaDefaults(schema, {});
    case "array":
      return [];
    case "boolean":
      return false;
    case "number":
    case "integer":
      return undefined;
    case "null":
      return null;
    default:
      return "";
  }
}

export function itemSchema(schema: OperatorPropertySchema, index: number): OperatorPropertySchema {
  return asSchema(Array.isArray(schema.items) ? schema.items[index] : schema.items);
}

export function resolveSchemaReference(
  schema: OperatorPropertySchema,
  root: OperatorPropertySchema
): OperatorPropertySchema {
  const seen = new Set<string>();
  let result = schema;
  while (result.$ref?.startsWith("#/") && !seen.has(result.$ref)) {
    seen.add(result.$ref);
    const parts = result.$ref
      .slice(2)
      .split("/")
      .map(part => part.replace(/~1/g, "/").replace(/~0/g, "~"));
    const target = getAtPath(root, parts);
    if (!target || typeof target !== "object") break;
    result = { ...target, ...result, $ref: (target as OperatorPropertySchema).$ref };
  }
  return result;
}

export function sortedProperties(schema: OperatorPropertySchema): [string, OperatorPropertySchema][] {
  return Object.entries(schema.properties ?? {})
    .filter(
      (entry): entry is [string, OperatorPropertySchema] => typeof entry[1] !== "boolean" && !unsafeKeys.has(entry[0])
    )
    .sort((a, b) => (a[1].propertyOrder ?? Number.MAX_SAFE_INTEGER) - (b[1].propertyOrder ?? Number.MAX_SAFE_INTEGER));
}

export function fieldIsHidden(key: string, parentSchema: OperatorPropertySchema, model: unknown): boolean {
  const object = model !== null && typeof model === "object" ? (model as Record<string, unknown>) : {};
  for (const [source, sourceSchema] of sortedProperties(parentSchema)) {
    if (sourceSchema.toggleHidden?.includes(key) && !object[source]) return true;
  }
  const field = asSchema(parentSchema.properties?.[key]);
  if (!field.hideTarget || field.hideExpectedValue === undefined || !field.hideType) return false;
  const target = object[field.hideTarget];
  if (target === null || target === undefined) return field.hideOnNull === true;
  if (field.hideType === "equals") return String(target) === field.hideExpectedValue;
  try {
    return new RegExp(`^(${field.hideExpectedValue})$`).test(String(target));
  } catch {
    return false;
  }
}

export function matchingValueRule(schema: OperatorPropertySchema, model: unknown): ValueRule | undefined {
  if (model === null || typeof model !== "object") return undefined;
  const object = model as Record<string, unknown>;
  return schema.valueRules?.allOf.find(branch =>
    Object.entries(branch.if).every(([key, condition]) => condition.valEnum?.includes(String(object[key])) === true)
  )?.then;
}

export function getAtPath(value: unknown, path: PropertyPath): unknown {
  let current = value;
  for (const part of path) {
    if (typeof part === "string" && unsafeKeys.has(part)) return undefined;
    if (current === null || typeof current !== "object" || !Object.hasOwn(current, part)) return undefined;
    current = (current as Record<string | number, unknown>)[part];
  }
  return current;
}

/** Copy only the edited branch, preserving unrelated saved properties and list order. */
export function setAtPath(value: unknown, path: PropertyPath, next: unknown): unknown {
  if (path.some(part => typeof part === "string" && unsafeKeys.has(part))) throw new Error("Unsafe property path");
  if (!path.length) return next;
  const [first, ...rest] = path;
  if (typeof first === "number") {
    if (!Number.isSafeInteger(first) || first < 0) throw new Error("Invalid array index");
    const array = Array.isArray(value) ? [...value] : [];
    array[first] = setAtPath(array[first], rest, next);
    return array;
  }
  const object = value !== null && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
  const previous = Object.hasOwn(object, first) ? (object as Record<string, unknown>)[first] : undefined;
  if (!rest.length && next === undefined) delete (object as Record<string, unknown>)[first];
  else
    Object.defineProperty(object, first, {
      value: setAtPath(previous, rest, next),
      enumerable: true,
      writable: true,
      configurable: true,
    });
  return object;
}

function pathFromPointer(pointer: string, model: unknown): (string | number)[] {
  const path: (string | number)[] = [];
  let current = model;
  for (const encoded of pointer.split("/").slice(1)) {
    const decoded = encoded.replace(/~1/g, "/").replace(/~0/g, "~");
    const key = Array.isArray(current) ? Number(decoded) : decoded;
    path.push(key);
    current = getAtPath(current, [key]);
  }
  return path;
}

function schemaIssue(error: ErrorObject, value: unknown): PropertyIssue {
  const path = pathFromPointer(error.instancePath, value);
  if (error.keyword === "required") path.push(String(error.params.missingProperty));
  return {
    path,
    keyword: error.keyword,
    message: error.keyword === "required" ? "This field is required" : error.message ?? "Invalid value",
  };
}

function empty(value: unknown): boolean {
  return value === undefined || value === null || value === "" || (Array.isArray(value) && value.length === 0);
}

function validValueRule(rule: ValueRule, value: unknown): boolean {
  if (empty(value)) return true;
  const text = String(value).trim();
  if (rule.enum) return rule.enum.includes(text);
  if (rule.pattern) {
    try {
      return new RegExp(rule.pattern).test(String(value));
    } catch {
      return false;
    }
  }
  if (rule.type === "integer" && !/^[-+]?\d+$/.test(text)) return false;
  if (rule.type === "number" && !(text.length > 0 && Number.isFinite(Number(text)))) return false;
  const number = Number(text);
  return (
    (rule.minimum === undefined || number >= rule.minimum) &&
    (rule.exclusiveMinimum === undefined || number > rule.exclusiveMinimum)
  );
}

function valueRuleMessage(rule: ValueRule): string {
  if (rule.enum) return `must be one of ${rule.enum.join(", ")}`;
  if (rule.pattern)
    return rule.examples?.length
      ? `must match a value such as ${rule.examples[0]}`
      : "is not a value this parameter takes";
  const kind = rule.type === "integer" ? "a whole number" : "a number";
  if (rule.minimum !== undefined) return `must be ${kind} of at least ${rule.minimum}`;
  if (rule.exclusiveMinimum !== undefined) return `must be ${kind} greater than ${rule.exclusiveMinimum}`;
  return `must be ${kind}`;
}

function customIssues(
  schema: OperatorPropertySchema,
  value: unknown,
  path: PropertyPath,
  issues: PropertyIssue[],
  parent?: unknown,
  root = schema
): void {
  schema = resolveSchemaReference(schema, root);
  const rule = matchingValueRule(schema, parent);
  if (rule && !validValueRule(rule, value))
    issues.push({ path, keyword: "valueRules", message: valueRuleMessage(rule) });
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const object = value as Record<string, unknown>;
    for (const required of schema.required ?? []) {
      if (empty(object[required]))
        issues.push({ path: [...path, required], keyword: "required", message: "This field is required" });
    }
    for (const [key, property] of sortedProperties(schema))
      customIssues(property, object[key], [...path, key], issues, object, root);
  } else if (Array.isArray(value)) {
    value.forEach((item, index) =>
      customIssues(itemSchema(schema, index), item, [...path, index], issues, value, root)
    );
    const rowSchema = itemSchema(schema, 0);
    for (const [key, property] of sortedProperties(rowSchema)) {
      if (!property.uniqueAmongRows) continue;
      value.forEach((row, index) => {
        const fieldValue = getAtPath(row, [key]);
        if (empty(fieldValue)) return;
        if (value.some((other, otherIndex) => otherIndex !== index && getAtPath(other, [key]) === fieldValue)) {
          issues.push({
            path: [...path, index, key],
            keyword: "uniqueAmongRows",
            message: "must be unique among rows",
          });
        }
      });
    }
  }
  for (const branch of schema.allOf ?? []) customIssues(asSchema(branch), value, path, issues, parent, root);
  if (schema.if) {
    const validateCondition = ajv.compile(schema.if);
    const branch = validateCondition(value) ? schema.then : schema.else;
    if (branch) customIssues(asSchema(branch), value, path, issues, parent, root);
  }
}

/** Standard JSON-schema errors and Texera's sibling/row validation share field paths. */
export function validateSchemaValue(schema: OperatorPropertySchema, value: unknown): PropertyIssue[] {
  try {
    let validate = validators.get(schema);
    if (!validate) {
      validate = ajv.compile(schema);
      validators.set(schema, validate);
    }
    validate(value);
    const issues = (validate.errors ?? []).map(error => schemaIssue(error, value));
    customIssues(schema, value, [], issues);
    return issues.filter(
      (issue, index) =>
        issues.findIndex(
          other => other.keyword === issue.keyword && JSON.stringify(other.path) === JSON.stringify(issue.path)
        ) === index
    );
  } catch (error) {
    return [
      {
        path: [],
        keyword: "schema",
        message: `Invalid property schema: ${error instanceof Error ? error.message : String(error)}`,
      },
    ];
  }
}
