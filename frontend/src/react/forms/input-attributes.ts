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

import type { OperatorPropertySchema, PropertyIssue, PropertyPath, PropertyValidationContext } from "./schema";
import { resolveSchemaReference } from "./schema";

type InputAttributes = PropertyValidationContext["inputAttributes"];
const transformed = new WeakMap<
  OperatorPropertySchema,
  WeakMap<NonNullable<InputAttributes>, OperatorPropertySchema>
>();
function portAttributes(attributes: InputAttributes, port: number | undefined) {
  if (port === undefined || !Number.isInteger(port)) return undefined;
  return attributes?.[String(port)] ?? attributes?.[`${port}_false`];
}

/** Propagated column choices retain backend property annotations and optional empty/default choices. */
export function applyInputAttributeSchema(
  schema: OperatorPropertySchema,
  attributes: InputAttributes
): OperatorPropertySchema {
  if (!attributes || !Object.keys(attributes).length) return schema;
  const existing = transformed.get(schema)?.get(attributes);
  if (existing) return existing;
  const walk = (node: unknown, key?: string): unknown => {
    if (Array.isArray(node)) return node.map(item => walk(item));
    if (!node || typeof node !== "object") return node;
    const input = node as OperatorPropertySchema;
    const copy: Record<string, unknown> = { ...input };
    for (const keyword of ["properties", "definitions", "patternProperties"] as const) {
      const members = input[keyword];
      if (members && typeof members === "object")
        copy[keyword] = Object.fromEntries(
          Object.entries(members).map(([name, property]) => [name, walk(property, name)])
        );
    }
    for (const keyword of [
      "items",
      "additionalItems",
      "additionalProperties",
      "allOf",
      "anyOf",
      "oneOf",
      "if",
      "then",
      "else",
      "not",
    ] as const) {
      if (input[keyword] !== undefined) copy[keyword] = walk(input[keyword]);
    }
    if (input.autofill === "attributeName" || input.autofill === "attributeNameList") {
      const choices = portAttributes(attributes, input.autofillAttributeOnPort)?.map(
        attribute => attribute.attributeName
      );
      if (choices) {
        if (input.additionalEnumValue) choices.push(input.additionalEnumValue);
        if (!schema.required?.includes(key ?? "")) {
          if (input.default && typeof input.default !== "string") throw new Error("default value must be a string");
          choices.push(input.default ? (input.default as string) : "");
        }
      }
      if (input.autofill === "attributeName") {
        copy.type = "string";
        copy.enum = choices;
        copy.uniqueItems = true;
      } else {
        copy.type = "array";
        copy.uniqueItems = true;
        copy.items = {
          ...(copy.items && typeof copy.items === "object" ? copy.items : {}),
          type: "string",
          enum: choices,
        };
      }
    }
    return copy;
  };
  const result = walk(schema) as OperatorPropertySchema;
  let cache = transformed.get(schema);
  if (!cache) {
    cache = new WeakMap();
    transformed.set(schema, cache);
  }
  cache.set(attributes, result);
  return result;
}

export function attributeTypeIssues(
  schema: OperatorPropertySchema,
  value: unknown,
  attributes: InputAttributes,
  path: PropertyPath = [],
  root = schema
): PropertyIssue[] {
  schema = resolveSchemaReference(schema, root);
  const issues: PropertyIssue[] = [];
  if (!attributes || value === null || typeof value !== "object") return issues;
  if (Array.isArray(value)) {
    const item = !Array.isArray(schema.items) && typeof schema.items === "object" ? schema.items : {};
    return value.flatMap((row, index) => attributeTypeIssues(item, row, attributes, [...path, index], root));
  }
  const object = value as Record<string, unknown>;
  const names = (key: string): string[] =>
    Array.isArray(object[key])
      ? (object[key] as unknown[]).filter((name): name is string => typeof name === "string")
      : typeof object[key] === "string"
        ? [object[key] as string]
        : [];
  const typeOf = (key: string, name: string) => {
    const property = schema.properties?.[key];
    return property && typeof property === "object"
      ? portAttributes(attributes, property.autofillAttributeOnPort)?.find(
          attribute => attribute.attributeName === name
        )?.attributeType
      : undefined;
  };
  for (const [key, rule] of Object.entries(schema.attributeTypeRules ?? {})) {
    for (const name of names(key)) {
      const actual = typeOf(key, name);
      if (!actual) continue;
      let expected: string | undefined;
      if (rule.enum && !rule.enum.includes(actual)) expected = `it's expected to be ${rule.enum.join(" or ")}.`;
      const comparedKey = rule.const?.$data;
      if (comparedKey) {
        const comparedName = names(comparedKey)[0];
        const comparedType = comparedName ? typeOf(comparedKey, comparedName) : undefined;
        if (comparedType && comparedType !== actual)
          expected = `it's expected to be the same type as '${comparedName}' (${comparedType}).`;
      }
      for (const branch of rule.allOf ?? []) {
        if (
          Object.entries(branch.if).every(([sibling, condition]) =>
            condition.valEnum?.includes(String(object[sibling]))
          )
        ) {
          if (branch.then.enum && !branch.then.enum.includes(actual))
            expected = `it's expected to be ${branch.then.enum.join(" or ")}, given that ${Object.keys(branch.if)
              .map(sibling => `'${sibling}' is ${object[sibling]}`)
              .join(" and ")}`;
        }
      }
      if (expected)
        issues.push({
          path: [...path, key],
          keyword: "attributeTypeRules",
          message: `Warning: The type of '${name}' is ${actual}, but ${expected}`,
        });
    }
  }
  for (const [key, property] of Object.entries(schema.properties ?? {})) {
    if (typeof property === "object")
      issues.push(...attributeTypeIssues(property, object[key], attributes, [...path, key], root));
  }
  return issues;
}
