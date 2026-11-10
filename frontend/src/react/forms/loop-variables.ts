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

import type { OperatorPropertySchema } from "./schema";
import { resolveOperatorWidget } from "./widget-metadata";

export type PrimitiveSchemaType = "string" | "number" | "integer" | "boolean";
const primitiveTypes = new Set(["string", "number", "integer", "boolean"]);
const referencePattern = /^\$[A-Za-z_][A-Za-z0-9_]*$/;
const integerPattern = /^[+-]?\d+$/;
const numberPattern = /^[+-]?(\d+(\.\d+)?|\.\d+)([eE][+-]?\d+)?$/;

export function isReference(value: unknown): boolean {
  return typeof value === "string" && referencePattern.test(value);
}

export function primitiveSchemaType(schema: OperatorPropertySchema): PrimitiveSchemaType | undefined {
  const types = (Array.isArray(schema.type) ? schema.type : [schema.type]).filter(type => type !== "null");
  return types.length === 1 && types[0] !== undefined && primitiveTypes.has(types[0])
    ? (types[0] as PrimitiveSchemaType)
    : undefined;
}

export function coerceOrReference(value: unknown, type: PrimitiveSchemaType): unknown {
  if (typeof value !== "string" || type === "string") return value;
  const trimmed = value.trim();
  if (isReference(trimmed)) return trimmed;
  if (trimmed === "") return undefined;
  if (type === "integer") return integerPattern.test(trimmed) ? Number(trimmed) : value;
  if (type === "number") {
    const parsed = numberPattern.test(trimmed) ? Number(trimmed) : NaN;
    return Number.isFinite(parsed) ? parsed : value;
  }
  const lower = trimmed.toLowerCase();
  return lower === "true" ? true : lower === "false" ? false : value;
}

export function unknownReferenceWarning(value: unknown, names: readonly string[]): string | undefined {
  return isReference(value) && !names.includes(String(value).slice(1))
    ? `${value} is not a variable of an enclosing block; the run will fail if no Loop Start sets it`
    : undefined;
}

export function acceptsLoopReference(
  schema: OperatorPropertySchema,
  key: unknown,
  operatorType?: string,
  blocked = false
): boolean {
  return (
    !blocked &&
    schema.noLoopVariable !== true &&
    schema.enum === undefined &&
    primitiveSchemaType(schema) !== undefined &&
    resolveOperatorWidget(schema, key, operatorType) === undefined
  );
}
