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

import contract from "../../../../../../common/workflow-operator/src/main/resources/udf/ui-parameter-types.json";
import type { AttributeType, SchemaAttribute } from "../../types/workflow-compiling.interface";

export type UiUdfParameter = Readonly<{ attribute: SchemaAttribute; value: string; inputType?: string }>;
export type UiUdfLanguage = "python" | "r";
export class UiUdfParametersParseError extends Error {}
export class UiUdfParametersEditError extends Error {}

/** Shared by declaration parsers, insertion, and the parameter editor. */
export const UI_PARAMETER_TYPES = Object.keys(contract.types) as AttributeType[];
export const ATTRIBUTE_TYPES_BY_TOKEN: Readonly<Record<string, AttributeType>> = Object.fromEntries(
  Object.entries(contract.types).flatMap(([type, aliases]) => aliases.map(alias => [alias, type as AttributeType]))
);
export const UI_PARAMETER_TOKENS: Readonly<Partial<Record<AttributeType, string>>> = Object.fromEntries(
  Object.entries(contract.types).map(([type, aliases]) => [type, aliases[0]])
);
export const INPUT_TYPES_BY_RESOURCE_TOKEN: Readonly<Record<string, string>> = contract.resources;
export const MODEL_INPUT_TYPE = contract.resources.MODEL;
export const DATASET_INPUT_TYPE = contract.resources.DATASET;

export function createUiParameter(
  attributeName: string | undefined,
  attributeType: AttributeType | undefined,
  inputType?: string
): UiUdfParameter | undefined {
  if (
    !attributeName ||
    !attributeType ||
    !UI_PARAMETER_TYPES.includes(attributeType) ||
    (inputType && (!Object.values(INPUT_TYPES_BY_RESOURCE_TOKEN).includes(inputType) || attributeType !== "string"))
  )
    return undefined;
  return { attribute: { attributeName, attributeType }, value: "", ...(inputType ? { inputType } : {}) };
}

export function assertUniqueParameterNames(parameters: UiUdfParameter[]): UiUdfParameter[] {
  const names = new Set<string>();
  for (const parameter of parameters) {
    const name = parameter.attribute.attributeName;
    if (names.has(name)) throw new UiUdfParametersParseError(`UiParameter name '${name}' is declared more than once.`);
    names.add(name);
  }
  return parameters;
}
