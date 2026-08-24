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

import { JSONSchema7, JSONSchema7Definition } from "json-schema";

export const hideTypes = ["regex", "equals"] as const;
export type HideType = (typeof hideTypes)[number];

export type AttributeTypeEnumRule = ReadonlyArray<string>;
export type AttributeTypeConstRule = Readonly<{
  $data?: string;
}>;
export type AttributeTypeAllOfRule = ReadonlyArray<{
  if: {
    [key: string]: {
      valEnum?: string[];
    };
  };
  then: {
    enum?: AttributeTypeEnumRule;
  };
}>;
export type AttributeTypeRuleSet = Readonly<{
  enum?: AttributeTypeEnumRule;
  const?: AttributeTypeConstRule;
  allOf?: AttributeTypeAllOfRule;
}>;

export type AttributeTypeRuleSchema = Readonly<{
  [key: string]: AttributeTypeRuleSet;
}>;

/**
 * What one field may hold, given what a sibling holds. Borrows `attributeTypeRules`' grammar
 * and, like it, sits under a key of Texera's own rather than as a JSON-Schema `allOf`: the
 * form builder merges the members of an `allOf` into a single field, which would leave one
 * control carrying every branch's constraints at once.
 */
export type ValueRuleSet = Readonly<{
  allOf: ReadonlyArray<{
    if: {
      [siblingField: string]: {
        valEnum?: string[];
      };
    };
    then: {
      // the accepted set, where the value is chosen from one
      enum?: ReadonlyArray<string>;
      // otherwise how the value is read, in JSON Schema's names
      type?: "number" | "integer";
      // or, where the value is a choice between a set and a number and no type names it,
      // the shape it takes
      pattern?: string;
      // a value that is accepted, for a reader that has to supply one; the form does not
      // render it, the same as everywhere else `examples` is declared
      examples?: ReadonlyArray<string>;
    };
  }>;
}>;

export interface CustomJSONSchema7 extends JSONSchema7 {
  propertyOrder?: number;
  properties?: {
    [key: string]: CustomJSONSchema7 | boolean;
  };
  items?: CustomJSONSchema7 | boolean | JSONSchema7Definition[];

  // new custom properties:
  autofill?: "attributeName" | "attributeNameList";
  autofillAttributeOnPort?: number;
  attributeTypeRules?: AttributeTypeRuleSchema;
  valueRules?: ValueRuleSet;

  "enable-presets"?: boolean; // include property in schema of preset

  dependOn?: string;
  toggleHidden?: string[]; // the field names which will be toggle hidden or not by this field.

  hideExpectedValue?: string;
  hideTarget?: string;
  hideType?: HideType;
  hideOnNull?: boolean;

  additionalEnumValue?: string;
}
