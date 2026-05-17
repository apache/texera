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

export type CustomOperatorPropertyType = "string" | "number" | "boolean" | "select";

export interface CustomOperatorPort {
  name: string;
  /** Free-form label for the port type (informational only — Texera ports are untyped). */
  type: string;
}

export interface CustomOperatorProperty {
  name: string;
  type: CustomOperatorPropertyType;
  default: string | number | boolean;
  description: string;
  /** Only meaningful when type === "select". */
  options?: string[];
}

export interface CustomOperator {
  id: string;
  name: string;
  description: string;
  /** Emoji used to visually identify the operator. */
  icon: string;
  /** Category name shown in the operator panel ("My Operators" by default). */
  category: string;
  author: string;
  code: string;
  language: "python";
  inputPorts: CustomOperatorPort[];
  outputPorts: CustomOperatorPort[];
  properties: CustomOperatorProperty[];
  isPublic: boolean;
  createdAt: string;
  updatedAt: string;
}

export const DEFAULT_CUSTOM_OPERATOR_CATEGORY = "My Operators";

export const DEFAULT_CUSTOM_OPERATOR_CODE = `from pytexera import *

class ProcessTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        # 'table' is a pandas DataFrame.
        # Access properties via self.args['property_name'].
        df = table
        # Example: add a new column
        # df['new_col'] = df['existing_col'] * 2
        yield df
`;

export const CUSTOM_OPERATOR_PROPERTY_TYPE_OPTIONS: { value: CustomOperatorPropertyType; label: string }[] = [
  { value: "string", label: "Text" },
  { value: "number", label: "Number" },
  { value: "boolean", label: "Boolean" },
  { value: "select", label: "Select" },
];

/**
 * Operator-type prefix used when injecting a custom operator into a workflow.
 * The actual operator instantiated in the graph is still a Python UDF; this prefix
 * is only used to distinguish the synthetic operator schema served from the
 * frontend custom-operator registry.
 */
export const CUSTOM_OPERATOR_TYPE_PREFIX = "CustomOp__";
