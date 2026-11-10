/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Ajv from "ajv";
import { v4 as uuid } from "uuid";
import type { OperatorMetadata, OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";

export interface OperatorFactoryOptions {
  customDisplayName?: string;
  uuid?: () => string;
}
export function createOperator(schema: OperatorSchema, options: OperatorFactoryOptions = {}): OperatorPredicate {
  const properties: Record<string, unknown> = {};
  const ajv = new Ajv({ useDefaults: true, strict: false });
  ajv.compile({ ...schema.jsonSchema })(properties);
  const metadata = schema.additionalMetadata;
  return {
    operatorID: `${schema.operatorType}-operator-${(options.uuid ?? uuid)()}`,
    operatorType: schema.operatorType,
    operatorVersion: schema.operatorVersion,
    operatorProperties: properties,
    inputPorts: metadata.inputPorts.map((port, index) => ({
      portID: `input-${index}`,
      displayName: port.displayName ?? "",
      disallowMultiInputs: port.disallowMultiLinks ?? false,
      isDynamicPort: false,
      dependencies: port.dependencies ?? [],
    })),
    outputPorts: metadata.outputPorts.map((port, index) => ({
      portID: `output-${index}`,
      displayName: port.displayName ?? "",
      disallowMultiInputs: false,
      isDynamicPort: false,
    })),
    showAdvanced: false,
    isDisabled: false,
    customDisplayName: options.customDisplayName ?? metadata.userFriendlyName,
    dynamicInputPorts: metadata.dynamicInputPorts ?? false,
    dynamicOutputPorts: metadata.dynamicOutputPorts ?? false,
  };
}

export function createOperatorFactory(
  metadata: OperatorMetadata | readonly OperatorSchema[],
  options: Omit<OperatorFactoryOptions, "customDisplayName"> = {}
) {
  const schemas = "operators" in metadata ? metadata.operators : metadata;
  return {
    getNewOperatorPredicate(operatorType: string, customDisplayName?: string): OperatorPredicate {
      const schema = schemas.find(schema => schema.operatorType === operatorType);
      if (!schema) throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
      return createOperator(schema, { ...options, customDisplayName });
    },
  };
}
