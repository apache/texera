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

import Ajv, { type ErrorObject } from "ajv";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
import type { WorkflowGraphReadonly } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import type { OperatorPortSchemaMap } from "../../core/workspace/types/workflow-compiling.interface";
import { primitiveSchemaType, isReference, valueAtPointer } from "../../core/workspace/util/loop-variable-field.util";
import { loopVariablesInScope } from "../../core/workspace/util/loop-variable.util";
import { inputSchemasForOperator, restoreOperatorInputAttrs, setOperatorInputAttrs } from "../forms/schema-propagation";
import type { OperatorPropertySchema } from "../forms/schema";

export type ValidationError = { isValid: false; messages: Record<string, string> };
export type Validation = { isValid: true } | ValidationError;
export interface WorkflowRunValidation {
  isValid: boolean;
  workflowEmpty: boolean;
  errors: Record<string, ValidationError>;
}
export interface WorkflowRunValidationOptions {
  graph: WorkflowGraphReadonly;
  metadata: OperatorMetadata;
  outputSchemas?: Readonly<Record<string, OperatorPortSchemaMap>>;
  schemaForOperator?: (operator: OperatorPredicate) => OperatorPropertySchema | undefined;
}
const ajv = new Ajv({ allErrors: true, strict: false });

/** Retains the original validation merge contract: later messages win for the same keyword. */
export function combineValidation(...validations: Validation[]): Validation {
  let isValid = true;
  let messages: Record<string, string> = {};
  for (const validation of validations) {
    isValid = isValid && validation.isValid;
    if (!validation.isValid) messages = { ...messages, ...validation.messages };
  }
  return isValid ? { isValid: true } : { isValid: false, messages };
}

/** Pure run preflight. Disabled nodes are valid, and only enabled links satisfy input ports. */
export function validateWorkflowForRun({
  graph,
  metadata,
  outputSchemas,
  schemaForOperator,
}: WorkflowRunValidationOptions): WorkflowRunValidation {
  const errors: Record<string, ValidationError> = {};
  const operators = graph.getAllOperators();
  const workflowEmpty = !operators.some(operator => !graph.isOperatorDisabled(operator.operatorID));
  for (const operator of operators) {
    const operatorId = operator.operatorID;
    if (graph.isOperatorDisabled(operatorId)) continue;
    const base =
      metadata.operators.find(
        schema => schema.operatorType === operator.operatorType && schema.operatorVersion === operator.operatorVersion
      ) ?? metadata.operators.find(schema => schema.operatorType === operator.operatorType);
    const messages: Record<string, string> = {};
    if (!base) messages.schema = `Operator metadata is unavailable for ${operator.operatorType}.`;
    else
      try {
        const input = inputSchemasForOperator({
          operatorId,
          graph,
          dynamicSchema: base,
          outputSchemas: outputSchemas ?? {},
        });
        let schema = schemaForOperator?.(operator);
        if (!schema) {
          const dynamic = input.inputSchemas
            ? setOperatorInputAttrs(base, input.inputSchemas)
            : base.additionalMetadata.inputPorts.length
              ? restoreOperatorInputAttrs(base)
              : base;
          schema = dynamic.jsonSchema as OperatorPropertySchema;
        }
        let schemaErrors: ErrorObject[];
        try {
          const valid = ajv.validate(schema, operator.operatorProperties);
          schemaErrors = valid ? [] : ajv.errors ?? [];
        } finally {
          // Dynamic input schemas are short-lived; AJV otherwise retains each editing revision.
          ajv.removeSchema(schema);
        }
        const atReference = (error: ErrorObject) =>
          error.keyword === "type" &&
          primitiveSchemaType(error.params.type) !== undefined &&
          isReference(valueAtPointer(operator.operatorProperties, error.instancePath));
        if (schemaErrors.some(atReference) && loopVariablesInScope(graph, operatorId) !== undefined)
          schemaErrors = schemaErrors.filter(error => !atReference(error));
        for (const error of schemaErrors) messages[error.keyword] = error.message ?? "";
        if (outputSchemas && input.errors.length)
          messages.inputSchema = input.errors.map(error => error.message).join("; ");
      } catch (error) {
        messages.schema = `Invalid operator schema: ${error instanceof Error ? error.message : "unknown schema error"}`;
      }
    const counts = new Map<string, number>();
    for (const link of graph.getInputLinksByOperatorId(operatorId)) {
      if (graph.isLinkEnabled(link.linkID)) counts.set(link.target.portID, (counts.get(link.target.portID) ?? 0) + 1);
    }
    let inputMessage = "";
    for (const port of operator.inputPorts) {
      const count = counts.get(port.portID) ?? 0;
      if (port.disallowMultiInputs ? count !== 1 : count < 1)
        inputMessage += port.disallowMultiInputs
          ? `${port.displayName ?? ""} requires 1 input, has ${count}`
          : `${port.displayName ?? ""} requires at least 1 inputs, has ${count}`;
    }
    if (inputMessage) messages.inputs = inputMessage;
    if (Object.keys(messages).length) errors[operatorId] = { isValid: false, messages };
  }
  return { isValid: !workflowEmpty && !Object.keys(errors).length, workflowEmpty, errors };
}
