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

import { ExecutionMode, FormBindingConfig, WorkflowContent } from "../type/workflow";
import { OperatorLink, OperatorPredicate } from "../../workspace/types/workflow-common.interface";

export const testOperator = (operatorID: string, extra: Partial<OperatorPredicate> = {}): OperatorPredicate => ({
  operatorID,
  operatorType: "Filter",
  operatorProperties: {},
  operatorVersion: "1",
  showAdvanced: false,
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  ...extra,
});

export const testLink = (
  source: string,
  target: string,
  output = "output-0",
  input = "input-0",
  linkID = `${source}-${target}`
): OperatorLink => ({
  linkID,
  source: { operatorID: source, portID: output },
  target: { operatorID: target, portID: input },
});

export function testWorkflow<C extends Partial<WorkflowContent>>(
  operators: OperatorPredicate[],
  links: OperatorLink[],
  extra: C
): WorkflowContent & C;
export function testWorkflow(operators?: OperatorPredicate[], links?: OperatorLink[]): WorkflowContent;
export function testWorkflow(
  operators: OperatorPredicate[] = [],
  links: OperatorLink[] = [],
  extra: Partial<WorkflowContent> = {}
): WorkflowContent {
  return {
    operators,
    links,
    operatorPositions: Object.fromEntries(operators.map(op => [op.operatorID, { x: 0, y: 0 }])),
    commentBoxes: [],
    settings: { dataTransferBatchSize: 10, executionMode: ExecutionMode.PIPELINED },
    ...extra,
  };
}

export const testField = (operatorID: string, propertyKey: string, id = propertyKey, displayName = propertyKey) => ({
  id,
  operatorID,
  propertyKey,
  displayName,
});

export const testBinding = (...args: Parameters<typeof testField>): FormBindingConfig => ({
  fields: [testField(...args)],
});
