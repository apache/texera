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

import type {
  MacroDefinition,
  MacroInterface,
  OperatorPredicate as CoreOperator,
  OperatorLink,
  PortDescription,
} from "@texera/workflow-macros";
export type { OperatorLink, PortDescription } from "@texera/workflow-macros";

interface PortIdentity {
  readonly id: number;
  readonly internal: boolean;
}

export interface OperatorPredicate extends CoreOperator {
  readonly operatorProperties: Record<string, any>;
}

export interface LogicalOperator {
  readonly operatorID: string;
  readonly operatorType: string;
  readonly [key: string]: any;
}

export interface LogicalLink {
  readonly fromOpId: string;
  readonly fromPortId: PortIdentity;
  readonly toOpId: string;
  readonly toPortId: PortIdentity;
}

export interface LogicalPlan {
  readonly operators: LogicalOperator[];
  readonly links: LogicalLink[];
  readonly opsToViewResult?: string[];
  readonly opsToReuseResult?: string[];
}

export interface Point {
  readonly x: number;
  readonly y: number;
}

export interface CommentBox {
  readonly commentBoxID: string;
  readonly comments: string;
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}

export interface WorkflowSettings {
  readonly dataTransferBatchSize: number;
}

export interface WorkflowContent {
  readonly operators: OperatorPredicate[];
  readonly operatorPositions: { [key: string]: Point };
  readonly links: OperatorLink[];
  readonly commentBoxes: CommentBox[];
  readonly settings: WorkflowSettings;
  // The Form View definition (which properties are exposed as form fields). The agent
  // never reads or edits it, but it lives in workflow.content and must survive an agent
  // save untouched -- rebuilding content without it would leave a workflow whose default_view
  // is form with a form that has no fields. Carried through opaquely.
  readonly formBinding?: unknown;
  readonly macroInterface?: MacroInterface;
  readonly macroDependencies?: MacroDefinition<WorkflowContent>[];
}

type AttributeType = "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary";

export interface SchemaAttribute {
  readonly attributeName: string;
  readonly attributeType: AttributeType;
}

export type PortSchema = readonly SchemaAttribute[];

export type OperatorPortSchemaMap = Record<string, PortSchema | undefined>;

export interface OperatorDetail {
  operatorId: string;
  operatorType: string;
  customDisplayName?: string;
  operatorProperties: Record<string, any>;
  inputPorts: PortDescription[];
  outputPorts: PortDescription[];
}

export type ValidationError = {
  isValid: false;
  messages: Record<string, string>;
};

export type Validation = { isValid: true } | ValidationError;
