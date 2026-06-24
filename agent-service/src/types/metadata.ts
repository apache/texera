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

// Operator metadata shapes served by the Dashboard Service
// (`/api/resources/operator-metadata`) and the compact variants the agent
// derives from them for prompts and validation.

export interface InputPortInfo {
  displayName?: string;
  disallowMultiLinks?: boolean;
  dependencies?: { id: number; internal: boolean }[];
}

export interface OutputPortInfo {
  displayName?: string;
}

export interface OperatorAdditionalMetadata {
  userFriendlyName: string;
  operatorGroupName: string;
  operatorDescription?: string;
  inputPorts: InputPortInfo[];
  outputPorts: OutputPortInfo[];
  dynamicInputPorts?: boolean;
  dynamicOutputPorts?: boolean;
  supportReconfiguration?: boolean;
  allowPortCustomization?: boolean;
}

export interface OperatorSchema {
  operatorType: string;
  jsonSchema: Record<string, unknown>;
  additionalMetadata: OperatorAdditionalMetadata;
  operatorVersion: string;
}

export interface GroupInfo {
  groupName: string;
  children?: GroupInfo[] | null;
}

export interface OperatorMetadata {
  operators: OperatorSchema[];
  groups: GroupInfo[];
}

/** Full per-operator schema slice surfaced to debugging/inspection callers. */
export interface OperatorSchemaInfo {
  properties: Record<string, unknown>;
  required: string[];
  definitions: Record<string, unknown>;
}

/** Reduced operator schema (refs inlined, noise stripped) used in prompts and errors. */
export interface CompactOperatorSchema {
  properties: Record<string, unknown>;
  required: string[];
}
