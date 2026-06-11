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

import { env } from "../config/env";

interface BackendConfig {
  apiEndpoint: string;
  modelsEndpoint: string;
  compileEndpoint: string;
  executionEndpoint: string;
}

const currentConfig: BackendConfig = {
  apiEndpoint: env.TEXERA_DASHBOARD_SERVICE_ENDPOINT,
  modelsEndpoint: env.LLM_ENDPOINT,
  compileEndpoint: env.WORKFLOW_COMPILING_SERVICE_ENDPOINT,
  executionEndpoint: env.WORKFLOW_EXECUTION_SERVICE_ENDPOINT,
};

export function getBackendConfig(): BackendConfig {
  return { ...currentConfig };
}

export interface InputPortInfo {
  displayName?: string;
  disallowMultiLinks?: boolean;
  dependencies?: { id: number; internal: boolean }[];
}

export interface OutputPortInfo {
  displayName?: string;
}

interface OperatorAdditionalMetadata {
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
  jsonSchema: any;
  additionalMetadata: OperatorAdditionalMetadata;
  operatorVersion: string;
}

interface GroupInfo {
  groupName: string;
  children?: GroupInfo[] | null;
}

export interface OperatorMetadata {
  operators: OperatorSchema[];
  groups: GroupInfo[];
}

export async function fetchOperatorMetadata(): Promise<OperatorMetadata> {
  const url = `${currentConfig.apiEndpoint}/api/resources/operator-metadata`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch operator metadata: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as OperatorMetadata;
}
