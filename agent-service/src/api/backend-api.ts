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

/**
 * Backend API client for Texera Agent Service.
 *
 * Configuration priority (highest to lowest):
 * 1. Environment variables (API_ENDPOINT, MODELS_ENDPOINT, etc.)
 * 2. Config file (config/backend.config.json)
 * 3. Default values (localhost with standard ports)
 */

import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

interface BackendConfig {
  apiEndpoint: string;
  operatorMetadataEndpoint: string;
  modelsEndpoint: string;
  compileEndpoint: string;
  executionEndpoint: string;
  wsEndpoint: string;
  datasetEndpoint: string;
  computingEndpoint: string;
  configEndpoint: string;
}

interface ConfigFileService {
  description: string;
  target: string;
  endpoints: string[];
}

interface ConfigFile {
  services: Record<string, ConfigFileService>;
  defaults?: {
    secure?: boolean;
    changeOrigin?: boolean;
  };
}

function loadConfigFile(): Partial<BackendConfig> {
  try {
    const possiblePaths = [
      join(process.cwd(), "config", "backend.config.json"),
      join(dirname(fileURLToPath(import.meta.url)), "..", "..", "config", "backend.config.json"),
    ];

    for (const configPath of possiblePaths) {
      if (existsSync(configPath)) {
        const configData = readFileSync(configPath, "utf-8");
        const config: ConfigFile = JSON.parse(configData);

        return {
          apiEndpoint: config.services.main?.target,
          operatorMetadataEndpoint: config.services.main?.target,
          modelsEndpoint: config.services.models?.target,
          compileEndpoint: config.services.compile?.target,
          executionEndpoint:
            config.services.execution?.target || config.services.websocket?.target?.replace("ws://", "http://"),
          wsEndpoint: config.services.websocket?.target,
          datasetEndpoint: config.services.dataset?.target,
          computingEndpoint: config.services.computing?.target,
          configEndpoint: config.services.config?.target,
        };
      }
    }
  } catch (error) {
    console.warn("[BackendAPI] Failed to load config file:", error);
  }
  return {};
}

const fileConfig = loadConfigFile();

const currentConfig: BackendConfig = {
  apiEndpoint: process.env.API_ENDPOINT || fileConfig.apiEndpoint || "http://localhost:8080",
  operatorMetadataEndpoint:
    process.env.OPERATOR_METADATA_ENDPOINT || fileConfig.operatorMetadataEndpoint || "http://localhost:8080",
  modelsEndpoint: process.env.MODELS_ENDPOINT || fileConfig.modelsEndpoint || "http://localhost:9096",
  compileEndpoint: process.env.COMPILE_ENDPOINT || fileConfig.compileEndpoint || "http://localhost:9090",
  executionEndpoint: process.env.EXECUTION_ENDPOINT || fileConfig.executionEndpoint || "http://localhost:8085",
  wsEndpoint: process.env.WS_ENDPOINT || fileConfig.wsEndpoint || "ws://localhost:8085",
  datasetEndpoint: process.env.DATASET_ENDPOINT || fileConfig.datasetEndpoint || "http://localhost:9092",
  computingEndpoint: process.env.COMPUTING_ENDPOINT || fileConfig.computingEndpoint || "http://localhost:8888",
  configEndpoint: process.env.CONFIG_ENDPOINT || fileConfig.configEndpoint || "http://localhost:9094",
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
  const url = `${currentConfig.operatorMetadataEndpoint}/api/resources/operator-metadata`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch operator metadata: ${response.status} ${response.statusText}`);
  }

  return response.json();
}
