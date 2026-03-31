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
 * Provides functions to interact with various Texera backend services.
 *
 * Configuration priority (highest to lowest):
 * 1. Environment variables (API_ENDPOINT, MODELS_ENDPOINT, etc.)
 * 2. Config file (config/backend.config.json)
 * 3. Default values (localhost with standard ports)
 */

import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

// ============================================================================
// Configuration
// ============================================================================

export interface BackendConfig {
  /** Main API endpoint (default: http://localhost:8080) */
  apiEndpoint: string;
  /** Operator metadata endpoint (default: http://localhost:8080) */
  operatorMetadataEndpoint: string;
  /** Models API endpoint (default: http://localhost:9096) */
  modelsEndpoint: string;
  /** Compile service endpoint (default: http://localhost:9090) */
  compileEndpoint: string;
  /** Execution service endpoint (default: http://localhost:8085) */
  executionEndpoint: string;
  /** WebSocket endpoint (default: ws://localhost:8085) */
  wsEndpoint: string;
  /** Dataset service endpoint (default: http://localhost:9092) */
  datasetEndpoint: string;
  /** Computing unit service endpoint (default: http://localhost:8888) */
  computingEndpoint: string;
  /** Config service endpoint (default: http://localhost:9094) */
  configEndpoint: string;
  /** Hamilton sidecar endpoint (default: http://localhost:8111) */
  hamiltonEndpoint: string;
  /** Dagster sidecar endpoint (default: http://localhost:8112) */
  dagsterEndpoint: string;
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

/**
 * Load configuration from config/backend.config.json if it exists.
 */
function loadConfigFile(): Partial<BackendConfig> {
  try {
    // Try to find config file relative to this module
    const possiblePaths = [
      join(process.cwd(), "config", "backend.config.json"),
      join(dirname(fileURLToPath(import.meta.url)), "..", "..", "config", "backend.config.json"),
    ];

    for (const configPath of possiblePaths) {
      if (existsSync(configPath)) {
        const configData = readFileSync(configPath, "utf-8");
        const config: ConfigFile = JSON.parse(configData);

        // Map services to BackendConfig
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
          hamiltonEndpoint: config.services.hamilton?.target,
          dagsterEndpoint: config.services.dagster?.target,
        };
      }
    }
  } catch (error) {
    console.warn("[BackendAPI] Failed to load config file:", error);
  }
  return {};
}

const fileConfig = loadConfigFile();

const DEFAULT_CONFIG: BackendConfig = {
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
  hamiltonEndpoint: process.env.HAMILTON_ENDPOINT || fileConfig.hamiltonEndpoint || "http://localhost:8111",
  dagsterEndpoint: process.env.DAGSTER_ENDPOINT || fileConfig.dagsterEndpoint || "http://localhost:8112",
};

let currentConfig = { ...DEFAULT_CONFIG };

export function setBackendConfig(config: Partial<BackendConfig>): void {
  currentConfig = { ...currentConfig, ...config };
}

export function getBackendConfig(): BackendConfig {
  return { ...currentConfig };
}

// ============================================================================
// Operator Metadata Types
// ============================================================================

export interface InputPortInfo {
  displayName?: string;
  allowMultiLinks?: boolean;
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
  jsonSchema: any;
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

// ============================================================================
// Model Types
// ============================================================================

export interface LiteLLMModel {
  id: string;
  object: string;
  created: number;
  owned_by: string;
}

export interface LiteLLMModelsResponse {
  data: LiteLLMModel[];
  object: string;
}

export interface ModelType {
  id: string;
  name: string;
  description: string;
}

// ============================================================================
// API Functions
// ============================================================================

/**
 * Fetch operator metadata from the backend.
 * @returns Promise with operator metadata
 */
export async function fetchOperatorMetadata(): Promise<OperatorMetadata> {
  const url = `${currentConfig.operatorMetadataEndpoint}/api/resources/operator-metadata`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch operator metadata: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

/**
 * Fetch available models from the models API.
 * @returns Promise with array of model types
 */
export async function fetchModels(): Promise<ModelType[]> {
  const url = `${currentConfig.modelsEndpoint}/api/models`;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      console.warn(`Failed to fetch models: ${response.status} ${response.statusText}`);
      return [];
    }

    const data: LiteLLMModelsResponse = await response.json();

    return data.data.map(model => ({
      id: model.id,
      name: formatModelName(model.id),
      description: `Model: ${model.id}`,
    }));
  } catch (error) {
    console.warn("Failed to fetch models:", error);
    return [];
  }
}

/**
 * Format model ID into a human-readable name.
 * Example: "claude-3.7" -> "Claude 3.7"
 */
function formatModelName(modelId: string): string {
  return modelId
    .split("-")
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

/**
 * Health check for the main API endpoint.
 * @returns Promise with health status
 */
export async function healthCheck(): Promise<{ healthy: boolean; message: string }> {
  try {
    const response = await fetch(`${currentConfig.apiEndpoint}/api/health`);
    if (response.ok) {
      return { healthy: true, message: "Backend is healthy" };
    }
    return { healthy: false, message: `Backend returned ${response.status}` };
  } catch (error) {
    return { healthy: false, message: `Cannot reach backend: ${error}` };
  }
}
