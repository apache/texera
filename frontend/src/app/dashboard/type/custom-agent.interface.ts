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
 * LiteLLM `model_name` values registered in `bin/litellm-config.yaml`. Keep
 * this union in sync with that file — picking a value not registered there
 * will cause LiteLLM to return 404 on chat completion.
 */
export type AgentModel = "claude-opus-4.7" | "claude-haiku-4.5";

export const DEFAULT_AGENT_MODEL: AgentModel = "claude-haiku-4.5";

export const AGENT_MODEL_OPTIONS: { value: AgentModel; label: string }[] = [
  { value: "claude-opus-4.7", label: "claude-opus-4.7 (most capable, slower)" },
  { value: "claude-haiku-4.5", label: "claude-haiku-4.5 (fastest, cheapest)" },
];

export type AgentDomain = "biomedical" | "nlp" | "finance" | "social_science" | "cv" | "general";

export type AgentMethodology = "crisp_dm" | "semma" | "kdd" | "none";

export type AgentTaskType = "classification" | "regression" | "clustering" | "eda" | "cleaning" | "custom";

export interface AgentGuardrails {
  requireTrainTestSplit: boolean;
  requireEvaluation: boolean;
  preventDataLeakage: boolean;
  handleMissingValues: boolean;
  featureScalingCheck: boolean;
}

export type AgentOutputFormat = "dashboard" | "csv_export" | "report" | "none";

export interface AgentOutputPreferences {
  includeVisualization: boolean;
  exportToCsv: boolean;
  generateSummaryStats: boolean;
  includeDataProfiling: boolean;
  defaultFormat: AgentOutputFormat;
}

export interface KnowledgeFile {
  id: string;
  name: string;
  mimeType: string;
  size: number;
  /** Base64-encoded contents (no data: prefix). For hackathon storage; size-capped. */
  contentBase64: string;
}

export interface CustomAgent {
  id: string;
  name: string;
  description: string;
  icon: string;
  creator: string;
  /** LiteLLM model_name to use for this agent's LLM calls. */
  model: AgentModel;
  domain: AgentDomain;
  methodology: AgentMethodology;
  taskType: AgentTaskType;
  guardrails: AgentGuardrails;
  customRules: string;
  /** Reference files (data dictionaries, methodology docs, papers). */
  knowledgeFiles: KnowledgeFile[];
  /** wids of user workflows used as templates. */
  exampleWorkflowIds: number[];
  outputPreferences: AgentOutputPreferences;
  preferredOperators: string[];
  isPublic: boolean;
  createdAt: string;
  updatedAt: string;
}

export const DEFAULT_GUARDRAILS: AgentGuardrails = {
  requireTrainTestSplit: true,
  requireEvaluation: true,
  preventDataLeakage: true,
  handleMissingValues: true,
  featureScalingCheck: true,
};

export const AGENT_DOMAIN_OPTIONS: { value: AgentDomain; label: string }[] = [
  { value: "biomedical", label: "Biomedical" },
  { value: "nlp", label: "NLP / Text Analysis" },
  { value: "finance", label: "Finance" },
  { value: "social_science", label: "Social Science" },
  { value: "cv", label: "Computer Vision" },
  { value: "general", label: "General" },
];

export const AGENT_METHODOLOGY_OPTIONS: { value: AgentMethodology; label: string }[] = [
  { value: "crisp_dm", label: "CRISP-DM" },
  { value: "semma", label: "SEMMA" },
  { value: "kdd", label: "KDD" },
  { value: "none", label: "None" },
];

export const AGENT_TASK_TYPE_OPTIONS: { value: AgentTaskType; label: string }[] = [
  { value: "classification", label: "Classification" },
  { value: "regression", label: "Regression" },
  { value: "clustering", label: "Clustering" },
  { value: "eda", label: "EDA" },
  { value: "cleaning", label: "Data Cleaning" },
  { value: "custom", label: "Custom" },
];

export const AGENT_OUTPUT_FORMAT_OPTIONS: { value: AgentOutputFormat; label: string }[] = [
  { value: "dashboard", label: "Dashboard" },
  { value: "csv_export", label: "CSV Export" },
  { value: "report", label: "Report" },
  { value: "none", label: "None" },
];

export const DEFAULT_OUTPUT_PREFERENCES: AgentOutputPreferences = {
  includeVisualization: false,
  exportToCsv: false,
  generateSummaryStats: false,
  includeDataProfiling: false,
  defaultFormat: "none",
};

/** Knowledge file size cap (per file) to keep localStorage usable. */
export const KNOWLEDGE_FILE_MAX_BYTES = 1_000_000; // 1 MB
export const KNOWLEDGE_FILE_ACCEPT = ".csv,.pdf,.txt,.md,.json";
