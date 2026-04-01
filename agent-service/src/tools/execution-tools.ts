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
 * Execution tools for Texera Agent Service.
 * These tools provide workflow execution capabilities via HTTP REST API.
 */

import { z } from "zod";
import { tool } from "ai";
import { encode as toonEncode } from "@toon-format/toon";
import { createErrorResult, formatExecuteOperatorResult } from "./tools-utility";
import type { WorkflowState } from "../workflow/workflow-state";
import { getBackendConfig } from "../api/backend-api";
import type { LogicalPlan, LogicalLink } from "../api/execution-api";
import type { OperatorInfo, SyncExecutionResult } from "../types/execution";
import { OperatorMetadataStore } from "./metadata-tools";
import { OperatorResultSerializationMode, ExecutionBackend, DEFAULT_AGENT_SETTINGS } from "../types/agent";

// ============================================================================
// Tool Name Constants
// ============================================================================

export const TOOL_NAME_EXECUTE_OPERATOR = "executeOperator";

// Marker used by context-optimization.ts and latest-only-filter.ts to locate
// the boundary between metadata lines and table data within execution results.
// The marker is a tab-prefixed header line (the first line starting with \t).
// Legacy section separators have been removed — metadata and table data are
// now rendered contiguously without visible section headers.

// ============================================================================
// Execution Configuration
// ============================================================================

export interface ExecutionConfig {
  userToken: string;
  workflowId: number;
  computingUnitId?: number;
  /** Serialization mode for operator results: "json" or "table" */
  serializationMode?: OperatorResultSerializationMode;
  /** Maximum characters for operator results (uses symmetric truncation) */
  maxOperatorResultCharLimit?: number;
  /** Maximum characters per cell */
  maxOperatorResultCellCharLimit?: number;
  /** Execution timeout in milliseconds */
  executionTimeoutMs?: number;
  /** Whether to enable operator result caching */
  cacheEnabled?: boolean;
  /** Execution backend: texera (default) or hamilton */
  executionBackend?: ExecutionBackend;
  /** When true, omit the execution metadata section from results */
  noExecutionMetadata?: boolean;
  /** When true, include per-column statistics in the execution metadata section */
  carryMetadata?: boolean;
}

// ============================================================================
// Execution Mutex
// ============================================================================

/**
 * Simple async mutex for serializing execution requests per workflow.
 * Ensures concurrent requests wait in queue and execute one at a time.
 */
class AsyncMutex {
  private queue: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    let release: () => void;
    const currentQueue = this.queue;

    // Chain a new promise that will resolve when the caller releases
    this.queue = new Promise<void>(resolve => {
      release = resolve;
    });

    // Wait for all previous operations to complete
    await currentQueue;

    return release!;
  }
}

/** Map of workflow ID to its mutex */
const workflowMutexes = new Map<number, AsyncMutex>();

function getWorkflowMutex(workflowId: number): AsyncMutex {
  let mutex = workflowMutexes.get(workflowId);
  if (!mutex) {
    mutex = new AsyncMutex();
    workflowMutexes.set(workflowId, mutex);
  }
  return mutex;
}

// ============================================================================
// Workflow Validation
// ============================================================================

export interface WorkflowValidationResult {
  isValid: boolean;
  errors: Record<string, Record<string, string>>;
}

interface OperatorValidation {
  isValid: boolean;
  messages: Record<string, string>;
}

function validateOperatorSchema(operatorType: string, operatorProperties: Record<string, any>): OperatorValidation {
  const metadataStore = OperatorMetadataStore.getInstance();
  const validation = metadataStore.validateOperatorProperties(operatorType, operatorProperties);
  return validation.isValid ? { isValid: true, messages: {} } : { isValid: false, messages: validation.messages };
}

function validateOperatorConnection(operatorId: string, workflowState: WorkflowState): OperatorValidation {
  const operator = workflowState.getOperator(operatorId);
  if (!operator) {
    return { isValid: false, messages: { error: `Operator ${operatorId} not found` } };
  }

  const numInputLinksByPort = new Map<string, number>();
  const allLinks = workflowState.getAllLinks();

  for (const link of allLinks) {
    if (link.target.operatorID === operatorId) {
      const portID = link.target.portID;
      numInputLinksByPort.set(portID, (numInputLinksByPort.get(portID) ?? 0) + 1);
    }
  }

  let satisfyInput = true;
  let violationMessage = "";

  for (const port of operator.inputPorts) {
    const portNumInputs = numInputLinksByPort.get(port.portID) ?? 0;

    if (port.allowMultiInputs) {
      if (portNumInputs < 1) {
        satisfyInput = false;
        violationMessage += `${port.displayName ?? port.portID} requires at least 1 input, has ${portNumInputs}. `;
      }
    } else {
      if (portNumInputs !== 1) {
        satisfyInput = false;
        violationMessage += `${port.displayName ?? port.portID} requires 1 input, has ${portNumInputs}. `;
      }
    }
  }

  return satisfyInput
    ? { isValid: true, messages: {} }
    : { isValid: false, messages: { inputs: violationMessage.trim() } };
}

function combineValidations(...validations: OperatorValidation[]): OperatorValidation {
  let isValid = true;
  let messages: Record<string, string> = {};

  for (const validation of validations) {
    if (!validation.isValid) {
      isValid = false;
      messages = { ...messages, ...validation.messages };
    }
  }

  return { isValid, messages };
}

export function validateWorkflow(workflowState: WorkflowState): WorkflowValidationResult {
  const errors: Record<string, Record<string, string>> = {};

  for (const operator of workflowState.getAllEnabledOperators()) {
    const schemaValidation = validateOperatorSchema(operator.operatorType, operator.operatorProperties);
    const connectionValidation = validateOperatorConnection(operator.operatorID, workflowState);
    const combined = combineValidations(schemaValidation, connectionValidation);

    if (!combined.isValid) {
      errors[operator.operatorID] = combined.messages;
    }
  }

  return {
    isValid: Object.keys(errors).length === 0,
    errors,
  };
}

function formatWorkflowValidationErrors(validationResult: WorkflowValidationResult): string {
  if (validationResult.isValid) return "";

  const lines: string[] = ["Workflow validation failed:"];
  for (const [operatorId, fieldErrors] of Object.entries(validationResult.errors)) {
    lines.push(`  Operator ${operatorId}:`);
    for (const [field, message] of Object.entries(fieldErrors)) {
      lines.push(`    - ${field}: ${message}`);
    }
  }
  return lines.join("\n");
}

// ============================================================================
// Logical Plan Builder
// ============================================================================

export function buildLogicalPlan(workflowState: WorkflowState, opsToViewResult?: string[]): LogicalPlan {
  const useSubDAG = opsToViewResult && opsToViewResult.length === 1;
  const targetOperatorId = useSubDAG ? opsToViewResult[0] : undefined;

  let operatorsList: { operatorID: string; operatorType: string; [key: string]: any }[];
  let linksList: LogicalLink[];

  const getInputPortOrdinal = (operatorID: string, inputPortID: string): number => {
    const op = workflowState.getOperator(operatorID);
    if (!op) return 0;
    const idx = op.inputPorts.findIndex(port => port.portID === inputPortID);
    return idx >= 0 ? idx : 0;
  };

  const getOutputPortOrdinal = (operatorID: string, outputPortID: string): number => {
    const op = workflowState.getOperator(operatorID);
    if (!op) return 0;
    const idx = op.outputPorts.findIndex(port => port.portID === outputPortID);
    return idx >= 0 ? idx : 0;
  };

  if (targetOperatorId) {
    const subDAG = workflowState.getSubDAG(targetOperatorId);

    operatorsList = subDAG.operators.map(op => ({
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType,
      inputPorts: op.inputPorts,
      outputPorts: op.outputPorts,
    }));

    linksList = subDAG.links.map(link => ({
      fromOpId: link.source.operatorID,
      fromPortId: { id: getOutputPortOrdinal(link.source.operatorID, link.source.portID), internal: false },
      toOpId: link.target.operatorID,
      toPortId: { id: getInputPortOrdinal(link.target.operatorID, link.target.portID), internal: false },
    }));
  } else {
    operatorsList = workflowState.getAllEnabledOperators().map(op => ({
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType,
      inputPorts: op.inputPorts,
      outputPorts: op.outputPorts,
    }));

    linksList = workflowState.getAllLinks().map(link => ({
      fromOpId: link.source.operatorID,
      fromPortId: { id: getOutputPortOrdinal(link.source.operatorID, link.source.portID), internal: false },
      toOpId: link.target.operatorID,
      toPortId: { id: getInputPortOrdinal(link.target.operatorID, link.target.portID), internal: false },
    }));
  }

  let allOpsToView: string[];
  if (opsToViewResult && opsToViewResult.length > 0) {
    const operatorIds = new Set(operatorsList.map(op => op.operatorID));
    allOpsToView = opsToViewResult.filter(id => operatorIds.has(id));
  } else {
    allOpsToView = operatorsList
      .filter(op => !linksList.some(link => link.fromOpId === op.operatorID))
      .map(op => op.operatorID);
  }

  return {
    operators: operatorsList,
    links: linksList,
    opsToViewResult: allOpsToView,
  };
}

// ============================================================================
// HTTP Execution Function
// ============================================================================

async function executeWorkflowHttp(
  config: ExecutionConfig,
  logicalPlan: LogicalPlan,
  options: { abortSignal?: AbortSignal } = {}
): Promise<SyncExecutionResult> {
  const backendConfig = getBackendConfig();

  const workflowId = config.workflowId;
  const computingUnitId = config.computingUnitId ?? 0;

  // In k8s, each computing unit is a separate pod in the pool namespace.
  // Use EXECUTION_ENDPOINT_TEMPLATE if set (e.g. "http://computing-unit-{cuid}.texera-workflow-computing-unit-svc.texera-workflow-computing-unit-pool.svc.cluster.local:8085")
  // Otherwise fall back to a static endpoint.
  const endpointTemplate = process.env.EXECUTION_ENDPOINT_TEMPLATE;
  const executionEndpoint = endpointTemplate
    ? endpointTemplate.replace("{cuid}", String(computingUnitId))
    : backendConfig.executionEndpoint || "http://localhost:8085";

  const url = `${executionEndpoint}/api/execution/${workflowId}/${computingUnitId}/run`;

  const timeoutSeconds = config.executionTimeoutMs
    ? Math.ceil(config.executionTimeoutMs / 1000)
    : Math.ceil(DEFAULT_AGENT_SETTINGS.executionTimeoutMs / 1000);

  // Backend returns JSON - agent-service handles serialization to table/toon format
  const request = {
    executionName: "agent-execution",
    logicalPlan: {
      operators: logicalPlan.operators,
      links: logicalPlan.links,
      opsToViewResult: logicalPlan.opsToViewResult || [],
      opsToReuseResult: [],
    },
    targetOperatorIds: logicalPlan.opsToViewResult || [],
    timeoutSeconds,
    maxOperatorResultCharLimit: config.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit,
    maxOperatorResultCellCharLimit:
      config.maxOperatorResultCellCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCellCharLimit,
  };

  console.log(
    `[ExecutionTools] Executing workflow via HTTP: ${url} ` +
      `(maxOperatorResultCharLimit: ${request.maxOperatorResultCharLimit}, ` +
      `maxOperatorResultCellCharLimit: ${request.maxOperatorResultCellCharLimit})`
  );

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.userToken}`,
      },
      body: JSON.stringify(request),
      signal: options.abortSignal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Execution request failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    console.error("[ExecutionTools] Execution failed:", error);
    return {
      success: false,
      state: "Error",
      operators: {},
      errors: [error instanceof Error ? error.message : "Unknown error"],
    };
  }
}

// ============================================================================
// Hamilton Execution Function
// ============================================================================

/**
 * Execute a workflow via the Hamilton sidecar.
 * Sends WorkflowContent format (operators with operatorProperties.code,
 * links with source/target) rather than the Texera LogicalPlan format.
 * The sidecar is stateless — it translates to Hamilton and executes.
 */
async function executeWorkflowHamilton(
  config: ExecutionConfig,
  workflowState: WorkflowState,
  operatorId: string,
  options: { abortSignal?: AbortSignal } = {}
): Promise<SyncExecutionResult> {
  const backendConfig = getBackendConfig();
  const hamiltonEndpoint = backendConfig.hamiltonEndpoint || "http://localhost:8111";

  const url = `${hamiltonEndpoint}/execute`;

  const timeoutSeconds = config.executionTimeoutMs
    ? Math.ceil(config.executionTimeoutMs / 1000)
    : Math.ceil(DEFAULT_AGENT_SETTINGS.executionTimeoutMs / 1000);

  // Get the sub-DAG up to the target operator, in WorkflowContent format
  const subDAG = workflowState.getSubDAG(operatorId);

  const request = {
    operators: subDAG.operators,
    links: subDAG.links,
    targetOperatorIds: [operatorId],
    timeoutSeconds,
    maxResultChars: config.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit,
    maxCellChars: config.maxOperatorResultCellCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCellCharLimit,
  };

  console.log(
    `[ExecutionTools] Executing workflow via Hamilton: ${url} ` +
      `(operators: ${subDAG.operators.length}, links: ${subDAG.links.length}, target: ${operatorId})`
  );

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
      signal: options.abortSignal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Hamilton execution failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    console.error("[ExecutionTools] Hamilton execution failed:", error);
    return {
      success: false,
      state: "Error",
      operators: {},
      errors: [error instanceof Error ? error.message : "Unknown error"],
    };
  }
}

// ============================================================================
// Dagster Execution Function
// ============================================================================

/**
 * Execute a workflow via the Dagster sidecar.
 * Sends WorkflowContent format (operators with operatorProperties.code,
 * links with source/target) rather than the Texera LogicalPlan format.
 * The sidecar is stateless — it translates to Dagster assets and executes.
 */
async function executeWorkflowDagster(
  config: ExecutionConfig,
  workflowState: WorkflowState,
  operatorId: string,
  options: { abortSignal?: AbortSignal } = {}
): Promise<SyncExecutionResult> {
  const backendConfig = getBackendConfig();
  const dagsterEndpoint = backendConfig.dagsterEndpoint || "http://localhost:8112";

  const url = `${dagsterEndpoint}/execute`;

  const timeoutSeconds = config.executionTimeoutMs
    ? Math.ceil(config.executionTimeoutMs / 1000)
    : Math.ceil(DEFAULT_AGENT_SETTINGS.executionTimeoutMs / 1000);

  // Get the sub-DAG up to the target operator, in WorkflowContent format
  const subDAG = workflowState.getSubDAG(operatorId);

  const request = {
    operators: subDAG.operators,
    links: subDAG.links,
    targetOperatorIds: [operatorId],
    timeoutSeconds,
    maxResultChars: config.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit,
    maxCellChars: config.maxOperatorResultCellCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCellCharLimit,
  };

  console.log(
    `[ExecutionTools] Executing workflow via Dagster: ${url} ` +
      `(operators: ${subDAG.operators.length}, links: ${subDAG.links.length}, target: ${operatorId})`
  );

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
      signal: options.abortSignal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Dagster execution failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    console.error("[ExecutionTools] Dagster execution failed:", error);
    return {
      success: false,
      state: "Error",
      operators: {},
      errors: [error instanceof Error ? error.message : "Unknown error"],
    };
  }
}

// ============================================================================
// Result Formatting (agent-service side)
// ============================================================================

/**
 * Format compact Input/Output metadata lines for an operator.
 * Uses upstream operator IDs from workflow links to label each input port.
 * - Source operator (no inputs): "Output table shape: (rows, cols)"
 * - Single input: "Input operator(table shape): upstream(rows, cols)\nOutput table shape: (rows, cols)"
 * - Multi input: "Input operator(table shape): up1(rows, cols), up2(rows, cols)\nOutput table shape: (rows, cols)"
 */
function formatInputOutput(
  workflowState: WorkflowState,
  operatorId: string,
  opInfo: OperatorInfo,
  outputColumns: number
): string {
  const outputRows = opInfo.totalRowCount ?? opInfo.outputTuples;
  const outputLine = `Output table shape: (${outputRows}, ${outputColumns})`;

  const inputShapes = opInfo.inputPortShapes;
  if (!inputShapes || inputShapes.length === 0) {
    return outputLine;
  }

  // Build a map from portIndex to upstream operator ID using workflow links
  const inputLinks = workflowState.getAllLinks().filter(l => l.target.operatorID === operatorId);
  const portIndexToUpstream = new Map<number, string>();
  const op = workflowState.getOperator(operatorId);
  for (const link of inputLinks) {
    const portIdx = op?.inputPorts.findIndex(p => p.portID === link.target.portID) ?? -1;
    if (portIdx >= 0) {
      portIndexToUpstream.set(portIdx, link.source.operatorID);
    }
  }

  const inputPart = inputShapes
    .sort((a, b) => a.portIndex - b.portIndex)
    .map(p => {
      const name = portIndexToUpstream.get(p.portIndex) ?? `input${p.portIndex}`;
      return `${name}(${p.rows}, ${p.columns})`;
    })
    .join(", ");

  return `Input operator(table shape): ${inputPart}\n${outputLine}`;
}

/** Maximum number of columns to include in the Column Stats section. */
const MAX_STATS_COLUMNS = 50;

/**
 * Type priority for sorting columns in the stats section.
 * Lower number = shown first (more informative types surface early).
 */
function typePriority(dataType: string): number {
  const t = dataType.toLowerCase();
  if (t === "bool" || t === "boolean") return 0;
  if (t === "str" || t === "string" || t === "object") return 1;
  if (t.startsWith("date") || t === "datetime") return 2;
  if (t === "int" || t === "integer" || t === "int64" || t === "int32") return 3;
  if (t === "float" || t === "numeric" || t === "float64" || t === "float32" || t === "number") return 4;
  return 5; // unknown types last among basic types but before numeric
}

/**
 * Format per-column statistics as a vertical "Column Stats" section.
 * Columns are sorted by type priority (bool > string > datetime > int > float)
 * and capped at MAX_STATS_COLUMNS with a truncation notice.
 *
 * If `headers` is provided, only those columns are included (in sorted order).
 * Otherwise, all columns in resultStatistics are used.
 *
 * Example output:
 *   Column Stats (showing 50 of 179 columns):
 *   - "Case_excluded" (str): null=0, distinct=2, top_10={"No"=144, "Yes"=9}
 *   - "age" (int): null=0, mean=62.3, min=28, max=91
 */
function formatColumnStatsSection(resultStatistics: Record<string, string>, headers?: string[]): string[] {
  // Parse all columns and their stats
  const parsed: Array<{ colName: string; dataType: string; kvPairs: string }> = [];

  const columnNames = headers ?? Object.keys(resultStatistics);
  for (const colName of columnNames) {
    const statsJson = resultStatistics[colName];
    if (!statsJson) continue;
    try {
      const p = JSON.parse(statsJson);
      const dataType: string = p.data_type ?? "unknown";
      const stats: Record<string, any> = p.statistics ?? {};

      const kvPairs = Object.entries(stats)
        .filter(([k, v]) => v !== null && v !== undefined && !EXCLUDED_STAT_KEYS.has(k))
        .map(([k, v]) => {
          if (k === "top_10" && typeof v === "object") {
            const inner = Object.entries(v)
              .map(([ik, iv]) => `"${ik}"=${formatStatValue(iv)}`)
              .join(", ");
            return `top_10={${inner}}`;
          }
          if (typeof v === "object") return null;
          return `${k}=${formatStatValue(v)}`;
        })
        .filter(Boolean)
        .join(", ");

      parsed.push({ colName, dataType, kvPairs });
    } catch {
      // skip unparseable columns
    }
  }

  if (parsed.length === 0) return [];

  // Sort by type priority (bool > string > datetime > int > float)
  parsed.sort((a, b) => typePriority(a.dataType) - typePriority(b.dataType));

  const totalColumns = parsed.length;
  const truncated = totalColumns > MAX_STATS_COLUMNS;
  const shown = truncated ? parsed.slice(0, MAX_STATS_COLUMNS) : parsed;

  const header = truncated
    ? `Column Stats (showing ${MAX_STATS_COLUMNS} of ${totalColumns} columns):`
    : `Column Stats:`;

  const lines = shown.map(({ colName, dataType, kvPairs }) =>
    kvPairs ? `- "${colName}" (${dataType}): ${kvPairs}` : `- "${colName}" (${dataType})`
  );

  return [header, ...lines];
}

/** Stat keys to exclude from per-column stats (redundant with Output table shape). */
const EXCLUDED_STAT_KEYS = new Set(["count", "std", "p25", "median", "p75"]);

/** Maximum significant digits for floating-point stat values. */
const STAT_PRECISION = 4;

/** Format a stat value, rounding floats to STAT_PRECISION significant digits. */
function formatStatValue(v: any): string {
  if (v === null || v === undefined) return "N/A";
  if (typeof v === "number" && !Number.isInteger(v)) {
    return Number(v.toPrecision(STAT_PRECISION)).toString();
  }
  return String(v);
}


/**
 * Formats execution error with structured sections.
 */
function formatExecutionError(
  compilationErrors?: Record<string, string>,
  operatorErrors?: Array<{ operatorId: string; error: string }>,
  generalErrors?: string[]
): string {
  const lines: string[] = ["Execution failed due to the following error:"];

  if (compilationErrors && Object.keys(compilationErrors).length > 0) {
    lines.push("Compilation error:");
    for (const [key, value] of Object.entries(compilationErrors)) {
      lines.push(`  ${key}: ${value}`);
    }
  }

  if (operatorErrors && operatorErrors.length > 0) {
    lines.push("Execution error:");
    for (const { operatorId, error } of operatorErrors) {
      lines.push(`  ${operatorId}: ${error}`);
    }
  }

  if (generalErrors && generalErrors.length > 0) {
    lines.push("Error:");
    for (const error of generalErrors) {
      lines.push(`  ${error}`);
    }
  }

  return lines.join("\n");
}

/**
 * Convert JSON result to pandas DataFrame-style table format (tab-separated).
 * Includes row indices (0, 1, 2, ...) and a leading tab on the header row
 * to align with the index column, matching pandas `__repr__` output.
 * Uses tab (\t) as column separator for readability.
 */
function jsonToTableFormat(jsonResult: Record<string, any>[]): string {
  if (!jsonResult || jsonResult.length === 0) return "";

  // Use __row_index__ from backend if present, otherwise fall back to array index
  const hasRowIndex = jsonResult.length > 0 && "__row_index__" in jsonResult[0];
  const headers = Object.keys(jsonResult[0]).filter(h => h !== "__row_index__");
  // Leading tab aligns headers with the index column (pandas style)
  const headerLine = "\t" + headers.join("\t");

  const formattedRows: string[] = [];
  let prevIndex = -1;

  for (let i = 0; i < jsonResult.length; i++) {
    const row = jsonResult[i];
    const rowIndex = hasRowIndex ? (row["__row_index__"] as number) : i;

    // Detect gap in row indices — insert pandas-style "..." separator
    if (prevIndex >= 0 && rowIndex > prevIndex + 1) {
      const dots = headers.map(() => "...").join("\t");
      formattedRows.push(`...\t${dots}`);
    }
    prevIndex = rowIndex;

    const cells = headers.map(h => {
      const val = row[h];
      if (val === null) return "NaN";
      if (val === undefined) return "";
      if (typeof val === "number" || typeof val === "boolean") return String(val);
      if (typeof val === "string") {
        if (val === "NULL") return "NaN";
        return val.replace(/\t/g, "\\t").replace(/\n/g, "\\n");
      }
      return JSON.stringify(val);
    });
    formattedRows.push(`${rowIndex}\t${cells.join("\t")}`);
  }

  return [headerLine, ...formattedRows].join("\n");
}

/**
 * Convert JSON result to TOON format (Token-Oriented Object Notation).
 * Uses the official @toon-format/toon library.
 * TOON is a compact format designed for LLMs that reduces token usage by 30-60%.
 *
 * Example output:
 *   data[3]{ID,card_scheme,account_type,aci}:
 *   1,TransactPlus,[],["C","B"]
 *   2,GlobalCard,["R"],["A"]
 *   3,NexPay,[],[]
 */
function jsonToToonFormat(jsonResult: Record<string, any>[]): string {
  if (!jsonResult || jsonResult.length === 0) return "";

  // Wrap the array in an object with "data" key for TOON encoding
  // This produces: data[n]{col1,col2,...}: followed by rows
  return toonEncode({ data: jsonResult });
}

// ============================================================================
// Common Execution Function
// ============================================================================

/**
 * Execute a workflow for a specific operator and return the formatted result.
 * This is the core execution logic shared by the executeWorkflow tool and auto-execute feature.
 *
 * @param workflowState - The workflow state
 * @param config - Execution configuration
 * @param operatorId - The operator to execute and get results for
 * @param options - Optional abort signal
 * @returns Formatted result string or error message
 */
export async function executeOperatorAndFormat(
  workflowState: WorkflowState,
  config: ExecutionConfig,
  operatorId: string,
  options: {
    abortSignal?: AbortSignal;
    onResult?: (operatorId: string, operatorInfo: OperatorInfo) => void;
    /** @deprecated Use onResult with OperatorInfo instead */
    onResultLegacy?: (operatorId: string, backendStats?: Record<string, string>) => void;
  } = {}
): Promise<string> {
  // Acquire mutex to serialize executions for this workflow
  // This prevents ConcurrentModificationException on the backend
  const release = await getWorkflowMutex(config.workflowId).acquire();

  try {
    // Build logical plan for the single operator (sub-DAG up to this operator)
    const logicalPlan = buildLogicalPlan(workflowState, [operatorId]);

    if (logicalPlan.operators.length === 0) {
      return createErrorResult("Cannot execute: workflow has no operators.");
    }

    // Validate entire workflow but only block on the target operator's errors.
    // Other operators' errors are irrelevant — if upstream has issues, execution
    // will fail with a runtime error that correctly identifies the failing operator.
    const validationResult = validateWorkflow(workflowState);
    if (!validationResult.isValid) {
      const targetErrors = validationResult.errors[operatorId];
      if (targetErrors) {
        const lines = [`Operator ${operatorId}:`];
        for (const [field, message] of Object.entries(targetErrors)) {
          lines.push(`  - ${field}: ${message}`);
        }
        return createErrorResult(lines.join("\n"));
      }
      // Target operator is valid — proceed with execution despite other operators' errors
    }

    let result: SyncExecutionResult;
    if (config.executionBackend === ExecutionBackend.HAMILTON) {
      result = await executeWorkflowHamilton(config, workflowState, operatorId, {
        abortSignal: options.abortSignal,
      });
    } else if (config.executionBackend === ExecutionBackend.DAGSTER) {
      result = await executeWorkflowDagster(config, workflowState, operatorId, {
        abortSignal: options.abortSignal,
      });
    } else {
      result = await executeWorkflowHttp(config, logicalPlan, {
        abortSignal: options.abortSignal,
      });
    }

    // Handle execution failure
    if (!result.success) {
      const compilationErrors =
        result.state === "CompilationFailed" || result.state === "ValidationFailed"
          ? result.compilationErrors
          : undefined;

      const operatorErrors =
        result.state === "Failed"
          ? Object.entries(result.operators)
              .filter(([_, op]) => op.error)
              .map(([opId, op]) => ({ operatorId: opId, error: op.error! }))
          : undefined;

      const generalErrors =
        result.state === "Killed" ? ["Workflow execution was killed (timeout)."] : result.errors;

      const errorText = formatExecutionError(compilationErrors, operatorErrors, generalErrors);

      // Store a synthetic OperatorInfo with the error so it appears in the DAG summary
      if (options.onResult) {
        const errorInfo: OperatorInfo = {
          state: result.state,
          inputTuples: 0,
          outputTuples: 0,
          resultMode: "table",
          error: errorText,
        };
        options.onResult(operatorId, errorInfo);
      }

      return createErrorResult(errorText);
    }

    // Check operator result
    const opInfo = result.operators[operatorId];
    if (!opInfo) {
      return createErrorResult(
        formatExecutionError(undefined, undefined, [`No result found for operator: ${operatorId}`])
      );
    }

    if (opInfo.error) {
      // Store the OperatorInfo with error so it appears in the DAG summary
      if (options.onResult) {
        options.onResult(operatorId, opInfo);
      }
      return createErrorResult(formatExecutionError(undefined, [{ operatorId, error: opInfo.error }]));
    }

    // Get result info - backend always returns JSON array, agent-service serializes
    const serializationMode = config.serializationMode ?? OperatorResultSerializationMode.TABLE;

    if (!opInfo.result || !Array.isArray(opInfo.result)) {
      return "(no result data)";
    }

    // Both Texera and Hamilton enforce per-cell truncation server-side.
    const jsonArray = opInfo.result as Record<string, any>[];
    const headers = jsonArray.length > 0
      ? Object.keys(jsonArray[0]).filter(k => k !== "__row_index__")
      : [];
    const columns = headers.length;

    // Notify caller with OperatorInfo for all operators in the execution (not just the target)
    // This ensures upstream operators (CSVFileScan, Join, etc.) also get their stats stored
    if (options.onResult) {
      for (const [opId, info] of Object.entries(result.operators)) {
        if (info && !info.error) {
          options.onResult(opId, info);
        }
      }
    }

    let dataString: string;
    let modeLabel: string;

    switch (serializationMode) {
      case OperatorResultSerializationMode.TABLE:
        dataString = jsonToTableFormat(jsonArray);
        modeLabel = "table";
        break;
      case OperatorResultSerializationMode.TOON:
        dataString = jsonToToonFormat(jsonArray);
        modeLabel = "toon";
        break;
      case OperatorResultSerializationMode.JSON:
      default:
        dataString = JSON.stringify(jsonArray);
        modeLabel = "json";
        break;
    }

    // Safety-net: both backends truncate server-side, but serialization to
    // table/toon format may add padding beyond the raw record size estimate.
    // This ensures the final serialized string respects the character limit.
    const charLimit = config.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit;

    if (dataString.length > charLimit) {
      const allLines = dataString.split("\n");
      // First line is the header (for table/toon) or opening bracket (for json)
      const headerLine = allLines[0];
      const dataRows = allLines.slice(1);

      // Reserve space for header
      const reservedSize = headerLine.length + 1;

      // Symmetric truncation: keep first half + last half of rows within budget
      const halfLimit = Math.floor((charLimit - reservedSize) / 2);

      let frontSize = 0;
      const frontRows: string[] = [];
      for (const row of dataRows) {
        const rowLen = row.length + 1;
        if (frontSize + rowLen > halfLimit && frontRows.length > 0) break;
        frontRows.push(row);
        frontSize += rowLen;
      }

      let backSize = 0;
      const backRows: string[] = [];
      for (let i = dataRows.length - 1; i >= frontRows.length; i--) {
        const rowLen = dataRows[i].length + 1;
        if (backSize + rowLen > halfLimit && backRows.length > 0) break;
        backRows.unshift(dataRows[i]);
        backSize += rowLen;
      }

      const keptRows = [...frontRows, ...backRows];
      dataString = [headerLine, ...keptRows].join("\n");
    }

    // Build compact Input/Output lines using upstream operator IDs from links
    const shapeLine = formatInputOutput(workflowState, operatorId, opInfo, columns);

    // Surface warnings (e.g., duplicate column renames) so the agent can adjust its code
    const warningLines = opInfo.warnings?.map(w => w) ?? [];

    // Per-column statistics as a vertical "Column Stats" section (after the table)
    const columnStatsLines = (config.carryMetadata && opInfo.resultStatistics)
      ? formatColumnStatsSection(opInfo.resultStatistics, headers)
      : [];

    // Build result: metadata lines → table data → column stats section.
    // Context optimization / latest-only-filter locate the table by finding
    // the first line starting with \t (the header row). Column stats go after
    // the table so they don't interfere with table boundary detection.
    const metadataLines = config.noExecutionMetadata
      ? []
      : [shapeLine, ...warningLines].filter(Boolean);

    const briefSummary = formatExecuteOperatorResult(operatorId);
    return [briefSummary, ...metadataLines, dataString, ...columnStatsLines].filter(Boolean).join("\n");
  } catch (error: any) {
    if (error.name === "AbortError") {
      throw error;
    }
    return createErrorResult(`Execution failed: ${error.message || String(error)}`);
  } finally {
    release();
  }
}

// ============================================================================
// Tool Creator
// ============================================================================

/**
 * Create the executeOperator tool.
 * @param workflowState - The workflow state
 * @param getConfig - Function that returns the current execution config (called at execution time)
 */
export function createExecuteOperatorTool(
  workflowState: WorkflowState,
  getConfig: () => ExecutionConfig,
  onResult?: (operatorId: string, operatorInfo: OperatorInfo) => void
) {
  return tool({
    description: "Execute the workflow and get the specified operator's result. The execution result(if succeeded) includes the shape of the input tables(if any) and output table, and the records in the output table",
    inputSchema: z.object({
      operatorId: z.string().describe("The operator ID to view result for."),
    }),
    execute: async (args: { operatorId: string }, options: { abortSignal?: AbortSignal }) => {
      const config = getConfig();
      return await executeOperatorAndFormat(workflowState, config, args.operatorId, { ...options, onResult });
    },
  });
}
