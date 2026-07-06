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

import { z } from "zod";
import { tool } from "ai";
import {
  createErrorResult,
  formatExecuteOperatorResult,
  formatSampleRowsAsTsv,
  getOperatorErrorText,
  getOperatorWarnings,
  tupleColumns,
} from "./tools-utility";
import type { WorkflowState } from "../workflow-state";
import { getBackendConfig } from "../../api/backend-api";
import { env } from "../../config/env";
import type { LogicalPlan, LogicalLink } from "../../api/execution-api";
import {
  OperatorState,
  WorkflowFatalErrorType,
  WorkflowExecutionState,
  type OperatorExecutionSummary,
  type WorkflowFatalError,
  type WorkflowExecutionSummary,
} from "../../types/execution";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import { DEFAULT_AGENT_SETTINGS } from "../../types/agent";
import { createLogger } from "../../logger";

const log = createLogger("ExecutionTools");

export const TOOL_NAME_EXECUTE_OPERATOR = "executeOperator";

export interface ExecutionConfig {
  userToken: string;
  workflowId: number;
  computingUnitId?: number;
  maxOperatorResultCharLimit?: number;
  maxOperatorResultCellCharLimit?: number;
  executionTimeoutMs?: number;
}

/**
 * FIFO async lock used to serialize workflow executions per workflow id.
 *
 * `acquire()` resolves with a release function once prior holders have
 * released. Callers must invoke the release in a `finally` to avoid
 * deadlocking subsequent waiters.
 */
class AsyncMutex {
  private queue: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    let release: () => void;
    const currentQueue = this.queue;

    this.queue = new Promise<void>(resolve => {
      release = resolve;
    });

    await currentQueue;

    return release!;
  }
}

const workflowMutexes = new Map<number, AsyncMutex>();

function getWorkflowMutex(workflowId: number): AsyncMutex {
  let mutex = workflowMutexes.get(workflowId);
  if (!mutex) {
    mutex = new AsyncMutex();
    workflowMutexes.set(workflowId, mutex);
  }
  return mutex;
}

interface WorkflowValidationResult {
  isValid: boolean;
  errors: Record<string, Record<string, string>>;
}

interface OperatorValidation {
  isValid: boolean;
  messages: Record<string, string>;
}

function validateOperatorSchema(operatorType: string, operatorProperties: Record<string, any>): OperatorValidation {
  const metadataStore = WorkflowSystemMetadata.getInstance();
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

    if (port.disallowMultiInputs) {
      if (portNumInputs !== 1) {
        satisfyInput = false;
        violationMessage += `${port.displayName ?? port.portID} requires 1 input, has ${portNumInputs}. `;
      }
    } else {
      if (portNumInputs < 1) {
        satisfyInput = false;
        violationMessage += `${port.displayName ?? port.portID} requires at least 1 input, has ${portNumInputs}. `;
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

function validateWorkflow(workflowState: WorkflowState): WorkflowValidationResult {
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

function buildLogicalPlan(workflowState: WorkflowState, opsToViewResult?: string[]): LogicalPlan {
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

async function executeWorkflowHttp(
  config: ExecutionConfig,
  logicalPlan: LogicalPlan,
  options: { abortSignal?: AbortSignal } = {}
): Promise<WorkflowExecutionSummary> {
  const backendConfig = getBackendConfig();

  const workflowId = config.workflowId;
  const computingUnitId = config.computingUnitId ?? 0;

  // In k8s each computing unit is a separate pod, so the endpoint varies per cuid.
  const executionEndpoint = env.EXECUTION_ENDPOINT_TEMPLATE
    ? env.EXECUTION_ENDPOINT_TEMPLATE.replace("{cuid}", String(computingUnitId))
    : backendConfig.executionEndpoint;

  const url = `${executionEndpoint}/api/execution/${workflowId}/${computingUnitId}/run`;

  const timeoutSeconds = config.executionTimeoutMs
    ? Math.ceil(config.executionTimeoutMs / 1000)
    : Math.ceil(DEFAULT_AGENT_SETTINGS.executionTimeoutMs / 1000);

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

  log.debug(
    {
      url,
      maxOperatorResultCharLimit: request.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: request.maxOperatorResultCellCharLimit,
    },
    "executing workflow"
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

    return (await response.json()) as WorkflowExecutionSummary;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    log.error({ err: error }, "execution failed");
    return {
      success: false,
      state: WorkflowExecutionState.ERROR,
      operators: {},
      errors: [makeExecutionFailure(error instanceof Error ? error.message : "Unknown error", "")],
    };
  }
}

function formatExecutionError(
  operatorErrors?: Array<{ operatorId: string; error: string }>,
  generalErrors?: WorkflowFatalError[]
): string {
  const lines: string[] = ["Execution failed due to the following error:"];

  if (operatorErrors && operatorErrors.length > 0) {
    lines.push("Execution error:");
    for (const { operatorId, error } of operatorErrors) {
      lines.push(`  ${operatorId}: ${error}`);
    }
  }

  if (generalErrors && generalErrors.length > 0) {
    lines.push("Error:");
    for (const error of generalErrors) {
      lines.push(`  ${error.message}`);
    }
  }

  return lines.join("\n");
}

function makeExecutionFailure(message: string, operatorId: string): WorkflowFatalError {
  const now = Date.now();
  return {
    type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
    timestamp: { seconds: Math.floor(now / 1000), nanos: (now % 1000) * 1_000_000 },
    message,
    details: "",
    operatorId,
    workerId: "",
  };
}

export async function executeOperatorAndFormat(
  workflowState: WorkflowState,
  config: ExecutionConfig,
  operatorId: string,
  options: {
    abortSignal?: AbortSignal;
    onResult?: (operatorId: string, operatorInfo: OperatorExecutionSummary) => void;
    onResultLegacy?: (operatorId: string, backendStats?: Record<string, string>) => void;
  } = {}
): Promise<string> {
  // Serialize executions per workflow to avoid ConcurrentModificationException on the backend.
  const release = await getWorkflowMutex(config.workflowId).acquire();

  try {
    const logicalPlan = buildLogicalPlan(workflowState, [operatorId]);

    if (logicalPlan.operators.length === 0) {
      return createErrorResult("Cannot execute: workflow has no operators.");
    }

    // Only block on the target operator's validation errors; upstream issues will
    // surface as runtime errors that correctly identify the failing operator.
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
    }

    const result: WorkflowExecutionSummary = await executeWorkflowHttp(config, logicalPlan, {
      abortSignal: options.abortSignal,
    });

    if (!result.success) {
      const operatorErrors =
        result.state === WorkflowExecutionState.FAILED
          ? Object.entries(result.operators)
              .map(([opId, op]) => ({ operatorId: opId, error: getOperatorErrorText(op) }))
              .filter(({ error }) => error)
          : undefined;

      const generalErrors =
        result.state === WorkflowExecutionState.KILLED
          ? [makeExecutionFailure("Workflow execution was killed (timeout).", "")]
          : result.errors;

      const errorText = formatExecutionError(operatorErrors, generalErrors);

      if (options.onResult) {
        const errorInfo: OperatorExecutionSummary = {
          state: OperatorState.FAILED,
          errorMessages: [makeExecutionFailure(errorText, operatorId)],
        };
        options.onResult(operatorId, errorInfo);
      }

      return createErrorResult(errorText);
    }

    const opInfo = result.operators[operatorId];
    if (!opInfo) {
      return createErrorResult(
        formatExecutionError(undefined, [makeExecutionFailure(`No result found for operator: ${operatorId}`, "")])
      );
    }

    const opError = getOperatorErrorText(opInfo);
    if (opError) {
      if (options.onResult) {
        options.onResult(operatorId, opInfo);
      }
      return createErrorResult(formatExecutionError([{ operatorId, error: opError }]));
    }

    const sampleTuples = opInfo.resultSummary?.sampleTuples;
    if (!sampleTuples || !Array.isArray(sampleTuples)) {
      return "(no result data)";
    }

    const headers = sampleTuples.length > 0 ? tupleColumns(sampleTuples[0][1]) : [];
    const columns = headers.length;

    // Notify for every operator in the execution so upstream stats are also stored.
    if (options.onResult) {
      for (const [opId, info] of Object.entries(result.operators)) {
        if (info && !getOperatorErrorText(info)) {
          options.onResult(opId, info);
        }
      }
    }

    let dataString = formatSampleRowsAsTsv(sampleTuples);

    // Safety-net: TSV serialization may add padding beyond backend's raw-record budget.
    const charLimit = config.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit;

    if (dataString.length > charLimit) {
      const allLines = dataString.split("\n");
      const headerLine = allLines[0];
      const dataRows = allLines.slice(1);

      const reservedSize = headerLine.length + 1;

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

    // Output shape only; the agent derives input-port shapes from the DAG + the
    // upstream operators' own output shapes shown in context.
    const shapeLine = `Output table shape: (${opInfo.resultSummary?.tuplesCount ?? 0}, ${columns})`;

    const warningLines = getOperatorWarnings(opInfo);

    const metadataLines = [shapeLine, ...warningLines].filter(Boolean);

    const briefSummary = formatExecuteOperatorResult(operatorId);
    return [briefSummary, ...metadataLines, dataString].filter(Boolean).join("\n");
  } catch (error: any) {
    if (error.name === "AbortError") {
      throw error;
    }
    return createErrorResult(`Execution failed: ${error.message || String(error)}`);
  } finally {
    release();
  }
}

export function createExecuteOperatorTool(
  workflowState: WorkflowState,
  getConfig: () => ExecutionConfig,
  onResult?: (operatorId: string, operatorInfo: OperatorExecutionSummary) => void
) {
  return tool({
    description:
      "Execute the workflow and get the specified operator's result. The execution result(if succeeded) includes the shape of the input tables(if any) and output table, and the records in the output table",
    inputSchema: z.object({
      operatorId: z.string().describe("The operator ID to view result for."),
    }),
    execute: async (args: { operatorId: string }, options: { abortSignal?: AbortSignal }) => {
      const config = getConfig();
      return await executeOperatorAndFormat(workflowState, config, args.operatorId, { ...options, onResult });
    },
  });
}
