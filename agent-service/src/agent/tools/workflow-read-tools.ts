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

// The listing uses plain markdown rather than XML-like tags to reduce format mimicry, where the model
// echoes the shape of what it read into its output instead of calling tools via the native protocol.

import { z } from "zod";
import { tool } from "ai";
import type { WorkflowState } from "../workflow-state";
import type { OperatorPredicate, OperatorPortSchemaMap, PortSchema } from "../../types/workflow";
import type { OperatorExecutionSummary } from "../../types/execution";
import type { WorkflowCompilationResponse, WorkflowFatalError } from "../../api/compile-api";
import { extractOperatorInputPortSchemaMap } from "../util/workflow-utils";
import { createErrorResult, formatOperatorOutput, getOperatorErrorText, tupleColumns } from "./tools-utility";

export const TOOL_NAME_GET_WORKFLOW = "get_workflow";
export const TOOL_NAME_GET_OPERATOR_RESULT = "get_operator_result";

/** An operator's latest result, and whether the workflow has changed under it since. */
export interface OperatorResultRecord {
  summary: OperatorExecutionSummary;
  /** True when the operator, or an operator it reads from, changed after the execution. */
  outdated: boolean;
}

export interface WorkflowReadContext {
  workflowState: WorkflowState;
  /** Compiles the workflow for its schemas and errors; null when the compile service gave no answer. */
  compileWorkflow: () => Promise<WorkflowCompilationResponse | null>;
  /** The operator's latest result on the conversation's current path, if it has one. */
  getResult: (operatorId: string) => OperatorResultRecord | undefined;
  /** The result size limit, read each time a result is returned. */
  getResultCharLimit: () => number | undefined;
}

export function createGetWorkflowTool(context: WorkflowReadContext) {
  return tool({
    description:
      "Get the workflow as it is now: each operator with its type, summary, properties, input and output schemas, " +
      "compilation error and whether it has a result, then the links. The result's records are not listed; " +
      `${TOOL_NAME_GET_OPERATOR_RESULT} returns them.`,
    inputSchema: z.object({}),
    execute: async () => await describeWorkflow(context),
  });
}

export function createGetOperatorResultTool(context: WorkflowReadContext) {
  return tool({
    description:
      "Get the result of an operator's latest execution in this conversation: the output table's shape and " +
      "records, or the error it failed with. It does not execute anything, and it says when the result is " +
      "outdated because the operator or one it reads from changed since.",
    inputSchema: z.object({
      operatorId: z.string().describe("The ID of the operator whose result to get."),
    }),
    execute: async (args: { operatorId: string }) => operatorResult(context, args.operatorId),
  });
}

/** What get_workflow returns: the operators in dataflow order, then the links. */
export async function describeWorkflow(context: WorkflowReadContext): Promise<string> {
  const { workflowState } = context;
  if (workflowState.getAllOperators().length === 0) return "The workflow is empty.";

  let compilation: WorkflowCompilationResponse | null = null;
  let compileFailure: string | undefined;
  try {
    compilation = await context.compileWorkflow();
    if (!compilation) compileFailure = "the compile service gave no answer";
  } catch (error: any) {
    compileFailure = error?.message || String(error);
  }

  const listing = serializeWorkflow(workflowState, context.getResult, compilation);
  return compileFailure ? `${listing}\n\nNo schemas: compiling the workflow failed (${compileFailure}).` : listing;
}

/** What get_operator_result returns for one operator. */
export function operatorResult(context: WorkflowReadContext, operatorId: string): string {
  if (!context.workflowState.getOperator(operatorId)) {
    return createErrorResult(`Operator ${operatorId} not found`);
  }
  const record = context.getResult(operatorId);
  if (!record) return `Operator ${operatorId} has no result: it has not been executed in this conversation.`;
  if (record.outdated) {
    return (
      `Operator ${operatorId}'s latest result is outdated: it or an operator it reads from changed after that ` +
      "execution. Execute it again for a current result."
    );
  }
  const error = getOperatorErrorText(record.summary);
  if (error) return `Operator ${operatorId} failed when it was last executed:\n${error}`;
  if (!record.summary.resultSummary) return `Operator ${operatorId} was executed and has no output table.`;
  // The shared output starts with "Executed operator <id>", which would read as an execution just now.
  const [, ...rest] = formatOperatorOutput(operatorId, record.summary, context.getResultCharLimit()).split("\n");
  return [`Latest result of operator ${operatorId}:`, ...rest].join("\n");
}

function serializeWorkflow(
  workflowState: WorkflowState,
  getResult: (operatorId: string) => OperatorResultRecord | undefined,
  compilation: WorkflowCompilationResponse | null
): string {
  const allOperators = workflowState.getAllOperators();
  const allLinks = workflowState.getAllLinks();

  const opIds = new Set(allOperators.map(op => op.operatorID));
  const inDegree = new Map<string, number>();
  const children = new Map<string, string[]>();
  for (const id of opIds) {
    inDegree.set(id, 0);
    children.set(id, []);
  }
  for (const link of allLinks) {
    children.get(link.source.operatorID)?.push(link.target.operatorID);
    inDegree.set(link.target.operatorID, (inDegree.get(link.target.operatorID) ?? 0) + 1);
  }
  const queue: string[] = [...opIds].filter(id => (inDegree.get(id) ?? 0) === 0);
  const topoOrder = new Map<string, number>();
  let rank = 0;
  while (queue.length > 0) {
    const node = queue.shift()!;
    topoOrder.set(node, rank++);
    for (const child of children.get(node) ?? []) {
      const newDeg = (inDegree.get(child) ?? 1) - 1;
      inDegree.set(child, newDeg);
      if (newDeg === 0) queue.push(child);
    }
  }

  const sortedOps = [...allOperators].sort(
    (a, b) => (topoOrder.get(a.operatorID) ?? 0) - (topoOrder.get(b.operatorID) ?? 0)
  );

  const outputSchemas = compilation?.operatorOutputSchemas ?? {};
  const compilationErrors = compilation?.operatorErrors ?? {};

  const lines: string[] = ["## Operators", ""];
  for (const op of sortedOps) {
    lines.push(
      serializeOperator(
        op,
        getResult(op.operatorID),
        extractOperatorInputPortSchemaMap(op.operatorID, op, outputSchemas, allLinks),
        outputSchemas[op.operatorID],
        compilationErrors[op.operatorID]
      )
    );
    lines.push("");
  }

  if (allLinks.length > 0) {
    const sortedLinks = [...allLinks].sort((a, b) => {
      const srcA = topoOrder.get(a.source.operatorID) ?? 0;
      const srcB = topoOrder.get(b.source.operatorID) ?? 0;
      if (srcA !== srcB) return srcA - srcB;
      return (topoOrder.get(a.target.operatorID) ?? 0) - (topoOrder.get(b.target.operatorID) ?? 0);
    });

    lines.push("## Links");
    for (const link of sortedLinks) {
      lines.push(`- ${link.source.operatorID} → ${link.target.operatorID}`);
    }
  }

  return lines.join("\n").trimEnd();
}

function serializeOperator(
  op: OperatorPredicate,
  result: OperatorResultRecord | undefined,
  inputSchemaMap?: OperatorPortSchemaMap,
  outputSchemaMap?: OperatorPortSchemaMap,
  compilationError?: WorkflowFatalError
): string {
  const lines: string[] = [];
  lines.push(`### Operator \`${op.operatorID}\` (${op.operatorType})`);
  lines.push(`Summary: ${op.customDisplayName || op.operatorID}`);

  if (inputSchemaMap) {
    for (const [portId, schema] of Object.entries(inputSchemaMap)) {
      if (schema) {
        lines.push(`Input Schema (port ${parsePortIndex(portId)}): ${formatSchema(schema)}`);
      }
    }
  }

  const props = op.operatorProperties;
  if (props && Object.keys(props).length > 0) {
    lines.push("Properties:");
    for (const [key, value] of Object.entries(props)) {
      if (value !== undefined && value !== null && value !== "") {
        const valueStr = typeof value === "string" ? value : JSON.stringify(value);
        lines.push(`  ${key}: ${valueStr}`);
      }
    }
  }

  if (outputSchemaMap) {
    const firstSchema = Object.values(outputSchemaMap).find(s => s !== undefined);
    if (firstSchema) {
      lines.push(`Output Schema: ${formatSchema(firstSchema)}`);
    }
  }

  if (compilationError) {
    lines.push(`Compilation Error: ${compilationError.message}`);
  }

  lines.push(`Result: ${describeResult(result)}`);
  return lines.join("\n");
}

/** One line on an operator's latest result: enough to know whether get_operator_result has anything. */
function describeResult(record: OperatorResultRecord | undefined): string {
  if (!record) return "none, not executed in this conversation";
  if (record.outdated) return "outdated, it or an operator it reads from changed after its last execution";
  const error = getOperatorErrorText(record.summary);
  if (error) return `failed: ${lastLine(error)}`;
  const summary = record.summary.resultSummary;
  if (!summary) return "executed, no output table";
  const rows = `${summary.totalTuplesCount} ${summary.totalTuplesCount === 1 ? "row" : "rows"}`;
  if (summary.sampleTuples.length === 0) return rows;
  const columns = tupleColumns(summary.sampleTuples[0][1]).length;
  return `${rows}, ${columns} ${columns === 1 ? "column" : "columns"}`;
}

/**
 * The last non-empty line of an error, which says the most: a failure's text starts with a fixed
 * header and section labels, and a Python traceback ends with the exception.
 */
function lastLine(error: string): string {
  const lines = error
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean);
  const last = lines[lines.length - 1] ?? error;
  return last.length > 300 ? `${last.slice(0, 300)}…` : last;
}

function formatSchema(schema: PortSchema): string {
  const attrs = schema.map(a => `${a.attributeName}: ${a.attributeType}`);
  return `[${attrs.join(", ")}]`;
}

/** The bare port ordinal of a wire port id such as "0_false". */
function parsePortIndex(portId: string): string {
  const idx = portId.indexOf("_");
  return idx >= 0 ? portId.substring(0, idx) : portId;
}
