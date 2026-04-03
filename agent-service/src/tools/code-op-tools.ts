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
 * Code operator tools for Texera Agent Service (CODE mode).
 * These tools work with Python code operators (DataLoading, DataProcessing).
 */

import { z } from "zod";
import { tool } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import type { OperatorPredicate, OperatorLink } from "../types/workflow";
import { autoLayoutWorkflow } from "../workflow/auto-layout";
import { WorkflowUtilService } from "../workflow/workflow-util";
import {
  createToolResult,
  createErrorResult,
  formatAddOperatorResult,
  formatModifyOperatorResult,
  formatOperatorError,
} from "./tools-utility";
import type { ToolContext } from "./workflow-tools";

// ============================================================================
// Tool Name Constants
// ============================================================================

export const TOOL_NAME_CREATE_OR_MODIFY_OPERATOR = "createOrModifyOperator";

// ============================================================================
// Operator Types for Code Blocks
// ============================================================================

const DATA_PROCESSING_OPERATOR_TYPE = "DataProcessing";
const DATA_LOADING_OPERATOR_TYPE = "DataLoading";

// ============================================================================
// Code Block Parsing and Validation
// ============================================================================

/**
 * Result of parsing a Python code block.
 */
interface CodeBlockParseResult {
  type: "DataLoading" | "DataProcessing";
  functionName: string;
  parameters: string[];
  numInputPorts: number;
}

/**
 * Python reserved keywords that cannot be used as variable names.
 */
const PYTHON_KEYWORDS = new Set([
  "False", "None", "True", "and", "as", "assert", "async", "await",
  "break", "class", "continue", "def", "del", "elif", "else", "except",
  "finally", "for", "from", "global", "if", "import", "in", "is",
  "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try",
  "while", "with", "yield"
]);

/**
 * Validates if a string is a valid Python variable name.
 * Returns null if valid, error message if invalid.
 */
export function validatePythonVariableName(name: string): string | null {
  if (!name || name.length === 0) {
    return "Variable name cannot be empty";
  }

  // Check if it matches Python identifier pattern: starts with letter or underscore,
  // followed by letters, digits, or underscores
  const validPattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  if (!validPattern.test(name)) {
    return `'${name}' is not a valid Python variable name. ` +
      `Must start with a letter or underscore, followed by letters, digits, or underscores.`;
  }

  // Check if it's a Python keyword
  if (PYTHON_KEYWORDS.has(name)) {
    return `'${name}' is a Python reserved keyword and cannot be used as a variable name.`;
  }

  return null; // Valid
}

/**
 * Parses a Python code block to determine its type and extract function info.
 *
 * DataLoading: def load() - EXACTLY "load", no parameters, source operator
 * DataProcessing: def process(table1, table2) - EXACTLY "process", parameters become input ports
 */
function parseCodeBlock(code: string): CodeBlockParseResult | { error: string } {
  // Match function definition: def func_name(params) or def func_name(params) -> ReturnType:
  const funcPattern = /def\s+(\w+)\s*\(([^)]*)\)/;
  const match = funcPattern.exec(code);

  if (!match) {
    return { error: "No valid Python function definition found. Expected: def load() or def process(...):" };
  }

  const functionName = match[1];
  const paramsStr = match[2].trim();

  // Parse parameters (handle type annotations like "table1: pd.DataFrame")
  const parameters: string[] = paramsStr
    ? paramsStr
        .split(",")
        .map(p => p.trim())
        .map(p => p.split(":")[0].trim()) // Remove type annotations
        .filter(p => p.length > 0)
    : [];

  // Strict function name validation
  // DataLoading: MUST be exactly "load" with no parameters
  // DataProcessing: MUST be exactly "process" with at least one parameter
  if (functionName === "load") {
    if (parameters.length > 0) {
      return {
        error: `Function "load" must have no parameters. Found parameters: [${parameters.join(", ")}]. ` +
          `For data processing with inputs, use "def process(${parameters.join(", ")})".`,
      };
    }
    return {
      type: "DataLoading",
      functionName,
      parameters: [],
      numInputPorts: 0,
    };
  }

  if (functionName === "process") {
    if (parameters.length === 0) {
      return {
        error: `Function "process" must have at least one parameter representing input data. ` +
          `Example: def process(data) -> pd.DataFrame: ...`,
      };
    }
    return {
      type: "DataProcessing",
      functionName,
      parameters,
      numInputPorts: parameters.length,
    };
  }

  // Invalid function name
  return {
    error: `Invalid function name "${functionName}". Function name must be exactly "load" or "process".\n` +
      `- Use "def load() -> pd.DataFrame:" for data loading (no input ports)\n` +
      `- Use "def process(input1, input2, ...) -> pd.DataFrame:" for data processing (with input ports)`,
  };
}

/**
 * Find an operator by its ID (variable name) in the workflow.
 * Returns the operator if found, undefined otherwise.
 */
function findOperatorByName(workflowState: WorkflowState, operatorName: string): OperatorPredicate | undefined {
  return workflowState.getOperator(operatorName);
}

/**
 * Format operator content for error messages.
 */
function formatOperatorContent(operator: OperatorPredicate): string {
  const props = operator.operatorProperties || {};
  const code = props.code ? `\n  code: ${props.code.substring(0, 100)}...` : "";
  return `{type: ${operator.operatorType}, displayName: "${operator.customDisplayName || operator.operatorID}"${code}}`;
}

/**
 * Validates Python syntax using the Python interpreter.
 * Returns null if valid, error message if invalid.
 */
async function validatePythonSyntax(code: string): Promise<string | null> {
  const { spawn } = await import("child_process");

  return new Promise(resolve => {
    // Use Python's ast module to check syntax
    const pythonCode = `
import ast
import sys
try:
    ast.parse('''${code.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}''')
    sys.exit(0)
except SyntaxError as e:
    print(f"Line {e.lineno}: {e.msg}", file=sys.stderr)
    sys.exit(1)
`;

    const process = spawn("python3", ["-c", pythonCode]);
    let stderr = "";

    process.stderr.on("data", data => {
      stderr += data.toString();
    });

    process.on("close", exitCode => {
      if (exitCode === 0) {
        resolve(null);
      } else {
        resolve(stderr.trim() || "Python syntax error");
      }
    });

    process.on("error", err => {
      // If Python is not available, skip syntax validation
      console.warn("[addCodeBlock] Python not available for syntax validation:", err.message);
      resolve(null);
    });

    // Timeout after 5 seconds
    setTimeout(() => {
      process.kill();
      resolve(null); // Don't fail if timeout
    }, 5000);
  });
}

// ============================================================================
// Unified Coding Tool (Add or Modify)
// ============================================================================

/**
 * Validates input parameters for process() functions and returns input operators.
 */
function validateInputParameters(
  workflowState: WorkflowState,
  parameters: string[]
): { inputOperators: OperatorPredicate[] } | { error: string } {
  const inputOperators: OperatorPredicate[] = [];
  const missingParams: string[] = [];

  for (const param of parameters) {
    const inputOp = findOperatorByName(workflowState, param);
    if (!inputOp) {
      missingParams.push(param);
    } else {
      inputOperators.push(inputOp);
    }
  }

  if (missingParams.length > 0) {
    return {
      error: `Input parameter(s) not found as operators: [${missingParams.join(", ")}]. ` +
        `Each parameter in def process(${parameters.join(", ")}) must reference an existing operator.`,
    };
  }

  return { inputOperators };
}

/**
 * Creates links from input operators to target operator.
 */
function createInputLinks(
  workflowState: WorkflowState,
  operatorId: string,
  inputOperators: OperatorPredicate[]
): string[] {
  const createdLinkIds: string[] = [];

  for (let i = 0; i < inputOperators.length; i++) {
    const sourceOp = inputOperators[i];
    const linkId = `${sourceOp.operatorID}-->${operatorId}`;

    const link: OperatorLink = {
      linkID: linkId,
      source: {
        operatorID: sourceOp.operatorID,
        portID: sourceOp.outputPorts[0]?.portID || "output-0",
      },
      target: {
        operatorID: operatorId,
        portID: `input-${i}`,
      },
    };

    workflowState.addLink(link);
    createdLinkIds.push(linkId);
  }

  return createdLinkIds;
}

export function createCreateOrModifyOperatorTool(
  workflowState: WorkflowState,
  operatorSchemas: Map<string, any>,
  context?: ToolContext
) {
  const workflowUtil = context?.metadataStore ? new WorkflowUtilService(context.metadataStore, workflowState) : null;

  return tool({
    description: `Add or modify a Python function as an operator in the dataflow, then automatically execute it.
- If operatorId does NOT exist: creates a new operator
- If operatorId exists: modifies the existing operator (must keep same type)
- After creation/modification, the operator is automatically executed.
- retrieveResult: if true, execution result and metadata are included; if false, only success/failure is reported (saves tokens). Errors are always reported.

RULES:
1. operatorId must be a valid Python variable name
2. Function name MUST be exactly "load" or "process"
3. For process(): each parameter MUST match an existing operator's operatorId - links are auto-created/updated

## def load() -> pd.DataFrame
Load data from files. No input parameters. File I/O allowed.

Example: operatorId="customers"
  def load() -> pd.DataFrame:
      return pd.read_csv('/data/customers.csv')

## def process(opId1, opId2, ...) -> pd.DataFrame
Transform input data. Each parameter references an existing operator id. Links between operators will be auto-created.

Example: operatorId="filtered" (requires "customers" to exist)
  def process(customers) -> pd.DataFrame:
      return customers[customers['age'] > 18]
  # Creates link: customers-->filtered`,
    inputSchema: z.object({
      operatorId: z.string().describe(
        "Unique operator name (valid Python variable). Other operators reference this as input parameter."
      ),
      code: z.string().describe("Python function: def load() or def process(...)"),
      summary: z.string().optional().describe("Detailed summary of the operator behavior. For load() operators, include the filename(s) being loaded."),
    }),
    execute: async (args: { operatorId: string; code: string; summary?: string }) => {
      const coordinator = context?.parallelCoordinator;
      try {
        const { operatorId, code, summary } = args;

        // Validate operatorId
        const nameValidationError = validatePythonVariableName(operatorId);
        if (nameValidationError) {
          return createErrorResult(`Invalid operatorId: ${nameValidationError}`);
        }

        // Parse code block
        const parseResult = parseCodeBlock(code);
        if ("error" in parseResult) {
          return createErrorResult(parseResult.error);
        }

        const { type, numInputPorts, parameters } = parseResult;
        const isDataProcessing = type === "DataProcessing";

        // Register with parallel coordinator (sync, before first await).
        // This ensures all sibling parallel calls register before any of them wait.
        coordinator?.register(operatorId);

        // Validate Python syntax
        const syntaxError = await validatePythonSyntax(code);
        if (syntaxError) {
          return createErrorResult(`Python syntax error: ${syntaxError}`);
        }

        // Validate input parameters for process() functions
        let inputOperators: OperatorPredicate[] = [];
        if (isDataProcessing && parameters.length > 0) {
          // Wait for dependencies being created by sibling parallel calls
          await coordinator?.waitForDependencies(parameters, id => !!workflowState.getOperator(id));

          const validation = validateInputParameters(workflowState, parameters);
          if ("error" in validation) {
            return createErrorResult(validation.error);
          }
          inputOperators = validation.inputOperators;
        }

        const existingOperator = findOperatorByName(workflowState, operatorId);
        const displayName = summary || operatorId;

        let resultMsg: string;
        let createdLinkPairs: { source: string; target: string }[] = [];
        let deletedLinkPairs: { source: string; target: string }[] = [];

        if (!existingOperator) {
          // === ADD NEW OPERATOR ===
          const operatorType = isDataProcessing ? DATA_PROCESSING_OPERATOR_TYPE : DATA_LOADING_OPERATOR_TYPE;

          const schemaEntry = operatorSchemas.get(operatorType);
          if (!schemaEntry) {
            return createErrorResult(`Operator type "${operatorType}" is not available.`);
          }

          if (!workflowUtil) {
            return createErrorResult("Metadata store not available for operator creation");
          }

          let operator = workflowUtil.getNewOperatorPredicate(operatorType, displayName);
          operator = {
            ...operator,
            operatorID: operatorId,
            operatorProperties: { ...operator.operatorProperties, code },
          };

          workflowState.addOperator(operator);

          if (isDataProcessing && numInputPorts > 1) {
            workflowState.updateOperatorInputPorts(operatorId, numInputPorts);
          }

          if (isDataProcessing && inputOperators.length > 0) {
            createInputLinks(workflowState, operatorId, inputOperators);
            createdLinkPairs = inputOperators.map(src => ({ source: src.operatorID, target: operatorId }));
          }

          autoLayoutWorkflow(workflowState);

          const finalOperator = workflowState.getOperator(operatorId) || operator;
          resultMsg = formatAddOperatorResult(
            operatorId, finalOperator.inputPorts.length, finalOperator.outputPorts.length,
            createdLinkPairs.length > 0 ? createdLinkPairs : undefined
          );
        } else {
          // === MODIFY EXISTING OPERATOR ===
          const isExistingDataLoading = existingOperator.operatorType === DATA_LOADING_OPERATOR_TYPE;
          const isExistingDataProcessing = existingOperator.operatorType === DATA_PROCESSING_OPERATOR_TYPE;

          if (!isExistingDataLoading && !isExistingDataProcessing) {
            return createErrorResult(
              `Operator ${operatorId} is not a code operator. ` +
              `Expected: ${DATA_LOADING_OPERATOR_TYPE} or ${DATA_PROCESSING_OPERATOR_TYPE}, got: ${existingOperator.operatorType}`
            );
          }

          const expectedType = isExistingDataLoading ? "DataLoading" : "DataProcessing";
          if (type !== expectedType) {
            return createErrorResult(
              `Type mismatch: ${operatorId} is ${expectedType}, but code is ${type}. ` +
              `Use ${expectedType === "DataLoading" ? "load()" : "process(...)"}.`
            );
          }

          workflowState.updateOperatorProperties(operatorId, { code });
          if (summary) {
            workflowState.updateOperatorDisplayName(operatorId, summary);
          }

          if (isExistingDataProcessing) {
            if (numInputPorts !== existingOperator.inputPorts.length) {
              workflowState.updateOperatorInputPorts(operatorId, numInputPorts);
            }

            // Delete existing incoming links and create new ones
            const currentLinks = workflowState.getLinksConnectedToOperator(operatorId)
              .filter(link => link.target.operatorID === operatorId);
            for (const link of currentLinks) {
              deletedLinkPairs.push({ source: link.source.operatorID, target: link.target.operatorID });
              workflowState.deleteLink(link.linkID);
            }

            if (inputOperators.length > 0) {
              createInputLinks(workflowState, operatorId, inputOperators);
              createdLinkPairs = inputOperators.map(src => ({ source: src.operatorID, target: operatorId }));
            }
          }

          resultMsg = formatModifyOperatorResult(
            operatorId,
            createdLinkPairs.length > 0 ? createdLinkPairs : undefined,
            deletedLinkPairs.length > 0 ? deletedLinkPairs : undefined
          );
        }

        return createToolResult(resultMsg);
      } catch (error: any) {
        return createErrorResult(formatOperatorError(args.operatorId, error.message || String(error)));
      } finally {
        coordinator?.markDone(args.operatorId);
      }
    },
  });
}
