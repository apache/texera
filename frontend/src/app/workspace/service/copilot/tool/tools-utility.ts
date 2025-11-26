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

// Tool execution timeout in milliseconds (2 minutes)
export const TOOL_TIMEOUT_MS = 120000;

// Maximum token limit for operator result data to prevent overwhelming LLM context
// Estimated as characters / 4 (common approximation for token counting)
export const MAX_OPERATOR_RESULT_TOKEN_LIMIT = 1000;

/**
 * Base interface for all tool execution results.
 * Ensures consistent structure across all tools with required tracking fields.
 */
export interface BaseToolResult {
  /**
   * Indicates whether the tool execution was successful.
   */
  success: boolean;

  /**
   * Error message if the tool execution failed.
   */
  error?: string;

  /**
   * List of operator IDs that were viewed/read during tool execution.
   * Empty array if no operators were viewed.
   */
  viewedOperatorIds: string[];

  /**
   * List of operator IDs that were modified/written during tool execution.
   * Empty array if no operators were modified.
   */
  modifiedOperatorIds: string[];
}

/**
 * Creates a successful tool result with default values for required fields.
 * Tools can extend this with additional custom fields.
 *
 * @param data - Custom data fields for the tool result
 * @param viewedOperatorIds - Operator IDs that were viewed (default: [])
 * @param modifiedOperatorIds - Operator IDs that were modified (default: [])
 * @returns BaseToolResult with success=true and provided data
 */
export function createSuccessResult<T extends Record<string, any>>(
  data: T,
  viewedOperatorIds: string[] = [],
  modifiedOperatorIds: string[] = []
): BaseToolResult & T {
  return {
    success: true,
    viewedOperatorIds,
    modifiedOperatorIds,
    ...data,
  };
}

/**
 * Creates a failed tool result with an error message.
 *
 * @param error - Error message describing the failure
 * @returns BaseToolResult with success=false and error message
 */
export function createErrorResult(error: string): BaseToolResult {
  return {
    success: false,
    error,
    viewedOperatorIds: [],
    modifiedOperatorIds: [],
  };
}

/**
 * Estimates the number of tokens in a JSON-serializable object
 * Uses a common approximation: tokens ≈ characters / 4
 */
export function estimateTokenCount(data: any): number {
  try {
    const jsonString = JSON.stringify(data);
    return Math.ceil(jsonString.length / 4);
  } catch (error) {
    // Fallback if JSON.stringify fails
    return 0;
  }
}

/**
 * Wraps a tool definition to add timeout protection to its execute function
 * Uses AbortController to properly cancel operations on timeout
 */
export function toolWithTimeout(toolConfig: any): any {
  const originalExecute = toolConfig.execute;

  return {
    ...toolConfig,
    execute: async (args: any) => {
      const abortController = new AbortController();

      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          abortController.abort();
          reject(new Error("timeout"));
        }, TOOL_TIMEOUT_MS);
      });

      try {
        const argsWithSignal = { ...args, signal: abortController.signal };
        return await Promise.race([originalExecute(argsWithSignal), timeoutPromise]);
      } catch (error: any) {
        if (error.message === "timeout") {
          return createErrorResult(
            "Tool execution timeout - operation took longer than 2 minutes. Please try again later."
          );
        }
        throw error;
      }
    },
  };
}
