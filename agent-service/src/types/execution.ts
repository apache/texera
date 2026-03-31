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
 * Execution types for Texera Agent Service.
 * These types match the backend SyncExecutionResource response exactly.
 */

// ============================================================================
// Console Message
// ============================================================================

/**
 * Simplified console message - just type and message.
 */
export interface ConsoleMessage {
  msgType: string;
  message: string;
}

// ============================================================================
// Port Shape
// ============================================================================

/**
 * Per-input-port shape info: rows and columns flowing through a port.
 */
export interface PortShape {
  portIndex: number;
  rows: number;
  columns: number;
}

// ============================================================================
// Operator Info
// ============================================================================

/**
 * Per-operator execution info returned by the sync API.
 * Result is always a JSON array - serialization to table/toon format is done in agent-service.
 */
export interface OperatorInfo {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: PortShape[]; // per-input-port (rows, columns)
  resultMode: string; // "table" or "visualization"
  result?: Record<string, any>[]; // JSON array of tuples
  totalRowCount?: number;
  displayedRows?: number;
  truncated?: boolean;
  consoleLogs?: ConsoleMessage[];
  error?: string;
  warnings?: string[];
  resultStatistics?: Record<string, string>; // column_name -> stats JSON from DataProfiler
}

// ============================================================================
// Execution Result
// ============================================================================

/**
 * Sync execution result from the backend.
 */
export interface SyncExecutionResult {
  success: boolean;
  state: string;
  operators: Record<string, OperatorInfo>;
  compilationErrors?: Record<string, string>;
  errors?: string[];
}
