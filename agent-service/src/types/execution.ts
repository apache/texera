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

interface ConsoleMessage {
  msgType: string;
  message: string;
}

interface PortShape {
  portIndex: number;
  rows: number;
  columns: number;
}

export interface OperatorInfo {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: PortShape[];
  resultMode: string;
  result?: Record<string, any>[];
  totalRowCount?: number;
  displayedRows?: number;
  truncated?: boolean;
  consoleLogs?: ConsoleMessage[];
  error?: string;
  warnings?: string[];
  resultStatistics?: Record<string, string>;
}

export interface SyncExecutionResult {
  success: boolean;
  state: string;
  operators: Record<string, OperatorInfo>;
  compilationErrors?: Record<string, string>;
  errors?: string[];
}
