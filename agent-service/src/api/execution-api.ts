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
 * Execution API types for Texera Agent Service.
 * These types match the backend SyncExecutionResource request/response.
 */

// ============================================================================
// Types matching the backend SyncExecutionResource
// ============================================================================

export interface LogicalLink {
  fromOpId: string;
  fromPortId: { id: number; internal: boolean };
  toOpId: string;
  toPortId: { id: number; internal: boolean };
}

export interface LogicalOperator {
  operatorID: string;
  operatorType: string;
  [key: string]: any;
}

export interface LogicalPlan {
  operators: LogicalOperator[];
  links: LogicalLink[];
  opsToViewResult?: string[];
  opsToReuseResult?: string[];
}

export interface WorkflowSettings {
  dataTransferBatchSize?: number;
  outputPortsNeedingStorage?: string[];
}

/** Request body for the sync execution API */
export interface SyncExecutionRequest {
  executionName: string;
  logicalPlan: {
    operators: LogicalOperator[];
    links: LogicalLink[];
    opsToViewResult?: string[];
    opsToReuseResult?: string[];
  };
  workflowSettings?: WorkflowSettings;
  targetOperatorIds: string[];
  timeoutSeconds?: number;
  maxResultRows?: number;
}
