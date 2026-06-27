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

import { getServiceEndpoints } from "../config/endpoints";
import { env } from "../config/env";
import type { LogicalPlan } from "../types/workflow";
import type { SyncExecutionResult } from "../types/execution";
import { DEFAULT_AGENT_SETTINGS, type ExecutionRequestParams } from "../types/agent";
import { createLogger } from "../logger";

const log = createLogger("ExecutionClient");

/**
 * POSTs a logical plan to the Workflow Execution Service and returns the
 * synchronous run result. Network/abort errors are surfaced; non-abort
 * failures are logged and returned as an error-state result.
 */
export async function executeWorkflowHttp(
  params: ExecutionRequestParams,
  logicalPlan: LogicalPlan,
  options: { abortSignal?: AbortSignal } = {}
): Promise<SyncExecutionResult> {
  const workflowId = params.workflowId;
  const computingUnitId = params.computingUnitId ?? 0;

  // In k8s each computing unit is a separate pod, so the endpoint varies per cuid.
  const executionEndpoint = env.EXECUTION_ENDPOINT_TEMPLATE
    ? env.EXECUTION_ENDPOINT_TEMPLATE.replace("{cuid}", String(computingUnitId))
    : getServiceEndpoints().executionEndpoint;

  const url = `${executionEndpoint}/api/execution/${workflowId}/${computingUnitId}/run`;

  const timeoutSeconds = params.executionTimeoutMs
    ? Math.ceil(params.executionTimeoutMs / 1000)
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
    maxOperatorResultCharLimit: params.maxOperatorResultCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit,
    maxOperatorResultCellCharLimit:
      params.maxOperatorResultCellCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCellCharLimit,
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
        Authorization: `Bearer ${params.userToken}`,
      },
      body: JSON.stringify(request),
      signal: options.abortSignal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Execution request failed: ${response.status} ${response.statusText} - ${errorText}`);
    }

    return (await response.json()) as SyncExecutionResult;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw error;
    }
    log.error({ err: error }, "execution failed");
    return {
      success: false,
      state: "Error",
      operators: {},
      errors: [error instanceof Error ? error.message : "Unknown error"],
    };
  }
}
