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

import { getBackendConfig } from "./backend-api";
import { createAuthHeaders } from "./auth-api";
import type { WorkflowContent } from "../types/workflow";

export interface Workflow {
  wid: number;
  name: string;
  description?: string;
  content: WorkflowContent;
  creationTime?: number;
  lastModifiedTime?: number;
  isPublished?: boolean;
}

interface WorkflowPersistRequest {
  wid?: number;
  name: string;
  description?: string;
  content: string;
  isPublic?: boolean;
}

const WORKFLOW_BASE_URL = "workflow";

export async function persistWorkflow(
  token: string,
  wid: number,
  name: string,
  content: WorkflowContent,
  description?: string
): Promise<Workflow> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/persist`;

  const response = await fetch(url, {
    method: "POST",
    headers: createAuthHeaders(token),
    body: JSON.stringify({
      wid,
      name,
      description: description || "",
      content: JSON.stringify(content),
      isPublic: false,
    } as WorkflowPersistRequest),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to persist workflow: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data = (await response.json()) as Workflow;
  if (typeof data.content === "string") {
    data.content = JSON.parse(data.content as unknown as string);
  }
  return data;
}

export async function createWorkflow(token: string, name: string): Promise<number> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/create`;

  const emptyContent: WorkflowContent = {
    operators: [],
    commentBoxes: [],
    links: [],
    operatorPositions: {},
    settings: { dataTransferBatchSize: 400, executionMode: "pipelined" } as any,
  };

  const response = await fetch(url, {
    method: "POST",
    headers: createAuthHeaders(token),
    body: JSON.stringify({ name, content: JSON.stringify(emptyContent) }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to create workflow: ${response.status} - ${text}`);
  }

  const data = (await response.json()) as { workflow?: { wid: number }; wid?: number };
  const wid = data.workflow?.wid ?? (data as any).wid;
  if (!wid) throw new Error("Created workflow has no wid");
  return wid;
}

export async function retrieveWorkflow(token: string, wid: number): Promise<Workflow> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/${wid}`;

  const response = await fetch(url, {
    method: "GET",
    headers: createAuthHeaders(token),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to retrieve workflow: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data = (await response.json()) as Workflow;
  if (typeof data.content === "string") {
    data.content = JSON.parse(data.content as unknown as string);
  }
  return data;
}
