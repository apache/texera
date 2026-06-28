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
import { createAuthHeaders } from "../auth/jwt";
import type { WorkflowContent } from "../types/workflow";
import type { Workflow, WorkflowPojo, WorkflowWithPrivilege, WorkflowPersistRequest } from "../types/dto";

const WORKFLOW_BASE_URL = "workflow";

// The backend serializes `content` as a JSON string; decode it into the
// in-memory WorkflowContent. Tolerate an already-parsed object too, since some
// callers/tests pass one.
function toWorkflow(raw: WorkflowPojo | WorkflowWithPrivilege): Workflow {
  const rawContent: unknown = raw.content;
  const content =
    typeof rawContent === "string" ? (JSON.parse(rawContent) as WorkflowContent) : (rawContent as WorkflowContent);
  return {
    wid: raw.wid,
    name: raw.name,
    description: raw.description,
    creationTime: raw.creationTime,
    lastModifiedTime: raw.lastModifiedTime,
    content,
  };
}

export async function persistWorkflow(
  token: string,
  wid: number,
  name: string,
  content: WorkflowContent,
  description?: string
): Promise<Workflow> {
  const { apiEndpoint } = getServiceEndpoints();
  const url = `${apiEndpoint}/api/${WORKFLOW_BASE_URL}/persist`;

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

  return toWorkflow((await response.json()) as WorkflowPojo);
}

export async function retrieveWorkflow(token: string, wid: number): Promise<Workflow> {
  const { apiEndpoint } = getServiceEndpoints();
  const url = `${apiEndpoint}/api/${WORKFLOW_BASE_URL}/${wid}`;

  const response = await fetch(url, {
    method: "GET",
    headers: createAuthHeaders(token),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to retrieve workflow: ${response.status} ${response.statusText} - ${errorText}`);
  }

  return toWorkflow((await response.json()) as WorkflowWithPrivilege);
}
