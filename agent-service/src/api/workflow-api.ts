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
 * Workflow API client for Texera Agent Service.
 * Handles workflow persistence operations against the Texera backend.
 */

import { getBackendConfig } from "./backend-api";
import { createAuthHeaders } from "./auth-api";
import type { WorkflowContent } from "../types/workflow";

// ============================================================================
// Types
// ============================================================================

export interface Workflow {
  wid: number;
  name: string;
  description?: string;
  content: WorkflowContent;
  creationTime?: number;
  lastModifiedTime?: number;
  isPublished?: boolean;
}

export interface WorkflowPersistRequest {
  wid?: number;
  name: string;
  description?: string;
  content: string; // JSON stringified WorkflowContent
  isPublic?: boolean;
}

export interface WorkflowCreateRequest {
  name: string;
  content: string; // JSON stringified WorkflowContent
}

export interface DashboardWorkflow {
  workflow: Workflow;
  ownerName?: string;
  accessLevel?: string;
  projectsContainingWorkflow?: number[];
}

// ============================================================================
// API Functions
// ============================================================================

const WORKFLOW_BASE_URL = "workflow";

/**
 * Create a new workflow.
 * @param token - JWT token for authentication
 * @param name - Workflow name
 * @param content - Workflow content
 * @returns Created workflow info
 */
export async function createWorkflow(
  token: string,
  name: string,
  content: WorkflowContent
): Promise<DashboardWorkflow> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/create`;

  const response = await fetch(url, {
    method: "POST",
    headers: createAuthHeaders(token),
    body: JSON.stringify({
      name,
      content: JSON.stringify(content),
    } as WorkflowCreateRequest),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to create workflow: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data: DashboardWorkflow = await response.json();

  // Parse the content string back to object if needed
  if (typeof data.workflow.content === "string") {
    data.workflow.content = JSON.parse(data.workflow.content as unknown as string);
  }

  return data;
}

/**
 * Persist (update) an existing workflow.
 * @param token - JWT token for authentication
 * @param wid - Workflow ID
 * @param name - Workflow name
 * @param content - Workflow content
 * @param description - Optional description
 * @returns Updated workflow
 */
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

  const data: Workflow = await response.json();

  // Parse the content string back to object if needed
  if (typeof data.content === "string") {
    data.content = JSON.parse(data.content as unknown as string);
  }

  return data;
}

/**
 * Retrieve a workflow by ID.
 * @param token - JWT token for authentication
 * @param wid - Workflow ID
 * @returns Workflow data
 */
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

  const data: Workflow = await response.json();

  // Parse the content string back to object if needed
  if (typeof data.content === "string") {
    data.content = JSON.parse(data.content as unknown as string);
  }

  return data;
}

/**
 * List all workflows for the authenticated user.
 * @param token - JWT token for authentication
 * @returns Array of dashboard workflows
 */
export async function listWorkflows(token: string): Promise<DashboardWorkflow[]> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/list`;

  const response = await fetch(url, {
    method: "GET",
    headers: createAuthHeaders(token),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to list workflows: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data: DashboardWorkflow[] = await response.json();

  // Parse content strings
  for (const item of data) {
    if (typeof item.workflow.content === "string") {
      item.workflow.content = JSON.parse(item.workflow.content as unknown as string);
    }
  }

  return data;
}

/**
 * Delete workflows by IDs.
 * @param token - JWT token for authentication
 * @param wids - Array of workflow IDs to delete
 */
export async function deleteWorkflows(token: string, wids: number[]): Promise<void> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/delete`;

  const response = await fetch(url, {
    method: "POST",
    headers: createAuthHeaders(token),
    body: JSON.stringify({ wids }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to delete workflows: ${response.status} ${response.statusText} - ${errorText}`);
  }
}

/**
 * Update workflow name.
 * @param token - JWT token for authentication
 * @param wid - Workflow ID
 * @param name - New name
 */
export async function updateWorkflowName(token: string, wid: number, name: string): Promise<void> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/${WORKFLOW_BASE_URL}/update/name`;

  const response = await fetch(url, {
    method: "POST",
    headers: createAuthHeaders(token),
    body: JSON.stringify({ wid, name }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to update workflow name: ${response.status} ${response.statusText} - ${errorText}`);
  }
}
