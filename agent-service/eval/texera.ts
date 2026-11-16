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

// The calls the evaluation makes to Texera as the user: sign in, make and remove the workflows the
// cases run on, and put the orders file in a dataset. Workflows are read and saved with the agent
// service's own client (`src/api/workflow-api.ts`), so the evaluation sees them as the agent does.

import { getBackendConfig } from "../src/api/backend-api";
import { createAuthHeaders, extractUserFromToken } from "../src/api/auth-api";
import type { WorkflowContent } from "../src/types/workflow";

async function call(token: string | undefined, url: string, init: RequestInit = {}): Promise<Response> {
  const headers = { ...(token ? createAuthHeaders(token) : { "Content-Type": "application/json" }), ...init.headers };
  const response = await fetch(url, { ...init, headers });
  if (!response.ok) {
    throw new Error(`${init.method ?? "GET"} ${url} failed: ${response.status} ${await response.text()}`);
  }
  return response;
}

const dashboard = () => `${getBackendConfig().apiEndpoint}/api`;

export async function login(username: string, password: string): Promise<string> {
  const response = await call(undefined, `${dashboard()}/auth/login`, {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  const { accessToken } = (await response.json()) as { accessToken?: string };
  if (!accessToken) throw new Error(`login as ${username} returned no token`);
  return accessToken;
}

export const EMPTY_WORKFLOW: WorkflowContent = {
  operators: [],
  operatorPositions: {},
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400 } as WorkflowContent["settings"],
};

export async function createWorkflow(token: string, name: string, content: WorkflowContent): Promise<number> {
  const response = await call(token, `${dashboard()}/workflow/create`, {
    method: "POST",
    body: JSON.stringify({ name, content: JSON.stringify(content) }),
  });
  const created = (await response.json()) as { workflow: { wid: number } };
  return created.workflow.wid;
}

export async function deleteWorkflows(token: string, wids: number[]): Promise<void> {
  if (wids.length === 0) return;
  await call(token, `${dashboard()}/workflow/delete`, { method: "POST", body: JSON.stringify({ wids }) });
}

/**
 * Makes sure the user owns a dataset holding the given file, and returns the file's path in the
 * form the file scan operators take: `/<owner email>/<dataset>/<version>/<file>`. A dataset of that
 * name the user already owns is reused as it is.
 */
export async function ensureDataset(
  token: string,
  fileService: string,
  datasetName: string,
  fileName: string,
  content: string
): Promise<string> {
  const owner = extractUserFromToken(token).email;
  const api = `${fileService}/api/dataset`;
  const listed = (await (await call(token, `${api}/list`)).json()) as Array<{
    dataset: { did: number; name: string };
    isOwner: boolean;
  }>;
  let did = listed.find(d => d.isOwner && d.dataset.name === datasetName)?.dataset.did;

  if (did === undefined) {
    const created = (await (
      await call(token, `${api}/create`, {
        method: "POST",
        body: JSON.stringify({
          datasetName,
          datasetDescription: "Orders made by the agent evaluation (agent-service/eval).",
          isDatasetPublic: false,
        }),
      })
    ).json()) as { dataset: { did: number } };
    did = created.dataset.did;

    const bytes = new TextEncoder().encode(content);
    const target = `ownerEmail=${encodeURIComponent(owner)}&datasetName=${encodeURIComponent(datasetName)}&filePath=${encodeURIComponent(fileName)}`;
    await call(
      token,
      `${api}/multipart-upload?type=init&${target}&fileSizeBytes=${bytes.length}&partSizeBytes=${bytes.length}`,
      {
        method: "POST",
      }
    );
    await call(token, `${api}/multipart-upload/part?${target}&partNumber=1`, {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream" },
      body: bytes,
    });
    await call(token, `${api}/multipart-upload?type=finish&${target}`, { method: "POST" });
    await call(token, `${api}/${did}/version/create`, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: "",
    });
  }

  const latest = (await (await call(token, `${api}/${did}/version/latest`)).json()) as {
    datasetVersion: { name: string };
  };
  return `/${owner}/${datasetName}/${latest.datasetVersion.name}/${fileName}`;
}
