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

import { getBackendConfig } from "../api/backend-api";
import { createAuthHeaders } from "../api/auth-api";

export interface CreatedDataset {
  did: number;
  datasetName: string;
  ownerEmail: string;
  filePath: string;
}

interface DashboardDataset {
  isOwner: boolean;
  ownerEmail: string;
  dataset: {
    did?: number;
    name: string;
    description: string;
  };
}

/**
 * Push a CSV string into Texera as a new private dataset and commit an initial
 * version. Used by the `fetch_api_data` agent tool so it can hand the result
 * back to the workflow as a real, addressable dataset file.
 *
 * Returns the dataset name + filePath that a CSVFileScan / TableFileScan
 * operator can reference.
 */
export async function importCsvAsDataset(opts: {
  userToken: string;
  datasetName: string;
  description: string;
  fileName: string;
  csv: string;
}): Promise<CreatedDataset> {
  const { userToken, datasetName, description, fileName, csv } = opts;
  const config = getBackendConfig();
  const baseUrl = `${config.apiEndpoint}/api/dataset`;
  const authHeaders = createAuthHeaders(userToken);

  // 1. Create the dataset record
  const createRes = await fetch(`${baseUrl}/create`, {
    method: "POST",
    headers: {
      ...authHeaders,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      datasetName,
      datasetDescription: description,
      isDatasetPublic: false,
      isDatasetDownloadable: false,
    }),
  });
  if (!createRes.ok) {
    const text = await createRes.text();
    throw new Error(`Failed to create dataset: ${createRes.status} ${text}`);
  }
  const dashboard = (await createRes.json()) as DashboardDataset;
  const did = dashboard.dataset.did;
  if (!did) {
    throw new Error("Dataset created but missing did in response.");
  }

  // 2. Multipart upload — for the modest CSV sizes the URL fetcher returns,
  //    a single part is plenty.
  const bytes = new TextEncoder().encode(csv);
  const fileSize = bytes.byteLength;
  const partSize = Math.max(1, fileSize); // single part covers the whole file

  const initParams = new URLSearchParams({
    type: "init",
    ownerEmail: dashboard.ownerEmail,
    datasetName,
    filePath: encodeURIComponent(fileName),
    fileSizeBytes: String(fileSize),
    partSizeBytes: String(partSize),
    restart: "false",
  });
  const initRes = await fetch(`${baseUrl}/multipart-upload?${initParams.toString()}`, {
    method: "POST",
    headers: authHeaders,
  });
  if (!initRes.ok) {
    const text = await initRes.text();
    throw new Error(`Failed to init upload: ${initRes.status} ${text}`);
  }
  const initBody = (await initRes.json()) as { missingParts: number[] };
  const missingParts = initBody?.missingParts ?? [1];

  for (const partNumber of missingParts) {
    const partParams = new URLSearchParams({
      ownerEmail: dashboard.ownerEmail,
      datasetName,
      filePath: encodeURIComponent(fileName),
      partNumber: String(partNumber),
    });
    const partRes = await fetch(`${baseUrl}/multipart-upload/part?${partParams.toString()}`, {
      method: "POST",
      headers: {
        ...authHeaders,
        "Content-Type": "application/octet-stream",
      },
      body: bytes,
    });
    if (!partRes.ok) {
      const text = await partRes.text();
      throw new Error(`Failed to upload part ${partNumber}: ${partRes.status} ${text}`);
    }
  }

  const finishParams = new URLSearchParams({
    type: "finish",
    ownerEmail: dashboard.ownerEmail,
    datasetName,
    filePath: encodeURIComponent(fileName),
  });
  const finishRes = await fetch(`${baseUrl}/multipart-upload?${finishParams.toString()}`, {
    method: "POST",
    headers: authHeaders,
  });
  if (!finishRes.ok) {
    const text = await finishRes.text();
    throw new Error(`Failed to finish upload: ${finishRes.status} ${text}`);
  }

  // 3. Commit an initial version so the dataset is readable
  const versionRes = await fetch(`${baseUrl}/${did}/version/create`, {
    method: "POST",
    headers: {
      ...authHeaders,
      "Content-Type": "text/plain",
    },
    body: "Imported via fetch_api_data",
  });
  if (!versionRes.ok) {
    const text = await versionRes.text();
    throw new Error(`Failed to create version: ${versionRes.status} ${text}`);
  }

  return {
    did,
    datasetName,
    ownerEmail: dashboard.ownerEmail,
    filePath: `${datasetName}/${fileName}`,
  };
}
