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
import { createAuthHeaders, extractUserFromToken } from "./auth-api";
import { createLogger } from "../logger";

const log = createLogger("dataset-import");

const PART_SIZE_BYTES = 5 * 1024 * 1024; // 5 MB
const FETCH_TIMEOUT_MS = 60_000;

export interface ImportFromUrlRequest {
  url: string;
  name: string;
  description?: string;
}

export interface ImportFromUrlResult {
  did: number;
  datasetName: string;
  fileName: string;
  fileSize: number;
}

export class DatasetImportError extends Error {
  constructor(
    message: string,
    public readonly status: number
  ) {
    super(message);
    this.name = "DatasetImportError";
  }
}

function sanitizeDatasetName(name: string): string {
  const cleaned = name
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  return cleaned || "imported-dataset";
}

function guessFilename(name: string, sourceUrl: string): string {
  try {
    const u = new URL(sourceUrl);
    const last = u.pathname.split("/").filter(Boolean).pop();
    if (last && /\.[a-zA-Z0-9]+$/.test(last)) return last;
  } catch {
    // fall through
  }
  return `${sanitizeDatasetName(name)}.csv`;
}

async function fetchWithTimeout(url: string, timeoutMs = FETCH_TIMEOUT_MS): Promise<ArrayBuffer> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { method: "GET", signal: controller.signal, redirect: "follow" });
    if (!resp.ok) {
      throw new DatasetImportError(`Source responded ${resp.status} ${resp.statusText}`, 502);
    }
    return await resp.arrayBuffer();
  } finally {
    clearTimeout(t);
  }
}

/**
 * Server-side dataset import. The frontend can't fetch arbitrary catalog URLs
 * because of CORS; this proxy runs in agent-service (Bun/Node) and has no such
 * restriction, then drives the existing dashboard dataset endpoints with the
 * caller's bearer token forwarded verbatim.
 *
 * Pipeline mirrors the browser flow in DatasetService.multipartUpload:
 *   1. createDataset                              -> did
 *   2. multipart-upload?type=init                 -> missingParts[]
 *   3. multipart-upload/part?partNumber=N         (per chunk)
 *   4. multipart-upload?type=finish               -> committed
 *   5. {did}/version/create  body "v1"            -> published
 */
export async function importDatasetFromUrl(
  token: string,
  req: ImportFromUrlRequest
): Promise<ImportFromUrlResult> {
  // Dataset endpoints (create / multipart-upload / version/create) are served
  // by file-service (port 9092), not amber/dashboard.
  const base = getBackendConfig().fileServiceEndpoint;
  const user = extractUserFromToken(token);
  if (!user.email) {
    throw new DatasetImportError("Token does not include the user's email.", 401);
  }
  const datasetName = sanitizeDatasetName(req.name);
  const fileName = guessFilename(req.name, req.url);

  // 0. Fetch the source file.
  const buffer = await fetchWithTimeout(req.url);
  const file = new Uint8Array(buffer);
  if (file.byteLength === 0) {
    throw new DatasetImportError("Source returned an empty file.", 502);
  }

  const authHeaders = createAuthHeaders(token);
  // JSON requests use the auth headers as-is (already include Content-Type: application/json);
  // raw chunk uploads override Content-Type below.

  // 1. Create the dataset.
  const createUrl = `${base}/api/dataset/create`;
  log.info({ url: createUrl, datasetName, fileBytes: file.byteLength }, "creating dataset");
  const createResp = await fetch(createUrl, {
    method: "POST",
    headers: authHeaders,
    body: JSON.stringify({
      datasetName,
      datasetDescription: req.description ?? "",
      isDatasetPublic: false,
      isDatasetDownloadable: true,
    }),
  });
  if (!createResp.ok) {
    const text = await createResp.text();
    log.error({ url: createUrl, status: createResp.status, body: text }, "createDataset failed");
    throw new DatasetImportError(`createDataset failed: ${createResp.status} ${text}`, createResp.status);
  }
  const created = (await createResp.json()) as { dataset?: { did?: number; name?: string } };
  const did = created.dataset?.did;
  if (did === undefined || did === null) {
    throw new DatasetImportError("createDataset returned no dataset id.", 502);
  }

  // 2. Init multipart upload.
  const initUrl =
    `${base}/api/dataset/multipart-upload` +
    `?type=init` +
    `&ownerEmail=${encodeURIComponent(user.email)}` +
    `&datasetName=${encodeURIComponent(datasetName)}` +
    `&filePath=${encodeURIComponent(encodeURIComponent(fileName))}` +
    `&fileSizeBytes=${file.byteLength}` +
    `&partSizeBytes=${PART_SIZE_BYTES}` +
    `&restart=false`;
  log.info({ url: initUrl, did }, "multipart-upload init");
  const initResp = await fetch(initUrl, { method: "POST", headers: authHeaders, body: "{}" });
  if (!initResp.ok) {
    const text = await initResp.text();
    log.error({ url: initUrl, status: initResp.status, body: text }, "multipart-upload init failed");
    throw new DatasetImportError(`multipart-upload init failed: ${initResp.status} ${text}`, initResp.status);
  }
  const init = (await initResp.json()) as { missingParts: number[]; completedPartsCount: number };
  const missing = init.missingParts ?? [];

  // 3. Upload each missing part.
  for (const partNumber of missing) {
    const start = (partNumber - 1) * PART_SIZE_BYTES;
    const end = Math.min(start + PART_SIZE_BYTES, file.byteLength);
    const chunk = file.subarray(start, end);

    const partUrl =
      `${base}/api/dataset/multipart-upload/part` +
      `?ownerEmail=${encodeURIComponent(user.email)}` +
      `&datasetName=${encodeURIComponent(datasetName)}` +
      `&filePath=${encodeURIComponent(encodeURIComponent(fileName))}` +
      `&partNumber=${partNumber}`;
    const partResp = await fetch(partUrl, {
      method: "POST",
      headers: {
        Authorization: authHeaders.Authorization,
        "Content-Type": "application/octet-stream",
      },
      body: chunk,
    });
    if (!partResp.ok) {
      const text = await partResp.text();
      log.error({ url: partUrl, partNumber, status: partResp.status, body: text }, "part upload failed");
      throw new DatasetImportError(
        `multipart-upload part ${partNumber} failed: ${partResp.status} ${text}`,
        partResp.status
      );
    }
  }

  // 4. Finish multipart upload.
  const finishUrl =
    `${base}/api/dataset/multipart-upload` +
    `?type=finish` +
    `&ownerEmail=${encodeURIComponent(user.email)}` +
    `&datasetName=${encodeURIComponent(datasetName)}` +
    `&filePath=${encodeURIComponent(encodeURIComponent(fileName))}`;
  log.info({ url: finishUrl }, "multipart-upload finish");
  const finishResp = await fetch(finishUrl, { method: "POST", headers: authHeaders, body: "{}" });
  if (!finishResp.ok) {
    const text = await finishResp.text();
    log.error({ url: finishUrl, status: finishResp.status, body: text }, "finish failed");
    throw new DatasetImportError(`multipart-upload finish failed: ${finishResp.status} ${text}`, finishResp.status);
  }

  // 5. Publish v1.
  const versionUrl = `${base}/api/dataset/${did}/version/create`;
  log.info({ url: versionUrl, did }, "creating version v1");
  const versionResp = await fetch(versionUrl, {
    method: "POST",
    headers: { Authorization: authHeaders.Authorization, "Content-Type": "text/plain" },
    body: "v1",
  });
  if (!versionResp.ok) {
    const text = await versionResp.text();
    log.error({ url: versionUrl, status: versionResp.status, body: text }, "createDatasetVersion failed");
    throw new DatasetImportError(`createDatasetVersion failed: ${versionResp.status} ${text}`, versionResp.status);
  }

  log.info({ did, datasetName, fileBytes: file.byteLength }, "dataset import succeeded");
  return { did, datasetName, fileName, fileSize: file.byteLength };
}
