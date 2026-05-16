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

export interface UserDatasetSummary {
  did: number;
  name: string;
  description: string;
  ownerEmail: string;
  isPublic: boolean;
  isOwner: boolean;
  /** Path prefix that File Scan operators use to reference files in this dataset. */
  pathPrefix: string;
}

/**
 * Fetches the user's accessible datasets from the dashboard service's
 * `/api/dataset/list` endpoint (defined in file-service DatasetResource). Returns
 * a flat summary suitable for injection into the agent system prompt.
 *
 * On any error (token expired, service down, malformed payload) returns [] —
 * the agent should still work without this context.
 */
export async function fetchUserDatasetSummaries(userToken: string): Promise<UserDatasetSummary[]> {
  const url = `${getBackendConfig().fileServiceEndpoint}/api/dataset/list`;
  let resp: Response;
  try {
    resp = await fetch(url, { method: "GET", headers: createAuthHeaders(userToken) });
  } catch {
    return [];
  }
  if (!resp.ok) return [];
  let body: unknown;
  try {
    body = await resp.json();
  } catch {
    return [];
  }
  if (!Array.isArray(body)) return [];

  return body
    .map((raw): UserDatasetSummary | null => {
      const r = (raw ?? {}) as Record<string, unknown>;
      const dataset = (r.dataset ?? {}) as Record<string, unknown>;
      const did = Number(dataset.did);
      const name = typeof dataset.name === "string" ? dataset.name : undefined;
      const ownerEmail = typeof r.ownerEmail === "string" ? r.ownerEmail : "";
      if (!Number.isFinite(did) || !name) return null;
      const description = typeof dataset.description === "string" ? dataset.description : "";
      return {
        did,
        name,
        description,
        ownerEmail,
        isPublic: Boolean(dataset.isPublic),
        isOwner: Boolean(r.isOwner),
        pathPrefix: ownerEmail ? `/${ownerEmail}/${name}` : `/${name}`,
      };
    })
    .filter((d): d is UserDatasetSummary => d !== null);
}
