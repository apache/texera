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

import { z } from "zod";
import { tool } from "ai";
import { importCsvAsDataset } from "../../data-source/dataset-import";
import { env } from "../../config/env";
import { createLogger } from "../../logger";

const log = createLogger("FetchApiDataTool");

export const TOOL_NAME_FETCH_API_DATA = "fetch_api_data";

interface FetchUrlResponse {
  url: string;
  contentType: string;
  format: "json" | "csv";
  byteLength: number;
  filename: string;
  rows: number;
  columns: string[];
  preview: any[];
  csv: string;
}

/**
 * Resolve the URL of the local /api/data-source/fetch-url endpoint. The tool
 * runs inside the same agent-service process, so we hit ourselves via loopback
 * rather than going through any external proxy.
 */
function internalFetchUrlEndpoint(): string {
  return `http://127.0.0.1:${env.PORT}${env.API_PREFIX}/data-source/fetch-url`;
}

/**
 * Pull a URL via the same /api/data-source/fetch-url endpoint the Datasets page
 * uses (loopback, no proxy), convert JSON/CSV to a tabular format, and save it
 * as a private Texera dataset.
 *
 * Why call the internal endpoint instead of fetching the URL directly? It
 * keeps the format-detection + JSON-flattening + CSV-preview logic in one
 * place, and it isolates the tool from any host networking quirks the agent
 * runtime may have — the endpoint is already verified to work via curl, so if
 * fetch_api_data used to fail while curl succeeded, this routing is the fix.
 *
 * Requires a user token in `getDelegateUserToken()` — without it, the tool can
 * fetch the URL but cannot create the dataset, so it returns the preview only.
 */
export function createFetchApiDataTool(getDelegateUserToken: () => string | undefined) {
  return tool({
    description: `Fetch data from a REST API URL and add it as a private dataset in Texera.

Use this for: "fetch data from <URL>", "load this API into the workflow", "pull patient data from <URL>".
The result includes a "filePath" (e.g. "imported-data/data.csv") that you should pass to a CSVFileScan
operator's "fileName" property in a follow-up addOperator call.

Input: { url: string, method?: 'GET'|'POST', headers?: {...}, format?: 'json'|'csv'|'auto' }
Output: { datasetName, filePath, rows, columns, preview }`,
    inputSchema: z.object({
      url: z.string().min(1).describe("HTTP(S) URL to fetch."),
      method: z.enum(["GET", "POST"]).optional().describe("HTTP method (default GET)."),
      headers: z
        .record(z.string())
        .optional()
        .describe("Optional request headers (e.g. Authorization). Sent verbatim to the upstream URL."),
      format: z
        .enum(["json", "csv", "auto"])
        .optional()
        .describe("Format of the response body. 'auto' detects from Content-Type and sniffs the body."),
      datasetName: z
        .string()
        .optional()
        .describe(
          "Optional dataset name. Will be sanitized (lowercase, hyphens). If omitted, a name is derived from the URL path."
        ),
    }),
    execute: async (args: {
      url: string;
      method?: "GET" | "POST";
      headers?: Record<string, string>;
      format?: "json" | "csv" | "auto";
      datasetName?: string;
    }) => {
      try {
        // Call our own /api/data-source/fetch-url. It already does
        // server-side fetch, format detection, JSON→CSV flattening, and CSV preview.
        const endpoint = internalFetchUrlEndpoint();
        log.info({ endpoint, url: args.url }, "fetch_api_data calling internal endpoint");
        const internalResponse = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            url: args.url,
            method: args.method || "GET",
            headers: args.headers || {},
            format: args.format || "auto",
          }),
        });
        if (!internalResponse.ok) {
          let detail = "";
          try {
            const errBody = (await internalResponse.json()) as { error?: string };
            detail = errBody?.error || "";
          } catch {
            detail = await internalResponse.text();
          }
          return {
            error: `/api/data-source/fetch-url returned ${internalResponse.status}: ${detail || internalResponse.statusText}`,
          };
        }
        const fetched = (await internalResponse.json()) as FetchUrlResponse;
        const { csv, columns, preview, rows, format: detectedFormat, byteLength } = fetched;

        const baseName = deriveBaseName(args.datasetName || urlBasename(args.url));
        const ts = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
        const datasetName = `${baseName}-${ts}`;
        const csvFileName = fetched.filename || `${baseName}.csv`;

        const userToken = getDelegateUserToken();
        if (!userToken) {
          // No user context; return the preview so the model has something useful.
          return {
            warning:
              "Fetched the URL but no user token is available in this agent — dataset was NOT created. Returning preview only.",
            url: args.url,
            format: detectedFormat,
            byteLength,
            rows,
            columns,
            preview,
          };
        }

        const created = await importCsvAsDataset({
          userToken,
          datasetName,
          description: `Fetched from ${args.url}`,
          fileName: csvFileName,
          csv,
        });

        return {
          datasetName: created.datasetName,
          did: created.did,
          filePath: created.filePath,
          format: detectedFormat,
          rows,
          columns,
          preview,
          message: `Fetched ${rows} rows, ${columns.length} columns from ${args.url} and saved as dataset "${created.datasetName}". Use this filePath in CSVFileScan.fileName to load it: ${created.filePath}`,
        };
      } catch (err: any) {
        log.error({ err: err?.message || err }, "fetch_api_data tool failed");
        return { error: err?.message || String(err) };
      }
    },
  });
}

function urlBasename(url: string): string {
  try {
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.length > 0) return segments[segments.length - 1];
    return parsed.hostname;
  } catch {
    return "api-data";
  }
}

function deriveBaseName(value: string): string {
  const stripped = value.replace(/\.[^.]+$/, "");
  const slug = stripped
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || "api-data";
}
