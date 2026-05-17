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

export type ImportRequest =
  | { sourceType: "url"; url: string; name: string; description?: string }
  | { sourceType: "pubmed"; pubmedId: string; name: string; description?: string }
  | { sourceType: "who"; whoIndicator: string; name: string; description?: string };

export interface ImportResult {
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

function guessFilenameFromUrl(name: string, sourceUrl: string): string {
  try {
    const u = new URL(sourceUrl);
    const last = u.pathname.split("/").filter(Boolean).pop();
    if (last && /\.[a-zA-Z0-9]+$/.test(last)) return last;
  } catch {
    // fall through
  }
  return `${sanitizeDatasetName(name)}.csv`;
}

async function fetchUrlBytes(url: string, timeoutMs = FETCH_TIMEOUT_MS): Promise<ArrayBuffer> {
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

async function fetchUrlJson<T = unknown>(url: string, timeoutMs = FETCH_TIMEOUT_MS): Promise<T> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
      signal: controller.signal,
      redirect: "follow",
    });
    if (!resp.ok) {
      throw new DatasetImportError(`Source responded ${resp.status} ${resp.statusText}`, 502);
    }
    return (await resp.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

async function fetchUrlText(url: string, timeoutMs = FETCH_TIMEOUT_MS): Promise<string> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { method: "GET", signal: controller.signal, redirect: "follow" });
    if (!resp.ok) {
      throw new DatasetImportError(`Source responded ${resp.status} ${resp.statusText}`, 502);
    }
    return await resp.text();
  } finally {
    clearTimeout(t);
  }
}

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (s.includes(",") || s.includes('"') || s.includes("\n") || s.includes("\r")) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function toCsv(headers: string[], rows: unknown[][]): Uint8Array {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(row.map(csvEscape).join(","));
  }
  return new TextEncoder().encode(lines.join("\n") + "\n");
}

// ---------- PubMed: eFetch a single PMID, parse 5 fields, emit 1-row CSV ----------

function stripTags(s: string): string {
  return s.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}

function extractAllMatches(xml: string, tag: string): string[] {
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, "g");
  const out: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    out.push(stripTags(m[1]));
  }
  return out;
}

function extractFirst(xml: string, tag: string): string {
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`);
  const m = re.exec(xml);
  return m ? stripTags(m[1]) : "";
}

async function buildPubmedCsv(pmid: string): Promise<{ bytes: Uint8Array; fileName: string }> {
  if (!/^\d+$/.test(pmid)) {
    throw new DatasetImportError(`Invalid PubMed PMID: ${pmid}`, 400);
  }
  const xml = await fetchUrlText(
    `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id=${pmid}`
  );
  // Extract just the <PubmedArticle> block we care about — pmid is unique so
  // exactly one article should be returned.
  const articleMatch = /<PubmedArticle\b[\s\S]*?<\/PubmedArticle>/.exec(xml);
  if (!articleMatch) {
    throw new DatasetImportError(`PubMed returned no article for PMID ${pmid}.`, 502);
  }
  const article = articleMatch[0];
  const title = extractFirst(article, "ArticleTitle");
  const abstractParts = extractAllMatches(article, "AbstractText");
  const abstract = abstractParts.join(" ");
  const journal = extractFirst(article, "Title"); // Journal/Title is the journal name
  const year = extractFirst(article, "Year");
  const authorBlocks = article.match(/<Author\b[^>]*>[\s\S]*?<\/Author>/g) ?? [];
  const authors = authorBlocks
    .map(block => {
      const last = extractFirst(block, "LastName");
      const initials = extractFirst(block, "Initials");
      return (last + (initials ? " " + initials : "")).trim();
    })
    .filter(Boolean)
    .slice(0, 12)
    .join("; ");
  const headers = ["pmid", "title", "abstract", "authors", "journal", "year"];
  const bytes = toCsv(headers, [[pmid, title, abstract, authors, journal, year]]);
  const fileName = `pubmed-${pmid}.csv`;
  return { bytes, fileName };
}

// ---------- WHO: GHO indicator JSON → (Country, Year, Sex, Value) CSV ----------

interface GhoRow {
  SpatialDim?: string;
  TimeDim?: number | string;
  Dim1?: string;
  NumericValue?: number | null;
  Value?: string | null;
}
interface GhoResponse {
  value?: GhoRow[];
}

async function buildWhoCsv(indicator: string): Promise<{ bytes: Uint8Array; fileName: string }> {
  if (!/^[A-Z0-9_]+$/i.test(indicator)) {
    throw new DatasetImportError(`Invalid WHO indicator code: ${indicator}`, 400);
  }
  const json = await fetchUrlJson<GhoResponse>(`https://ghoapi.azureedge.net/api/${indicator}`);
  const items = json?.value ?? [];
  if (items.length === 0) {
    throw new DatasetImportError(`WHO indicator ${indicator} returned no rows.`, 502);
  }
  const rows = items.map(r => [
    r.SpatialDim ?? "",
    r.TimeDim ?? "",
    r.Dim1 ?? "",
    r.NumericValue !== undefined && r.NumericValue !== null ? r.NumericValue : "",
    r.Value ?? "",
  ]);
  const bytes = toCsv(["country", "year", "sex", "numeric_value", "value"], rows);
  return { bytes, fileName: `who-${indicator}.csv` };
}

// ---------- Shared upload pipeline ----------

async function uploadToTexera(
  token: string,
  bytes: Uint8Array,
  fileName: string,
  datasetName: string,
  description: string
): Promise<ImportResult> {
  const base = getBackendConfig().fileServiceEndpoint;
  const user = extractUserFromToken(token);
  if (!user.email) {
    throw new DatasetImportError("Token does not include the user's email.", 401);
  }
  if (bytes.byteLength === 0) {
    throw new DatasetImportError("Source returned an empty file.", 502);
  }

  const authHeaders = createAuthHeaders(token);

  // 1. createDataset
  const createUrl = `${base}/api/dataset/create`;
  log.info({ url: createUrl, datasetName, fileBytes: bytes.byteLength }, "creating dataset");
  const createResp = await fetch(createUrl, {
    method: "POST",
    headers: authHeaders,
    body: JSON.stringify({
      datasetName,
      datasetDescription: description,
      isDatasetPublic: false,
      isDatasetDownloadable: true,
    }),
  });
  if (!createResp.ok) {
    const text = await createResp.text();
    log.error({ url: createUrl, status: createResp.status, body: text }, "createDataset failed");
    throw new DatasetImportError(`createDataset failed: ${createResp.status} ${text}`, createResp.status);
  }
  const created = (await createResp.json()) as { dataset?: { did?: number } };
  const did = created.dataset?.did;
  if (did === undefined || did === null) {
    throw new DatasetImportError("createDataset returned no dataset id.", 502);
  }

  // 2. init multipart upload
  const initUrl =
    `${base}/api/dataset/multipart-upload` +
    `?type=init` +
    `&ownerEmail=${encodeURIComponent(user.email)}` +
    `&datasetName=${encodeURIComponent(datasetName)}` +
    `&filePath=${encodeURIComponent(encodeURIComponent(fileName))}` +
    `&fileSizeBytes=${bytes.byteLength}` +
    `&partSizeBytes=${PART_SIZE_BYTES}` +
    `&restart=false`;
  log.info({ url: initUrl, did }, "multipart-upload init");
  const initResp = await fetch(initUrl, { method: "POST", headers: authHeaders, body: "{}" });
  if (!initResp.ok) {
    const text = await initResp.text();
    log.error({ url: initUrl, status: initResp.status, body: text }, "init failed");
    throw new DatasetImportError(`multipart-upload init failed: ${initResp.status} ${text}`, initResp.status);
  }
  const init = (await initResp.json()) as { missingParts: number[]; completedPartsCount: number };

  // 3. upload each missing part
  for (const partNumber of init.missingParts ?? []) {
    const start = (partNumber - 1) * PART_SIZE_BYTES;
    const end = Math.min(start + PART_SIZE_BYTES, bytes.byteLength);
    const chunk = bytes.subarray(start, end);
    const partUrl =
      `${base}/api/dataset/multipart-upload/part` +
      `?ownerEmail=${encodeURIComponent(user.email)}` +
      `&datasetName=${encodeURIComponent(datasetName)}` +
      `&filePath=${encodeURIComponent(encodeURIComponent(fileName))}` +
      `&partNumber=${partNumber}`;
    const partResp = await fetch(partUrl, {
      method: "POST",
      headers: { Authorization: authHeaders.Authorization, "Content-Type": "application/octet-stream" },
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

  // 4. finish
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

  // 5. publish v1
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

  log.info({ did, datasetName, fileBytes: bytes.byteLength }, "dataset import succeeded");
  return { did, datasetName, fileName, fileSize: bytes.byteLength };
}

/**
 * Server-side dataset import for the Dataset Bank page. The frontend can't
 * fetch arbitrary catalog URLs (CORS), so we do it here and drive the
 * existing dashboard upload pipeline with the caller's bearer token.
 *
 * Source-specific behavior:
 *   - "url":    download the URL verbatim (UCI / Kaggle / dkNET direct files)
 *   - "pubmed": fetch NCBI eFetch for pubmedId, emit a 1-row CSV with
 *               (pmid, title, abstract, authors, journal, year)
 *   - "who":    fetch GHO indicator JSON, emit a (country, year, sex,
 *               numeric_value, value) CSV across all returned rows
 */
export async function importDataset(token: string, req: ImportRequest): Promise<ImportResult> {
  const datasetName = sanitizeDatasetName(req.name);
  const description = req.description ?? "";

  let bytes: Uint8Array;
  let fileName: string;

  if (req.sourceType === "url") {
    log.info({ sourceType: req.sourceType, url: req.url }, "fetching source");
    const buf = await fetchUrlBytes(req.url);
    bytes = new Uint8Array(buf);
    fileName = guessFilenameFromUrl(req.name, req.url);
  } else if (req.sourceType === "pubmed") {
    log.info({ sourceType: req.sourceType, pubmedId: req.pubmedId }, "fetching pubmed");
    const built = await buildPubmedCsv(req.pubmedId);
    bytes = built.bytes;
    fileName = built.fileName;
  } else {
    log.info({ sourceType: req.sourceType, whoIndicator: req.whoIndicator }, "fetching WHO");
    const built = await buildWhoCsv(req.whoIndicator);
    bytes = built.bytes;
    fileName = built.fileName;
  }

  return uploadToTexera(token, bytes, fileName, datasetName, description);
}
