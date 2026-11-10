/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Observable, firstValueFrom, throwError } from "rxjs";
import { ApiClient, ApiError, type RequestOptions } from "../core/api-client";
import { MultipartUploadService, type MultipartUploadProgress } from "./multipart";
import { DATASET_FILE_RESOURCE_ENDPOINT, MODEL_FILE_RESOURCE_ENDPOINT } from "./storage-endpoints";
import { HttpParams, uploadHttp } from "./upload-http";

export type ResourceKind = "dataset" | "model";
export interface Contributor {
  name: string;
  creator: boolean;
  email?: string;
  affiliation?: string;
  comments?: string;
}
export interface StorageResource {
  did?: number;
  mid?: number;
  ownerUid?: number;
  name: string;
  description: string;
  isPublic: boolean;
  isDownloadable: boolean;
  creationTime?: number;
  coverImage?: string;
  framework?: string;
  format?: string;
  storagePath?: string;
  repositoryName?: string;
}
export interface StorageEntry {
  id: number;
  resource: StorageResource;
  isOwner: boolean;
  ownerEmail: string;
  accessPrivilege: "READ" | "WRITE" | "NONE";
  size: number;
  contributors?: Contributor[];
}
export interface StorageFileNode {
  name: string;
  type: "file" | "directory";
  parentDir: string;
  children?: StorageFileNode[];
  ownerEmail?: string;
  size?: number;
}
export interface StorageVersion {
  versionId: number;
  dvid?: number;
  mvid?: number;
  did?: number;
  mid?: number;
  creatorUid?: number;
  name: string;
  versionHash?: string;
  creationTime?: number;
  fileNodes?: StorageFileNode[];
}
export interface StagedFile {
  path: string;
  pathType: "file" | "directory";
  diffType: "added" | "removed" | "changed";
  sizeBytes?: number;
}
export type StorageCreate = Pick<
  StorageResource,
  "name" | "description" | "isPublic" | "isDownloadable" | "framework" | "format"
>;
export const MODEL_FRAMEWORKS = ["pytorch", "tensorflow", "onnx", "sklearn", "other"] as const;
export const MODEL_FORMATS = [
  "torchscript",
  "state-dict",
  "safetensors",
  "onnx",
  "savedmodel",
  "joblib",
  "pickle",
  "other",
] as const;
export function validStorageName(value: string): boolean {
  return value.length <= 128 && /^[A-Za-z0-9_-]+$/.test(value);
}
function identifier(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid resource identifier");
  return value;
}
function normalizeContributor(person: Contributor): Contributor {
  return {
    ...person,
    email: person.email?.trim() || undefined,
    affiliation: person.affiliation?.trim() || undefined,
    comments: person.comments?.trim() || undefined,
  };
}
function normalizeEntry(kind: ResourceKind, raw: Record<string, unknown>): StorageEntry {
  const resource = raw?.[kind] as StorageResource | undefined;
  const id = kind === "dataset" ? resource?.did : resource?.mid;
  if (!resource || typeof resource.name !== "string" || !Number.isSafeInteger(id))
    throw new Error("Invalid resource response");
  return {
    id: id!,
    resource,
    isOwner: raw.isOwner === true,
    ownerEmail: typeof raw.ownerEmail === "string" ? raw.ownerEmail : "",
    accessPrivilege: ["READ", "WRITE", "NONE"].includes(String(raw.accessPrivilege))
      ? (raw.accessPrivilege as StorageEntry["accessPrivilege"])
      : "NONE",
    size: typeof raw.size === "number" ? raw.size : 0,
    contributors: Array.isArray(raw.contributors) ? (raw.contributors as Contributor[]) : undefined,
  };
}
function normalizeVersion(kind: ResourceKind, raw: Omit<StorageVersion, "versionId">): StorageVersion {
  const id = kind === "dataset" ? raw.dvid : raw.mvid;
  if (!Number.isSafeInteger(id)) throw new Error("Invalid resource version response");
  return { ...raw, versionId: id! };
}

/** Dataset and model routes differ only in their family/key/version envelopes. */
export class StorageApi {
  private readonly uploader: MultipartUploadService;
  private readonly fetcher: typeof fetch;
  constructor(
    private readonly api: ApiClient,
    options: { fetch?: typeof fetch; getToken?: () => string | null } = {}
  ) {
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.uploader = new MultipartUploadService(
      uploadHttp(api),
      options.getToken ?? (() => globalThis.localStorage?.getItem("access_token") ?? null)
    );
  }
  async list(kind: ResourceKind, options?: RequestOptions): Promise<StorageEntry[]> {
    const records = await this.api.get<Record<string, unknown>[]>(`${kind}/list`, options);
    if (!Array.isArray(records)) throw new Error("Invalid resource response");
    return records.map(raw => normalizeEntry(kind, raw));
  }
  async get(kind: ResourceKind, id: number, publicView = false, options?: RequestOptions): Promise<StorageEntry> {
    return normalizeEntry(kind, await this.api.get(`${kind}/${publicView ? "public/" : ""}${identifier(id)}`, options));
  }
  async create(kind: ResourceKind, form: StorageCreate, contributors: Contributor[] = []): Promise<StorageEntry> {
    const body =
      kind === "dataset"
        ? {
            datasetName: form.name,
            datasetDescription: form.description,
            isDatasetPublic: form.isPublic,
            isDatasetDownloadable: form.isDownloadable,
            contributors: contributors.map(normalizeContributor),
          }
        : {
            modelName: form.name,
            modelDescription: form.description,
            isModelPublic: form.isPublic,
            isModelDownloadable: form.isDownloadable,
            framework: form.framework,
            format: form.format,
          };
    return normalizeEntry(kind, await this.api.post(`${kind}/create`, body));
  }
  async delete(kind: ResourceKind, id: number): Promise<void> {
    await this.api.delete(`${kind}/${identifier(id)}`);
  }
  async update(
    kind: ResourceKind,
    id: number,
    field: "name" | "description" | "framework" | "format",
    value: string
  ): Promise<void> {
    await this.api.post(`${kind}/update/${field}`, {
      [kind === "dataset" ? "did" : "mid"]: identifier(id),
      [field]: value,
    });
  }
  async toggle(kind: ResourceKind, id: number, field: "publicity" | "downloadable"): Promise<void> {
    await this.api.post(`${kind}/${identifier(id)}/update/${field}`, {});
  }
  async updateCover(kind: ResourceKind, id: number, coverImage: string): Promise<void> {
    await this.api.post(`${kind}/${identifier(id)}/update/cover`, { coverImage });
  }
  coverUrl(kind: ResourceKind, id: number): Promise<{ url: string | null }> {
    return this.api.get(`${kind}/${identifier(id)}/cover-url`);
  }
  async contributors(id: number, contributors: Contributor[]): Promise<void> {
    await this.api.post("dataset/update/contributors", {
      did: identifier(id),
      contributors: contributors.map(normalizeContributor),
    });
  }
  owners(kind: ResourceKind): Promise<string[]> {
    return this.api.get(`${kind}/user-${kind}-owners`);
  }
  async versions(
    kind: ResourceKind,
    id: number,
    publicView = false,
    options?: RequestOptions
  ): Promise<StorageVersion[]> {
    const rows = await this.api.get<Omit<StorageVersion, "versionId">[]>(
      `${kind}/${identifier(id)}/${publicView ? "publicVersion" : "version"}/list`,
      options
    );
    if (!Array.isArray(rows)) throw new Error("Invalid resource version response");
    return rows.map(raw => normalizeVersion(kind, raw));
  }
  async createVersion(kind: ResourceKind, id: number, name: string): Promise<StorageVersion> {
    const raw = await this.api.post<Record<string, unknown>>(`${kind}/${identifier(id)}/version/create`, name, {
      headers: { "Content-Type": "text/plain" },
    });
    return {
      ...normalizeVersion(kind, raw[`${kind}Version`] as Omit<StorageVersion, "versionId">),
      fileNodes: raw.fileNodes as StorageFileNode[],
    };
  }
  async fileTree(
    kind: ResourceKind,
    id: number,
    versionId: number,
    publicView = false,
    options?: RequestOptions
  ): Promise<{ fileNodes: StorageFileNode[]; size: number }> {
    return this.api.get(
      `${kind}/${identifier(id)}/${publicView ? "publicVersion" : "version"}/${identifier(versionId)}/rootFileNodes`,
      options
    );
  }
  versionZip(kind: ResourceKind, id: number, versionId?: number): Promise<Blob> {
    return this.api.get(
      `${kind}/${identifier(id)}/versionZip?${versionId === undefined ? "latest=true" : `${kind === "dataset" ? "dvid" : "mvid"}=${identifier(versionId)}`}`,
      { responseType: "blob" }
    );
  }
  async file(kind: ResourceKind, path: string, publicView = false, signal?: AbortSignal): Promise<Blob> {
    const { presignedUrl } = await this.api.get<{ presignedUrl: string }>(
      `${kind}/${publicView ? "public-presign-download" : "presign-download"}?filePath=${encodeURIComponent(path)}`,
      { signal }
    );
    if (typeof presignedUrl !== "string" || !presignedUrl.trim()) throw new Error("Invalid download URL");
    let url: URL;
    try {
      url = new URL(presignedUrl, globalThis.location?.origin ?? "http://localhost");
    } catch {
      throw new Error("Invalid download URL");
    }
    if (!["http:", "https:"].includes(url.protocol)) throw new Error("Invalid download URL");
    const response = await this.fetcher(url.href, { signal, credentials: "omit" });
    if (!response.ok) throw new ApiError(response.status, (await response.text()) || "File download failed");
    return response.blob();
  }
  diff(kind: ResourceKind, id: number, options?: RequestOptions): Promise<StagedFile[]> {
    return this.api.get(`${kind}/${identifier(id)}/diff`, options);
  }
  async resetFile(kind: ResourceKind, id: number, path: string): Promise<void> {
    const params = new HttpParams().set("filePath", encodeURIComponent(path));
    await this.api.put(`${kind}/${identifier(id)}/diff?${params}`, {});
  }
  async deleteFile(kind: ResourceKind, id: number, path: string): Promise<void> {
    const params = new HttpParams().set("filePath", encodeURIComponent(path));
    await this.api.delete(`${kind}/${identifier(id)}/file?${params}`);
  }
  async uploadSettings(
    kind: ResourceKind
  ): Promise<{ maxFileBytes: number; partSize: number; concurrency: number; concurrentFiles: number }> {
    const settings = await this.api.get<Record<string, string>>("config/settings/public");
    const endpoint = kind === "dataset" ? DATASET_FILE_RESOURCE_ENDPOINT : MODEL_FILE_RESOURCE_ENDPOINT;
    const number = (key: string, fallback: number) => {
      const result = Number.parseInt(settings[key] ?? "", 10);
      return Number.isFinite(result) && result > 0 ? result : fallback;
    };
    return {
      maxFileBytes: number(endpoint.maxFileSizeSettingKey, endpoint.defaultMaxFileSizeMiB) * 1024 ** 2,
      partSize: number(endpoint.chunkSizeSettingKey, 50) * 1024 ** 2,
      concurrency: number(endpoint.maxConcurrentChunksSettingKey, 10),
      concurrentFiles: number(endpoint.maxConcurrentFilesSettingKey, 3),
    };
  }
  upload(
    kind: ResourceKind,
    ownerEmail: string,
    name: string,
    path: string,
    file: File,
    partSize: number,
    concurrency: number,
    restart: boolean
  ): Observable<MultipartUploadProgress> {
    if (
      !Number.isSafeInteger(partSize) ||
      partSize <= 0 ||
      !Number.isSafeInteger(concurrency) ||
      concurrency <= 0 ||
      Math.ceil(file.size / partSize) > 10000
    )
      return throwError(() => new Error("Invalid upload limits"));
    return this.uploader.multipartUpload(
      kind === "dataset" ? DATASET_FILE_RESOURCE_ENDPOINT : MODEL_FILE_RESOURCE_ENDPOINT,
      ownerEmail,
      name,
      path,
      file,
      partSize,
      concurrency,
      restart
    );
  }
  pendingUploads(kind: ResourceKind, ownerEmail: string, name: string): Promise<string[]> {
    return firstValueFrom(
      this.uploader.listMultipartUploads(
        kind === "dataset" ? DATASET_FILE_RESOURCE_ENDPOINT : MODEL_FILE_RESOURCE_ENDPOINT,
        ownerEmail,
        name
      )
    );
  }
  existingFiles(kind: ResourceKind, id: number, files: { path: string; sizeBytes: number }[]): Promise<string[]> {
    return firstValueFrom(
      this.uploader.findExistingUploadFiles(
        kind === "dataset" ? DATASET_FILE_RESOURCE_ENDPOINT : MODEL_FILE_RESOURCE_ENDPOINT,
        identifier(id),
        files
      )
    );
  }
  async finalizeUpload(
    kind: ResourceKind,
    ownerEmail: string,
    name: string,
    path: string,
    abort: boolean
  ): Promise<void> {
    await firstValueFrom(
      this.uploader.finalizeMultipartUpload(
        kind === "dataset" ? DATASET_FILE_RESOURCE_ENDPOINT : MODEL_FILE_RESOURCE_ENDPOINT,
        ownerEmail,
        name,
        path,
        abort
      )
    );
  }
}
