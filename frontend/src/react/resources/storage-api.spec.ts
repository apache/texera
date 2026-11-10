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
import { describe, expect, it, vi } from "vitest";
import { lastValueFrom } from "rxjs";
import { ApiClient } from "../core/api-client";
import { StorageApi, type ResourceKind } from "./storage-api";

function setup() {
  const fetch = vi.fn(async (url: string, init: RequestInit = {}) => {
    if (url.startsWith("https://storage.example")) return new Response("file text");
    if (url.includes("presign-download")) return Response.json({ presignedUrl: "https://storage.example/file" });
    if (url.includes("multipart-upload"))
      return Response.json({ missingParts: [], completedPartsCount: 0, filePaths: [] });
    if (url.includes("config/settings")) return Response.json({});
    const kind = url.includes("/model") ? "model" : "dataset";
    const record = {
      [kind]: {
        [kind === "model" ? "mid" : "did"]: 7,
        name: "Resource",
        description: "Description",
        isPublic: true,
        isDownloadable: true,
        framework: "onnx",
        format: "onnx",
      },
      ownerEmail: "owner@example.com",
      isOwner: true,
      accessPrivilege: "WRITE",
      size: 10,
    };
    if (url.includes("versionZip")) return new Response("zip");
    if (url.includes("rootFileNodes")) return Response.json({ fileNodes: [], size: 0 });
    if (url.includes("version/create"))
      return Response.json({
        [`${kind}Version`]: { [kind === "model" ? "mvid" : "dvid"]: 2, name: "v2" },
        fileNodes: [],
      });
    if (url.includes("version/list") || url.includes("publicVersion/list"))
      return Response.json([{ [kind === "model" ? "mvid" : "dvid"]: 1, name: "v1" }]);
    return Response.json(url.endsWith("/list") ? [record] : init.method ? record : record);
  });
  return {
    fetch,
    storage: new StorageApi(new ApiClient({ fetch, getToken: () => "token" }), { fetch, getToken: () => "token" }),
  };
}

describe.each<ResourceKind>(["dataset", "model"])("%s storage API", kind => {
  it("preserves public/private retrieval and normalizes backend envelopes", async () => {
    const { storage, fetch } = setup();
    expect((await storage.list(kind))[0].resource.name).toBe("Resource");
    expect((await storage.get(kind, 7)).id).toBe(7);
    await storage.get(kind, 7, true);
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `/api/${kind}/list`,
      `/api/${kind}/7`,
      `/api/${kind}/public/7`,
    ]);
  });
  it("sends resource-family create fields and preserves normalized optional contributors", async () => {
    const { storage, fetch } = setup();
    await storage.create(
      kind,
      {
        name: "Name",
        description: "Description",
        isPublic: false,
        isDownloadable: true,
        framework: "onnx",
        format: "onnx",
      },
      [{ name: "Name", creator: true, email: "  ", affiliation: " UCI ", comments: " " }]
    );
    const body = JSON.parse(String(fetch.mock.calls[0][1]?.body));
    expect(body).toMatchObject(
      kind === "dataset"
        ? {
            datasetName: "Name",
            datasetDescription: "Description",
            isDatasetPublic: false,
            isDatasetDownloadable: true,
            contributors: [{ name: "Name", creator: true, affiliation: "UCI" }],
          }
        : {
            modelName: "Name",
            modelDescription: "Description",
            isModelPublic: false,
            isModelDownloadable: true,
            framework: "onnx",
            format: "onnx",
          }
    );
  });
  it("preserves version bodies, public file trees and zip version query keys", async () => {
    const { storage, fetch } = setup();
    await storage.versions(kind, 7, true);
    await storage.fileTree(kind, 7, 1, true);
    const version = await storage.createVersion(kind, 7, "文 v2");
    expect(version.fileNodes).toEqual([]);
    expect(fetch.mock.calls[2][1]?.body).toBe("文 v2");
    expect(new Headers(fetch.mock.calls[2][1]?.headers).get("Content-Type")).toBe("text/plain");
    await storage.versionZip(kind, 7, 2);
    await storage.versionZip(kind, 7);
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `/api/${kind}/7/publicVersion/list`,
      `/api/${kind}/7/publicVersion/1/rootFileNodes`,
      `/api/${kind}/7/version/create`,
      `/api/${kind}/7/versionZip?${kind === "dataset" ? "dvid" : "mvid"}=2`,
      `/api/${kind}/7/versionZip?latest=true`,
    ]);
  });
  it("preserves metadata/toggle/cover/staged file contracts including pre-encoded paths", async () => {
    const { storage, fetch } = setup();
    await storage.update(kind, 7, "name", "Renamed");
    await storage.toggle(kind, 7, "publicity");
    await storage.toggle(kind, 7, "downloadable");
    await storage.updateCover(kind, 7, "v1/image.png");
    await storage.diff(kind, 7);
    await storage.resetFile(kind, 7, "folder/a+b.csv");
    await storage.deleteFile(kind, 7, "folder/a+b.csv");
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({
      [kind === "dataset" ? "did" : "mid"]: 7,
      name: "Renamed",
    });
    expect(fetch.mock.calls.slice(1, 5).map(([url]) => url)).toEqual([
      `/api/${kind}/7/update/publicity`,
      `/api/${kind}/7/update/downloadable`,
      `/api/${kind}/7/update/cover`,
      `/api/${kind}/7/diff`,
    ]);
    for (const call of fetch.mock.calls.slice(-2))
      expect(new URL(call[0], "https://texera.example").searchParams.get("filePath")).toBe(
        encodeURIComponent("folder/a+b.csv")
      );
  });
  it("downloads through the public presign endpoint without leaking a bearer token to object storage", async () => {
    const { storage, fetch } = setup();
    const blob = await storage.file(kind, "/folder/a+b.csv", true);
    const text = await new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result));
      reader.onerror = reject;
      reader.readAsText(blob);
    });
    expect(text).toBe("file text");
    expect(fetch.mock.calls[0][0]).toBe(`/api/${kind}/public-presign-download?filePath=%2Ffolder%2Fa%2Bb.csv`);
    const signed = fetch.mock.calls[1];
    expect(signed[0]).toBe("https://storage.example/file");
    expect(new Headers(signed[1]?.headers).has("Authorization")).toBe(false);
    expect(signed[1]?.credentials).toBe("omit");
  });
  it("uses the preserved resumable engine and validates malformed upload limits before network activity", async () => {
    const { storage, fetch } = setup();
    const file = new File([], "empty.txt");
    for (const [partSize, concurrency] of [
      [0, 1],
      [1, 0],
      [NaN, 1],
    ])
      await expect(
        lastValueFrom(storage.upload(kind, "o@e.com", "Name", "a+b.txt", file, partSize, concurrency, false))
      ).rejects.toThrow("upload limits");
    expect(fetch).not.toHaveBeenCalled();
    const finished = await lastValueFrom(storage.upload(kind, "o@e.com", "Name", "a+b.txt", file, 5, 1, false));
    expect(finished.status).toBe("finished");
    const init = new URL(fetch.mock.calls[0][0], "https://texera.example");
    expect(init.searchParams.get(`${kind}Name`)).toBe("Name");
    expect(init.searchParams.get("filePath")).toBe(encodeURIComponent("a+b.txt"));
    expect(new URL(fetch.mock.calls[1][0], "https://texera.example").searchParams.get("type")).toBe("finish");
  });
});

it("rejects malformed resource envelopes, unsafe signed URLs and invalid identifiers", async () => {
  const fetch = vi.fn(async (url: string) =>
    Response.json(url.includes("presign") ? { presignedUrl: "javascript:bad" } : {})
  );
  const storage = new StorageApi(new ApiClient({ fetch }), { fetch });
  await expect(storage.get("dataset", 1)).rejects.toThrow("resource response");
  await expect(storage.file("dataset", "file")).rejects.toThrow("download URL");
  await expect(storage.get("dataset", NaN)).rejects.toThrow("identifier");
});
it.each([undefined, null, "", "   "])(
  "rejects a missing or empty signed URL (%s) before downloading",
  async presignedUrl => {
    const fetch = vi.fn(async () => Response.json({ presignedUrl }));
    const storage = new StorageApi(new ApiClient({ fetch }), { fetch });
    await expect(storage.file("dataset", "file")).rejects.toThrow("download URL");
    expect(fetch).toHaveBeenCalledOnce();
  }
);
