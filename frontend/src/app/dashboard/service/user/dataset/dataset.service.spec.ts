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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom } from "rxjs";

import { DATASET_BASE_URL, DatasetService, MultipartUploadProgress, validateDatasetName } from "./dataset.service";
import { AppSettings } from "../../../../common/app-setting";
import { AuthService } from "../../../../common/service/user/auth.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";

const API = "api";

function buildDataset(overrides: Partial<Dataset> = {}): Dataset {
  return {
    did: 1,
    ownerUid: 1,
    name: "ds",
    isPublic: false,
    isDownloadable: false,
    storagePath: undefined,
    description: "",
    creationTime: undefined,
    coverImage: undefined,
    ...overrides,
  };
}

function buildDashboardDataset(overrides: Partial<DashboardDataset> = {}): DashboardDataset {
  return {
    isOwner: true,
    ownerEmail: "owner@example.com",
    dataset: buildDataset(),
    accessPrivilege: "WRITE",
    size: 0,
    ...overrides,
  };
}

function buildDatasetVersion(overrides: Partial<DatasetVersion> = {}): DatasetVersion {
  return {
    dvid: 5,
    did: 1,
    creatorUid: 1,
    name: "v1",
    versionHash: "abc",
    creationTime: 0,
    fileNodes: undefined,
    ...overrides,
  };
}

const SAMPLE_FILE_NODES: DatasetFileNode[] = [
  { name: "root", type: "directory", parentDir: "", children: [] as DatasetFileNode[] } as DatasetFileNode,
];

class FakeXMLHttpRequest {
  static instances: FakeXMLHttpRequest[] = [];

  // Capturing upload target so tests can drive `upload.progress` events.
  readonly upload = {
    listeners: new Map<string, EventListener[]>(),
    addEventListener(type: string, listener: EventListener): void {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    },
  };
  status = 0;
  url = "";
  readonly requestHeaders = new Map<string, string>();
  private listeners = new Map<string, EventListener[]>();

  open(_method: string, url: string): void {
    this.url = url;
  }

  setRequestHeader(name: string, value: string): void {
    this.requestHeaders.set(name, value);
  }

  send(): void {
    FakeXMLHttpRequest.instances.push(this);
  }

  abort(): void {}

  addEventListener(type: string, listener: EventListener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  /** Drives the `upload.progress` listener registered by the service. */
  emitProgress(loaded: number, lengthComputable = true): void {
    const event = { lengthComputable, loaded } as unknown as Event;
    for (const listener of this.upload.listeners.get("progress") ?? []) {
      listener(event);
    }
  }

  respond(status: number): void {
    this.status = status;
    this.emit("load");
  }

  fail(): void {
    this.emit("error");
  }

  private emit(type: string): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(new Event(type));
    }
  }
}

describe("validateDatasetName", () => {
  it("returns null for a valid name", () => {
    expect(validateDatasetName("my-dataset_1")).toBeNull();
  });

  it("returns null for a single valid character", () => {
    expect(validateDatasetName("a")).toBeNull();
  });

  it("returns null for a name exactly at the 128-character limit", () => {
    expect(validateDatasetName("a".repeat(128))).toBeNull();
  });

  it("returns an error for an empty string", () => {
    expect(validateDatasetName("")).not.toBeNull();
  });

  it("returns an error for names with spaces", () => {
    expect(validateDatasetName("has space")).not.toBeNull();
  });

  it("returns an error for names with dots", () => {
    expect(validateDatasetName("dot.dot")).not.toBeNull();
  });

  it("returns an error for names with slashes", () => {
    expect(validateDatasetName("a/b")).not.toBeNull();
  });

  it("returns an error for names with non-ASCII characters", () => {
    expect(validateDatasetName("名前")).not.toBeNull();
  });

  it("returns an error for names exceeding 128 characters", () => {
    expect(validateDatasetName("a".repeat(129))).not.toBeNull();
  });
});

describe("DatasetService", () => {
  let service: DatasetService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [DatasetService, ...commonTestProviders],
    });
    service = TestBed.inject(DatasetService);
    http = TestBed.inject(HttpTestingController);
    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    http.verify();
  });

  // ─── createDataset ────────────────────────────────────────────────────────

  it("createDataset POSTs the dataset metadata under the create endpoint", async () => {
    const dataset = buildDataset({ name: "demo", description: "desc", isPublic: true, isDownloadable: true });
    const dashboard = buildDashboardDataset();
    const pending = firstValueFrom(service.createDataset(dataset));

    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/create`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({
      datasetName: "demo",
      datasetDescription: "desc",
      isDatasetPublic: true,
      isDatasetDownloadable: true,
    });
    req.flush(dashboard);
    expect(await pending).toEqual(dashboard);
  });

  // ─── getDataset (login vs public branch) ──────────────────────────────────

  it("getDataset hits /dataset/{did} when logged in", () => {
    service.getDataset(7).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/7`).flush(buildDashboardDataset());
  });

  it("getDataset hits /dataset/public/{did} when anonymous", () => {
    service.getDataset(7, false).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/public/7`).flush(buildDashboardDataset());
  });

  // ─── retrieveDatasetVersionSingleFile (chained presigned URL) ─────────────

  it("retrieveDatasetVersionSingleFile resolves the presigned URL then GETs the blob", async () => {
    const filePath = "folder/file.txt";
    const blob = new Blob(["bytes"]);
    const pending = firstValueFrom(service.retrieveDatasetVersionSingleFile(filePath));

    const presignReq = http.expectOne(
      `${API}/${DATASET_BASE_URL}/presign-download?filePath=${encodeURIComponent(filePath)}`
    );
    expect(presignReq.request.method).toBe("GET");
    presignReq.flush({ presignedUrl: "https://s3.example/blob" });

    const blobReq = http.expectOne("https://s3.example/blob");
    expect(blobReq.request.responseType).toBe("blob");
    blobReq.flush(blob);

    expect(await pending).toBe(blob);
  });

  it("retrieveDatasetVersionSingleFile uses the public presign endpoint when anonymous", () => {
    const filePath = "f.txt";
    service.retrieveDatasetVersionSingleFile(filePath, false).subscribe();
    const presignReq = http.expectOne(
      `${API}/${DATASET_BASE_URL}/public-presign-download?filePath=${encodeURIComponent(filePath)}`
    );
    presignReq.flush({ presignedUrl: "https://s3.example/x" });
    http.expectOne("https://s3.example/x").flush(new Blob());
  });

  // ─── retrieveDatasetVersionZip ────────────────────────────────────────────

  it("retrieveDatasetVersionZip sets dvid when a version is specified", () => {
    service.retrieveDatasetVersionZip(3, 99).subscribe();
    const req = http.expectOne(r => r.url === `${API}/dataset/3/versionZip`);
    expect(req.request.params.get("dvid")).toBe("99");
    expect(req.request.params.get("latest")).toBeNull();
    expect(req.request.responseType).toBe("blob");
    req.flush(new Blob());
  });

  it("retrieveDatasetVersionZip sets latest=true when dvid is omitted", () => {
    service.retrieveDatasetVersionZip(3).subscribe();
    const req = http.expectOne(r => r.url === `${API}/dataset/3/versionZip`);
    expect(req.request.params.get("latest")).toBe("true");
    expect(req.request.params.get("dvid")).toBeNull();
    req.flush(new Blob());
  });

  // ─── retrieveAccessibleDatasets ───────────────────────────────────────────

  it("retrieveAccessibleDatasets GETs /dataset/list", async () => {
    const datasets = [buildDashboardDataset()];
    const pending = firstValueFrom(service.retrieveAccessibleDatasets());
    http.expectOne(`${API}/${DATASET_BASE_URL}/list`).flush(datasets);
    expect(await pending).toEqual(datasets);
  });

  // ─── createDatasetVersion (mapper attaches fileNodes) ─────────────────────

  it("createDatasetVersion attaches fileNodes onto the returned DatasetVersion", async () => {
    const did = 1;
    const newVersion = "v2";
    const dv = buildDatasetVersion({ name: newVersion });
    const pending = firstValueFrom(service.createDatasetVersion(did, newVersion));

    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/${did}/version/create`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toBe(newVersion);
    expect(req.request.headers.get("Content-Type")).toBe("text/plain");
    req.flush({ datasetVersion: dv, fileNodes: SAMPLE_FILE_NODES });

    const result = await pending;
    expect(result.fileNodes).toBe(SAMPLE_FILE_NODES);
    expect(result.name).toBe(newVersion);
  });

  // ─── listMultipartUploads ─────────────────────────────────────────────────

  it("listMultipartUploads returns the filePaths array", async () => {
    const pending = firstValueFrom(service.listMultipartUploads("a@b.com", "ds"));
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload`);
    expect(req.request.method).toBe("POST");
    expect(req.request.params.get("type")).toBe("list");
    expect(req.request.params.get("ownerEmail")).toBe("a@b.com");
    expect(req.request.params.get("datasetName")).toBe("ds");
    req.flush({ filePaths: ["a", "b"] });
    expect(await pending).toEqual(["a", "b"]);
  });

  it("listMultipartUploads tolerates a null payload", async () => {
    const pending = firstValueFrom(service.listMultipartUploads("a@b.com", "ds"));
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload`);
    req.flush(null);
    expect(await pending).toEqual([]);
  });

  // ─── finalizeMultipartUpload (abort vs finish) ────────────────────────────

  it("multipartUpload resumes a failed upload by sending only missing parts", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    const file = new File(["abcdefgh"], "resume.txt");
    const firstProgress: string[] = [];

    const firstAttempt = new Promise<unknown>(resolve => {
      service.multipartUpload("a@b.com", "ds", "resume.txt", file, 4, 1, false).subscribe({
        next: progress => firstProgress.push(progress.status),
        error: (error: unknown): void => {
          resolve(error);
        },
        complete: () => resolve(undefined),
      });
    });

    http
      .expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload` && r.params.get("type") === "init")
      .flush({ missingParts: [1, 2], completedPartsCount: 0 });

    expect(FakeXMLHttpRequest.instances[0].url).toContain("partNumber=1");
    FakeXMLHttpRequest.instances[0].respond(204);
    expect(FakeXMLHttpRequest.instances[1].url).toContain("partNumber=2");
    FakeXMLHttpRequest.instances[1].fail();

    expect(await firstAttempt).toBeInstanceOf(Error);
    expect(firstProgress).toContain("failed");

    FakeXMLHttpRequest.instances = [];
    const secondProgress: Array<{ percentage: number; status: string }> = [];
    const secondAttempt = new Promise<void>((resolve, reject) => {
      service.multipartUpload("a@b.com", "ds", "resume.txt", file, 4, 1, false).subscribe({
        next: progress => secondProgress.push({ percentage: progress.percentage, status: progress.status }),
        error: (error: unknown): void => {
          reject(error);
        },
        complete: resolve,
      });
    });

    http
      .expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload` && r.params.get("type") === "init")
      .flush({ missingParts: [2], completedPartsCount: 1 });

    expect(secondProgress[0]).toEqual({ percentage: 50, status: "initializing" });
    expect(
      FakeXMLHttpRequest.instances.map(xhr => new URL(xhr.url, "http://localhost").searchParams.get("partNumber"))
    ).toEqual(["2"]);
    FakeXMLHttpRequest.instances[0].respond(204);

    http
      .expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload` && r.params.get("type") === "finish")
      .flush({});

    await secondAttempt;
    expect(secondProgress.at(-1)).toEqual({ percentage: 100, status: "finished" });
  });

  it("findExistingUploadFiles posts path and size candidates", async () => {
    const pending = firstValueFrom(service.findExistingUploadFiles(7, [{ path: "a.csv", sizeBytes: 12 }]));
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/7/existing-upload-files`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ files: [{ path: "a.csv", sizeBytes: 12 }] });
    req.flush({ filePaths: ["a.csv"] });
    expect(await pending).toEqual(["a.csv"]);
  });

  it("findExistingUploadFiles tolerates a null payload", async () => {
    const pending = firstValueFrom(service.findExistingUploadFiles(7, [{ path: "a.csv", sizeBytes: 12 }]));
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/7/existing-upload-files`);
    req.flush(null);
    expect(await pending).toEqual([]);
  });

  it("finalizeMultipartUpload routes through type=finish when not aborting", () => {
    service.finalizeMultipartUpload("a@b.com", "ds", "f", false).subscribe();
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload`);
    expect(req.request.params.get("type")).toBe("finish");
    expect(req.request.params.get("filePath")).toBe(encodeURIComponent("f"));
    req.flush({});
  });

  it("finalizeMultipartUpload routes through type=abort when aborting", () => {
    service.finalizeMultipartUpload("a@b.com", "ds", "f", true).subscribe();
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/multipart-upload`);
    expect(req.request.params.get("type")).toBe("abort");
    req.flush({});
  });

  // ─── resetDatasetFileDiff / deleteDatasetFile ─────────────────────────────

  it("resetDatasetFileDiff PUTs /dataset/{did}/diff with the encoded filePath", () => {
    service.resetDatasetFileDiff(2, "a/b.txt").subscribe();
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/2/diff`);
    expect(req.request.method).toBe("PUT");
    expect(req.request.params.get("filePath")).toBe(encodeURIComponent("a/b.txt"));
    req.flush({});
  });

  it("deleteDatasetFile DELETEs /dataset/{did}/file with the encoded filePath", () => {
    service.deleteDatasetFile(2, "a/b.txt").subscribe();
    const req = http.expectOne(r => r.url === `${API}/${DATASET_BASE_URL}/2/file`);
    expect(req.request.method).toBe("DELETE");
    expect(req.request.params.get("filePath")).toBe(encodeURIComponent("a/b.txt"));
    req.flush({});
  });

  // ─── getDatasetDiff ───────────────────────────────────────────────────────

  it("getDatasetDiff returns the staged-object list", async () => {
    const diff: DatasetStagedObject[] = [{ path: "p", pathType: "file", diffType: "added" }];
    const pending = firstValueFrom(service.getDatasetDiff(9));
    http.expectOne(`${API}/${DATASET_BASE_URL}/9/diff`).flush(diff);
    expect(await pending).toEqual(diff);
  });

  // ─── retrieveDatasetVersionList (login vs public) ─────────────────────────

  it("retrieveDatasetVersionList hits the authenticated path when logged in", () => {
    service.retrieveDatasetVersionList(1).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/1/version/list`).flush([]);
  });

  it("retrieveDatasetVersionList hits the public path when anonymous", () => {
    service.retrieveDatasetVersionList(1, false).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/1/publicVersion/list`).flush([]);
  });

  // ─── retrieveDatasetLatestVersion (mapper attaches fileNodes) ─────────────

  it("retrieveDatasetLatestVersion attaches fileNodes onto the returned version", async () => {
    const dv = buildDatasetVersion();
    const pending = firstValueFrom(service.retrieveDatasetLatestVersion(1));
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/1/version/latest`);
    req.flush({ datasetVersion: dv, fileNodes: SAMPLE_FILE_NODES });
    const result = await pending;
    expect(result.fileNodes).toBe(SAMPLE_FILE_NODES);
  });

  // ─── retrieveDatasetVersionFileTree (login vs public) ─────────────────────

  it("retrieveDatasetVersionFileTree picks the authenticated path when logged in", () => {
    service.retrieveDatasetVersionFileTree(1, 2).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/1/version/2/rootFileNodes`).flush({ fileNodes: [], size: 0 });
  });

  it("retrieveDatasetVersionFileTree picks the public path when anonymous", () => {
    service.retrieveDatasetVersionFileTree(1, 2, false).subscribe();
    http.expectOne(`${API}/${DATASET_BASE_URL}/1/publicVersion/2/rootFileNodes`).flush({ fileNodes: [], size: 0 });
  });

  // ─── deleteDatasets ───────────────────────────────────────────────────────

  it("deleteDatasets DELETEs /dataset/{did}", () => {
    service.deleteDatasets(5).subscribe();
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/5`);
    expect(req.request.method).toBe("DELETE");
    req.flush({});
  });

  // ─── updateDatasetName / Description / Publicity / Downloadable ───────────

  it("updateDatasetName POSTs name + did into /update/name", () => {
    service.updateDatasetName(2, "renamed").subscribe();
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/update/name`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ did: 2, name: "renamed" });
    req.flush({});
  });

  it("updateDatasetDescription POSTs description + did into /update/description", () => {
    service.updateDatasetDescription(2, "newdesc").subscribe();
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/update/description`);
    expect(req.request.body).toEqual({ did: 2, description: "newdesc" });
    req.flush({});
  });

  it("updateDatasetPublicity POSTs to /dataset/{did}/update/publicity", () => {
    service.updateDatasetPublicity(2).subscribe();
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/2/update/publicity`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush({});
  });

  it("updateDatasetDownloadable POSTs to /dataset/{did}/update/downloadable", () => {
    service.updateDatasetDownloadable(2).subscribe();
    const req = http.expectOne(`${API}/${DATASET_BASE_URL}/2/update/downloadable`);
    req.flush({});
  });

  // ─── retrieveOwners / updateDatasetCoverImage ─────────────────────────────

  it("retrieveOwners GETs /dataset/user-dataset-owners", async () => {
    const pending = firstValueFrom(service.retrieveOwners());
    http.expectOne(`${API}/${DATASET_BASE_URL}/user-dataset-owners`).flush(["a", "b"]);
    expect(await pending).toEqual(["a", "b"]);
  });

  it("updateDatasetCoverImage POSTs the cover image base64 to /dataset/{did}/update/cover", () => {
    service.updateDatasetCoverImage(3, "data:image/png;base64,ZGF0YQ==").subscribe();
    const req = http.expectOne(`${API}/dataset/3/update/cover`);
    expect(req.request.body).toEqual({ coverImage: "data:image/png;base64,ZGF0YQ==" });
    req.flush({});
  });

  // ─── getDatasetCoverUrl ───────────────────────────────────────────────────

  it("getDatasetCoverUrl GETs /dataset/{did}/cover-url and returns the mapped payload", async () => {
    const pending = firstValueFrom(service.getDatasetCoverUrl(4));
    const req = http.expectOne(`${API}/dataset/4/cover-url`);
    expect(req.request.method).toBe("GET");
    req.flush({ url: "https://img.example/cover.png" });
    expect(await pending).toEqual({ url: "https://img.example/cover.png" });
  });

  it("getDatasetCoverUrl passes through a null url", async () => {
    const pending = firstValueFrom(service.getDatasetCoverUrl(4));
    http.expectOne(`${API}/dataset/4/cover-url`).flush({ url: null });
    expect(await pending).toEqual({ url: null });
  });

  // ─── multipartUpload: progress / stats / load / error branches ────────────

  const isInit = (r: { url: string; params: { get(k: string): string | null } }) =>
    r.url === `${API}/${DATASET_BASE_URL}/multipart-upload` && r.params.get("type") === "init";
  const isFinish = (r: { url: string; params: { get(k: string): string | null } }) =>
    r.url === `${API}/${DATASET_BASE_URL}/multipart-upload` && r.params.get("type") === "finish";

  it("multipartUpload emits uploading progress, attaches the auth header, and finishes on HTTP 200", async () => {
    const tokenSpy = vi.spyOn(AuthService, "getAccessToken").mockReturnValue("tok123");
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    try {
      const file = new File([new Uint8Array(8)], "d.bin"); // 8 bytes, partSize 8 => 1 part
      const emissions: MultipartUploadProgress[] = [];
      const done = new Promise<void>((resolve, reject) => {
        service.multipartUpload("o@e.com", "ds", "d.bin", file, 8, 1, false).subscribe({
          next: p => emissions.push(p),
          error: (error: unknown): void => reject(error),
          complete: resolve,
        });
      });

      http.expectOne(isInit).flush({ missingParts: [1], completedPartsCount: 0 });

      const xhr = FakeXMLHttpRequest.instances[0];
      expect(xhr.requestHeaders.get("Content-Type")).toBe("application/octet-stream");
      expect(xhr.requestHeaders.get("Authorization")).toBe("Bearer tok123");

      xhr.emitProgress(4); // half a part uploaded -> "uploading" emission
      xhr.respond(200); // load handler takes the 200 branch
      http.expectOne(isFinish).flush({});
      await done;

      const uploading = emissions.filter(e => e.status === "uploading");
      expect(uploading.length).toBeGreaterThan(0);
      expect(uploading.some(e => e.percentage > 0 && e.percentage <= 99)).toBe(true);
      expect(emissions.at(-1)).toMatchObject({ status: "finished", percentage: 100 });
    } finally {
      tokenSpy.mockRestore();
    }
  });

  it("multipartUpload errors out when a part upload load returns a non-2xx status", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    const file = new File([new Uint8Array(4)], "e.bin");
    const emissions: MultipartUploadProgress[] = [];
    const outcome = new Promise<unknown>(resolve => {
      service.multipartUpload("o@e.com", "ds", "e.bin", file, 4, 1, false).subscribe({
        next: p => emissions.push(p),
        error: (error: unknown): void => resolve(error),
        complete: () => resolve(null),
      });
    });

    http.expectOne(isInit).flush({ missingParts: [1], completedPartsCount: 0 });
    FakeXMLHttpRequest.instances[0].respond(500);

    const err = await outcome;
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toContain("HTTP 500");
    expect(emissions.at(-1)).toMatchObject({ status: "failed", percentage: 0 });
  });

  it("multipartUpload tolerates a sparse init payload and finishes when no parts are missing", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    const file = new File([new Uint8Array(4)], "c.bin");
    const emissions: MultipartUploadProgress[] = [];
    const done = new Promise<void>((resolve, reject) => {
      service.multipartUpload("o@e.com", "ds", "c.bin", file, 4, 1, false).subscribe({
        next: p => emissions.push(p),
        error: reject,
        complete: resolve,
      });
    });

    // Missing `missingParts` / `completedPartsCount` exercise the nullish-coalescing defaults.
    http.expectOne(isInit).flush({});
    expect(FakeXMLHttpRequest.instances.length).toBe(0);
    http.expectOne(isFinish).flush({});
    await done;

    expect(emissions[0]).toMatchObject({ status: "initializing", percentage: 0 });
    expect(emissions.at(-1)).toMatchObject({ status: "finished", percentage: 100 });
  });

  it("multipartUpload reports a 0% failure when the finish call errors for an empty file", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    const file = new File([], "empty.bin"); // size 0 => partCount 0 (partCount>0 false branch)
    const emissions: MultipartUploadProgress[] = [];
    const outcome = new Promise<unknown>(resolve => {
      service.multipartUpload("o@e.com", "ds", "empty.bin", file, 4, 1, false).subscribe({
        next: p => emissions.push(p),
        error: (error: unknown): void => resolve(error),
        complete: () => resolve(null),
      });
    });

    http.expectOne(isInit).flush({ missingParts: [], completedPartsCount: 0 });
    expect(FakeXMLHttpRequest.instances.length).toBe(0);
    http.expectOne(isFinish).error(new ProgressEvent("error"));

    const err = await outcome;
    expect(err).not.toBeNull();
    expect(emissions[0]).toMatchObject({ status: "initializing", percentage: 0 });
    expect(emissions.at(-1)).toMatchObject({ status: "failed", percentage: 0 });
  });

  it("multipartUpload smooths, throttles and shifts the progress statistics across events", async () => {
    let now = 1_000_000;
    const dateSpy = vi.spyOn(Date, "now").mockImplementation(() => now);
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    try {
      const file = new File([new Uint8Array(100)], "big.bin"); // 100 bytes, partSize 100 => 1 part
      const emissions: MultipartUploadProgress[] = [];
      const done = new Promise<void>((resolve, reject) => {
        service.multipartUpload("o@e.com", "ds", "big.bin", file, 100, 1, false).subscribe({
          next: p => emissions.push(p),
          error: (error: unknown): void => reject(error),
          complete: resolve,
        });
      });

      http.expectOne(isInit).flush({ missingParts: [1], completedPartsCount: 0 });
      const xhr = FakeXMLHttpRequest.instances[0];

      xhr.emitProgress(5, false); // non-lengthComputable -> ignored
      xhr.emitProgress(10); // first update: startTime set, elapsed 0
      xhr.emitProgress(20); // same timestamp -> throttled (returns cached stats)
      now = 1_002_000;
      xhr.emitProgress(30); // elapsed>0 so speed/avg become positive
      now = 1_003_000;
      xhr.emitProgress(90); // large ETA drop -> smoothing clamps the change
      now = 1_004_000;
      xhr.emitProgress(99); // >95% complete -> ETA capped to 10s
      now = 1_005_000;
      xhr.emitProgress(99);
      now = 1_006_000;
      xhr.emitProgress(99); // 6th sample -> speedSamples window shifts
      xhr.respond(200);
      http.expectOne(isFinish).flush({});
      await done;

      const uploading = emissions.filter(e => e.status === "uploading");
      expect(uploading.length).toBeGreaterThan(0);
      expect(uploading.every(e => e.percentage <= 99)).toBe(true);
      // Once real elapsed time exists the smoothed speed becomes positive.
      expect(uploading.some(e => (e.uploadSpeed ?? 0) > 0)).toBe(true);
      // ETA is always reported as a non-negative integer number of seconds.
      expect(
        uploading.every(e => Number.isInteger(e.estimatedTimeRemaining) && (e.estimatedTimeRemaining ?? -1) >= 0)
      ).toBe(true);
      expect(emissions.at(-1)).toMatchObject({
        status: "finished",
        percentage: 100,
        uploadSpeed: 0,
        estimatedTimeRemaining: 0,
      });
    } finally {
      dateSpy.mockRestore();
    }
  });

  it("multipartUpload clamps a sharply rising ETA to the +30% smoothing bound", async () => {
    let now = 1_000_000;
    const dateSpy = vi.spyOn(Date, "now").mockImplementation(() => now);
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    FakeXMLHttpRequest.instances = [];
    try {
      const file = new File([new Uint8Array(100)], "rise.bin"); // 100 bytes, 1 part
      const emissions: MultipartUploadProgress[] = [];
      const done = new Promise<void>((resolve, reject) => {
        service.multipartUpload("o@e.com", "ds", "rise.bin", file, 100, 1, false).subscribe({
          next: p => emissions.push(p),
          error: (error: unknown): void => reject(error),
          complete: resolve,
        });
      });

      http.expectOne(isInit).flush({ missingParts: [1], completedPartsCount: 0 });
      const xhr = FakeXMLHttpRequest.instances[0];

      xhr.emitProgress(50); // first update: elapsed 0 -> ETA 0
      now = 1_001_000;
      xhr.emitProgress(50); // fast sample -> ETA becomes small (~2s), sets lastETA
      now = 1_100_000;
      xhr.emitProgress(50); // huge elapsed collapses avg speed -> ETA rises >30% and is clamped up
      xhr.respond(200);
      http.expectOne(isFinish).flush({});
      await done;

      const uploading = emissions.filter(e => e.status === "uploading");
      expect(uploading.length).toBeGreaterThanOrEqual(3);

      const etaBefore = uploading[1].estimatedTimeRemaining ?? 0;
      const etaAfter = uploading[2].estimatedTimeRemaining ?? 0;

      expect(etaBefore).toBeGreaterThan(0);
      expect(etaAfter).toBeLessThanOrEqual(Math.round(etaBefore * 1.3));
      expect(emissions.at(-1)).toMatchObject({ status: "finished", percentage: 100 });
    } finally {
      dateSpy.mockRestore();
    }
  });
});
