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

import { DATASET_BASE_URL, DatasetService, validateDatasetName } from "./dataset.service";
import { AppSettings } from "../../../../common/app-setting";
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

  readonly upload = {
    addEventListener: vi.fn(),
  };
  status = 0;
  url = "";
  private listeners = new Map<string, EventListener[]>();

  open(_method: string, url: string): void {
    this.url = url;
  }

  setRequestHeader(): void {}

  send(): void {
    FakeXMLHttpRequest.instances.push(this);
  }

  abort(): void {}

  addEventListener(type: string, listener: EventListener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
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
});
