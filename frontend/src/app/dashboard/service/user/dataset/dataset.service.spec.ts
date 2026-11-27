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

import { DatasetService, validateDatasetName } from "./dataset.service";
import { AppSettings } from "../../../../common/app-setting";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Contributor, Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";

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
    http.verify();
  });

  // ─── createDataset ────────────────────────────────────────────────────────

  it("createDataset POSTs the dataset metadata under the create endpoint", async () => {
    const dataset = buildDataset({ name: "demo", description: "desc", isPublic: true, isDownloadable: true });
    const dashboard = buildDashboardDataset();
    const pending = firstValueFrom(service.createDataset(dataset));

    const req = http.expectOne(`${API}/dataset/create`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({
      datasetName: "demo",
      datasetDescription: "desc",
      isDatasetPublic: true,
      isDatasetDownloadable: true,
      contributors: [],
    });
    req.flush(dashboard);
    expect(await pending).toEqual(dashboard);
  });

  it("createDataset includes contributors in the request body when present", () => {
    const contributors: Contributor[] = [
      {
        name: "Contributor A",
        creator: true,
        affiliation: "Test Lab",
        email: "contributor-a@test.com",
        comments: "collected the data",
      },
    ];
    service.createDataset(buildDataset(), contributors).subscribe();

    const req = http.expectOne(`${API}/dataset/create`);
    expect(req.request.body.contributors).toEqual(contributors);
    req.flush(buildDashboardDataset());
  });

  it("createDataset omits blank optional contributor fields", () => {
    const contributors: Contributor[] = [
      { name: "Contributor A", creator: false, affiliation: "  ", email: "", comments: undefined },
    ];
    service.createDataset(buildDataset(), contributors).subscribe();

    const req = http.expectOne(`${API}/dataset/create`);
    expect(req.request.body.contributors).toEqual([
      { name: "Contributor A", creator: false, affiliation: undefined, email: undefined, comments: undefined },
    ]);
    req.flush(buildDashboardDataset());
  });

  // ─── updateDatasetContributors ────────────────────────────────────────────

  it("updateDatasetContributors POSTs the did and list, omitting blank optional fields", () => {
    const contributors: Contributor[] = [
      {
        name: "Contributor B",
        creator: false,
        affiliation: "Test Lab",
        email: " contributor-b@test.com ",
        comments: "",
      },
    ];
    service.updateDatasetContributors(7, contributors).subscribe();

    const req = http.expectOne(`${API}/dataset/update/contributors`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({
      did: 7,
      contributors: [
        {
          name: "Contributor B",
          creator: false,
          affiliation: "Test Lab",
          email: "contributor-b@test.com",
          comments: undefined,
        },
      ],
    });
    req.flush(null);
  });

  it("updateDatasetContributors sends an empty list when all contributors are removed", () => {
    service.updateDatasetContributors(7, []).subscribe();

    const req = http.expectOne(`${API}/dataset/update/contributors`);
    expect(req.request.body).toEqual({ did: 7, contributors: [] });
    req.flush(null);
  });

  // ─── getDataset (login vs public branch) ──────────────────────────────────

  it("getDataset hits /dataset/{did} when logged in", () => {
    service.getDataset(7).subscribe();
    http.expectOne(`${API}/dataset/7`).flush(buildDashboardDataset());
  });

  it("getDataset hits /dataset/public/{did} when anonymous", () => {
    service.getDataset(7, false).subscribe();
    http.expectOne(`${API}/dataset/public/7`).flush(buildDashboardDataset());
  });

  // ─── retrieveDatasetVersionSingleFile (chained presigned URL) ─────────────

  it("retrieveDatasetVersionSingleFile resolves the presigned URL then GETs the blob", async () => {
    const filePath = "folder/file.txt";
    const blob = new Blob(["bytes"]);
    const pending = firstValueFrom(service.retrieveDatasetVersionSingleFile(filePath));

    const presignReq = http.expectOne(`${API}/dataset/presign-download?filePath=${encodeURIComponent(filePath)}`);
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
      `${API}/dataset/public-presign-download?filePath=${encodeURIComponent(filePath)}`
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
    http.expectOne(`${API}/dataset/list`).flush(datasets);
    expect(await pending).toEqual(datasets);
  });

  // ─── createDatasetVersion (mapper attaches fileNodes) ─────────────────────

  it("createDatasetVersion attaches fileNodes onto the returned DatasetVersion", async () => {
    const did = 1;
    const newVersion = "v2";
    const dv = buildDatasetVersion({ name: newVersion });
    const pending = firstValueFrom(service.createDatasetVersion(did, newVersion));

    const req = http.expectOne(`${API}/dataset/${did}/version/create`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toBe(newVersion);
    expect(req.request.headers.get("Content-Type")).toBe("text/plain");
    req.flush({ datasetVersion: dv, fileNodes: SAMPLE_FILE_NODES });

    const result = await pending;
    expect(result.fileNodes).toBe(SAMPLE_FILE_NODES);
    expect(result.name).toBe(newVersion);
  });

  // ─── deleteDatasetFile ────────────────────────────────────────────────────

  it("deleteDatasetFile DELETEs /dataset/{did}/file with the encoded filePath", () => {
    service.deleteDatasetFile(2, "a/b.txt").subscribe();
    const req = http.expectOne(r => r.url === `${API}/dataset/2/file`);
    expect(req.request.method).toBe("DELETE");
    expect(req.request.params.get("filePath")).toBe(encodeURIComponent("a/b.txt"));
    req.flush({});
  });

  // ─── retrieveDatasetVersionList (login vs public) ─────────────────────────

  it("retrieveDatasetVersionList hits the authenticated path when logged in", () => {
    service.retrieveDatasetVersionList(1).subscribe();
    http.expectOne(`${API}/dataset/1/version/list`).flush([]);
  });

  it("retrieveDatasetVersionList hits the public path when anonymous", () => {
    service.retrieveDatasetVersionList(1, false).subscribe();
    http.expectOne(`${API}/dataset/1/publicVersion/list`).flush([]);
  });

  // ─── retrieveDatasetLatestVersion (mapper attaches fileNodes) ─────────────

  it("retrieveDatasetLatestVersion attaches fileNodes onto the returned version", async () => {
    const dv = buildDatasetVersion();
    const pending = firstValueFrom(service.retrieveDatasetLatestVersion(1));
    const req = http.expectOne(`${API}/dataset/1/version/latest`);
    req.flush({ datasetVersion: dv, fileNodes: SAMPLE_FILE_NODES });
    const result = await pending;
    expect(result.fileNodes).toBe(SAMPLE_FILE_NODES);
  });

  // ─── retrieveDatasetVersionFileTree (login vs public) ─────────────────────

  it("retrieveDatasetVersionFileTree picks the authenticated path when logged in", () => {
    service.retrieveDatasetVersionFileTree(1, 2).subscribe();
    http.expectOne(`${API}/dataset/1/version/2/rootFileNodes`).flush({ fileNodes: [], size: 0 });
  });

  it("retrieveDatasetVersionFileTree picks the public path when anonymous", () => {
    service.retrieveDatasetVersionFileTree(1, 2, false).subscribe();
    http.expectOne(`${API}/dataset/1/publicVersion/2/rootFileNodes`).flush({ fileNodes: [], size: 0 });
  });

  // ─── deleteDatasets ───────────────────────────────────────────────────────

  it("deleteDatasets DELETEs /dataset/{did}", () => {
    service.deleteDatasets(5).subscribe();
    const req = http.expectOne(`${API}/dataset/5`);
    expect(req.request.method).toBe("DELETE");
    req.flush({});
  });

  // ─── updateDatasetName / Description / Publicity / Downloadable ───────────

  it("updateDatasetName POSTs name + did into /update/name", () => {
    service.updateDatasetName(2, "renamed").subscribe();
    const req = http.expectOne(`${API}/dataset/update/name`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ did: 2, name: "renamed" });
    req.flush({});
  });

  it("updateDatasetDescription POSTs description + did into /update/description", () => {
    service.updateDatasetDescription(2, "newdesc").subscribe();
    const req = http.expectOne(`${API}/dataset/update/description`);
    expect(req.request.body).toEqual({ did: 2, description: "newdesc" });
    req.flush({});
  });

  it("updateDatasetPublicity POSTs to /dataset/{did}/update/publicity", () => {
    service.updateDatasetPublicity(2).subscribe();
    const req = http.expectOne(`${API}/dataset/2/update/publicity`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
    req.flush({});
  });

  it("updateDatasetDownloadable POSTs to /dataset/{did}/update/downloadable", () => {
    service.updateDatasetDownloadable(2).subscribe();
    const req = http.expectOne(`${API}/dataset/2/update/downloadable`);
    req.flush({});
  });

  // ─── retrieveOwners / updateDatasetCoverImage ─────────────────────────────

  it("retrieveOwners GETs /dataset/user-dataset-owners", async () => {
    const pending = firstValueFrom(service.retrieveOwners());
    http.expectOne(`${API}/dataset/user-dataset-owners`).flush(["a", "b"]);
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
});
