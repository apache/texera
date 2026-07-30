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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom } from "rxjs";

import {
  MODEL_BASE_URL,
  MODEL_FORMATS,
  MODEL_FRAMEWORKS,
  MODEL_NAME_MAX_LENGTH,
  ModelService,
  validateModelName,
} from "./model.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Model, ModelVersion } from "../../../../common/type/model";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DashboardModel } from "../../../type/dashboard-model.interface";

const API = "api";

function buildModel(overrides: Partial<Model> = {}): Model {
  return {
    mid: 1,
    ownerUid: 1,
    name: "my-model",
    repositoryName: "model-1",
    isPublic: false,
    isDownloadable: false,
    description: "",
    creationTime: undefined,
    coverImage: undefined,
    framework: "pytorch",
    format: "safetensors",
    ...overrides,
  };
}

function buildDashboardModel(overrides: Partial<DashboardModel> = {}): DashboardModel {
  return {
    isOwner: true,
    ownerEmail: "owner@example.com",
    model: buildModel(),
    accessPrivilege: "WRITE",
    size: 0,
    ...overrides,
  };
}

function buildModelVersion(overrides: Partial<ModelVersion> = {}): ModelVersion {
  return {
    mvid: 10,
    mid: 1,
    creatorUid: 1,
    name: "v1",
    versionHash: "commit-abc",
    creationTime: 1,
    fileNodes: undefined,
    ...overrides,
  };
}

function buildFileNode(name: string): DatasetFileNode {
  return { name, type: "file", parentDir: "/datasets/owner@example.com/my-model/v1", size: 4 };
}

describe("ModelService", () => {
  let service: ModelService;
  let httpMock: HttpTestingController;

  beforeEach(async () => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [ModelService, ...commonTestProviders],
    });
    service = TestBed.inject(ModelService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("createModel posts the backend's field names", async () => {
    const model = buildModel({ name: "resnet", description: "d", isPublic: true, isDownloadable: true });
    const pending = firstValueFrom(service.createModel(model));

    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/create`);
    expect(req.request.method).toBe("POST");
    // The DTO is CreateModelRequest, not the Model shape — mismatched keys would be
    // silently dropped by Jackson and land as nulls.
    expect(req.request.body).toEqual({
      modelName: "resnet",
      modelDescription: "d",
      isModelPublic: true,
      isModelDownloadable: true,
      framework: "pytorch",
      format: "safetensors",
    });
    req.flush(buildDashboardModel());
    await pending;
  });

  it("getModel uses the authenticated endpoint when logged in", async () => {
    const pending = firstValueFrom(service.getModel(7));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/7`);
    expect(req.request.method).toBe("GET");
    req.flush(buildDashboardModel());
    await pending;
  });

  it("getModel uses the public endpoint when not logged in", async () => {
    const pending = firstValueFrom(service.getModel(7, false));
    httpMock.expectOne(`${API}/${MODEL_BASE_URL}/public/7`).flush(buildDashboardModel());
    await pending;
  });

  it("retrieveAccessibleModels gets the list endpoint", async () => {
    const pending = firstValueFrom(service.retrieveAccessibleModels());
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/list`);
    expect(req.request.method).toBe("GET");
    req.flush([buildDashboardModel()]);
    expect((await pending).length).toBe(1);
  });

  it("deleteModel issues a DELETE on the model", async () => {
    const pending = firstValueFrom(service.deleteModel(3));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/3`);
    expect(req.request.method).toBe("DELETE");
    req.flush({});
    await pending;
  });

  it("updateModelName posts mid and name", async () => {
    const pending = firstValueFrom(service.updateModelName(4, "renamed"));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/update/name`);
    expect(req.request.body).toEqual({ mid: 4, name: "renamed" });
    req.flush({});
    await pending;
  });

  it("updateModelDescription posts mid and description", async () => {
    const pending = firstValueFrom(service.updateModelDescription(4, "new"));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/update/description`);
    expect(req.request.body).toEqual({ mid: 4, description: "new" });
    req.flush({});
    await pending;
  });

  it("updateModelPublicity and updateModelDownloadable toggle via the mid-scoped paths", async () => {
    const publicity = firstValueFrom(service.updateModelPublicity(9));
    httpMock.expectOne(`${API}/${MODEL_BASE_URL}/9/update/publicity`).flush({});
    await publicity;

    const downloadable = firstValueFrom(service.updateModelDownloadable(9));
    httpMock.expectOne(`${API}/${MODEL_BASE_URL}/9/update/downloadable`).flush({});
    await downloadable;
  });

  it("retrieveOwners gets the model owners endpoint, not the dataset one", async () => {
    const pending = firstValueFrom(service.retrieveOwners());
    httpMock.expectOne(`${API}/${MODEL_BASE_URL}/user-model-owners`).flush(["a@b.com"]);
    expect(await pending).toEqual(["a@b.com"]);
  });

  it("retrieveModelVersionList gets the mid-scoped version list", async () => {
    const pending = firstValueFrom(service.retrieveModelVersionList(5));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/5/version/list`);
    expect(req.request.method).toBe("GET");
    req.flush([buildModelVersion()]);
    expect((await pending).length).toBe(1);
  });

  it("retrieveModelLatestVersion folds fileNodes onto the version", async () => {
    const pending = firstValueFrom(service.retrieveModelLatestVersion(5));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/5/version/latest`);
    req.flush({ modelVersion: buildModelVersion(), fileNodes: [buildFileNode("model.pt")] });

    const version = await pending;
    expect(version.name).toBe("v1");
    expect(version.fileNodes?.map(node => node.name)).toEqual(["model.pt"]);
  });

  it("retrieveModelVersionFileTree gets the version's root file nodes and size", async () => {
    const pending = firstValueFrom(service.retrieveModelVersionFileTree(5, 10));
    const req = httpMock.expectOne(`${API}/${MODEL_BASE_URL}/5/version/10/rootFileNodes`);
    expect(req.request.method).toBe("GET");
    req.flush({ fileNodes: [buildFileNode("model.pt")], size: 42 });
    expect(await pending).toEqual({ fileNodes: [buildFileNode("model.pt")], size: 42 });
  });

  it("retrieveModelVersionZip sets mvid when a version is specified", () => {
    service.retrieveModelVersionZip(5, 10).subscribe();
    const req = httpMock.expectOne(r => r.url === `${API}/${MODEL_BASE_URL}/5/versionZip`);
    expect(req.request.params.get("mvid")).toBe("10");
    expect(req.request.params.get("latest")).toBeNull();
    expect(req.request.responseType).toBe("blob");
    req.flush(new Blob());
  });

  it("retrieveModelVersionZip sets latest=true when mvid is omitted", () => {
    service.retrieveModelVersionZip(5).subscribe();
    const req = httpMock.expectOne(r => r.url === `${API}/${MODEL_BASE_URL}/5/versionZip`);
    expect(req.request.params.get("latest")).toBe("true");
    expect(req.request.params.get("mvid")).toBeNull();
    req.flush(new Blob());
  });

  it("retrieveModelVersionSingleFile resolves the presigned URL then GETs the blob", async () => {
    // The logical path must carry the models prefix; a /datasets/... path would resolve
    // against the dataset table on the backend.
    const filePath = "/models/owner@example.com/my-model/v1/weights/model.pt";
    const blob = new Blob(["bytes"]);
    const pending = firstValueFrom(service.retrieveModelVersionSingleFile(filePath));

    const presignReq = httpMock.expectOne(
      `${API}/${MODEL_BASE_URL}/presign-download?filePath=${encodeURIComponent(filePath)}`
    );
    expect(presignReq.request.method).toBe("GET");
    presignReq.flush({ presignedUrl: "https://s3.example/blob" });

    const blobReq = httpMock.expectOne("https://s3.example/blob");
    expect(blobReq.request.responseType).toBe("blob");
    blobReq.flush(blob);

    expect(await pending).toBe(blob);
  });

  it("retrieveModelVersionSingleFile uses the public presign endpoint when anonymous", () => {
    const filePath = "/models/owner@example.com/my-model/v1/f.txt";
    service.retrieveModelVersionSingleFile(filePath, false).subscribe();
    httpMock
      .expectOne(`${API}/${MODEL_BASE_URL}/public-presign-download?filePath=${encodeURIComponent(filePath)}`)
      .flush({ presignedUrl: "https://s3.example/x" });
    httpMock.expectOne("https://s3.example/x").flush(new Blob());
  });

  it("surfaces a server error to the caller", async () => {
    const pending = firstValueFrom(service.retrieveAccessibleModels());
    httpMock
      .expectOne(`${API}/${MODEL_BASE_URL}/list`)
      .flush({ message: "nope" }, { status: 403, statusText: "Forbidden" });
    await expect(pending).rejects.toBeTruthy();
  });
});

describe("validateModelName", () => {
  it("accepts letters, numbers, underscores and hyphens", () => {
    expect(validateModelName("Model_1-v2")).toBeNull();
  });

  it("rejects an empty name", () => {
    expect(validateModelName("")).not.toBeNull();
  });

  it("rejects spaces and punctuation", () => {
    expect(validateModelName("my model")).not.toBeNull();
    expect(validateModelName("model.pt")).not.toBeNull();
    expect(validateModelName("../escape")).not.toBeNull();
  });

  it("rejects a name longer than the maximum", () => {
    expect(validateModelName("a".repeat(MODEL_NAME_MAX_LENGTH))).toBeNull();
    expect(validateModelName("a".repeat(MODEL_NAME_MAX_LENGTH + 1))).not.toBeNull();
  });
});

describe("framework and format options", () => {
  // These must match ModelResource.SUPPORTED_FRAMEWORKS / SUPPORTED_FORMATS, which reject
  // anything else with a 400. A drift here becomes a failed create at runtime.
  it("matches the backend whitelists", () => {
    expect([...MODEL_FRAMEWORKS]).toEqual(["pytorch", "tensorflow", "onnx", "sklearn"]);
    expect([...MODEL_FORMATS]).toEqual([
      "torchscript",
      "state-dict",
      "safetensors",
      "onnx",
      "savedmodel",
      "joblib",
      "pickle",
    ]);
  });
});
