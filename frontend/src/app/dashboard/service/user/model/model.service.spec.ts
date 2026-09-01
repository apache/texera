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
import {
  DEFAULT_MODEL_NAME,
  MODEL_FORMATS,
  MODEL_FRAMEWORKS,
  MODEL_NAME_MAX_LENGTH,
  ModelService,
  validateModelName,
} from "./model.service";
import { Model } from "../../../../common/type/model";
import { DashboardModel } from "../../../type/dashboard-model.interface";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const API = "api";

const newModel = (overrides: Partial<Model> = {}): Model => ({
  mid: undefined,
  ownerUid: undefined,
  name: "resnet-50",
  repositoryName: undefined,
  isPublic: false,
  isDownloadable: false,
  description: "a model",
  creationTime: undefined,
  coverImage: undefined,
  framework: "pytorch",
  format: "torchscript",
  ...overrides,
});

describe("validateModelName", () => {
  it("accepts letters, digits, underscores and hyphens", () => {
    expect(validateModelName("resnet-50")).toBeNull();
    expect(validateModelName("my_model_2")).toBeNull();
    expect(validateModelName("A")).toBeNull();
  });

  it("rejects anything else, including the empty name", () => {
    for (const name of ["", "has spaces", "slash/es", "dots.", "emoji-🙂", "semi;colon"]) {
      expect(validateModelName(name)).toContain("Invalid model name");
    }
  });

  it("rejects a name one character past the limit but accepts the limit itself", () => {
    expect(validateModelName("a".repeat(MODEL_NAME_MAX_LENGTH))).toBeNull();
    expect(validateModelName("a".repeat(MODEL_NAME_MAX_LENGTH + 1))).toContain("Invalid model name");
  });

  it("offers a default name that is itself valid", () => {
    expect(validateModelName(DEFAULT_MODEL_NAME)).toBeNull();
  });
});

describe("model whitelists", () => {
  it("mirror the backend's accepted frameworks and formats", () => {
    // The backend answers anything outside these with a 400, so drift here is a broken create form.
    expect([...MODEL_FRAMEWORKS]).toEqual(["pytorch", "tensorflow", "onnx", "sklearn", "other"]);
    expect([...MODEL_FORMATS]).toEqual([
      "torchscript",
      "state-dict",
      "safetensors",
      "onnx",
      "savedmodel",
      "joblib",
      "pickle",
      "other",
    ]);
  });
});

describe("ModelService", () => {
  let service: ModelService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [ModelService, ...commonTestProviders],
    });
    service = TestBed.inject(ModelService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http.verify();
  });

  it("posts a create request under the model-shaped field names", () => {
    // The backend reads modelName/isModelPublic; the dataset spellings would be silently dropped.
    service.createModel(newModel({ isPublic: true, isDownloadable: true })).subscribe();

    const req = http.expectOne(`${API}/model/create`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({
      modelName: "resnet-50",
      modelDescription: "a model",
      isModelPublic: true,
      isModelDownloadable: true,
      framework: "pytorch",
      format: "torchscript",
    });
    req.flush({});
  });

  it("returns the created model to the caller", async () => {
    const created = { model: { mid: 7 } } as DashboardModel;
    const pending = firstValueFrom(service.createModel(newModel()));

    http.expectOne(`${API}/model/create`).flush(created);

    expect(await pending).toEqual(created);
  });

  it("lists the accessible models, including when the user has none", async () => {
    const pending = firstValueFrom(service.retrieveAccessibleModels());
    http.expectOne(`${API}/model/list`).flush([]);
    expect(await pending).toEqual([]);
  });

  it("deletes by id", () => {
    service.deleteModel(7).subscribe();
    expect(http.expectOne(`${API}/model/7`).request.method).toBe("DELETE");
    http.verify();
  });

  it("sends renames and description edits with the id in the body", () => {
    service.updateModelName(7, "new-name").subscribe();
    const rename = http.expectOne(`${API}/model/update/name`);
    expect(rename.request.body).toEqual({ mid: 7, name: "new-name" });
    rename.flush({});

    service.updateModelDescription(7, "new description").subscribe();
    const describe = http.expectOne(`${API}/model/update/description`);
    expect(describe.request.body).toEqual({ mid: 7, description: "new description" });
    describe.flush({});
  });

  it("reads a model from the authenticated or the public endpoint", () => {
    service.getModel(7).subscribe();
    const authenticated = http.expectOne(`${API}/model/7`);
    expect(authenticated.request.method).toBe("GET");
    authenticated.flush({});

    service.getModel(7, false).subscribe();
    http.expectOne(`${API}/model/public/7`).flush({});
  });

  it("lists versions from the endpoint matching the caller's login state", () => {
    service.retrieveModelVersionList(7).subscribe();
    http.expectOne(`${API}/model/7/version/list`).flush([]);

    service.retrieveModelVersionList(7, false).subscribe();
    http.expectOne(`${API}/model/7/publicVersion/list`).flush([]);
  });

  it("reads a version's root file nodes with its size", async () => {
    const tree = { fileNodes: [], size: 42 };
    const pending = firstValueFrom(service.retrieveModelVersionFileTree(7, 3));
    http.expectOne(`${API}/model/7/version/3/rootFileNodes`).flush(tree);
    expect(await pending).toEqual(tree);

    service.retrieveModelVersionFileTree(7, 3, false).subscribe();
    http.expectOne(`${API}/model/7/publicVersion/3/rootFileNodes`).flush(tree);
  });

  it("asks for exactly one of mvid or latest on a version zip", () => {
    // The backend answers a request carrying both, or neither, with a 400.
    service.retrieveModelVersionZip(7, 3).subscribe();
    const byId = http.expectOne(req => req.url === `${API}/model/7/versionZip`);
    expect(byId.request.params.get("mvid")).toBe("3");
    expect(byId.request.params.has("latest")).toBe(false);
    byId.flush(new Blob());

    service.retrieveModelVersionZip(7).subscribe();
    const latest = http.expectOne(req => req.url === `${API}/model/7/versionZip`);
    expect(latest.request.params.get("latest")).toBe("true");
    expect(latest.request.params.has("mvid")).toBe(false);
    latest.flush(new Blob());
  });

  it("fetches a single file by following the presigned url it is handed", async () => {
    const blob = new Blob(["weights"]);
    const pending = firstValueFrom(service.retrieveModelVersionSingleFile("/model/a/m/v1/model.pt"));

    const presign = http.expectOne(
      `${API}/model/presign-download?filePath=${encodeURIComponent("/model/a/m/v1/model.pt")}`
    );
    presign.flush({ presignedUrl: "http://minio/model.pt" });
    http.expectOne("http://minio/model.pt").flush(blob);

    expect(await pending).toEqual(blob);
  });

  it("uses the anonymous presign endpoint for a logged-out viewer", () => {
    service.retrieveModelVersionSingleFile("/model/a/m/v1/model.pt", false).subscribe();
    http
      .expectOne(`${API}/model/public-presign-download?filePath=${encodeURIComponent("/model/a/m/v1/model.pt")}`)
      .flush({ presignedUrl: "http://minio/model.pt" });
    http.expectOne("http://minio/model.pt").flush(new Blob());
  });

  it("reads the presigned cover url, which is null for a model without one", async () => {
    const pending = firstValueFrom(service.getModelCoverUrl(7));
    http.expectOne(`${API}/model/7/cover-url`).flush({ url: null });
    expect(await pending).toEqual({ url: null });
  });

  it("posts a version name as text/plain and folds the file nodes into the version", async () => {
    const pending = firstValueFrom(service.createModelVersion(7, "v2"));
    const req = http.expectOne(`${API}/model/7/version/create`);

    expect(req.request.method).toBe("POST");
    expect(req.request.body).toBe("v2");
    expect(req.request.headers.get("Content-Type")).toBe("text/plain");

    const fileNodes = [{ name: "model.pt", type: "file", parentDir: "/model/a/m/v2", size: 4 }];
    req.flush({ modelVersion: { mvid: 2, mid: 7, creatorUid: 1, name: "v2" }, fileNodes });

    expect(await pending).toMatchObject({ mvid: 2, name: "v2", fileNodes });
  });

  it("lets the backend name the version when none is given", () => {
    service.createModelVersion(7, "").subscribe();
    expect(http.expectOne(`${API}/model/7/version/create`).request.body).toBe("");
  });

  it("updates the framework and the format through their own endpoints", () => {
    service.updateModelFramework(7, "onnx").subscribe();
    const framework = http.expectOne(`${API}/model/update/framework`);
    expect(framework.request.body).toEqual({ mid: 7, framework: "onnx" });
    framework.flush({});

    service.updateModelFormat(7, "safetensors").subscribe();
    const format = http.expectOne(`${API}/model/update/format`);
    expect(format.request.body).toEqual({ mid: 7, format: "safetensors" });
    format.flush({});
  });

  it("toggles publicity and downloadability without a payload", () => {
    service.updateModelPublicity(7).subscribe();
    const publicity = http.expectOne(`${API}/model/7/update/publicity`);
    expect(publicity.request.method).toBe("POST");
    expect(publicity.request.body).toEqual({});
    publicity.flush({});

    service.updateModelDownloadable(7).subscribe();
    const downloadable = http.expectOne(`${API}/model/7/update/downloadable`);
    expect(downloadable.request.body).toEqual({});
    downloadable.flush({});
  });

  it("points the cover at a path already committed to the model", () => {
    service.updateModelCoverImage(7, "v2/preview.png").subscribe();
    const req = http.expectOne(`${API}/model/7/update/cover`);
    expect(req.request.body).toEqual({ coverImage: "v2/preview.png" });
    req.flush({});
  });

  it("lists the owners of the models the user can see", async () => {
    const pending = firstValueFrom(service.retrieveOwners());
    http.expectOne(`${API}/model/user-model-owners`).flush(["alice@texera.com"]);
    expect(await pending).toEqual(["alice@texera.com"]);
  });

  it("surfaces a server error rather than swallowing it", async () => {
    const outcome = firstValueFrom(service.retrieveAccessibleModels()).catch((err: unknown) => err);
    http.expectOne(`${API}/model/list`).flush({ message: "nope" }, { status: 500, statusText: "Server Error" });
    expect(await outcome).toMatchObject({ status: 500 });
  });
});
