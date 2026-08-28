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

  it("surfaces a server error rather than swallowing it", async () => {
    const outcome = firstValueFrom(service.retrieveAccessibleModels()).catch((err: unknown) => err);
    http.expectOne(`${API}/model/list`).flush({ message: "nope" }, { status: 500, statusText: "Server Error" });
    expect(await outcome).toMatchObject({ status: 500 });
  });
});
