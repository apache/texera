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
import { StagedFileService } from "./staged-file.service";
import {
  DATASET_FILE_RESOURCE_ENDPOINT,
  FileResourceEndpoint,
  MODEL_FILE_RESOURCE_ENDPOINT,
} from "./file-resource-endpoint";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { commonTestProviders } from "../../../../common/testing/test-utils";

const API = "api";

describe("StagedFileService", () => {
  let service: StagedFileService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [StagedFileService, ...commonTestProviders],
    });
    service = TestBed.inject(StagedFileService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  const endpoints: Array<[string, FileResourceEndpoint]> = [
    ["dataset", DATASET_FILE_RESOURCE_ENDPOINT],
    ["model", MODEL_FILE_RESOURCE_ENDPOINT],
  ];

  for (const [name, endpoint] of endpoints) {
    it(`lists the staged objects of a ${name}`, async () => {
      const staged: DatasetStagedObject[] = [{ path: "a.txt", pathType: "file", diffType: "added", sizeBytes: 1 }];
      const pending = firstValueFrom(service.getDiff(endpoint, 7));

      http.expectOne(`${API}/${endpoint.baseUrl}/7/diff`).flush(staged);

      expect(await pending).toEqual(staged);
    });

    it(`reverts one staged ${name} path`, () => {
      service.resetFileDiff(endpoint, 7, "dir/a b.txt").subscribe();

      // The path is encoded into the query param, so a space or slash survives the round trip.
      const req = http.expectOne(
        `${API}/${endpoint.baseUrl}/7/diff?filePath=${encodeURIComponent(encodeURIComponent("dir/a b.txt"))}`
      );
      expect(req.request.method).toBe("PUT");
      req.flush({});
    });

    it(`stages a deletion of a committed ${name} file`, () => {
      service.deleteFile(endpoint, 7, "dir/a.txt").subscribe();

      const req = http.expectOne(
        `${API}/${endpoint.baseUrl}/7/file?filePath=${encodeURIComponent(encodeURIComponent("dir/a.txt"))}`
      );
      expect(req.request.method).toBe("DELETE");
      req.flush({});
    });
  }

  it("surfaces a server error rather than swallowing it", async () => {
    const outcome = firstValueFrom(service.getDiff(MODEL_FILE_RESOURCE_ENDPOINT, 7)).catch((err: unknown) => err);
    http.expectOne(`${API}/model/7/diff`).flush({ message: "nope" }, { status: 500, statusText: "Server Error" });
    expect(await outcome).toMatchObject({ status: 500 });
  });
});
