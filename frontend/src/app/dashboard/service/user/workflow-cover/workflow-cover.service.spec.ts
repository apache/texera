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
import { WorkflowCoverService } from "./workflow-cover.service";
import { AppSettings } from "../../../../common/app-setting";

describe("WorkflowCoverService", () => {
  let service: WorkflowCoverService;
  let httpMock: HttpTestingController;
  const coverUrl = (wid: number) => `${AppSettings.getApiEndpoint()}/workflow/${wid}/cover`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowCoverService],
    });
    service = TestBed.inject(WorkflowCoverService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("getCover returns the stored image data URL", () => {
    let result: string | undefined;
    service.getCover(7).subscribe(image => (result = image));
    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("GET");
    req.flush({ image: "data:image/jpeg;base64,abc" });
    expect(result).toBe("data:image/jpeg;base64,abc");
  });

  it("getCover resolves to undefined when no cover exists (404)", () => {
    let result: string | undefined = "unset";
    service.getCover(7).subscribe(image => (result = image));
    httpMock.expectOne(coverUrl(7)).flush(null, { status: 404, statusText: "Not Found" });
    expect(result).toBeUndefined();
  });

  it("clearCover issues a DELETE", () => {
    let completed = false;
    service.clearCover(7).subscribe(() => (completed = true));
    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("DELETE");
    req.flush(null);
    expect(completed).toBe(true);
  });

  it("setCoverFromFile PUTs the resized data URL and resolves with it", async () => {
    const dataUrl = "data:image/jpeg;base64,resized";
    // The resize step relies on canvas/Image decoding, which jsdom cannot run;
    // stub it so the test exercises the upload wiring deterministically.
    (service as any).fileToResizedDataUrl = vi.fn().mockResolvedValue(dataUrl);
    const file = new File(["x"], "pic.png", { type: "image/png" });

    const resultPromise = service.setCoverFromFile(7, file);
    // Let the stubbed resize promise settle so the HTTP request is issued.
    await Promise.resolve();
    await Promise.resolve();

    const req = httpMock.expectOne(coverUrl(7));
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ image: dataUrl });
    req.flush(null);

    await expect(resultPromise).resolves.toBe(dataUrl);
  });
});
