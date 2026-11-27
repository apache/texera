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
import { HttpErrorResponse } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { CuImage, CuImageService, CuImageStatus, isInProgress, isStartable } from "./cu-image.service";

const CU_IMAGE_URL = "api/cu-image";

const image = (over: Partial<CuImage> = {}): CuImage => ({
  iid: 1,
  name: "base",
  sourceRef: "ghcr.io/apache/texera-computing-unit:latest",
  sourceDigest: null,
  status: "PENDING",
  imageTag: null,
  attempt: 1,
  creationTime: 0,
  updateTime: 0,
  ...over,
});

const ALL_STATUSES: CuImageStatus[] = ["PENDING", "VALIDATING", "READY", "FAILED"];

describe("cu-image helpers", () => {
  it("treats only a READY image as startable", () => {
    const startable = ALL_STATUSES.filter(status => isStartable(image({ status })));
    expect(startable).toEqual(["READY"]);
  });

  it("treats PENDING and VALIDATING as in progress, and nothing else", () => {
    const inProgress = ALL_STATUSES.filter(status => isInProgress(image({ status })));
    expect(inProgress).toEqual(["PENDING", "VALIDATING"]);
  });

  it("never reports an image as both startable and in progress", () => {
    for (const status of ALL_STATUSES) {
      const img = image({ status });
      expect(isStartable(img) && isInProgress(img)).toBe(false);
    }
  });
});

describe("CuImageService", () => {
  let service: CuImageService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [CuImageService],
    });
    httpTestingController = TestBed.inject(HttpTestingController);
    service = TestBed.inject(CuImageService);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("lists images with a GET to the collection", () => {
    const images = [image({ iid: 1, status: "READY" }), image({ iid: 2, status: "FAILED" })];
    let received: CuImage[] | undefined;
    service.list().subscribe(value => (received = value));

    const req = httpTestingController.expectOne(CU_IMAGE_URL);
    expect(req.request.method).toEqual("GET");
    req.flush(images);

    expect(received).toEqual(images);
  });

  it("passes an empty list through", () => {
    let received: CuImage[] | undefined;
    service.list().subscribe(value => (received = value));
    httpTestingController.expectOne(CU_IMAGE_URL).flush([]);
    expect(received).toEqual([]);
  });

  it("registers an image with a POST carrying only its name and reference", () => {
    let received: CuImage | undefined;
    service.create("pandas", "docker.io/library/python:3.12").subscribe(value => (received = value));

    const req = httpTestingController.expectOne(CU_IMAGE_URL);
    expect(req.request.method).toEqual("POST");
    expect(req.request.body).toEqual({ name: "pandas", sourceRef: "docker.io/library/python:3.12" });
    const created = image({ iid: 7, name: "pandas", sourceRef: "docker.io/library/python:3.12" });
    req.flush(created);

    expect(received).toEqual(created);
  });

  it("refreshes one image with an empty POST to its refresh path", () => {
    let received: CuImage | undefined;
    service.refresh(7).subscribe(value => (received = value));

    const req = httpTestingController.expectOne(`${CU_IMAGE_URL}/7/refresh`);
    expect(req.request.method).toEqual("POST");
    expect(req.request.body).toEqual({});
    const refreshed = image({ iid: 7, status: "VALIDATING", attempt: 2 });
    req.flush(refreshed);

    expect(received).toEqual(refreshed);
  });

  it("reads one image's validation log with a GET", () => {
    let received: unknown;
    service.log(7).subscribe(value => (received = value));

    const req = httpTestingController.expectOne(`${CU_IMAGE_URL}/7/log`);
    expect(req.request.method).toEqual("GET");
    const log = { iid: 7, status: "FAILED", attempt: 2, log: "computing-unit-master: not found" };
    req.flush(log);

    expect(received).toEqual(log);
  });

  it("deletes one image with a DELETE to its path", () => {
    let completed = false;
    service.delete(7).subscribe({ complete: () => (completed = true) });

    const req = httpTestingController.expectOne(`${CU_IMAGE_URL}/7`);
    expect(req.request.method).toEqual("DELETE");
    req.flush(null);

    expect(completed).toBe(true);
  });

  it("hands an HTTP error to the subscriber unchanged", () => {
    let status: number | undefined;
    service
      .create("bad", "not a reference")
      .subscribe({ error: (err: unknown) => (status = (err as HttpErrorResponse).status) });

    httpTestingController
      .expectOne(CU_IMAGE_URL)
      .flush({ message: "invalid reference" }, { status: 400, statusText: "Bad Request" });

    expect(status).toEqual(400);
  });
});
