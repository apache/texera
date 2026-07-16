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

import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import {
  WorkflowSnapshotService,
  WORKFLOW_SNAPSHOT_API_BASE_URL,
  WORKFLOW_SNAPSHOT_UPLOAD_URL,
} from "./workflow-snapshot.service";

// createSnapShotCanvas() delegates to html2canvas, whose module-level mock is not
// reliable here (other specs import it unmocked and the builder shares one module
// registry), so it is exercised in the e2e/browser suite rather than pinned here.
describe("WorkflowSnapshotService", () => {
  let service: WorkflowSnapshotService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowSnapshotService],
    });
    service = TestBed.inject(WorkflowSnapshotService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("uploadWorkflowSnapshot", () => {
    it("PUTs a multipart form with the blob and stringified wid", () => {
      const blob = new Blob(["img"], { type: "image/png" });
      service.uploadWorkflowSnapshot(blob, 12).subscribe();

      const req = httpMock.expectOne(WORKFLOW_SNAPSHOT_UPLOAD_URL);
      expect(req.request.method).toEqual("PUT");
      const body = req.request.body as FormData;
      expect(body.get("wid")).toEqual("12");
      expect(body.get("SnapshotBlob")).toBeInstanceOf(Blob);
      req.flush({});
    });

    it("sends an empty wid string when wid is undefined", () => {
      service.uploadWorkflowSnapshot(new Blob([]), undefined).subscribe();

      const req = httpMock.expectOne(WORKFLOW_SNAPSHOT_UPLOAD_URL);
      expect((req.request.body as FormData).get("wid")).toEqual("");
      req.flush({});
    });
  });

  describe("retrieveWorkflowSnapshot", () => {
    it("GETs the per-snapshot endpoint", () => {
      const entry = { sid: 3, snapshot: "data" } as any;
      let result: any;
      service.retrieveWorkflowSnapshot(3).subscribe(r => (result = r));

      const req = httpMock.expectOne(`${WORKFLOW_SNAPSHOT_API_BASE_URL}/3`);
      expect(req.request.method).toEqual("GET");
      req.flush(entry);

      expect(result).toEqual(entry);
    });

    it("propagates a server error to the caller", () => {
      let errored = false;
      service.retrieveWorkflowSnapshot(9).subscribe({ error: (_e: unknown) => (errored = true) });

      httpMock.expectOne(`${WORKFLOW_SNAPSHOT_API_BASE_URL}/9`).flush("boom", {
        status: 500,
        statusText: "Server Error",
      });

      expect(errored).toBe(true);
    });
  });
});
