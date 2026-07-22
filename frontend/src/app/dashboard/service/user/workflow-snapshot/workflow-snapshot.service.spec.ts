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
import html2canvas from "html2canvas";
import {
  WorkflowSnapshotService,
  WORKFLOW_SNAPSHOT_API_BASE_URL,
  WORKFLOW_SNAPSHOT_UPLOAD_URL,
} from "./workflow-snapshot.service";

// Stub html2canvas so createSnapShotCanvas() can be unit tested under jsdom.
vi.mock("html2canvas", () => ({ default: vi.fn() }));

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

    it("propagates a server error to the caller", () => {
      let errored = false;
      service.uploadWorkflowSnapshot(new Blob([]), 12).subscribe({ error: (_e: unknown) => (errored = true) });

      httpMock.expectOne(WORKFLOW_SNAPSHOT_UPLOAD_URL).flush("boom", {
        status: 500,
        statusText: "Server Error",
      });

      expect(errored).toBe(true);
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

  describe("createSnapShotCanvas", () => {
    it("falls back to document.body and applies the crop ratios", async () => {
      const fakeCanvas = document.createElement("canvas");
      vi.mocked(html2canvas).mockResolvedValue(fakeCanvas);
      // No #texera-workflow-editor element, so it should fall back to document.body.
      vi.spyOn(document, "getElementById").mockReturnValue(null);
      vi.spyOn(document.body, "getBoundingClientRect").mockReturnValue({ height: 200, width: 100 } as DOMRect);

      const result = await service.createSnapShotCanvas(0.5, 0.1, 0.25, 0.2);

      expect(result).toBe(fakeCanvas);
      const [target, options] = vi.mocked(html2canvas).mock.calls[0];
      expect(target).toBe(document.body);
      expect(options).toEqual(expect.objectContaining({ height: 100, y: 20, width: 25, x: 20 }));
    });
  });
});
