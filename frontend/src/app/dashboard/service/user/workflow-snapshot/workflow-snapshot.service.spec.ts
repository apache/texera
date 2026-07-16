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

vi.mock("html2canvas", () => ({ default: vi.fn() }));

describe("WorkflowSnapshotService", () => {
  let service: WorkflowSnapshotService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    vi.clearAllMocks();
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

  describe("createSnapShotCanvas", () => {
    it("scales the target element's rect by the given ratios and returns the canvas", async () => {
      const canvas = document.createElement("canvas");
      (html2canvas as unknown as ReturnType<typeof vi.fn>).mockResolvedValue(canvas);

      // Force a deterministic rect regardless of the jsdom layout.
      vi.spyOn(document.body, "getBoundingClientRect").mockReturnValue({
        height: 200,
        width: 100,
      } as DOMRect);

      const result = await service.createSnapShotCanvas(0.5, 0.1, 0.5, 0.2);

      expect(result).toBe(canvas);
      expect(html2canvas).toHaveBeenCalledTimes(1);
      const [, options] = (html2canvas as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(options).toMatchObject({ height: 100, y: 20, width: 50, x: 20 });
    });

    it("falls back to document.body when the editor element is absent", async () => {
      const canvas = document.createElement("canvas");
      (html2canvas as unknown as ReturnType<typeof vi.fn>).mockResolvedValue(canvas);

      await service.createSnapShotCanvas(1, 0, 1, 0);

      const [element] = (html2canvas as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
      expect(element).toBe(document.body);
    });
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
  });
});
