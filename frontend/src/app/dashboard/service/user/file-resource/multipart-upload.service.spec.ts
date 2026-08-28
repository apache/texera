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

import { MultipartUploadProgress, MultipartUploadService } from "./multipart-upload.service";
import { DATASET_FILE_RESOURCE_ENDPOINT, FileResourceEndpoint } from "./file-resource-endpoint";
import { FakeXMLHttpRequest } from "./testing/fake-xml-http-request";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { AuthService } from "src/app/common/service/user/auth.service";

const API = "api";

/**
 * A second endpoint that is neither dataset nor model. Using a synthetic one proves the engine is
 * genuinely parameterized rather than accidentally dataset-shaped.
 */
const WIDGET_ENDPOINT: FileResourceEndpoint = {
  baseUrl: "widget",
  nameParamKey: "widgetName",
  maxFileSizeSettingKey: "widget_single_file_upload_max_size_mib",
  defaultMaxFileSizeMiB: 64,
  chunkSizeSettingKey: "widget_multipart_upload_chunk_size_mib",
  maxConcurrentChunksSettingKey: "widget_max_number_of_concurrent_uploading_file_chunks",
  maxConcurrentFilesSettingKey: "widget_max_number_of_concurrent_uploading_file",
};

describe("MultipartUploadService", () => {
  let service: MultipartUploadService;
  let http: HttpTestingController;

  const isType =
    (endpoint: FileResourceEndpoint, type: string) => (r: { url: string; params: { get(k: string): string | null } }) =>
      r.url === `${API}/${endpoint.baseUrl}/multipart-upload` && r.params.get("type") === type;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [MultipartUploadService, ...commonTestProviders],
    });
    service = TestBed.inject(MultipartUploadService);
    http = TestBed.inject(HttpTestingController);
    FakeXMLHttpRequest.reset();
  });

  afterEach(() => {
    http.verify();
  });

  // ─── the endpoint seam ────────────────────────────────────────────────────

  it("addresses init, part and finish through the supplied endpoint", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File([new Uint8Array(4)], "w.bin");
    const done = new Promise<void>((resolve, reject) => {
      service
        .multipartUpload(WIDGET_ENDPOINT, "o@e.com", "my-widget", "w.bin", file, 4, 1, false)
        .subscribe({ error: reject, complete: resolve });
    });

    const init = http.expectOne(isType(WIDGET_ENDPOINT, "init"));
    expect(init.request.params.get("widgetName")).toBe("my-widget");
    expect(init.request.params.get("datasetName")).toBeNull();
    init.flush({ missingParts: [1], completedPartsCount: 0 });

    // The part URL is built by hand rather than through HttpParams, so it needs asserting
    // separately — a hardcoded name key here would only break for non-dataset resources.
    const xhr = FakeXMLHttpRequest.instances[0];
    expect(xhr.url.startsWith(`${API}/widget/multipart-upload/part`)).toBe(true);
    expect(xhr.params().get("widgetName")).toBe("my-widget");
    expect(xhr.params().get("datasetName")).toBeNull();
    xhr.respond(204);

    const finish = http.expectOne(isType(WIDGET_ENDPOINT, "finish"));
    expect(finish.request.params.get("widgetName")).toBe("my-widget");
    finish.flush({});
    await done;
  });

  it("uses the endpoint for listMultipartUploads", async () => {
    const pending = firstValueFrom(service.listMultipartUploads(WIDGET_ENDPOINT, "o@e.com", "my-widget"));
    const req = http.expectOne(isType(WIDGET_ENDPOINT, "list"));
    expect(req.request.params.get("widgetName")).toBe("my-widget");
    req.flush({ filePaths: ["a"] });
    expect(await pending).toEqual(["a"]);
  });

  it("uses the endpoint for findExistingUploadFiles", async () => {
    const pending = firstValueFrom(
      service.findExistingUploadFiles(WIDGET_ENDPOINT, 7, [{ path: "a.csv", sizeBytes: 1 }])
    );
    const req = http.expectOne(`${API}/widget/7/existing-upload-files`);
    expect(req.request.body).toEqual({ files: [{ path: "a.csv", sizeBytes: 1 }] });
    req.flush({ filePaths: ["a.csv"] });
    expect(await pending).toEqual(["a.csv"]);
  });

  it("uses the endpoint for finalizeMultipartUpload and distinguishes abort from finish", () => {
    service.finalizeMultipartUpload(WIDGET_ENDPOINT, "o@e.com", "my-widget", "f", true).subscribe();
    const abort = http.expectOne(isType(WIDGET_ENDPOINT, "abort"));
    expect(abort.request.params.get("widgetName")).toBe("my-widget");
    abort.flush({});

    service.finalizeMultipartUpload(WIDGET_ENDPOINT, "o@e.com", "my-widget", "f", false).subscribe();
    http.expectOne(isType(WIDGET_ENDPOINT, "finish")).flush({});
  });

  // ─── upload flow ──────────────────────────────────────────────────────────

  it("emits progress, attaches the auth header, and finishes at 100%", async () => {
    const tokenSpy = vi.spyOn(AuthService, "getAccessToken").mockReturnValue("tok123");
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    try {
      const file = new File([new Uint8Array(8)], "d.bin");
      const emissions: MultipartUploadProgress[] = [];
      const done = new Promise<void>((resolve, reject) => {
        service
          .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "d.bin", file, 8, 1, false)
          .subscribe({ next: p => emissions.push(p), error: reject, complete: resolve });
      });

      http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
        missingParts: [1],
        completedPartsCount: 0,
      });

      const xhr = FakeXMLHttpRequest.instances[0];
      expect(xhr.requestHeaders.get("Content-Type")).toBe("application/octet-stream");
      expect(xhr.requestHeaders.get("Authorization")).toBe("Bearer tok123");

      xhr.emitProgress(4);
      xhr.respond(200);
      http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "finish")).flush({});
      await done;

      expect(emissions[0]).toMatchObject({ status: "initializing" });
      expect(emissions.some(e => e.status === "uploading" && e.percentage > 0 && e.percentage <= 99)).toBe(true);
      expect(emissions.at(-1)).toMatchObject({ status: "finished", percentage: 100 });
    } finally {
      tokenSpy.mockRestore();
    }
  });

  it("omits the auth header when there is no access token", () => {
    const tokenSpy = vi.spyOn(AuthService, "getAccessToken").mockReturnValue(null);
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    try {
      const file = new File([new Uint8Array(4)], "anon.bin");
      const subscription = service
        .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "anon.bin", file, 4, 1, false)
        .subscribe();
      http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
        missingParts: [1],
        completedPartsCount: 0,
      });

      expect(FakeXMLHttpRequest.instances[0].requestHeaders.has("Authorization")).toBe(false);
      subscription.unsubscribe();
    } finally {
      tokenSpy.mockRestore();
    }
  });

  it("resumes by uploading only the parts the backend reports missing", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File(["abcdefgh"], "resume.txt");
    const emissions: MultipartUploadProgress[] = [];
    const done = new Promise<void>((resolve, reject) => {
      service
        .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "resume.txt", file, 4, 1, false)
        .subscribe({ next: p => emissions.push(p), error: reject, complete: resolve });
    });

    // One of two parts already landed, so the baseline starts at 50%.
    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
      missingParts: [2],
      completedPartsCount: 1,
    });

    expect(emissions[0]).toMatchObject({ percentage: 50, status: "initializing" });
    expect(FakeXMLHttpRequest.instances.map(x => x.params().get("partNumber"))).toEqual(["2"]);
    FakeXMLHttpRequest.instances[0].respond(204);

    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "finish")).flush({});
    await done;
    expect(emissions.at(-1)).toMatchObject({ percentage: 100, status: "finished" });
  });

  it("finishes without uploading anything when no parts are missing", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File([new Uint8Array(4)], "c.bin");
    const done = new Promise<void>((resolve, reject) => {
      service
        .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "c.bin", file, 4, 1, false)
        .subscribe({ error: reject, complete: resolve });
    });

    // A sparse payload also exercises the nullish-coalescing defaults.
    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({});
    expect(FakeXMLHttpRequest.instances.length).toBe(0);
    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "finish")).flush({});
    await done;
  });

  // ─── failure and teardown ─────────────────────────────────────────────────

  it("fails the upload when a part returns a non-2xx status", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File([new Uint8Array(4)], "e.bin");
    const emissions: MultipartUploadProgress[] = [];
    const outcome = new Promise<unknown>(resolve => {
      service
        .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "e.bin", file, 4, 1, false)
        .subscribe({ next: p => emissions.push(p), error: resolve, complete: () => resolve(null) });
    });

    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
      missingParts: [1],
      completedPartsCount: 0,
    });
    FakeXMLHttpRequest.instances[0].respond(500);

    const err = await outcome;
    expect((err as Error).message).toContain("HTTP 500");
    expect(emissions.at(-1)).toMatchObject({ status: "failed" });
  });

  it("fails the upload when a part errors at the transport level", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File([new Uint8Array(4)], "x.bin");
    const outcome = new Promise<unknown>(resolve => {
      service
        .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "x.bin", file, 4, 1, false)
        .subscribe({ error: resolve, complete: () => resolve(null) });
    });

    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
      missingParts: [1],
      completedPartsCount: 0,
    });
    FakeXMLHttpRequest.instances[0].fail();

    expect(await outcome).toBeInstanceOf(Error);
  });

  it("aborts the in-flight part request when the caller unsubscribes", () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const file = new File([new Uint8Array(4)], "t.bin");
    const subscription = service
      .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", "t.bin", file, 4, 1, false)
      .subscribe();

    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init")).flush({
      missingParts: [1],
      completedPartsCount: 0,
    });

    const xhr = FakeXMLHttpRequest.instances[0];
    expect(xhr.aborted).toBe(false);

    // Teardown must reach the inner per-part closure, not just the outer subscription.
    subscription.unsubscribe();

    expect(xhr.aborted).toBe(true);
  });

  // ─── payload tolerance ────────────────────────────────────────────────────

  it("tolerates null payloads from the list and existing-files endpoints", async () => {
    const listPending = firstValueFrom(service.listMultipartUploads(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds"));
    http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "list")).flush(null);
    expect(await listPending).toEqual([]);

    const existingPending = firstValueFrom(service.findExistingUploadFiles(DATASET_FILE_RESOURCE_ENDPOINT, 7, []));
    http.expectOne(`${API}/dataset/7/existing-upload-files`).flush(null);
    expect(await existingPending).toEqual([]);
  });

  it("percent-encodes the file path on both the init params and the part URL", async () => {
    vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest);
    const nested = "folder sub/a+b.csv";
    const file = new File([new Uint8Array(4)], "a.csv");
    const subscription = service
      .multipartUpload(DATASET_FILE_RESOURCE_ENDPOINT, "o@e.com", "ds", nested, file, 4, 1, false)
      .subscribe();

    const init = http.expectOne(isType(DATASET_FILE_RESOURCE_ENDPOINT, "init"));
    // HttpParams decodes on read, so this asserts the pre-encoding the backend expects.
    expect(init.request.params.get("filePath")).toBe(encodeURIComponent(nested));
    init.flush({ missingParts: [1], completedPartsCount: 0 });

    // The hand-built part URL encodes once, so reading it back yields the raw path.
    expect(FakeXMLHttpRequest.instances[0].params().get("filePath")).toBe(nested);
    subscription.unsubscribe();
  });
});
