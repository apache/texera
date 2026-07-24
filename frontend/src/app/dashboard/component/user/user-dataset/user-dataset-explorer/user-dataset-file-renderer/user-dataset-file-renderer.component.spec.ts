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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { getMimeType, MIME_TYPES, UserDatasetFileRendererComponent } from "./user-dataset-file-renderer.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { DomSanitizer } from "@angular/platform-browser";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";
import { of } from "rxjs";
import * as Papa from "papaparse";
import { SimpleChange, SimpleChanges } from "@angular/core";

describe("UserDatasetFileRendererComponent", () => {
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [UserDatasetFileRendererComponent, HttpClientTestingModule],
      providers: [
        DatasetService,
        NotificationService,
        { provide: DomSanitizer, useValue: { bypassSecurityTrustUrl: vi.fn() } },
        ...commonTestProviders,
      ],
    });
    const fixture = TestBed.createComponent(UserDatasetFileRendererComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should return true for supported MIME type", () => {
    const supportedMimeType = "image/jpeg"; // Example of a supported MIME type
    const result = component.isPreviewSupported(supportedMimeType);
    expect(result).toBe(true);
  });

  it("should return false for unsupported MIME type", () => {
    const unsupportedMimeType = "application/unknown"; // Example of an unsupported MIME type
    const result = component.isPreviewSupported(unsupportedMimeType);
    expect(result).toBe(false);
  });

  describe("reloadFileContent", () => {
    it("flags an unsupported file type and does not hit the backend", () => {
      const spy = vi.spyOn(TestBed.inject(DatasetService), "retrieveDatasetVersionSingleFile");
      // did/dvid are set so the early-return is what stops the request, not the missing ids.
      component.did = 1;
      component.dvid = 2;
      component.filePath = "archive.bin"; // -> OCTET_STREAM -> unsupported

      component.reloadFileContent();

      expect(component.isFileTypePreviewUnsupported).toBe(true);
      expect(spy).not.toHaveBeenCalled();
    });

    it("flags an oversized file and does not hit the backend", () => {
      const spy = vi.spyOn(TestBed.inject(DatasetService), "retrieveDatasetVersionSingleFile");
      component.did = 1;
      component.dvid = 2;
      component.filePath = "notes.txt"; // TXT limit is 1 MB
      component.fileSize = 5 * 1024 * 1024;

      component.reloadFileContent();

      expect(component.isFileSizeUnloadable).toBe(true);
      expect(spy).not.toHaveBeenCalled();
    });

    it("retrieves a supported file and switches on the matching display", () => {
      const datasetService = TestBed.inject(DatasetService);
      const blob = new Blob(["hello"], { type: "text/plain" });
      const spy = vi.spyOn(datasetService, "retrieveDatasetVersionSingleFile").mockReturnValue(of(blob));
      component.did = 1;
      component.dvid = 2;
      component.filePath = "notes.txt";
      component.isLogin = true;
      component.fileSize = 100;

      component.reloadFileContent();

      expect(spy).toHaveBeenCalledWith("notes.txt", true);
      expect(component.displayPlainText).toBe(true);
      expect(component.isLoading).toBe(false);
    });
  });

  describe("error handlers", () => {
    it("onFileLoadingError sets the loading-error state and clears displays", () => {
      component.displayCSV = true;

      component.onFileLoadingError();

      expect(component.isFileLoadingError).toBe(true);
      expect(component.displayCSV).toBe(false);
    });

    it("onFileSizeNotLoadable sets the size-unloadable state", () => {
      component.onFileSizeNotLoadable();

      expect(component.isFileSizeUnloadable).toBe(true);
    });

    it("onFileTypePreviewUnsupported sets the unsupported-type state", () => {
      component.onFileTypePreviewUnsupported();

      expect(component.isFileTypePreviewUnsupported).toBe(true);
    });
  });

  describe("display toggles", () => {
    it("toggleImageModal flips showImageModal", () => {
      expect(component.showImageModal).toBe(false);

      component.toggleImageModal();
      expect(component.showImageModal).toBe(true);

      component.toggleImageModal();
      expect(component.showImageModal).toBe(false);
    });

    it("turnOffAllDisplay resets every display and error flag", () => {
      component.displayCSV = true;
      component.displayXlsx = true;
      component.displayImage = true;
      component.displayPlainText = true;
      component.displayMarkdown = true;
      component.displayJson = true;
      component.displayMP4 = true;
      component.displayMP3 = true;
      component.isLoading = true;
      component.isFileLoadingError = true;
      component.isFileSizeUnloadable = true;
      component.isFileTypePreviewUnsupported = true;

      component.turnOffAllDisplay();

      expect(component.displayCSV).toBe(false);
      expect(component.displayXlsx).toBe(false);
      expect(component.displayImage).toBe(false);
      expect(component.displayPlainText).toBe(false);
      expect(component.displayMarkdown).toBe(false);
      expect(component.displayJson).toBe(false);
      expect(component.displayMP4).toBe(false);
      expect(component.displayMP3).toBe(false);
      expect(component.isLoading).toBe(false);
      expect(component.isFileLoadingError).toBe(false);
      expect(component.isFileSizeUnloadable).toBe(false);
      expect(component.isFileTypePreviewUnsupported).toBe(false);
    });
  });

  describe("getMimeType", () => {
    it("maps known extensions to their MIME type", () => {
      expect(getMimeType("photo.png")).toBe(MIME_TYPES.PNG);
      expect(getMimeType("data.csv")).toBe(MIME_TYPES.CSV);
      expect(getMimeType("clip.mp4")).toBe(MIME_TYPES.MP4);
      expect(getMimeType("notes.json")).toBe(MIME_TYPES.JSON);
    });

    it("resolves the extension case-insensitively", () => {
      expect(getMimeType("PHOTO.PNG")).toBe(MIME_TYPES.PNG);
      expect(getMimeType("Report.Csv")).toBe(MIME_TYPES.CSV);
    });

    it("uses only the final extension for names with multiple dots", () => {
      expect(getMimeType("archive.tar.gz")).toBe(MIME_TYPES.OCTET_STREAM);
      expect(getMimeType("my.report.json")).toBe(MIME_TYPES.JSON);
    });

    it("falls back to octet-stream for unknown extensions", () => {
      expect(getMimeType("program.exe")).toBe(MIME_TYPES.OCTET_STREAM);
      expect(getMimeType("archive.bin")).toBe(MIME_TYPES.OCTET_STREAM);
    });

    it("falls back to octet-stream when there is no extension", () => {
      expect(getMimeType("README")).toBe(MIME_TYPES.OCTET_STREAM);
      expect(getMimeType("")).toBe(MIME_TYPES.OCTET_STREAM);
    });
  });

  describe("file content loading", () => {
    it("loadTabularFile sets the header, pads short rows to the header width, and keeps longer rows intact", () => {
      (component as unknown as { loadTabularFile: (d: unknown[][]) => void }).loadTabularFile([
        ["a", "b", "c"],
        ["1", "2"],
        ["x", "y", "z", "w"],
      ]);

      expect(component.tableDataHeader).toEqual(["a", "b", "c"]);
      expect(component.tableContent[0]).toEqual(["1", "2", ""]); // short row padded to the header width
      expect(component.tableContent[1]).toEqual(["x", "y", "z", "w"]);
    });

    it("loadTabularFile leaves state unchanged for empty data", () => {
      component.tableDataHeader = ["keep"];
      component.tableContent = [["keep-content"]];
      (component as unknown as { loadTabularFile: (d: unknown[][]) => void }).loadTabularFile([]);
      expect(component.tableDataHeader).toEqual(["keep"]);
      expect(component.tableContent).toEqual([["keep-content"]]);
    });

    it("readFileAsText reads the blob text into textContent", async () => {
      // Deterministic FileReader stub — fire onload on a microtask, never rely on jsdom's real async.
      class FakeFileReader {
        onload: ((e: { target: { result: string } }) => void) | null = null;
        result: string | null = null;
        readAsText(_blob: Blob): void {
          this.result = "canned text";
          queueMicrotask(() => this.onload?.({ target: { result: this.result as string } }));
        }
      }
      const realFileReader = globalThis.FileReader;
      (globalThis as unknown as { FileReader: unknown }).FileReader = FakeFileReader;
      try {
        (component as unknown as { readFileAsText: (b: Blob) => void }).readFileAsText(new Blob(["ignored"]));
        await Promise.resolve();
        expect(component.textContent).toBe("canned text");
      } finally {
        (globalThis as unknown as { FileReader: unknown }).FileReader = realFileReader;
      }
    });
  });

  describe("ngOnChanges", () => {
    // Realistic SimpleChange(previousValue, currentValue, firstChange) inputs so the tests
    // don't rely on empty {} placeholders and stay valid if ngOnChanges starts reading the values.
    const chg = (previous: unknown, current: unknown, firstChange = false): SimpleChange =>
      new SimpleChange(previous, current, firstChange);

    it("reloads when filePath changes", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ filePath: chg("/old.txt", "/new.txt") } as SimpleChanges);
      expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    it("reloads when both did and dvid change together", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ did: chg(1, 2), dvid: chg(3, 4) } as SimpleChanges);
      expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    it("does not reload when only did changes without dvid", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ did: chg(1, 2) } as SimpleChanges);
      expect(reloadSpy).not.toHaveBeenCalled();
    });

    it("does not reload for an unrelated input change", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ isMaximized: chg(false, true) } as SimpleChanges);
      expect(reloadSpy).not.toHaveBeenCalled();
    });
  });

  describe("ngOnDestroy", () => {
    it("revokes the object URL when one was created", () => {
      const originalRevoke = URL.revokeObjectURL;
      const revokeSpy = vi.fn();
      (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = revokeSpy;
      try {
        component.fileURL = "blob:to-revoke";
        component.ngOnDestroy();
        expect(revokeSpy).toHaveBeenCalledWith("blob:to-revoke");
      } finally {
        (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = originalRevoke;
      }
    });

    it("does nothing when no object URL exists", () => {
      const originalRevoke = URL.revokeObjectURL;
      const revokeSpy = vi.fn();
      (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = revokeSpy;
      try {
        component.fileURL = undefined;
        component.ngOnDestroy();
        expect(revokeSpy).not.toHaveBeenCalled();
      } finally {
        (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = originalRevoke;
      }
    });
  });

  describe("turnOffAllDisplay URL cleanup", () => {
    it("revokes both the raw and the safe object URLs when present", () => {
      const originalRevoke = URL.revokeObjectURL;
      const revokeSpy = vi.fn();
      (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = revokeSpy;
      try {
        component.fileURL = "blob:raw";
        // safeFileURL is a SafeUrl (an opaque object); the code calls .toString() on it before
        // revoking, so use an object with a toString() rather than a raw string cast.
        component.safeFileURL = { toString: () => "blob:safe" } as unknown as typeof component.safeFileURL;
        component.turnOffAllDisplay();
        expect(revokeSpy).toHaveBeenCalledWith("blob:raw");
        expect(revokeSpy).toHaveBeenCalledWith("blob:safe");
      } finally {
        (URL as unknown as { revokeObjectURL: unknown }).revokeObjectURL = originalRevoke;
      }
    });
  });

  describe("reloadFileContent viewer selection", () => {
    // Helper: drive reloadFileContent through the async subscribe with a canned blob.
    // fileSize is left undefined so the pre-check size guard is skipped and the
    // blob-size branch inside next() is exercised instead.
    function loadWith(filePath: string, blob: Blob) {
      const datasetService = TestBed.inject(DatasetService);
      vi.spyOn(datasetService, "retrieveDatasetVersionSingleFile").mockReturnValue(of(blob));
      component.did = 1;
      component.dvid = 2;
      component.filePath = filePath;
      component.isLogin = false;
      component.fileSize = undefined;
      component.reloadFileContent();
    }

    let originalCreate: typeof URL.createObjectURL;
    let createSpy: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      originalCreate = URL.createObjectURL;
      createSpy = vi.fn(() => "blob:created");
      (URL as unknown as { createObjectURL: unknown }).createObjectURL = createSpy;
    });

    afterEach(() => {
      (URL as unknown as { createObjectURL: unknown }).createObjectURL = originalCreate;
    });

    it("selects the image viewer and builds a safe URL for a PNG", () => {
      const blob = new Blob(["img"], { type: "image/png" });
      loadWith("photo.png", blob);
      expect(component.displayImage).toBe(true);
      expect(createSpy).toHaveBeenCalledWith(blob);
      expect(component.fileURL).toBe("blob:created");
    });

    it("selects the video viewer for an MP4", () => {
      const blob = new Blob(["vid"], { type: "video/mp4" });
      loadWith("clip.mp4", blob);
      expect(component.displayMP4).toBe(true);
      expect(createSpy).toHaveBeenCalledWith(blob);
    });

    it("selects the audio viewer for an MP3", () => {
      const blob = new Blob(["aud"], { type: "audio/mpeg" });
      loadWith("song.mp3", blob);
      expect(component.displayMP3).toBe(true);
      expect(createSpy).toHaveBeenCalledWith(blob);
    });

    it("selects the markdown viewer for a MD file", () => {
      const blob = new Blob(["# hi"], { type: "text/markdown" });
      loadWith("readme.md", blob);
      expect(component.displayMarkdown).toBe(true);
    });

    it("selects the JSON viewer for a JSON file", () => {
      const blob = new Blob(['{"a":1}'], { type: "application/json" });
      loadWith("data.json", blob);
      expect(component.displayJson).toBe(true);
    });

    // Papa.parse's property on the vite namespace is a non-configurable getter (cannot be spied or
    // reassigned), and module-mocking papaparse is disallowed, so the real parser runs against a
    // real CSV File and the async result is awaited via vi.waitFor.
    it("selects the CSV viewer and parses the file into the tabular header and content", async () => {
      // Reference the imported namespace so the papaparse import is retained for the real parse path.
      expect(typeof Papa.parse).toBe("function");
      const blob = new Blob(["h1,h2\na,b\n"], { type: "text/csv" });
      loadWith("data.csv", blob);
      expect(component.displayCSV).toBe(true);
      await vi.waitFor(() => expect(component.tableDataHeader.length).toBeGreaterThan(0));
      expect(component.tableDataHeader).toEqual(["h1", "h2"]);
      expect(component.tableContent[0]).toEqual(["a", "b"]);
    });

    it("flags an oversized blob returned by the backend and warns the user", () => {
      const notificationService = TestBed.inject(NotificationService);
      const warnSpy = vi.spyOn(notificationService, "warning").mockImplementation(() => {});
      // TXT limit is 1 MB; build a blob just over it. Pre-check is skipped (fileSize undefined),
      // so the inner blob.size guard is what rejects it.
      const oversized = new Blob(["x".repeat(1024 * 1024 + 10)], { type: "text/plain" });
      loadWith("big.txt", oversized);
      expect(component.isFileSizeUnloadable).toBe(true);
      expect(warnSpy).toHaveBeenCalled();
    });

    it("re-checks preview support on the loaded blob and rejects an unsupported one", () => {
      // The pre-check passes (first call true) but the in-callback re-check fails (subsequent false),
      // exercising the defensive guard inside next().
      vi.spyOn(component, "isPreviewSupported").mockReturnValueOnce(true).mockReturnValue(false);
      const blob = new Blob(["hi"], { type: "text/plain" });
      loadWith("notes.txt", blob);
      expect(component.isFileTypePreviewUnsupported).toBe(true);
    });
  });
});
