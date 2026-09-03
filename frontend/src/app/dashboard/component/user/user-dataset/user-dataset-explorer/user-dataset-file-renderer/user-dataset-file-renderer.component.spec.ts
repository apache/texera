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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { getMimeType, MIME_TYPES, UserDatasetFileRendererComponent } from "./user-dataset-file-renderer.component";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { ModelService } from "../../../../../service/user/model/model.service";
import { EntityType } from "../../../../../../hub/service/hub.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { DomSanitizer } from "@angular/platform-browser";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";
import { of } from "rxjs";
import * as Papa from "papaparse";
import JSZip from "jszip";
import readXlsxFile from "read-excel-file";
import { SimpleChange, SimpleChanges } from "@angular/core";
import { MarkdownModule } from "ngx-markdown";

describe("UserDatasetFileRendererComponent", () => {
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [UserDatasetFileRendererComponent, HttpClientTestingModule],
      providers: [
        DatasetService,
        ModelService,
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
      // The ids are set so the early-return is what stops the request, not the missing ids.
      component.resourceId = 1;
      component.versionId = 2;
      component.filePath = "archive.bin"; // -> OCTET_STREAM -> unsupported

      component.reloadFileContent();

      expect(component.isFileTypePreviewUnsupported).toBe(true);
      expect(spy).not.toHaveBeenCalled();
    });

    it("flags an oversized file and does not hit the backend", () => {
      const spy = vi.spyOn(TestBed.inject(DatasetService), "retrieveDatasetVersionSingleFile");
      component.resourceId = 1;
      component.versionId = 2;
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
      component.resourceId = 1;
      component.versionId = 2;
      component.filePath = "notes.txt";
      component.isLogin = true;
      component.fileSize = 100;

      component.reloadFileContent();

      expect(spy).toHaveBeenCalledWith("notes.txt", true);
      expect(component.displayPlainText).toBe(true);
      expect(component.isLoading).toBe(false);
    });

    it("fetches from the model endpoint when the file belongs to a model", () => {
      const datasetSpy = vi.spyOn(TestBed.inject(DatasetService), "retrieveDatasetVersionSingleFile");
      const modelSpy = vi
        .spyOn(TestBed.inject(ModelService), "retrieveModelVersionSingleFile")
        .mockReturnValue(of(new Blob(["weights"], { type: "text/plain" })));
      component.resourceType = EntityType.Model;
      component.resourceId = 1;
      component.versionId = 2;
      component.filePath = "/model/a/m/v1/notes.txt";
      component.isLogin = true;

      component.reloadFileContent();

      expect(modelSpy).toHaveBeenCalledWith("/model/a/m/v1/notes.txt", true);
      expect(datasetSpy).not.toHaveBeenCalled();
    });

    it("hits nothing for a kind that has no file preview", () => {
      const datasetSpy = vi.spyOn(TestBed.inject(DatasetService), "retrieveDatasetVersionSingleFile");
      component.resourceType = EntityType.Workflow;
      component.resourceId = 1;
      component.versionId = 2;
      component.filePath = "notes.txt";

      component.reloadFileContent();

      expect(datasetSpy).not.toHaveBeenCalled();
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
      expect(getMimeType("report.xlsx")).toBe(MIME_TYPES.MSEXCEL);
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

    it("reloads when both resourceId and versionId change together", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ resourceId: chg(1, 2), versionId: chg(3, 4) } as SimpleChanges);
      expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    it("does not reload when only resourceId changes without versionId", () => {
      const reloadSpy = vi.spyOn(component, "reloadFileContent").mockImplementation(() => {});
      component.ngOnChanges({ resourceId: chg(1, 2) } as SimpleChanges);
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
      component.resourceId = 1;
      component.versionId = 2;
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

    it("does not fetch when the dataset ids are missing", () => {
      const datasetService = TestBed.inject(DatasetService);
      const spy = vi.spyOn(datasetService, "retrieveDatasetVersionSingleFile");
      // A supported, in-limit file, so the two pre-checks pass and the id guard is the
      // only thing left to stop the request.
      component.resourceId = undefined;
      component.versionId = 2;
      component.filePath = "notes.txt";
      component.fileSize = 100;

      component.reloadFileContent();

      expect(spy).not.toHaveBeenCalled();
      // Characterizing a defect, not an intent: `isLoading` is set true just above the id
      // guard and nothing on this path clears it, so the spinner keeps running until some
      // later change triggers another reload. Flip this to `false` when that is fixed.
      expect(component.isLoading).toBe(true);
    });

    /**
     * The spreadsheet branch runs the real `read-excel-file`, so these build an actual
     * .xlsx with JSZip (already a dependency, and already used this way in
     * user-workflow.component.spec.ts) rather than mocking the parser module.
     */
    async function buildXlsxBlob(rowsXml: string): Promise<Blob> {
      const zip = new JSZip();
      zip.file(
        "[Content_Types].xml",
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>`
      );
      zip.file(
        "_rels/.rels",
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`
      );
      zip.file(
        "xl/workbook.xml",
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>`
      );
      zip.file(
        "xl/_rels/workbook.xml.rels",
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>`
      );
      zip.file(
        "xl/worksheets/sheet1.xml",
        `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<sheetData>${rowsXml}</sheetData>
</worksheet>`
      );
      const content = await zip.generateAsync({ type: "arraybuffer" });
      const blob = new Blob([content], { type: MIME_TYPES.MSEXCEL });
      // jsdom's Blob has no `arrayBuffer()`, which is how read-excel-file reads its input.
      // Hand back the buffer we just built rather than patching Blob.prototype globally.
      (blob as unknown as { arrayBuffer: () => Promise<ArrayBuffer> }).arrayBuffer = () => Promise.resolve(content);
      return blob;
    }

    // `getMimeType` looks an uppercased extension up as a key of MIME_TYPES, and the Excel key
    // is MSEXCEL — so ".msexcel" is the only suffix that reaches this branch, and a real
    // ".xlsx"/".xls" resolves to OCTET_STREAM and is rejected as unsupported. These tests use
    // the suffix the code actually accepts; see the PR for the defect that implies.
    const spreadsheetPath = "data.msexcel";

    it("selects the spreadsheet viewer and parses the workbook into the table", async () => {
      // Row 2 skips column B, so the parser yields a null cell and the empty-string arm of
      // the cell mapping is exercised alongside the populated one.
      const blob = await buildXlsxBlob(`
        <row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c></row>
        <row r="2"><c r="A2"><v>3</v></c><c r="C2"><v>4</v></c></row>
      `);

      loadWith(spreadsheetPath, blob);

      await vi.waitFor(() => expect(component.displayXlsx).toBe(true));
      // Every row is padded to the widest one, so the header picks up a trailing empty cell
      // from row 2's column C.
      expect(component.tableDataHeader).toEqual(["1", "2", ""]);
      expect(component.tableContent[0]).toEqual(["3", "", "4"]);
    });

    it("leaves the spreadsheet viewer off for a workbook with no rows", async () => {
      const blob = await buildXlsxBlob("");

      loadWith(spreadsheetPath, blob);
      // Parsing the same workbook here is the barrier: it starts strictly after the
      // component's read of an identical blob and runs the identical promise chain, so it
      // cannot settle first. No timers and no counting of microtask turns.
      await readXlsxFile(blob as unknown as File);

      expect(component.displayXlsx).toBe(false);
      expect(component.tableDataHeader).toEqual([]);
    });
  });

  /**
   * papaparse reads a File through `FileReader`, so replacing the global with a fake that
   * settles on a microtask makes both the empty-result and the read-failure arms of the CSV
   * branch deterministic — jsdom's real timing is never relied on.
   */
  describe("CSV parsing outcomes", () => {
    let realFileReader: typeof globalThis.FileReader;
    let outcome: { text: string } | { fail: true };

    class FakeFileReader {
      public result: string | null = null;
      public error: unknown = null;
      public onload: ((event: unknown) => void) | null = null;
      public onerror: ((event: unknown) => void) | null = null;
      readAsText(): void {
        queueMicrotask(() => {
          if ("fail" in outcome) {
            this.error = new Error("read failed");
            this.onerror?.({});
          } else {
            this.result = outcome.text;
            this.onload?.({ target: { result: outcome.text } });
          }
        });
      }
      abort(): void {}
    }

    beforeEach(() => {
      realFileReader = globalThis.FileReader;
      (globalThis as unknown as { FileReader: unknown }).FileReader = FakeFileReader;
    });

    afterEach(() => {
      (globalThis as unknown as { FileReader: unknown }).FileReader = realFileReader;
    });

    function loadCsv(): void {
      const datasetService = TestBed.inject(DatasetService);
      vi.spyOn(datasetService, "retrieveDatasetVersionSingleFile").mockReturnValue(
        of(new Blob(["ignored"], { type: MIME_TYPES.CSV }))
      );
      component.resourceId = 1;
      component.versionId = 2;
      component.filePath = "data.csv";
      component.fileSize = undefined;
      component.reloadFileContent();
    }

    it("leaves the table empty when the CSV has no rows", async () => {
      outcome = { text: "" };

      loadCsv();
      await vi.waitFor(() => expect(component.displayCSV).toBe(true));

      expect(component.tableDataHeader).toEqual([]);
      expect(component.tableContent).toEqual([]);
    });

    it("flags a loading error when the CSV cannot be read", async () => {
      const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      outcome = { fail: true };

      loadCsv();

      await vi.waitFor(() => expect(component.isFileLoadingError).toBe(true));
      expect(consoleSpy).toHaveBeenCalled();
    });
  });
});

/**
 * The template is a viewer switch: each `displayX` flag selects exactly one preview, and the media
 * branches additionally require `safeFileURL` so a flag on its own cannot render a source-less
 * <img>/<video>/<audio>. The suite above drives the flags; this one checks what they put on screen.
 *
 * Its own TestBed configuration supplies a DomSanitizer with `sanitize`, which Angular calls when
 * binding [src] — the stub used above only has `bypassSecurityTrustUrl` and cannot render media.
 */
describe("UserDatasetFileRendererComponent rendering", () => {
  let fixture: ComponentFixture<UserDatasetFileRendererComponent>;
  let component: UserDatasetFileRendererComponent;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      // MarkdownModule.forRoot() backs the <markdown> element in the markdown preview.
      imports: [UserDatasetFileRendererComponent, HttpClientTestingModule, MarkdownModule.forRoot()],
      providers: [
        DatasetService,
        ModelService,
        NotificationService,
        {
          provide: DomSanitizer,
          useValue: {
            bypassSecurityTrustUrl: (url: string) => url,
            sanitize: (_context: number, value: string) => value,
          },
        },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(UserDatasetFileRendererComponent);
    component = fixture.componentInstance;
  });

  /**
   * Applies a viewer state and renders it.
   *
   * The first change-detection cycle runs ngOnInit, which inspects the (empty) filePath and settles
   * on "preview unsupported". Flags set before that cycle are silently overwritten, so the state is
   * cleared through the component's own reset and applied afterwards.
   */
  function render(setup: (c: UserDatasetFileRendererComponent) => void): HTMLElement {
    fixture.detectChanges();
    component.turnOffAllDisplay();
    setup(component);
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  }

  describe("status messages", () => {
    it("shows a spinner while the file is loading", () => {
      const el = render(c => (c.isLoading = true));

      expect(el.querySelector("nz-spin")).not.toBeNull();
      expect(el.textContent).toContain("File content is loading");
    });

    it("shows an error alert when loading failed", () => {
      const el = render(c => (c.isFileLoadingError = true));

      expect(el.textContent).toContain("File loading encounter error.");
    });

    it("shows a distinct alert for a file that is too large", () => {
      const el = render(c => (c.isFileSizeUnloadable = true));

      expect(el.textContent).toContain("File is too large to preview");
      expect(el.textContent).not.toContain("Preview of the file type is currently not supported");
    });

    it("shows a distinct alert for an unsupported file type", () => {
      const el = render(c => (c.isFileTypePreviewUnsupported = true));

      expect(el.textContent).toContain("Preview of the file type is currently not supported");
      expect(el.textContent).not.toContain("File is too large to preview");
    });
  });

  describe("tabular preview", () => {
    it("renders the parsed header and cells for a CSV", () => {
      const el = render(c => {
        c.displayCSV = true;
        c.tableDataHeader = ["name", "score"];
        c.tableContent = [
          ["ada", "10"],
          ["grace", "20"],
        ];
      });

      expect(el.querySelectorAll("th")).toHaveLength(2);
      expect(Array.from(el.querySelectorAll("th")).map(th => th.textContent?.trim())).toEqual(["name", "score"]);
      expect(el.textContent).toContain("grace");
    });

    it("uses the same table for a spreadsheet", () => {
      const el = render(c => {
        c.displayXlsx = true;
        c.tableDataHeader = ["col"];
        c.tableContent = [["cell"]];
      });

      expect(el.querySelector("nz-table")).not.toBeNull();
      expect(el.textContent).toContain("cell");
    });
  });

  describe("media previews", () => {
    it("renders an image and opens the full-size modal when it is clicked", () => {
      const el = render(c => {
        c.displayImage = true;
        c.safeFileURL = "blob:image";
      });
      const img = el.querySelector<HTMLImageElement>(".file-display-area img");
      expect(img).not.toBeNull();

      img!.click();
      fixture.detectChanges();

      expect(component.showImageModal).toBe(true);
      expect(el.querySelector(".image-modal")).not.toBeNull();
    });

    it("renders a video for an MP4 and not an audio player", () => {
      const el = render(c => {
        c.displayMP4 = true;
        c.safeFileURL = "blob:video";
      });

      expect(el.querySelector("video")).not.toBeNull();
      expect(el.querySelector("audio")).toBeNull();
    });

    it("renders an audio player for an MP3 and not a video", () => {
      const el = render(c => {
        c.displayMP3 = true;
        c.safeFileURL = "blob:audio";
      });

      expect(el.querySelector("audio")).not.toBeNull();
      expect(el.querySelector("video")).toBeNull();
    });

    it("renders no media element while the safe URL is still missing", () => {
      // The flag is set as soon as the MIME type is known, but the object URL is built
      // asynchronously; rendering on the flag alone would emit a source-less element.
      const el = render(c => {
        c.displayImage = true;
        c.displayMP4 = true;
        c.displayMP3 = true;
        c.safeFileURL = undefined;
      });

      expect(el.querySelector(".file-display-area img")).toBeNull();
      expect(el.querySelector("video")).toBeNull();
      expect(el.querySelector("audio")).toBeNull();
    });
  });

  describe("text previews", () => {
    it("renders markdown through the markdown viewer", () => {
      const el = render(c => {
        c.displayMarkdown = true;
        c.textContent = "# heading";
      });

      expect(el.querySelector("markdown")).not.toBeNull();
      expect(el.querySelector("ngx-json-viewer")).toBeNull();
    });

    it("renders JSON through the JSON viewer", () => {
      const el = render(c => {
        c.displayJson = true;
        c.textContent = '{"a":1}';
      });

      expect(el.querySelector("ngx-json-viewer")).not.toBeNull();
      expect(el.querySelector("markdown")).toBeNull();
    });

    it("renders plain text inline", () => {
      const el = render(c => {
        c.displayPlainText = true;
        c.textContent = "hello world";
      });

      expect(el.textContent).toContain("hello world");
      expect(el.querySelector("markdown")).toBeNull();
    });
  });

  it("renders nothing in the display area until a viewer is selected", () => {
    const el = render(() => {});

    const area = el.querySelector(".file-display-area")!;
    expect(area.textContent?.trim()).toBe("");
  });

  it("fills the container height only when maximized", () => {
    const outer = render(c => (c.isMaximized = false)).querySelector<HTMLElement>("div")!;
    expect(outer.style.height).toBe("80%");

    const maximized = render(c => (c.isMaximized = true)).querySelector<HTMLElement>("div")!;
    expect(maximized.style.height).toBe("100%");
  });
});
