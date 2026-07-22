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
});
