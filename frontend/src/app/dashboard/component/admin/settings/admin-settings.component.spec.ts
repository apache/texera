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
import { AdminSettingsComponent } from "./admin-settings.component";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzMessageService } from "ng-zorro-antd/message";
import { NotificationService } from "../../../../common/service/notification/notification.service";

describe("AdminSettingsComponent", () => {
  let component: AdminSettingsComponent;
  let fixture: ComponentFixture<AdminSettingsComponent>;
  let httpTestingController: HttpTestingController;

  const SETTINGS_URL = "/api/config/settings";

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AdminSettingsComponent, HttpClientTestingModule, NzCardModule],
    }).compileComponents();
  });

  beforeEach(() => {
    httpTestingController = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(AdminSettingsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("renders MiB unit beside both size-based inputs", () => {
    const units = fixture.nativeElement.querySelectorAll(".input-with-unit .unit");
    expect(units.length).toBe(2);
    units.forEach((el: HTMLElement) => {
      expect(el.textContent?.trim()).toBe("MiB");
    });
  });

  it("loads every form field from one bulk settings request", () => {
    const req = httpTestingController.expectOne(SETTINGS_URL);
    expect(req.request.method).toBe("GET");
    req.flush({
      logo: "logo.png",
      mini_logo: "mini.png",
      favicon: "fav.ico",
      hub_enabled: "true",
      home_enabled: "false",
      max_number_of_concurrent_uploading_file: "5",
      single_file_upload_max_size_mib: "128",
      max_number_of_concurrent_uploading_file_chunks: "7",
      multipart_upload_chunk_size_mib: "64",
      csv_parser_max_columns: "4096",
    });

    expect(component.logoData).toBe("logo.png");
    expect(component.miniLogoData).toBe("mini.png");
    expect(component.faviconData).toBe("fav.ico");
    expect(component.sidebarTabs.hub_enabled).toBe(true);
    expect(component.sidebarTabs.home_enabled).toBe(false);
    expect(component.maxConcurrentFiles).toBe(5);
    expect(component.maxFileSizeMiB).toBe(128);
    expect(component.maxConcurrentChunks).toBe(7);
    expect(component.chunkSizeMiB).toBe(64);
    expect(component.csvMaxColumns).toBe(4096);
  });

  it("keeps the initializer defaults for missing or unparsable values", () => {
    httpTestingController.expectOne(SETTINGS_URL).flush({
      single_file_upload_max_size_mib: "not-a-number",
    });

    expect(component.logoData).toBeNull();
    expect(component.maxFileSizeMiB).toBe(20);
    expect(component.maxConcurrentFiles).toBe(3);
    expect(component.csvMaxColumns).toBe(512);
  });

  it("surfaces a load failure through the message service", () => {
    const message = TestBed.inject(NzMessageService);
    const errorSpy = vi.spyOn(message, "error").mockReturnValue({} as ReturnType<NzMessageService["error"]>);

    httpTestingController.expectOne(SETTINGS_URL).flush("boom", { status: 500, statusText: "Server Error" });

    expect(errorSpy).toHaveBeenCalledWith("Failed to load settings.");
  });

  it("preserves a legitimately stored 0 instead of falling back to the default", () => {
    httpTestingController.expectOne(SETTINGS_URL).flush({
      max_number_of_concurrent_uploading_file: "0",
      csv_parser_max_columns: "0",
    });

    expect(component.maxConcurrentFiles).toBe(0);
    expect(component.csvMaxColumns).toBe(0);
  });

  it("blocks a tab save when the bulk load failed (no destructive all-off write)", () => {
    const message = TestBed.inject(NzMessageService);
    const errorSpy = vi.spyOn(message, "error").mockReturnValue({} as ReturnType<NzMessageService["error"]>);
    httpTestingController.expectOne(SETTINGS_URL).flush("boom", { status: 500, statusText: "Server Error" });

    component.saveTabs();

    httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
    expect(errorSpy).toHaveBeenCalledWith("Settings have not loaded; refresh before saving.");
  });

  describe("save / reset handlers", () => {
    let msgSuccess: ReturnType<typeof vi.fn>;
    let msgError: ReturnType<typeof vi.fn>;
    let msgInfo: ReturnType<typeof vi.fn>;
    let notifySuccess: ReturnType<typeof vi.fn>;
    let notifyError: ReturnType<typeof vi.fn>;
    let notifyInfo: ReturnType<typeof vi.fn>;

    const updateUrl = (key: string) => `${SETTINGS_URL}/${key}`;
    const resetUrl = (key: string) => `${SETTINGS_URL}/reset/${key}`;
    const HTTP_ERROR = { status: 500, statusText: "Server Error" };

    // Flush the ngOnInit bulk GET so the component is `settingsLoaded`.
    function completeLoad(settings: Record<string, string> = {}): void {
      httpTestingController.expectOne(SETTINGS_URL).flush(settings);
    }

    beforeEach(() => {
      // The reset/save handlers schedule window.location.reload() via setTimeout;
      // fake timers keep it from firing (jsdom can't navigate).
      vi.useFakeTimers();

      const message = TestBed.inject(NzMessageService);
      const notification = TestBed.inject(NotificationService);
      msgSuccess = vi.spyOn(message, "success").mockReturnValue({} as ReturnType<NzMessageService["success"]>);
      msgError = vi.spyOn(message, "error").mockReturnValue({} as ReturnType<NzMessageService["error"]>);
      msgInfo = vi.spyOn(message, "info").mockReturnValue({} as ReturnType<NzMessageService["info"]>);
      notifySuccess = vi.spyOn(notification, "success").mockImplementation(() => {});
      notifyError = vi.spyOn(notification, "error").mockImplementation(() => {});
      notifyInfo = vi.spyOn(notification, "info").mockImplementation(() => {});
    });

    afterEach(() => {
      // nz-icon lazily fetches its SVG assets over HTTP; drain those so verify()
      // only asserts on the requests the handlers under test actually issue.
      httpTestingController.match(req => req.url.startsWith("assets/")).forEach(req => req.flush(""));
      httpTestingController.verify();
      vi.useRealTimers();
      vi.restoreAllMocks();
    });

    describe("branding", () => {
      it("saveLogos PUTs only the branding assets that are set and notifies success", () => {
        completeLoad();
        component.logoData = "logo.png";
        component.miniLogoData = "mini.png";
        component.faviconData = null;

        component.saveLogos();

        const logoReq = httpTestingController.expectOne(updateUrl("logo"));
        expect(logoReq.request.method).toBe("PUT");
        expect(logoReq.request.body).toEqual({ value: "logo.png" });
        const miniReq = httpTestingController.expectOne(updateUrl("mini_logo"));
        expect(miniReq.request.body).toEqual({ value: "mini.png" });
        httpTestingController.expectNone(updateUrl("favicon"));
        logoReq.flush(null);
        miniReq.flush(null);

        expect(msgSuccess).toHaveBeenCalledWith("Branding saved successfully.");
      });

      it("saveLogos does nothing when no branding asset is set", () => {
        completeLoad();
        component.logoData = null;
        component.miniLogoData = null;
        component.faviconData = null;

        component.saveLogos();

        httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
        expect(msgSuccess).not.toHaveBeenCalled();
      });

      it("saveLogos notifies an error when the request fails", () => {
        completeLoad();
        component.logoData = "logo.png";
        component.miniLogoData = null;
        component.faviconData = null;

        component.saveLogos();

        httpTestingController.expectOne(updateUrl("logo")).flush("boom", HTTP_ERROR);
        expect(msgError).toHaveBeenCalledWith("Failed to save branding.");
      });

      it("resetBranding POSTs a reset for all three branding settings", () => {
        completeLoad();

        component.resetBranding();

        ["logo", "mini_logo", "favicon"].forEach(key => {
          const req = httpTestingController.expectOne(resetUrl(key));
          expect(req.request.method).toBe("POST");
          req.flush(null);
        });
        expect(msgInfo).toHaveBeenCalledWith("Resetting branding...");
      });
    });

    describe("tabs", () => {
      it("saveTabs PUTs every sidebar tab and notifies success", () => {
        completeLoad();

        component.saveTabs();

        Object.keys(component.sidebarTabs).forEach(tab => {
          const req = httpTestingController.expectOne(updateUrl(tab));
          expect(req.request.method).toBe("PUT");
          req.flush(null);
        });
        expect(msgSuccess).toHaveBeenCalledWith("Tabs saved successfully.");
      });

      it("saveTabs notifies an error when a request fails", () => {
        completeLoad();

        component.saveTabs();

        // Fail the last request; forkJoin errors only after the earlier ones
        // have resolved, so every issued PUT is flushed (none left pending).
        const tabs = Object.keys(component.sidebarTabs);
        tabs.forEach((tab, i) => {
          const req = httpTestingController.expectOne(updateUrl(tab));
          if (i === tabs.length - 1) req.flush("boom", HTTP_ERROR);
          else req.flush(null);
        });
        expect(msgError).toHaveBeenCalledWith("Failed to save tabs.");
      });

      it("resetTabs POSTs a reset for every sidebar tab", () => {
        completeLoad();

        component.resetTabs();

        Object.keys(component.sidebarTabs).forEach(tab => {
          httpTestingController.expectOne(resetUrl(tab)).flush(null);
        });
        expect(msgInfo).toHaveBeenCalledWith("Resetting tabs...");
      });
    });

    describe("dataset settings", () => {
      it("saveDatasetSettings PUTs the four upload settings and notifies success", () => {
        completeLoad(); // defaults (20 / 3 / 10 / 50) are valid

        component.saveDatasetSettings();

        const expectPut = (key: string, value: string) => {
          const req = httpTestingController.expectOne(updateUrl(key));
          expect(req.request.method).toBe("PUT");
          expect(req.request.body).toEqual({ value });
          req.flush(null);
        };
        expectPut("max_number_of_concurrent_uploading_file", "3");
        expectPut("single_file_upload_max_size_mib", "20");
        expectPut("max_number_of_concurrent_uploading_file_chunks", "10");
        expectPut("multipart_upload_chunk_size_mib", "50");

        expect(msgSuccess).toHaveBeenCalledWith("Dataset upload settings saved successfully.");
      });

      it("saveDatasetSettings rejects non-positive values without saving", () => {
        completeLoad();
        component.maxFileSizeMiB = 0;

        component.saveDatasetSettings();

        httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
        expect(msgError).toHaveBeenCalledWith("Please enter valid integer values.");
      });

      it("saveDatasetSettings rejects a configuration that would exceed the 10,000-part limit", () => {
        completeLoad();
        component.maxFileSizeMiB = 100000;
        component.chunkSizeMiB = 1;

        component.saveDatasetSettings();

        httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
        expect(msgError).toHaveBeenCalled();
      });

      it("saveDatasetSettings notifies an error when a request fails", () => {
        completeLoad();

        component.saveDatasetSettings();

        // Fail the last of the four PUTs so forkJoin errors with every request flushed.
        const keys = [
          "max_number_of_concurrent_uploading_file",
          "single_file_upload_max_size_mib",
          "max_number_of_concurrent_uploading_file_chunks",
          "multipart_upload_chunk_size_mib",
        ];
        keys.forEach((key, i) => {
          const req = httpTestingController.expectOne(updateUrl(key));
          if (i === keys.length - 1) req.flush("boom", HTTP_ERROR);
          else req.flush(null);
        });
        expect(msgError).toHaveBeenCalledWith("Failed to save dataset settings.");
      });

      it("resetDatasetSettings POSTs a reset for all four upload settings", () => {
        completeLoad();

        component.resetDatasetSettings();

        [
          "max_number_of_concurrent_uploading_file",
          "single_file_upload_max_size_mib",
          "max_number_of_concurrent_uploading_file_chunks",
          "multipart_upload_chunk_size_mib",
        ].forEach(key => httpTestingController.expectOne(resetUrl(key)).flush(null));
        expect(msgInfo).toHaveBeenCalledWith("Resetting dataset settings...");
      });
    });

    describe("csv (result panel) settings", () => {
      it("saveCsvSettings PUTs the max-columns value and notifies success", () => {
        completeLoad();
        component.csvMaxColumns = 256;

        component.saveCsvSettings();

        const req = httpTestingController.expectOne(updateUrl("csv_parser_max_columns"));
        expect(req.request.method).toBe("PUT");
        expect(req.request.body).toEqual({ value: "256" });
        req.flush(null);

        expect(notifySuccess).toHaveBeenCalledWith("Result panel settings saved.");
      });

      it("saveCsvSettings notifies an error when the request fails", () => {
        completeLoad();

        component.saveCsvSettings();

        httpTestingController.expectOne(updateUrl("csv_parser_max_columns")).flush("boom", HTTP_ERROR);
        expect(notifyError).toHaveBeenCalledWith("Could not save result panel settings.");
      });

      it("resetCsvSettings POSTs a reset and notifies info", () => {
        completeLoad();

        component.resetCsvSettings();

        httpTestingController.expectOne(resetUrl("csv_parser_max_columns")).flush(null);
        expect(notifyInfo).toHaveBeenCalledWith("Resetting result panel settings...");
      });
    });

    describe("onFileChange", () => {
      it("rejects a non-image file with an error and leaves the existing logo untouched", () => {
        completeLoad();
        component.logoData = "data:image/png;base64,EXISTING";
        const event = {
          target: { files: [new File(["x"], "notes.txt", { type: "text/plain" })] },
        } as unknown as Event;

        component.onFileChange("logo", event);

        expect(msgError).toHaveBeenCalledWith("Please upload a valid image file.");
        expect(component.logoData).toBe("data:image/png;base64,EXISTING");
      });

      it("reads a valid image file into the matching branding field", async () => {
        completeLoad();
        const dataUrl = "data:image/png;base64,AAA";
        class FakeFileReader {
          onload: ((e: { target: { result: string } }) => void) | null = null;
          readAsDataURL(): void {
            queueMicrotask(() => this.onload?.({ target: { result: dataUrl } }));
          }
        }
        const realFileReader = globalThis.FileReader;
        (globalThis as any).FileReader = FakeFileReader;

        try {
          const event = {
            target: { files: [new File(["x"], "logo.png", { type: "image/png" })] },
          } as unknown as Event;

          component.onFileChange("mini_logo", event);
          await Promise.resolve();

          expect(component.miniLogoData).toBe(dataUrl);
        } finally {
          (globalThis as any).FileReader = realFileReader;
        }
      });
    });
  });
});
