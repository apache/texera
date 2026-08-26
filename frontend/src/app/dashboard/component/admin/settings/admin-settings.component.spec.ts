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
import { By } from "@angular/platform-browser";

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

      it("saveLogos PUTs the favicon too when one is set", () => {
        completeLoad();
        component.logoData = "logo.png";
        component.miniLogoData = "mini.png";
        component.faviconData = "fav.ico";

        component.saveLogos();

        const requests = ["logo", "mini_logo", "favicon"].map(key => {
          const req = httpTestingController.expectOne(updateUrl(key));
          expect(req.request.method).toBe("PUT");
          return req;
        });
        expect(requests[2].request.body).toEqual({ value: "fav.ico" });
        requests.forEach(req => req.flush(null));

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

      it("saveDatasetSettings refuses to save before the bulk load completes", () => {
        // The ngOnInit GET is left outstanding on purpose: settingsLoaded is still false.
        const pending = httpTestingController.expectOne(SETTINGS_URL);

        component.saveDatasetSettings();

        httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
        expect(msgError).toHaveBeenCalledWith("Settings have not loaded; refresh before saving.");
        pending.flush({});
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

    // The issue labels these lines as `resetTabs`; they are actually the two computed
    // getters that sit just below it, so the tests target the getters.
    describe("computed part-size properties", () => {
      it("partsAtMax is 0 unless both the total size and the chunk size are set", () => {
        completeLoad();

        component.maxFileSizeMiB = 0;
        component.chunkSizeMiB = 8;
        expect(component.partsAtMax).toBe(0);

        component.maxFileSizeMiB = 100;
        component.chunkSizeMiB = 0;
        expect(component.partsAtMax).toBe(0);

        component.maxFileSizeMiB = 100;
        component.chunkSizeMiB = 8;
        expect(component.partsAtMax).toBe(13);
      });

      it("requiredMinPartSizeMiB falls back to the floor when no total size is set", () => {
        completeLoad();

        component.maxFileSizeMiB = 0;
        expect(component.requiredMinPartSizeMiB).toBe(component.MIN_PART_SIZE_MiB);

        // Above the floor the parts limit takes over: 10,000 parts must cover the total.
        component.maxFileSizeMiB = component.MIN_PART_SIZE_MiB * component.MAX_TOTAL_PARTS * 2;
        expect(component.requiredMinPartSizeMiB).toBe(component.MIN_PART_SIZE_MiB * 2);
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

      it("saveCsvSettings refuses to save before the bulk load completes", () => {
        const pending = httpTestingController.expectOne(SETTINGS_URL);

        component.saveCsvSettings();

        httpTestingController.expectNone((req: { method: string }) => req.method === "PUT");
        expect(msgError).toHaveBeenCalledWith("Settings have not loaded; refresh before saving.");
        pending.flush({});
      });

      it("saveCsvSettings notifies an error when the request fails", () => {
        completeLoad();

        component.saveCsvSettings();

        httpTestingController.expectOne(updateUrl("csv_parser_max_columns")).flush("boom", HTTP_ERROR);
        expect(notifyError).toHaveBeenCalledWith("Could not save result panel settings.");
      });

      it("resetCsvSettings notifies an error when the reset fails", () => {
        completeLoad();

        component.resetCsvSettings();

        httpTestingController.expectOne(resetUrl("csv_parser_max_columns")).flush("boom", HTTP_ERROR);
        expect(notifyError).toHaveBeenCalledWith("Could not reset result panel settings.");
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

      const dataUrl = "data:image/png;base64,AAA";

      /**
       * Uploads a valid image through a FileReader double that resolves to `result`.
       * The double keeps the read off jsdom's real async, so the assertion only has to
       * wait for the microtask the fake itself queues.
       */
      async function uploadImageResolvingTo(type: "logo" | "mini_logo" | "favicon", result: unknown): Promise<void> {
        class FakeFileReader {
          onload: ((e: { target: { result: unknown } }) => void) | null = null;
          readAsDataURL(): void {
            queueMicrotask(() => this.onload?.({ target: { result } }));
          }
        }
        const realFileReader = globalThis.FileReader;
        (globalThis as any).FileReader = FakeFileReader;
        try {
          const event = {
            target: { files: [new File(["x"], "logo.png", { type: "image/png" })] },
          } as unknown as Event;
          component.onFileChange(type, event);
          await Promise.resolve();
        } finally {
          (globalThis as any).FileReader = realFileReader;
        }
      }

      it("reads a valid image file into the matching branding field", async () => {
        completeLoad();

        await uploadImageResolvingTo("mini_logo", dataUrl);

        expect(component.miniLogoData).toBe(dataUrl);
      });

      it("routes a logo upload to logoData", async () => {
        completeLoad();

        await uploadImageResolvingTo("logo", dataUrl);

        expect(component.logoData).toBe(dataUrl);
        expect(component.miniLogoData).toBeNull();
        expect(component.faviconData).toBeNull();
      });

      it("routes a favicon upload to faviconData", async () => {
        completeLoad();

        await uploadImageResolvingTo("favicon", dataUrl);

        expect(component.faviconData).toBe(dataUrl);
        expect(component.logoData).toBeNull();
        expect(component.miniLogoData).toBeNull();
      });

      it("stores null when the reader yields something other than a string", async () => {
        completeLoad();
        component.logoData = "data:image/png;base64,EXISTING";

        // readAsDataURL always yields a string, but the handler guards the type anyway;
        // an ArrayBuffer result takes the other arm of that ternary.
        await uploadImageResolvingTo("logo", new ArrayBuffer(8));

        expect(component.logoData).toBeNull();
      });
    });
  });
});
/**
 * The settings form is four near-identical Save/Reset cards, three near-identical upload blocks and
 * twelve switches whose keys include two confusable singular/plural pairs (workflow_enabled vs
 * workflows_enabled, dataset_enabled vs datasets_enabled). Cross-wiring from copy-paste is the
 * realistic defect here, and the suite above never renders an interaction, so none of it was pinned.
 */
describe("AdminSettingsComponent wiring", () => {
  let component: AdminSettingsComponent;
  let fixture: ComponentFixture<AdminSettingsComponent>;
  let http: HttpTestingController;

  /** Sidebar switches in the order the template renders them. */
  const SWITCH_KEYS = [
    "hub_enabled",
    "home_enabled",
    "workflow_enabled",
    "dataset_enabled",
    "your_work_enabled",
    "projects_enabled",
    "workflows_enabled",
    "datasets_enabled",
    "compute_enabled",
    "quota_enabled",
    "forum_enabled",
    "about_enabled",
  ] as const;

  /** Numeric inputs in the order the template renders them. */
  const NUMBER_FIELDS = [
    "maxConcurrentFiles",
    "maxFileSizeMiB",
    "maxConcurrentChunks",
    "chunkSizeMiB",
    "csvMaxColumns",
  ] as const;

  beforeEach(async () => {
    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: [AdminSettingsComponent, HttpClientTestingModule, NzCardModule],
    }).compileComponents();

    http = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(AdminSettingsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    // ngOnInit loads the settings; answer it so the form starts from a known state.
    http.expectOne("/api/config/settings").flush({});
    fixture.detectChanges();
  });

  afterEach(() => {
    // nz-icon may lazily fetch its SVG assets over HTTP; drain those so verify()
    // only asserts on requests this suite actually expects.
    http.match(req => req.url.startsWith("assets/")).forEach(req => req.flush(""));
    http.verify();
    vi.restoreAllMocks();
  });

  function host(): HTMLElement {
    return fixture.nativeElement as HTMLElement;
  }

  function switches() {
    return fixture.debugElement.queryAll(By.css("nz-switch"));
  }

  function numberInputs() {
    return fixture.debugElement.queryAll(By.css("nz-input-number"));
  }

  /** Buttons carrying the given trimmed label, in document order. */
  function buttonsLabelled(label: string): HTMLButtonElement[] {
    return Array.from(host().querySelectorAll<HTMLButtonElement>("button")).filter(
      b => b.textContent?.trim() === label
    );
  }

  describe("sidebar switches", () => {
    it("gives every switch its own setting, in template order", () => {
      // The point of the test: workflow_enabled/workflows_enabled and dataset_enabled/
      // datasets_enabled are one character apart, and a swap between them is invisible on screen.
      expect(switches().length).toBe(SWITCH_KEYS.length);

      SWITCH_KEYS.forEach((key, i) => {
        SWITCH_KEYS.forEach(k => ((component.sidebarTabs as any)[k] = false));

        switches()[i].triggerEventHandler("ngModelChange", true);

        const flipped = SWITCH_KEYS.filter(k => (component.sidebarTabs as any)[k]);
        expect(flipped).toEqual([key]);
      });
    });

    it("locks the Hub children until Hub itself is on", () => {
      const hubChildren = [1, 2, 3];

      component.sidebarTabs.hub_enabled = false;
      fixture.detectChanges();
      hubChildren.forEach(i => expect(switches()[i].componentInstance.nzDisabled).toBe(true));

      component.sidebarTabs.hub_enabled = true;
      fixture.detectChanges();
      hubChildren.forEach(i => expect(switches()[i].componentInstance.nzDisabled).toBe(false));
    });

    it("locks the Your Work children until Your Work itself is on", () => {
      const yourWorkChildren = [5, 6, 7, 8, 9, 10];

      component.sidebarTabs.your_work_enabled = false;
      fixture.detectChanges();
      yourWorkChildren.forEach(i => expect(switches()[i].componentInstance.nzDisabled).toBe(true));

      component.sidebarTabs.your_work_enabled = true;
      fixture.detectChanges();
      yourWorkChildren.forEach(i => expect(switches()[i].componentInstance.nzDisabled).toBe(false));
    });

    it("never locks the two section switches or About", () => {
      // Locking a section behind itself would make it impossible to switch back on.
      component.sidebarTabs.hub_enabled = false;
      component.sidebarTabs.your_work_enabled = false;
      fixture.detectChanges();

      [0, 4, 11].forEach(i => expect(switches()[i].componentInstance.nzDisabled).toBeFalsy());
    });
  });

  describe("numeric settings", () => {
    it("gives every number input its own field, in template order", () => {
      expect(numberInputs().length).toBe(NUMBER_FIELDS.length);

      NUMBER_FIELDS.forEach((field, i) => {
        numberInputs()[i].triggerEventHandler("ngModelChange", 42 + i);

        expect((component as any)[field]).toBe(42 + i);
      });

      // Distinct values, so a shared target would have collapsed them.
      const values = NUMBER_FIELDS.map(f => (component as any)[f]);
      expect(new Set(values).size).toBe(NUMBER_FIELDS.length);
    });
  });

  describe("card buttons", () => {
    it("routes each card's Save to that card's own handler", () => {
      const spies = {
        saveLogos: vi.spyOn(component, "saveLogos").mockImplementation(() => {}),
        saveTabs: vi.spyOn(component, "saveTabs").mockImplementation(() => {}),
        saveDatasetSettings: vi.spyOn(component, "saveDatasetSettings").mockImplementation(() => {}),
        saveCsvSettings: vi.spyOn(component, "saveCsvSettings").mockImplementation(() => {}),
      };
      const saves = buttonsLabelled("Save");
      expect(saves.length).toBe(4);

      saves.forEach(b => b.click());

      expect(spies.saveLogos).toHaveBeenCalledTimes(1);
      expect(spies.saveTabs).toHaveBeenCalledTimes(1);
      expect(spies.saveDatasetSettings).toHaveBeenCalledTimes(1);
      expect(spies.saveCsvSettings).toHaveBeenCalledTimes(1);
    });

    it("routes each card's Reset to that card's own handler", () => {
      const spies = {
        resetBranding: vi.spyOn(component, "resetBranding").mockImplementation(() => {}),
        resetTabs: vi.spyOn(component, "resetTabs").mockImplementation(() => {}),
        resetDatasetSettings: vi.spyOn(component, "resetDatasetSettings").mockImplementation(() => {}),
        resetCsvSettings: vi.spyOn(component, "resetCsvSettings").mockImplementation(() => {}),
      };
      const resets = buttonsLabelled("Reset");
      expect(resets.length).toBe(4);

      resets.forEach(b => b.click());

      expect(spies.resetBranding).toHaveBeenCalledTimes(1);
      expect(spies.resetTabs).toHaveBeenCalledTimes(1);
      expect(spies.resetDatasetSettings).toHaveBeenCalledTimes(1);
      expect(spies.resetCsvSettings).toHaveBeenCalledTimes(1);
    });
  });

  describe("branding uploads", () => {
    const PICKERS = [
      { label: "Choose a Logo", key: "logo" },
      { label: "Choose a Mini Logo", key: "mini_logo" },
      { label: "Choose a Favicon", key: "favicon" },
    ] as const;

    function fileInputs(): HTMLInputElement[] {
      return Array.from(host().querySelectorAll<HTMLInputElement>('input[type="file"]'));
    }

    it("points each picker at its own hidden input", () => {
      // Three identical blocks; a copy-pasted template reference would open the wrong picker.
      const inputs = fileInputs();
      expect(inputs.length).toBe(PICKERS.length);
      const clicks = inputs.map(i => vi.spyOn(i, "click").mockImplementation(() => {}));

      PICKERS.forEach((picker, i) => {
        buttonsLabelled(picker.label)[0].click();

        expect(clicks[i]).toHaveBeenCalledTimes(1);
        clicks.forEach((c, j) => j !== i && expect(c).toHaveBeenCalledTimes(j < i ? 1 : 0));
      });
    });

    it("tags each hidden input's change with its own setting key", () => {
      const spy = vi.spyOn(component, "onFileChange").mockImplementation(() => {});

      fileInputs().forEach(input => input.dispatchEvent(new Event("change")));

      expect(spy.mock.calls.map(c => c[0])).toEqual(PICKERS.map(p => p.key));
    });

    it("shows no preview until something has been chosen", () => {
      // Without the *ngIf on each preview, a card renders a broken image on first load.
      expect(host().querySelectorAll("img.preview-img").length).toBe(0);
    });

    it("previews only the images that have been chosen", () => {
      component.logoData = "data:image/png;base64,LOGO";
      fixture.detectChanges();

      const previews = Array.from(host().querySelectorAll<HTMLImageElement>("img.preview-img"));
      expect(previews.length).toBe(1);
      expect(previews[0].alt).toBe("Logo Preview");
      expect(previews[0].getAttribute("src")).toBe("data:image/png;base64,LOGO");
    });

    it("previews each image beside its own label", () => {
      component.logoData = "LOGO";
      component.miniLogoData = "MINI";
      component.faviconData = "FAV";
      fixture.detectChanges();

      const previews = Array.from(host().querySelectorAll<HTMLImageElement>("img.preview-img"));
      expect(previews.map(i => i.getAttribute("src"))).toEqual(["LOGO", "MINI", "FAV"]);
    });
  });
});
