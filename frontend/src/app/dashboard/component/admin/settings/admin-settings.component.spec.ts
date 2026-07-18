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
});
