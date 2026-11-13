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
import { AdminSettingsService } from "./admin-settings.service";

describe("AdminSettingsService", () => {
  let service: AdminSettingsService;
  let httpTestingController: HttpTestingController;

  const PUBLIC_URL = "/api/config/settings/public";

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AdminSettingsService],
    });

    httpTestingController = TestBed.inject(HttpTestingController);
    service = TestBed.inject(AdminSettingsService);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("shares one public-settings request across per-key subscribers", () => {
    let logo: string | null | undefined;
    let hubEnabled: string | null | undefined;

    service.getPublicSetting("logo").subscribe(value => (logo = value));
    service.getPublicSetting("hub_enabled").subscribe(value => (hubEnabled = value));

    const req = httpTestingController.expectOne(PUBLIC_URL);
    req.flush({ logo: "custom.png", hub_enabled: "true" });

    expect(logo).toEqual("custom.png");
    expect(hubEnabled).toEqual("true");
  });

  it("emits null for a key absent from the public payload", () => {
    let value: string | null | undefined;
    service.getPublicSetting("no-such-key").subscribe(v => (value = v));

    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "custom.png" });

    expect(value).toBeNull();
  });

  it("drops the cached observable on error so the next read retries", () => {
    let firstError = false;
    service.getPublicSetting("logo").subscribe({ error: () => (firstError = true) });
    httpTestingController.expectOne(PUBLIC_URL).flush("boom", { status: 500, statusText: "Server Error" });
    expect(firstError).toBeTruthy();

    let logo: string | null | undefined;
    service.getPublicSetting("logo").subscribe(value => (logo = value));
    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "recovered.png" });
    expect(logo).toEqual("recovered.png");
  });

  it("invalidates the public-settings cache after a save", () => {
    service.getPublicSetting("logo").subscribe();
    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "old.png" });

    service.updateSetting("logo", "new.png").subscribe();
    const put = httpTestingController.expectOne("/api/config/settings/logo");
    expect(put.request.method).toEqual("PUT");
    expect(put.request.body).toEqual({ value: "new.png" });
    expect(put.request.withCredentials).toBe(true);
    put.flush(null);

    let logo: string | null | undefined;
    service.getPublicSetting("logo").subscribe(value => (logo = value));
    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "new.png" });
    expect(logo).toEqual("new.png");
  });

  it("invalidates the public-settings cache after a reset", () => {
    service.getPublicSetting("logo").subscribe();
    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "old.png" });

    service.resetSetting("logo").subscribe();
    const post = httpTestingController.expectOne("/api/config/settings/reset/logo");
    expect(post.request.method).toEqual("POST");
    expect(post.request.body).toEqual({});
    post.flush(null);

    service.getPublicSetting("logo").subscribe();
    httpTestingController.expectOne(PUBLIC_URL).flush({ logo: "default.png" });
  });

  it("reads every stored setting through the bulk management endpoint", () => {
    let settings: Record<string, string> | undefined;
    service.getAllSettings().subscribe(value => (settings = value));

    const req = httpTestingController.expectOne("/api/config/settings");
    expect(req.request.method).toEqual("GET");
    req.flush({ logo: "a.png", csv_parser_max_columns: "4096" });

    expect(settings).toEqual({ logo: "a.png", csv_parser_max_columns: "4096" });
  });
});
