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
import { AdminSettingsService } from "./admin-settings.service";
import { TestBed } from "@angular/core/testing";

describe("AdminSettingsService", () => {
  let httpMock: HttpTestingController;
  let service: AdminSettingsService;

  const BASE_URL = "/api/admin/settings";
  const EXAMPLE_SETTING = "multipart_upload_chunk_size_mib";
  const EXAMPLE_SETTING_VALUE = "4";

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AdminSettingsService],
    });
    httpMock = TestBed.inject(HttpTestingController);
    service = TestBed.inject(AdminSettingsService);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("an empty getSetting body should map to null", () => {
    let result: string | null | undefined;
    service.getSetting(EXAMPLE_SETTING).subscribe(res => {
      result = res;
    });

    const req = httpMock.expectOne(`${BASE_URL}/${EXAMPLE_SETTING}`);

    req.flush(null);

    expect(req.request.method).toBe("GET");
    expect(result).toBeNull();
  });

  it("a getSetting body should map to its value", () => {
    let result: string | undefined;
    service.getSetting(EXAMPLE_SETTING).subscribe(res => {
      result = res;
    });

    const req = httpMock.expectOne(`${BASE_URL}/${EXAMPLE_SETTING}`);

    req.flush({ key: EXAMPLE_SETTING, value: EXAMPLE_SETTING_VALUE });
    expect(req.request.method).toBe("GET");
    expect(result).toBe(EXAMPLE_SETTING_VALUE);
  });

  it("updateSetting issues a PUT request with value and credentials", () => {
    service.updateSetting(EXAMPLE_SETTING, EXAMPLE_SETTING_VALUE).subscribe();

    const req = httpMock.expectOne(`${BASE_URL}/${EXAMPLE_SETTING}`);

    req.flush(null);
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ value: EXAMPLE_SETTING_VALUE });
    expect(req.request.withCredentials).toBe(true);
  });

  it("resetSetting issues a POST request with an empty body", () => {
    service.resetSetting(EXAMPLE_SETTING).subscribe();

    const req = httpMock.expectOne(`${BASE_URL}/reset/${EXAMPLE_SETTING}`);

    req.flush(null);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({});
  });
});
