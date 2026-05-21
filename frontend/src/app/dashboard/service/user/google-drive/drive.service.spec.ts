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

import { TestBed, fakeAsync, tick } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NgZone } from "@angular/core";
import { DriveService } from "./drive.service";
import { AppSettings } from "../../../../common/app-setting";
import { firstValueFrom } from "rxjs";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("DriveService", () => {
  let service: DriveService;
  let httpMock: HttpTestingController;
  let ngZone: NgZone;

  const BASE = `${AppSettings.getApiEndpoint()}/auth/google/drive`;

  beforeEach(() => {
    // Stub gapi so loadPicker() resolves without the real script
    vi.stubGlobal("gapi", {
      load: (_feature: string, cb: () => void) => cb(),
    });

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [DriveService, ...commonTestProviders],
    });

    service = TestBed.inject(DriveService);
    httpMock = TestBed.inject(HttpTestingController);
    ngZone = TestBed.inject(NgZone);
  });

  afterEach(() => {
    httpMock.verify();
    vi.unstubAllGlobals();
  });

  describe("getToken", () => {
    it("calls the token endpoint and returns the response", async () => {
      const promise = firstValueFrom(service.getToken());

      const req = httpMock.expectOne(`${BASE}/token`);
      expect(req.request.method).toBe("GET");
      req.flush({ status: "ok", accessToken: "abc123" });

      const result = await promise;
      expect(result.status).toBe("ok");
      expect(result.accessToken).toBe("abc123");
    });

    it("returns no_refresh_token status when user has not connected Drive", async () => {
      const promise = firstValueFrom(service.getToken());

      httpMock.expectOne(`${BASE}/token`).flush({ status: "no_refresh_token" });

      const result = await promise;
      expect(result.status).toBe("no_refresh_token");
      expect(result.accessToken).toBeUndefined();
    });
  });

  describe("connect", () => {
    it("fetches the connect URL and opens a popup", () => {
      const openSpy = vi.spyOn(window, "open").mockReturnValue(null);

      service.connect();

      const req = httpMock.expectOne(`${BASE}/connect?reauth=false`);
      expect(req.request.method).toBe("GET");
      req.flush("https://accounts.google.com/o/oauth2/auth?...");

      expect(openSpy).toHaveBeenCalledWith(
        "https://accounts.google.com/o/oauth2/auth?...",
        "gdrive-connect",
        "width=500,height=600"
      );
    });

    it("passes reauth=true when reauth flag is set", () => {
      vi.spyOn(window, "open").mockReturnValue(null);

      service.connect(true);

      httpMock.expectOne(`${BASE}/connect?reauth=true`).flush("https://accounts.google.com/...");
    });

    it("emits on onConnected() when the popup posts gdrive-connected", fakeAsync(() => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      vi.spyOn(window, "open").mockReturnValue(mockPopup);

      let connected = false;
      service.onConnected().subscribe(() => {
        connected = true;
      });

      service.connect();
      httpMock.expectOne(`${BASE}/connect?reauth=false`).flush("https://accounts.google.com/...");

      ngZone.run(() => {
        window.dispatchEvent(new MessageEvent("message", { data: "gdrive-connected" }));
      });
      tick();

      expect(connected).toBe(true);
      expect(mockPopup.close).toHaveBeenCalled();
    }));

    it("does not emit on onConnected() for unrelated messages", fakeAsync(() => {
      vi.spyOn(window, "open").mockReturnValue(null);

      let connected = false;
      service.onConnected().subscribe(() => {
        connected = true;
      });

      service.connect();
      httpMock.expectOne(`${BASE}/connect?reauth=false`).flush("https://accounts.google.com/...");

      window.dispatchEvent(new MessageEvent("message", { data: "some-other-message" }));
      tick();

      expect(connected).toBe(false);
    }));
  });

  describe("exportToDrive", () => {
    it("errors the observable when no access token is available", fakeAsync(async () => {
      // Token endpoint returns no_refresh_token so getAccessToken returns null
      const result$ = service.exportToDrive(new Blob(["test"]), "test.json");

      // getToken is called inside getAccessToken
      httpMock.expectOne(`${BASE}/token`).flush({ status: "no_refresh_token" });

      tick();

      let errorMessage = "";
      result$.subscribe({ error: (e: unknown) => (errorMessage = (e as Error).message) });
      tick();

      expect(errorMessage).toBe("Not connected to Google Drive");
    }));
  });
});
