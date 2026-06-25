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
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("DriveService", () => {
  let service: DriveService;
  let httpMock: HttpTestingController;
  let ngZone: NgZone;

  const CONNECT_URL = `${AppSettings.getApiEndpoint()}/auth/google/drive/connect`;

  beforeEach(() => {
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
  });

  describe("connect", () => {
    it("fetches the connect URL and opens a popup", () => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      const openSpy = vi.spyOn(window, "open").mockReturnValue(mockPopup);

      service.connect().subscribe();

      const req = httpMock.expectOne(CONNECT_URL);
      expect(req.request.method).toBe("GET");
      req.flush("https://accounts.google.com/o/oauth2/auth?...");

      expect(openSpy).toHaveBeenCalledWith(
        "https://accounts.google.com/o/oauth2/auth?...",
        "gdrive-connect",
        "width=500,height=600"
      );
    });

    it("completes the observable when the popup posts gdrive-connected", fakeAsync(() => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      vi.spyOn(window, "open").mockReturnValue(mockPopup);

      let completed = false;
      service.connect().subscribe({ complete: () => (completed = true) });

      httpMock.expectOne(CONNECT_URL).flush("https://accounts.google.com/...");

      ngZone.run(() => {
        window.dispatchEvent(
          new MessageEvent("message", {
            data: "gdrive-connected",
            origin: window.location.origin,
            source: mockPopup as unknown as MessageEventSource,
          })
        );
      });
      tick();

      expect(completed).toBe(true);
      expect(mockPopup.close).toHaveBeenCalled();
    }));

    it("errors the observable when the popup posts gdrive-error", fakeAsync(() => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      vi.spyOn(window, "open").mockReturnValue(mockPopup);

      let errorMessage = "";
      service.connect().subscribe({ error: (e: unknown) => (errorMessage = (e as Error).message) });

      httpMock.expectOne(CONNECT_URL).flush("https://accounts.google.com/...");

      ngZone.run(() => {
        window.dispatchEvent(
          new MessageEvent("message", {
            data: "gdrive-error",
            origin: window.location.origin,
            source: mockPopup as unknown as MessageEventSource,
          })
        );
      });
      tick();

      expect(errorMessage).toBe("Google Drive connection failed");
    }));

    it("errors immediately when the popup is blocked", fakeAsync(() => {
      vi.spyOn(window, "open").mockReturnValue(null);

      let errorMessage = "";
      service.connect().subscribe({ error: (e: unknown) => (errorMessage = (e as Error).message) });

      httpMock.expectOne(CONNECT_URL).flush("https://accounts.google.com/...");
      tick();

      expect(errorMessage).toBe("Popup blocked. Please allow popups for this site.");
    }));

    it("ignores messages from other origins", fakeAsync(() => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      vi.spyOn(window, "open").mockReturnValue(mockPopup);

      let completed = false;
      let errored = false;
      service.connect().subscribe({
        complete: () => (completed = true),
        error: () => (errored = true),
      });

      httpMock.expectOne(CONNECT_URL).flush("https://accounts.google.com/...");

      window.dispatchEvent(
        new MessageEvent("message", {
          data: "gdrive-connected",
          origin: "https://evil.example.com",
        })
      );
      tick();

      expect(completed).toBe(false);
      expect(errored).toBe(false);
    }));

    it("ignores same-origin messages not sent by the OAuth popup", fakeAsync(() => {
      const mockPopup = { close: vi.fn() } as unknown as Window;
      vi.spyOn(window, "open").mockReturnValue(mockPopup);

      let completed = false;
      let errored = false;
      service.connect().subscribe({
        complete: () => (completed = true),
        error: () => (errored = true),
      });

      httpMock.expectOne(CONNECT_URL).flush("https://accounts.google.com/...");

      // same origin, but source is a different window (not the popup)
      const otherWindow = { close: vi.fn() } as unknown as Window;
      window.dispatchEvent(
        new MessageEvent("message", {
          data: "gdrive-connected",
          origin: window.location.origin,
          source: otherWindow as unknown as MessageEventSource,
        })
      );
      tick();

      expect(completed).toBe(false);
      expect(errored).toBe(false);
    }));
  });
});
