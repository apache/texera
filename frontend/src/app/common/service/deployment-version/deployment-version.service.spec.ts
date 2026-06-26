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
import { TestBed, fakeAsync, tick } from "@angular/core/testing";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzNotificationRef, NzNotificationService } from "ng-zorro-antd/notification";
import { Subject } from "rxjs";
import { NotificationService } from "../notification/notification.service";
import { DeploymentVersionService, VERSION_MANIFEST_URL, VERSION_POLL_INTERVAL_MS } from "./deployment-version.service";

describe("DeploymentVersionService", () => {
  let service: DeploymentVersionService;
  let httpMock: HttpTestingController;
  // The real NotificationService, with its single side-effecting method spied.
  let notification: NotificationService;
  let blankSpy: ReturnType<typeof vi.spyOn>;
  // Drives the onClick of the ref returned by the spied blank() call.
  let notificationClick: Subject<MouseEvent>;

  beforeEach(() => {
    notificationClick = new Subject<MouseEvent>();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        NotificationService,
        // ng-zorro's lower-level services are not under test; stub them so the
        // real NotificationService can be constructed and spied on.
        { provide: NzNotificationService, useValue: { blank: vi.fn(), remove: vi.fn() } },
        { provide: NzMessageService, useValue: {} },
      ],
    });
    service = TestBed.inject(DeploymentVersionService);
    httpMock = TestBed.inject(HttpTestingController);
    notification = TestBed.inject(NotificationService);
    blankSpy = vi.spyOn(notification, "blank").mockReturnValue({
      onClick: notificationClick,
      onClose: new Subject(),
      messageId: "test",
    } as unknown as NzNotificationRef);
  });

  afterEach(() => httpMock.verify());

  function takeManifestRequest() {
    return httpMock.expectOne(req => req.url === VERSION_MANIFEST_URL);
  }

  function check(): { value: boolean | undefined } {
    const out: { value: boolean | undefined } = { value: undefined };
    service.checkForUpdate().subscribe(v => (out.value = v));
    return out;
  }

  describe("checkForUpdate (positive)", () => {
    it("reports an update when the deployed build differs from the running one", () => {
      const out = check();
      takeManifestRequest().flush({ buildNumber: "different-build-123" });
      expect(out.value).toBe(true);
    });
  });

  describe("checkForUpdate (no update / negative)", () => {
    it("reports no update when the build matches the running one", () => {
      const out = check();
      // Version.buildNumber is "dev" under test (the non-replaced version.ts).
      takeManifestRequest().flush({ buildNumber: "dev" });
      expect(out.value).toBe(false);
    });
  });

  describe("checkForUpdate (malformed manifest)", () => {
    it("ignores a manifest with no buildNumber field", () => {
      const out = check();
      takeManifestRequest().flush({});
      expect(out.value).toBe(false);
    });

    it("ignores an empty-string buildNumber", () => {
      const out = check();
      takeManifestRequest().flush({ buildNumber: "" });
      expect(out.value).toBe(false);
    });

    it("ignores a non-string buildNumber", () => {
      const out = check();
      takeManifestRequest().flush({ buildNumber: 12345 });
      expect(out.value).toBe(false);
    });

    it("ignores a null response body", () => {
      const out = check();
      takeManifestRequest().flush(null);
      expect(out.value).toBe(false);
    });
  });

  describe("checkForUpdate (transport failures stay silent)", () => {
    it("returns false on a network error", () => {
      const out = check();
      takeManifestRequest().error(new ProgressEvent("error"));
      expect(out.value).toBe(false);
    });

    it("returns false on a 404 (manifest not deployed)", () => {
      const out = check();
      takeManifestRequest().flush("not found", { status: 404, statusText: "Not Found" });
      expect(out.value).toBe(false);
    });

    it("returns false on a 500 server error", () => {
      const out = check();
      takeManifestRequest().flush("boom", { status: 500, statusText: "Server Error" });
      expect(out.value).toBe(false);
    });
  });

  describe("checkForUpdate (request shape)", () => {
    it("requests the manifest with a cache-busting query param so a CDN/browser cache cannot mask a deploy", () => {
      check();
      const req = takeManifestRequest();
      expect(req.request.method).toBe("GET");
      expect(req.request.params.has("t")).toBe(true);
      expect(req.request.params.get("t")).toBeTruthy();
      req.flush({ buildNumber: "dev" });
    });
  });

  describe("promptReload", () => {
    it("shows exactly one sticky, dismissible notification with a refresh message", () => {
      service.promptReload();
      expect(blankSpy).toHaveBeenCalledTimes(1);
      const [title, content, options] = blankSpy.mock.calls[0] as [string, string, { nzDuration?: number }];
      expect(options.nzDuration).toBe(0);
      expect(title.length).toBeGreaterThan(0);
      expect(content.toLowerCase()).toContain("refresh");
    });

    it("reloads the page when the notification is clicked", () => {
      const reloadSpy = vi.spyOn(service, "reload").mockImplementation(() => undefined);
      service.promptReload();
      expect(reloadSpy).not.toHaveBeenCalled();
      notificationClick.next(new MouseEvent("click"));
      expect(reloadSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("startPollingForUpdates", () => {
    it("polls after the interval and prompts once when a new deployment is detected", fakeAsync(() => {
      const sub = service.startPollingForUpdates(1000);
      expect(blankSpy).not.toHaveBeenCalled(); // nothing before the first interval
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(blankSpy).toHaveBeenCalledTimes(1);
      sub.unsubscribe();
    }));

    it("does not prompt while the deployed build is unchanged", fakeAsync(() => {
      const sub = service.startPollingForUpdates(1000);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "dev" });
      expect(blankSpy).not.toHaveBeenCalled();
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "dev" });
      expect(blankSpy).not.toHaveBeenCalled();
      sub.unsubscribe();
    }));

    it("prompts only once and stops polling after an update is found", fakeAsync(() => {
      const sub = service.startPollingForUpdates(1000);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(blankSpy).toHaveBeenCalledTimes(1);
      tick(1000);
      // take(1) completed the stream: no further polling.
      httpMock.expectNone(req => req.url === VERSION_MANIFEST_URL);
      expect(blankSpy).toHaveBeenCalledTimes(1);
      sub.unsubscribe();
    }));

    it("uses a 5 minute default poll interval", () => {
      expect(VERSION_POLL_INTERVAL_MS).toBe(5 * 60 * 1000);
    });

    it("does not poll before the default 5 minute interval elapses", fakeAsync(() => {
      const sub = service.startPollingForUpdates();
      tick(VERSION_POLL_INTERVAL_MS - 1);
      httpMock.expectNone(req => req.url === VERSION_MANIFEST_URL);
      sub.unsubscribe();
    }));

    it("keeps polling and still prompts after a transient request failure", fakeAsync(() => {
      const sub = service.startPollingForUpdates(1000);
      tick(1000);
      // First poll fails at the transport level: the stream must survive it.
      takeManifestRequest().error(new ProgressEvent("error"));
      expect(blankSpy).not.toHaveBeenCalled();
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(blankSpy).toHaveBeenCalledTimes(1);
      sub.unsubscribe();
    }));
  });
});
