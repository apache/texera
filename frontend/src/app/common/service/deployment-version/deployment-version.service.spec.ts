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
import { NzNotificationDataOptions } from "ng-zorro-antd/notification";
import { NotificationService } from "../notification/notification.service";
import { DeploymentVersionService, VERSION_MANIFEST_URL, VERSION_POLL_INTERVAL_MS } from "./deployment-version.service";

// Records blank() calls so the prompt is observable without a spy framework.
class FakeNotificationService {
  public blankCalls: { title: string; content: string; options: NzNotificationDataOptions }[] = [];
  blank(title: string, content: string, options: NzNotificationDataOptions = {}): void {
    this.blankCalls.push({ title, content, options });
  }
}

describe("DeploymentVersionService", () => {
  let service: DeploymentVersionService;
  let httpMock: HttpTestingController;
  let notification: FakeNotificationService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{ provide: NotificationService, useClass: FakeNotificationService }],
    });
    service = TestBed.inject(DeploymentVersionService);
    httpMock = TestBed.inject(HttpTestingController);
    notification = TestBed.inject(NotificationService) as unknown as FakeNotificationService;
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
      expect(notification.blankCalls.length).toBe(1);
      const call = notification.blankCalls[0];
      expect(call.options.nzDuration).toBe(0);
      expect(call.title.length).toBeGreaterThan(0);
      expect(call.content.toLowerCase()).toContain("refresh");
    });
  });

  describe("start (polling)", () => {
    it("polls after the interval and prompts once when a new deployment is detected", fakeAsync(() => {
      const sub = service.start(1000);
      expect(notification.blankCalls.length).toBe(0); // nothing before the first interval
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(notification.blankCalls.length).toBe(1);
      sub.unsubscribe();
    }));

    it("does not prompt while the deployed build is unchanged", fakeAsync(() => {
      const sub = service.start(1000);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "dev" });
      expect(notification.blankCalls.length).toBe(0);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "dev" });
      expect(notification.blankCalls.length).toBe(0);
      sub.unsubscribe();
    }));

    it("prompts only once and stops polling after an update is found", fakeAsync(() => {
      const sub = service.start(1000);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(notification.blankCalls.length).toBe(1);
      tick(1000);
      // take(1) completed the stream: no further polling.
      httpMock.expectNone(req => req.url === VERSION_MANIFEST_URL);
      expect(notification.blankCalls.length).toBe(1);
      sub.unsubscribe();
    }));

    it("uses a 5 minute default poll interval", () => {
      expect(VERSION_POLL_INTERVAL_MS).toBe(5 * 60 * 1000);
    });

    it("does not poll before the default 5 minute interval elapses", fakeAsync(() => {
      const sub = service.start();
      tick(VERSION_POLL_INTERVAL_MS - 1);
      httpMock.expectNone(req => req.url === VERSION_MANIFEST_URL);
      sub.unsubscribe();
    }));

    it("keeps polling and still prompts after a transient request failure", fakeAsync(() => {
      const sub = service.start(1000);
      tick(1000);
      // First poll fails at the transport level: the stream must survive it.
      takeManifestRequest().error(new ProgressEvent("error"));
      expect(notification.blankCalls.length).toBe(0);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(notification.blankCalls.length).toBe(1);
      sub.unsubscribe();
    }));
  });

  describe("start (idempotency)", () => {
    it("returns the in-flight subscription instead of stacking a second poller", fakeAsync(() => {
      const first = service.start(1000);
      const second = service.start(1000);
      expect(second).toBe(first);
      tick(1000);
      // Only one poller is active, so only one manifest request is issued.
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(notification.blankCalls.length).toBe(1);
      first.unsubscribe();
    }));

    it("starts a fresh poller once the previous run has completed", fakeAsync(() => {
      const first = service.start(1000);
      tick(1000);
      // take(1) completes the first run after the update is detected.
      takeManifestRequest().flush({ buildNumber: "new-build" });
      expect(notification.blankCalls.length).toBe(1);

      // A subsequent start() is no longer a no-op: the prior run is closed.
      const second = service.start(1000);
      expect(second).not.toBe(first);
      tick(1000);
      takeManifestRequest().flush({ buildNumber: "another-new-build" });
      expect(notification.blankCalls.length).toBe(2);
      second.unsubscribe();
    }));
  });
});
