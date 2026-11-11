/*
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
import { firstValueFrom } from "rxjs";
import { vi } from "vitest";

import { AppleAuthService } from "./apple-auth.service";
import { AppSettings } from "../../app-setting";

const SDK_URL = "https://appleid.cdn-apple.com/appleauth/static/jsapi/appleid/1/en_US/appleid.auth.js";

/**
 * This is where Apple's real surface is exercised — the injected script tag and the `AppleID`
 * global. The login component's spec stubs this service wholesale, so keeping the SDK's mechanics
 * here is what stops those concerns leaking into a component test.
 */
describe("AppleAuthService", () => {
  let service: AppleAuthService;
  let httpTestingController: HttpTestingController;
  let appleId: { auth: { init: ReturnType<typeof vi.fn>; signIn: ReturnType<typeof vi.fn> } };

  const expectedUrl = `${AppSettings.getApiEndpoint()}/auth/apple/clientid`;
  const sdkScripts = () => Array.from(document.querySelectorAll<HTMLScriptElement>(`script[src="${SDK_URL}"]`));

  /**
   * Answer the client-id request the service makes before it touches the SDK, then let the awaiting
   * continuation inside `configure()` run so the script tag exists by the time the caller looks.
   */
  const flushClientId = async (clientId = "apple.client.id") => {
    httpTestingController
      .expectOne(r => r.method === "GET" && r.url === expectedUrl && r.responseType === "text")
      .flush(clientId);
    for (let i = 0; i < 5; i++) {
      await Promise.resolve();
    }
  };

  const setGlobal = (value: unknown) => {
    (globalThis as unknown as { AppleID?: unknown }).AppleID = value;
  };

  beforeEach(() => {
    appleId = {
      auth: { init: vi.fn(), signIn: vi.fn().mockResolvedValue({ authorization: { id_token: "apple-id-token" } }) },
    };
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AppleAuthService],
    });
    service = TestBed.inject(AppleAuthService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    delete (globalThis as unknown as { AppleID?: unknown }).AppleID;
    // jsdom shares one document across a file, so an appended script would leak between tests.
    sdkScripts().forEach(script => script.remove());
    httpTestingController.verify();
  });

  it("issues a GET to the client-id endpoint and emits the returned id", async () => {
    const clientId$ = firstValueFrom(service.getClientId());

    await flushClientId("apple-client-id-abc");

    expect(await clientId$).toBe("apple-client-id-abc");
  });

  describe("signIn", () => {
    it("configures Apple with the fetched client id, the page origin and a popup", async () => {
      setGlobal(appleId);

      const ready = service.signIn();
      await flushClientId("apple.services.id");
      await ready;

      // usePopup is load-bearing twice over: it keeps the identity token in the page, and it stops
      // Apple posting a form to redirectURI, which would navigate away from the SPA.
      expect(appleId.auth.init).toHaveBeenCalledWith({
        clientId: "apple.services.id",
        scope: "email",
        redirectURI: window.location.origin,
        usePopup: true,
      });
    });

    it("appends Apple's SDK script when the global is not already present", async () => {
      const ready = service.signIn();
      await flushClientId();

      const script = sdkScripts()[0];
      expect(script).toBeTruthy();
      expect(script.async).toBe(true);

      setGlobal(appleId);
      script.onload!(new Event("load"));
      await ready;

      expect(appleId.auth.init).toHaveBeenCalled();
    });

    it("skips the script when Apple's SDK is already on the page", async () => {
      setGlobal(appleId);

      const ready = service.signIn();
      await flushClientId();
      await ready;

      expect(sdkScripts()).toHaveLength(0);
    });

    it("neither refetches the client id nor re-appends the script on a second call", async () => {
      setGlobal(appleId);

      const first = service.signIn();
      await flushClientId();
      await first;

      await service.signIn();

      // expectNone would pass trivially; verify() in afterEach catches an unanswered second request.
      expect(appleId.auth.init).toHaveBeenCalledTimes(1);
      expect(sdkScripts()).toHaveLength(0);
    });

    it("lets a later call retry after the script fails to load", async () => {
      const failing = service.signIn();
      await flushClientId();
      sdkScripts()[0].onerror!(new Event("error"));

      await expect(failing).rejects.toThrow("Failed to load Apple's sign-in SDK");

      // The memo must have been cleared, or a transient CDN blip would be cached forever.
      setGlobal(appleId);
      const retry = service.signIn();
      await flushClientId();
      await retry;

      expect(appleId.auth.init).toHaveBeenCalled();
    });
  });

  it("resolves with the identity token from Apple's popup", async () => {
    setGlobal(appleId);

    const token = service.signIn();
    await flushClientId();

    expect(await token).toBe("apple-id-token");
  });

  // Apple rejects with the same shape for a dismissal and a real failure, and neither detail is
  // worth surfacing, so both resolve as "no token" rather than throwing.
  it("resolves undefined when Apple's popup is dismissed or fails", async () => {
    setGlobal(appleId);
    appleId.auth.signIn.mockRejectedValue({ error: "popup_closed_by_user" });

    const token = service.signIn();
    await flushClientId();

    expect(await token).toBeUndefined();
  });

  it("resolves undefined when Apple returns no identity token", async () => {
    setGlobal(appleId);
    appleId.auth.signIn.mockResolvedValue({ authorization: {} });

    const token = service.signIn();
    await flushClientId();

    expect(await token).toBeUndefined();
  });
});
