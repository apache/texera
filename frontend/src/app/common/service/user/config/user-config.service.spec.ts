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
import { TestBed } from "@angular/core/testing";
import { AppSettings } from "src/app/common/app-setting";
import { UserConfigService, UserConfig } from "./user-config.service";
import { UserService } from "../user.service";
import { StubUserService, MOCK_USER } from "../stub-user.service";
import { Subject } from "rxjs";
import { User } from "../../../type/user";

describe("UserConfigService", () => {
  let service: UserConfigService;
  let stubUserService: StubUserService;
  let httpMock: HttpTestingController;

  const endpoint = `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{ provide: UserService, useClass: StubUserService }, UserConfigService],
    });

    stubUserService = TestBed.inject(UserService) as unknown as StubUserService;
    service = TestBed.inject(UserConfigService);
    httpMock = TestBed.inject(HttpTestingController);

    // The constructor calls fetchAll() because StubUserService starts logged in.
    // Flush the request with an empty dictionary so each test starts from a clean slate.
    httpMock.expectOne(endpoint).flush({});
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("starts with an empty local dictionary after the initial fetch", () => {
    service.delete("anyKey");
    httpMock.expectNone(`${endpoint}/anyKey`);
  });

  describe("fetchAll", () => {
    it("issues a GET to the config endpoint and replaces the local dictionary", () => {
      service.set("prior", "zero");
      httpMock.expectOne(`${endpoint}/prior`).flush(null);

      const observable = service.fetchAll();

      const req = httpMock.expectOne(endpoint);
      expect(req.request.method).toEqual("GET");

      const payload: UserConfig = { foo: "1", bar: "2" };
      req.flush(payload);

      observable.subscribe(value => expect(value).toEqual(payload));

      service.delete("foo");
      httpMock.expectOne(`${endpoint}/foo`).flush(null);

      service.delete("bar");
      httpMock.expectOne(`${endpoint}/bar`).flush(null);

      service.delete("prior");
      httpMock.expectNone(`${endpoint}/prior`);

      service.delete("baz");
      httpMock.expectNone(`${endpoint}/baz`);
    });

    it("throws when the user is not logged in", () => {
      stubUserService.user = undefined;
      expect(() => service.fetchAll()).toThrowError("user not logged in");
    });
  });

  describe("fetchKey", () => {
    it("issues a GET to the per-key endpoint and merges the value into the local dict", () => {
      const observable = service.fetchKey("alpha");

      const req = httpMock.expectOne(`${endpoint}/alpha`);
      expect(req.request.method).toEqual("GET");
      expect(req.request.responseType).toEqual("text");

      req.flush("one");
      observable.subscribe(value => expect(value).toEqual("one"));

      service.delete("alpha");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      service.delete("beta");
      httpMock.expectNone(`${endpoint}/beta`);
    });

    it("throws when the user is not logged in", () => {
      stubUserService.user = undefined;
      expect(() => service.fetchKey("alpha")).toThrowError("user not logged in");
    });

    it("throws when given an empty key", () => {
      expect(() => service.fetchKey("   ")).toThrowError(/key cannot be empty/);
    });
  });

  describe("set", () => {
    it("issues a PUT with the value as the body and updates the local dict", () => {
      service.set("alpha", "one");

      const req = httpMock.expectOne(`${endpoint}/alpha`);
      expect(req.request.method).toEqual("PUT");
      expect(req.request.body).toEqual("one");

      req.flush(null);
      service.delete("alpha");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      service.delete("beta");
      httpMock.expectNone(`${endpoint}/beta`);
    });

    it("issues a PUT and keeps the entry when setting the same value twice", () => {
      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      service.set("alpha", "one");
      const req = httpMock.expectOne(`${endpoint}/alpha`);
      expect(req.request.method).toEqual("PUT");
      expect(req.request.body).toEqual("one");
      req.flush(null);

      service.delete("alpha");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);
    });

    it("throws when the user is not logged in", () => {
      stubUserService.user = undefined;
      expect(() => service.set("alpha", "one")).toThrowError("user not logged in");
    });

    it("throws when given an empty key", () => {
      expect(() => service.set(" ", "one")).toThrowError(/key cannot be empty/);
    });
  });

  describe("delete", () => {
    beforeEach(() => {
      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);
    });

    it("issues a DELETE to the per-key endpoint and removes the entry from the local dict", () => {
      service.delete("alpha");

      const req = httpMock.expectOne(`${endpoint}/alpha`);
      expect(req.request.method).toEqual("DELETE");
      req.flush(null);

      service.delete("alpha");
      httpMock.expectNone(`${endpoint}/alpha`);
    });

    it("is a no-op (no HTTP request) when the key is not present in the local dict", () => {
      service.delete("missing");
      httpMock.expectNone(`${endpoint}/missing`);
    });

    it("throws when the user is not logged in", () => {
      stubUserService.user = undefined;
      expect(() => service.delete("alpha")).toThrowError("user not logged in");
    });

    it("throws when given an empty key", () => {
      expect(() => service.delete("")).toThrowError(/key cannot be empty/);
    });
  });

  // updateEntry is private and validates its own arguments rather than trusting
  // its callers. Both guards below are unreachable through the public API --
  // fetchKey/set/delete reject a blank key before they ever tap into
  // updateEntry, and delete() returns early when the key is absent -- so the
  // contract updateEntry keeps for itself is pinned here by calling it directly.
  describe("updateEntry (private, called directly)", () => {
    it("rejects a blank key even though no public caller can supply one", () => {
      expect(() => (service as any).updateEntry("   ", "v")).toThrowError(/key cannot be empty/);
      // The guard runs before any mutation, so nothing was written under a
      // trimmed or raw form of the blank key.
      service.delete("v");
      httpMock.expectNone(`${endpoint}/v`);
    });

    it("leaves the dictionary untouched when told to delete a key that is absent", () => {
      expect(() => (service as any).updateEntry("absent", undefined)).not.toThrow();
      service.delete("absent");
      httpMock.expectNone(`${endpoint}/absent`);
    });
  });

  describe("user-change reactions", () => {
    it("re-fetches the dictionary when a logged-in user is emitted on userChanged", () => {
      stubUserService.userChangeSubject.next(MOCK_USER);

      const req = httpMock.expectOne(endpoint);
      expect(req.request.method).toEqual("GET");
      req.flush({ rehydrated: "yes" });

      service.delete("rehydrated");
      httpMock.expectOne(`${endpoint}/rehydrated`).flush(null);

      service.delete("other");
      httpMock.expectNone(`${endpoint}/other`);
    });

    it("clears the local dictionary when the user logs out", () => {
      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      // User logs out: local dictionary is reset to {}
      stubUserService.user = undefined;
      stubUserService.userChangeSubject.next(undefined);

      httpMock.expectNone(endpoint);

      // Restore authentication without triggering a re-fetch so we test the post-logout cache state directly
      stubUserService.user = MOCK_USER;

      // Prove that alpha was evicted on logout: delete("alpha") sends no request
      service.delete("alpha");
      httpMock.expectNone(`${endpoint}/alpha`);

      // Prove that the authenticated user CAN delete an active key, showing that the absence of
      // the request above was due to cache eviction, not authentication failure
      service.set("beta", "two");
      httpMock.expectOne(`${endpoint}/beta`).flush(null);
      service.delete("beta");
      httpMock.expectOne(`${endpoint}/beta`).flush(null);
    });
  });
});

// StubUserService hard-codes `this.user = MOCK_USER` in its constructor, so the
// suite above can only ever bring the service up while logged in, and the
// constructor's `if (this.userService.isLogin())` guard is only ever seen from
// its true side. This second TestBed constructs the service logged out instead.
describe("UserConfigService (constructed while logged out)", () => {
  let httpMock: HttpTestingController;
  const endpoint = `${AppSettings.getApiEndpoint()}/${UserConfigService.USER_DICTIONARY_ENDPOINT}`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: { isLogin: () => false, userChanged: () => new Subject<User>() } },
        UserConfigService,
      ],
    });
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("does not fetch the dictionary on construction when nobody is logged in", () => {
    // The load-bearing assertion is that construction does not THROW. fetchAll()
    // re-checks the login state and raises Error("user not logged in") before it
    // issues any request, so a constructor that skipped the guard takes the whole
    // service down at injection time; it never reaches a point where a request
    // could be observed. (The cached dictionary is deliberately not asserted here: it is
    // fixed at {} by its field initialiser and would hold either way.)
    expect(() => TestBed.inject(UserConfigService)).not.toThrow();

    httpMock.expectNone(endpoint);
  });
});
