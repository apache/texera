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
    expect(service.getDict()).toEqual({});
  });

  describe("fetchAll", () => {
    it("issues a GET to the config endpoint and replaces the local dictionary", () => {
      const observable = service.fetchAll();

      const req = httpMock.expectOne(endpoint);
      expect(req.request.method).toEqual("GET");

      const payload: UserConfig = { foo: "1", bar: "2" };
      req.flush(payload);

      observable.subscribe(value => expect(value).toEqual(payload));
      expect(service.getDict()).toEqual(payload);
    });

    it("notifies dictionaryChanged subscribers when the dictionary is replaced", () => {
      const next = vi.fn();
      const sub = (service as any).dictionaryChangedSubject.subscribe(next);

      service.fetchAll();
      httpMock.expectOne(endpoint).flush({ k: "v" });

      expect(next).toHaveBeenCalledTimes(1);
      sub.unsubscribe();
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

      expect(service.getDict()).toEqual({ alpha: "one" });
    });

    it("notifies dictionaryChanged subscribers only when the value actually changes", () => {
      const next = vi.fn();
      const sub = (service as any).dictionaryChangedSubject.subscribe(next);

      service.fetchKey("alpha");
      httpMock.expectOne(`${endpoint}/alpha`).flush("one");
      expect(next).toHaveBeenCalledTimes(1);

      service.fetchKey("alpha");
      httpMock.expectOne(`${endpoint}/alpha`).flush("one");
      expect(next).toHaveBeenCalledTimes(1);

      sub.unsubscribe();
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
      expect(service.getDict()).toEqual({ alpha: "one" });
    });

    it("does not refire dictionaryChanged when setting the same value twice", () => {
      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      const next = vi.fn();
      const sub = (service as any).dictionaryChangedSubject.subscribe(next);

      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);

      expect(next).not.toHaveBeenCalled();
      sub.unsubscribe();
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

      expect(service.getDict()).toEqual({});
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

  describe("user-change reactions", () => {
    it("re-fetches the dictionary when a logged-in user is emitted on userChanged", () => {
      stubUserService.userChangeSubject.next(MOCK_USER);

      const req = httpMock.expectOne(endpoint);
      expect(req.request.method).toEqual("GET");
      req.flush({ rehydrated: "yes" });

      expect(service.getDict()).toEqual({ rehydrated: "yes" });
    });

    it("clears the local dictionary when the user logs out", () => {
      service.set("alpha", "one");
      httpMock.expectOne(`${endpoint}/alpha`).flush(null);
      expect(service.getDict()).toEqual({ alpha: "one" });

      stubUserService.user = undefined;
      stubUserService.userChangeSubject.next(undefined);

      expect(service.getDict()).toEqual({});
      httpMock.expectNone(endpoint);
    });
  });
});
