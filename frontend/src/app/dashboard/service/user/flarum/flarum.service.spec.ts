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
import { FlarumService } from "./flarum.service";
import { UserService } from "../../../../common/service/user/user.service";

describe("FlarumService", () => {
  let service: FlarumService;
  let httpMock: HttpTestingController;

  const mockUser = { uid: 42, email: "alice@example.com", googleId: "secret-token" };
  const getCurrentUser = vi.fn();

  beforeEach(() => {
    getCurrentUser.mockReturnValue(mockUser);
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [FlarumService, { provide: UserService, useValue: { getCurrentUser } }],
    });
    service = TestBed.inject(FlarumService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("register", () => {
    it("POSTs a user with a username derived from email local-part + uid", () => {
      service.register().subscribe();

      const req = httpMock.expectOne("forum/api/users");
      expect(req.request.method).toEqual("POST");
      expect(req.request.body.data.attributes).toEqual({
        username: "alice42",
        email: "alice@example.com",
        password: "secret-token",
      });
      expect(req.request.headers.get("Authorization")).toContain("Token ");
      req.flush({});
    });

    it("handles an email that contains multiple '@' by using only the first local-part segment", () => {
      getCurrentUser.mockReturnValue({ uid: 7, email: "weird@name@example.com", googleId: "g" });

      service.register().subscribe();

      const req = httpMock.expectOne("forum/api/users");
      expect(req.request.body.data.attributes.username).toEqual("weird7");
      req.flush({});
    });

    it("propagates HTTP errors to the subscriber", () => {
      const onError = vi.fn();
      service.register().subscribe({ error: onError });

      const req = httpMock.expectOne("forum/api/users");
      req.flush("boom", { status: 500, statusText: "Server Error" });

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0][0].status).toEqual(500);
    });
  });

  describe("auth", () => {
    it("POSTs credentials to the token endpoint", () => {
      service.auth().subscribe();

      const req = httpMock.expectOne("forum/api/token");
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({
        identification: "alice@example.com",
        password: "secret-token",
        remember: "1",
      });
      req.flush({});
    });

    it("propagates HTTP errors to the subscriber", () => {
      const onError = vi.fn();
      service.auth().subscribe({ error: onError });

      const req = httpMock.expectOne("forum/api/token");
      req.flush("boom", { status: 500, statusText: "Server Error" });

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0][0].status).toEqual(500);
    });
  });

  it("throws when no user is logged in", () => {
    getCurrentUser.mockReturnValue(undefined);
    expect(() => service.register()).toThrow();
    expect(() => service.auth()).toThrow();
  });
});
