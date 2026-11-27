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
import { HttpErrorResponse } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { ORCID_STATE_KEY, OrcidAuthService, OrcidConfig } from "./orcid-auth.service";

const CONFIG_URL = "api/auth/orcid/config";

describe("OrcidAuthService", () => {
  let service: OrcidAuthService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [OrcidAuthService],
    });
    httpTestingController = TestBed.inject(HttpTestingController);
    service = TestBed.inject(OrcidAuthService);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("reads the ORCID config with one GET and passes it through unchanged", () => {
    const config: OrcidConfig = {
      clientId: "APP-123",
      authorizeUrl: "https://sandbox.orcid.org/oauth/authorize",
      redirectUri: "https://texera.example.org/user/orcid/callback",
    };
    let received: OrcidConfig | undefined;
    service.getConfig().subscribe(value => (received = value));

    const req = httpTestingController.expectOne(CONFIG_URL);
    expect(req.request.method).toEqual("GET");
    expect(req.request.body).toBeNull();
    req.flush(config);

    // The redirect URI must reach the login page byte-for-byte: ORCID compares it
    // with the one the backend's token exchange sends.
    expect(received).toEqual(config);
  });

  it("does not request anything until subscribed", () => {
    service.getConfig();
    httpTestingController.expectNone(CONFIG_URL);
  });

  it("hands an HTTP error, such as ORCID login being disabled, to the subscriber", () => {
    let status: number | undefined;
    service.getConfig().subscribe({ error: (err: unknown) => (status = (err as HttpErrorResponse).status) });

    httpTestingController.expectOne(CONFIG_URL).flush("not configured", { status: 404, statusText: "Not Found" });

    expect(status).toEqual(404);
  });

  it("keeps the CSRF state under a stable sessionStorage key", () => {
    // The login page writes and the callback page reads this key; renaming it
    // would break a round trip that is already in flight.
    expect(ORCID_STATE_KEY).toEqual("orcid_state");
  });
});
