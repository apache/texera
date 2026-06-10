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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom } from "rxjs";

import { GoogleAuthService } from "./google-auth.service";
import { AppSettings } from "../../app-setting";

describe("GoogleAuthService", () => {
  let service: GoogleAuthService;
  let httpTestingController: HttpTestingController;
  const expectedUrl = `${AppSettings.getApiEndpoint()}/auth/google/clientid`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [GoogleAuthService],
    });
    service = TestBed.inject(GoogleAuthService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("issues a GET to the client-id endpoint and emits the returned id", async () => {
    const clientId$ = firstValueFrom(service.getClientId());

    const req = httpTestingController.expectOne(
      r => r.method === "GET" && r.url === expectedUrl && r.responseType === "text"
    );
    req.flush("google-client-id-abc");

    expect(await clientId$).toBe("google-client-id-abc");
  });
});
