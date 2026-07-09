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

import { BASE, ShareAccessService } from "./share-access.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { Privilege, ShareAccess } from "../../../type/share-access.interface";

describe("ShareAccessService", () => {
  let service: ShareAccessService;
  let httpMock: HttpTestingController;

  const type: string = "resource";
  const id: number = 42;
  const email: string = "johnDoe@gmail.com";
  const privilege: string = "write";
  const username: string = "johnDaBeast1999";

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [ShareAccessService],
    });
    service = TestBed.inject(ShareAccessService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("grantAccess composes the correct PUT url", () => {
    service.grantAccess(type, id, email, privilege).subscribe();

    const req = httpMock.expectOne(`${BASE}/${type}/grant/${id}/${email}/${privilege}`);

    expect(req.request.method).toBe("PUT");
    req.flush(null);
  });

  it("revokeAccess composes the correct DELETE url", () => {
    service.revokeAccess(type, id, username).subscribe();

    const req = httpMock.expectOne(`${BASE}/${type}/revoke/${id}/${username}`);

    expect(req.request.method).toBe("DELETE");
    req.flush(null);
  });

  it("getOwner should respond with type text", () => {
    let owner: string | undefined;
    service.getOwner(type, id).subscribe(res => {
      owner = res;
    });
    const req = httpMock.expectOne(`${BASE}/${type}/owner/${id}`);

    expect(req.request.method).toBe("GET");
    expect(req.request.responseType).toBe("text");
    req.flush("owner@example.com");
    expect(owner).toBe("owner@example.com");
  });

  it("getAccessList should resolve to an array", () => {
    const mockList: ReadonlyArray<ShareAccess> = [
      { email: "JohnDaBeast1999@example.com", name: "John", privilege: Privilege.READ },
      { email: "alice@example.com", name: "Alice", privilege: Privilege.WRITE },
    ];

    let result: ReadonlyArray<ShareAccess> | undefined;
    service.getAccessList(type, id).subscribe(res => {
      result = res;
    });

    const req = httpMock.expectOne(`${BASE}/${type}/list/${id}`);

    expect(req.request.method).toBe("GET");
    req.flush(mockList);

    expect(result).toEqual(mockList);
  });
});
