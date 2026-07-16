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
import { PublicProjectService, USER_BASE_URL } from "./public-project.service";

describe("PublicProjectService", () => {
  let service: PublicProjectService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [PublicProjectService],
    });
    service = TestBed.inject(PublicProjectService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("getType() GETs the per-project type endpoint as text", () => {
    let type: string | undefined;
    service.getType(9).subscribe(t => (type = t));

    const req = httpMock.expectOne(`${USER_BASE_URL}/type/9`);
    expect(req.request.method).toEqual("GET");
    expect(req.request.responseType).toEqual("text");
    req.flush("Public");

    expect(type).toEqual("Public");
  });

  it("makePublic() PUTs to the public endpoint with a null body", () => {
    service.makePublic(3).subscribe();

    const req = httpMock.expectOne(`${USER_BASE_URL}/public/3`);
    expect(req.request.method).toEqual("PUT");
    expect(req.request.body).toBeNull();
    req.flush(null);
  });

  it("makePrivate() PUTs to the private endpoint with a null body", () => {
    service.makePrivate(4).subscribe();

    const req = httpMock.expectOne(`${USER_BASE_URL}/private/4`);
    expect(req.request.method).toEqual("PUT");
    expect(req.request.body).toBeNull();
    req.flush(null);
  });

  it("getPublicProjects() GETs the list endpoint", () => {
    const projects = [{ pid: 1 } as any];
    let result: readonly any[] | undefined;

    service.getPublicProjects().subscribe(r => (result = r));

    const req = httpMock.expectOne(`${USER_BASE_URL}/list`);
    expect(req.request.method).toEqual("GET");
    req.flush(projects);

    expect(result).toEqual(projects);
  });

  it("addPublicProjects() PUTs the checked ids as the request body", () => {
    service.addPublicProjects([1, 2, 3]).subscribe();

    const req = httpMock.expectOne(`${USER_BASE_URL}/add`);
    expect(req.request.method).toEqual("PUT");
    expect(req.request.body).toEqual([1, 2, 3]);
    req.flush(null);
  });

  it("addPublicProjects() still issues a request when given an empty list", () => {
    service.addPublicProjects([]).subscribe();

    const req = httpMock.expectOne(`${USER_BASE_URL}/add`);
    expect(req.request.body).toEqual([]);
    req.flush(null);
  });
});
