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
import {
  AdminUserService,
  USER_LIST_URL,
  USER_UPDATE_URL,
  USER_ADD_URL,
  USER_CREATED_FILES,
  USER_CREATED_DATASETS,
  USER_CREATED_WORKFLOWS,
  USER_ACCESS_FILES,
  USER_ACCESS_WORKFLOWS,
  USER_QUOTA_SIZE,
  USER_DELETE_EXECUTION_COLLECTION,
} from "./admin-user.service";
import { Role } from "../../../../common/type/user";

describe("AdminUserService", () => {
  let service: AdminUserService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AdminUserService],
    });
    service = TestBed.inject(AdminUserService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("getUserList() GETs the list endpoint", () => {
    const users = [{ uid: 1 } as any];
    let result: readonly any[] | undefined;
    service.getUserList().subscribe(r => (result = r));

    const req = httpMock.expectOne(USER_LIST_URL);
    expect(req.request.method).toEqual("GET");
    req.flush(users);

    expect(result).toEqual(users);
  });

  it("updateUser() PUTs the full user record", () => {
    service.updateUser(1, "Alice", "alice@x.com", Role.ADMIN, "vip").subscribe();

    const req = httpMock.expectOne(USER_UPDATE_URL);
    expect(req.request.method).toEqual("PUT");
    expect(req.request.body).toEqual({
      uid: 1,
      name: "Alice",
      email: "alice@x.com",
      role: Role.ADMIN,
      comment: "vip",
    });
    req.flush(null);
  });

  it("addUser() POSTs an empty body to the trailing-slash add endpoint", () => {
    service.addUser().subscribe();

    const req = httpMock.expectOne(`${USER_ADD_URL}/`);
    expect(req.request.method).toEqual("POST");
    expect(req.request.body).toEqual({});
    req.flush({});
  });

  it("getUploadedFiles() sends the uid as a user_id query param", () => {
    service.getUploadedFiles(9).subscribe();

    const req = httpMock.expectOne(r => r.url === USER_CREATED_FILES);
    expect(req.request.params.get("user_id")).toEqual("9");
    req.flush([]);
  });

  it("getCreatedDatasets() GETs without a query param", () => {
    service.getCreatedDatasets(9).subscribe();

    const req = httpMock.expectOne(USER_CREATED_DATASETS);
    expect(req.request.method).toEqual("GET");
    expect(req.request.params.keys().length).toEqual(0);
    req.flush([]);
  });

  it("getCreatedWorkflows() sends the uid as a user_id query param", () => {
    service.getCreatedWorkflows(3).subscribe();

    const req = httpMock.expectOne(r => r.url === USER_CREATED_WORKFLOWS);
    expect(req.request.params.get("user_id")).toEqual("3");
    req.flush([]);
  });

  it("getAccessFiles() sends the uid as a user_id query param", () => {
    service.getAccessFiles(3).subscribe();

    const req = httpMock.expectOne(r => r.url === USER_ACCESS_FILES);
    expect(req.request.params.get("user_id")).toEqual("3");
    req.flush([]);
  });

  it("getAccessWorkflows() sends the uid as a user_id query param", () => {
    service.getAccessWorkflows(3).subscribe();

    const req = httpMock.expectOne(r => r.url === USER_ACCESS_WORKFLOWS);
    expect(req.request.params.get("user_id")).toEqual("3");
    req.flush([]);
  });

  it("getExecutionQuota() sends the uid as a user_id query param", () => {
    service.getExecutionQuota(3).subscribe();

    const req = httpMock.expectOne(r => r.url === USER_QUOTA_SIZE);
    expect(req.request.params.get("user_id")).toEqual("3");
    req.flush([]);
  });

  it("deleteExecutionCollection() DELETEs the per-execution endpoint", () => {
    service.deleteExecutionCollection(77).subscribe();

    const req = httpMock.expectOne(`${USER_DELETE_EXECUTION_COLLECTION}/77`);
    expect(req.request.method).toEqual("DELETE");
    req.flush(null);
  });
});
