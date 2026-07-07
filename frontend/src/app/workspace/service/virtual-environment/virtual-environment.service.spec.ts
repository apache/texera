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
import { AppSettings } from "../../../common/app-setting";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { UserPveRecord, WorkflowPveService } from "./virtual-environment.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("WorkflowPveService", () => {
  let service: WorkflowPveService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowPveService, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowPveService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("savePve() POSTs to /pve/db with name + packages and returns the new veid", () => {
    const packages = { numpy: "==1.26.0" };
    service.savePve("env-a", packages).subscribe(resp => {
      expect(resp.veid).toBe(42);
    });

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/pve/db`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ name: "env-a", packages });
    req.flush({ veid: 42 });
  });

  it("updateUserPve() PUTs to /pve/db/{veid} with name + packages", () => {
    const packages = { pandas: "" };
    service.updateUserPve(7, "env-b", packages).subscribe(resp => {
      expect(resp.veid).toBe(7);
    });

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/pve/db/7`);
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ name: "env-b", packages });
    req.flush({ veid: 7 });
  });

  it("listUserPves() GETs /pve/db and returns the array of records", () => {
    const records: UserPveRecord[] = [{ veid: 1, name: "env-a", packages: { numpy: "==1.26.0" } }];
    service.listUserPves().subscribe(resp => {
      expect(resp).toEqual(records);
    });

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/pve/db`);
    expect(req.request.method).toBe("GET");
    req.flush(records);
  });

  it("deleteUserPve() DELETEs /pve/db/{veid}", () => {
    service.deleteUserPve(9).subscribe();

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/pve/db/9`);
    expect(req.request.method).toBe("DELETE");
    req.flush(null);
  });
});
