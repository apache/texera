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
import { AppSettings } from "../../../app-setting";
import {
  WorkflowComputingUnitManagingService,
  COMPUTING_UNIT_BASE_URL,
  COMPUTING_UNIT_CREATE_URL,
  COMPUTING_UNIT_LIST_URL,
  COMPUTING_UNIT_TYPES_URL,
} from "./workflow-computing-unit-managing.service";

describe("WorkflowComputingUnitManagingService", () => {
  let service: WorkflowComputingUnitManagingService;
  let httpMock: HttpTestingController;

  const api = AppSettings.getApiEndpoint();
  const unitWithResource = (resource: any) => ({ computingUnit: { cuid: 1, name: "u", resource } }) as any;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowComputingUnitManagingService],
    });
    service = TestBed.inject(WorkflowComputingUnitManagingService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("createKubernetesBasedComputingUnit", () => {
    it("POSTs a kubernetes body with an empty uri and parses the resource JSON string", () => {
      let result: any;
      service.createKubernetesBasedComputingUnit("k8s", "2", "4G", "1", "1G", "64M").subscribe(r => (result = r));

      const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_CREATE_URL}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({
        name: "k8s",
        cpuLimit: "2",
        memoryLimit: "4G",
        gpuLimit: "1",
        jvmMemorySize: "1G",
        shmSize: "64M",
        uri: "",
        unitType: "kubernetes",
      });

      req.flush(unitWithResource('{"cpuLimit":"2","memoryLimit":"4G"}'));
      expect(result.computingUnit.resource).toEqual({ cpuLimit: "2", memoryLimit: "4G" });
    });
  });

  describe("createLocalComputingUnit", () => {
    it("POSTs a local body with NaN resource placeholders and the given uri", () => {
      service.createLocalComputingUnit("local", "http://localhost:8080").subscribe();

      const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_CREATE_URL}`);
      expect(req.request.body).toEqual({
        name: "local",
        cpuLimit: "NaN",
        memoryLimit: "NaN",
        gpuLimit: "NaN",
        jvmMemorySize: "NaN",
        shmSize: "NaN",
        uri: "http://localhost:8080",
        unitType: "local",
      });
      req.flush(unitWithResource({ cpuLimit: "NaN" }));
    });
  });

  describe("resource parsing", () => {
    it("falls back to a NaN-filled resource object when the resource JSON is malformed", () => {
      let result: any;
      service.getComputingUnit(5).subscribe(r => (result = r));

      httpMock.expectOne(`${api}/${COMPUTING_UNIT_BASE_URL}/5`).flush(unitWithResource("not-json"));

      expect(result.computingUnit.resource).toEqual({
        cpuLimit: "NaN",
        memoryLimit: "NaN",
        gpuLimit: "NaN",
        jvmMemorySize: "NaN",
        shmSize: "NaN",
        nodeAddresses: [],
      });
    });

    it("leaves an already-parsed resource object untouched", () => {
      let result: any;
      service.getComputingUnit(6).subscribe(r => (result = r));

      const resource = { cpuLimit: "1", nodeAddresses: ["a"] };
      httpMock.expectOne(`${api}/${COMPUTING_UNIT_BASE_URL}/6`).flush(unitWithResource(resource));

      expect(result.computingUnit.resource).toEqual(resource);
    });
  });

  it("terminateComputingUnit() DELETEs the terminate endpoint", () => {
    service.terminateComputingUnit(9).subscribe();
    const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_BASE_URL}/9/terminate`);
    expect(req.request.method).toEqual("DELETE");
    req.flush({});
  });

  it("getComputingUnitLimitOptions() GETs the limits endpoint", () => {
    service.getComputingUnitLimitOptions().subscribe();
    const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_BASE_URL}/limits`);
    expect(req.request.method).toEqual("GET");
    req.flush({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: [] });
  });

  it("getComputingUnitTypes() GETs the types endpoint", () => {
    service.getComputingUnitTypes().subscribe();
    const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_TYPES_URL}`);
    expect(req.request.method).toEqual("GET");
    req.flush({ typeOptions: [] });
  });

  it("listComputingUnits() GETs the list endpoint and parses every unit's resource", () => {
    let result: any[] = [];
    service.listComputingUnits().subscribe(r => (result = r));

    const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_LIST_URL}`);
    expect(req.request.method).toEqual("GET");
    req.flush([unitWithResource('{"cpuLimit":"1"}'), unitWithResource('{"cpuLimit":"2"}')]);

    expect(result.map(u => u.computingUnit.resource)).toEqual([{ cpuLimit: "1" }, { cpuLimit: "2" }]);
  });

  it("renameComputingUnit() PUTs to a URI-encoded rename endpoint", () => {
    service.renameComputingUnit(3, "my unit/name").subscribe();

    const req = httpMock.expectOne(`${api}/${COMPUTING_UNIT_BASE_URL}/3/rename/${encodeURIComponent("my unit/name")}`);
    expect(req.request.method).toEqual("PUT");
    req.flush({});
  });
});
