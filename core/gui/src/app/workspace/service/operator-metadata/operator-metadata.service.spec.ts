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
import { HttpClient } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { OperatorMetadataService } from "./operator-metadata.service";
import { mockOperatorMetaData } from "./mock-operator-metadata.data";

describe("OperatorMetadataService", () => {
  let service: OperatorMetadataService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [OperatorMetadataService, HttpClient],
    });

    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    service = TestBed.get(OperatorMetadataService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should send http request once", () => {
    service.getOperatorMetadata().subscribe(value => expect(value).toBeTruthy());
    httpTestingController.expectOne(request => request.method === "GET");
  });

  it("should check if operatorType exists correctly", () => {
    service.getOperatorMetadata().subscribe(() => {
      expect(service.operatorTypeExists("ScanSource")).toBeTruthy();
      expect(service.operatorTypeExists("InvalidOperatorType")).toBeFalsy();
    });
    const req = httpTestingController.match(request => request.method === "GET");
    req[0].flush(mockOperatorMetaData);
  });
});
