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
import { OPERATOR_METADATA_ENDPOINT, OperatorMetadataService } from "./operator-metadata.service";
import { mockOperatorMetaData } from "./mock-operator-metadata.data";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { AppSettings } from "../../../common/app-setting";
import type { OperatorMetadata } from "../../types/operator-schema.interface";

describe("OperatorMetadataService", () => {
  let service: OperatorMetadataService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [OperatorMetadataService, ...commonTestProviders],
    });

    httpClient = TestBed.inject(HttpClient);
    httpTestingController = TestBed.inject(HttpTestingController);
    service = TestBed.inject(OperatorMetadataService);
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

  const metadataUrl = `${AppSettings.getApiEndpoint()}/${OPERATOR_METADATA_ENDPOINT}`;

  // The service fetches metadata once in its constructor (shareReplay), so a single
  // request is pending after injection; flush it with the fixture.
  const flushMetadata = () =>
    httpTestingController.expectOne(req => req.method === "GET" && req.url === metadataUrl).flush(mockOperatorMetaData);

  it("getOperatorMetadata emits the fetched metadata to subscribers", () => {
    let emitted: OperatorMetadata | undefined;
    service.getOperatorMetadata().subscribe(m => (emitted = m));
    flushMetadata();
    expect(emitted).toEqual(mockOperatorMetaData);
  });

  it("getOperatorSchema returns the schema for a known operator type", () => {
    flushMetadata();
    const schema = service.getOperatorSchema("ScanSource");
    expect(schema.operatorType).toBe("ScanSource");
    expect(schema).toEqual(mockOperatorMetaData.operators.find(op => op.operatorType === "ScanSource"));
  });

  it("getOperatorSchema throws for an unknown operator type", () => {
    flushMetadata();
    expect(() => service.getOperatorSchema("NoSuchOperator")).toThrow(
      "can't find operator schema of type NoSuchOperator"
    );
  });

  it("getOperatorSchema throws when the metadata has not been fetched yet", () => {
    // do not flush: the constructor's request is still pending, so metadata is undefined
    expect(() => service.getOperatorSchema("ScanSource")).toThrow("operator metadata is undefined");
    flushMetadata(); // drain the pending request
  });

  it("operatorTypeExists is true for a fetched type and false for an unknown one", () => {
    flushMetadata();
    expect(service.operatorTypeExists("ScanSource")).toBe(true);
    expect(service.operatorTypeExists("NoSuchOperator")).toBe(false);
  });

  it("operatorTypeExists is false before the metadata request resolves", () => {
    expect(service.operatorTypeExists("ScanSource")).toBe(false);
    flushMetadata();
  });

  it("operatorTypeExists matches the user-friendly name only when that filter is enabled", () => {
    flushMetadata();
    // ScanSource's userFriendlyName is "Source: Scan"
    expect(service.operatorTypeExists("Source: Scan", true)).toBe(true);
    expect(service.operatorTypeExists("Source: Scan", false)).toBe(false);
  });

  it("operatorTypeExists honors case-insensitive matching when requested", () => {
    flushMetadata();
    expect(service.operatorTypeExists("scansource", false, true)).toBe(true);
    expect(service.operatorTypeExists("scansource", false, false)).toBe(false);
  });
});
