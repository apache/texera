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
import { WorkflowResultService, OperatorPaginationResultService } from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { of, Subject } from "rxjs";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowResultService, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowResultService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});

describe("OperatorPaginationResultService", () => {
  let service: OperatorPaginationResultService;
  let mockWorkflowWebsocketService: jasmine.SpyObj<WorkflowWebsocketService>;

  beforeEach(() => {
    mockWorkflowWebsocketService = jasmine.createSpyObj("WorkflowWebsocketService", ["subscribeToEvent", "send"]);
    mockWorkflowWebsocketService.subscribeToEvent.and.returnValue(new Subject());

    service = new OperatorPaginationResultService("testOperator", mockWorkflowWebsocketService);
  });

  describe("getSchema", () => {
    it("should return the current schema", () => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      expect(service.getSchema()).toEqual(testSchema);
    });
  });

  describe("selectTuple", () => {
    it("should return the correct tuple and schema", done => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      const testTable = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ];

      spyOn(service, "selectPage").and.returnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      service.selectTuple(1, 3).subscribe(result => {
        expect(result.tuple).toEqual({ id: 2, name: "Bob" });
        expect(result.schema).toEqual(testSchema);
        done();
      });
    });

    it("should handle out-of-bounds tuple index", done => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      const testTable = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ];

      spyOn(service, "selectPage").and.returnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      service.selectTuple(2, 3).subscribe(result => {
        expect(result.tuple).toBeUndefined();
        expect(result.schema).toEqual(testSchema);
        done();
      });
    });
  });
});
