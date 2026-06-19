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
import { OperatorPaginationResultService, WorkflowResultService } from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { firstValueFrom, of, Subject } from "rxjs";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import type { Mocked } from "vitest";
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

  it("clearResults() drops cached operator results", () => {
    (service as any).operatorResultServices.set("op1", {});
    (service as any).paginatedResultServices.set("op2", {});
    expect(service.hasAnyResult("op1")).toBe(true);
    expect(service.hasAnyResult("op2")).toBe(true);

    service.clearResults();

    expect(service.hasAnyResult("op1")).toBe(false);
    expect(service.hasAnyResult("op2")).toBe(false);
  });

  it("clearResults() resets table stats to empty for subscribers", () => {
    const pairs: [unknown, unknown][] = [];
    service.getResultTableStats().subscribe(p => pairs.push(p));
    (service as any).resultTableStats.next({ op1: {} });
    service.clearResults();
    expect(pairs[pairs.length - 1][1]).toEqual({});
  });

  it("clearResults() emits on the cleared stream so the UI tears down stale frames", () => {
    let clearedCount = 0;
    service.getResultClearedStream().subscribe(() => clearedCount++);
    service.clearResults();
    expect(clearedCount).toBe(1);
  });
});

describe("OperatorPaginationResultService", () => {
  let service: OperatorPaginationResultService;
  let mockWorkflowWebsocketService: Mocked<WorkflowWebsocketService>;

  beforeEach(() => {
    mockWorkflowWebsocketService = {
      subscribeToEvent: vi.fn(),
      send: vi.fn(),
    } as unknown as Mocked<WorkflowWebsocketService>;
    mockWorkflowWebsocketService.subscribeToEvent.mockReturnValue(new Subject());

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
    it("should return the correct tuple and schema", async () => {
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

      vi.spyOn(service, "selectPage").mockReturnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      const result = await firstValueFrom(service.selectTuple(1, 3));
      expect(result.tuple).toEqual({ id: 2, name: "Bob" });
      expect(result.schema).toEqual(testSchema);
    });

    it("should handle out-of-bounds tuple index", async () => {
      const testSchema: SchemaAttribute[] = [
        { attributeName: "id", attributeType: "integer" },
        { attributeName: "name", attributeType: "string" },
      ];
      service["schema"] = testSchema;

      const testTable = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ];

      vi.spyOn(service, "selectPage").mockReturnValue(
        of({
          requestID: "test",
          operatorID: "testOperator",
          pageIndex: 1,
          table: testTable,
          schema: testSchema,
        })
      );

      const result = await firstValueFrom(service.selectTuple(2, 3));
      expect(result.tuple).toBeUndefined();
      expect(result.schema).toEqual(testSchema);
    });
  });
});
