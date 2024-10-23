import { TestBed } from "@angular/core/testing";
import { WorkflowResultService, OperatorPaginationResultService } from "./workflow-result.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { of, Subject } from "rxjs";
import { SchemaAttribute } from "../dynamic-schema/schema-propagation/schema-propagation.service";

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;

  beforeEach(() => {
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
