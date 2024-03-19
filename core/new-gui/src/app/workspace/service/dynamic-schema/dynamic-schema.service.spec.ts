import { OperatorSchema } from "../../types/operator-schema.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { TestBed, inject } from "@angular/core/testing";
import { marbles } from "rxjs-marbles";

import { DynamicSchemaService } from "./dynamic-schema.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import {
  mockScanPredicate,
  mockPoint,
  mockResultPredicate,
  mockScanResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { mockScanSourceSchema } from "../operator-metadata/mock-operator-metadata.data";
import { environment } from "../../../../environments/environment";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";

describe("DynamicSchemaService", () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        DynamicSchemaService,
      ],
    });
  });

  it("should be created", inject([DynamicSchemaService], (service: DynamicSchemaService) => {
    expect(service).toBeTruthy();
  }));

  it("should update dynamic schema map when operator is added/deleted", () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    expect(dynamicSchemaService.getDynamicSchemaMap().size === 1);

    workflowActionService.deleteOperator(mockScanPredicate.operatorID);
    expect(dynamicSchemaService.getDynamicSchemaMap().size === 0);
  });

  it("should call all initial schema transformers when creating a new dynamic schema", () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);

    const testTransformers = {
      transformer1: (op: OperatorPredicate, schema: OperatorSchema) => schema,
      transformer2: (op: OperatorPredicate, schema: OperatorSchema) => schema,
    };

    const transformer1Spy = spyOn(testTransformers, "transformer1").and.callThrough();
    const transformer2Spy = spyOn(testTransformers, "transformer2").and.callThrough();

    dynamicSchemaService.registerInitialSchemaTransformer(testTransformers.transformer1);
    dynamicSchemaService.registerInitialSchemaTransformer(testTransformers.transformer2);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(transformer1Spy).toHaveBeenCalledTimes(1);
    expect(transformer2Spy).toHaveBeenCalledTimes(1);
  });

  it(
    "should emit event when dynamic schema is changed",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
      const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);

      const newSchema: OperatorSchema = {
        ...mockScanSourceSchema,
        jsonSchema: {
          properties: {
            tableName: {
              type: "string",
            },
          },
          type: "object",
        },
      };

      const trigger = m.hot("-a-c-", {
        a: () => workflowActionService.addOperator(mockScanPredicate, mockPoint),
        c: () => dynamicSchemaService.setDynamicSchema(mockScanPredicate.operatorID, newSchema),
      });

      trigger.subscribe(eventFunc => eventFunc());

      const expected = m.hot("-d-e-", {
        d: { operatorID: mockScanPredicate.operatorID },
        e: { operatorID: mockScanPredicate.operatorID },
      });

      m.expect(dynamicSchemaService.getOperatorDynamicSchemaChangedStream()).toBeObservable(expected);
    })
  );

  it(
    "should not emit event if the updated dynamic schema is same",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
      const dynamicSchemaService: DynamicSchemaService = TestBed.get(DynamicSchemaService);

      const trigger = m.hot("-a-c-", {
        a: () => workflowActionService.addOperator(mockScanPredicate, mockPoint),
        c: () => dynamicSchemaService.setDynamicSchema(mockScanPredicate.operatorID, mockScanSourceSchema),
      });

      trigger.subscribe(eventFunc => eventFunc());

      const expected = m.hot("-d---", {
        d: { operatorID: mockScanPredicate.operatorID },
      });

      m.expect(dynamicSchemaService.getOperatorDynamicSchemaChangedStream()).toBeObservable(expected);
    })
  );
});
