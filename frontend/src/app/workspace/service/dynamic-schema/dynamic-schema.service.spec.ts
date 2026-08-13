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

import { OperatorSchema } from "../../types/operator-schema.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { inject, TestBed } from "@angular/core/testing";
import { marbles } from "rxjs-marbles";

import { DynamicSchemaService } from "./dynamic-schema.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { mockPoint, mockScanPredicate } from "../workflow-graph/model/mock-workflow-data";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { mockScanSourceSchema } from "../operator-metadata/mock-operator-metadata.data";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";

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
        ...commonTestProviders,
      ],
    });
  });

  it("should be created", inject([DynamicSchemaService], (service: DynamicSchemaService) => {
    expect(service).toBeTruthy();
  }));

  it("should update dynamic schema map when operator is added/deleted", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    expect(dynamicSchemaService.getDynamicSchemaMap().size === 1);

    workflowActionService.deleteOperator(mockScanPredicate.operatorID);
    expect(dynamicSchemaService.getDynamicSchemaMap().size === 0);
  });

  it("should call all initial schema transformers when creating a new dynamic schema", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);

    const testTransformers = {
      transformer1: (op: OperatorPredicate, schema: OperatorSchema) => schema,
      transformer2: (op: OperatorPredicate, schema: OperatorSchema) => schema,
    };

    const transformer1Spy = vi.spyOn(testTransformers, "transformer1");
    const transformer2Spy = vi.spyOn(testTransformers, "transformer2");

    dynamicSchemaService.registerInitialSchemaTransformer(testTransformers.transformer1);
    dynamicSchemaService.registerInitialSchemaTransformer(testTransformers.transformer2);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(transformer1Spy).toHaveBeenCalledTimes(1);
    expect(transformer2Spy).toHaveBeenCalledTimes(1);
  });

  it(
    "should emit event when dynamic schema is changed",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);

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
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);

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

describe("DynamicSchemaService.mutateProperty", () => {
  const matchByName = (name: string) => (propertyName: string, _: CustomJSONSchema7) => propertyName === name;
  const markMutated = (_: string, propertyValue: CustomJSONSchema7): CustomJSONSchema7 => ({
    ...propertyValue,
    description: "mutated",
  });

  it("should replace a matched top-level property without mutating the original schema", () => {
    const original = {
      type: "object",
      properties: {
        target: { type: "string", description: "original" },
        other: { type: "number" },
      },
    } as CustomJSONSchema7;

    const result = DynamicSchemaService.mutateProperty(original, matchByName("target"), markMutated);

    // the returned schema has the matched property mutated
    expect((result.properties!.target as CustomJSONSchema7).description).toEqual("mutated");
    // the non-matching property is left untouched
    expect(result.properties!.other).toEqual({ type: "number" });
    // the original schema object is deep cloned and stays unchanged
    expect((original.properties!.target as CustomJSONSchema7).description).toEqual("original");
    expect(result).not.toBe(original);
  });

  it("should recurse into nested object properties to find the matched property", () => {
    const original = {
      type: "object",
      properties: {
        nested: {
          type: "object",
          properties: {
            deepTarget: { type: "string", description: "original" },
          },
        },
      },
    } as CustomJSONSchema7;

    const result = DynamicSchemaService.mutateProperty(original, matchByName("deepTarget"), markMutated);

    const nested = result.properties!.nested as CustomJSONSchema7;
    expect((nested.properties!.deepTarget as CustomJSONSchema7).description).toEqual("mutated");
  });

  it("should recurse into definitions to find the matched property", () => {
    const original = {
      type: "object",
      definitions: {
        target: { type: "string", description: "original" },
      },
    } as CustomJSONSchema7;

    const result = DynamicSchemaService.mutateProperty(original, matchByName("target"), markMutated);

    expect((result.definitions!.target as CustomJSONSchema7).description).toEqual("mutated");
  });

  it("should recurse into array items and single-schema items to find the matched property", () => {
    const original = {
      type: "object",
      properties: {
        listTuple: {
          type: "array",
          items: [
            {
              type: "object",
              properties: {
                target: { type: "string", description: "original" },
              },
            },
          ],
        },
        listSchema: {
          type: "array",
          items: {
            type: "object",
            properties: {
              target: { type: "string", description: "original" },
            },
          },
        },
      },
    } as CustomJSONSchema7;

    const result = DynamicSchemaService.mutateProperty(original, matchByName("target"), markMutated);

    const listTuple = result.properties!.listTuple as CustomJSONSchema7;
    const firstItem = (listTuple.items as CustomJSONSchema7[])[0];
    expect((firstItem.properties!.target as CustomJSONSchema7).description).toEqual("mutated");

    const listSchema = result.properties!.listSchema as CustomJSONSchema7;
    const itemSchema = listSchema.items as CustomJSONSchema7;
    expect((itemSchema.properties!.target as CustomJSONSchema7).description).toEqual("mutated");
  });

  it("should not invoke the mutation function when nothing matches", () => {
    const original = {
      type: "object",
      properties: {
        a: { type: "string" },
        b: { type: "number" },
      },
    } as CustomJSONSchema7;
    const mutationSpy = vi.fn((_: string, value: CustomJSONSchema7) => value);

    const result = DynamicSchemaService.mutateProperty(original, matchByName("missing"), mutationSpy);

    expect(mutationSpy).not.toHaveBeenCalled();
    // with no match the clone is structurally equal to the original
    expect(result).toEqual(original);
    expect(result).not.toBe(original);
  });
});
