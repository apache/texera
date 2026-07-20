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

import { JSONSchema7Definition } from "json-schema";
import { TestBed } from "@angular/core/testing";
import { WorkflowCompilingService } from "./workflow-compiling.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { mockPoint, mockScanPredicate } from "../workflow-graph/model/mock-workflow-data";
import { serializePortIdentity } from "../../../common/util/port-identity-serde";
import { commonTestImports, commonTestProviders } from "../../../common/testing/test-utils";
import { firstValueFrom } from "rxjs";
import { CompilationState } from "../../types/workflow-compiling.interface";

describe("WorkflowCompilingService.dropInvalidAttributeValues", () => {
  // A schema shaped like the Aggregate operator after schema propagation has filled in the
  // valid input attribute names ("col_y" is the only attribute available on the new input).
  const aggregateSchema = (): JSONSchema7Definition =>
    ({
      type: "object",
      properties: {
        groupByKeys: {
          type: "array",
          autofill: "attributeNameList",
          items: { type: "string", enum: ["col_y", ""] },
        },
        aggregations: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attribute: { type: "string", autofill: "attributeName", enum: ["col_y"] },
              aggFunction: { type: "string" },
              resultAttribute: { type: "string" },
            },
          },
        },
      },
    }) as unknown as JSONSchema7Definition;

  it("drops list entries and resets single attributes that are no longer valid", () => {
    const properties = {
      groupByKeys: ["col_x", "col_y"],
      aggregations: [{ attribute: "col_x", aggFunction: "sum", resultAttribute: "r" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(true);
    expect(value.groupByKeys).toEqual(["col_y"]);
    expect(value.aggregations[0].attribute).toBe("");
    // non-attribute fields are preserved
    expect(value.aggregations[0].aggFunction).toBe("sum");
    expect(value.aggregations[0].resultAttribute).toBe("r");
    // the input object is never mutated
    expect(properties.groupByKeys).toEqual(["col_x", "col_y"]);
    expect(properties.aggregations[0].attribute).toBe("col_x");
  });

  it("reports no change when all attribute references are valid", () => {
    const properties = {
      groupByKeys: ["col_y"],
      aggregations: [{ attribute: "col_y", aggFunction: "sum", resultAttribute: "r" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });

  it("makes no change when the input schema (enum) is unknown", () => {
    const schemaWithoutEnum: JSONSchema7Definition = {
      type: "object",
      properties: {
        groupByKeys: {
          type: "array",
          autofill: "attributeNameList",
          items: { type: "string" },
        },
        aggregations: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attribute: { type: "string", autofill: "attributeName" },
            },
          },
        },
      },
    } as unknown as JSONSchema7Definition;

    const properties = {
      groupByKeys: ["col_x"],
      aggregations: [{ attribute: "col_x" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(schemaWithoutEnum, properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });

  it("returns the value unchanged for non-object schemas or nullish values", () => {
    // boolean schema (e.g. `additionalProperties: true`)
    expect(WorkflowCompilingService.dropInvalidAttributeValues(true, { a: 1 })).toEqual({
      value: { a: 1 },
      changed: false,
    });
    // null / undefined values are not walked
    expect(WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), null)).toEqual({
      value: null,
      changed: false,
    });
    expect(WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), undefined)).toEqual({
      value: undefined,
      changed: false,
    });
  });

  it("skips schema properties that are absent from the value object", () => {
    // the value is missing both `groupByKeys` and `aggregations` defined in the schema
    const properties = { unrelated: "keep-me" };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });
});

describe("WorkflowCompilingService schema propagation property cleanup", () => {
  let service: WorkflowCompilingService;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [...commonTestImports],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        DynamicSchemaService,
        ValidationWorkflowService,
        WorkflowCompilingService,
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(WorkflowCompilingService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
  });

  it("drops operator property values that the propagated input schema no longer supports", () => {
    const operatorID = mockScanPredicate.operatorID;
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // give the operator a schema with attribute-autofill properties bound to input port 0
    const baseSchema = dynamicSchemaService.getDynamicSchema(operatorID);
    dynamicSchemaService.setDynamicSchema(operatorID, {
      ...baseSchema,
      jsonSchema: {
        type: "object",
        properties: {
          groupByKeys: {
            type: "array",
            autofill: "attributeNameList",
            autofillAttributeOnPort: 0,
            items: { type: "string" },
          },
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      } as any,
    });

    // stale references to "col_x", a column that does not exist on the new input
    workflowActionService.setOperatorProperty(operatorID, { groupByKeys: ["col_x", "col_y"], attribute: "col_x" });

    // the propagated input schema only contains "col_y"
    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue({
      [serializePortIdentity({ id: 0, internal: false })]: [{ attributeName: "col_y", attributeType: "string" }],
    } as any);

    // invoke the private propagation handler directly (normally triggered by a compile response)
    (service as any).applySchemaPropagationResult();

    const cleaned = workflowActionService.getTexeraGraph().getOperator(operatorID).operatorProperties;
    expect(cleaned.groupByKeys).toEqual(["col_y"]);
    expect(cleaned.attribute).toBe("");
  });

  it("leaves valid property values untouched", () => {
    const operatorID = mockScanPredicate.operatorID;
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    const baseSchema = dynamicSchemaService.getDynamicSchema(operatorID);
    dynamicSchemaService.setDynamicSchema(operatorID, {
      ...baseSchema,
      jsonSchema: {
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      } as any,
    });

    workflowActionService.setOperatorProperty(operatorID, { attribute: "col_y" });

    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue({
      [serializePortIdentity({ id: 0, internal: false })]: [{ attributeName: "col_y", attributeType: "string" }],
    } as any);

    const setSpy = vi.spyOn(workflowActionService, "setOperatorProperty");
    (service as any).applySchemaPropagationResult();

    expect(setSpy).not.toHaveBeenCalled();
    expect(workflowActionService.getTexeraGraph().getOperator(operatorID).operatorProperties.attribute).toBe("col_y");
  });
});

describe("WorkflowCompilingService public getters", () => {
  let service: WorkflowCompilingService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [...commonTestImports],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        DynamicSchemaService,
        ValidationWorkflowService,
        WorkflowCompilingService,
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(WorkflowCompilingService);
  });

  // Overwrite the private compilation-state snapshot the getters read from.
  const setState = (info: unknown): void => {
    (service as any).currentCompilationStateInfo = info;
  };

  it("getWorkflowCompilationState returns the current state", () => {
    setState({ state: CompilationState.Succeeded });
    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Succeeded);
  });

  it("getWorkflowCompilationErrors is empty while succeeded or uninitialized", () => {
    setState({ state: CompilationState.Succeeded, operatorErrors: { op1: { message: "x" } } });
    expect(service.getWorkflowCompilationErrors()).toEqual({});

    setState({ state: CompilationState.Uninitialized });
    expect(service.getWorkflowCompilationErrors()).toEqual({});
  });

  it("getWorkflowCompilationErrors surfaces the operator errors when compilation failed", () => {
    const errors = { op1: { message: "boom" } };
    setState({ state: CompilationState.Failed, operatorOutputPortSchemaMap: {}, operatorErrors: errors });
    expect(service.getWorkflowCompilationErrors()).toBe(errors);
  });

  it("getCompilationStateInfoChangedStream replays the latest state", async () => {
    (service as any).compilationStateInfoChangedStream.next(CompilationState.Succeeded);
    expect(await firstValueFrom(service.getCompilationStateInfoChangedStream())).toBe(CompilationState.Succeeded);
  });

  it("getOperatorOutputSchemaMap returns undefined when uninitialized", () => {
    setState({ state: CompilationState.Uninitialized });
    expect(service.getOperatorOutputSchemaMap("op1")).toBeUndefined();
  });

  it("getOperatorOutputSchemaMap returns the operator's output port schema map", () => {
    const opMap = {
      [serializePortIdentity({ id: 0, internal: false })]: [{ attributeName: "a", attributeType: "string" }],
    };
    setState({ state: CompilationState.Succeeded, operatorOutputPortSchemaMap: { op1: opMap } });
    expect(service.getOperatorOutputSchemaMap("op1")).toBe(opMap);
  });

  it("getPortInputSchema looks the port up by its serialized identity", () => {
    const portSchema = [{ attributeName: "a", attributeType: "string" }];
    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue({
      [serializePortIdentity({ id: 0, internal: false })]: portSchema,
    } as any);
    expect(service.getPortInputSchema("op1", 0)).toBe(portSchema);
  });

  it("getPortInputSchema returns undefined when the operator has no input schema map", () => {
    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue(undefined);
    expect(service.getPortInputSchema("op1", 0)).toBeUndefined();
  });

  it("getOperatorInputAttributeType finds the named attribute's type on the input port", () => {
    vi.spyOn(service, "getPortInputSchema").mockReturnValue([
      { attributeName: "a", attributeType: "string" },
      { attributeName: "b", attributeType: "integer" },
    ]);
    expect(service.getOperatorInputAttributeType("op1", 0, "b")).toBe("integer");
    expect(service.getOperatorInputAttributeType("op1", 0, "missing")).toBeUndefined();
  });
});
