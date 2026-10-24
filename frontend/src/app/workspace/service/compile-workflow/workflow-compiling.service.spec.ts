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
import { fakeAsync, TestBed, tick } from "@angular/core/testing";
import { HttpTestingController, TestRequest } from "@angular/common/http/testing";
import {
  WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS,
  WORKFLOW_COMPILATION_ENDPOINT,
  WorkflowCompilingService,
} from "./workflow-compiling.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import {
  mockPoint,
  mockScanPredicate,
  mockSentimentPredicate,
  mockMultiInputOutputPredicate,
  mockResultPredicate,
  mockScanResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { AppSettings } from "../../../common/app-setting";
import { serializePortIdentity } from "../../../common/util/port-identity-serde";
import { commonTestImports, commonTestProviders } from "../../../common/testing/test-utils";
import { firstValueFrom } from "rxjs";
import { CompilationState } from "../../types/workflow-compiling.interface";
import { OperatorSchema } from "../../types/operator-schema.interface";

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

  it("getOperatorInputSchemaMap returns undefined when uninitialized", () => {
    setState({ state: CompilationState.Uninitialized });
    expect(service.getOperatorInputSchemaMap("op1")).toBeUndefined();
  });

  it("getOperatorInputSchemaMap returns undefined when there is no output schema map", () => {
    setState({ state: CompilationState.Succeeded, operatorOutputPortSchemaMap: undefined });
    expect(service.getOperatorInputSchemaMap("op1")).toBeUndefined();
  });

  it("getOperatorInputSchemaMap returns undefined for an operator with no input links", () => {
    const workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    setState({ state: CompilationState.Succeeded, operatorOutputPortSchemaMap: {} });
    expect(service.getOperatorInputSchemaMap(mockScanPredicate.operatorID)).toBeUndefined();
  });

  it("getOperatorInputSchemaMap resolves the input port schema from the upstream operator's output schema", () => {
    const workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    const port0 = serializePortIdentity({ id: 0, internal: false });
    const scanOutputSchema = [{ attributeName: "a", attributeType: "string" }];
    setState({
      state: CompilationState.Succeeded,
      operatorOutputPortSchemaMap: { [mockScanPredicate.operatorID]: { [port0]: scanOutputSchema } },
    });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockResultPredicate.operatorID);
    expect(inputSchemaMap?.[port0]).toEqual(scanOutputSchema);
  });
});

describe("WorkflowCompilingService.setOperatorInputAttrs / restoreOperatorInputAttrs", () => {
  const port0 = serializePortIdentity({ id: 0, internal: false });

  // Builds an OperatorSchema wrapping the given json schema. Only jsonSchema matters for these pure transforms;
  // additionalMetadata is included only to satisfy the OperatorSchema type.
  const makeOperatorSchema = (jsonSchema: any): OperatorSchema =>
    ({
      operatorType: "TestOp",
      operatorVersion: "1",
      jsonSchema,
      additionalMetadata: {
        userFriendlyName: "Test Op",
        operatorGroupName: "Test",
        inputPorts: [{ displayName: "input" }],
        outputPorts: [{ displayName: "output" }],
      },
    }) as unknown as OperatorSchema;

  // input port 0 exposes two attributes.
  const inputPortSchemaMap = {
    [port0]: [
      { attributeName: "col_a", attributeType: "string" },
      { attributeName: "col_b", attributeType: "integer" },
    ],
  } as any;

  describe("setOperatorInputAttrs", () => {
    it("returns the original operator schema unchanged when the input schema map is undefined", () => {
      const schema = makeOperatorSchema({ type: "object", properties: {} });
      expect(WorkflowCompilingService.setOperatorInputAttrs(schema, undefined)).toBe(schema);
    });

    it("returns the original operator schema unchanged when the input schema map is empty", () => {
      const schema = makeOperatorSchema({ type: "object", properties: {} });
      expect(WorkflowCompilingService.setOperatorInputAttrs(schema, {})).toBe(schema);
    });

    it("injects the input attribute names as an enum on an optional attributeName property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      const result = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      const prop = (result.jsonSchema.properties as any).attribute;
      // optional (not in `required`) properties append "" so the empty selection passes validation.
      expect(prop.enum).toEqual(["col_a", "col_b", ""]);
      expect(prop.type).toBe("string");
      expect(prop.uniqueItems).toBe(true);
    });

    it('appends the property\'s string default instead of "" for optional properties', () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0, default: "col_a" },
        },
      });

      const result = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      expect((result.jsonSchema.properties as any).attribute.enum).toEqual(["col_a", "col_b", "col_a"]);
    });

    it("throws when the property's default value is not a string", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0, default: 42 },
        },
      });

      expect(() => WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap)).toThrow(
        "default value must be a string"
      );
    });

    it("does not append an empty option for required properties", () => {
      const schema = makeOperatorSchema({
        type: "object",
        required: ["attribute"],
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toEqual(["col_a", "col_b"]);
    });

    it("includes additionalEnumValue before the optional empty option", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: {
            type: "string",
            autofill: "attributeName",
            autofillAttributeOnPort: 0,
            additionalEnumValue: "*",
          },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toEqual(["col_a", "col_b", "*", ""]);
    });

    it("yields an undefined enum when autofillAttributeOnPort is missing", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName" },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toBeUndefined();
    });

    it("yields an undefined enum when the referenced input port has no schema", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          // port 5 is not present in inputPortSchemaMap.
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 5 },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toBeUndefined();
    });

    it("injects the enum into the items of an attributeNameList property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attributes: {
            type: "array",
            autofill: "attributeNameList",
            autofillAttributeOnPort: 0,
            items: { type: "string" },
          },
        },
      });

      const prop = (
        WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any
      ).attributes;
      expect(prop.type).toBe("array");
      expect(prop.uniqueItems).toBe(true);
      expect(prop.items.type).toBe("string");
      expect(prop.items.enum).toEqual(["col_a", "col_b", ""]);
    });
  });

  describe("restoreOperatorInputAttrs", () => {
    it("clears the injected enum/uniqueItems from an attributeName property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: {
            type: "string",
            autofill: "attributeName",
            autofillAttributeOnPort: 0,
            enum: ["col_a", "col_b", ""],
            uniqueItems: true,
          },
        },
      });

      const prop = (WorkflowCompilingService.restoreOperatorInputAttrs(schema).jsonSchema.properties as any).attribute;
      expect(prop.enum).toBeUndefined();
      expect(prop.uniqueItems).toBeUndefined();
      expect(prop.type).toBe("string");
    });

    it("clears the injected enum from the items of an attributeNameList property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attributes: {
            type: "array",
            autofill: "attributeNameList",
            autofillAttributeOnPort: 0,
            uniqueItems: true,
            items: { type: "string", enum: ["col_a", "col_b", ""] },
          },
        },
      });

      const prop = (WorkflowCompilingService.restoreOperatorInputAttrs(schema).jsonSchema.properties as any).attributes;
      expect(prop.type).toBe("array");
      expect(prop.uniqueItems).toBeUndefined();
      expect(prop.items.enum).toBeUndefined();
      expect(prop.items.type).toBe("string");
    });

    it("round-trips: restore reverses the enums that setOperatorInputAttrs injected", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      const propagated = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      expect((propagated.jsonSchema.properties as any).attribute.enum).toEqual(["col_a", "col_b", ""]);

      const restored = WorkflowCompilingService.restoreOperatorInputAttrs(propagated);
      expect((restored.jsonSchema.properties as any).attribute.enum).toBeUndefined();
    });
  });
});

/** The real service graph the compiling service is wired into; no operator metadata or compile backend is contacted. */
const configureCompilingTestBed = (): void => {
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
};

const port = (id: number): string => serializePortIdentity({ id, internal: false });

describe("WorkflowCompilingService compile pipeline", () => {
  let service: WorkflowCompilingService;
  let workflowActionService: WorkflowActionService;
  let httpTestingController: HttpTestingController;

  const compileUrl = `${AppSettings.getApiEndpoint()}/${WORKFLOW_COMPILATION_ENDPOINT}`;
  const scanOutputSchema = [{ attributeName: "col_a", attributeType: "string" }];

  beforeEach(() => {
    configureCompilingTestBed();
    // injecting the service is what subscribes its constructor pipeline to the graph streams
    service = TestBed.inject(WorkflowCompilingService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  /**
   * Builds a valid Scan -> ViewResults workflow and lets the debounce window elapse,
   * which is what makes the constructor pipeline issue exactly one compile request.
   */
  const buildWorkflowAndCompile = (): TestRequest => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);
    // ScanSource requires `tableName`; without it the operator is filtered out of the valid graph
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "twitter" });
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    return httpTestingController.expectOne(compileUrl);
  };

  it("debounces graph edits into a single POST carrying the logical plan", fakeAsync(() => {
    const request = buildWorkflowAndCompile();

    expect(request.request.method).toBe("POST");
    expect(request.request.headers.get("Content-Type")).toBe("application/json");

    const body = JSON.parse(request.request.body);
    expect(body.operators.map((operator: any) => operator.operatorID).sort()).toEqual(
      [mockScanPredicate.operatorID, mockResultPredicate.operatorID].sort()
    );
    expect(body.links.length).toBe(1);
    expect(body.opsToReuseResult).toEqual([]);
    expect(body.opsToViewResult).toEqual([]);

    request.flush({ physicalPlan: { operators: [], links: [] }, operatorOutputSchemas: {}, operatorErrors: {} });
  }));

  it("records the physical plan and output schemas when the response carries a physical plan", fakeAsync(() => {
    const request = buildWorkflowAndCompile();
    const physicalPlan = { operators: [{ id: "physical-1" }], links: [] };

    request.flush({
      physicalPlan,
      operatorOutputSchemas: { [mockScanPredicate.operatorID]: { [port(0)]: scanOutputSchema } },
      operatorErrors: { [mockScanPredicate.operatorID]: { message: "ignored while succeeded" } },
    });

    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Succeeded);
    expect((service as any).currentCompilationStateInfo.physicalPlan).toEqual(physicalPlan);
    expect(service.getOperatorOutputSchemaMap(mockScanPredicate.operatorID)).toEqual({ [port(0)]: scanOutputSchema });
    // a succeeded compilation never surfaces operator errors, even when the response carries some
    expect(service.getWorkflowCompilationErrors()).toEqual({});
  }));

  it("records the operator errors when the response carries no physical plan", fakeAsync(() => {
    const request = buildWorkflowAndCompile();
    const operatorErrors = { [mockResultPredicate.operatorID]: { message: "compilation blew up" } };

    request.flush({
      operatorOutputSchemas: { [mockScanPredicate.operatorID]: { [port(0)]: scanOutputSchema } },
      operatorErrors,
    });

    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Failed);
    expect(service.getWorkflowCompilationErrors()).toEqual(operatorErrors);
    // the output schemas of the partially-compiled workflow are still kept
    expect(service.getOperatorOutputSchemaMap(mockScanPredicate.operatorID)).toEqual({ [port(0)]: scanOutputSchema });
  }));

  it("pushes each compile outcome onto the compilation-state stream in order", fakeAsync(() => {
    const states: CompilationState[] = [];
    const subscription = service.getCompilationStateInfoChangedStream().subscribe(state => states.push(state));

    buildWorkflowAndCompile().flush({
      physicalPlan: { operators: [], links: [] },
      operatorOutputSchemas: {},
      operatorErrors: {},
    });

    // a second edit compiles again, this time without a physical plan
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "reddit" });
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    httpTestingController.expectOne(compileUrl).flush({ operatorOutputSchemas: {}, operatorErrors: {} });

    expect(states).toEqual([CompilationState.Succeeded, CompilationState.Failed]);
    subscription.unsubscribe();
  }));

  it("swallows a failing compile request and keeps compiling afterwards", fakeAsync(() => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    buildWorkflowAndCompile().flush("boom", { status: 500, statusText: "Server Error" });

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toBe("compile workflow API returns error");
    // the error is turned into EMPTY, so the state is left untouched...
    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Uninitialized);

    // ...and the outer subscription survives, so the next edit still compiles
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "reddit" });
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    httpTestingController
      .expectOne(compileUrl)
      .flush({ physicalPlan: { operators: [], links: [] }, operatorOutputSchemas: {}, operatorErrors: {} });

    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Succeeded);
    warn.mockRestore();
  }));
});

describe("WorkflowCompilingService.applySchemaPropagationResult without a propagated input schema", () => {
  let service: WorkflowCompilingService;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;

  beforeEach(() => {
    configureCompilingTestBed();
    service = TestBed.inject(WorkflowCompilingService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
  });

  /** Replaces the operator's dynamic schema with one that already carries a propagated `enum` of attribute names. */
  const givePropagatedAttributeEnum = (operatorID: string): void => {
    const base = dynamicSchemaService.getDynamicSchema(operatorID);
    dynamicSchemaService.setDynamicSchema(operatorID, {
      ...base,
      jsonSchema: {
        type: "object",
        properties: {
          attribute: {
            type: "string",
            autofill: "attributeName",
            autofillAttributeOnPort: 0,
            enum: ["col_a", ""],
            uniqueItems: true,
          },
        },
      } as any,
    });
  };

  it("restores the original input attributes of a non-source operator", () => {
    // NlpSentiment declares one input port, so it is not a source operator
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    const operatorID = mockSentimentPredicate.operatorID;
    givePropagatedAttributeEnum(operatorID);

    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue(undefined);
    (service as any).applySchemaPropagationResult();

    const attribute = (dynamicSchemaService.getDynamicSchema(operatorID).jsonSchema.properties as any).attribute;
    expect(attribute.enum).toBeUndefined();
    expect(attribute.uniqueItems).toBeUndefined();
  });

  it("keeps a source operator's attributes, which come from its own table rather than an input port", () => {
    // ScanSource declares no input ports, so its attributes must survive untouched
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const operatorID = mockScanPredicate.operatorID;
    givePropagatedAttributeEnum(operatorID);
    const propagated = dynamicSchemaService.getDynamicSchema(operatorID);

    vi.spyOn(service, "getOperatorInputSchemaMap").mockReturnValue(undefined);
    const setDynamicSchema = vi.spyOn(dynamicSchemaService, "setDynamicSchema");
    (service as any).applySchemaPropagationResult();

    const attribute = (dynamicSchemaService.getDynamicSchema(operatorID).jsonSchema.properties as any).attribute;
    expect(attribute.enum).toEqual(["col_a", ""]);
    expect(attribute.uniqueItems).toBe(true);
    // the schema is unchanged, so it is never written back
    expect(setDynamicSchema).not.toHaveBeenCalled();
    expect(dynamicSchemaService.getDynamicSchema(operatorID)).toBe(propagated);
  });
});

describe("WorkflowCompilingService input port schema resolution", () => {
  let service: WorkflowCompilingService;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;

  const schemaA = [{ attributeName: "col_a", attributeType: "string" }];
  const schemaB = [{ attributeName: "col_b", attributeType: "integer" }];

  beforeEach(() => {
    configureCompilingTestBed();
    service = TestBed.inject(WorkflowCompilingService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
  });

  /** Overwrites the private compilation-state snapshot the resolution logic reads the output schemas from. */
  const setOutputSchemas = (operatorOutputPortSchemaMap: unknown): void => {
    (service as any).currentCompilationStateInfo = {
      state: CompilationState.Succeeded,
      physicalPlan: { operators: [], links: [] },
      operatorOutputPortSchemaMap,
    };
  };

  const addOperators = (...predicates: OperatorPredicate[]): void =>
    predicates.forEach(predicate => workflowActionService.addOperator(predicate, mockPoint));

  const link = (sourceID: string, sourcePort: string, targetID: string, targetPort: string, linkID: string) =>
    workflowActionService.addLink({
      linkID,
      source: { operatorID: sourceID, portID: sourcePort },
      target: { operatorID: targetID, portID: targetPort },
    });

  it("ignores an input link whose target port ID is not in the input-<n> form", () => {
    const target: OperatorPredicate = { ...mockSentimentPredicate, inputPorts: [{ portID: "the-input" }] };
    addOperators(mockScanPredicate, target);
    link(mockScanPredicate.operatorID, "output-0", target.operatorID, "the-input", "link-bad-target");
    setOutputSchemas({ [mockScanPredicate.operatorID]: { [port(0)]: schemaA } });

    const inputSchemaMap = service.getOperatorInputSchemaMap(target.operatorID);

    expect(Object.keys(inputSchemaMap!)).toEqual([port(0)]);
    expect(inputSchemaMap![port(0)]).toBeUndefined();
  });

  it("ignores an input link whose source port ID is not in the output-<n> form", () => {
    const source: OperatorPredicate = { ...mockScanPredicate, outputPorts: [{ portID: "the-output" }] };
    addOperators(source, mockSentimentPredicate);
    link(source.operatorID, "the-output", mockSentimentPredicate.operatorID, "input-0", "link-bad-source");
    setOutputSchemas({ [source.operatorID]: { [port(0)]: schemaA } });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID);

    expect(Object.keys(inputSchemaMap!)).toEqual([port(0)]);
    expect(inputSchemaMap![port(0)]).toBeUndefined();
  });

  it("resolves no schema when the upstream operator is missing from the output schema map", () => {
    addOperators(mockScanPredicate, mockSentimentPredicate);
    link(mockScanPredicate.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-sentiment");
    // the map holds a schema, but for a different operator
    setOutputSchemas({ "some-other-operator": { [port(0)]: schemaA } });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID);

    expect(Object.keys(inputSchemaMap!)).toEqual([port(0)]);
    expect(inputSchemaMap![port(0)]).toBeUndefined();
  });

  it("leaves unlinked input ports unresolved and puts the schema on the linked port only", () => {
    // MultiInputOutput declares three input ports; only input-1 is wired up
    addOperators(mockScanPredicate, mockMultiInputOutputPredicate);
    link(
      mockScanPredicate.operatorID,
      "output-0",
      mockMultiInputOutputPredicate.operatorID,
      "input-1",
      "link-scan-multi"
    );
    setOutputSchemas({ [mockScanPredicate.operatorID]: { [port(0)]: schemaA } });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockMultiInputOutputPredicate.operatorID);

    expect(Object.keys(inputSchemaMap!)).toEqual([port(0), port(1), port(2)]);
    expect(inputSchemaMap![port(0)]).toBeUndefined();
    expect(inputSchemaMap![port(1)]).toEqual(schemaA);
    expect(inputSchemaMap![port(2)]).toBeUndefined();
  });

  it("accepts two links into the same input port when they agree on the schema", () => {
    const secondScan: OperatorPredicate = { ...mockScanPredicate, operatorID: "scan-2" };
    addOperators(mockScanPredicate, secondScan, mockSentimentPredicate);
    link(mockScanPredicate.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-1");
    link(secondScan.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-2");
    setOutputSchemas({
      [mockScanPredicate.operatorID]: { [port(0)]: schemaA },
      [secondScan.operatorID]: { [port(0)]: [...schemaA] },
    });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID);

    expect(inputSchemaMap![port(0)]).toEqual(schemaA);
    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Succeeded);
  });

  it("fails compilation when two links into the same input port disagree on the schema", () => {
    const secondScan: OperatorPredicate = { ...mockScanPredicate, operatorID: "scan-2" };
    addOperators(mockScanPredicate, secondScan, mockSentimentPredicate);
    link(mockScanPredicate.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-1");
    link(secondScan.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-2");
    setOutputSchemas({
      [mockScanPredicate.operatorID]: { [port(0)]: schemaA },
      [secondScan.operatorID]: { [port(0)]: schemaB },
    });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID);

    // the conflicting port is left unresolved and the compilation is marked failed
    expect(inputSchemaMap![port(0)]).toBeUndefined();
    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Failed);
    const error = service.getWorkflowCompilationErrors()[mockSentimentPredicate.operatorID];
    expect(error.message).toBe("Multiple links with different schemas connected to the same input port 0");
    expect(error.details).toBe("Port 0 received 2 different schemas (some may be undefined)");
    expect(error.operatorId).toBe(mockSentimentPredicate.operatorID);
  });

  it("resolves nothing for an operator whose dynamic schema declares no input ports", () => {
    addOperators(mockScanPredicate, mockSentimentPredicate);
    link(mockScanPredicate.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-sentiment");
    const base = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    dynamicSchemaService.setDynamicSchema(mockSentimentPredicate.operatorID, {
      ...base,
      additionalMetadata: { ...base.additionalMetadata, inputPorts: [] },
    });
    setOutputSchemas({ [mockScanPredicate.operatorID]: { [port(0)]: schemaA } });

    expect(service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID)).toBeUndefined();
  });
});
