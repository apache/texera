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
import { testPort, testMacro, expansionFixture } from "../../../common/testing/macro-graph";
import { testField, testLink } from "../../../common/testing/workflow-graph";

import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
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
import { CompilationState, OperatorPortSchemaMap } from "../../types/workflow-compiling.interface";
import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
import { mockNlpSentimentSchema } from "../operator-metadata/mock-operator-metadata.data";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { macroInstance, macroRuntimeID } from "../../../common/util/workflow-macro";

const port = (id: number): string => serializePortIdentity({ id, internal: false });

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
    configureCompilingTestBed();
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
    configureCompilingTestBed();
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

describe("WorkflowCompilingService column schemas", () => {
  const columns: OperatorPortSchemaMap = {
    [port(0)]: [
      { attributeName: "col_a", attributeType: "string" },
      { attributeName: "col_b", attributeType: "integer" },
    ],
  };
  const scalar: CustomJSONSchema7 = { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 };
  const list: CustomJSONSchema7 = {
    type: "array",
    autofill: "attributeNameList",
    autofillAttributeOnPort: 0,
    items: { type: "string" },
  };
  const schemaWith = (attribute: CustomJSONSchema7, required = false): OperatorSchema => ({
    ...mockNlpSentimentSchema,
    jsonSchema: { type: "object", properties: { attribute }, required: required ? ["attribute"] : [] },
  });
  const attributeOf = (schema: OperatorSchema) => schema.jsonSchema.properties!.attribute as CustomJSONSchema7;
  const fill = (attribute: CustomJSONSchema7, required = false) =>
    attributeOf(WorkflowCompilingService.setOperatorInputAttrs(schemaWith(attribute, required), columns));

  it.each([undefined, {}])("leaves the schema unchanged with no input schema (%j)", inputs => {
    const schema = schemaWith(scalar);
    expect(WorkflowCompilingService.setOperatorInputAttrs(schema, inputs)).toBe(schema);
  });

  it.each([
    { label: "optional", field: scalar, expected: ["col_a", "col_b", ""] },
    { label: "with a default", field: { ...scalar, default: "col_a" }, expected: ["col_a", "col_b", "col_a"] },
    { label: "required", field: scalar, required: true, expected: ["col_a", "col_b"] },
    {
      label: "with an extra value",
      field: { ...scalar, additionalEnumValue: "*" },
      expected: ["col_a", "col_b", "*", ""],
    },
    { label: "with no port", field: { ...scalar, autofillAttributeOnPort: undefined }, expected: undefined },
    { label: "with no port schema", field: { ...scalar, autofillAttributeOnPort: 5 }, expected: undefined },
  ])("fills a scalar attribute $label", ({ field, required, expected }) => {
    expect(fill(field, required)).toMatchObject({ type: "string", uniqueItems: true, enum: expected });
  });

  it("rejects a non-string default for a scalar attribute", () => {
    expect(() => fill({ ...scalar, default: 42 })).toThrow("default value must be a string");
  });

  it("fills the items of an attribute list", () => {
    expect(fill(list)).toMatchObject({
      type: "array",
      uniqueItems: true,
      items: { type: "string", enum: ["col_a", "col_b", ""] },
    });
  });

  it.each([
    { label: "scalar", filled: { ...scalar, enum: ["col_a", "col_b", ""] }, original: scalar },
    {
      label: "list",
      filled: { ...list, items: { type: "string" as const, enum: ["col_a", "col_b", ""] } },
      original: list,
    },
  ])("clears the propagated $label choices and uniqueness", ({ filled, original }) => {
    const restored = WorkflowCompilingService.restoreOperatorInputAttrs(schemaWith({ ...filled, uniqueItems: true }));
    expect(attributeOf(restored)).toEqual(original);
  });

  it("restores the scalar schema after propagation", () => {
    const propagated = fill(scalar);
    expect(propagated.enum).toEqual(["col_a", "col_b", ""]);
    expect(attributeOf(WorkflowCompilingService.restoreOperatorInputAttrs(schemaWith(propagated)))).toEqual(scalar);
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
      { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
    ],
  });
};

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

    const body = request.request.body;
    expect(body.operators.map((operator: any) => operator.operatorID).sort()).toEqual(
      [mockScanPredicate.operatorID, mockResultPredicate.operatorID].sort()
    );
    expect(body.links.length).toBe(1);
    expect(body.opsToReuseResult).toEqual([]);
    expect(body.opsToViewResult).toEqual([]);

    request.flush({ physicalPlan: { operators: [], links: [] }, operatorOutputSchemas: {}, operatorErrors: {} });
  }));

  it("sends pinned macro graphs and projects backend schemas and errors without changing snapshots", fakeAsync(() => {
    const definition = testMacro("backend", [{ ...mockScanPredicate, operatorProperties: { tableName: "twitter" } }], {
      outputs: [testPort("out", mockScanPredicate.operatorID, "output-0")],
      name: "Nested source",
    });
    workflowActionService.setMacroDependencies([definition]);
    workflowActionService.addOperator(macroInstance(definition, "instance"), mockPoint);
    const content = workflowActionService.getWorkflowContent(),
      before = JSON.stringify(content);
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    const request = httpTestingController.expectOne(compileUrl);
    const plan = request.request.body;
    expect(plan.operators).toEqual([]);
    expect(plan.macroContent.macroDependencies).toEqual([definition]);
    const expanded = expansionFixture(content, [
        [["instance", mockScanPredicate.operatorID], definition.content.operators[0]],
      ]),
      leaf = expanded.content.operators[0].operatorID;
    request.flush({
      macroExpansion: expanded,
      // A scoped passthrough's native source can be outside the visible expansion.
      operatorOutputSchemas: { instance: { [port(0)]: scanOutputSchema } },
      operatorErrors: {
        [leaf]: { operatorId: leaf, message: "invalid column" },
        instance: { operatorId: "instance", message: "Nested source / Scan: invalid column" },
      },
    });
    expect(service.getOperatorOutputSchemaMap("instance")).toEqual({ [port(0)]: scanOutputSchema });
    expect(service.getWorkflowCompilationErrors()["instance"].message).toBe("Nested source / Scan: invalid column");
    expect(workflowActionService.getMacroExpansion()).toEqual(expanded);
    expect(JSON.stringify(workflowActionService.getWorkflowContent())).toBe(before);
  }));

  it("cancels obsolete compilation responses when another graph edit starts compilation", fakeAsync(() => {
    const previous = buildWorkflowAndCompile();
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "new" });
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    const current = httpTestingController.expectOne(compileUrl);
    expect(previous.cancelled).toBe(true);
    current.flush({ physicalPlan: { operators: [], links: [] }, operatorOutputSchemas: {}, operatorErrors: {} });
    expect(service.getWorkflowCompilationState()).toBe(CompilationState.Succeeded);
  }));

  it("propagates per-instance internal column choices through exposed nested fields and clears unavailable schemas", fakeAsync(() => {
    const scan = { ...mockScanPredicate, operatorProperties: { tableName: "twitter" } };
    const secondScan = {
      ...scan,
      operatorID: "second-scan",
      outputPorts: [{ portID: "output-0" }, { portID: "output-2" }],
    };
    const secondSentiment = { ...mockSentimentPredicate, operatorID: "second-sentiment" };
    const operators = [scan, mockSentimentPredicate, secondScan, secondSentiment];
    const links = [
      testLink(scan.operatorID, mockSentimentPredicate.operatorID),
      testLink(secondScan.operatorID, secondSentiment.operatorID, "output-2"),
    ];
    const leaf = testMacro("leaf", operators, {
      links,
      formBinding: {
        fields: [mockSentimentPredicate, secondSentiment].map((op, i) =>
          testField(op.operatorID, "attribute", `column-${i}`, `Column ${i}`)
        ),
      },
      name: "Internal columns",
    });
    const outer = testMacro("outer", [macroInstance(leaf, "child")], {
      formBinding: {
        fields: leaf.content.formBinding.fields.map(f => ({ ...f, operatorID: "child", propertyKey: f.id })),
      },
      name: "Nested columns",
    });
    workflowActionService.setMacroDependencies([leaf, outer]);
    workflowActionService.addOperator(macroInstance(outer, "outer-instance"), mockPoint);
    const content = workflowActionService.getWorkflowContent(),
      before = JSON.stringify(content);
    const path = ["outer-instance", "child"];
    const runtime = (id: string) => macroRuntimeID(path, id);
    const expanded = expansionFixture(
      content,
      operators.map(op => [[...path, op.operatorID], op]),
      links.map(({ linkID, source, target }) =>
        testLink(runtime(source.operatorID), runtime(target.operatorID), source.portID, target.portID, linkID)
      )
    );
    expanded.inspectors[JSON.stringify(path)].inputs = {
      [mockSentimentPredicate.operatorID]: [{ link: 0, input: 0, output: 0 }],
      [secondSentiment.operatorID]: [{ link: 1, input: 0, output: 1 }],
    };
    const columns = () =>
      TestBed.inject(DynamicSchemaService).getDynamicSchema("outer-instance").jsonSchema.properties!;
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    httpTestingController.expectOne(compileUrl).flush({
      macroExpansion: expanded,
      operatorOutputSchemas: {
        [runtime(scan.operatorID)]: { [port(0)]: scanOutputSchema },
        [runtime(secondScan.operatorID)]: { [port(1)]: [{ attributeName: "col_b", attributeType: "integer" }] },
      },
      operatorErrors: {},
    });
    expect((columns()["column-0"] as any).enum).toEqual(["col_a"]);
    expect((columns()["column-1"] as any).enum).toEqual(["col_b"]);
    expect(JSON.stringify(workflowActionService.getWorkflowContent())).toBe(before);

    workflowActionService.setOperatorProperty("outer-instance", { "column-0": "col_a" });
    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);
    httpTestingController
      .expectOne(compileUrl)
      .flush({ macroExpansion: expanded, operatorOutputSchemas: {}, operatorErrors: {} });
    expect((columns()["column-0"] as any).enum).toBeUndefined();
    expect((columns()["column-1"] as any).enum).toBeUndefined();
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
    workflowActionService.addLink(testLink(sourceID, targetID, sourcePort, targetPort, linkID));

  const addMacro = () => {
    const definition = testMacro("schema-boundary", [], {
      inputs: ["input-first", "input-second"].map(portID => ({ portID, targets: [] })),
      outputs: ["output-first", "output-second"].map(portID => ({ portID, targets: [] })),
      name: "Two ports",
    });
    workflowActionService.setMacroDependencies([definition]);
    const instance = macroInstance(definition, "two-ports");
    addOperators(instance);
    return instance;
  };

  it.each([false, true])("uses the macro output ordinal even if its schema is absent (%s)", absent => {
    const source = addMacro();
    addOperators(mockSentimentPredicate);
    link(source.operatorID, "output-second", mockSentimentPredicate.operatorID, "input-0", "macro-output");
    setOutputSchemas({
      [source.operatorID]: absent ? { [port(0)]: schemaA } : { [port(0)]: schemaA, [port(1)]: schemaB },
    });
    expect(service.getPortInputSchema(mockSentimentPredicate.operatorID, 0)).toEqual(absent ? undefined : schemaB);
  });

  it("resolves a macro's stable input ID without assigning the schema to another port", () => {
    const target = addMacro();
    addOperators(mockScanPredicate);
    link(mockScanPredicate.operatorID, "output-0", target.operatorID, "input-second", "macro-input");
    setOutputSchemas({ [mockScanPredicate.operatorID]: { [port(0)]: schemaA } });

    expect(service.getPortInputSchema(target.operatorID, 0)).toBeUndefined();
    expect(service.getPortInputSchema(target.operatorID, 1)).toEqual(schemaA);
  });

  it("keeps native port schema ordinals after removing a middle input and output", () => {
    const source = {
      ...mockMultiInputOutputPredicate,
      operatorID: "native-source",
      outputPorts: [{ portID: "output-0" }, { portID: "output-2" }],
    };
    const target = {
      ...mockMultiInputOutputPredicate,
      inputPorts: [{ portID: "input-0" }, { portID: "input-2" }],
    };
    addOperators(source, target);
    const base = dynamicSchemaService.getDynamicSchema(target.operatorID);
    dynamicSchemaService.setDynamicSchema(target.operatorID, {
      ...base,
      additionalMetadata: { ...base.additionalMetadata, inputPorts: [{}, {}] },
    });
    link(source.operatorID, "output-2", target.operatorID, "input-2", "native-gaps");
    setOutputSchemas({ [source.operatorID]: { [port(0)]: schemaA, [port(1)]: schemaB } });

    expect(service.getPortInputSchema(target.operatorID, 0)).toBeUndefined();
    expect(service.getPortInputSchema(target.operatorID, 1)).toEqual(schemaB);
  });

  it.each(["input", "output"] as const)("resolves a native %s independently of its ID format", direction => {
    const sourcePort = direction === "output" ? "the-output" : "output-0";
    const targetPort = direction === "input" ? "the-input" : "input-0";
    const source = { ...mockScanPredicate, outputPorts: [{ portID: sourcePort }] };
    const target = { ...mockSentimentPredicate, inputPorts: [{ portID: targetPort }] };
    addOperators(source, target);
    link(source.operatorID, sourcePort, target.operatorID, targetPort, "custom-port");
    setOutputSchemas({ [source.operatorID]: { [port(0)]: schemaA } });
    expect(service.getOperatorInputSchemaMap(target.operatorID)).toEqual({ [port(0)]: schemaA });
  });

  it("uses numbered projected ports only for sources outside the visible graph", () => {
    addOperators(mockSentimentPredicate);
    setOutputSchemas({});
    const input = {
      source: { operatorID: "external", portID: "output-2" },
      target: { operatorID: mockSentimentPredicate.operatorID, portID: "input-0" },
    };
    (service as any).macroInputs = {
      doc: workflowActionService.suspendWorkflow().shared,
      inputLinks: () => [input],
      schemas: { external: { [port(2)]: schemaB } },
    };
    expect(service.getPortInputSchema(mockSentimentPredicate.operatorID, 0)).toEqual(schemaB);

    input.source.portID = "missing";
    expect(service.getPortInputSchema(mockSentimentPredicate.operatorID, 0)).toBeUndefined();

    addOperators({ ...mockScanPredicate, operatorID: "external" });
    input.source.portID = "output-2";
    expect(service.getPortInputSchema(mockSentimentPredicate.operatorID, 0)).toBeUndefined();
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

  it.each([true, false])("resolves two links to one port based on whether their schemas agree (%s)", agree => {
    const secondScan: OperatorPredicate = { ...mockScanPredicate, operatorID: "scan-2" };
    addOperators(mockScanPredicate, secondScan, mockSentimentPredicate);
    link(mockScanPredicate.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-1");
    link(secondScan.operatorID, "output-0", mockSentimentPredicate.operatorID, "input-0", "link-scan-2");
    setOutputSchemas({
      [mockScanPredicate.operatorID]: { [port(0)]: schemaA },
      [secondScan.operatorID]: { [port(0)]: agree ? [...schemaA] : schemaB },
    });

    const inputSchemaMap = service.getOperatorInputSchemaMap(mockSentimentPredicate.operatorID);

    expect(inputSchemaMap![port(0)]).toEqual(agree ? schemaA : undefined);
    expect(service.getWorkflowCompilationState()).toBe(agree ? CompilationState.Succeeded : CompilationState.Failed);
    if (!agree)
      expect(service.getWorkflowCompilationErrors()[mockSentimentPredicate.operatorID]).toMatchObject({
        message: "Multiple links with different schemas connected to the same input port 0",
        details: "Port 0 received 2 different schemas (some may be undefined)",
        operatorId: mockSentimentPredicate.operatorID,
      });
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
