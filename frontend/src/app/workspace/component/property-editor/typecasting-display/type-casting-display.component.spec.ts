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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { Subject } from "rxjs";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";

import { TypeCastingDisplayComponent, TYPE_CASTING_OPERATOR_TYPE } from "./type-casting-display.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { WorkflowGraph, WorkflowGraphReadonly } from "../../../service/workflow-graph/model/workflow-graph";
import { AttributeType, CompilationState, OperatorPortSchemaMap } from "../../../types/workflow-compiling.interface";
import { ValidationWorkflowService } from "../../../service/validation/validation-workflow.service";

describe("TypecastingDisplayComponent", () => {
  let component: TypeCastingDisplayComponent;
  let fixture: ComponentFixture<TypeCastingDisplayComponent>;
  let compilationStateInfoChangedStream: Subject<CompilationState>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TypeCastingDisplayComponent, HttpClientTestingModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        JointUIService,
        UndoRedoService,
        WorkflowUtilService,
        WorkflowActionService,
        WorkflowCompilingService,
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    // Control the compilation-state stream the component subscribes to in ngOnInit, so tests can
    // drive it without touching WorkflowCompilingService's private field. Must be wired before
    // detectChanges() triggers ngOnInit.
    compilationStateInfoChangedStream = new Subject<CompilationState>();
    vi.spyOn(TestBed.inject(WorkflowCompilingService), "getCompilationStateInfoChangedStream").mockReturnValue(
      compilationStateInfoChangedStream
    );

    fixture = TestBed.createComponent(TypeCastingDisplayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  afterEach(() => {
    fixture.destroy();
    vi.restoreAllMocks();
  });

  // Build an OperatorPredicate; when typeCastingUnits is supplied it is placed under
  // operatorProperties exactly as the TypeCasting operator descriptor stores it.
  const makeOperator = (
    operatorID: string,
    operatorType: string,
    typeCastingUnits?: ReadonlyArray<{ attribute: string; resultType: AttributeType }>
  ): OperatorPredicate => ({
    operatorID,
    operatorType,
    operatorVersion: "v1",
    operatorProperties: typeCastingUnits ? { typeCastingUnits } : {},
    inputPorts: [{ portID: "input-0" }],
    outputPorts: [{ portID: "output-0" }],
    showAdvanced: true,
    isDisabled: false,
  });

  // The component reads/writes the same graph instance the injected WorkflowActionService owns.
  const getGraph = (): WorkflowGraphReadonly => TestBed.inject(WorkflowActionService).getTexeraGraph();

  describe("ngOnChanges", () => {
    it("hides the display and skips rerender when there is no currentOperatorId", () => {
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});
      component.currentOperatorId = undefined;
      component.displayTypeCastingSchemaInformation = true;

      component.ngOnChanges();

      expect(component.displayTypeCastingSchemaInformation).toBe(false);
      expect(rerenderSpy).not.toHaveBeenCalled();
    });

    it("hides the display and skips rerender when the operator is not a TypeCasting operator", () => {
      const graph = getGraph();
      vi.spyOn(graph, "getOperator").mockReturnValue(makeOperator("op1", "ScanSource"));
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});
      component.currentOperatorId = "op1";
      component.displayTypeCastingSchemaInformation = true;

      component.ngOnChanges();

      expect(component.displayTypeCastingSchemaInformation).toBe(false);
      expect(rerenderSpy).not.toHaveBeenCalled();
    });

    it("shows the display and rerenders when the operator is a TypeCasting operator", () => {
      const graph = getGraph();
      vi.spyOn(graph, "getOperator").mockReturnValue(
        makeOperator("tc1", TYPE_CASTING_OPERATOR_TYPE, [{ attribute: "age", resultType: "string" }])
      );
      const compiling = TestBed.inject(WorkflowCompilingService);
      vi.spyOn(compiling, "getOperatorInputSchemaMap").mockReturnValue({
        "port-0": [{ attributeName: "age", attributeType: "integer" }],
      } as OperatorPortSchemaMap);
      component.currentOperatorId = "tc1";

      component.ngOnChanges();

      expect(component.displayTypeCastingSchemaInformation).toBe(true);
      expect(component.schemaToDisplay).toEqual([{ attributeName: "age", attributeType: "string" }]);
    });
  });

  describe("rerender", () => {
    it("returns early and leaves schemaToDisplay untouched when currentOperatorId is undefined", () => {
      const compiling = TestBed.inject(WorkflowCompilingService);
      const schemaSpy = vi.spyOn(compiling, "getOperatorInputSchemaMap");
      const sentinel = [{ attributeName: "keep", attributeType: "string" as AttributeType }];
      component.currentOperatorId = undefined;
      component.schemaToDisplay = sentinel;

      component.rerender();

      expect(component.schemaToDisplay).toBe(sentinel);
      expect(schemaSpy).not.toHaveBeenCalled();
    });

    it("casts attributes listed in typeCastingUnits and passes the remaining attributes through", () => {
      const graph = getGraph();
      vi.spyOn(graph, "getOperator").mockReturnValue(
        makeOperator("tc1", TYPE_CASTING_OPERATOR_TYPE, [{ attribute: "age", resultType: "string" }])
      );
      const compiling = TestBed.inject(WorkflowCompilingService);
      // Two ports: port-0 carries the schema, port-1 is undefined and must be skipped safely.
      vi.spyOn(compiling, "getOperatorInputSchemaMap").mockReturnValue({
        "port-0": [
          { attributeName: "age", attributeType: "integer" },
          { attributeName: "name", attributeType: "string" },
        ],
        "port-1": undefined,
      } as OperatorPortSchemaMap);
      component.currentOperatorId = "tc1";

      component.rerender();

      expect(component.schemaToDisplay).toEqual([
        { attributeName: "age", attributeType: "string" },
        { attributeName: "name", attributeType: "string" },
      ]);
    });

    it("passes every attribute through unchanged when typeCastingUnits is absent", () => {
      const graph = getGraph();
      vi.spyOn(graph, "getOperator").mockReturnValue(makeOperator("tc1", TYPE_CASTING_OPERATOR_TYPE));
      const compiling = TestBed.inject(WorkflowCompilingService);
      vi.spyOn(compiling, "getOperatorInputSchemaMap").mockReturnValue({
        "port-0": [{ attributeName: "x", attributeType: "integer" }],
      } as OperatorPortSchemaMap);
      component.currentOperatorId = "tc1";

      component.rerender();

      expect(component.schemaToDisplay).toEqual([{ attributeName: "x", attributeType: "integer" }]);
    });

    it("produces an empty schemaToDisplay when there is no input schema", () => {
      const graph = getGraph();
      vi.spyOn(graph, "getOperator").mockReturnValue(
        makeOperator("tc1", TYPE_CASTING_OPERATOR_TYPE, [{ attribute: "age", resultType: "string" }])
      );
      const compiling = TestBed.inject(WorkflowCompilingService);
      vi.spyOn(compiling, "getOperatorInputSchemaMap").mockReturnValue(undefined);
      component.currentOperatorId = "tc1";
      component.schemaToDisplay = [{ attributeName: "stale", attributeType: "string" }];

      component.rerender();

      expect(component.schemaToDisplay).toEqual([]);
    });
  });

  describe("stream handlers registered in ngOnInit", () => {
    // The operator-property-change stream is a shared multicast: the real ValidationWorkflowService
    // also subscribes and calls validateOperator() on the emitted id. These synthetic events reference
    // operators that were never registered in metadata, so neutralize that unrelated collaborator to
    // keep the tests focused on the component's own handler (and free of incidental console noise).
    beforeEach(() => {
      vi.spyOn(TestBed.inject(ValidationWorkflowService), "validateOperator").mockReturnValue({ isValid: true });
    });

    it("rerenders when the matching TypeCasting operator's properties change", () => {
      const graph = getGraph();
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});
      component.currentOperatorId = "tc1";

      (graph as WorkflowGraph).operatorPropertyChangeSubject.next({
        operator: makeOperator("tc1", TYPE_CASTING_OPERATOR_TYPE),
      });

      expect(rerenderSpy).toHaveBeenCalledTimes(1);
    });

    it("ignores property changes emitted for a different operator id", () => {
      const graph = getGraph();
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});
      component.currentOperatorId = "tc1";

      (graph as WorkflowGraph).operatorPropertyChangeSubject.next({
        operator: makeOperator("other", TYPE_CASTING_OPERATOR_TYPE),
      });

      expect(rerenderSpy).not.toHaveBeenCalled();
    });

    it("ignores property changes for the matching id but a non-TypeCasting operator type", () => {
      const graph = getGraph();
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});
      component.currentOperatorId = "tc1";

      (graph as WorkflowGraph).operatorPropertyChangeSubject.next({
        operator: makeOperator("tc1", "ScanSource"),
      });

      expect(rerenderSpy).not.toHaveBeenCalled();
    });

    it("rerenders when the workflow compilation state changes", () => {
      const rerenderSpy = vi.spyOn(component, "rerender").mockImplementation(() => {});

      compilationStateInfoChangedStream.next(CompilationState.Succeeded);

      expect(rerenderSpy).toHaveBeenCalled();
    });
  });
});
