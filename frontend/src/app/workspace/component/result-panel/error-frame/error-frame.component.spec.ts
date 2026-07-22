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

import { ErrorFrameComponent } from "./error-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";

function fatalError(overrides: Partial<WorkflowFatalError> = {}): WorkflowFatalError {
  return {
    message: "msg",
    details: "details",
    operatorId: "op1",
    workerId: "w1",
    type: { name: "GENERAL" },
    timestamp: { nanos: 0, seconds: 0 },
    ...overrides,
  } as WorkflowFatalError;
}

describe("ErrorFrameComponent", () => {
  let component: ErrorFrameComponent;
  let fixture: ComponentFixture<ErrorFrameComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let compilingService: WorkflowCompilingService;
  let actionService: WorkflowActionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ErrorFrameComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ErrorFrameComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    compilingService = TestBed.inject(WorkflowCompilingService);
    actionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  afterEach(() => vi.restoreAllMocks());

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("renderError", () => {
    it("groups the errors by their type name", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ type: { name: "TYPE_A" } }),
        fatalError({ type: { name: "TYPE_A" } }),
        fatalError({ type: { name: "TYPE_B" } }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect([...component.categoryToErrorMapping.keys()]).toEqual(["TYPE_A", "TYPE_B"]);
      expect(component.categoryToErrorMapping.get("TYPE_A")).toHaveLength(2);
      expect(component.categoryToErrorMapping.get("TYPE_B")).toHaveLength(1);
    });

    it("appends the compilation errors from the compiling service", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({
        op1: fatalError({ type: { name: "COMPILATION_ERROR" }, message: "boom" }),
      });

      component.renderError();

      expect(component.categoryToErrorMapping.get("COMPILATION_ERROR")).toHaveLength(1);
    });

    it("keeps only the errors of the bound operator when operatorId is set", () => {
      component.operatorId = "op1";
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ operatorId: "op1", type: { name: "X" } }),
        fatalError({ operatorId: "op2", type: { name: "X" } }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect(component.categoryToErrorMapping.get("X")).toHaveLength(1);
      expect(component.categoryToErrorMapping.get("X")![0].operatorId).toBe("op1");
    });

    it("strips the exception prefix and 'requirement failed:' for COMPILATION_ERROR", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({
          type: { name: "COMPILATION_ERROR" },
          message: "java.lang.RuntimeException: something broke",
          details: "requirement failed: bad input",
        }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      const error = component.categoryToErrorMapping.get("COMPILATION_ERROR")![0];
      expect(error.message).toBe("something broke");
      expect(error.details).toBe("bad input");
    });

    it("leaves the message untouched for non-formatted error types", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ type: { name: "RESOURCE_ERROR" }, message: "java.lang.RuntimeException: keep me" }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect(component.categoryToErrorMapping.get("RESOURCE_ERROR")![0].message).toBe(
        "java.lang.RuntimeException: keep me"
      );
    });
  });

  describe("onClickGotoButton", () => {
    it("highlights the offending operator via the workflow action service", () => {
      const highlight = vi.spyOn(actionService, "highlightOperators").mockImplementation(() => {});

      component.onClickGotoButton("op-42");

      expect(highlight).toHaveBeenCalledWith(false, "op-42");
    });
  });
});
