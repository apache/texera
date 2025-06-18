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

import { async, ComponentFixture, TestBed } from "@angular/core/testing";

import { ResultPanelComponent } from "./result-panel.component";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { By } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { mockPoint, mockResultPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { ComputingUnitStatusService } from "../../service/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../service/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("ResultPanelComponent", () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let workflowActionService: WorkflowActionService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ResultPanelComponent],
      imports: [HttpClientTestingModule, NzModalModule],
      providers: [
        WorkflowActionService,
        ExecuteWorkflowService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => expect(component).toBeTruthy());

  it("should show nothing by default", () => {
    expect(component.frameComponentConfigs.size).toBe(0);
  });

  it("should show the result panel if a workflow finishes execution", () => {
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Running,
    });
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Completed,
    });
    fixture.detectChanges();
    const resultPanelDiv = fixture.debugElement.query(By.css("#result-container"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;
    expect(resultPanelHtmlElement).toBeTruthy();
  });
});
