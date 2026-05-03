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
import { WorkflowResultExportService } from "./workflow-result-export.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { of } from "rxjs";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { DownloadService } from "src/app/dashboard/service/user/download/download.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;
  let workflowWebsocketServiceSpy: any;
  let workflowActionServiceSpy: any;
  let notificationServiceSpy: any;
  let executeWorkflowServiceSpy: any;
  let workflowResultServiceSpy: any;
  let downloadServiceSpy: any;
  let datasetServiceSpy: any;

  let jointGraphWrapperSpy: any;
  let texeraGraphSpy: any;

  beforeEach(() => {
    // Create spies for the required services
    jointGraphWrapperSpy = {
      getCurrentHighlightedOperatorIDs: vi.fn(),
      getJointOperatorHighlightStream: vi.fn(),
      getJointOperatorUnhighlightStream: vi.fn(),
    };
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getJointOperatorHighlightStream.mockReturnValue(of());
    jointGraphWrapperSpy.getJointOperatorUnhighlightStream.mockReturnValue(of());

    texeraGraphSpy = {
      getAllOperators: vi.fn(),
      getOperatorAddStream: vi.fn(),
      getOperatorDeleteStream: vi.fn(),
      getOperatorPropertyChangeStream: vi.fn(),
      getLinkAddStream: vi.fn(),
      getLinkDeleteStream: vi.fn(),
      getDisabledOperatorsChangedStream: vi.fn(),
      getAllLinks: vi.fn(),
    };
    texeraGraphSpy.getAllOperators.mockReturnValue([]);
    texeraGraphSpy.getOperatorAddStream.mockReturnValue(of());
    texeraGraphSpy.getOperatorDeleteStream.mockReturnValue(of());
    texeraGraphSpy.getOperatorPropertyChangeStream.mockReturnValue(of());
    texeraGraphSpy.getLinkAddStream.mockReturnValue(of());
    texeraGraphSpy.getLinkDeleteStream.mockReturnValue(of());
    texeraGraphSpy.getDisabledOperatorsChangedStream.mockReturnValue(of());
    texeraGraphSpy.getAllLinks.mockReturnValue([]);

    const wsSpy = { subscribeToEvent: vi.fn(), send: vi.fn() };
    wsSpy.subscribeToEvent.mockReturnValue(of()); // Return an empty observable
    const waSpy = { getJointGraphWrapper: vi.fn(), getTexeraGraph: vi.fn(), getWorkflow: vi.fn() };
    waSpy.getJointGraphWrapper.mockReturnValue(jointGraphWrapperSpy);
    waSpy.getTexeraGraph.mockReturnValue(texeraGraphSpy);
    waSpy.getWorkflow.mockReturnValue({ wid: "workflow1", name: "Test Workflow" });

    const ntSpy = { success: vi.fn(), error: vi.fn(), loading: vi.fn() };
    const ewSpy = { getExecutionStateStream: vi.fn(), getExecutionState: vi.fn() };
    ewSpy.getExecutionStateStream.mockReturnValue(of({ previous: {}, current: { state: ExecutionState.Completed } }));
    ewSpy.getExecutionState.mockReturnValue({ state: ExecutionState.Completed });

    const wrSpy = { hasAnyResult: vi.fn(), getResultService: vi.fn(), getPaginatedResultService: vi.fn() };
    const downloadSpy = { downloadOperatorsResult: vi.fn() };
    downloadSpy.downloadOperatorsResult.mockReturnValue(of(new Blob()));

    const datasetSpy = { retrieveAccessibleDatasets: vi.fn() };
    datasetSpy.retrieveAccessibleDatasets.mockReturnValue(of([]));

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        WorkflowResultExportService,
        { provide: WorkflowWebsocketService, useValue: wsSpy },
        { provide: WorkflowActionService, useValue: waSpy },
        { provide: NotificationService, useValue: ntSpy },
        { provide: ExecuteWorkflowService, useValue: ewSpy },
        { provide: WorkflowResultService, useValue: wrSpy },
        { provide: DownloadService, useValue: downloadSpy },
        { provide: DatasetService, useValue: datasetSpy },
        ...commonTestProviders,
      ],
    });

    // Inject the service and spies
    service = TestBed.inject(WorkflowResultExportService);
    workflowWebsocketServiceSpy = TestBed.inject(WorkflowWebsocketService) as any;
    workflowActionServiceSpy = TestBed.inject(WorkflowActionService) as any;
    notificationServiceSpy = TestBed.inject(NotificationService) as any;
    executeWorkflowServiceSpy = TestBed.inject(ExecuteWorkflowService) as any;
    workflowResultServiceSpy = TestBed.inject(WorkflowResultService) as any;
    downloadServiceSpy = TestBed.inject(DownloadService) as any;
    datasetServiceSpy = TestBed.inject(DatasetService) as any;
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
