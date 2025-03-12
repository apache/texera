import { TestBed, fakeAsync, tick } from "@angular/core/testing";
import { WorkflowResultExportService } from "./workflow-result-export.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import {
  WorkflowResultService,
  OperatorPaginationResultService,
  OperatorResultService,
} from "../workflow-result/workflow-result.service";
import { FileSaverService } from "../../../dashboard/service/user/file/file-saver.service";
import { of, EMPTY } from "rxjs";
import { PaginatedResultEvent } from "../../types/workflow-websocket.interface";
import { ExecutionState } from "../../types/execute-workflow.interface";
import * as JSZip from "jszip";
import { DownloadService } from "src/app/dashboard/service/user/download/download.service";

describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;
  let workflowWebsocketServiceSpy: jasmine.SpyObj<WorkflowWebsocketService>;
  let workflowActionServiceSpy: jasmine.SpyObj<WorkflowActionService>;
  let notificationServiceSpy: jasmine.SpyObj<NotificationService>;
  let executeWorkflowServiceSpy: jasmine.SpyObj<ExecuteWorkflowService>;
  let workflowResultServiceSpy: jasmine.SpyObj<WorkflowResultService>;
  let downloadServiceSpy: jasmine.SpyObj<DownloadService>;

  let jointGraphWrapperSpy: jasmine.SpyObj<any>;
  let texeraGraphSpy: jasmine.SpyObj<any>;

  beforeEach(() => {
    // Create spies for the required services
    jointGraphWrapperSpy = jasmine.createSpyObj("JointGraphWrapper", [
      "getCurrentHighlightedOperatorIDs",
      "getJointOperatorHighlightStream",
      "getJointOperatorUnhighlightStream",
    ]);
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue([]);
    jointGraphWrapperSpy.getJointOperatorHighlightStream.and.returnValue(of());
    jointGraphWrapperSpy.getJointOperatorUnhighlightStream.and.returnValue(of());

    texeraGraphSpy = jasmine.createSpyObj("TexeraGraph", ["getAllOperators"]);
    texeraGraphSpy.getAllOperators.and.returnValue([]);

    const wsSpy = jasmine.createSpyObj("WorkflowWebsocketService", ["subscribeToEvent", "send"]);
    wsSpy.subscribeToEvent.and.returnValue(of()); // Return an empty observable
    const waSpy = jasmine.createSpyObj("WorkflowActionService", [
      "getJointGraphWrapper",
      "getTexeraGraph",
      "getWorkflow",
    ]);
    waSpy.getJointGraphWrapper.and.returnValue(jointGraphWrapperSpy);
    waSpy.getTexeraGraph.and.returnValue(texeraGraphSpy);
    waSpy.getWorkflow.and.returnValue({ wid: "workflow1", name: "Test Workflow" });

    const ntSpy = jasmine.createSpyObj("NotificationService", ["success", "error", "loading"]);
    const ewSpy = jasmine.createSpyObj("ExecuteWorkflowService", ["getExecutionStateStream", "getExecutionState"]);
    ewSpy.getExecutionStateStream.and.returnValue(of({ previous: {}, current: { state: ExecutionState.Completed } }));
    ewSpy.getExecutionState.and.returnValue({ state: ExecutionState.Completed });

    const wrSpy = jasmine.createSpyObj("WorkflowResultService", [
      "hasAnyResult",
      "getResultService",
      "getPaginatedResultService",
    ]);
    const downloadSpy = jasmine.createSpyObj("DownloadService", ["downloadOperatorsResult"]);
    downloadSpy.downloadOperatorsResult.and.returnValue(of(new Blob()));

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
      ],
    });

    // Inject the service and spies
    service = TestBed.inject(WorkflowResultExportService);
    workflowWebsocketServiceSpy = TestBed.inject(WorkflowWebsocketService) as jasmine.SpyObj<WorkflowWebsocketService>;
    workflowActionServiceSpy = TestBed.inject(WorkflowActionService) as jasmine.SpyObj<WorkflowActionService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as jasmine.SpyObj<NotificationService>;
    executeWorkflowServiceSpy = TestBed.inject(ExecuteWorkflowService) as jasmine.SpyObj<ExecuteWorkflowService>;
    workflowResultServiceSpy = TestBed.inject(WorkflowResultService) as jasmine.SpyObj<WorkflowResultService>;
    downloadServiceSpy = TestBed.inject(DownloadService) as jasmine.SpyObj<DownloadService>;
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
