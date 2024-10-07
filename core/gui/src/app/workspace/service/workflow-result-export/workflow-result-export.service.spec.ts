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

describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;
  let workflowWebsocketServiceSpy: jasmine.SpyObj<WorkflowWebsocketService>;
  let workflowActionServiceSpy: jasmine.SpyObj<WorkflowActionService>;
  let notificationServiceSpy: jasmine.SpyObj<NotificationService>;
  let executeWorkflowServiceSpy: jasmine.SpyObj<ExecuteWorkflowService>;
  let workflowResultServiceSpy: jasmine.SpyObj<WorkflowResultService>;
  let fileSaverServiceSpy: jasmine.SpyObj<FileSaverService>;

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
    const fsSpy = jasmine.createSpyObj("FileSaverService", ["saveAs"]);

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        WorkflowResultExportService,
        { provide: WorkflowWebsocketService, useValue: wsSpy },
        { provide: WorkflowActionService, useValue: waSpy },
        { provide: NotificationService, useValue: ntSpy },
        { provide: ExecuteWorkflowService, useValue: ewSpy },
        { provide: WorkflowResultService, useValue: wrSpy },
        { provide: FileSaverService, useValue: fsSpy },
      ],
    });

    // Inject the service and spies
    service = TestBed.inject(WorkflowResultExportService);
    workflowWebsocketServiceSpy = TestBed.inject(WorkflowWebsocketService) as jasmine.SpyObj<WorkflowWebsocketService>;
    workflowActionServiceSpy = TestBed.inject(WorkflowActionService) as jasmine.SpyObj<WorkflowActionService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as jasmine.SpyObj<NotificationService>;
    executeWorkflowServiceSpy = TestBed.inject(ExecuteWorkflowService) as jasmine.SpyObj<ExecuteWorkflowService>;
    workflowResultServiceSpy = TestBed.inject(WorkflowResultService) as jasmine.SpyObj<WorkflowResultService>;
    fileSaverServiceSpy = TestBed.inject(FileSaverService) as jasmine.SpyObj<FileSaverService>;
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should export paginated results as CSV for highlighted operators", fakeAsync(() => {
    // Arrange
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator1"]);

    const paginatedResultServiceSpy = jasmine.createSpyObj("OperatorPaginationResultService", ["selectPage"]);

    // Mock the paginated result service for 'operator1'
    workflowResultServiceSpy.getPaginatedResultService.and.callFake(operatorId => {
      if (operatorId === "operator1") {
        return paginatedResultServiceSpy;
      }
      return undefined;
    });
    workflowResultServiceSpy.getResultService.and.returnValue(undefined);

    // Mock paginated results for multiple pages
    const paginatedResults: PaginatedResultEvent[] = [
      {
        requestID: "request1",
        operatorID: "operator1",
        pageIndex: 0,
        table: Array.from({ length: 10 }, (_, i) => ({ column1: `value${i}`, column2: `value${i}` })),
      },
      {
        requestID: "request1",
        operatorID: "operator1",
        pageIndex: 1,
        table: Array.from({ length: 10 }, (_, i) => ({ column1: `value${i + 10}`, column2: `value${i + 10}` })),
      },
      {
        requestID: "request1",
        operatorID: "operator1",
        pageIndex: 2,
        table: [{ column1: "value20", column2: "value20" }],
      },
    ];

    paginatedResultServiceSpy.selectPage.and.callFake((page: number, size: any) => {
      const index = page - 1;
      if (index < paginatedResults.length) {
        return of(paginatedResults[index]);
      } else {
        return EMPTY;
      }
    });

    // Act
    service.exportOperatorsResultAsFile();

    // Simulate asynchronous operations
    tick();

    // Assert
    expect(paginatedResultServiceSpy.selectPage.calls.count()).toBe(3);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalled();
    const args = fileSaverServiceSpy.saveAs.calls.mostRecent().args;
    expect(args[1]).toBe("result_operator1.csv");
  }));

  it("should export a single visualization result as an HTML file when there is only one result", done => {
    // Arrange
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator2"]);

    const resultServiceSpy = jasmine.createSpyObj("OperatorResultService", ["getCurrentResultSnapshot"]);

    // Mock the result service for 'operator2'
    workflowResultServiceSpy.getResultService.and.callFake(operatorId => {
      if (operatorId === "operator2") {
        return resultServiceSpy;
      }
      return undefined;
    });
    workflowResultServiceSpy.getPaginatedResultService.and.returnValue(undefined);

    // Mock the result snapshot with one result
    const resultSnapshot = [{ "html-content": "<html><body><p>Visualization</p></body></html>" }];

    resultServiceSpy.getCurrentResultSnapshot.and.returnValue(resultSnapshot);

    // Spy on the 'saveAs' method and capture the arguments when it's called
    fileSaverServiceSpy.saveAs.and.callFake((blob: Blob, filename: string) => {
      expect(filename).toBe("result_operator2_1.html");

      const reader = new FileReader();
      reader.onload = () => {
        const content = reader.result as string;
        expect(content).toBe("<html><body><p>Visualization</p></body></html>");
        done();
      };
      reader.readAsText(blob);
    });

    // Act
    service.exportOperatorsResultAsFile();
  });

  it("should export multiple visualization results as a zip file when there are multiple results", done => {
    // Arrange
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator2"]);

    const resultServiceSpy = jasmine.createSpyObj("OperatorResultService", ["getCurrentResultSnapshot"]);

    // Mock the result service for 'operator2'
    workflowResultServiceSpy.getResultService.and.callFake(operatorId => {
      if (operatorId === "operator2") {
        return resultServiceSpy;
      }
      return undefined;
    });
    workflowResultServiceSpy.getPaginatedResultService.and.returnValue(undefined);

    // Mock the result snapshot with multiple results
    const resultSnapshot = [
      { "html-content": "<html><body><p>Visualization 1</p></body></html>" },
      { "html-content": "<html><body><p>Visualization 2</p></body></html>" },
    ];

    resultServiceSpy.getCurrentResultSnapshot.and.returnValue(resultSnapshot);

    // Spy on the 'saveAs' method and capture the arguments when it's called
    fileSaverServiceSpy.saveAs.and.callFake((blob: Blob, filename: string) => {
      expect(filename).toBe("results_workflow1_Test Workflow.zip");

      JSZip.loadAsync(blob).then(zip => {
        expect(Object.keys(zip.files)).toContain("result_operator2_1.html");
        expect(Object.keys(zip.files)).toContain("result_operator2_2.html");

        Promise.all([
          zip.file("result_operator2_1.html")?.async("string"),
          zip.file("result_operator2_2.html")?.async("string"),
        ]).then(contents => {
          expect(contents[0]).toBe("<html><body><p>Visualization 1</p></body></html>");
          expect(contents[1]).toBe("<html><body><p>Visualization 2</p></body></html>");
          done();
        });
      });
    });

    // Act
    service.exportOperatorsResultAsFile();
  });
});
