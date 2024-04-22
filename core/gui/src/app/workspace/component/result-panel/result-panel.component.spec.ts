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
    const resultPanelDiv = fixture.debugElement.query(By.css(".texera-workspace-result-panel-body"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;
    expect(resultPanelHtmlElement).toBeTruthy();
  });
});
