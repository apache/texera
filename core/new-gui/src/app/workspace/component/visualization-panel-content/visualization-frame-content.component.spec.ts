import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { JointUIService } from "../../service/joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { VisualizationFrameContentComponent } from "./visualization-frame-content.component";
import { OperatorResultService, WorkflowResultService } from "../../service/workflow-result/workflow-result.service";

describe("VisualizationFrameContentComponent", () => {
  let component: VisualizationFrameContentComponent;
  let fixture: ComponentFixture<VisualizationFrameContentComponent>;
  let workflowResultService: WorkflowResultService;
  const operatorID = "operator1";
  let operatorResultService: OperatorResultService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [VisualizationFrameContentComponent],
      providers: [
        JointUIService,
        WorkflowUtilService,
        UndoRedoService,
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        WorkflowStatusService,
        ExecuteWorkflowService,
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationFrameContentComponent);
    component = fixture.componentInstance;
    component.operatorId = operatorID;
    workflowResultService = TestBed.get(WorkflowResultService);
    operatorResultService = new OperatorResultService(operatorID);
    spyOn(workflowResultService, "getResultService").and.returnValue(operatorResultService);
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
