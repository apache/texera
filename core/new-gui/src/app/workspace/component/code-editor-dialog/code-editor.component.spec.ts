import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { CodeEditorComponent } from "./code-editor.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";

describe("CodeEditorDialogComponent", () => {
  let component: CodeEditorComponent;
  let fixture: ComponentFixture<CodeEditorComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [CodeEditorComponent],
      providers: [
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // it("should create a websocket when the editor is opened", () => {
  //   let socketInstance = component.getLanguageServerSocket();
  //   expect(socketInstance).toBeTruthy();
  // });
});
