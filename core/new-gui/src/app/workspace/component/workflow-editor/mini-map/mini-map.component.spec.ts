import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { MiniMapComponent } from "./mini-map.component";
import { WorkflowEditorComponent } from "../workflow-editor.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";

describe("MiniMapComponent", () => {
  let fixture: ComponentFixture<MiniMapComponent>;
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [MiniMapComponent, WorkflowEditorComponent],
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(fixture.componentInstance).toBeTruthy();
  });
});
