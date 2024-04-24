import { WorkflowUtilService } from "../../../../service/workflow-graph/util/workflow-util.service";
import { JointUIService } from "../../../../service/joint-ui/joint-ui.service";
import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { OperatorMetadataService } from "../../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../../service/operator-metadata/stub-operator-metadata.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../../../service/undo-redo/undo-redo.service";
import { RouterTestingModule } from "@angular/router/testing";
import { RecursiveCollapseComponent } from "./recursive-collapse.component";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";

describe("RecursiveCollapseComponent", () => {
  let component: RecursiveCollapseComponent;
  let fixture: ComponentFixture<RecursiveCollapseComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [RecursiveCollapseComponent],
      imports: [
        RouterTestingModule.withRoutes([]),
        NzCollapseModule,
        NzDropDownModule,
        NzToolTipModule,
        BrowserAnimationsModule,
      ],
      providers: [
        DragDropService,
        JointUIService,
        WorkflowUtilService,
        WorkflowActionService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RecursiveCollapseComponent);
    component = fixture.componentInstance;
    // Initialize component's @Input()
    component.operators = [{ name: "Operator 1", operator: [{ name: "Sub Operator 1" }] }, { name: "Operator 2" }];
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
