import { mockScanSourceSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { OperatorMenuComponent } from "./operator-menu.component";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { RouterTestingModule } from "@angular/router/testing";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzCollapseModule } from "ng-zorro-antd/collapse";

describe("OperatorPanelComponent", () => {
  let component: OperatorMenuComponent;
  let fixture: ComponentFixture<OperatorMenuComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorMenuComponent, OperatorLabelComponent],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        DragDropService,
        WorkflowActionService,
        UndoRedoService,
        WorkflowUtilService,
        JointUIService,
      ],
      imports: [NzDropDownModule, NzCollapseModule, BrowserAnimationsModule, RouterTestingModule.withRoutes([])],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorMenuComponent);
    fixture.detectChanges();
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should search an operator by its user friendly name", () => {
    component.searchInputValue = "Source: Scan";

    fixture.detectChanges();

    expect(component.autocompleteOptions.length === 1);
    expect(component.autocompleteOptions[0] === mockScanSourceSchema);
  });

  it("should support fuzzy search on operator user friendly name", () => {
    component.searchInputValue = "scan";
    fixture.detectChanges();

    expect(component.autocompleteOptions.length === 1);
    expect(component.autocompleteOptions[0] === mockScanSourceSchema);
  });

  it("should clear the search box when an operator from search box is dropped", () => {
    component.searchInputValue = "scan";
    fixture.detectChanges();

    const dragDropService = TestBed.get(DragDropService);
    dragDropService.operatorDroppedSubject.next({
      operatorType: "ScanSource",
      offset: { x: 1, y: 1 },
      dragElementID: "operator-label-ScanSource",
    });

    fixture.detectChanges();

    expect(component.searchInputValue).toBeFalsy();
  });
});
