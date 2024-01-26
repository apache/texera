import { ComponentFixture, TestBed } from "@angular/core/testing";
import { LeftPanelComponent } from "./left-panel.component";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { VersionsListComponent } from "./versions-list/versions-list.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";

describe("LeftPanelComponent", () => {
  let component: LeftPanelComponent;

  let workflowActionService: WorkflowActionService;
  let fixture: ComponentFixture<LeftPanelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      declarations: [LeftPanelComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LeftPanelComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should switch to versions frame component when get all versions is clicked", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    //the operator shall be highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(1);

    // click on versions display
    component.openVersionsFrame();
    fixture.detectChanges();

    // all the elements shall be un-highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length).toBe(0);

    // the component should switch to versions display
    expect(component.currentComponent).toBe(VersionsListComponent);
  });
});
