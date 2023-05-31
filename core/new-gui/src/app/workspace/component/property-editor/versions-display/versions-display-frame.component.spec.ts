import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { VersionsDisplayFrameComponent } from "./versions-display-frame.component";

describe("VersionsListDisplayComponent", () => {
  let component: VersionsDisplayFrameComponent;
  let fixture: ComponentFixture<VersionsDisplayFrameComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [VersionsDisplayFrameComponent],
      providers: [WorkflowActionService],
      imports: [BrowserAnimationsModule, FormsModule, FormlyModule.forRoot(TEXERA_FORMLY_CONFIG), ReactiveFormsModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VersionsDisplayFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
