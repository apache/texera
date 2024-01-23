import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { TimeTravelComponent } from "./time-travel.component";

describe("VersionsListDisplayComponent", () => {
  let component: TimeTravelComponent;
  let fixture: ComponentFixture<TimeTravelComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [TimeTravelComponent],
      providers: [WorkflowActionService],
      imports: [
        BrowserAnimationsModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        ReactiveFormsModule,
        HttpClientTestingModule,
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TimeTravelComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
