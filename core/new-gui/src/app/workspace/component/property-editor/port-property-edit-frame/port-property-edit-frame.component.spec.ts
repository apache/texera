import { ComponentFixture, TestBed } from "@angular/core/testing";

import { PortPropertyEditFrameComponent } from "./port-property-edit-frame.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("PortPropertyEditFrameComponent", () => {
  let component: PortPropertyEditFrameComponent;
  let fixture: ComponentFixture<PortPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [PortPropertyEditFrameComponent],
      providers: [WorkflowActionService],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PortPropertyEditFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
