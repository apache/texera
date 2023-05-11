import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { WorkflowAccessService } from "../../../../service/workflow-access/workflow-access.service";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-workflow-share-access.component";
import { StubWorkflowAccessService } from "../../../../service/workflow-access/stub-workflow-access.service";

describe("NgbdModalShareAccessComponent", () => {
  let component: NgbdModalWorkflowShareAccessComponent;
  let fixture: ComponentFixture<NgbdModalWorkflowShareAccessComponent>;
  let service: StubWorkflowAccessService;

  beforeEach(waitForAsync(async () => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, FormsModule],
      declarations: [NgbdModalWorkflowShareAccessComponent],
      providers: [
        NgbActiveModal,
        HttpClient,
        HttpHandler,
        {
          provide: WorkflowAccessService,
          useClass: StubWorkflowAccessService,
        },
      ],
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalWorkflowShareAccessComponent);
    component = fixture.componentInstance;
    service = TestBed.get(WorkflowAccessService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
