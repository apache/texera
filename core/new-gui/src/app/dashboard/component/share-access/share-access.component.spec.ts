import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { AccessService } from "../../service/workflow-access/access.service";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { ShareAccessComponent } from "./ngbd-modal-workflow-share-access.component";
import { StubWorkflowAccessService } from "../../service/workflow-access/stub-workflow-access.service";

describe("NgbdModalShareAccessComponent", () => {
  let component: ShareAccessComponent;
  let fixture: ComponentFixture<ShareAccessComponent>;
  let service: StubWorkflowAccessService;

  beforeEach(waitForAsync(async () => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, FormsModule],
      declarations: [ShareAccessComponent],
      providers: [
        NgbActiveModal,
        HttpClient,
        HttpHandler,
        {
          provide: AccessService,
          useClass: StubWorkflowAccessService,
        },
      ],
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ShareAccessComponent);
    component = fixture.componentInstance;
    service = TestBed.get(AccessService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
