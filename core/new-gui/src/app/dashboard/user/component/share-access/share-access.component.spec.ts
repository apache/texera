import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { ShareAccessService } from "../../service/share-access/share-access.service";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { ShareAccessComponent } from "./share-access.component";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";

describe("NgbdModalShareAccessComponent", () => {
  let component: ShareAccessComponent;
  let fixture: ComponentFixture<ShareAccessComponent>;

  beforeEach(waitForAsync(async () => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, FormsModule],
      declarations: [ShareAccessComponent],
      providers: [
        { provide: UserService, useClass: StubUserService },
        NgbActiveModal,
        HttpClient,
        HttpHandler,
        {
          provide: ShareAccessService,
        },
      ],
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ShareAccessComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
