import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserQuotaComponent } from "./user-quota.component";
import { UserQuotaService } from "../../../service/user/quota/user-quota.service";

describe("UserQuotaComponent", () => {
  let component: UserQuotaComponent;
  let fixture: ComponentFixture<UserQuotaComponent>;
  let userQuotaService: jasmine.SpyObj<UserQuotaService>;

  beforeEach(() => {
    const userQuotaServiceSpy = jasmine.createSpyObj("UserQuotaService", [
      "getUploadedFiles",
      "getCreatedDatasets",
      "getCreatedWorkflows",
      "getAccessFiles",
      "getAccessWorkflows",
      "getMongoDBs",
    ]);

    TestBed.configureTestingModule({
      declarations: [UserQuotaComponent],
      providers: [{ provide: UserQuotaService, useValue: userQuotaServiceSpy }],
    });

    fixture = TestBed.createComponent(UserQuotaComponent);
    component = fixture.componentInstance;
    userQuotaService = TestBed.inject(UserQuotaService) as jasmine.SpyObj<UserQuotaService>;
  });
});
