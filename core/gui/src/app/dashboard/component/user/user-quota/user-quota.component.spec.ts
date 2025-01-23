import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserQuotaComponent } from "./user-quota.component";
import { UserQuotaService } from "../../../service/user/quota/user-quota.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("UserQuotaComponent", () => {
  let component: UserQuotaComponent;
  let fixture: ComponentFixture<UserQuotaComponent>;
  let mockUserQuotaService: jasmine.SpyObj<UserQuotaService>;

  beforeEach(() => {
    mockUserQuotaService = jasmine.createSpyObj("UserQuotaService", [
      "getUploadedFiles",
      "getCreatedDatasets",
      "getCreatedWorkflows",
      "getAccessFiles",
      "getAccessWorkflows",
      "getMongoDBs",
    ]);

    TestBed.configureTestingModule({
      declarations: [UserQuotaComponent],
      providers: [{ provide: UserQuotaService, useValue: mockUserQuotaService }],
      imports: [HttpClientTestingModule],
    });

    fixture = TestBed.createComponent(UserQuotaComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
