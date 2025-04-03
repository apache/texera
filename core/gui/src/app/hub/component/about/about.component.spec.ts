import { ComponentFixture, TestBed } from "@angular/core/testing";

import { AboutComponent } from "./about.component";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { AdminUserService } from "../../../dashboard/service/admin/user/admin-user.service";
import { AuthService } from "../../../common/service/user/auth.service";

describe("AboutComponent", () => {
  let component: AboutComponent;
  let fixture: ComponentFixture<AboutComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AboutComponent],
      providers: [{ provide: UserService, useClass: StubUserService }],
    });
    fixture = TestBed.createComponent(AboutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
