import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientModule } from "@angular/common/http";
import { UserAvatarComponent } from "./user-avatar.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";

describe("UserAvatarComponent", () => {
  let component: UserAvatarComponent;
  let fixture: ComponentFixture<UserAvatarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserAvatarComponent],
      imports: [HttpClientModule, HttpClientTestingModule, NzAvatarModule],
      providers: [{ provide: UserService, useClass: StubUserService }],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserAvatarComponent);
    component = fixture.componentInstance;
    component.userName = "fake Texera user";
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
