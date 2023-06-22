import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { HomeComponent } from "./home.component";
import { UserService } from "../../common/service/user/user.service";
import { StubUserService } from "../../common/service/user/stub-user.service";
import { GoogleLoginService } from "../service/google-login.service";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("HomeComponent", () => {
  let component: HomeComponent;
  let fixture: ComponentFixture<HomeComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [HomeComponent],
      providers: [{ provide: UserService, useClass: StubUserService }, GoogleLoginService],
      imports: [HttpClientTestingModule, RouterTestingModule.withRoutes([{ path: "home", component: HomeComponent }])],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
