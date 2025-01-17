import { ComponentFixture, TestBed } from "@angular/core/testing";
import { DashboardComponent } from "./dashboard.component";
import { UserService } from "../../common/service/user/user.service";
import { SocialAuthService } from "@abacritt/angularx-social-login";
import { Router } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { BehaviorSubject } from "rxjs";
import { CUSTOM_ELEMENTS_SCHEMA } from "@angular/core";
import { DASHBOARD_USER_WORKFLOW } from "../../app-routing.constant";

describe("DashboardComponent", () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let userServiceMock: jasmine.SpyObj<UserService>;
  let socialAuthServiceMock: jasmine.SpyObj<SocialAuthService>;
  let routerMock: jasmine.SpyObj<Router>;
  let authStateMock: BehaviorSubject<any>;

  beforeEach(async () => {
    userServiceMock = jasmine.createSpyObj("UserService", ["isLogin", "isAdmin"]);
    userServiceMock.isLogin.and.returnValue(false);
    userServiceMock.isAdmin.and.returnValue(false);

    authStateMock = new BehaviorSubject(null);
    socialAuthServiceMock = jasmine.createSpyObj("SocialAuthService", [], {
      authState: authStateMock.asObservable(),
    });

    routerMock = jasmine.createSpyObj("Router", ["navigateByUrl"]);

    await TestBed.configureTestingModule({
      imports: [RouterTestingModule],
      declarations: [DashboardComponent],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: SocialAuthService, useValue: socialAuthServiceMock },
        { provide: Router, useValue: routerMock },
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should render Google login button when user is NOT logged in", () => {
    const compiled = fixture.nativeElement;
    const googleSignInButton = compiled.querySelector("asl-google-signin-button");

    expect(googleSignInButton).toBeTruthy();
  });

  it("should NOT render Google login button when user IS logged in", () => {
    userServiceMock.isLogin.and.returnValue(true);
    fixture.detectChanges();

    const compiled = fixture.nativeElement;
    const googleSignInButton = compiled.querySelector("asl-google-signin-button");

    expect(googleSignInButton).toBeFalsy();
  });

  it("should redirect to DASHBOARD_USER_WORKFLOW after Google login", () => {
    authStateMock.next({ idToken: "test_token" } as any);
    fixture.detectChanges();

    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(DASHBOARD_USER_WORKFLOW);
  });
});
