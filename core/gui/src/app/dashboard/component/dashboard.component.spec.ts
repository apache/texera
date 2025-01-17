import { ComponentFixture, TestBed } from "@angular/core/testing";
import { DashboardComponent } from "./dashboard.component";
import { UserService } from "../../common/service/user/user.service";
import { SocialAuthService } from "@abacritt/angularx-social-login";
import { FlarumService } from "../service/user/flarum/flarum.service";
import { Router, ActivatedRoute } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of, BehaviorSubject } from "rxjs";
import { CUSTOM_ELEMENTS_SCHEMA } from "@angular/core";
import {DASHBOARD_USER_WORKFLOW} from "../../app-routing.constant";

describe("DashboardComponent", () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let userServiceMock: jasmine.SpyObj<UserService>;
  let socialAuthServiceMock: jasmine.SpyObj<SocialAuthService>;
  let flarumServiceMock: jasmine.SpyObj<FlarumService>;
  let routerMock: jasmine.SpyObj<Router>;
  let activatedRouteMock: any;
  let authStateMock: BehaviorSubject<any>;

  beforeEach(async () => {
    userServiceMock = jasmine.createSpyObj("UserService", ["isLogin", "isAdmin", "userChanged", "googleLogin"]);
    userServiceMock.isLogin.and.returnValue(false);
    userServiceMock.isAdmin.and.returnValue(false);
    userServiceMock.userChanged.and.returnValue(of(undefined));
    userServiceMock.googleLogin.and.returnValue(of(undefined));

    authStateMock = new BehaviorSubject(null);
    socialAuthServiceMock = jasmine.createSpyObj("SocialAuthService", [], {
      authState: authStateMock.asObservable(),
    });

    flarumServiceMock = jasmine.createSpyObj("FlarumService", ["auth", "register"]);
    flarumServiceMock.auth.and.returnValue(of({ token: "fake_token" }));
    flarumServiceMock.register.and.returnValue(of());

    routerMock = jasmine.createSpyObj("Router", ["navigateByUrl"]);

    activatedRouteMock = {
      snapshot: {
        queryParams: {},
      },
    };

    await TestBed.configureTestingModule({
      imports: [
        RouterTestingModule.withRoutes([]), // 确保 Router 正常工作
        HttpClientTestingModule,
      ],
      declarations: [DashboardComponent],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: SocialAuthService, useValue: socialAuthServiceMock },
        { provide: FlarumService, useValue: flarumServiceMock },
        { provide: Router, useValue: routerMock },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
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
