import { ComponentFixture, TestBed } from "@angular/core/testing";
import { GoogleLoginComponent } from "./google-login.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { UserService } from "../../../../common/service/user/user.service";
import { SocialAuthService } from "@abacritt/angularx-social-login";
import { of } from "rxjs";

describe("GoogleLoginComponent", () => {
  let component: GoogleLoginComponent;
  let fixture: ComponentFixture<GoogleLoginComponent>;
  let userService: jasmine.SpyObj<UserService>;
  let socialAuthService: jasmine.SpyObj<SocialAuthService>;

  beforeEach(async () => {
    const userServiceSpy = jasmine.createSpyObj("UserService", ["googleLogin"]);
    const socialAuthServiceSpy = jasmine.createSpyObj("SocialAuthService", [], {
      authState: of({ idToken: "mock-id-token" }),
    });

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule],
      declarations: [GoogleLoginComponent],
      providers: [
        { provide: UserService, useValue: userServiceSpy },
        { provide: SocialAuthService, useValue: socialAuthServiceSpy },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(GoogleLoginComponent);
    component = fixture.componentInstance;
    userService = TestBed.inject(UserService) as jasmine.SpyObj<UserService>;
    socialAuthService = TestBed.inject(SocialAuthService) as jasmine.SpyObj<SocialAuthService>;
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should render Google Sign-In button", () => {
    const compiled = fixture.nativeElement as HTMLElement;
    const button = compiled.querySelector("asl-google-signin-button");
    expect(button).toBeTruthy();
  });
});
