import { ComponentFixture, TestBed } from "@angular/core/testing";
import { GoogleLoginComponent } from "./google-login.component";
import { NO_ERRORS_SCHEMA } from "@angular/core";

describe("GoogleLoginComponent", () => {
  let component: GoogleLoginComponent;
  let fixture: ComponentFixture<GoogleLoginComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [GoogleLoginComponent],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(GoogleLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
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
