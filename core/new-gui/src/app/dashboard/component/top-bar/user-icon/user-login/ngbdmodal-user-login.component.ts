import { Component, OnInit } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { UserService } from "../../../../../common/service/user/user.service";
import { FormBuilder, FormControl, FormGroup, Validators } from "@angular/forms";
import { isDefined } from "../../../../../common/util/predicate";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

/**
 * NgbdModalUserLoginComponent is the pop up for user login/registration
 *
 * @author Adam
 */
@UntilDestroy()
@Component({
  selector: "texera-ngbdmodal-user-login",
  templateUrl: "./ngbdmodal-user-login.component.html",
  styleUrls: ["./ngbdmodal-user-login.component.scss"],
})
export class NgbdModalUserLoginComponent implements OnInit {
  public selectedTab = 0;
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;
  public allForms: FormGroup;

  constructor(private formBuilder: FormBuilder, public activeModal: NgbActiveModal, private userService: UserService) {
    this.allForms = this.formBuilder.group({
      loginUsername: new FormControl("", [Validators.required]),
      registerUsername: new FormControl("", [Validators.required]),
      loginPassword: new FormControl("", [Validators.required]),
      registerPassword: new FormControl("", [Validators.required]),
      registerConfirmationPassword: new FormControl("", [Validators.required]),
    });
  }

  ngOnInit() {
    this.detectUserChange();
  }

  public errorMessageUsernameNull(): string {
    return "Username required";
  }

  public errorMessagePasswordNull(): string {
    return this.allForms.controls["registerPassword"].hasError("required")
      ? "Password required"
      : this.allForms.controls["registerConfirmationPassword"].hasError("required")
      ? "Confirmation required"
      : this.allForms.controls["loginPassword"].hasError("required")
      ? "Password required"
      : "";
  }

  /**
   * This method is respond for the sign in button in the pop up
   * It will send data inside the text entry to the user service to login
   */
  public login(): void {
    // validate the credentials format
    this.loginErrorMessage = undefined;
    const validation = UserService.validateUsername(this.allForms.get("loginUsername")?.value);
    if (!validation.result) {
      this.loginErrorMessage = validation.message;
      return;
    }

    const username = this.allForms.get("loginUsername")?.value.trim();
    const password = this.allForms.get("loginPassword")?.value;

    this.userService
      .login(username, password)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.activeModal.close(),
        () => (this.loginErrorMessage = "Incorrect credentials")
      );
  }

  /**
   * This method is respond for the sign on button in the pop up
   * It will send data inside the text entry to the user service to register
   */
  public register(): void {
    // validate the credentials format
    this.registerErrorMessage = undefined;
    const registerPassword = this.allForms.get("registerPassword")?.value;
    const registerConfirmationPassword = this.allForms.get("registerConfirmationPassword")?.value;
    const registerUsername = this.allForms.get("registerUsername")?.value.trim();
    const validation = UserService.validateUsername(registerUsername);
    if (registerPassword.length < 6) {
      this.registerErrorMessage = "Password length should be greater than 5";
      return;
    }
    if (registerPassword !== registerConfirmationPassword) {
      this.registerErrorMessage = "Passwords do not match";
      return;
    }
    if (!validation.result) {
      this.registerErrorMessage = validation.message;
      return;
    }
    // register the credentials with backend
    this.userService
      .register(registerUsername, registerPassword)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.activeModal.close(),
        () => (this.registerErrorMessage = "Registration failed. Could due to duplicate username.")
      );
  }

  /**
   * this method will retrieve a usable Google OAuth Instance first,
   * with that available instance, get googleUsername and authorization code respectively,
   * then sending the code to the backend
   */
  public googleLogin(): void {
    this.userService
      .googleLogin()
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.activeModal.close(),
        () => (this.loginErrorMessage = "Incorrect credentials")
      );
  }

  /**
   * this method will handle the pop up when user successfully login
   */
  private detectUserChange(): void {
    // TODO temporary solution, need improvement
    this.userService
      .userChanged()
      .pipe(filter(isDefined))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.activeModal.close();
      });
  }
}
