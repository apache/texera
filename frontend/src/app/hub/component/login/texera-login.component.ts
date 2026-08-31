/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, NgZone, OnInit } from "@angular/core";
import {
  AbstractControl,
  FormBuilder,
  FormControl,
  FormGroup,
  ReactiveFormsModule,
  ValidationErrors,
  Validators,
} from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError, filter } from "rxjs/operators";
import { throwError } from "rxjs";
import { HttpErrorResponse } from "@angular/common/http";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SocialAuthService, GoogleSigninButtonModule, SocialUser } from "@abacritt/angularx-social-login";
import { UserService } from "../../../common/service/user/user.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabComponent, NzTabsComponent } from "ng-zorro-antd/tabs";
import { NzInputDirective, NzInputGroupComponent, NzInputGroupWhitSuffixOrPrefixDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { NzTypographyComponent } from "ng-zorro-antd/typography";

/**
 * The reason a failed call carries, preferring the server's own words.
 *
 * An `HttpErrorResponse` puts the backend's JSON body on `.error`, while its `.message` is
 * Angular's generic "Http failure response for <url>: 503 ...". Operator-facing refusals — an
 * enabled verification flow with no SMTP sender behind it, say — are only useful if the body's
 * message is what reaches the screen.
 */
function reasonFor(e: unknown): string | undefined {
  return (e as HttpErrorResponse)?.error?.message ?? (e as Error)?.message;
}

/** Reported by every path that ends with an account existing. */
const ACCOUNT_CREATED =
  "Your account has been created. Please contact the Texera administrator to activate your account.";

type LoginMode = "signin" | "signup";

/**
 * Full-page login card: tabbed local sign-in / sign-up plus Google sign-in.
 *
 * This is the single login surface. It replaces the `texera-local-login` form that used to be
 * embedded in the About page and the standalone Google button that sat on the dashboard shell,
 * so `auth-guard` and the 401 interceptor now both redirect here. Adding another identity
 * provider means one more button in the `social-buttons` block, not another login surface.
 */
@UntilDestroy()
@Component({
  selector: "texera-login",
  templateUrl: "./texera-login.component.html",
  styleUrls: ["./texera-login.component.scss"],
  imports: [
    ReactiveFormsModule,
    GoogleSigninButtonModule,
    NzIconDirective,
    NzTabsComponent,
    NzTabComponent,
    NzInputGroupComponent,
    NzInputGroupWhitSuffixOrPrefixDirective,
    NzInputDirective,
    NzButtonComponent,
    NzDividerComponent,
    NzTypographyComponent,
  ],
})
export class TexeraLoginComponent implements OnInit {
  public mode: LoginMode = "signin";
  public passwordVisible = false;
  public errorMessage: string | undefined;

  public form: FormGroup;

  constructor(
    private formBuilder: FormBuilder,
    private userService: UserService,
    private notificationService: NotificationService,
    private route: ActivatedRoute,
    private router: Router,
    private ngZone: NgZone,
    private socialAuthService: SocialAuthService,
    protected config: GuiConfigService
  ) {
    this.form = this.formBuilder.group({
      username: new FormControl("", [Validators.required]),
      // Registration requires an email; sign-in does not, so the control is only
      // validated and submitted in sign-up mode.
      email: new FormControl("", [Validators.email]),
      password: new FormControl("", [Validators.required, Validators.minLength(6)]),
      confirm: new FormControl("", [this.confirmationValidator]),
      // Carries the mailed code. Only collected, and only required, where
      // `user-sys.email-verification` is on.
      code: new FormControl(""),
    });
  }

  ngOnInit(): void {
    // Nothing on this page is useful to someone already signed in, so send them straight on.
    if (this.userService.isLogin()) {
      this.navigateAfterLogin();
      return;
    }

    // Prefill the configured local dev credentials, as the previous login form did.
    if (this.config.env.defaultLocalUser && Object.keys(this.config.env.defaultLocalUser).length > 0) {
      this.form.patchValue({
        username: this.config.env.defaultLocalUser.username,
        password: this.config.env.defaultLocalUser.password,
      });
    }

    // Google emits the signed-in user here after its own button completes the flow.
    // The null filter matters: logging out pushes null through this subject, and it is a
    // ReplaySubject, so that stale null is replayed into this subscription the moment it starts.
    this.socialAuthService.authState
      .pipe(
        filter((user): user is SocialUser => user != null),
        untilDestroyed(this)
      )
      .subscribe(user => {
        this.userService
          .googleLogin(user.idToken)
          .pipe(
            catchError((e: unknown) => {
              this.notificationService.error((e as Error)?.message || "Google sign-in failed");
              return throwError(() => e);
            }),
            untilDestroyed(this)
          )
          .subscribe(() => this.ngZone.run(() => this.navigateAfterLogin()));
      });
  }

  public setMode(mode: LoginMode): void {
    this.mode = mode;
    this.errorMessage = undefined;
    // Switching tabs abandons a half-finished signup; the code that was mailed expires on its own.
    this.form.controls.code.setValue("");
    // The confirm-password rule only applies in sign-up mode, so re-evaluate it on every switch.
    this.form.controls.confirm.updateValueAndValidity();
  }

  public togglePasswordVisibility(): void {
    this.passwordVisible = !this.passwordVisible;
  }

  public submit(): void {
    if (this.mode === "signin") {
      this.login();
    } else {
      this.register();
    }
  }

  private login(): void {
    this.errorMessage = undefined;
    const username = this.form.get("username")?.value?.trim();
    const password = this.form.get("password")?.value;

    const validation = UserService.validateUsername(username);
    if (!validation.result) {
      this.errorMessage = validation.message;
      return;
    }
    if (!password || password.length < 6) {
      this.errorMessage = "Password length should be greater than 5.";
      return;
    }

    this.userService
      .login(username, password)
      .pipe(
        catchError((e: unknown) => {
          this.errorMessage = (e as Error)?.message || "Incorrect username or password";
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(() => this.navigateAfterLogin());
  }

  /**
   * Mail a verification code to the address in the form. Nothing is created: the account does not
   * exist until [[register]] presents the code.
   *
   * Its own button rather than a step of signing up, because it puts mail in an inbox. The address
   * belongs to whoever owns it and not necessarily to the person typing it, so sending has to be
   * something the user visibly asks for instead of a side effect of pressing Sign up.
   */
  public sendCode(): void {
    this.errorMessage = undefined;
    const fields = this.validatedFields();
    if (!fields) {
      return;
    }

    this.userService
      .register(fields.username, fields.email, fields.password)
      .pipe(
        catchError((e: unknown) => {
          this.errorMessage = reasonFor(e) || "That code could not be sent.";
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(({ verificationRequired }) => {
        // The button is only rendered where the config says verification is on. If the server
        // disagrees, `register` has just created the account outright and there is no code coming.
        this.notificationService.success(
          verificationRequired ? `A verification code has been sent to ${fields.email}.` : ACCOUNT_CREATED
        );
      });
  }

  private register(): void {
    this.errorMessage = undefined;
    const fields = this.validatedFields();
    if (!fields) {
      return;
    }
    if (fields.password !== this.form.get("confirm")?.value) {
      this.errorMessage = "Two passwords are inconsistent.";
      return;
    }

    // Where verification is on the account does not exist yet, so this submit is the second half of
    // the signup: the same fields go back with the code that [[sendCode]] had mailed.
    if (this.config.env.emailVerification) {
      const code = (this.form.get("code")?.value ?? "").trim();
      if (!code) {
        this.errorMessage = "Enter the code that was emailed to you.";
        return;
      }
      this.userService
        .registerVerify(fields.username, fields.email, fields.password, code)
        .pipe(
          catchError((e: unknown) => {
            this.errorMessage = reasonFor(e) || "That code is not valid or has expired.";
            return throwError(() => e);
          }),
          untilDestroyed(this)
        )
        .subscribe(() => this.notificationService.success(ACCOUNT_CREATED));
      return;
    }

    this.userService
      .register(fields.username, fields.email, fields.password)
      .pipe(
        catchError((e: unknown) => {
          this.errorMessage = reasonFor(e) || "Registration failed";
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(() => this.notificationService.success(ACCOUNT_CREATED));
  }

  /**
   * Trim and check the three fields a registration call needs, reporting the first problem.
   * Returns null once something has been reported. The confirm-password match is not checked here:
   * it guards a submit, and has nothing to say about whether a code can be sent.
   */
  private validatedFields(): { username: string; email: string; password: string } | null {
    const username = this.form.get("username")?.value?.trim();
    const email = (this.form.get("email")?.value ?? "").trim();
    const password = this.form.get("password")?.value;

    const usernameValidation = UserService.validateUsername(username);
    if (!usernameValidation.result) {
      this.errorMessage = usernameValidation.message;
      return null;
    }
    const emailValidation = UserService.validateEmail(email);
    if (!emailValidation.result) {
      this.errorMessage = emailValidation.message;
      return null;
    }
    if (!password || password.length < 6) {
      this.errorMessage = "Password length should be greater than 5.";
      return null;
    }
    return { username, email, password };
  }

  private navigateAfterLogin(): void {
    this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || USER_WORKFLOW);
  }

  // Confirm-password matches password; only enforced in sign-up mode.
  private confirmationValidator = (control: AbstractControl): ValidationErrors | null => {
    if (this.mode === "signup" && this.form && control.value !== this.form.controls.password.value) {
      return { confirm: true };
    }
    return null;
  };
}
