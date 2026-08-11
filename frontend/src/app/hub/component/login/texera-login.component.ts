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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SocialAuthService, GoogleSigninButtonModule, SocialUser } from "@abacritt/angularx-social-login";
import { UserService } from "../../../common/service/user/user.service";
import { AppleAuthService } from "../../../common/service/user/apple-auth.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabComponent, NzTabsComponent } from "ng-zorro-antd/tabs";
import { NzInputDirective, NzInputGroupComponent, NzInputGroupWhitSuffixOrPrefixDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { NzTypographyComponent } from "ng-zorro-antd/typography";

type LoginMode = "signin" | "signup";

/**
 * Full-page login card: tabbed local sign-in / sign-up plus Google and Apple sign-in.
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
  // Guards against a second click while Apple's popup is already open.
  public appleSignInPending = false;
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
    private appleAuthService: AppleAuthService,
    protected config: GuiConfigService
  ) {
    this.form = this.formBuilder.group({
      username: new FormControl("", [Validators.required]),
      // Registration requires an email; sign-in does not, so the control is only
      // validated and submitted in sign-up mode.
      email: new FormControl("", [Validators.email]),
      password: new FormControl("", [Validators.required, Validators.minLength(6)]),
      confirm: new FormControl("", [this.confirmationValidator]),
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

  /**
   * Apple has no Angular button component (`@abacritt/angularx-social-login` ships no Apple
   * provider), so unlike Google this flow is started by a click rather than pushed through
   * `socialAuthService.authState`. A dismissed popup yields no token and is not an error.
   */
  public async signInWithApple(): Promise<void> {
    this.appleSignInPending = true;
    try {
      const idToken = await this.appleAuthService.signIn();
      if (!idToken) return;

      this.userService
        .appleLogin(idToken)
        .pipe(
          catchError((e: unknown) => {
            this.notificationService.error((e as Error)?.message || "Apple sign-in failed");
            return throwError(() => e);
          }),
          untilDestroyed(this)
        )
        .subscribe(() => this.ngZone.run(() => this.navigateAfterLogin()));
    } catch (e) {
      this.notificationService.error((e as Error)?.message || "Apple sign-in failed");
    } finally {
      this.appleSignInPending = false;
    }
  }

  public setMode(mode: LoginMode): void {
    this.mode = mode;
    this.errorMessage = undefined;
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

  private register(): void {
    this.errorMessage = undefined;
    const username = this.form.get("username")?.value?.trim();
    const email = (this.form.get("email")?.value ?? "").trim();
    const password = this.form.get("password")?.value;
    const confirm = this.form.get("confirm")?.value;

    const usernameValidation = UserService.validateUsername(username);
    if (!usernameValidation.result) {
      this.errorMessage = usernameValidation.message;
      return;
    }
    const emailValidation = UserService.validateEmail(email);
    if (!emailValidation.result) {
      this.errorMessage = emailValidation.message;
      return;
    }
    if (!password || password.length < 6) {
      this.errorMessage = "Password length should be greater than 5.";
      return;
    }
    if (password !== confirm) {
      this.errorMessage = "Two passwords are inconsistent.";
      return;
    }

    this.userService
      .register(username, email, password)
      .pipe(
        catchError((e: unknown) => {
          this.errorMessage = (e as Error)?.message || "Registration failed";
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(() =>
        this.notificationService.success(
          "Your account has been created. Please contact the Texera administrator to activate your account."
        )
      );
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
