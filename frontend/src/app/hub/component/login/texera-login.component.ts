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
import { NgIf } from "@angular/common";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError } from "rxjs/operators";
import { throwError } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SocialAuthService, GoogleSigninButtonModule, FacebookLoginProvider } from "@abacritt/angularx-social-login";
import { UserService } from "../../../common/service/user/user.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";

type LoginMode = "signin" | "signup";

/**
 * Full-page Texera login card: local sign-in / sign-up (tabbed) plus Google sign-in.
 * Splits the single-file `Texera Login.html` prototype into the standard 3-way Angular
 * component (ts / html / scss), mirroring the AboutComponent layout and reusing the same
 * auth wiring as LocalLoginComponent + DashboardComponent (UserService + SocialAuthService).
 */
@UntilDestroy()
@Component({
  selector: "texera-login",
  templateUrl: "./texera-login.component.html",
  styleUrls: ["./texera-login.component.scss"],
  imports: [NgIf, ReactiveFormsModule, GoogleSigninButtonModule],
})
export class TexeraLoginComponent implements OnInit {
  public mode: LoginMode = "signin";
  public passwordVisible = false;
  public rememberMe = false; // UI-only: the backend session lifetime is driven by the JWT expiry
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
      password: new FormControl("", [Validators.required, Validators.minLength(6)]),
      confirm: new FormControl("", [this.confirmationValidator]),
    });
  }

  ngOnInit(): void {
    // Prefill the local dev credentials when configured, matching LocalLoginComponent.
    if (this.config.env.defaultLocalUser && Object.keys(this.config.env.defaultLocalUser).length > 0) {
      this.form.patchValue({
        username: this.config.env.defaultLocalUser.username,
        password: this.config.env.defaultLocalUser.password,
      });
    }

    // Social sign-in: both Google and Facebook emit here after their sign-in flow.
    // Branch on the provider — Google yields an idToken, Facebook an authToken.
    this.socialAuthService.authState.pipe(untilDestroyed(this)).subscribe(user => {
      const isFacebook = user.provider === FacebookLoginProvider.PROVIDER_ID;
      const login$ = isFacebook
        ? this.userService.facebookLogin(user.authToken)
        : this.userService.googleLogin(user.idToken);
      login$
        .pipe(
          catchError((e: unknown) => {
            this.notificationService.error(
              (e as Error)?.message || `${isFacebook ? "Facebook" : "Google"} sign-in failed`
            );
            return throwError(() => e);
          }),
          untilDestroyed(this)
        )
        .subscribe(() => this.ngZone.run(() => this.navigateAfterLogin()));
    });
  }

  /**
   * Facebook has no rendered-button component in the social-login library, so this is wired
   * to a custom button and triggers the flow programmatically. The resulting credential
   * arrives via socialAuthService.authState (handled in ngOnInit).
   */
  public facebookLogin(): void {
    this.socialAuthService.signIn(FacebookLoginProvider.PROVIDER_ID).catch((e: unknown) => {
      this.notificationService.error((e as Error)?.message || "Facebook sign-in failed");
    });
  }

  public setMode(mode: LoginMode): void {
    this.mode = mode;
    this.errorMessage = undefined;
    // Re-evaluate the confirm-password validator, which only applies in sign-up mode.
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
    const password = this.form.get("password")?.value;
    const confirm = this.form.get("confirm")?.value;

    const validation = UserService.validateUsername(username);
    if (!validation.result) {
      this.errorMessage = validation.message;
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
      .register(username, password)
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
