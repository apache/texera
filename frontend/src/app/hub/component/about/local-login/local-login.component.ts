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

import { Component, OnInit } from "@angular/core";
import { FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActivatedRoute, Router } from "@angular/router";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { catchError } from "rxjs/operators";
import { throwError } from "rxjs";
import { DASHBOARD_USER_WORKFLOW } from "../../../../app-routing.constant";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { NgIf } from "@angular/common";
import { NzFormDirective, NzFormItemComponent, NzFormControlComponent } from "ng-zorro-antd/form";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputGroupComponent, NzInputDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";

@UntilDestroy()
@Component({
  selector: "texera-local-login",
  templateUrl: "./local-login.component.html",
  styleUrls: ["./local-login.component.scss"],
  imports: [
    NzTabsComponent,
    NgIf,
    NzTabComponent,
    FormsModule,
    NzFormDirective,
    ReactiveFormsModule,
    NzRowDirective,
    NzFormItemComponent,
    NzColDirective,
    NzFormControlComponent,
    ɵNzTransitionPatchDirective,
    NzSpaceCompactItemDirective,
    NzInputGroupComponent,
    NzInputDirective,
    NzButtonComponent,
    NzWaveDirective,
  ],
})
export class LocalLoginComponent implements OnInit {
  public loginErrorMessage: string | undefined;
  public registerErrorMessage: string | undefined;
  public allForms: FormGroup;

  constructor(
    private formBuilder: FormBuilder,
    private userService: UserService,
    private route: ActivatedRoute,
    private notificationService: NotificationService,
    private router: Router,
    private config: GuiConfigService
  ) {
    this.allForms = this.formBuilder.group({
      loginUsername: new FormControl("", [Validators.required]),
      registerUsername: new FormControl("", [Validators.required]),
      loginPassword: new FormControl("", [Validators.required, Validators.minLength(6)]),
      registerPassword: new FormControl("", [Validators.required, Validators.minLength(6)]),
      registerConfirmationPassword: new FormControl("", [Validators.required, this.confirmationValidator]),
    });
  }

  ngOnInit() {
    if (this.config.env.defaultLocalUser && Object.keys(this.config.env.defaultLocalUser).length > 0) {
      this.allForms.patchValue({
        loginUsername: this.config.env.defaultLocalUser.username,
        loginPassword: this.config.env.defaultLocalUser.password,
      });
    }
  }

  public updateConfirmValidator(): void {
    // immediately update validator (asynchronously to wait for value to refresh)
    setTimeout(() => this.allForms.controls.registerConfirmationPassword.updateValueAndValidity(), 0);
  }

  // validator for confirm password in sign up page
  public confirmationValidator = (control: FormControl): { [s: string]: boolean } => {
    if (this.allForms && control.value !== this.allForms.controls.registerPassword.value) {
      return { confirm: true };
    }
    return {};
  };

  /**
   * This method responds to the sign-in button
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
      .pipe(
        catchError((e: unknown) => {
          const errorMessage = (e as Error)?.message || "Incorrect username or password";
          this.notificationService.error(errorMessage);
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(() =>
        this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || DASHBOARD_USER_WORKFLOW)
      );
  }

  /**
   * This method responds to the sign-up button
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
      .pipe(
        catchError((e: unknown) => {
          const errorMessage = (e as Error)?.message || "Registration failed";
          this.notificationService.error(errorMessage);
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
}
