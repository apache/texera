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
import { HttpErrorResponse } from "@angular/common/http";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError } from "rxjs/operators";
import { EMPTY } from "rxjs";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { UserService } from "../../../common/service/user/user.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ORCID_STATE_KEY } from "../../../common/service/user/orcid-auth.service";
import { LOGIN, USER_WORKFLOW } from "../../../app-routing.constant";

/**
 * Where ORCID sends the browser back to after its consent screen, carrying the one-time `code`
 * that only the backend can redeem (see `OrcidAuthResource`). Nothing here is interactive: it
 * checks the round trip was one we started, hands the code over, and leaves.
 */
@UntilDestroy()
@Component({
  selector: "texera-orcid-callback",
  template: `
    <div class="orcid-callback">
      <nz-spin nzSimple></nz-spin>
      <p>Signing you in with ORCID…</p>
    </div>
  `,
  styles: [
    `
      .orcid-callback {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 16px;
        height: 100vh;
      }
    `,
  ],
  imports: [NzSpinComponent],
})
export class OrcidCallbackComponent implements OnInit {
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private userService: UserService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    const params = this.route.snapshot.queryParamMap;

    const expectedState = sessionStorage.getItem(ORCID_STATE_KEY);

    //remove key to prevent leakage that would authorize future sessions
    sessionStorage.removeItem(ORCID_STATE_KEY);

    const error = params.get("error");
    if (error !== null) {
      this.failBackToLogin(params.get("error_description") ?? "ORCID sign-in was not completed");
      return;
    }

    const state = params.get("state");
    if (expectedState === null || state !== expectedState) {
      this.failBackToLogin("ORCID sign-in could not be verified. Please try again.");
      return;
    }

    const code = params.get("code");
    if (code === null) {
      this.failBackToLogin("ORCID sign-in was not completed");
      return;
    }

    this.userService
      .orcidLogin(code)
      .pipe(
        catchError((e: unknown) => {
          const failure = e as HttpErrorResponse;
          this.failBackToLogin(failure?.error?.message || "ORCID sign-in failed");
          return EMPTY;
        }),
        untilDestroyed(this)
      )
      .subscribe(() => this.router.navigateByUrl(USER_WORKFLOW));
  }

  private failBackToLogin(message: string): void {
    this.notificationService.error(message);
    this.router.navigateByUrl(LOGIN, { replaceUrl: true });
  }
}
