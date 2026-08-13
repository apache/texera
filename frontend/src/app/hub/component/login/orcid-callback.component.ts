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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError } from "rxjs/operators";
import { EMPTY } from "rxjs";
import { UserService } from "../../../common/service/user/user.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { LOGIN, USER_WORKFLOW } from "../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-orcid-callback",
  template: "...",
  imports: [],
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

    const error = params.get("error");
    if (error !== null) {
      this.failBackToLogin(params.get("error_description") ?? "ORCID sign-in was not completed");
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
          this.failBackToLogin((e as Error)?.message || "ORCID sign-in failed");
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
