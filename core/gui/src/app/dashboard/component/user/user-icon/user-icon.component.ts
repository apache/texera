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

import { Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { User } from "../../../../common/type/user";
import { UntilDestroy } from "@ngneat/until-destroy";
import { Router } from "@angular/router";
import { DASHBOARD_ABOUT } from "../../../../app-routing.constant";

/**
 * UserIconComponent is used to control user system on the top right corner
 * It includes the button for login/registration/logout
 * It also includes what is shown on the top right
 */
@UntilDestroy()
@Component({
  selector: "texera-user-icon",
  templateUrl: "./user-icon.component.html",
  styleUrls: ["./user-icon.component.scss"],
})
export class UserIconComponent {
  public user: User | undefined;

  constructor(
    private userService: UserService,
    private router: Router
  ) {
    this.user = this.userService.getCurrentUser();
  }

  /**
   * handle the event when user click on the logout button
   */
  public onClickLogout(): void {
    this.userService.logout();
    document.cookie = "flarum_remember=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
    this.router.navigate([DASHBOARD_ABOUT]);
  }
}
