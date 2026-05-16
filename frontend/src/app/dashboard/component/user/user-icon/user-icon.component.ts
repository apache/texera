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
import { CommonModule } from "@angular/common";
import { UserService } from "../../../../common/service/user/user.service";
import { User } from "../../../../common/type/user";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";
import { DASHBOARD_ABOUT } from "../../../../app-routing.constant";
import { UserAvatarComponent } from "../user-avatar/user-avatar.component";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzMenuDirective, NzMenuItemComponent, NzSubMenuComponent } from "ng-zorro-antd/menu";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { ThemeService } from "../../../../common/service/theme/theme.service";
import { Theme } from "../../../../common/service/theme/themes";
import { MotionService } from "../../../../common/service/motion/motion.service";

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
  imports: [
    CommonModule,
    UserAvatarComponent,
    ɵNzTransitionPatchDirective,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzSubMenuComponent,
    NzIconDirective,
  ],
})
export class UserIconComponent {
  public user: User | undefined;
  public themes: ReadonlyArray<Theme>;
  public currentThemeId: string;
  public motionOn: boolean;
  public soundOn: boolean;

  constructor(
    private userService: UserService,
    private router: Router,
    private themeService: ThemeService,
    private motionService: MotionService
  ) {
    this.user = this.userService.getCurrentUser();
    this.themes = this.themeService.themes;
    this.currentThemeId = this.themeService.getCurrent().id;
    this.motionOn = this.motionService.isMotionEnabled();
    this.soundOn = this.motionService.isSoundEnabled();
    this.themeService
      .current()
      .pipe(untilDestroyed(this))
      .subscribe(theme => (this.currentThemeId = theme.id));
    this.motionService
      .motionEnabled()
      .pipe(untilDestroyed(this))
      .subscribe(v => (this.motionOn = v));
    this.motionService
      .soundEnabled()
      .pipe(untilDestroyed(this))
      .subscribe(v => (this.soundOn = v));
  }

  public toggleMotion(): void {
    this.motionService.setMotionEnabled(!this.motionOn);
  }

  public toggleSound(): void {
    this.motionService.setSoundEnabled(!this.soundOn);
  }

  /**
   * handle the event when user click on the logout button
   */
  public onClickLogout(): void {
    this.userService.logout();
    document.cookie = "flarum_remember=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
    this.router.navigate([DASHBOARD_ABOUT]);
  }

  /**
   * handle the event when user picks a theme from the dropdown
   */
  public onSelectTheme(theme: Theme): void {
    this.themeService.setTheme(theme);
  }
}
