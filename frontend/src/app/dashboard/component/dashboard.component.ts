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

import { Component, NgZone, OnInit, ViewChild } from "@angular/core";
import { UserService } from "../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FlarumService } from "../service/user/flarum/flarum.service";
import { HttpErrorResponse } from "@angular/common/http";
import { ActivatedRoute, NavigationEnd, Router, RouterLink, RouterOutlet } from "@angular/router";
import { HubComponent } from "../../hub/component/hub.component";
import { SocialAuthService, GoogleSigninButtonModule } from "@abacritt/angularx-social-login";
import { AdminSettingsService } from "../service/admin/settings/admin-settings.service";
import { GuiConfigService } from "../../common/service/gui-config.service";

import {
  ABOUT,
  ADMIN_EXECUTION,
  ADMIN_GMAIL,
  ADMIN_SETTINGS,
  ADMIN_USER,
  USER_COMPUTING_UNIT,
  USER_DATASET,
  USER_DISCUSSION,
  USER_PROJECT,
  USER_QUOTA,
  USER_WORKFLOW,
} from "../../app-routing.constant";
import { Version } from "../../../environments/version";
import { SidebarTabs } from "../../common/type/gui-config";
import { User } from "../../common/type/user";
import { Role } from "../../common/type/user";
import { NzLayoutComponent, NzSiderComponent, NzContentComponent } from "ng-zorro-antd/layout";
import { NzMenuDirective, NzSubMenuComponent, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NgIf } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { SearchBarComponent } from "./user/search-bar/search-bar.component";
import { UserIconComponent } from "./user/user-icon/user-icon.component";

@Component({
  selector: "texera-dashboard",
  templateUrl: "dashboard.component.html",
  styleUrls: ["dashboard.component.scss"],
  imports: [
    NzLayoutComponent,
    NzSiderComponent,
    NzMenuDirective,
    NgIf,
    NzSubMenuComponent,
    ɵNzTransitionPatchDirective,
    HubComponent,
    NzMenuItemComponent,
    NzTooltipDirective,
    RouterLink,
    NzIconDirective,
    SearchBarComponent,
    UserIconComponent,
    GoogleSigninButtonModule,
    NzContentComponent,
    RouterOutlet,
  ],
})
@UntilDestroy()
export class DashboardComponent implements OnInit {
  @ViewChild(HubComponent) hubComponent!: HubComponent;

  isAdmin: boolean = this.userService.isAdmin();
  isLogin = this.userService.isLogin();
  public buildNumber: string = Version.buildNumber;
  displayForum: boolean = true;
  displayNavbar: boolean = true;
  isCollapsed: boolean = false;
  showLinks: boolean = false;
  logo: string = "";
  miniLogo: string = "";
  sidebarTabs: SidebarTabs = {
    hub_enabled: false,
    home_enabled: false,
    workflow_enabled: false,
    dataset_enabled: false,
    your_work_enabled: false,
    projects_enabled: false,
    workflows_enabled: false,
    datasets_enabled: false,
    compute_enabled: false,
    quota_enabled: false,
    forum_enabled: false,
    about_enabled: false,
  };

  protected readonly USER_PROJECT = USER_PROJECT;
  protected readonly USER_WORKFLOW = USER_WORKFLOW;
  protected readonly USER_DATASET = USER_DATASET;
  protected readonly USER_COMPUTING_UNIT = USER_COMPUTING_UNIT;
  protected readonly USER_QUOTA = USER_QUOTA;
  protected readonly USER_DISCUSSION = USER_DISCUSSION;
  protected readonly ADMIN_USER = ADMIN_USER;
  protected readonly ADMIN_GMAIL = ADMIN_GMAIL;
  protected readonly ADMIN_EXECUTION = ADMIN_EXECUTION;
  protected readonly ADMIN_SETTINGS = ADMIN_SETTINGS;
  protected readonly ABOUT = ABOUT;
  protected readonly String = String;

  constructor(
    private userService: UserService,
    private router: Router,
    private flarumService: FlarumService,
    private ngZone: NgZone,
    private socialAuthService: SocialAuthService,
    private route: ActivatedRoute,
    private adminSettingsService: AdminSettingsService,
    protected config: GuiConfigService
  ) {}

  ngOnInit(): void {
    this.isCollapsed = false;

    this.router.events.pipe(untilDestroyed(this)).subscribe(() => {
      this.checkRoute();
    });

    this.router.events.pipe(untilDestroyed(this)).subscribe(event => {
      if (event instanceof NavigationEnd) {
        this.checkRoute();
        this.showLinks = event.url.includes("about");
      }
    });

    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => {
        this.ngZone.run(() => {
          this.isLogin = this.userService.isLogin();
          this.isAdmin = this.userService.isAdmin();
          this.forumLogin();
        });
      });

    this.socialAuthService.authState.pipe(untilDestroyed(this)).subscribe(user => {
      this.userService
        .googleLogin(user.idToken)
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          this.ngZone.run(() => {
            this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || USER_WORKFLOW);
          });
        });
    });

    this.loadLogos();

    this.loadTabs();
  }

  loadLogos(): void {
    this.adminSettingsService
      .getSetting("logo")
      .pipe(untilDestroyed(this))
      .subscribe(dataUri => {
        this.logo = dataUri;
      });

    this.adminSettingsService
      .getSetting("mini_logo")
      .pipe(untilDestroyed(this))
      .subscribe(dataUri => {
        this.miniLogo = dataUri;
      });

    this.adminSettingsService
      .getSetting("favicon")
      .pipe(untilDestroyed(this))
      .subscribe(dataUri => {
        document.querySelectorAll("link[rel*='icon']").forEach(el => ((el as HTMLLinkElement).href = dataUri));
      });
  }

  loadTabs(): void {
    (Object.keys(this.sidebarTabs) as (keyof SidebarTabs)[]).forEach(tab => {
      this.adminSettingsService
        .getSetting(tab)
        .pipe(untilDestroyed(this))
        .subscribe(value => {
          this.sidebarTabs[tab] = value === "true";
        });
    });
  }

  forumLogin(attemptRegister: boolean = true) {
    if (!document.cookie.includes("flarum_remember") && this.isLogin) {
      this.flarumService
        .auth()
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (response: any) => {
            document.cookie = `flarum_remember=${response.token};path=/`;
          },
          error: (err: unknown) => {
            // Stop retrying on a missing/broken forum service, or once we have
            // already attempted a registration, to avoid an infinite
            // auth -> register -> auth loop when auth keeps failing.
            if ([404, 500].includes((err as HttpErrorResponse).status) || !attemptRegister) {
              this.displayForum = false;
            } else {
              this.flarumService
                .register()
                .pipe(untilDestroyed(this))
                .subscribe({
                  next: () => this.forumLogin(false),
                  error: () => (this.displayForum = false),
                });
            }
          },
        });
    }
  }

  checkRoute() {
    const currentRoute = this.router.url;
    this.displayNavbar = this.isNavbarEnabled(currentRoute);
  }

  isNavbarEnabled(currentRoute: string) {
    // Hide navbar for workflow workspace pages (with numeric ID)
    if (currentRoute.match(/\/user\/workflow\/\d+/)) {
      return false;
    }
    return true;
  }

  handleCollapseChange(collapsed: boolean) {
    this.isCollapsed = collapsed;
    const resizeEvent = new Event("resize");
    const editor = document.getElementById("workflow-editor");
    if (editor) {
      setTimeout(() => {
        window.dispatchEvent(resizeEvent);
      }, 175);
    }
  }
}
