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

import { ChangeDetectorRef, Component, NgZone, OnInit, ViewChild } from "@angular/core";
import { UserService } from "../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FlarumService } from "../service/user/flarum/flarum.service";
import { HttpErrorResponse } from "@angular/common/http";
import { ActivatedRoute, NavigationEnd, Router } from "@angular/router";
import { HubComponent } from "../../hub/component/hub.component";
import { SocialAuthService } from "@abacritt/angularx-social-login";
import { AdminSettingsService } from "../service/admin/settings/admin-settings.service";
import { GuiConfigService } from "../../common/service/gui-config.service";

import {
  DASHBOARD_ABOUT,
  DASHBOARD_ADMIN_EXECUTION,
  DASHBOARD_ADMIN_GMAIL,
  DASHBOARD_ADMIN_USER,
  DASHBOARD_ADMIN_SETTINGS,
  DASHBOARD_USER_DATASET,
  DASHBOARD_USER_DISCUSSION,
  DASHBOARD_USER_PROJECT,
  DASHBOARD_USER_QUOTA,
  DASHBOARD_USER_WORKFLOW,
} from "../../app-routing.constant";
import { Version } from "../../../environments/version";
import { SidebarTabs } from "../../common/type/gui-config";

@Component({
  selector: "texera-dashboard",
  templateUrl: "dashboard.component.html",
  styleUrls: ["dashboard.component.scss"],
})
@UntilDestroy()
export class DashboardComponent implements OnInit {
  @ViewChild(HubComponent) hubComponent!: HubComponent;

  isAdmin: boolean = this.userService.isAdmin();
  isLogin = this.userService.isLogin();
  public gitCommitHash: string = Version.raw;
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
    quota_enabled: false,
    forum_enabled: false,
    about_enabled: false,
  };

  protected readonly DASHBOARD_USER_PROJECT = DASHBOARD_USER_PROJECT;
  protected readonly DASHBOARD_USER_WORKFLOW = DASHBOARD_USER_WORKFLOW;
  protected readonly DASHBOARD_USER_DATASET = DASHBOARD_USER_DATASET;
  protected readonly DASHBOARD_USER_QUOTA = DASHBOARD_USER_QUOTA;
  protected readonly DASHBOARD_USER_DISCUSSION = DASHBOARD_USER_DISCUSSION;
  protected readonly DASHBOARD_ADMIN_USER = DASHBOARD_ADMIN_USER;
  protected readonly DASHBOARD_ADMIN_GMAIL = DASHBOARD_ADMIN_GMAIL;
  protected readonly DASHBOARD_ADMIN_EXECUTION = DASHBOARD_ADMIN_EXECUTION;
  protected readonly DASHBOARD_ADMIN_SETTINGS = DASHBOARD_ADMIN_SETTINGS;

  constructor(
    private userService: UserService,
    private router: Router,
    private flarumService: FlarumService,
    private cdr: ChangeDetectorRef,
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
      .subscribe(() => {
        this.ngZone.run(() => {
          this.isLogin = this.userService.isLogin();
          this.isAdmin = this.userService.isAdmin();
          this.forumLogin();
          this.cdr.detectChanges();
        });
      });

    this.socialAuthService.authState.pipe(untilDestroyed(this)).subscribe(user => {
      this.userService
        .googleLogin(user.idToken)
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          this.ngZone.run(() => {
            this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || DASHBOARD_USER_WORKFLOW);
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
        .subscribe(value => (this.sidebarTabs[tab] = value === "true"));
    });
  }

  forumLogin() {
    if (!document.cookie.includes("flarum_remember") && this.isLogin) {
      this.flarumService
        .auth()
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (response: any) => {
            document.cookie = `flarum_remember=${response.token};path=/`;
          },
          error: (err: unknown) => {
            if ([404, 500].includes((err as HttpErrorResponse).status)) {
              this.displayForum = false;
            } else {
              this.flarumService
                .register()
                .pipe(untilDestroyed(this))
                .subscribe(() => this.forumLogin());
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
    if (currentRoute.match(/\/dashboard\/user\/workflow\/\d+/)) {
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

  protected readonly DASHBOARD_ABOUT = DASHBOARD_ABOUT;
  protected readonly String = String;
}
